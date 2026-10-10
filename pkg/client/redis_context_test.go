package client

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/alicebob/miniredis/v2/server"
	"github.com/redis/go-redis/v9"
)

func redisPolicyFixture(t *testing.T) (*redis.Client, *miniredis.Miniredis) {
	t.Helper()
	s := miniredis.RunT(t)
	options := RedisOptions()
	options.Addr, options.Password, options.DB = s.Addr(), "", 0
	options.PoolSize = 1
	r := redis.NewClient(options)
	t.Cleanup(func() { _ = r.Close() })
	if err := r.Ping(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	return r, s
}

func TestRedisMigrationPolicyIsExplicit(t *testing.T) {
	r := redis.NewClient(RedisOptions())
	defer r.Close()
	o := r.Options()
	if o.Protocol != 2 || !o.DisableIdentity || !o.ContextTimeoutEnabled || o.MaxRetries != 0 || o.DialerRetries != -1 {
		t.Fatalf("unexpected normalized policy: protocol=%d identity=%t context=%t retries=%d dialRetries=%d", o.Protocol, o.DisableIdentity, o.ContextTimeoutEnabled, o.MaxRetries, o.DialerRetries)
	}
	if o.ReadTimeout != 3*time.Second || o.WriteTimeout != 3*time.Second || o.DialTimeout != 5*time.Second || o.PoolTimeout != 4*time.Second || o.ConnMaxIdleTime != 5*time.Minute {
		t.Fatal("migration silently changed I/O budgets")
	}
	if o.ReadBufferSize != 4096 || o.WriteBufferSize != 4096 {
		t.Fatal("migration silently increased per-connection buffers")
	}
}

func TestRedisMigrationDeadlineInterruptsSocketAndPoolRecovers(t *testing.T) {
	r, s := redisPolicyFixture(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer close(release)
	s.Server().SetPreHook(func(_ *server.Peer, cmd string, _ ...string) bool {
		if strings.EqualFold(cmd, "get") {
			once.Do(func() { close(entered) })
			<-release
		}
		return false
	})
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Get(ctx, "fixture").Err() }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("GET did not reach server")
	}
	select {
	case err := <-done:
		var netErr net.Error
		if err == nil || !(errors.Is(err, context.DeadlineExceeded) || errors.As(err, &netErr) && netErr.Timeout()) {
			t.Fatalf("unexpected deadline error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("socket ignored caller deadline")
	}
	// A discarded timed-out connection must not poison unrelated commands.
	if err := r.Ping(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	if r.PoolStats().TotalConns != 1 {
		t.Fatal("pool did not replace failed connection")
	}
}

func TestRedisMigrationPoolWaitHonorsCancelWithoutClosingOwner(t *testing.T) {
	r, s := redisPolicyFixture(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var once, releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	s.Server().SetPreHook(func(_ *server.Peer, cmd string, _ ...string) bool {
		if strings.EqualFold(cmd, "get") {
			once.Do(func() { close(entered) })
			<-release
		}
		return false
	})
	owner := make(chan error, 1)
	go func() { owner <- r.Get(context.Background(), "fixture").Err() }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("owner did not start")
	}
	ctx, cancel := context.WithCancel(context.Background())
	waiter := make(chan error, 1)
	go func() { waiter <- r.Ping(ctx).Err() }()
	// With the only slot held, this command cannot complete successfully.
	select {
	case err := <-waiter:
		t.Fatalf("waiter did not wait for the occupied pool: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	cancel()
	select {
	case err := <-waiter:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("pool wait ignored cancellation")
	}
	select {
	case <-owner:
		t.Fatal("canceling waiter interrupted unrelated owner")
	default:
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case err := <-owner:
		if err != redis.Nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("owner did not finish")
	}
	if err := r.Ping(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
}

func TestRedisMigrationAmbiguousWriteIsNotRetried(t *testing.T) {
	r, s := redisPolicyFixture(t)
	var writes atomic.Int32
	s.Server().SetPreHook(func(peer *server.Peer, cmd string, _ ...string) bool {
		if !strings.EqualFold(cmd, "incr") {
			return false
		}
		writes.Add(1)
		_, _ = s.Incr("fixture-counter", 1)
		peer.Close() // The write happened, but its reply was lost.
		return true
	})
	if err := r.Incr(context.Background(), "fixture-counter").Err(); err == nil {
		t.Fatal("lost write response reported success")
	}
	value, err := s.Get("fixture-counter")
	if err != nil || value != "1" || writes.Load() != 1 {
		t.Fatalf("write replayed: value=%q calls=%d error=%v", value, writes.Load(), err)
	}
	if err := r.Ping(context.Background()).Err(); err != nil {
		t.Fatal("shared client was closed", err)
	}
}

func TestRedisMigrationManualCancelStillNeedsSocketBudget(t *testing.T) {
	// Document the upstream boundary: ContextTimeoutEnabled installs deadlines,
	// not a socket watcher for cancel(). Never claim this migration alone fixes it.
	r, s := redisPolicyFixture(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var once, releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	s.Server().SetPreHook(func(_ *server.Peer, cmd string, _ ...string) bool {
		if strings.EqualFold(cmd, "get") {
			once.Do(func() { close(entered) })
			<-release
		}
		return false
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Get(ctx, "fixture").Err() }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("GET did not start")
	}
	cancel()
	select {
	case <-done:
		t.Fatal("upstream cancellation behavior changed; re-audit boundary")
	case <-time.After(50 * time.Millisecond):
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("read did not exit after server response")
	}
}
