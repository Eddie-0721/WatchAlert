package tools

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/alicebob/miniredis/v2/server"
	"github.com/redis/go-redis/v9"
)

func lifecycleRedis(t *testing.T) (*redis.Client, *miniredis.Miniredis) {
	t.Helper()
	s := miniredis.RunT(t)
	// Keep this package test independent of client -> models -> tools imports.
	o := &redis.Options{Addr: s.Addr(), Protocol: 2, DisableIdentity: true,
		ContextTimeoutEnabled: true, MaxRetries: -1, DialerRetries: -1,
		ReadTimeout: 3 * time.Second, WriteTimeout: 3 * time.Second}
	r := redis.NewClient(o)
	t.Cleanup(func() { _ = r.Close() })
	return r, s
}

func TestRedisMigrationSubscriptionCancelDuringConfirmation(t *testing.T) {
	r, s := lifecycleRedis(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer close(release)
	s.Server().SetPreHook(func(_ *server.Peer, cmd string, _ ...string) bool {
		if strings.EqualFold(cmd, "subscribe") {
			once.Do(func() { close(entered) })
			<-release
		}
		return false
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { SubscribeReloadMessages(ctx, r, "fixture", func(ReloadMessage) {}); close(done) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("subscription did not start")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("confirmation did not stop")
	}
	if err := r.Ping(context.Background()).Err(); err != nil {
		t.Fatal("shared client was closed", err)
	}
}

func TestRedisMigrationSubscriptionDeliveryAndCancel(t *testing.T) {
	r, s := lifecycleRedis(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	delivered := make(chan ReloadMessage, 1)
	done := make(chan struct{})
	go func() {
		SubscribeReloadMessages(ctx, r, "fixture", func(msg ReloadMessage) { delivered <- msg })
		close(done)
	}()
	deadline := time.Now().Add(time.Second)
	for s.PubSubNumSub("fixture")["fixture"] == 0 {
		if time.Now().After(deadline) {
			t.Fatal("subscription not ready")
		}
		time.Sleep(time.Millisecond)
	}
	want := ReloadMessage{Action: ActionUpdate, ID: "rule", TenantID: "tenant", Name: "中文规则"}
	if err := PublishReloadMessage(ctx, r, "fixture", want); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-delivered:
		if got != want {
			t.Fatalf("message changed: %+v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("message not delivered")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("subscription did not stop")
	}
	if err := r.Ping(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
}

func TestRedisMigrationResignAfterCancelOnlyDeletesOwnLock(t *testing.T) {
	for _, owner := range []string{"self", "other"} {
		t.Run(owner, func(t *testing.T) {
			r, s := lifecycleRedis(t)
			ctx, cancel := context.WithCancel(context.Background())
			le := NewLeaderElector(ctx, r, nil, nil)
			le.instanceID, le.isLeader = "self", true
			if err := s.Set(LeaderElectionKey, owner); err != nil {
				t.Fatal(err)
			}
			cancel()
			le.resign()
			if le.isLeader {
				t.Fatal("local leadership not released")
			}
			if owner == "self" {
				if s.Exists(LeaderElectionKey) {
					t.Fatal("own lock not removed after cancel")
				}
			} else {
				if got, _ := s.Get(LeaderElectionKey); got != "other" {
					t.Fatal("another instance's lock deleted")
				}
			}
		})
	}
}
