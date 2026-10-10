package cache

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
	"watchAlert/internal/models"
	redisclient "watchAlert/pkg/client"

	"github.com/alicebob/miniredis/v2"
	"github.com/alicebob/miniredis/v2/server"
	"github.com/redis/go-redis/v9"
)

// Exercise real TCP waits through the production cache methods and client
// options, not just cancellation before/after a mocked command.
func TestRedisMigrationCacheReadDeadline(t *testing.T) {
	for _, kind := range []string{"events", "exact", "counts", "silences", "rule-fallback"} {
		t.Run(kind, func(t *testing.T) {
			s := miniredis.RunT(t)
			o := redisclient.RedisOptions()
			o.Addr, o.Password, o.DB = s.Addr(), "", 0
			r := redis.NewClient(o)
			t.Cleanup(func() { _ = r.Close() })
			if err := r.Ping(context.Background()).Err(); err != nil {
				t.Fatal(err)
			}
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			defer close(release)
			s.Server().SetPreHook(func(_ *server.Peer, cmd string, _ ...string) bool {
				switch strings.ToLower(cmd) {
				case "hget", "hgetall", "hvals":
					once.Do(func() { close(entered) })
					<-release
				}
				return false
			})
			ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
			defer cancel()
			a := &AlertCache{rc: r}
			key := models.BuildAlertEventCacheKey("t", "fc")
			done := make(chan error, 1)
			go func() {
				var err error
				switch kind {
				case "events":
					_, err = a.GetAllEventsContext(ctx, key)
				case "exact":
					_, err = a.GetEventsByFingerprintContext(ctx, key, "fp")
				case "counts":
					_, err = a.CountEventStates(ctx, key)
				case "silences":
					_, err = (&SilenceCache{rc: r}).ListAlertMutesContext(ctx, "t", "fc")
				case "rule-fallback":
					_, err = a.readRuleEventsFull(ctx, string(key), "t", "rule")
				}
				done <- err
			}()
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("read never reached Redis")
			}
			select {
			case err := <-done:
				var netErr net.Error
				if err == nil || !(errors.Is(err, context.DeadlineExceeded) || errors.As(err, &netErr) && netErr.Timeout()) {
					t.Fatalf("timeout became an empty success or unrelated error: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("cache method failed to forward deadline")
			}
		})
	}
}
