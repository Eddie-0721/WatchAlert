package cache

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis"
	"watchAlert/internal/models"
)

// Deterministically cancel while traversing a decoded batch, without relying on
// goroutine timing or a large CPU fixture. Err remains canceled thereafter.
type cancelAfterChecks struct {
	context.Context
	cancel    context.CancelFunc
	remaining int
}

func (c *cancelAfterChecks) Err() error {
	c.remaining--
	if c.remaining == 0 {
		c.cancel()
	}
	return c.Context.Err()
}

func TestCacheQueryStopsDuringDecode(t *testing.T) {
	for _, kind := range []string{"events", "silences"} {
		t.Run(kind, func(t *testing.T) {
			server := miniredis.RunT(t)
			client := redis.NewClient(&redis.Options{Addr: server.Addr()})
			t.Cleanup(func() { client.Close() })
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			checks := &cancelAfterChecks{Context: ctx, cancel: cancel, remaining: 10}
			for i := 0; i < 100; i++ {
				id := fmt.Sprint(i)
				server.HSet(string(models.BuildAlertEventCacheKey("t", "fc")), id, `{}`)
				server.HSet(string(models.BuildAlertMuteCacheKey("t", "fc")), id, fmt.Sprintf(`{"tenantId":"t","faultCenterId":"fc","id":%q}`, id))
			}
			var err error
			if kind == "events" {
				var rows map[string]*models.AlertCurEvent
				rows, err = (&AlertCache{rc: client}).GetAllEventsContext(checks, models.BuildAlertEventCacheKey("t", "fc"))
				if rows != nil {
					t.Fatal("partial events returned")
				}
			} else {
				var rows []models.AlertSilences
				rows, err = (&SilenceCache{rc: client}).ListAlertMutesContext(checks, "t", "fc")
				if rows != nil {
					t.Fatal("partial silences returned")
				}
			}
			if !errors.Is(err, context.Canceled) || checks.remaining != 0 {
				t.Fatal("decode did not stop", err, checks.remaining)
			}
		})
	}
}

func TestCacheQueryCancellationBoundaries(t *testing.T) {
	for _, afterRead := range []bool{false, true} {
		for _, kind := range []string{"events", "silences"} {
			t.Run(kind+map[bool]string{false: "/before", true: "/after"}[afterRead], func(t *testing.T) {
				server := miniredis.RunT(t)
				client := redis.NewClient(&redis.Options{Addr: server.Addr()})
				t.Cleanup(func() { client.Close() })
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				if afterRead {
					client.WrapProcess(func(next func(redis.Cmder) error) func(redis.Cmder) error {
						return func(cmd redis.Cmder) error { err := next(cmd); cancel(); return err }
					})
				} else {
					cancel()
				}
				var err error
				if kind == "events" {
					var events map[string]*models.AlertCurEvent
					events, err = (&AlertCache{rc: client}).GetAllEventsContext(ctx, models.BuildAlertEventCacheKey("t", "fc"))
					if events != nil {
						t.Fatal("canceled query returned events")
					}
				} else {
					var rules []models.AlertSilences
					rules, err = (&SilenceCache{rc: client}).ListAlertMutesContext(ctx, "t", "fc")
					if rules != nil {
						t.Fatal("canceled query returned rules")
					}
				}
				if !errors.Is(err, context.Canceled) {
					t.Fatal("expected cancellation", err)
				}
				want := 0
				if afterRead {
					want = 1
				}
				if server.CommandCount() != want {
					t.Fatal("unexpected reads", server.CommandCount())
				}
			})
		}
	}
}
