package cache

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"watchAlert/internal/models"
	"watchAlert/internal/testutil"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestEventCountsMatchFullTypedDecode(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { client.Close() })
	cache := &AlertCache{rc: client}
	key := models.BuildAlertEventCacheKey("t", "fc")
	rows := []string{
		`{"status":"pre_alert"}`, `{"status":"alerting"}`, `{"status":"pending_recovery"}`,
		`{"status":"recovered"}`, `{"status":"unknown"}`, `{}`, `null`, `[]`, `not-json`,
		`{"status":1}`, `{"status":null}`, `{"status":"alerting","labels":1}`,
		`{"status":"alerting","confirmState":false}`, `{"status":"alerting","last_eval_time":"invalid"}`,
		`{"status":"alerting","faultCenter":{"noticeRoutes":false}}`,
		`{"status":"alerting","labels":{"nested":{"a":[1,true,null]}}}`,
		`{"status":"pre_alert","STATUS":"pending_recovery"}`,
		`{"status":"alerting","is_recovered":true,"confirmState":{"isOk":true}}`,
		// Count stored rows, not distinct body fingerprints or inferred lifecycle.
		`{"fingerprint":"same","status":"alerting"}`, `{"fingerprint":"same","status":"alerting"}`,
		`{"status":"alerting","status":"pre_alert"}`, `{"st\u0061tus":"pending_recovery"}`,
	}
	for i, raw := range rows {
		server.HSet(string(key), fmt.Sprint(i), raw)
	}
	events, err := cache.GetAllEventsContext(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	var want EventStateCounts
	for _, event := range events {
		switch event.Status {
		case models.StatePreAlert:
			want.PreAlert++
		case models.StateAlerting:
			want.Alerting++
		case models.StatePendingRecovery:
			want.PendingRecovery++
		}
	}
	commands := []string{}
	testutil.WrapRedisProcess(client, func(next func(redis.Cmder) error) func(redis.Cmder) error {
		return func(cmd redis.Cmder) error { commands = append(commands, cmd.Name()); return next(cmd) }
	})
	counts, err := cache.CountEventStates(context.Background(), key)
	if err != nil || counts != want {
		t.Fatal("counting diverged from full typed decode", counts, want, err)
	}
	if len(commands) != 1 || commands[0] != "hvals" {
		t.Fatal("counts must fetch values once without hash fields", commands)
	}
	server.Del(string(key))
	if counts, err = cache.CountEventStates(context.Background(), key); err != nil || counts != (EventStateCounts{}) {
		t.Fatal("empty hash must be a successful zero count", counts, err)
	}
	server.Set(string(key), "wrong-type")
	if counts, err = cache.CountEventStates(context.Background(), key); err == nil || counts != (EventStateCounts{}) {
		t.Fatal("storage errors must not masquerade as successful zeros", counts, err)
	}
}

func TestEventCountsResetEveryDecodedRow(t *testing.T) {
	// Ordered data ensures null/absent status cannot inherit from the last row,
	// including an invalid row that partially sets status before decoding fails.
	counts, err := countEventValues(context.Background(), []string{
		`{"status":"alerting"}`, `null`, `{}`, `{"status":null}`,
		`{"status":"alerting","labels":false}`, `{}`,
		`{"status":"pre_alert"}`, `{"labels":{"env":"prod"}}`,
		`{"status":"pending_recovery"}`, `{"status":"recovered"}`,
	})
	if err != nil || counts != (EventStateCounts{PreAlert: 1, Alerting: 1, PendingRecovery: 1}) {
		t.Fatal("reused decoder inherited a previous row", counts, err)
	}
}

func TestEventCountsCancellation(t *testing.T) {
	for _, phase := range []string{"before", "after-read", "after-empty-read", "during-decode"} {
		t.Run(phase, func(t *testing.T) {
			server := miniredis.RunT(t)
			client := redis.NewClient(&redis.Options{Addr: server.Addr()})
			t.Cleanup(func() { client.Close() })
			key := models.BuildAlertEventCacheKey("t", "fc")
			if phase != "after-empty-read" {
				for i := 0; i < 100; i++ {
					server.HSet(string(key), fmt.Sprint(i), `{"status":"alerting"}`)
				}
			}
			requestCtx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if phase == "before" {
				cancel()
			} else if phase != "during-decode" {
				testutil.WrapRedisProcess(client, func(next func(redis.Cmder) error) func(redis.Cmder) error {
					return func(cmd redis.Cmder) error { err := next(cmd); cancel(); return err }
				})
			}
			if phase == "during-decode" {
				requestCtx = &cancelAfterChecks{Context: requestCtx, cancel: cancel, remaining: 10}
			}
			counts, err := (&AlertCache{rc: client}).CountEventStates(requestCtx, key)
			if counts != (EventStateCounts{}) || !errors.Is(err, context.Canceled) {
				t.Fatal("canceled count must not expose partial statistics", counts, err)
			}
			if phase == "before" && server.CommandCount() != 0 {
				t.Fatal("already canceled request touched Redis")
			}
		})
	}
}
