package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"watchAlert/internal/models"
	"watchAlert/internal/testutil"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func eventStateFixture(t *testing.T) (*AlertCache, *miniredis.Miniredis, models.AlertCurEvent) {
	t.Helper()
	s := miniredis.RunT(t)
	r := redis.NewClient(&redis.Options{Addr: s.Addr()})
	t.Cleanup(func() { r.Close() })
	e := models.AlertCurEvent{TenantId: "t", FaultCenterId: "fc", Fingerprint: "fp", EventId: "incident", RuleId: "rule", FirstTriggerTime: 10, LastEvalTime: 20, Status: models.StateAlerting, Labels: map[string]any{"value": 1}}
	c := &AlertCache{rc: r}
	if err := c.PushAlertEvent(&e); err != nil {
		t.Fatal(err)
	}
	return c, s, e
}

func TestNotificationPatchPreservesFreshEvaluationAndClaim(t *testing.T) {
	c, s, stale := eventStateFixture(t)
	fresh := stale
	fresh.LastEvalTime = 50
	fresh.Labels = map[string]any{"value": 99}
	if err := c.PushAlertEvent(&fresh); err != nil {
		t.Fatal(err)
	}
	if ok, err := c.ConfirmAlertEvent(context.Background(), stale, "operator", 60); err != nil || !ok {
		t.Fatal(ok, err)
	}
	if ok, err := c.UpdateNotificationTime(context.Background(), stale, 70, false); err != nil || !ok {
		t.Fatal(ok, err)
	}
	got, err := c.GetEventFromCache("t", "fc", "fp")
	if err != nil {
		t.Fatal(err)
	}
	if got.LastEvalTime != 50 || got.Labels["value"] != float64(99) || got.LastSendTime != 70 || got.ConfirmState.ConfirmUsername != "operator" {
		t.Fatalf("lost fresh fields: %+v", got)
	}
	// Evaluation read before the claim/notification must not undo those fields.
	fresh.LastEvalTime = 80
	fresh.Labels = map[string]any{"value": 123}
	if err := c.PushAlertEvent(&fresh); err != nil {
		t.Fatal(err)
	}
	got, _ = c.GetEventFromCache("t", "fc", "fp")
	if got.ConfirmState.ConfirmUsername != "operator" || got.LastSendTime != 70 || got.LastEvalTime != 80 {
		t.Fatal(got)
	}
	before := s.CommandCount()
	if ok, err := c.UpdateNotificationTime(context.Background(), stale, 65, false); err != nil || !ok {
		t.Fatal(ok, err)
	}
	if commands := s.CommandCount() - before; commands != 1 {
		t.Fatalf("unchanged clock should only read, commands=%d", commands)
	}
}

func TestAlertPatchesPreserveUnknownJSONAndExactNumbers(t *testing.T) {
	c, s, e := eventStateFixture(t)
	key := string(models.BuildAlertEventCacheKey("t", "fc"))
	raw := s.HGet(key, "fp")
	var fields map[string]json.RawMessage
	json.Unmarshal([]byte(raw), &fields)
	fields["futureField"] = json.RawMessage(`{"ids":[],"number":9007199254740993}`)
	fields["labels"] = json.RawMessage(`{"numeric":9007199254740993,"empty":[]}`)
	fields["confirmState"] = json.RawMessage(`{"isOk":false,"future":[]}`)
	encoded, _ := json.MarshalIndent(fields, "", " ")
	s.HSet(key, "fp", string(encoded))
	if ok, err := c.UpdateNotificationTime(context.Background(), e, 80, true); err != nil || !ok {
		t.Fatal(ok, err)
	}
	if ok, err := c.ConfirmAlertEvent(context.Background(), e, "sre", 90); err != nil || !ok {
		t.Fatal(ok, err)
	}
	stored := s.HGet(key, "fp")
	for _, want := range []string{`"numeric":9007199254740993`, `"ids":[]`, `"empty":[]`, `"future":[]`} {
		if !strings.Contains(stored, want) {
			t.Fatalf("lost %s: %s", want, stored)
		}
	}
}

func TestAlertUpdatesRejectDeletedReplacedRecoveredOrForeignEvents(t *testing.T) {
	for _, mode := range []string{"deleted", "newIncident", "newStart", "foreign", "recovered", "badJSON"} {
		t.Run(mode, func(t *testing.T) {
			c, s, old := eventStateFixture(t)
			key := string(models.BuildAlertEventCacheKey("t", "fc"))
			current := old
			switch mode {
			case "newIncident":
				current.EventId = "next"
			case "newStart":
				current.FirstTriggerTime++
			case "foreign":
				current.TenantId = "other"
			case "recovered":
				current.Status = models.StateRecovered
				current.IsRecovered = true
			}
			encoded, _ := json.Marshal(current)
			s.HSet(key, "fp", string(encoded))
			if mode == "deleted" {
				s.HDel(key, "fp")
			}
			if mode == "badJSON" {
				s.HSet(key, "fp", "{")
			}
			before := s.HGet(key, "fp")
			if ok, _ := c.UpdateNotificationTime(context.Background(), old, 90, false); ok {
				t.Fatal("stale timestamp accepted")
			}
			if ok, _ := c.ConfirmAlertEvent(context.Background(), old, "sre", 90); ok {
				t.Fatal("stale claim accepted")
			}
			if got := s.HGet(key, "fp"); got != before {
				t.Fatalf("changed event: %s", got)
			}
		})
	}
}

func TestClaimCASFirstOwnerWinsDuringConcurrentNotifications(t *testing.T) {
	c, _, e := eventStateFixture(t)
	var wg sync.WaitGroup
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := c.ConfirmAlertEvent(context.Background(), e, fmt.Sprintf("owner-%d", i), 100+int64(i))
			if err != nil && !errors.Is(err, ErrEventChanged) {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	claimed, _ := c.GetEventFromCache("t", "fc", "fp")
	if !claimed.ConfirmState.IsOk {
		t.Fatal("no claim won")
	}
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := c.UpdateNotificationTime(context.Background(), e, 200+int64(i), false)
			if err != nil && !errors.Is(err, ErrEventChanged) {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	got, _ := c.GetEventFromCache("t", "fc", "fp")
	if got.ConfirmState != claimed.ConfirmState || got.LastSendTime < 200 {
		t.Fatal("claim lost", got)
	}
	if ok, err := c.UpdateNotificationTime(context.Background(), e, 300, true); err != nil || ok {
		t.Fatal("already claimed alert upgraded", ok, err)
	}
}

func TestAlertCASRetriesConcurrentEditWithoutOverwritingIt(t *testing.T) {
	c, s, e := eventStateFixture(t)
	key := string(models.BuildAlertEventCacheKey("t", "fc"))
	injected := false
	testutil.WrapRedisProcess(c.rc, func(next func(redis.Cmder) error) func(redis.Cmder) error {
		return func(cmd redis.Cmder) error {
			if !injected && (cmd.Name() == "evalsha" || cmd.Name() == "eval") {
				injected = true
				newEvent := e
				newEvent.Labels = map[string]any{"concurrent": "edit"}
				raw, _ := json.Marshal(newEvent)
				s.HSet(key, "fp", string(raw))
			}
			return next(cmd)
		}
	})
	if ok, err := c.UpdateNotificationTime(context.Background(), e, 200, false); err != nil || !ok {
		t.Fatal(ok, err)
	}
	got, _ := c.GetEventFromCache("t", "fc", "fp")
	if got.Labels["concurrent"] != "edit" || got.LastSendTime != 200 {
		t.Fatal(got)
	}
}

func TestEvaluationCannotRegressTimeOrReviveRecovery(t *testing.T) {
	c, _, e := eventStateFixture(t)
	older := e
	older.LastEvalTime--
	if err := c.PushAlertEvent(&older); !errors.Is(err, ErrEventChanged) {
		t.Fatal(err)
	}
	recovered := e
	recovered.Status = models.StateRecovered
	recovered.IsRecovered = true
	recovered.RecoverTime = 100
	recovered.LastSendTime = 0
	if err := c.PushAlertEvent(&recovered); err != nil {
		t.Fatal(err)
	}
	if err := c.PushAlertEvent(&e); !errors.Is(err, ErrEventChanged) {
		t.Fatal("revived recovery", err)
	}
}

func TestCanceledAlertPatchDoesNotAccessRedis(t *testing.T) {
	c, s, e := eventStateFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	before := s.CommandCount()
	if _, err := c.UpdateNotificationTime(ctx, e, 200, false); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := c.ConfirmAlertEvent(ctx, e, "sre", 200); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if s.CommandCount() != before {
		t.Fatal("canceled patch accessed Redis")
	}
}

func TestDeletedEventCannotBeRecreatedByRecoveryWriter(t *testing.T) {
	c, _, e := eventStateFixture(t)
	c.RemoveAlertEvent(e.TenantId, e.FaultCenterId, e.Fingerprint)
	for _, state := range []models.AlertStatus{models.StatePendingRecovery, models.StateRecovered} {
		e.Status = state
		e.IsRecovered = state == models.StateRecovered
		if err := c.PushAlertEvent(&e); !errors.Is(err, ErrEventChanged) {
			t.Fatal(err)
		}
	}
	if _, err := c.GetEventFromCache("t", "fc", "fp"); err != redis.Nil {
		t.Fatal("deleted event recreated", err)
	}
}

func TestAlertCASConflictRetriesAreBounded(t *testing.T) {
	c, s, e := eventStateFixture(t)
	key := string(models.BuildAlertEventCacheKey("t", "fc"))
	calls := 0
	// Warm the script to avoid counting EVALSHA -> EVAL fallback twice.
	if ok, err := c.UpdateNotificationTime(context.Background(), e, 100, false); err != nil || !ok {
		t.Fatal(ok, err)
	}
	testutil.WrapRedisProcess(c.rc, func(next func(redis.Cmder) error) func(redis.Cmder) error {
		return func(cmd redis.Cmder) error {
			if cmd.Name() == "evalsha" {
				calls++
				updated := e
				updated.LastEvalTime += int64(calls)
				raw, _ := json.Marshal(updated)
				s.HSet(key, "fp", string(raw))
			}
			return next(cmd)
		}
	})
	if ok, err := c.UpdateNotificationTime(context.Background(), e, 200, false); ok || !errors.Is(err, ErrEventChanged) {
		t.Fatal(ok, err)
	}
	if calls != eventCASAttempts {
		t.Fatal("unbounded retries", calls)
	}
}

func TestEvaluationCanPopulateLegacyMissingIdentityOnlyOnce(t *testing.T) {
	c, s, e := eventStateFixture(t)
	key := string(models.BuildAlertEventCacheKey("t", "fc"))
	legacy := e
	legacy.EventId = ""
	legacy.FirstTriggerTime = 0
	legacy.ConfirmState = models.ConfirmState{IsOk: true, ConfirmUsername: "legacy-owner"}
	raw, _ := json.Marshal(legacy)
	s.HSet(key, "fp", string(raw))
	if err := c.PushAlertEvent(&e); err != nil {
		t.Fatal("legacy identity could not be upgraded", err)
	}
	got, _ := c.GetEventFromCache("t", "fc", "fp")
	if got.EventId != e.EventId || got.FirstTriggerTime != e.FirstTriggerTime || got.ConfirmState.ConfirmUsername != "legacy-owner" {
		t.Fatal(got)
	}
	e.EventId = "another"
	if err := c.PushAlertEvent(&e); !errors.Is(err, ErrEventChanged) {
		t.Fatal("existing identity replaced", err)
	}
}
