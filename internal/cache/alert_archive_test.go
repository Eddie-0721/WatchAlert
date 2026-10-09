package cache

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/go-redis/redis"
	"sync"
	"sync/atomic"
	"testing"
	"watchAlert/internal/models"
)

func TestRecoveredDeleteHasSingleOwner(t *testing.T) {
	c, _, event := eventStateFixture(t)
	event.Status = models.StateRecovered
	event.IsRecovered = true
	event.RecoverTime = 30
	if err := c.PushAlertEvent(&event); err != nil {
		t.Fatal(err)
	}
	var owners atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, err := c.RemoveRecoveredEvent(context.Background(), event)
			if err != nil {
				t.Error(err)
			}
			if ok {
				owners.Add(1)
			}
		}()
	}
	wg.Wait()
	if owners.Load() != 1 {
		t.Fatal("multiple delete owners", owners.Load())
	}
	if _, err := c.GetEventFromCache("t", "fc", "fp"); err != redis.Nil {
		t.Fatal("event not removed", err)
	}
}

func TestRecoveredDeleteDoesNotRemoveNewIncidentDuringCAS(t *testing.T) {
	c, s, event := eventStateFixture(t)
	event.Status = models.StateRecovered
	event.IsRecovered = true
	event.RecoverTime = 30
	if err := c.PushAlertEvent(&event); err != nil {
		t.Fatal(err)
	}
	key := string(models.BuildAlertEventCacheKey("t", "fc"))
	injected := false
	c.rc.WrapProcess(func(next func(redis.Cmder) error) func(redis.Cmder) error {
		return func(cmd redis.Cmder) error {
			if !injected && (cmd.Name() == "evalsha" || cmd.Name() == "eval") {
				injected = true
				fresh := event
				fresh.EventId = "new"
				fresh.Status = models.StateAlerting
				fresh.IsRecovered = false
				raw, _ := json.Marshal(fresh)
				s.HSet(key, "fp", string(raw))
			}
			return next(cmd)
		}
	})
	if ok, err := c.RemoveRecoveredEvent(context.Background(), event); err != nil || ok {
		t.Fatal(ok, err)
	}
	got, err := c.GetEventFromCache("t", "fc", "fp")
	if err != nil || got.EventId != "new" {
		t.Fatal("new incident deleted", got, err)
	}
}

func TestRecoveredDeleteRejectsInvalidStateCancellationAndBrokenCache(t *testing.T) {
	c, s, event := eventStateFixture(t)
	if ok, err := c.RemoveRecoveredEvent(context.Background(), event); err == nil || ok {
		t.Fatal("active event deleted")
	}
	event.Status = models.StateRecovered
	event.IsRecovered = true
	event.RecoverTime = 30
	if err := c.PushAlertEvent(&event); err != nil {
		t.Fatal(err)
	}
	requestCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if ok, err := c.RemoveRecoveredEvent(requestCtx, event); !errors.Is(err, context.Canceled) || ok {
		t.Fatal(ok, err)
	}
	wrong := event
	wrong.RecoverTime++
	if ok, err := c.RemoveRecoveredEvent(context.Background(), wrong); err != nil || ok {
		t.Fatal("different recovery deleted", ok, err)
	}
	s.HSet(string(models.BuildAlertEventCacheKey("t", "fc")), "fp", "broken-json")
	if ok, err := c.RemoveRecoveredEvent(context.Background(), event); err == nil || ok {
		t.Fatal("broken cache accepted")
	}
}

func TestRecoveredDeleteReportsRedisFailureAndBoundsConflicts(t *testing.T) {
	c, s, event := eventStateFixture(t)
	event.Status = models.StateRecovered
	event.IsRecovered = true
	event.RecoverTime = 30
	if err := c.PushAlertEvent(&event); err != nil {
		t.Fatal(err)
	}
	s.SetError("ERR unavailable")
	if ok, err := c.RemoveRecoveredEvent(context.Background(), event); err == nil || ok {
		t.Fatal("Redis error hidden")
	}
	s.SetError("")
	key := string(models.BuildAlertEventCacheKey("t", "fc"))
	conflicts := 0
	c.rc.WrapProcess(func(next func(redis.Cmder) error) func(redis.Cmder) error {
		return func(cmd redis.Cmder) error {
			if cmd.Name() == "evalsha" || cmd.Name() == "eval" {
				conflicts++
				changed := event
				changed.LastEvalTime = int64(conflicts + 100)
				raw, _ := json.Marshal(changed)
				s.HSet(key, "fp", string(raw))
			}
			return next(cmd)
		}
	})
	if ok, err := c.RemoveRecoveredEvent(context.Background(), event); ok || !errors.Is(err, ErrEventChanged) {
		t.Fatal(ok, err)
	}
	// First use can include one extra EVAL after EVALSHA reports NOSCRIPT.
	if conflicts < eventCASAttempts || conflicts > eventCASAttempts+1 {
		t.Fatal("unbounded conflict retries", conflicts)
	}
	if _, err := c.GetEventFromCache("t", "fc", "fp"); err != nil {
		t.Fatal("conflicted event removed", err)
	}
}
