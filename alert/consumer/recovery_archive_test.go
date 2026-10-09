package consumer

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
	"watchAlert/internal/cache"
	appctx "watchAlert/internal/ctx"
	"watchAlert/internal/models"
	"watchAlert/internal/repo"
)

type archiveFixtureRepo struct {
	repo.InterEventRepo
	history  map[string]models.AlertHisEvent
	failure  error
	calls    int
	deadline bool
}

func (r *archiveFixtureRepo) CreateHistoryEvent(ctx context.Context, event models.AlertHisEvent) error {
	r.calls++
	deadline, ok := ctx.Deadline()
	r.deadline = ok && time.Until(deadline) <= 10*time.Second
	if err := ctx.Err(); err != nil {
		return err
	}
	key, err := event.ArchiveIdentity()
	if err != nil {
		return err
	}
	if r.failure != nil {
		return r.failure
	}
	if _, exists := r.history[key]; !exists {
		r.history[key] = event
	}
	return nil
}

type archiveFixtureDB struct {
	repo.InterEntryRepo
	event *archiveFixtureRepo
}

func (r archiveFixtureDB) Event() repo.InterEventRepo { return r.event }

type archiveFixtureCache struct {
	cache.InterEntryCache
	event *archiveFixtureAlerts
}

func (r archiveFixtureCache) Alert() cache.AlertCacheInterface { return r.event }

type archiveFixtureAlerts struct {
	cache.AlertCacheInterface
	repo     *archiveFixtureRepo
	calls    int
	failure  error
	exists   bool
	mismatch bool
}

func (r *archiveFixtureAlerts) RemoveAlertEvent(string, string, string) {
	panic("unsafe unconditional recovery delete")
}
func (r *archiveFixtureAlerts) RemoveRecoveredEvent(ctx context.Context, event models.AlertCurEvent) (bool, error) {
	r.calls++
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if len(r.repo.history) == 0 {
		return false, fmt.Errorf("delete attempted before history commit")
	}
	if r.failure != nil {
		return false, r.failure
	}
	if !r.exists || r.mismatch {
		return false, nil
	}
	r.exists = false
	return true, nil
}

func recoveryFixture() (*Consume, *archiveFixtureRepo, *archiveFixtureAlerts, models.FaultCenter, *models.AlertCurEvent) {
	history := &archiveFixtureRepo{history: map[string]models.AlertHisEvent{}}
	cached := &archiveFixtureAlerts{repo: history, exists: true}
	c := &Consume{ctx: &appctx.Context{Ctx: context.Background(), DB: archiveFixtureDB{event: history}, Redis: archiveFixtureCache{event: cached}}}
	event := notificationEvent("fp", "prod")
	event.Status = models.StateRecovered
	event.IsRecovered = true
	event.RecoverTime = 30
	return c, history, cached, models.FaultCenter{TenantId: "t", ID: "fc"}, event
}

func TestRecoveryArchiveFailureKeepsCacheAndDoesNotBlockActiveAlerts(t *testing.T) {
	c, history, cached, fc, recovered := recoveryFixture()
	history.failure = errors.New("database unavailable")
	other := *recovered
	other.Fingerprint = "second"
	active := notificationEvent("active", "prod")
	result := c.filterAlertEvents(context.Background(), fc, map[string]*models.AlertCurEvent{"first": recovered, "second": &other, "active": active})
	if len(result) != 1 || result[0] != active || history.calls != 1 || cached.calls != 0 || !cached.exists || !history.deadline {
		t.Fatal("failed archive deleted/sent recovery or exhausted DB per event", result, history, cached)
	}
}

func TestRecoveryDeleteFailureRetriesArchiveThenOnlyDeleteOwnerNotifies(t *testing.T) {
	c, history, cached, fc, event := recoveryFixture()
	rows := map[string]*models.AlertCurEvent{"fp": event}
	cached.failure = errors.New("Redis unavailable")
	if result := c.filterAlertEvents(context.Background(), fc, rows); len(result) != 0 || !cached.exists || len(history.history) != 1 {
		t.Fatal("failed delete incorrectly delivered", result)
	}
	cached.failure = nil
	if result := c.filterAlertEvents(context.Background(), fc, rows); len(result) != 1 || cached.exists || len(history.history) != 1 {
		t.Fatal("retry failed", result)
	}
	if result := c.filterAlertEvents(context.Background(), fc, rows); len(result) != 0 || len(history.history) != 1 {
		t.Fatal("second consumer sent duplicate", result)
	}
	if history.calls != 3 || cached.calls != 3 {
		t.Fatal("unexpected call order/count", history.calls, cached.calls)
	}
}

func TestRecoveryDoesNotNotifyAfterReplacementAndRejectsInvalidScope(t *testing.T) {
	c, history, cached, fc, event := recoveryFixture()
	cached.mismatch = true
	if got := c.filterAlertEvents(context.Background(), fc, map[string]*models.AlertCurEvent{"fp": event}); len(got) != 0 {
		t.Fatal("replacement notified")
	}
	if !cached.exists || len(history.history) != 1 {
		t.Fatal("replacement discarded")
	}
	foreign := *event
	foreign.TenantId = "other"
	inconsistent := *event
	inconsistent.Status = models.StateAlerting
	if got := c.filterAlertEvents(context.Background(), fc, map[string]*models.AlertCurEvent{"foreign": &foreign, "invalid": &inconsistent, "nil": nil}); len(got) != 0 || history.calls != 1 {
		t.Fatal("invalid recovery reached stores")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if got := c.filterAlertEvents(ctx, fc, map[string]*models.AlertCurEvent{"fp": event}); len(got) != 0 || history.calls != 1 {
		t.Fatal("cancellation ignored")
	}
}

func TestInvalidRecoveryDoesNotPoisonOtherArchives(t *testing.T) {
	c, history, _, fc, event := recoveryFixture()
	invalid := *event
	invalid.FirstTriggerTime = 0
	invalid.Fingerprint = "invalid"
	result := c.filterAlertEvents(context.Background(), fc, map[string]*models.AlertCurEvent{"invalid": &invalid, "valid": event})
	if len(result) != 1 || result[0] != event || len(history.history) != 1 {
		t.Fatal("invalid record poisoned other recoveries", result)
	}
}
