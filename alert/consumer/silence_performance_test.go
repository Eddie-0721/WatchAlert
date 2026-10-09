package consumer

import (
	"context"
	"errors"
	"testing"
	"time"
	"watchAlert/internal/cache"
	appctx "watchAlert/internal/ctx"
	"watchAlert/internal/models"
	"watchAlert/internal/repo"
)

type lifecycleSilenceCache struct {
	cache.SilenceCacheInterface
	rows          []cache.SilenceSnapshot
	reads, writes int
	fail          bool
}

func (c *lifecycleSilenceCache) ListSilenceSnapshots(string, string) ([]cache.SilenceSnapshot, error) {
	c.reads++
	return append([]cache.SilenceSnapshot(nil), c.rows...), nil
}
func (c *lifecycleSilenceCache) CompareAndSwapSilenceStatus(before cache.SilenceSnapshot, next int) (bool, error) {
	c.writes++
	if c.fail {
		return false, errors.New("cache unavailable")
	}
	for i := range c.rows {
		if c.rows[i].Rule.ID == before.Rule.ID {
			if next == 2 {
				c.rows = append(c.rows[:i], c.rows[i+1:]...)
			} else {
				c.rows[i].Rule.Status = next
			}
			break
		}
	}
	return true, nil
}

type lifecycleCache struct {
	cache.InterEntryCache
	silence *lifecycleSilenceCache
}

func (c lifecycleCache) Silence() cache.SilenceCacheInterface { return c.silence }

type lifecycleSilenceRepo struct {
	repo.InterSilenceRepo
	writes         int
	fail, conflict bool
}

func (r *lifecycleSilenceRepo) TransitionStatus(context.Context, models.AlertSilences, int) (bool, error) {
	r.writes++
	if r.fail {
		return false, errors.New("SQL unavailable")
	}
	return !r.conflict, nil
}

type lifecycleDB struct {
	repo.InterEntryRepo
	silence *lifecycleSilenceRepo
}

func (d lifecycleDB) Silence() repo.InterSilenceRepo { return d.silence }

func TestSilenceLifecycleOnlyWritesTransitions(t *testing.T) {
	now := time.Now().Unix()
	cached := &lifecycleSilenceCache{}
	for i := 0; i < 1000; i++ {
		cached.rows = append(cached.rows, cache.SilenceSnapshot{Rule: models.AlertSilences{ID: string(rune(i + 1000)), Status: 1, StartsAt: now - 100, EndsAt: now + 1000}})
	}
	cached.rows = append(cached.rows,
		cache.SilenceSnapshot{Rule: models.AlertSilences{ID: "start", Status: 0, StartsAt: now - 1, EndsAt: now + 1000}},
		cache.SilenceSnapshot{Rule: models.AlertSilences{ID: "expired", Status: 2, EndsAt: now - 1}},
		cache.SilenceSnapshot{Rule: models.AlertSilences{ID: "expire", Status: 0, StartsAt: now - 100, EndsAt: now - 1}})
	db := &lifecycleSilenceRepo{}
	c := &Consume{ctx: &appctx.Context{Ctx: context.Background(), Redis: lifecycleCache{silence: cached}, DB: lifecycleDB{silence: db}}}
	fc := models.FaultCenter{TenantId: "t", ID: "fc"}
	c.processSilenceRule(context.Background(), fc)
	if cached.reads != 1 || cached.writes != 3 || db.writes != 3 {
		t.Fatal(cached.reads, cached.writes, db.writes)
	}
	c.processSilenceRule(context.Background(), fc)
	if cached.reads != 2 || cached.writes != 3 || db.writes != 3 {
		t.Fatal("unchanged lifecycle was rewritten", cached.reads, cached.writes, db.writes)
	}
}

func TestSilenceLifecycleFailureDoesNotOverwriteCache(t *testing.T) {
	for _, conflict := range []bool{false, true} {
		cached := &lifecycleSilenceCache{rows: []cache.SilenceSnapshot{{Rule: models.AlertSilences{ID: "s", Status: 0, EndsAt: 1}}}}
		db := &lifecycleSilenceRepo{fail: !conflict, conflict: conflict}
		c := &Consume{ctx: &appctx.Context{Ctx: context.Background(), Redis: lifecycleCache{silence: cached}, DB: lifecycleDB{silence: db}}}
		c.processSilenceRule(context.Background(), models.FaultCenter{ID: "fc", TenantId: "t"})
		if cached.writes != 0 {
			t.Fatal("SQL failure/conflict still wrote stale cache")
		}
	}
}

func TestSilenceLifecycleRetriesCacheFailureAndHonorsCancellation(t *testing.T) {
	cached := &lifecycleSilenceCache{fail: true, rows: []cache.SilenceSnapshot{{Rule: models.AlertSilences{ID: "s", Status: 0, EndsAt: 1}}}}
	db := &lifecycleSilenceRepo{}
	c := &Consume{ctx: &appctx.Context{Ctx: context.Background(), Redis: lifecycleCache{silence: cached}, DB: lifecycleDB{silence: db}}}
	fc := models.FaultCenter{ID: "fc", TenantId: "t"}
	c.processSilenceRule(context.Background(), fc)
	if len(cached.rows) != 1 {
		t.Fatal("failed cache transition lost retry state")
	}
	cached.fail = false
	c.processSilenceRule(context.Background(), fc)
	if len(cached.rows) != 0 || db.writes != 2 {
		t.Fatal("cache transition not retried")
	}
	cached.rows = []cache.SilenceSnapshot{{Rule: models.AlertSilences{ID: "s", Status: 0, EndsAt: 1}}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c.processSilenceRule(ctx, fc)
	if db.writes != 2 {
		t.Fatal("cancelled consumer wrote status")
	}
}
