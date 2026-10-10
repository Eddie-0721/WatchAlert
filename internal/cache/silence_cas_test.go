package cache

import (
	"encoding/json"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"sync"
	"sync/atomic"
	"testing"
	"watchAlert/internal/models"
)

func silenceCASFixture(t *testing.T) (*SilenceCache, *miniredis.Miniredis, models.AlertSilences) {
	t.Helper()
	s := miniredis.RunT(t)
	r := redis.NewClient(&redis.Options{Addr: s.Addr()})
	t.Cleanup(func() { _ = r.Close() })
	return &SilenceCache{rc: r}, s, models.AlertSilences{TenantId: "t", FaultCenterId: "fc", ID: "s", Status: 0, Labels: []models.SilenceLabel{}, StartsAt: 10, EndsAt: 100}
}

func TestSilenceCASPreservesWireFormatAndConcurrentEdits(t *testing.T) {
	c, s, rule := silenceCASFixture(t)
	key := string(models.BuildAlertMuteCacheKey("t", "fc"))
	raw, _ := json.MarshalIndent(rule, "", "  ")
	s.HSet(key, "s", string(raw)) // legacy formatting must compare against original bytes
	snapshots, err := c.ListSilenceSnapshots("t", "fc")
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := c.CompareAndSwapSilenceStatus(snapshots[0], 1); err != nil || !ok {
		t.Fatal(ok, err)
	}
	var stored models.AlertSilences
	if err := json.Unmarshal([]byte(s.HGet(key, "s")), &stored); err != nil || stored.Status != 1 || stored.Labels == nil {
		t.Fatal("empty array corrupted", stored, err)
	}
	snapshots, _ = c.ListSilenceSnapshots("t", "fc")
	stored.Comment = "concurrent user edit"
	stored.EndsAt = 200
	if err := c.PushAlertMute(stored); err != nil {
		t.Fatal(err)
	}
	if ok, err := c.CompareAndSwapSilenceStatus(snapshots[0], 2); err != nil || ok {
		t.Fatal("stale transition overwrote edit", ok, err)
	}
	snapshots, _ = c.ListSilenceSnapshots("t", "fc")
	if snapshots[0].Rule.Comment != "concurrent user edit" || snapshots[0].Rule.EndsAt != 200 {
		t.Fatal("edit lost")
	}
	if err := c.RemoveAlertMute("t", "fc", "s"); err != nil {
		t.Fatal(err)
	}
	if ok, err := c.CompareAndSwapSilenceStatus(snapshots[0], 2); err != nil || ok {
		t.Fatal("deleted silence resurrected", ok, err)
	}
	if s.HGet(key, "s") != "" {
		t.Fatal("deleted rule restored")
	}
}

func TestSilenceConcurrentCASOnlyOneWinner(t *testing.T) {
	c, _, rule := silenceCASFixture(t)
	if err := c.PushAlertMute(rule); err != nil {
		t.Fatal(err)
	}
	snapshots, _ := c.ListSilenceSnapshots("t", "fc")
	var wg sync.WaitGroup
	var updates atomic.Int32
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, err := c.CompareAndSwapSilenceStatus(snapshots[0], 1)
			if err != nil {
				t.Error(err)
			}
			if ok {
				updates.Add(1)
			}
		}()
	}
	wg.Wait()
	if updates.Load() != 1 {
		t.Fatal("CAS not atomic", updates.Load())
	}
	_ = c.rc.Close()
	if _, err := c.CompareAndSwapSilenceStatus(snapshots[0], 2); err == nil {
		t.Fatal("cache error hidden")
	}
}

func TestExpiredSilenceCASRemovesOnlyMatchingEntry(t *testing.T) {
	c, s, rule := silenceCASFixture(t)
	if err := c.PushAlertMute(rule); err != nil {
		t.Fatal(err)
	}
	other := rule
	other.ID = "other"
	if err := c.PushAlertMute(other); err != nil {
		t.Fatal(err)
	}
	snapshots, _ := c.ListSilenceSnapshots("t", "fc")
	for _, snapshot := range snapshots {
		if snapshot.Rule.ID != "s" {
			continue
		}
		if ok, err := c.CompareAndSwapSilenceStatus(snapshot, 2); !ok || err != nil {
			t.Fatal(ok, err)
		}
	}
	key := string(models.BuildAlertMuteCacheKey("t", "fc"))
	if s.HGet(key, "s") != "" || s.HGet(key, "other") == "" {
		t.Fatal("expiry evicted wrong cache entry")
	}
}

func TestSilenceSnapshotUsesOneRead(t *testing.T) {
	c, s, rule := silenceCASFixture(t)
	for i := 0; i < 1000; i++ {
		rule.ID = string(rune(1000 + i))
		if err := c.PushAlertMute(rule); err != nil {
			t.Fatal(err)
		}
	}
	before := s.CommandCount()
	rules, err := c.ListAlertMutes("t", "fc")
	if err != nil || len(rules) != 1000 || s.CommandCount()-before != 1 {
		t.Fatal("expected one read", len(rules), err, s.CommandCount()-before)
	}
}
