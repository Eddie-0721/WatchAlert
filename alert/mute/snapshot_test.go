package mute

import (
	"errors"
	"testing"
	"time"
	"watchAlert/internal/cache"
	"watchAlert/internal/ctx"
	"watchAlert/internal/models"
)

func TestSnapshotTimeBoundariesAndMatchers(t *testing.T) {
	rule := models.AlertSilences{Status: 1, StartsAt: 100, EndsAt: 200, Labels: []models.SilenceLabel{{Key: "env", Operator: "=~", Value: "^prod$"}, {Key: "service", Operator: "!=", Value: "test"}}}
	labels := map[string]interface{}{"env": "prod", "service": "payment"}
	for _, tc := range []struct {
		now      int64
		expected bool
	}{{99, false}, {100, true}, {199, true}, {200, false}} {
		if got := CompileSnapshot([]models.AlertSilences{rule}, tc.now)(labels); got != tc.expected {
			t.Fatalf("time=%d got=%v", tc.now, got)
		}
	}
	if CompileSnapshot([]models.AlertSilences{rule}, 150)(map[string]interface{}{"env": "prod"}) {
		t.Fatal("missing label matched")
	}
	rule.Status = 0
	if CompileSnapshot([]models.AlertSilences{rule}, 150)(labels) {
		t.Fatal("disabled rule matched")
	}
	if CompileSnapshot(nil, 150)(labels) {
		t.Fatal("empty rules matched")
	}
}

type decisionSilences struct {
	cache.SilenceCacheInterface
	rows  []models.AlertSilences
	reads int
	err   error
}

func (s *decisionSilences) ListAlertMutes(string, string) ([]models.AlertSilences, error) {
	s.reads++
	return s.rows, s.err
}

func TestCheckedSnapshotRejectsOnlyInvalidActiveRules(t *testing.T) {
	rule := models.AlertSilences{Status: 1, StartsAt: 100, EndsAt: 200, Labels: []models.SilenceLabel{{Key: "env", Operator: "=~", Value: "["}}}
	if _, err := CompileSnapshotChecked([]models.AlertSilences{rule}, 150); err == nil {
		t.Fatal("invalid active matcher accepted")
	}
	for _, now := range []int64{99, 200} {
		if _, err := CompileSnapshotChecked([]models.AlertSilences{rule}, now); err != nil {
			t.Fatal("inactive invalid rule blocked notifications", err)
		}
	}
	rule.Status = 2
	if _, err := CompileSnapshotChecked([]models.AlertSilences{rule}, 150); err != nil {
		t.Fatal(err)
	}
}

func TestLoadSnapshotReadsOnceForAllMembersAndPropagatesFailure(t *testing.T) {
	s := &decisionSilences{rows: []models.AlertSilences{{Status: 1, StartsAt: 100, EndsAt: 200, Labels: []models.SilenceLabel{{Key: "env", Operator: "=", Value: "prod"}}}}}
	match, err := LoadSnapshot(s, "t", "fc", 150)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 1000; i++ {
		if !match(map[string]interface{}{"env": "prod"}) {
			t.Fatal("match lost")
		}
	}
	if s.reads != 1 {
		t.Fatal("repeated silence reads", s.reads)
	}
	s.err = errors.New("unavailable")
	if _, err := LoadSnapshot(s, "t", "fc", 150); err == nil {
		t.Fatal("read failure hidden")
	}
}

type decisionCache struct {
	cache.InterEntryCache
	silence *decisionSilences
}

func (c decisionCache) Silence() cache.SilenceCacheInterface { return c.silence }

func TestSilenceDecisionReadsOnceAndSeesNextEdit(t *testing.T) {
	now := time.Now().Unix()
	s := &decisionSilences{rows: []models.AlertSilences{{Status: 1, StartsAt: now - 10, EndsAt: now + 100, Labels: []models.SilenceLabel{{Key: "env", Operator: "=", Value: "prod"}}}}}
	before := ctx.Redis
	ctx.Redis = decisionCache{silence: s}
	defer func() { ctx.Redis = before }()
	p := MuteParams{TenantId: "t", FaultCenterId: "fc", Labels: map[string]interface{}{"env": "prod"}}
	if !IsSilence(p) || s.reads != 1 {
		t.Fatal("match/read count changed", s.reads)
	}
	s.rows[0].Labels[0].Value = "test"
	if IsSilence(p) || s.reads != 2 {
		t.Fatal("stale snapshot reused", s.reads)
	}
}
