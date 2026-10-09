package mute

import (
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
}

func (s *decisionSilences) ListAlertMutes(string, string) ([]models.AlertSilences, error) {
	s.reads++
	return s.rows, nil
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
