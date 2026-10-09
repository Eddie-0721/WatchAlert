package mute

import (
	"testing"
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
