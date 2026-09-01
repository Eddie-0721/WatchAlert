package models

import (
	"encoding/json"
	"testing"
)

func TestRepeatNoticeIntervalsAcceptsLegacyNumber(t *testing.T) {
	var intervals RepeatNoticeIntervals
	if err := json.Unmarshal([]byte(`30`), &intervals); err != nil {
		t.Fatalf("unexpected legacy interval error: %v", err)
	}

	for _, severity := range []string{"P0", "P1", "P2"} {
		if intervals[severity] != 30 {
			t.Fatalf("%s interval = %d, want 30", severity, intervals[severity])
		}
	}
}

func TestRepeatNoticeIntervalsAcceptsSeverityMap(t *testing.T) {
	var intervals RepeatNoticeIntervals
	if err := json.Unmarshal([]byte(`{"P0": 5, "P1": 15, "P2": 30}`), &intervals); err != nil {
		t.Fatalf("unexpected interval map error: %v", err)
	}
	if intervals["P0"] != 5 || intervals["P1"] != 15 || intervals["P2"] != 30 {
		t.Fatalf("interval map was not retained: %#v", intervals)
	}
}
