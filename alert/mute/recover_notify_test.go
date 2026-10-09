package mute

import "testing"

func TestRecoveryNotificationUnsetUsesDisabledDefault(t *testing.T) {
	enabled, disabled := true, false
	for _, tc := range []struct {
		recovered bool
		setting   *bool
		muted     bool
	}{
		{true, nil, true}, {true, &disabled, true}, {true, &enabled, false},
		{false, nil, false}, {false, &disabled, false}, {false, &enabled, false},
	} {
		if got := RecoverNotify(MuteParams{IsRecovered: tc.recovered, RecoverNotify: tc.setting}); got != tc.muted {
			t.Fatalf("recovered=%v setting=%v: got %v", tc.recovered, tc.setting, got)
		}
	}
}
