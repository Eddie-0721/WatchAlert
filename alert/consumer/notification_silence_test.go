package consumer

import (
	"context"
	"errors"
	"strings"
	"testing"
	"watchAlert/internal/models"
)

func TestNotificationFiltersEveryMemberBeforeAggregation(t *testing.T) {
	for _, kind := range []string{"alarm", "upgrade"} {
		for _, mutedFirst := range []bool{true, false} {
			f := newNotificationFixture(t)
			f.silence.rows = []models.AlertSilences{notificationMute("prod")}
			muted, visible := notificationEvent("muted", "prod"), notificationEvent("visible", "test")
			events := []*models.AlertCurEvent{muted, visible}
			if !mutedFirst {
				events[0], events[1] = events[1], events[0]
			}
			if err := f.send(kind, events...); err != nil {
				t.Fatal(err)
			}
			if len(f.payloads) != 1 || f.payloads[0].Alarm.Fingerprint != "visible" || strings.Contains(f.payloads[0].Alarm.Annotations, "聚合") || f.clock.calls != 1 || f.silence.reads != 1 {
				t.Fatalf("%s first=%v sends=%+v clocks=%d reads=%d", kind, mutedFirst, f.payloads, f.clock.calls, f.silence.reads)
			}
			if muted.LastSendTime != 0 || muted.ConfirmState.ConfirmTimeoutSendTime != 0 {
				t.Fatal("muted member clock advanced")
			}
			for _, event := range events {
				if event.Annotations != "original" || event.DutyUser != "" {
					t.Fatal("source payload changed")
				}
			}
		}
	}
}

func TestNotificationSilenceFailuresDoNotAdvanceClockOrSend(t *testing.T) {
	for _, kind := range []string{"alarm", "upgrade"} {
		for _, invalidRule := range []bool{true, false} {
			f := newNotificationFixture(t)
			if invalidRule {
				rule := notificationMute("prod")
				rule.Labels[0].Operator, rule.Labels[0].Value = "=~", "["
				f.silence.rows = []models.AlertSilences{rule}
			} else {
				f.silence.err = errors.New("Redis unavailable")
			}
			if err := f.send(kind, notificationEvent("one", "prod")); err == nil {
				t.Fatal("silence failure hidden")
			}
			if len(f.payloads) != 0 || f.clock.calls != 0 {
				t.Fatal("silence failure allowed send/clock update")
			}
		}
	}
}

func TestNotificationNoEligibleRouteDoesNotAdvanceClock(t *testing.T) {
	f := newNotificationFixture(t)
	f.notice.data.Routes[0].Severitys = []string{"P2"}
	if err := f.send("alarm", notificationEvent("one", "prod")); err != nil {
		t.Fatal(err)
	}
	if f.clock.calls != 0 || f.silence.reads != 0 || len(f.payloads) != 0 {
		t.Fatal("no route still updated state")
	}
}

func TestNotificationRechecksSilencesAfterRoute(t *testing.T) {
	f := newNotificationFixture(t)
	f.notice.data.Routes = append(f.notice.data.Routes, f.notice.data.Routes[0])
	f.afterSend = func() {
		f.silence.mu.Lock()
		defer f.silence.mu.Unlock()
		f.silence.rows = []models.AlertSilences{notificationMute("prod")}
	}
	if err := f.send("alarm", notificationEvent("one", "prod")); err != nil {
		t.Fatal(err)
	}
	if len(f.payloads) != 1 || f.silence.reads != 2 || f.clock.calls != 1 {
		t.Fatalf("sends=%d reads=%d clocks=%d", len(f.payloads), f.silence.reads, f.clock.calls)
	}
}

func TestUpgradeRoutesAllCandidatesBySeverity(t *testing.T) {
	f := newNotificationFixture(t)
	one, two, three := notificationEvent("one", "prod"), notificationEvent("two", "test"), notificationEvent("three", "test")
	two.Severity, three.Severity = "P1", "P1"
	// Distinct routes detect accidental cross-severity delivery.
	f.notice.data.Routes[0].Severitys = []string{"P0"}
	route := f.notice.data.Routes[0]
	route.Severitys = []string{"P1"}
	f.notice.data.Routes = append(f.notice.data.Routes, route)
	if err := f.send("upgrade", one, two, three); err != nil {
		t.Fatal(err)
	}
	if len(f.payloads) != 2 || f.clock.calls != 3 {
		t.Fatal("severity group lost", f.payloads, f.clock.calls)
	}
	seen := map[string]bool{}
	for _, p := range f.payloads {
		seen[p.Alarm.Severity] = true
		if p.Alarm.Severity == "P1" && !strings.Contains(p.Alarm.Annotations, "聚合 2 条升级通知") {
			t.Fatal("wrong aggregation count")
		}
	}
	if !seen["P0"] || !seen["P1"] {
		t.Fatal("wrong severity routing")
	}
	for _, event := range []*models.AlertCurEvent{one, two, three} {
		if event.ConfirmState.ConfirmTimeoutSendTime == 0 || event.LastSendTime != 0 || event.Annotations != "original" {
			t.Fatal("incorrect upgrade source state")
		}
	}
}

func TestNotificationSuppressedAndDisabledRecoveryHaveNoWrites(t *testing.T) {
	f := newNotificationFixture(t)
	f.silence.rows = []models.AlertSilences{notificationMute("prod")}
	event := notificationEvent("one", "prod")
	if err := f.send("alarm", event); err != nil {
		t.Fatal(err)
	}
	event.IsRecovered = true
	if err := f.send("recover", event); err != nil {
		t.Fatal(err)
	}
	if f.clock.calls != 0 || len(f.payloads) != 0 || f.silence.reads != 1 {
		t.Fatal("muted/recovery-disabled event proceeded")
	}
}

func TestNotificationScopeMismatchAndCancellationFailClosed(t *testing.T) {
	f := newNotificationFixture(t)
	event := notificationEvent("one", "prod")
	event.TenantId = "other"
	if err := f.send("alarm", event); err == nil {
		t.Fatal("scope mismatch accepted")
	}
	requestCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := prepareNotificationMembers(requestCtx, f.app, "alarm", f.center, []*models.AlertCurEvent{notificationEvent("two", "prod")}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if f.clock.calls != 0 || f.silence.reads != 0 || len(f.payloads) != 0 {
		t.Fatal("scope/cancel changed state")
	}
}
