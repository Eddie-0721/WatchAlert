package consumer

import (
	"context"
	"errors"
	"testing"

	appctx "watchAlert/internal/ctx"
	"watchAlert/internal/models"
)

func TestCanceledNotificationWorkDoesNotReadOrWriteStores(t *testing.T) {
	requestCtx, cancel := context.WithCancel(context.Background())
	cancel()
	// Nil DB/cache panic if canceled paths attempt to prepare or mutate alerts.
	app := &appctx.Context{Ctx: context.Background()}
	fc := models.FaultCenter{ID: "center", TenantId: "tenant"}
	if err := handleAlert(requestCtx, app, "alarm", fc, "notice", nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("handleAlert: %v", err)
	}
	if err := alarmUpgrade(requestCtx, app, fc, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("upgrade: %v", err)
	}
	c := &Consume{ctx: app}
	c.sendAlerts(requestCtx, fc, &AlertGroups{Rules: map[string]RulesGroup{"rule": {Groups: map[string]EventsGroup{"notice": {NoticeID: "notice"}}}}})
}
