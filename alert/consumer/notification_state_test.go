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

type clockCache struct {
	cache.AlertCacheInterface
	calls    int
	accepted bool
	failure  error
}

func (c *clockCache) UpdateNotificationTime(context.Context, models.AlertCurEvent, int64, bool) (bool, error) {
	c.calls++
	return c.accepted, c.failure
}
func (c *clockCache) PushAlertEvent(*models.AlertCurEvent) error {
	panic("notification rewrote whole event")
}

type mutedCache struct{ cache.SilenceCacheInterface }

func (mutedCache) ListAlertMutes(string, string) ([]models.AlertSilences, error) {
	return []models.AlertSilences{{Status: 1, StartsAt: 1, EndsAt: time.Now().Unix() + 1000}}, nil
}

type notificationCache struct {
	cache.InterEntryCache
	clock *clockCache
}

func (c notificationCache) Alert() cache.AlertCacheInterface     { return c.clock }
func (c notificationCache) Silence() cache.SilenceCacheInterface { return mutedCache{} }

type notificationRepo struct{ repo.InterEntryRepo }

func (notificationRepo) Notice() repo.InterNoticeRepo { return clockNoticeRepo{} }

type clockNoticeRepo struct{ repo.InterNoticeRepo }

func (clockNoticeRepo) Get(string, string) (models.AlertNotice, error) {
	return models.AlertNotice{Name: "notice"}, nil
}

func TestNotificationAggregationUpdatesEachSourceClockOnlyOnce(t *testing.T) {
	for _, aggregation := range []string{"Rule", ""} {
		clock := &clockCache{accepted: true}
		rc := notificationCache{clock: clock}
		oldRedis := appctx.Redis
		appctx.Redis = rc
		app := &appctx.Context{Ctx: context.Background(), Redis: rc, DB: notificationRepo{}}
		events := []*models.AlertCurEvent{{TenantId: "t", FaultCenterId: "fc", Fingerprint: "one", Severity: "P0", Status: models.StateAlerting}, {TenantId: "t", FaultCenterId: "fc", Fingerprint: "two", Severity: "P0", Status: models.StateAlerting}}
		err := handleAlert(context.Background(), app, "alarm", models.FaultCenter{AggregationType: aggregation}, "notice", events)
		appctx.Redis = oldRedis
		if err != nil || clock.calls != 2 {
			t.Fatalf("%s: %v calls=%d", aggregation, err, clock.calls)
		}
		for _, event := range events {
			if event.LastSendTime == 0 {
				t.Fatal("local clock not updated")
			}
		}
	}
}

func TestNotificationRejectedClockCannotProceedToDelivery(t *testing.T) {
	for _, failure := range []error{nil, errors.New("Redis unavailable")} {
		clock := &clockCache{failure: failure}
		rc := notificationCache{clock: clock}
		// No global silence/SMTP/HTTP fixtures: reaching delivery would panic.
		app := &appctx.Context{Ctx: context.Background(), Redis: rc, DB: notificationRepo{}}
		event := &models.AlertCurEvent{Fingerprint: "one", Severity: "P0", Status: models.StateAlerting}
		err := handleAlert(context.Background(), app, "alarm", models.FaultCenter{}, "notice", []*models.AlertCurEvent{event})
		if (err != nil) != (failure != nil) || event.LastSendTime != 0 {
			t.Fatalf("%v %+v", err, event)
		}
	}
}

func TestUpgradeClockMustSucceedBeforeAddingNotification(t *testing.T) {
	for _, accepted := range []bool{false, true} {
		clock := &clockCache{accepted: accepted}
		rc := notificationCache{clock: clock}
		app := &appctx.Context{Ctx: context.Background(), Redis: rc}
		alert := &models.AlertCurEvent{Fingerprint: "one", FirstTriggerTime: 1}
		aggregated := &AggregatedAlert{}
		if err := processStage(context.Background(), app, models.FaultCenter{}, alert, 1000, aggregated, models.ConfirmStatus); err != nil {
			t.Fatal(err)
		}
		if (len(aggregated.Events) == 1) != accepted || (alert.ConfirmState.ConfirmTimeoutSendTime == 1000) != accepted {
			t.Fatal("upgrade ignored clock result", aggregated, alert)
		}
	}
}
