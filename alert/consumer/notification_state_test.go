package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
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
	mu       sync.Mutex
	accepted bool
	failure  error
}

func (c *clockCache) UpdateNotificationTime(context.Context, models.AlertCurEvent, int64, bool) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	return c.accepted, c.failure
}
func (c *clockCache) PushAlertEvent(*models.AlertCurEvent) error {
	panic("notification rewrote whole event")
}

type mutedCache struct {
	cache.SilenceCacheInterface
	rows  []models.AlertSilences
	err   error
	reads int
	mu    sync.Mutex
}

func (m *mutedCache) ListAlertMutes(string, string) ([]models.AlertSilences, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reads++
	return append([]models.AlertSilences(nil), m.rows...), m.err
}

type notificationCache struct {
	cache.InterEntryCache
	clock   *clockCache
	silence *mutedCache
}

func (c notificationCache) Alert() cache.AlertCacheInterface     { return c.clock }
func (c notificationCache) Silence() cache.SilenceCacheInterface { return c.silence }

type notificationRepo struct {
	repo.InterEntryRepo
	notice *clockNoticeRepo
}

func (r notificationRepo) Notice() repo.InterNoticeRepo       { return r.notice }
func (notificationRepo) DutyCalendar() repo.InterDutyCalendar { return notificationDuty{} }

type notificationDuty struct{ repo.InterDutyCalendar }

func (notificationDuty) GetDutyUserData(string, string) ([]models.Member, bool) {
	return []models.Member{{UserName: "test"}}, true
}

type clockNoticeRepo struct {
	repo.InterNoticeRepo
	data models.AlertNotice
}

func (r *clockNoticeRepo) Get(string, string) (models.AlertNotice, error) {
	return r.data, nil
}
func (*clockNoticeRepo) AddRecord(models.NoticeRecord) error { return nil }

type notificationFixture struct {
	app       *appctx.Context
	clock     *clockCache
	silence   *mutedCache
	notice    *clockNoticeRepo
	center    models.FaultCenter
	mu        sync.Mutex
	payloads  []WebhookContent
	afterSend func()
}

func newNotificationFixture(t *testing.T) *notificationFixture {
	t.Helper()
	f := &notificationFixture{clock: &clockCache{accepted: true}, silence: &mutedCache{}, center: models.FaultCenter{TenantId: "t", ID: "fc", AggregationType: "Rule"}}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload WebhookContent
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		f.mu.Lock()
		f.payloads = append(f.payloads, payload)
		f.mu.Unlock()
		if f.afterSend != nil {
			f.afterSend()
		}
		w.WriteHeader(200)
	}))
	t.Cleanup(server.Close)
	f.notice = &clockNoticeRepo{data: models.AlertNotice{Name: "notice", Routes: []models.Route{{NoticeType: "WebHook", Hook: server.URL, Severitys: []string{"P0", "P1"}}}}}
	f.app = &appctx.Context{Ctx: context.Background(), Redis: notificationCache{clock: f.clock, silence: f.silence}, DB: notificationRepo{notice: f.notice}}
	return f
}
func notificationEvent(fp, env string) *models.AlertCurEvent {
	return &models.AlertCurEvent{TenantId: "t", FaultCenterId: "fc", Fingerprint: fp, Severity: "P0", Status: models.StateAlerting, FirstTriggerTime: 1, Annotations: "original", Labels: map[string]interface{}{"env": env}}
}
func notificationMute(env string) models.AlertSilences {
	return models.AlertSilences{Status: 1, StartsAt: 1, EndsAt: time.Now().Unix() + 1000, Labels: []models.SilenceLabel{{Key: "env", Operator: "=", Value: env}}}
}
func (f *notificationFixture) send(kind string, events ...*models.AlertCurEvent) error {
	return handleAlert(context.Background(), f.app, kind, f.center, "notice", events)
}

func TestNotificationAggregationUpdatesEachSourceClockOnlyOnce(t *testing.T) {
	for _, aggregation := range []string{"Rule", ""} {
		f := newNotificationFixture(t)
		f.center.AggregationType = aggregation
		events := []*models.AlertCurEvent{{TenantId: "t", FaultCenterId: "fc", Fingerprint: "one", Severity: "P0", Status: models.StateAlerting}, {TenantId: "t", FaultCenterId: "fc", Fingerprint: "two", Severity: "P0", Status: models.StateAlerting}}
		err := f.send("alarm", events...)
		if err != nil || f.clock.calls != 2 {
			t.Fatalf("%s: %v calls=%d", aggregation, err, f.clock.calls)
		}
		want := 1
		if aggregation != "Rule" {
			want = 2
		}
		if len(f.payloads) != want || f.silence.reads != want {
			t.Fatal("send/read count", len(f.payloads), f.silence.reads)
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
		f := newNotificationFixture(t)
		f.clock.accepted, f.clock.failure = false, failure
		event := notificationEvent("one", "prod")
		err := f.send("alarm", event)
		if (err != nil) != (failure != nil) || event.LastSendTime != 0 || len(f.payloads) != 0 || f.clock.calls != 1 {
			t.Fatalf("%v %+v", err, event)
		}
	}
}

func TestUpgradeCandidateSelectionDoesNotWriteClock(t *testing.T) {
	for _, accepted := range []bool{false, true} {
		f := newNotificationFixture(t)
		f.clock.accepted = accepted
		alert := notificationEvent("one", "prod")
		aggregated := &AggregatedAlert{}
		if err := processStage(f.center, alert, 1000, aggregated); err != nil {
			t.Fatal(err)
		}
		if len(aggregated.Events) != 1 || alert.ConfirmState.ConfirmTimeoutSendTime != 0 || f.clock.calls != 0 {
			t.Fatal("candidate selection wrote state")
		}
		if err := f.send("upgrade", aggregated.Events...); err != nil {
			t.Fatal(err)
		}
		if (len(f.payloads) == 1) != accepted || (alert.ConfirmState.ConfirmTimeoutSendTime != 0) != accepted {
			t.Fatal("upgrade ignored clock result")
		}
	}
}
