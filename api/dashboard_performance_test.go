package api

import (
	"context"
	"errors"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"testing"
	"watchAlert/internal/cache"
	appctx "watchAlert/internal/ctx"
	"watchAlert/internal/models"
	"watchAlert/internal/repo"
)

type dashboardDB struct {
	repo.InterEntryRepo
	db *gorm.DB
}

func (d dashboardDB) DB() *gorm.DB { return d.db }

type dashboardAlerts struct {
	cache.AlertCacheInterface
	calls int
	err   error
}

func (a *dashboardAlerts) GetAllEvents(key models.AlertEventCacheKey) (map[string]*models.AlertCurEvent, error) {
	a.calls++
	return map[string]*models.AlertCurEvent{"a": {RuleName: "same", Severity: "P0"}, "b": {RuleName: "same", Severity: "P1"}, "c": {RuleName: "other", Severity: "P2"}, "nil": nil}, a.err
}

type dashboardCache struct {
	cache.InterEntryCache
	alerts *dashboardAlerts
}

func (c dashboardCache) Alert() cache.AlertCacheInterface { return c.alerts }

func TestDashboardCountsAndEventsReadOnce(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	conn, _ := db.DB()
	defer conn.Close()
	if err = db.AutoMigrate(&models.AlertRule{}, &models.Member{}, &models.FaultCenter{}); err != nil {
		t.Fatal(err)
	}
	if err = db.Create(&[]models.AlertRule{{TenantId: "t", RuleId: "r1"}, {TenantId: "t", RuleId: "r2"}, {TenantId: "other", RuleId: "r3"}}).Error; err != nil {
		t.Fatal(err)
	}
	if err = db.Create(&[]models.FaultCenter{{TenantId: "t", ID: "fc"}, {TenantId: "other", ID: "fc2"}}).Error; err != nil {
		t.Fatal(err)
	}
	queries := 0
	if err = db.Callback().Query().Before("gorm:query").Register("test_count", func(*gorm.DB) { queries++ }); err != nil {
		t.Fatal(err)
	}
	alerts := &dashboardAlerts{}
	c := &appctx.Context{Ctx: context.Background(), DB: dashboardDB{db: db}, Redis: dashboardCache{alerts: alerts}}
	data, err := loadDashboardInfo(c, "t", models.FaultCenter{ID: "fc", TenantId: "t"})
	if err != nil {
		t.Fatal(err)
	}
	if queries != 3 || alerts.calls != 1 {
		t.Fatalf("SQL=%d Redis=%d", queries, alerts.calls)
	}
	if data.CountAlertRules != 2 || data.FaultCenterNumber != 1 || data.UserNumber != 0 || len(data.CurAlertList) != 2 || data.AlarmDistribution.P0 != 1 || data.AlarmDistribution.P1 != 1 || data.AlarmDistribution.P2 != 1 {
		t.Fatalf("unexpected stats: %+v", data)
	}
	alerts.err = errors.New("redis unavailable")
	if _, err = loadDashboardInfo(c, "t", models.FaultCenter{ID: "fc"}); err == nil {
		t.Fatal("read failure must not be returned as zero alerts")
	}
}
