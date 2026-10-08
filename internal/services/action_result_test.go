package services

import (
	"errors"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"strings"
	"testing"
	"watchAlert/internal/cache"
	"watchAlert/internal/ctx"
	"watchAlert/internal/models"
	"watchAlert/internal/types"
)

type claimCache struct {
	cache.AlertCacheInterface
	events    map[string]models.AlertCurEvent
	failWrite bool
	writes    int
}

func (c *claimCache) GetEventFromCache(_, _, fp string) (models.AlertCurEvent, error) {
	e, ok := c.events[fp]
	if !ok {
		return e, errors.New("missing")
	}
	return e, nil
}
func (c *claimCache) PushAlertEvent(e *models.AlertCurEvent) error {
	c.writes++
	if c.failWrite {
		return errors.New("redis down")
	}
	c.events[e.Fingerprint] = *e
	return nil
}

type claimEntry struct {
	cache.InterEntryCache
	alerts *claimCache
}

func (c claimEntry) Alert() cache.AlertCacheInterface { return c.alerts }
func TestClaimReportsActualResults(t *testing.T) {
	for _, mode := range []string{"success", "duplicate", "already_claimed", "missing", "cache_failure", "partial", "foreign", "recovered"} {
		t.Run(mode, func(t *testing.T) {
			event := models.AlertCurEvent{TenantId: "t", FaultCenterId: "fc", Fingerprint: "fp", Status: models.StateAlerting}
			if mode == "foreign" {
				event.TenantId = "other"
			}
			if mode == "recovered" {
				event.Status = models.StateRecovered
			}
			if mode == "already_claimed" {
				event.ConfirmState.IsOk = true
				event.ConfirmState.ConfirmUsername = "previous"
			}
			c := &claimCache{events: map[string]models.AlertCurEvent{"fp": event}, failWrite: mode == "cache_failure"}
			request := &types.RequestProcessAlertEvent{TenantId: "t", FaultCenterId: "fc", Username: "sre", Fingerprints: []string{"fp"}}
			if mode == "missing" {
				request.Fingerprints = []string{"missing"}
			}
			if mode == "partial" {
				request.Fingerprints = append(request.Fingerprints, "missing")
			}
			if mode == "duplicate" {
				request.Fingerprints = append(request.Fingerprints, "fp")
			}
			data, err := (eventService{ctx: &ctx.Context{Redis: claimEntry{alerts: c}}}).ProcessAlertEvent(request)
			ok := mode == "success" || mode == "duplicate" || mode == "already_claimed"
			if (err == nil) != ok {
				t.Fatal("incorrect claim success", err)
			}
			if mode == "duplicate" && c.writes != 1 {
				t.Fatal("duplicate write")
			}
			if mode == "already_claimed" && c.writes != 0 {
				t.Fatal("changed previous owner")
			}
			if mode == "partial" {
				r := data.(map[string]int)
				if r["confirmed"] != 1 || r["unconfirmed"] != 1 {
					t.Fatal(r)
				}
			}
			if (mode == "foreign" || mode == "recovered") && c.writes != 0 {
				t.Fatal("invalid event written")
			}
		})
	}
}

func TestActionTerminalStateMustBeConfirmed(t *testing.T) {
	for _, mode := range []string{"executed", "failed", "db_failure", "wrong_state", "foreign"} {
		t.Run(mode, func(t *testing.T) {
			db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
			if err != nil {
				t.Fatal(err)
			}
			conn, _ := db.DB()
			defer conn.Close()
			if err := db.AutoMigrate(&models.AgentPendingAction{}); err != nil {
				t.Fatal(err)
			}
			action := models.AgentPendingAction{ID: "a", TenantId: "t", UserId: "u", Status: "executing"}
			if mode == "wrong_state" {
				action.Status = "executed"
			}
			if err := db.Create(&action).Error; err != nil {
				t.Fatal(err)
			}
			if mode == "db_failure" {
				db.Callback().Update().Before("gorm:update").Register("reject_terminal", func(tx *gorm.DB) { tx.AddError(errors.New("unavailable")) })
			}
			if mode == "foreign" {
				action.TenantId = "other"
			}
			var executionErr error
			if mode == "failed" {
				executionErr = errors.New("silence sync not confirmed")
			}
			service := agentService{ctx: &ctx.Context{DB: policyRepo{db: db}}}
			result, err := service.finishAction(action, map[string]string{"id": "silence"}, executionErr)
			if mode == "executed" {
				if err != nil || result.Status != "executed" {
					t.Fatal(result, err)
				}
				return
			}
			if err == nil {
				t.Fatal("failure reported as success")
			}
			if mode == "failed" {
				if result.Status != "failed" {
					t.Fatal(result)
				}
				return
			}
			if result.Status != "executing" || !strings.Contains(err.Error(), "不要重复执行") {
				t.Fatal(result, err)
			}
			var stored models.AgentPendingAction
			db.First(&stored, "id = ?", "a")
			if mode != "wrong_state" && stored.Status != "executing" {
				t.Fatal("unconfirmed action became repeatable", stored.Status)
			}
		})
	}
}
