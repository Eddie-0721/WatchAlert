package services

import (
	"encoding/json"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"testing"
	"time"
	"watchAlert/internal/ctx"
	"watchAlert/internal/models"
	"watchAlert/internal/repo"
	"watchAlert/internal/types"
)

type policySettings struct {
	repo.InterSettingRepo
	settings models.Settings
}

func (s *policySettings) Get() (models.Settings, error) { return s.settings, nil }

type policyRepo struct {
	repo.InterEntryRepo
	db       *gorm.DB
	settings *policySettings
}

func (r policyRepo) DB() *gorm.DB                   { return r.db }
func (r policyRepo) Setting() repo.InterSettingRepo { return r.settings }

func TestConfirmRejectsPreviouslyAuthorizedActions(t *testing.T) {
	for _, mode := range []string{"disabled", "permission", "environment", "production", "expired", "hash"} {
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
			enabled, disabled := true, false
			settings := &policySettings{settings: models.Settings{AgentConfig: models.AgentConfig{Enable: &enabled, AllowedTools: []string{"silences.propose_create"}, Scope: models.AgentScope{EnvironmentLabelKey: "env", Environments: []string{"test"}}}}}
			payload, _ := json.Marshal(types.RequestSilenceCreate{Name: "maintenance", Labels: []models.SilenceLabel{{Key: "env", Operator: "==", Value: "test"}}, StartsAt: time.Now().Unix(), EndsAt: time.Now().Add(time.Hour).Unix()})
			action := models.AgentPendingAction{ID: "a", TenantId: "tenant", UserId: "admin", ActionType: "silences.propose_create", Status: "pending_confirmation", Payload: string(payload), PayloadHash: "hash", ExpiresAt: time.Now().Add(time.Minute).Unix()}
			request := &types.RequestAgentActionConfirm{ActionId: "a", PayloadHash: "hash"}
			switch mode {
			case "disabled":
				settings.settings.AgentConfig.Enable = &disabled
			case "permission":
				settings.settings.AgentConfig.AllowedTools = []string{"alerts.search"}
			case "environment":
				settings.settings.AgentConfig.Scope.Environments = []string{"dev"}
			case "production":
				settings.settings.AgentConfig.Scope = models.AgentScope{}
			case "expired":
				action.ExpiresAt = time.Now().Add(-time.Minute).Unix()
			case "hash":
				request.PayloadHash = "changed"
			}
			if err := db.Create(&action).Error; err != nil {
				t.Fatal(err)
			}
			service := &agentService{ctx: &ctx.Context{DB: policyRepo{db: db, settings: settings}}}
			if _, err := service.ConfirmAction("tenant", "admin", request); err == nil {
				t.Fatal("stale action must be rejected")
			}
			var stored models.AgentPendingAction
			db.First(&stored, "id = ?", "a")
			if stored.Status == "executing" || stored.Status == "executed" {
				t.Fatal("rejected action was executed")
			}
		})
	}
}
