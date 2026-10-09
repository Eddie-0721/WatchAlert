package services

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"watchAlert/config"
	"watchAlert/internal/models"
	"watchAlert/internal/repo"
	"watchAlert/internal/types"
	"watchAlert/pkg/secretbox"

	"gorm.io/gorm"
)

type runSettingsFixture struct {
	policySettings
	reads int
	get   func(context.Context) error
}

func (s *runSettingsFixture) GetContext(ctx context.Context) (models.Settings, error) {
	s.reads++
	if s.get != nil {
		if err := s.get(ctx); err != nil {
			return models.Settings{}, err
		}
	}
	return s.settings, ctx.Err()
}

// A legacy configuration read is a regression in request preparation.
func (s *runSettingsFixture) Get() (models.Settings, error) {
	return models.Settings{}, errors.New("unexpected context-free settings read")
}

type runTenantFixture struct {
	repo.InterTenantRepo
	get func(context.Context, string, string) (models.TenantUser, error)
}

func (r runTenantFixture) GetTenantLinkedUserInfoContext(ctx context.Context, tenant, user string) (models.TenantUser, error) {
	return r.get(ctx, tenant, user)
}

type runRepoFixture struct {
	policyRepo
	settings *runSettingsFixture
	tenant   runTenantFixture
}

func (r runRepoFixture) Setting() repo.InterSettingRepo { return r.settings }
func (r runRepoFixture) Tenant() repo.InterTenantRepo   { return r.tenant }

func TestAgentRunReadsSettingsOnceAndRefreshes(t *testing.T) {
	for _, mode := range []string{"stream", "message", "diagnostics"} {
		t.Run(mode, func(t *testing.T) {
			service, db := completionService(t)
			enabled := true
			settings := &runSettingsFixture{policySettings: policySettings{settings: models.Settings{AgentConfig: models.AgentConfig{Enable: &enabled}}}}
			ciphertext, err := secretbox.Encrypt("test-model-key", "test-credential-key")
			if err != nil {
				t.Fatal(err)
			}
			settings.settings.AgentConfig.Model = models.AgentModelConfig{Provider: "deepseek", BaseURL: "https://model.invalid", Model: "test-model", APIKeyEncrypted: ciphertext}
			service.ctx.DB = runRepoFixture{policyRepo: policyRepo{db: db}, settings: settings}
			agentTransportServer(t, func(w http.ResponseWriter, r *http.Request) {
				var payload types.AgentRunRequest
				if err := json.NewDecoder(r.Body).Decode(&payload); err != nil || payload.ModelConfig.APIKey != "test-model-key" || payload.ModelConfig.Model != "test-model" {
					t.Error("snapshot model configuration did not reach internal Agent", err)
				}
				switch mode {
				case "stream":
					fmt.Fprint(w, "event: done\ndata: {\"content\":\"answer\"}\n\n")
				case "message":
					fmt.Fprint(w, `{"content":"answer"}`)
				default:
					fmt.Fprint(w, `{"checks":[{"id":"sdk","status":"ok"},{"id":"gateway","status":"ok"},{"id":"model","status":"ok"}]}`)
				}
			})
			config.Application.Agent.CredentialKey = "test-credential-key"
			req := &types.RequestAgentSessionMessage{SessionId: "s", Content: "question"}
			switch mode {
			case "stream":
				err = service.StreamMessage(context.Background(), "t", "admin", req, func(types.AgentStreamEvent) {})
			case "message":
				_, err = service.SendMessage(context.Background(), "t", "admin", req)
			default:
				_, err = service.Diagnostics(context.Background(), "t", "admin")
			}
			if err != nil || settings.reads != 1 {
				t.Fatal("configuration was not reused", settings.reads, err)
			}
			enabled = false
			caps, err := service.Capabilities("t", "admin")
			if err != nil || caps.Enabled || settings.reads != 2 {
				t.Fatal("policy was cached across requests", caps, settings.reads, err)
			}
		})
	}
}

func TestAgentRunSettingsCancelAndRoleScope(t *testing.T) {
	service, db := completionService(t)
	if err := db.AutoMigrate(&models.UserRole{}); err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&models.UserRole{ID: "role", Permissions: []models.UserPermissions{{API: "/api/w8t/event/curEvent"}}}).Error; err != nil {
		t.Fatal(err)
	}
	enabled := true
	settings := &runSettingsFixture{policySettings: policySettings{settings: models.Settings{AgentConfig: models.AgentConfig{Enable: &enabled, AllowedTools: []string{"alerts.search", "rules.get"}}}}}
	memberReads := 0
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cancelAtMember := false
	service.ctx.DB = runRepoFixture{policyRepo: policyRepo{db: db}, settings: settings, tenant: runTenantFixture{get: func(ctx context.Context, tenant, user string) (models.TenantUser, error) {
		memberReads++
		if tenant != "t" || user != "u" {
			t.Error("membership scope changed", tenant, user)
		}
		if _, ok := ctx.Deadline(); !ok {
			t.Error("membership query has no deadline")
		}
		if cancelAtMember {
			cancel()
		}
		return models.TenantUser{UserID: "u", UserRole: "role"}, nil
	}}}
	_, caps, err := service.agentRunSettings(ctx, "t", "u")
	if err != nil || len(caps.AllowedTools) != 1 || caps.AllowedTools[0] != "alerts.search" {
		t.Fatal("role permission filtering changed", caps, err)
	}
	roleContextChecked := false
	if err := db.Callback().Query().Before("gorm:query").Register("test:role_context", func(tx *gorm.DB) {
		if tx.Statement.Table == "user_roles" {
			roleContextChecked = errors.Is(tx.Statement.Context.Err(), context.Canceled)
		}
	}); err != nil {
		t.Fatal(err)
	}
	cancelAtMember = true
	if _, _, err := service.agentRunSettings(ctx, "t", "u"); !errors.Is(err, context.Canceled) {
		t.Fatal("role query ignored cancellation", err)
	}
	if !roleContextChecked {
		t.Fatal("role SQL did not inherit cancellation")
	}
	if _, _, err := service.agentRunSettings(ctx, "t", "u"); !errors.Is(err, context.Canceled) || memberReads != 2 {
		t.Fatal("canceled settings read continued into membership", memberReads, err)
	}
	injected := errors.New("settings unavailable")
	settings.get = func(context.Context) error { return injected }
	if _, _, err := service.agentRunSettings(context.Background(), "t", "u"); !errors.Is(err, injected) || memberReads != 2 {
		t.Fatal("settings failure continued into membership", memberReads, err)
	}
}
