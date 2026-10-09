package services

import (
	"context"
	"errors"
	"testing"
	"time"
	"watchAlert/internal/models"
	"watchAlert/internal/repo"
	"watchAlert/internal/types"
	"watchAlert/pkg/agenttoken"

	"gorm.io/gorm"
)

func TestAgentToolCancellationStillAudited(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		service, db := completionService(t)
		if err := db.AutoMigrate(&models.AgentToolCall{}, &models.AlertRule{}); err != nil {
			t.Fatal(err)
		}
		if err := db.Create(&models.AlertRule{TenantId: "t", RuleId: "r", RuleName: "test"}).Error; err != nil {
			t.Fatal(err)
		}
		previous := AgentService
		AgentService = service
		t.Cleanup(func() { AgentService = previous })
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		if canceled {
			cancel()
		}
		auditSeen := false
		if err := db.Callback().Create().Before("gorm:create").Register("test:audit_context", func(tx *gorm.DB) {
			if tx.Statement.Table == (models.AgentToolCall{}).TableName() {
				auditSeen = true
				deadline, ok := tx.Statement.Context.Deadline()
				if !ok || time.Until(deadline) > agentDatabaseTimeout || tx.Statement.Context.Err() != nil {
					t.Error("audit missing independent bounded context")
				}
			}
		}); err != nil {
			t.Fatal(err)
		}
		tool := &agentToolService{ctx: service.ctx}
		result, err := tool.Execute(ctx, agenttoken.Claims{TenantId: "t", UserId: "admin", SessionId: "s", Tools: []string{"rules.get"}}, "rules.get", map[string]interface{}{"ruleId": "r"})
		if canceled {
			if !errors.Is(err, context.Canceled) || result != nil {
				t.Fatal("canceled tool executed", result, err)
			}
		} else if err != nil || result.(models.AlertRule).RuleName != "test" {
			t.Fatal(result, err)
		}
		var audits []models.AgentToolCall
		if err := db.Find(&audits).Error; err != nil || !auditSeen || len(audits) != 1 {
			t.Fatal("audit missing", audits, err)
		}
		want := "completed"
		if canceled {
			want = "failed"
		}
		if audits[0].Status != want || audits[0].TenantId != "t" || audits[0].UserId != "admin" {
			t.Fatal("audit semantics changed", audits[0])
		}
	}
}

type toolDatasourceFixture struct {
	repo.InterDatasourceRepo
	observed bool
}

func (d *toolDatasourceFixture) ListSummariesContext(ctx context.Context, tenant, kind string) ([]models.AlertDataSource, error) {
	d.observed = tenant == "t" && kind == "Prometheus"
	return nil, ctx.Err()
}
func (d *toolDatasourceFixture) GetForTenantContext(ctx context.Context, tenant, id string) (models.AlertDataSource, error) {
	d.observed = tenant == "t" && id == "ds"
	return models.AlertDataSource{}, ctx.Err()
}

type toolRepoFixture struct {
	policyRepo
	datasource *toolDatasourceFixture
}

func (r toolRepoFixture) Datasource() repo.InterDatasourceRepo { return r.datasource }

type toolCenterFixture struct {
	InterFaultCenterService
	observed bool
}

func (s *toolCenterFixture) GetContext(ctx context.Context, request interface{}) (interface{}, interface{}) {
	r := request.(*types.RequestFaultCenterQuery)
	s.observed = r.TenantId == "t" && r.ID == "fc"
	return nil, ctx.Err()
}

type toolSilenceFixture struct {
	InterSilenceService
	observed bool
}

func (s *toolSilenceFixture) ListContext(ctx context.Context, request interface{}) (interface{}, interface{}) {
	r := request.(*types.RequestSilenceQuery)
	s.observed = r.TenantId == "t" && r.Status == "all" && r.Size == 50
	return nil, ctx.Err()
}

func TestAgentReadToolsForwardCancellationAndScope(t *testing.T) {
	service, db := completionService(t)
	datasource := &toolDatasourceFixture{}
	service.ctx.DB = toolRepoFixture{policyRepo: policyRepo{db: db}, datasource: datasource}
	tool := &agentToolService{ctx: service.ctx}
	previousCenter, previousSilence := FaultCenterService, SilenceService
	center, silence := &toolCenterFixture{}, &toolSilenceFixture{}
	FaultCenterService, SilenceService = center, silence
	defer func() { FaultCenterService, SilenceService = previousCenter, previousSilence }()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	claims := agenttoken.Claims{TenantId: "t"}
	for name, call := range map[string]func() (interface{}, error){
		"rule": func() (interface{}, error) { return tool.getRule(ctx, map[string]interface{}{"ruleId": "r"}, "t") },
		"promql": func() (interface{}, error) {
			return tool.getRulePromQL(ctx, map[string]interface{}{"ruleId": "r"}, "t")
		},
		"center": func() (interface{}, error) {
			return tool.getIncident(ctx, map[string]interface{}{"id": "fc", "tenantId": "other"}, "t")
		},
		"silences": func() (interface{}, error) {
			return tool.searchSilences(ctx, map[string]interface{}{"size": 100, "tenantId": "other"}, "t")
		},
		"datasources": func() (interface{}, error) { return tool.listPrometheusDatasources(ctx, claims) },
		"query": func() (interface{}, error) {
			return tool.queryPrometheus(ctx, map[string]interface{}{"datasourceId": "ds", "promql": "up"}, claims, false)
		},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := call(); !errors.Is(err, context.Canceled) {
				t.Fatal("request cancellation lost", err)
			}
		})
	}
	if !center.observed || !silence.observed || !datasource.observed {
		t.Fatal("tenant/page filters changed", center.observed, silence.observed, datasource.observed)
	}
}

func TestAgentToolAuditFailureDoesNotReplaceReadOutcome(t *testing.T) {
	service, db := completionService(t)
	if err := db.AutoMigrate(&models.AgentToolCall{}, &models.AlertRule{}); err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&models.AlertRule{TenantId: "t", RuleId: "r"}).Error; err != nil {
		t.Fatal(err)
	}
	previous := AgentService
	AgentService = service
	defer func() { AgentService = previous }()
	injected := errors.New("audit storage unavailable")
	if err := db.Callback().Create().Before("gorm:create").Register("test:audit_fail", func(tx *gorm.DB) { tx.AddError(injected) }); err != nil {
		t.Fatal(err)
	}
	tool := &agentToolService{ctx: service.ctx}
	if _, err := tool.Execute(context.Background(), agenttoken.Claims{TenantId: "t", UserId: "admin", Tools: []string{"rules.get"}}, "rules.get", map[string]interface{}{"ruleId": "r"}); err != nil {
		t.Fatal("best-effort audit changed query outcome", err)
	}
	if err := tool.saveAgentToolAudit(&models.AgentToolCall{ID: "audit"}); !errors.Is(err, injected) {
		t.Fatal("audit failure hidden", err)
	}
}
