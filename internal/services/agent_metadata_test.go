package services

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"watchAlert/internal/models"
	"watchAlert/internal/repo"
	"watchAlert/pkg/agenttoken"

	"gorm.io/gorm"
)

func TestAgentRulePromQLProjection(t *testing.T) {
	service, db := completionService(t)
	if err := db.AutoMigrate(&models.AlertRule{}); err != nil {
		t.Fatal(err)
	}
	rule := models.AlertRule{TenantId: "t", RuleId: "r", RuleName: "test", DatasourceIdList: []string{"ds"}, PrometheusConfig: models.PrometheusConfig{PromQL: "up", Rules: []models.Rules{{Severity: "P1", Expr: "<1"}}}}
	if err := db.Create(&rule).Error; err != nil {
		t.Fatal(err)
	}
	tool := &agentToolService{ctx: service.ctx}
	result, err := tool.getRulePromQL(context.Background(), map[string]interface{}{"ruleId": "r"}, "t")
	want := map[string]interface{}{"ruleId": "r", "ruleName": "test", "datasourceIds": rule.DatasourceIdList, "promql": "up", "severityRules": rule.PrometheusConfig.Rules}
	if err != nil || !reflect.DeepEqual(result, want) {
		t.Fatal("PromQL tool response changed", result, err)
	}
	if err := db.Model(&models.AlertRule{}).Where("rule_id = ?", "r").Update("loki_config", "invalid-json").Error; err != nil {
		t.Fatal(err)
	}
	if result, err := tool.getRulePromQL(context.Background(), map[string]interface{}{"ruleId": "r"}, "t"); err != nil || !reflect.DeepEqual(result, want) {
		t.Fatal("PromQL decoded unrelated configuration", result, err)
	}
	if _, err := tool.getRule(context.Background(), map[string]interface{}{"ruleId": "r"}, "t"); err == nil {
		t.Fatal("full rule lookup stopped validating full model")
	}
	if _, err := tool.getRulePromQL(context.Background(), map[string]interface{}{"ruleId": "r"}, "other"); !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatal("cross-tenant lookup accepted", err)
	}
	if _, err := tool.getRulePromQL(context.Background(), map[string]interface{}{}, "t"); err == nil {
		t.Fatal("empty rule ID accepted")
	}
}

type metadataSources struct {
	repo.InterDatasourceRepo
	rows []models.AlertDataSource
}

func (s metadataSources) ListSummariesContext(context.Context, string, string) ([]models.AlertDataSource, error) {
	return s.rows, nil
}

type metadataEntry struct {
	policyRepo
	sources metadataSources
}

func (r metadataEntry) Datasource() repo.InterDatasourceRepo { return r.sources }

func TestAgentDatasourceSummaryContract(t *testing.T) {
	service, db := completionService(t)
	yes, no := true, false
	rows := []models.AlertDataSource{{ID: "a", Name: "A", Type: "Prometheus", Labels: map[string]interface{}{"env": "prod"}, Description: "info", Enabled: &yes}, {ID: "b", Enabled: &no}, {ID: "c"}}
	service.ctx.DB = metadataEntry{policyRepo: policyRepo{db: db}, sources: metadataSources{rows: rows}}
	tool := &agentToolService{ctx: service.ctx}
	for _, ids := range [][]string{nil, {"a"}, {"b", "c"}, {"missing"}} {
		claims := agenttoken.Claims{TenantId: "t", DatasourceIds: ids, EnvironmentLabelKey: "env", Environments: []string{"dev"}}
		actual, err := tool.listPrometheusDatasources(context.Background(), claims)
		want := make([]map[string]interface{}, 0)
		for _, row := range rows {
			if len(ids) == 0 || containsTool(ids, row.ID) {
				want = append(want, map[string]interface{}{"id": row.ID, "name": row.Name, "type": row.Type, "labels": row.Labels, "description": row.Description, "enabled": row.GetEnabled()})
			}
		}
		if err != nil || !reflect.DeepEqual(actual, want) {
			t.Fatal("datasource output/scope changed", actual, err)
		}
	}
}
