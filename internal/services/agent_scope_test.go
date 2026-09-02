package services

import (
	"testing"
	"watchAlert/internal/models"
	"watchAlert/internal/types"
	"watchAlert/pkg/agenttoken"
)

func TestFilterAgentAlertScope(t *testing.T) {
	claims := agenttoken.Claims{
		DatasourceIds:       []string{"prom-prod"},
		EnvironmentLabelKey: "environment",
		Environments:        []string{"production"},
	}
	data := types.ResponseAlertCurEventList{List: []types.ResponseAlertCurEvent{
		{AlertCurEvent: models.AlertCurEvent{DatasourceId: "prom-prod", Labels: map[string]interface{}{"environment": "production"}}},
		{AlertCurEvent: models.AlertCurEvent{DatasourceId: "prom-dev", Labels: map[string]interface{}{"environment": "development"}}},
	}}
	result := filterAgentAlertScope(data, claims)
	if len(result.List) != 1 || result.List[0].DatasourceId != "prom-prod" {
		t.Fatalf("unexpected scoped alerts: %#v", result.List)
	}
}

func TestValidateSilenceActionScope(t *testing.T) {
	claims := agenttoken.Claims{
		DatasourceIds:       []string{"prom-prod"},
		EnvironmentLabelKey: "environment",
		Environments:        []string{"production"},
	}
	labels := []models.SilenceLabel{
		{Key: "environment", Value: "production", Operator: "="},
		{Key: "datasource_id", Value: "prom-prod", Operator: "="},
	}
	if err := validateSilenceActionScope(labels, claims); err != nil {
		t.Fatalf("expected labels to be allowed: %v", err)
	}
	if err := validateSilenceActionScope(labels[:1], claims); err == nil {
		t.Fatal("missing datasource scope must be rejected")
	}
}
