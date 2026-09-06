package services

import (
	"testing"
	"watchAlert/internal/models"
	"watchAlert/internal/types"
)

func TestSummaryCountsBeforePaginationAndSeparatesState(t *testing.T) {
	events := make([]types.ResponseAlertCurEvent, 125)
	for i := range events {
		events[i].LifecycleStatus = models.StateAlerting
		events[i].Scope.Environment = "prod"
	}
	events[0].Silenced = true
	events[1].Acknowledged = true
	events[2].LifecycleStatus = models.StatePreAlert
	events[3].LifecycleStatus = models.StateRecovered
	result := summarizeAlertEvents(events)
	if result.Queues["all"] != 124 || result.Queues["attention"] != 121 || result.Queues["processing"] != 1 || result.Queues["suppressed"] != 1 || result.Queues["observing"] != 1 {
		t.Fatalf("bad counts: %#v", result)
	}
	if len(result.Environments) != 1 {
		t.Fatal("facet values should be unique")
	}
}

func TestAgentScopeUsesConfiguredRawLabel(t *testing.T) {
	event := models.AlertCurEvent{DatasourceId: "shared", Labels: map[string]interface{}{"env": "test", "environment": "prod"}}
	query := &types.RequestAlertCurEventQuery{AgentDatasourceIds: []string{"shared"}, AgentEnvironmentLabelKey: "environment", AgentEnvironments: []string{"test"}}
	if eventWithinAgentScope(event, query) {
		t.Fatal("alias must not override configured security label")
	}
	event.Labels["environment"] = "test"
	if !eventWithinAgentScope(event, query) {
		t.Fatal("explicit matching environment must pass")
	}
}
