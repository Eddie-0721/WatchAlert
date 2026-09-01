package services

import (
	"testing"
	"watchAlert/internal/models"
	"watchAlert/internal/types"
)

func TestBuildCurrentEventResponseKeepsLifecycleDimensions(t *testing.T) {
	event := models.AlertCurEvent{
		Status:       models.StateAlerting,
		ConfirmState: models.ConfirmState{IsOk: true},
	}

	view := buildCurrentEventResponse(event, true)

	if event.Status != models.StateAlerting {
		t.Fatalf("cached event status was mutated: %s", event.Status)
	}
	if view.LifecycleStatus != models.StateAlerting {
		t.Fatalf("unexpected lifecycle status: %s", view.LifecycleStatus)
	}
	if !view.Acknowledged || !view.Silenced {
		t.Fatal("expected acknowledged and silenced dimensions to be true")
	}
	if view.Status != models.AlertStatus("muting") {
		t.Fatalf("legacy status should remain compatible, got %s", view.Status)
	}
}

func TestMatchCurrentEventFiltersIndependentDimensions(t *testing.T) {
	view := types.ResponseAlertCurEvent{
		AlertCurEvent:   models.AlertCurEvent{Status: models.AlertStatus("processing")},
		LifecycleStatus: models.StateAlerting,
		Acknowledged:    true,
		Silenced:        false,
	}
	acknowledged := true
	notSilenced := false
	query := &types.RequestAlertCurEventQuery{
		LifecycleStatus: string(models.StateAlerting),
		Acknowledged:    &acknowledged,
		Silenced:        &notSilenced,
	}

	if !matchCurrentEvent(view, query) {
		t.Fatal("expected independent lifecycle filters to match")
	}
}

func TestMatchCurrentEventExcludesRecoveredByDefault(t *testing.T) {
	view := types.ResponseAlertCurEvent{
		AlertCurEvent:   models.AlertCurEvent{Status: models.StateRecovered},
		LifecycleStatus: models.StateRecovered,
	}

	if matchCurrentEvent(view, &types.RequestAlertCurEventQuery{}) {
		t.Fatal("recovered events must not appear in the active event query")
	}
	if !matchCurrentEvent(view, &types.RequestAlertCurEventQuery{IncludeRecovered: true}) {
		t.Fatal("includeRecovered should allow transitional recovered events")
	}
}

func TestPageSliceUsesDefaultPageSize(t *testing.T) {
	data := make([]types.ResponseAlertCurEvent, 12)
	if got := len(pageSlice(data, 1, 0)); got != 10 {
		t.Fatalf("expected default page size 10, got %d", got)
	}
}

func TestBuildAlertScopeUsesCommonLabelAliases(t *testing.T) {
	scope := buildAlertScope(map[string]interface{}{
		"env":       "prod",
		"app":       "payments",
		"cluster":   "cn-prod-01",
		"namespace": "payments",
		"pod":       "payments-7f9c",
		"team":      "commerce",
	})

	if scope.Environment != "prod" || scope.Service != "payments" || scope.Cluster != "cn-prod-01" {
		t.Fatalf("unexpected core scope: %#v", scope)
	}
	if scope.Namespace != "payments" || scope.Resource != "payments-7f9c" || scope.Owner != "commerce" {
		t.Fatalf("unexpected derived scope: %#v", scope)
	}
}
