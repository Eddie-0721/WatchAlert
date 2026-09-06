package services

import (
	"sort"
	"watchAlert/internal/models"
	"watchAlert/internal/types"
)

func alertInQueue(event types.ResponseAlertCurEvent, queue string) bool {
	if event.LifecycleStatus == models.StateRecovered {
		return false
	}
	switch queue {
	case "all", "":
		return true
	case "suppressed":
		return event.Silenced
	case "observing":
		return event.LifecycleStatus == models.StatePreAlert && !event.Silenced
	case "processing":
		return event.LifecycleStatus != models.StatePreAlert && event.Acknowledged && !event.Silenced
	case "attention":
		return event.LifecycleStatus != models.StatePreAlert && !event.Acknowledged && !event.Silenced
	default:
		return false
	}
}

// Summaries are calculated before pagination and queue selection, within the
// same tenant and search filters as the list. Agent tools do not request them.
func summarizeAlertEvents(events []types.ResponseAlertCurEvent) *types.AlertEventSummary {
	result := &types.AlertEventSummary{Queues: map[string]int{"all": 0, "attention": 0, "processing": 0, "suppressed": 0, "observing": 0}, Environments: []string{}, Services: []string{}}
	environments, services := map[string]bool{}, map[string]bool{}
	for _, event := range events {
		for _, queue := range []string{"all", "attention", "processing", "suppressed", "observing"} {
			if alertInQueue(event, queue) {
				result.Queues[queue]++
			}
		}
		if event.Scope.Environment != "" {
			environments[event.Scope.Environment] = true
		}
		if event.Scope.Service != "" {
			services[event.Scope.Service] = true
		}
	}
	for value := range environments {
		result.Environments = append(result.Environments, value)
	}
	for value := range services {
		result.Services = append(result.Services, value)
	}
	sort.Strings(result.Environments)
	sort.Strings(result.Services)
	return result
}

func eventWithinAgentScope(event models.AlertCurEvent, query *types.RequestAlertCurEventQuery) bool {
	if len(query.AgentDatasourceIds) > 0 && !containsTool(query.AgentDatasourceIds, event.DatasourceId) {
		return false
	}
	if len(query.AgentEnvironments) > 0 {
		value, ok := event.Labels[query.AgentEnvironmentLabelKey].(string)
		return ok && containsTool(query.AgentEnvironments, value)
	}
	return true
}
