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
	result := newEventSummaryAccumulator()
	for i := range events {
		result.add(&events[i])
	}
	return result.finish()
}

type eventSummaryAccumulator struct {
	result                 *types.AlertEventSummary
	environments, services map[string]bool
}

func newEventSummaryAccumulator() *eventSummaryAccumulator {
	return &eventSummaryAccumulator{result: &types.AlertEventSummary{Queues: map[string]int{"all": 0, "attention": 0, "processing": 0, "suppressed": 0, "observing": 0}, Environments: []string{}, Services: []string{}}, environments: map[string]bool{}, services: map[string]bool{}}
}

func (s *eventSummaryAccumulator) add(event *types.ResponseAlertCurEvent) {
	if event.LifecycleStatus != models.StateRecovered {
		s.result.Queues["all"]++
		queue := "attention"
		if event.Silenced {
			queue = "suppressed"
		} else if event.LifecycleStatus == models.StatePreAlert {
			queue = "observing"
		} else if event.Acknowledged {
			queue = "processing"
		}
		s.result.Queues[queue]++
	}
	if event.Scope.Environment != "" {
		s.environments[event.Scope.Environment] = true
	}
	if event.Scope.Service != "" {
		s.services[event.Scope.Service] = true
	}
}

func (s *eventSummaryAccumulator) finish() *types.AlertEventSummary {
	if s == nil {
		return nil
	}
	for value := range s.environments {
		s.result.Environments = append(s.result.Environments, value)
	}
	for value := range s.services {
		s.result.Services = append(s.result.Services, value)
	}
	sort.Strings(s.result.Environments)
	sort.Strings(s.result.Services)
	return s.result
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
