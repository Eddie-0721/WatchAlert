package services

import (
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
	"watchAlert/alert/mute"
	"watchAlert/internal/models"
	"watchAlert/internal/types"
)

// A deliberately materialized oracle keeps the former filter -> full sort ->
// full summary -> queue -> page flow, independently of heap/accumulator code.
func referenceCurrentList(service eventService, r *types.RequestAlertCurEventQuery) types.ResponseAlertCurEventList {
	c := service.ctx.Redis.(benchmarkEventCache)
	match, _ := mute.CompileSnapshotChecked(c.rules, time.Now().Unix())
	rows := []types.ResponseAlertCurEvent{}
	for _, group := range c.alerts.events {
		for _, event := range group {
			if event == nil || event.TenantId != r.TenantId || (r.FaultCenterId != "" && event.FaultCenterId != r.FaultCenterId) || (r.Fingerprint != "" && event.Fingerprint != r.Fingerprint) || (r.RuleId != "" && event.RuleId != r.RuleId) || !eventWithinAgentScope(*event, r) || (r.DatasourceType != "" && event.DatasourceType != r.DatasourceType) || (r.Severity != "" && event.Severity != r.Severity) || !matchQuery(*event, r.Query) {
				continue
			}
			view := buildCurrentEventResponse(*event, match(event.Labels))
			view.Scope = buildAlertScope(event.Labels)
			if matchCurrentEvent(view, r) {
				rows = append(rows, view)
			}
		}
	}
	sort.Slice(rows, func(i, j int) bool {
		a, b := rows[i], rows[j]
		da, db := a.LastEvalTime-a.FirstTriggerTime, b.LastEvalTime-b.FirstTriggerTime
		if r.SortOrder == models.SortOrderASC && da != db {
			return da < db
		}
		if r.SortOrder == models.SortOrderDesc && da != db {
			return da > db
		}
		if r.SortOrder != models.SortOrderASC && r.SortOrder != models.SortOrderDesc && a.FirstTriggerTime != b.FirstTriggerTime {
			return a.FirstTriggerTime > b.FirstTriggerTime
		}
		return a.Fingerprint < b.Fingerprint
	})
	var summary *types.AlertEventSummary
	if r.IncludeSummary {
		summary = &types.AlertEventSummary{Queues: map[string]int{}, Environments: []string{}, Services: []string{}}
		for _, q := range []string{"all", "attention", "processing", "suppressed", "observing"} {
			summary.Queues[q] = 0
			for _, row := range rows {
				if alertInQueue(row, q) {
					summary.Queues[q]++
				}
			}
		}
		env, svc := map[string]bool{}, map[string]bool{}
		for _, row := range rows {
			if row.Scope.Environment != "" {
				env[row.Scope.Environment] = true
			}
			if row.Scope.Service != "" {
				svc[row.Scope.Service] = true
			}
		}
		for name := range env {
			summary.Environments = append(summary.Environments, name)
		}
		for name := range svc {
			summary.Services = append(summary.Services, name)
		}
		sort.Strings(summary.Environments)
		sort.Strings(summary.Services)
	}
	queued := []types.ResponseAlertCurEvent{}
	for _, row := range rows {
		if r.Queue == "" || alertInQueue(row, r.Queue) {
			queued = append(queued, row)
		}
	}
	return types.ResponseAlertCurEventList{List: pageSlice(queued, int(r.Index), int(r.Size)), Summary: summary, Page: models.Page{Total: int64(len(queued)), Index: r.Index, Size: r.Size}}
}

func TestStreamingEventListMatchesMaterializedReference(t *testing.T) {
	service := eventBenchmarkFixture(237, 100)
	trueValue, falseValue := true, false
	filters := []types.RequestAlertCurEventQuery{
		{}, {FaultCenterId: "fc"}, {FaultCenterId: "missing"}, {RuleId: "rule-3"}, {Fingerprint: "fp-00000037"},
		{Query: "value-42"}, {Query: "Service"}, {Environment: " PROD "}, {Service: "service-127"}, {Cluster: "cn-prod"},
		{Namespace: "payments", Instance: "10.0.0.37"}, {Acknowledged: &trueValue}, {Acknowledged: &falseValue},
		{Silenced: &trueValue}, {Silenced: &falseValue}, {Status: "muting"}, {Status: "processing"}, {LifecycleStatus: string(models.StatePreAlert)},
		{AgentEnvironmentLabelKey: "env", AgentEnvironments: []string{"prod"}}, {AgentEnvironmentLabelKey: "env", AgentEnvironments: []string{"dev"}},
		{AgentDatasourceIds: []string{"forbidden"}}, {Severity: "absent"}, {DatasourceType: "absent"},
	}
	random := rand.New(rand.NewSource(41))
	for i := 0; i < 250; i++ {
		query := filters[i%len(filters)]
		query.TenantId = "t"
		query.IncludeSummary = i%3 != 0
		query.IncludeRecovered = i%2 == 0
		query.SortOrder = []string{"", models.SortOrderASC, models.SortOrderDesc}[random.Intn(3)]
		query.Queue = []string{"", "all", "attention", "processing", "suppressed", "observing", "unknown"}[random.Intn(7)]
		query.Page = models.Page{Index: int64(random.Intn(10) - 1), Size: []int64{0, 1, 7, 30, 500}[random.Intn(5)]}
		want := referenceCurrentList(service, &query)
		value, err := service.ListCurrentEvent(&query)
		if err != nil {
			t.Fatal(err)
		}
		got := value.(types.ResponseAlertCurEventList)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("case %d query=%+v\ngot page=%+v summary=%+v\nwant page=%+v summary=%+v", i, query, got.Page, got.Summary, want.Page, want.Summary)
		}
	}
}

func TestEventSelectorBoundAndDeterministicTies(t *testing.T) {
	for _, order := range []string{"", models.SortOrderASC, models.SortOrderDesc} {
		for _, page := range []models.Page{{Index: 1, Size: 30}, {Index: 3, Size: 7}, {Index: 500, Size: 30}} {
			selector, offset, err := newCurrentEventPage(page, order)
			if err != nil {
				t.Fatal(err)
			}
			all := make([]types.ResponseAlertCurEvent, 1000)
			for i := range all {
				all[i] = types.ResponseAlertCurEvent{AlertCurEvent: models.AlertCurEvent{Fingerprint: fmt.Sprint(i % 5), FaultCenterId: fmt.Sprintf("fc%04d", i), FirstTriggerTime: int64(i % 3), LastEvalTime: int64(i % 7)}}
			}
			var first []types.ResponseAlertCurEvent
			for repeat := 0; repeat < 5; repeat++ {
				selector, offset, err = newCurrentEventPage(page, order)
				if err != nil {
					t.Fatal(err)
				}
				rand.New(rand.NewSource(int64(repeat))).Shuffle(len(all), func(i, j int) { all[i], all[j] = all[j], all[i] })
				for _, row := range all {
					selector.offer(row)
					if len(selector.items) > int(page.Index*page.Size) {
						t.Fatal("selector grew beyond page boundary")
					}
				}
				got := selector.page(offset)
				if repeat == 0 {
					first = got
				} else if !reflect.DeepEqual(first, got) {
					t.Fatal("page order depends on map iteration")
				}
			}
		}
	}
}

func TestEventPageRejectsOverflowAndNilRequest(t *testing.T) {
	service := eventBenchmarkFixture(1, 0)
	for _, page := range []models.Page{{Index: math.MaxInt64, Size: 2}, {Index: 2, Size: math.MaxInt64}} {
		if _, err := service.ListCurrentEvent(&types.RequestAlertCurEventQuery{TenantId: "t", Page: page}); err == nil {
			t.Fatal("overflow must not panic/allocate")
		}
	}
	if _, err := service.ListCurrentEvent((*types.RequestAlertCurEventQuery)(nil)); err == nil {
		t.Fatal("nil query accepted")
	}
}

func TestLabelFastPathPreservesFormatting(t *testing.T) {
	for _, value := range []interface{}{" prod ", 123, true, nil, []string{"x", "y"}} {
		if got, want := labelValue(map[string]interface{}{"ENV": value}, "env"), fmt.Sprint(value); got != strings.TrimSpace(want) {
			t.Fatalf("got %q want %q", got, want)
		}
	}
}

func TestScopeDirectLookupPreservesLegacyAliasesAndUnicode(t *testing.T) {
	for _, labels := range []map[string]interface{}{
		nil, {}, {"environment": "prod", "env": "test", "service": "payments", "app": "other", "resource": "cpu", "instance": "node", "owner": 123},
		{"ENV": "prod", "SERVICE": "payments", "namespace": true, "unrelated": "x"},
		{"environment": nil, "env": "test", "app": []string{"x", "y"}},
		{"ſtage": "prod", "ſervice": "payments", "Kubernetes_cluster": "c", "环境": "test"},
	} {
		want := types.AlertScope{
			Environment: labelValue(labels, "environment", "env", "stage", "deployment_environment"),
			Service:     labelValue(labels, "service", "app", "application", "job"),
			Cluster:     labelValue(labels, "cluster", "cluster_name", "kubernetes_cluster"),
			Namespace:   labelValue(labels, "namespace", "kubernetes_namespace", "k8s_namespace"),
			Resource:    labelValue(labels, "resource_name", "resource", "pod", "node", "host", "instance"),
			Instance:    labelValue(labels, "instance", "pod", "node", "host", "endpoint"),
			Owner:       labelValue(labels, "owner", "team", "service_owner"),
		}
		if got := buildAlertScope(labels); got != want {
			t.Fatalf("labels=%v got=%+v want=%+v", labels, got, want)
		}
	}
}
