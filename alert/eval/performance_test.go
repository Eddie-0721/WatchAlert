package eval

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"watchAlert/config"
	"watchAlert/internal/cache"
	appctx "watchAlert/internal/ctx"
	"watchAlert/internal/models"
	"watchAlert/pkg/provider"
)

func runtimeFixture(t *testing.T, handler http.HandlerFunc) (*AlertRule, models.AlertRule) {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	enabled := true
	source := models.AlertDataSource{ID: "ds", TenantId: "t", Type: "Prometheus", Enabled: &enabled, HTTP: models.HTTP{URL: server.URL, Timeout: 5}}
	client, err := provider.NewPrometheusClient(source)
	if err != nil {
		t.Fatal(err)
	}
	pool := cache.NewClientPoolStore()
	pool.SetClient("ds", client)
	c := &appctx.Context{Ctx: context.Background(), DB: evalDB{ds: evalDSRepo{sources: map[string]models.AlertDataSource{"ds": source}}}, Redis: evalCache{pools: pool}}
	rule := models.AlertRule{TenantId: "t", RuleId: "r", DatasourceType: "Prometheus", DatasourceIdList: []string{"ds"}, PrometheusConfig: models.PrometheusConfig{PromQL: "up", Rules: []models.Rules{{Expr: "> 1", Severity: "P1"}}}}
	return &AlertRule{ctx: c, querySlots: make(chan struct{}, 3)}, rule
}

func writeEmptyVector(w http.ResponseWriter) {
	_, _ = io.WriteString(w, `{"status":"success","data":{"resultType":"vector","result":[]}}`)
}

func TestEvaluationMakesOneRealQueryWithoutHealthPreflight(t *testing.T) {
	var calls atomic.Int32
	e, r := runtimeFixture(t, func(w http.ResponseWriter, req *http.Request) {
		calls.Add(1)
		_ = req.ParseForm()
		if req.Form.Get("query") != "up" {
			t.Error("unexpected health probe", req.Form)
		}
		writeEmptyVector(w)
	})
	if result := e.processDatasources(context.Background(), r); result.Status != "complete" {
		t.Fatal(result)
	}
	if calls.Load() != 1 {
		t.Fatal("expected one query", calls.Load())
	}
}

func TestDatasourceQueriesAreGloballyBoundedAcrossRules(t *testing.T) {
	var active, peak, calls atomic.Int32
	e, r := runtimeFixture(t, func(w http.ResponseWriter, req *http.Request) {
		_, _ = io.Copy(io.Discard, req.Body)
		now := active.Add(1)
		defer active.Add(-1)
		for old := peak.Load(); now > old && !peak.CompareAndSwap(old, now); old = peak.Load() {
		}
		calls.Add(1)
		time.Sleep(5 * time.Millisecond)
		writeEmptyVector(w)
	})
	r.DatasourceIdList = []string{"ds", "ds", "ds", "ds", "ds", "ds", "ds", "ds"}
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if got := e.processDatasources(context.Background(), r); got.Status != "complete" {
				t.Error(got)
			}
		}()
	}
	wg.Wait()
	if peak.Load() > 3 || peak.Load() < 2 || calls.Load() != 96 {
		t.Fatal("invalid concurrency or lost work", peak.Load(), calls.Load())
	}
	if len(e.querySlots) != 0 {
		t.Fatal("query slot leaked")
	}
}

func TestPerRuleFanoutIsBounded(t *testing.T) {
	old := config.Application.Evaluation
	config.Application.Evaluation.MaxDatasourcesPerRule = 2
	defer func() { config.Application.Evaluation = old }()
	var active, peak atomic.Int32
	e, r := runtimeFixture(t, func(w http.ResponseWriter, req *http.Request) {
		_, _ = io.Copy(io.Discard, req.Body)
		now := active.Add(1)
		defer active.Add(-1)
		for old := peak.Load(); now > old && !peak.CompareAndSwap(old, now); old = peak.Load() {
		}
		time.Sleep(5 * time.Millisecond)
		writeEmptyVector(w)
	})
	e.querySlots = make(chan struct{}, 20)
	r.DatasourceIdList = []string{"ds", "ds", "ds", "ds", "ds", "ds"}
	e.processDatasources(context.Background(), r)
	if peak.Load() > 2 {
		t.Fatal("per-rule fanout exceeded", peak.Load())
	}
}

func TestCancellationStopsInflightAndQueuedEvaluations(t *testing.T) {
	started := make(chan struct{}, 1)
	stopped := make(chan struct{}, 1)
	cleanup := make(chan struct{})
	e, r := runtimeFixture(t, func(w http.ResponseWriter, req *http.Request) {
		_, _ = io.Copy(io.Discard, req.Body)
		started <- struct{}{}
		select {
		case <-req.Context().Done():
			stopped <- struct{}{}
		case <-cleanup:
		}
	})
	defer close(cleanup)
	e.querySlots = make(chan struct{}, 1)
	r.DatasourceIdList = []string{"ds", "ds", "ds", "ds", "ds", "ds"}
	c, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan evaluationResult, 1)
	go func() { done <- e.processDatasources(c, r) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("query did not start")
	}
	cancel()
	select {
	case result := <-done:
		if result.Status == "complete" {
			t.Fatal("cancellation treated as complete")
		}
	case <-time.After(time.Second):
		t.Fatal("evaluation did not stop")
	}
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("upstream query survived cancellation")
	}
	if len(e.querySlots) != 0 {
		t.Fatal("cancelled run retained query slot")
	}
}

func TestRecoverySnapshotFailureCannotChangeEvents(t *testing.T) {
	e, r, a, pending := fixture(t, "complete")
	pending.failList = true
	runOnce(e, r)
	if a.writes != 0 || len(a.events) != 4 {
		t.Fatal("recovery ran with unreadable timestamps")
	}
}

func TestRecoveryTimestampsAreReadOnceNotPerEvent(t *testing.T) {
	e, r, alerts, pending := fixture(t, "complete")
	for i := 0; i < 1000; i++ {
		id := fmt.Sprint(i)
		alerts.events[id] = &models.AlertCurEvent{TenantId: "t", RuleId: "r", FaultCenterId: "fc", Fingerprint: id, Status: models.StatePendingRecovery}
		pending.entries[id] = time.Now().Unix() - 100
	}
	runOnce(e, r)
	if pending.listReads != 1 || pending.getReads != 0 {
		t.Fatal("redundant recovery reads", pending.listReads, pending.getReads)
	}
}

func TestRecordingRuleUsesOneScopedLookupAndOneQuery(t *testing.T) {
	var queries atomic.Int32
	e, _ := runtimeFixture(t, func(w http.ResponseWriter, req *http.Request) {
		queries.Add(1)
		_ = req.ParseForm()
		if req.Form.Get("query") != "up" {
			t.Error("recording preflight still present")
		}
		writeEmptyVector(w)
	})
	var lookups atomic.Int32
	db := e.ctx.DB.(evalDB)
	db.ds.lookups = &lookups
	e.ctx.DB = db
	recording := &RecordingRule{ctx: e.ctx}
	rule := models.RecordingRule{TenantId: "t", RuleId: "rr", DatasourceId: "ds", DatasourceType: "Prometheus", PromQL: "up"}
	recording.processSingleDatasource(context.Background(), rule)
	if lookups.Load() != 1 || queries.Load() != 1 {
		t.Fatal("recording redundant work", lookups.Load(), queries.Load())
	}
	rule.TenantId = "foreign"
	recording.processSingleDatasource(context.Background(), rule)
	if queries.Load() != 1 {
		t.Fatal("recording rule queried a foreign datasource")
	}
}

func TestNonMatchingSamplesReadExistingEventOnlyOnce(t *testing.T) {
	var samples []interface{}
	for i := 0; i < 1000; i++ {
		samples = append(samples, map[string]interface{}{"metric": map[string]string{"instance": fmt.Sprint(i)}, "value": []interface{}{1, "0"}})
	}
	e, r := runtimeFixture(t, func(w http.ResponseWriter, req *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"status": "success", "data": map[string]interface{}{"resultType": "vector", "result": samples}})
	})
	alerts := &countingEventReads{memoryAlerts: &memoryAlerts{events: map[string]*models.AlertCurEvent{}}}
	e.ctx.Redis = countEvalCache{evalCache: e.ctx.Redis.(evalCache), alerts: alerts}
	r.PrometheusConfig.Rules = []models.Rules{{Expr: "> 10", Severity: "P0"}, {Expr: "> 5", Severity: "P1"}, {Expr: "> 1", Severity: "P2"}}
	if got := e.processDatasources(context.Background(), r); got.Status != "complete" {
		t.Fatal(got)
	}
	if alerts.reads.Load() != 3000 || alerts.writes != 0 {
		t.Fatal("unexpected cache reads or writes", alerts.reads.Load(), alerts.writes)
	}
}

type countingEventReads struct {
	*memoryAlerts
	reads atomic.Int32
}

func (c *countingEventReads) GetEventFromCache(tenant, center, id string) (models.AlertCurEvent, error) {
	c.reads.Add(1)
	return c.memoryAlerts.GetEventFromCache(tenant, center, id)
}

type countEvalCache struct {
	evalCache
	alerts *countingEventReads
}

func (c countEvalCache) Alert() cache.AlertCacheInterface { return c.alerts }
