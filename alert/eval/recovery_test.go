package eval

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"watchAlert/internal/cache"
	appctx "watchAlert/internal/ctx"
	"watchAlert/internal/models"
	"watchAlert/internal/repo"
	"watchAlert/pkg/provider"
)

type evalDSRepo struct {
	repo.InterDatasourceRepo
	sources map[string]models.AlertDataSource
	lookups *atomic.Int32
}

func (r evalDSRepo) GetForTenant(tenant, id string) (models.AlertDataSource, error) {
	if r.lookups != nil {
		r.lookups.Add(1)
	}
	ds, ok := r.sources[id]
	if !ok || ds.TenantId != tenant {
		return models.AlertDataSource{}, fmt.Errorf("not found")
	}
	return ds, nil
}

func (r evalDSRepo) GetForTenantContext(c context.Context, tenant, id string) (models.AlertDataSource, error) {
	if err := c.Err(); err != nil {
		return models.AlertDataSource{}, err
	}
	return r.GetForTenant(tenant, id)
}

type evalRuleRepo struct{ repo.InterRuleRepo }

func (evalRuleRepo) IsEnabled(context.Context, string, string) (bool, error) { return true, nil }

func (evalRuleRepo) GetRuleObject(id string) models.AlertRule {
	enabled := true
	return models.AlertRule{RuleId: id, Enabled: &enabled}
}

type evalDB struct {
	repo.InterEntryRepo
	ds evalDSRepo
}

func (r evalDB) Datasource() repo.InterDatasourceRepo { return r.ds }
func (r evalDB) Rule() repo.InterRuleRepo             { return evalRuleRepo{} }

type memoryAlerts struct {
	cache.AlertCacheInterface
	events        map[string]*models.AlertCurEvent
	reads, writes int
	ruleReadError error
	mu            sync.Mutex
}

func (m *memoryAlerts) GetAllEvents(models.AlertEventCacheKey) (map[string]*models.AlertCurEvent, error) {
	m.reads++
	return m.events, nil
}

func (m *memoryAlerts) GetRuleEvents(_ context.Context, _ models.AlertEventCacheKey, tenant, rule string) (map[string]*models.AlertCurEvent, error) {
	m.reads++
	if m.ruleReadError != nil {
		return nil, m.ruleReadError
	}
	result := make(map[string]*models.AlertCurEvent)
	for fp, event := range m.events {
		if event != nil && event.TenantId == tenant && event.RuleId == rule {
			result[fp] = event
		}
	}
	return result, nil
}
func (m *memoryAlerts) GetEventFromCache(tenant, center, id string) (models.AlertCurEvent, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if e, ok := m.events[id]; ok {
		return *e, nil
	}
	return models.AlertCurEvent{}, redis.Nil
}
func (m *memoryAlerts) PushAlertEvent(e *models.AlertCurEvent) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	v := *e
	m.events[e.Fingerprint] = &v
	m.writes++
	return nil
}
func (m *memoryAlerts) RemoveAlertEvent(tenant, center, id string) { delete(m.events, id); m.writes++ }

type memoryPending struct {
	cache.PendingRecoverCacheInterface
	entries             map[string]int64
	failSet             bool
	failList            bool
	listReads, getReads int
}

func (m *memoryPending) List(string, string) map[string]int64 { return m.entries }
func (m *memoryPending) ListWithError(string, string) (map[string]int64, error) {
	m.listReads++
	if m.failList {
		return nil, fmt.Errorf("read failed")
	}
	result := make(map[string]int64, len(m.entries))
	for k, v := range m.entries {
		result[k] = v
	}
	return result, nil
}
func (m *memoryPending) Get(tenant, rule, id string) (int64, error) {
	m.getReads++
	v, ok := m.entries[id]
	if !ok {
		return 0, redis.Nil
	}
	return v, nil
}
func (m *memoryPending) Set(tenant, rule, id string, v int64) error {
	if m.failSet {
		return fmt.Errorf("write failed")
	}
	m.entries[id] = v
	return nil
}
func (m *memoryPending) Delete(tenant, rule, id string) { delete(m.entries, id) }

type memoryCenters struct {
	cache.FaultCenterCacheInterface
}

func (memoryCenters) GetFaultCenterInfo(models.FaultCenterInfoCacheKey) models.FaultCenter {
	return models.FaultCenter{RecoverWaitTime: 10}
}

type evalCache struct {
	cache.InterEntryCache
	alerts  *memoryAlerts
	pending *memoryPending
	pools   *cache.ProviderPoolStore
}

func (c evalCache) Alert() cache.AlertCacheInterface                   { return c.alerts }
func (c evalCache) PendingRecover() cache.PendingRecoverCacheInterface { return c.pending }
func (c evalCache) FaultCenter() cache.FaultCenterCacheInterface       { return memoryCenters{} }
func (c evalCache) ProviderPools() *cache.ProviderPoolStore            { return c.pools }

func fixture(t *testing.T, mode string) (*AlertRule, models.AlertRule, *memoryAlerts, *memoryPending) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("query") == "1+1" {
			if mode == "unhealthy" {
				w.WriteHeader(503)
			}
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if mode == "query_error" || mode == "unhealthy" {
			w.WriteHeader(503)
			_, _ = w.Write([]byte(`{"status":"error","errorType":"timeout","error":"fixture timeout"}`))
			return
		}
		var samples []interface{}
		if mode == "truncated" {
			for i := 0; i < 1001; i++ {
				samples = append(samples, map[string]interface{}{"metric": map[string]string{"instance": fmt.Sprint(i)}, "value": []interface{}{1, "0"}})
			}
		} else {
			samples = []interface{}{}
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"status": "success", "data": map[string]interface{}{"resultType": "vector", "result": samples}})
	}))
	t.Cleanup(server.Close)
	enabled := true
	ds := models.AlertDataSource{TenantId: "t", ID: "ds", Type: "Prometheus", Enabled: &enabled, HTTP: models.HTTP{URL: server.URL, Timeout: 2}}
	pool := cache.NewClientPoolStore()
	client, err := provider.NewPrometheusClient(ds)
	if err != nil {
		t.Fatal(err)
	}
	if mode == "timeout" {
		v := client.(provider.PrometheusProvider)
		v.Timeout = 0
		client = v
	}
	if mode == "panic" {
		pool.SetClient("ds", 42)
	} else if mode != "missing_client" {
		pool.SetClient("ds", client)
	}
	if mode == "disabled" {
		off := false
		ds.Enabled = &off
	}
	if mode == "foreign" {
		ds.TenantId = "other"
	}
	if mode == "missing_enabled" {
		ds.Enabled = nil
	}
	a := &memoryAlerts{events: map[string]*models.AlertCurEvent{
		"active":     {TenantId: "t", RuleId: "r", FaultCenterId: "fc", DatasourceId: "ds", Fingerprint: "active", Status: models.StateAlerting},
		"pending":    {TenantId: "t", RuleId: "r", FaultCenterId: "fc", DatasourceId: "ds", Fingerprint: "pending", Status: models.StatePendingRecovery},
		"pre":        {TenantId: "t", RuleId: "r", FaultCenterId: "fc", DatasourceId: "ds", Fingerprint: "pre", Status: models.StatePreAlert},
		"other-rule": {TenantId: "t", RuleId: "r-other", FaultCenterId: "fc", DatasourceId: "ds", Fingerprint: "other-rule", Status: models.StateAlerting},
	}}
	p := &memoryPending{entries: map[string]int64{"pending": time.Now().Unix() - 100}}
	db := evalDB{ds: evalDSRepo{sources: map[string]models.AlertDataSource{"ds": ds}}}
	c := &appctx.Context{Ctx: context.Background(), DB: db, Redis: evalCache{alerts: a, pending: p, pools: pool}}
	rule := models.AlertRule{TenantId: "t", RuleId: "r", FaultCenterId: "fc", DatasourceType: "Prometheus", DatasourceIdList: []string{"ds"}, PrometheusConfig: models.PrometheusConfig{PromQL: "up", Rules: []models.Rules{{Expr: "> 1", Severity: "P1"}}}}
	if mode == "partial_failure" {
		rule.DatasourceIdList = append(rule.DatasourceIdList, "missing")
	}
	if mode == "no_datasources" {
		rule.DatasourceIdList = nil
	}
	return &AlertRule{ctx: c}, rule, a, p
}
func runOnce(e *AlertRule, r models.AlertRule) {
	ch := make(chan struct{}, 1)
	ch <- struct{}{}
	e.executeTask(context.Background(), r, ch)
}

func TestIncompleteEvaluationNeverRunsRecovery(t *testing.T) {
	for _, mode := range []string{"query_error", "timeout", "unhealthy", "disabled", "missing_enabled", "foreign", "missing_client", "partial_failure", "truncated", "panic", "no_datasources"} {
		t.Run(mode, func(t *testing.T) {
			e, r, a, p := fixture(t, mode)
			originalPending := p.entries["pending"]
			runOnce(e, r)
			if a.reads != 0 || a.writes != 0 || len(a.events) != 4 || p.entries["pending"] != originalPending {
				t.Fatal("incomplete result mutated alert/recovery state")
			}
		})
	}
}

func TestCompleteEmptyResultRecoversWithoutTouchingOtherRules(t *testing.T) {
	e, r, a, p := fixture(t, "complete")
	runOnce(e, r)
	if a.events["active"].Status != models.StatePendingRecovery {
		t.Fatal("successful empty result must start recovery")
	}
	if _, ok := a.events["pre"]; ok {
		t.Fatal("successful empty result must remove expired prealert")
	}
	if a.events["other-rule"].Status != models.StateAlerting {
		t.Fatal("substring rule ID matched")
	}
	if a.events["pending"].Status != models.StatePendingRecovery || p.entries["pending"] < time.Now().Unix()-2 {
		t.Fatal("restart must reset old recovery window")
	}
	p.entries["active"] = time.Now().Unix() - 100
	p.entries["pending"] = time.Now().Unix() - 100
	runOnce(e, r)
	if a.events["active"].Status != models.StateRecovered || a.events["pending"].Status != models.StateRecovered {
		t.Fatal("confirmed continuous empty result failed to recover")
	}
}

func TestInterruptedRecoveryRestartsWaitingWindow(t *testing.T) {
	e, r, a, p := fixture(t, "complete")
	runOnce(e, r)
	p.entries["active"] = time.Now().Unix() - 100
	incomplete := r
	incomplete.DatasourceIdList = []string{"missing"}
	runOnce(e, incomplete)
	runOnce(e, r)
	if a.events["active"].Status != models.StatePendingRecovery || p.entries["active"] < time.Now().Unix()-2 {
		t.Fatal("failure period counted towards recovery window")
	}
}

func TestUnknownEvaluationStatusIsNotSuccess(t *testing.T) {
	for _, results := range [][]evaluationResult{nil, {{}}, {completeEvaluation([]string{"valid"}), truncatedEvaluation(nil)}} {
		if combineEvaluations(results).Status == "complete" {
			t.Fatal("unknown or partial result accepted")
		}
	}
}

func TestFailedWindowResetDoesNotPermitRecovery(t *testing.T) {
	e, r, a, p := fixture(t, "complete")
	p.failSet = true
	runOnce(e, r)
	if _, ok := e.lastComplete.Load(r.RuleId); ok {
		t.Fatal("failed reset was treated as complete")
	}
	p.failSet = false
	runOnce(e, r)
	if a.events["pending"].Status != models.StatePendingRecovery || p.entries["pending"] < time.Now().Unix()-2 {
		t.Fatal("recovered using an unconfirmed old window")
	}
}

func TestInvalidConditionsCannotRecoverEvenWhenQueryIsEmpty(t *testing.T) {
	for _, conditions := range [][]models.Rules{nil, {{Expr: "invalid", Severity: "P1"}}} {
		e, r, a, _ := fixture(t, "complete")
		r.PrometheusConfig.Rules = conditions
		runOnce(e, r)
		if a.reads != 0 || a.writes != 0 {
			t.Fatal("invalid condition triggered recovery")
		}
	}
}
