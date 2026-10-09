package eval

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
	"watchAlert/internal/cache"
	appctx "watchAlert/internal/ctx"
	"watchAlert/internal/models"
	"watchAlert/pkg/provider"
)

func TestHTTPProviderEvaluationSkipsPreflightButRejectsQueryFailures(t *testing.T) {
	for _, kind := range []string{"Loki", "VictoriaLogs", "Jaeger", "ElasticSearch"} {
		for _, status := range []int{200, 503} {
			t.Run(kind+http.StatusText(status), func(t *testing.T) {
				var requests atomic.Int32
				s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					requests.Add(1)
					wantPath := map[string]string{"Loki": "/loki/api/v1/query_range", "VictoriaLogs": "/select/logsql/query", "Jaeger": "/api/traces", "ElasticSearch": "/logs/_search"}[kind]
					if r.URL.Path != wantPath {
						t.Error("unexpected preflight", r.URL.Path)
					}
					w.WriteHeader(status)
					_, _ = io.Copy(io.Discard, r.Body)
					_, _ = io.WriteString(w, map[string]string{"Loki": `{"status":"success","data":{"resultType":"streams","result":[]}}`, "VictoriaLogs": "", "Jaeger": `{"data":[]}`, "ElasticSearch": `{"hits":{"hits":[]}}`}[kind])
				}))
				defer s.Close()
				enabled := true
				source := models.AlertDataSource{ID: "ds", TenantId: "t", Type: kind, Enabled: &enabled, HTTP: models.HTTP{URL: s.URL, Timeout: 5}}
				var client interface{}
				var err error
				switch kind {
				case "Loki":
					client, err = provider.NewLokiClient(source)
				case "VictoriaLogs":
					client, err = provider.NewVictoriaLogsClient(context.Background(), source)
				case "Jaeger":
					client, err = provider.NewJaegerClient(source)
				case "ElasticSearch":
					client, err = provider.NewElasticSearchClient(context.Background(), source)
				}
				if err != nil {
					t.Fatal(err)
				}
				if requests.Load() != 0 {
					t.Fatal("constructor sent redundant request")
				}
				pools := cache.NewClientPoolStore()
				pools.SetClient("ds", client)
				defer pools.RemoveClient("ds")
				c := &appctx.Context{Ctx: context.Background(), DB: evalDB{ds: evalDSRepo{sources: map[string]models.AlertDataSource{"ds": source}}}, Redis: evalCache{pools: pools}}
				rule := models.AlertRule{TenantId: "t", RuleId: "r", DatasourceType: kind, DatasourceIdList: []string{"ds"}, LogEvalCondition: "> 0"}
				rule.LokiConfig.LogQL = `{app="test"}`
				rule.VictoriaLogsConfig.LogQL = "*"
				rule.JaegerConfig.Service = "test"
				rule.ElasticSearchConfig = models.ElasticSearchConfig{Index: "logs", EsQueryType: models.EsQueryTypeRawJson, RawJson: `{"match_all":{}}`}
				e := &AlertRule{ctx: c, querySlots: make(chan struct{}, 1)}
				result := e.processDatasources(context.Background(), rule)
				if (result.Status == "complete") != (status == 200) {
					t.Fatal("query failure became successful empty evaluation", result)
				}
				if requests.Load() != 1 {
					t.Fatal("expected exactly one query", requests.Load())
				}
			})
		}
	}
}

func TestElasticFieldEvaluationIncludesConfiguredRange(t *testing.T) {
	var rangeSeen atomic.Bool
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]interface{}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		// The SDK may encode the single must clause as an object or an array.
		var inspect func(interface{})
		inspect = func(v interface{}) {
			switch value := v.(type) {
			case map[string]interface{}:
				if ts, ok := value["@timestamp"].(map[string]interface{}); ok {
					start, _ := time.Parse(time.RFC3339Nano, ts["from"].(string))
					end, _ := time.Parse(time.RFC3339Nano, ts["to"].(string))
					if end.Sub(start) != 5*time.Minute {
						t.Error("scope ignored", start, end)
					}
					rangeSeen.Store(true)
				}
				for _, child := range value {
					inspect(child)
				}
			case []interface{}:
				for _, child := range value {
					inspect(child)
				}
			}
		}
		inspect(body)
		_, _ = io.WriteString(w, `{"hits":{"hits":[]}}`)
	}))
	defer s.Close()
	source := models.AlertDataSource{HTTP: models.HTTP{URL: s.URL, Timeout: 2}}
	cli, err := provider.NewElasticSearchClient(context.Background(), source)
	if err != nil {
		t.Fatal(err)
	}
	pools := cache.NewClientPoolStore()
	pools.SetClient("ds", cli)
	defer pools.RemoveClient("ds")
	c := &appctx.Context{Ctx: context.Background(), Redis: evalCache{pools: pools}}
	rule := models.AlertRule{RuleId: "r", LogEvalCondition: "> 0", ElasticSearchConfig: models.ElasticSearchConfig{Index: "logs", EsQueryType: models.EsQueryTypeField, Scope: 5}}
	if result := logs(context.Background(), c, "ds", "ElasticSearch", rule); result.Status != "complete" || !rangeSeen.Load() {
		t.Fatal(result, rangeSeen.Load())
	}
}
