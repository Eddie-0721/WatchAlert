package eval

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"watchAlert/internal/cache"
	appctx "watchAlert/internal/ctx"
	"watchAlert/internal/models"
	"watchAlert/pkg/provider"
)

func TestHTTPProviderEvaluationSkipsPreflightButRejectsQueryFailures(t *testing.T) {
	for _, kind := range []string{"Loki", "VictoriaLogs", "Jaeger"} {
		for _, status := range []int{200, 503} {
			t.Run(kind+http.StatusText(status), func(t *testing.T) {
				var requests atomic.Int32
				s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					requests.Add(1)
					wantPath := map[string]string{"Loki": "/loki/api/v1/query_range", "VictoriaLogs": "/select/logsql/query", "Jaeger": "/api/traces"}[kind]
					if r.URL.Path != wantPath {
						t.Error("unexpected preflight", r.URL.Path)
					}
					w.WriteHeader(status)
					_, _ = io.WriteString(w, map[string]string{"Loki": `{"status":"success","data":{"resultType":"streams","result":[]}}`, "VictoriaLogs": "", "Jaeger": `{"data":[]}`}[kind])
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
				}
				if err != nil {
					t.Fatal(err)
				}
				if requests.Load() != 0 {
					t.Fatal("constructor sent redundant request")
				}
				pools := cache.NewClientPoolStore()
				pools.SetClient("ds", client)
				c := &appctx.Context{Ctx: context.Background(), DB: evalDB{ds: evalDSRepo{sources: map[string]models.AlertDataSource{"ds": source}}}, Redis: evalCache{pools: pools}}
				rule := models.AlertRule{TenantId: "t", RuleId: "r", DatasourceType: kind, DatasourceIdList: []string{"ds"}, LogEvalCondition: "> 0"}
				rule.LokiConfig.LogQL = `{app="test"}`
				rule.VictoriaLogsConfig.LogQL = "*"
				rule.JaegerConfig.Service = "test"
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
