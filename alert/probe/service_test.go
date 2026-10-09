package probe

import (
	"context"
	"io"
	"math"
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

func validProbeRule(endpoint string) models.ProbeRule {
	rule := models.ProbeRule{RuleId: "probe-rule", RuleName: "probe", TenantId: "t", RuleType: "HTTP"}
	rule.ProbingEndpointConfig.Endpoint = endpoint
	rule.ProbingEndpointConfig.Strategy.EvalInterval = 60
	rule.ProbingEndpointConfig.Strategy.Timeout = 30
	rule.ProbingEndpointConfig.HTTP.Method = "GET"
	return rule
}

func TestProbeStopCancelsCurrentTargetAndSkipsRemainingAndWrite(t *testing.T) {
	started, stopped, cleanup := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var tailCalls atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/first":
			w.WriteHeader(204)
		case "/slow":
			close(started)
			select {
			case <-r.Context().Done():
				close(stopped)
			case <-cleanup:
			}
		default:
			tailCalls.Add(1)
		}
	}))
	defer s.Close()
	defer close(cleanup)
	rule := validProbeRule(s.URL + "/first," + s.URL + "/slow," + s.URL + "/tail")
	rule.DatasourceId = "must-not-acquire-after-cancel"
	// Nil Redis is intentional: canceled/partial metrics must never reach it.
	service := NewProbeService(&appctx.Context{Ctx: context.Background()})
	requestCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	service.watchCtxMap[rule.RuleId] = cancel
	done := make(chan struct{})
	go func() { defer close(done); service.executeProbing(requestCtx, rule) }()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("probe not started")
	}
	if err := service.Stop(rule.RuleId); err != nil {
		t.Fatal(err)
	}
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("Stop did not cancel network request")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("canceled cycle did not finish")
	}
	if service.GetActiveRules() != 0 || tailCalls.Load() != 0 {
		t.Fatal("stopped task continued", tailCalls.Load())
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	service.executeProbing(ctx, rule)
	if metrics, err := service.executeProbeWithMetrics(ctx, rule); err != context.Canceled || metrics != nil {
		t.Fatal(metrics, err)
	}
}

type probeCacheFixture struct {
	cache.InterEntryCache
	pool *cache.ProviderPoolStore
}

func (c probeCacheFixture) ProviderPools() *cache.ProviderPoolStore { return c.pool }

func TestProbeCancellationReachesRemoteWrite(t *testing.T) {
	started, stopped, cleanup := make(chan struct{}), make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/write" {
			w.WriteHeader(204)
			return
		}
		// Read the body so the server can observe a peer disconnect afterwards.
		_, _ = io.Copy(io.Discard, r.Body)
		close(started)
		select {
		case <-r.Context().Done():
			close(stopped)
		case <-cleanup:
		}
	}))
	defer server.Close()
	defer close(cleanup)
	client, err := provider.NewPrometheusClient(models.AlertDataSource{HTTP: models.HTTP{URL: server.URL, Timeout: 30}, Write: models.Write{URL: server.URL + "/write"}})
	if err != nil {
		t.Fatal(err)
	}
	pool := cache.NewClientPoolStore()
	pool.SetClient("ds", client)
	defer pool.RemoveClient("ds")
	service := NewProbeService(&appctx.Context{Ctx: context.Background(), Redis: probeCacheFixture{pool: pool}})
	requestCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rule := validProbeRule(server.URL)
	rule.DatasourceId = "ds"
	done := make(chan struct{})
	go func() { defer close(done); service.executeProbing(requestCtx, rule) }()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("remote write not started")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("remote write ignored cancellation")
	}
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("remote write target did not see cancellation")
	}
}

func TestProbeValidationRejectsInvalidTimingBeforeStarting(t *testing.T) {
	changes := map[string]func(*models.ProbeRule){
		"zero interval":      func(r *models.ProbeRule) { r.ProbingEndpointConfig.Strategy.EvalInterval = 0 },
		"negative interval":  func(r *models.ProbeRule) { r.ProbingEndpointConfig.Strategy.EvalInterval = -1 },
		"overflow interval":  func(r *models.ProbeRule) { r.ProbingEndpointConfig.Strategy.EvalInterval = math.MaxInt64 },
		"zero timeout":       func(r *models.ProbeRule) { r.ProbingEndpointConfig.Strategy.Timeout = 0 },
		"negative timeout":   func(r *models.ProbeRule) { r.ProbingEndpointConfig.Strategy.Timeout = -1 },
		"empty segment":      func(r *models.ProbeRule) { r.ProbingEndpointConfig.Endpoint += "," },
		"unknown type":       func(r *models.ProbeRule) { r.RuleType = "invalid" },
		"unknown method":     func(r *models.ProbeRule) { r.ProbingEndpointConfig.HTTP.Method = "PATCH" },
		"icmp zero interval": func(r *models.ProbeRule) { r.RuleType = "ICMP"; r.ProbingEndpointConfig.ICMP.Count = 1 },
		"icmp zero count":    func(r *models.ProbeRule) { r.RuleType = "ICMP"; r.ProbingEndpointConfig.ICMP.Interval = 1 },
	}
	for name, change := range changes {
		t.Run(name, func(t *testing.T) {
			rule := validProbeRule("unused.invalid")
			change(&rule)
			service := NewProbeService(&appctx.Context{Ctx: context.Background()})
			if err := service.Add(rule); err == nil {
				t.Fatal("invalid rule accepted")
			}
			if service.GetActiveRules() != 0 {
				t.Fatal("invalid task installed")
			}
		})
	}
	rule := validProbeRule("unused.invalid")
	rule.ProbingEndpointConfig.Strategy.EvalInterval = math.MaxInt64 / int64(time.Second)
	if err := ValidateProbeRule(rule); err != nil {
		t.Fatal("valid duration upper bound rejected", err)
	}
	rule.ProbingEndpointConfig.Strategy.EvalInterval = 0
	if err := ValidateProbeConfig(rule.RuleType, rule.ProbingEndpointConfig, false); err != nil {
		t.Fatal("instant probe requires unused interval", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	service := NewProbeService(&appctx.Context{Ctx: ctx})
	rule = validProbeRule("unused.invalid")
	if err := service.Add(rule); err != context.Canceled {
		t.Fatal("parent cancellation ignored", err)
	}
}
