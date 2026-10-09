package services

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
	appctx "watchAlert/internal/ctx"
	"watchAlert/internal/models"
	"watchAlert/internal/repo"
	"watchAlert/internal/types"
)

func validProbeConfig(endpoint string) models.ProbingEndpointConfig {
	var config models.ProbingEndpointConfig
	config.Endpoint = endpoint
	config.HTTP.Method = "GET"
	config.Strategy.Timeout = 30
	config.Strategy.EvalInterval = 60
	return config
}

func TestProbingServiceRejectsInvalidConfigurationBeforePersistence(t *testing.T) {
	// No database dependency: malformed configuration must fail before writes.
	svc := probingService{}
	config := validProbeConfig("unused.invalid")
	config.Strategy.Timeout = 0
	if _, err := svc.Create(&types.RequestProbingRuleCreate{RuleName: "test", RuleType: "HTTP", ProbingEndpointConfig: config}); err == nil {
		t.Fatal("create accepted no timeout")
	}
	if _, err := svc.Update(&types.RequestProbingRuleUpdate{RuleId: "p", RuleName: "test", RuleType: "HTTP", ProbingEndpointConfig: config}); err == nil {
		t.Fatal("update accepted no timeout")
	}
	if _, err := svc.OnceContext(context.Background(), &types.RequestProbingOnce{RuleType: "HTTP", ProbingEndpointConfig: config}); err == nil {
		t.Fatal("instant probe accepted no timeout")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if data, err := svc.OnceContext(ctx, nil); data != nil || err != context.Canceled {
		t.Fatal("cancellation did not short-circuit", data, err)
	}
}

func TestProbingOnceCancellationReturnsErrorNotEmptySuccess(t *testing.T) {
	started, cleanup := make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		select {
		case <-r.Context().Done():
		case <-cleanup:
		}
	}))
	defer server.Close()
	defer close(cleanup)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	config := validProbeConfig(server.URL)
	config.Strategy.EvalInterval = 0 // Not required by the one-shot API.
	result := make(chan [2]interface{}, 1)
	svc := probingService{ctx: &appctx.Context{Ctx: context.Background()}}
	go func() {
		data, err := svc.OnceContext(ctx, &types.RequestProbingOnce{RuleType: "HTTP", ProbingEndpointConfig: config})
		result <- [2]interface{}{data, err}
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("request not started")
	}
	cancel()
	select {
	case got := <-result:
		if got[0] != nil || got[1] != context.Canceled {
			t.Fatal("cancellation became successful empty metrics", got)
		}
	case <-time.After(time.Second):
		t.Fatal("cancellation ignored")
	}
}

type probeStateRepo struct {
	repo.InterProbingRepo
	rule   models.ProbeRule
	writes int
}

var errProbeStateFixture = errors.New("state write reached")

func (p *probeStateRepo) Search(tenant, id string) (models.ProbeRule, error) {
	if tenant != "t" || id != "p" {
		return models.ProbeRule{}, errors.New("unexpected scope")
	}
	return p.rule, nil
}
func (p *probeStateRepo) ChangeState(tenant, id string, state *bool) error {
	p.writes++
	return errProbeStateFixture
}

type probeRepoEntry struct {
	repo.InterEntryRepo
	probing *probeStateRepo
}

func (p probeRepoEntry) Probing() repo.InterProbingRepo { return p.probing }

func TestProbingEnableValidatesLegacyConfigButDisableRemainsAvailable(t *testing.T) {
	fixture := &probeStateRepo{rule: models.ProbeRule{RuleId: "p", RuleName: "test", RuleType: "HTTP"}}
	svc := probingService{ctx: &appctx.Context{DB: probeRepoEntry{probing: fixture}}}
	enabled := true
	req := &types.RequestProbeChangeState{TenantId: "t", RuleId: "p", Enabled: &enabled}
	if _, err := svc.ChangeState(req); err == nil || fixture.writes != 0 {
		t.Fatal("invalid legacy config enabled", err)
	}
	enabled = false
	if _, err := svc.ChangeState(req); err != errProbeStateFixture || fixture.writes != 1 {
		t.Fatal("invalid legacy config cannot be disabled", err)
	}
	fixture.rule.ProbingEndpointConfig = validProbeConfig("unused.invalid")
	enabled = true
	if _, err := svc.ChangeState(req); err != errProbeStateFixture || fixture.writes != 2 {
		t.Fatal("valid legacy config rejected", err)
	}
	req.TenantId = ""
	if _, err := svc.ChangeState(req); err == nil || fixture.writes != 2 {
		t.Fatal("empty scope accepted", err)
	}
}
