package services

import (
	"testing"
	"watchAlert/internal/models"
	"watchAlert/internal/types"
	"watchAlert/pkg/agenttoken"
)

func TestPromQLScopeEverySelector(t *testing.T) {
	claims := agenttoken.Claims{EnvironmentLabelKey: "env", Environments: []string{"prod"}}
	tests := []struct {
		query   string
		allowed bool
	}{
		{`sum(rate(http_requests_total{env="prod"}[5m]))`, true},
		{`up{env="prod"} / on(instance) node_cpu_seconds_total{env="dev"}`, false},
		{`up{env="prod"} or up`, false}, {`up{env=~"prod|dev"}`, false},
		{`up{env!="dev"}`, false}, {`up{environment="prod"}`, false},
		{`sum_over_time((up{env="prod"})[30m:1m])`, true}, {`up{env=""}`, false},
		{`sum_over_time((up)[30m:1m])`, false}, {`vector(1)`, true},
	}
	for _, test := range tests {
		t.Run(test.query, func(t *testing.T) {
			if err := validateAgentPromQL(test.query, claims); (err == nil) != test.allowed {
				t.Fatalf("allowed=%v, err=%v", test.allowed, err)
			}
		})
	}
}

func TestSharedDatasourceDiscoveryUsesConnectionPermission(t *testing.T) {
	claims := agenttoken.Claims{DatasourceIds: []string{"shared"}, EnvironmentLabelKey: "env", Environments: []string{"prod"}}
	if !agentDatasourceAllowed(models.AlertDataSource{ID: "shared"}, claims) {
		t.Fatal("an authorized shared connection must be discoverable without a single environment label")
	}
	if agentDatasourceAllowed(models.AlertDataSource{ID: "other"}, claims) {
		t.Fatal("unlisted connections must remain inaccessible")
	}
}

func TestAgentPolicyRevocation(t *testing.T) {
	enabled, disabled := true, false
	tool := "silences.propose_create"
	scope := types.AgentScope{EnvironmentLabelKey: "env", Environments: []string{"test"}}
	config := models.AgentConfig{Enable: &enabled}
	caps := types.AgentCapabilities{Enabled: true, AllowedTools: []string{tool}, Scope: scope}
	if err := validateAgentWritePolicy(config, caps, tool); err != nil {
		t.Fatal(err)
	}
	config.Enable = &disabled
	if err := validateAgentWritePolicy(config, caps, tool); err == nil {
		t.Fatal("disabled Agent must reject old preview")
	}
	config.Enable = &enabled
	caps.AllowedTools = nil
	if err := validateAgentWritePolicy(config, caps, tool); err == nil {
		t.Fatal("revoked permission must reject")
	}
	caps.AllowedTools = []string{tool}
	for _, scope := range []types.AgentScope{{}, {EnvironmentLabelKey: "env", Environments: []string{"prod"}}, {EnvironmentLabelKey: "env", Environments: []string{"GFAI_PRD"}}} {
		caps.Scope = scope
		if err := validateAgentWritePolicy(config, caps, tool); err == nil {
			t.Fatal("production or unknown scope must require explicit write policy")
		}
	}
}

func TestAgentScopeIntersection(t *testing.T) {
	original := agenttoken.Claims{DatasourceIds: []string{"a", "b"}, EnvironmentLabelKey: "env", Environments: []string{"test", "prod"}}
	latest := types.AgentScope{DatasourceIds: []string{"b"}, EnvironmentLabelKey: "env", Environments: []string{"test"}}
	result, err := restrictAgentClaims(original, latest)
	if err != nil || len(result.DatasourceIds) != 1 || result.Environments[0] != "test" {
		t.Fatalf("%#v %v", result, err)
	}
	latest.DatasourceIds = []string{"c"}
	if _, err := restrictAgentClaims(original, latest); err == nil {
		t.Fatal("empty intersection must deny, not become unrestricted")
	}
}
