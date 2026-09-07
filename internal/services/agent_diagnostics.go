package services

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
	"watchAlert/config"
	"watchAlert/internal/types"
)

type AgentDiagnosticCheck struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}
type AgentDiagnostics struct {
	CheckedAt int64                  `json:"checkedAt"`
	Checks    []AgentDiagnosticCheck `json:"checks"`
}

// Diagnostics never generates model output or executes a business Tool.
func (a *agentService) Diagnostics(requestCtx context.Context, tenantID, userID string) (AgentDiagnostics, error) {
	result := AgentDiagnostics{CheckedAt: time.Now().Unix(), Checks: []AgentDiagnosticCheck{}}
	capabilities, err := a.Capabilities(tenantID, userID)
	if err != nil {
		return result, err
	}
	status := "disabled"
	if capabilities.Enabled {
		status = "ok"
	}
	result.Checks = append(result.Checks, AgentDiagnosticCheck{"enabled", status})
	status = "unavailable"
	if len(capabilities.AllowedTools) > 0 {
		status = "ok"
	}
	result.Checks = append(result.Checks, AgentDiagnosticCheck{"permissions", status})
	model, err := a.modelRuntimeConfig()
	if err != nil {
		result.Checks = append(result.Checks, AgentDiagnosticCheck{"credentials", "invalid"})
		return result, nil
	}
	if config.Application.Agent.URL == "" || config.Application.Agent.InternalToken == "" {
		result.Checks = append(result.Checks, AgentDiagnosticCheck{"runtime", "unconfigured"})
		return result, nil
	}
	checks, err := probeAgentRuntime(requestCtx, config.Application.Agent.URL, config.Application.Agent.InternalToken, model)
	if err != nil {
		result.Checks = append(result.Checks, AgentDiagnosticCheck{"runtime", "unavailable"})
		return result, nil
	}
	result.Checks = append(result.Checks, AgentDiagnosticCheck{"runtime", "ok"})
	result.Checks = append(result.Checks, checks...)
	return result, nil
}

func probeAgentRuntime(parent context.Context, baseURL, token string, model types.AgentModelRuntime) ([]AgentDiagnosticCheck, error) {
	payload, _ := json.Marshal(map[string]interface{}{"modelConfig": model})
	deadline, cancel := context.WithTimeout(parent, 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(deadline, http.MethodPost, strings.TrimRight(baseURL, "/")+"/v1/diagnostics", bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("diagnostics unavailable")
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-WatchAlert-Agent-Token", token)
	client := &http.Client{Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	res, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("diagnostics unavailable")
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("diagnostics unavailable")
	}
	raw, err := io.ReadAll(io.LimitReader(res.Body, 8193))
	if err != nil || len(raw) > 8192 {
		return nil, fmt.Errorf("invalid diagnostics")
	}
	var report AgentDiagnostics
	if err = json.Unmarshal(raw, &report); err != nil {
		return nil, fmt.Errorf("invalid diagnostics")
	}
	// Allow only known machine codes, never provider bodies, URLs or secrets.
	allowedIDs := map[string]bool{"sdk": true, "gateway": true, "model": true}
	allowedStatus := map[string]bool{"ok": true, "unavailable": true, "unconfigured": true, "unauthorized": true, "missing_model": true, "invalid": true, "unchecked": true}
	seen := map[string]bool{}
	for _, check := range report.Checks {
		if !allowedIDs[check.ID] || !allowedStatus[check.Status] || seen[check.ID] {
			return nil, fmt.Errorf("invalid diagnostics")
		}
		seen[check.ID] = true
	}
	if len(seen) != 3 {
		return nil, fmt.Errorf("incomplete diagnostics")
	}
	return report.Checks, nil
}
