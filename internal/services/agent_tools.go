package services

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
	"watchAlert/internal/ctx"
	"watchAlert/internal/models"
	"watchAlert/internal/types"
	"watchAlert/pkg/agenttoken"
	"watchAlert/pkg/provider"
	"watchAlert/pkg/tools"

	"github.com/prometheus/prometheus/promql/parser"
)

const (
	agentPrometheusMaxRange      = 6 * time.Hour
	agentPrometheusMaxPoints     = 1000
	agentPrometheusMaxResultRows = 1000
)

type agentToolService struct {
	ctx *ctx.Context
}

type InterAgentToolService interface {
	Execute(context.Context, agenttoken.Claims, string, map[string]interface{}) (interface{}, error)
}

func newInterAgentToolService(ctx *ctx.Context) InterAgentToolService {
	return &agentToolService{ctx: ctx}
}

func (a *agentToolService) Execute(requestCtx context.Context, claims agenttoken.Claims, tool string, arguments map[string]interface{}) (result interface{}, err error) {
	started := time.Now()
	input, _ := json.Marshal(arguments)
	status := "completed"
	operation := "query"
	if strings.Contains(tool, ".propose_") {
		operation = "write_proposal"
	}
	var callError string
	defer func() {
		if err != nil {
			status = "failed"
			callError = err.Error()
		}
		output, _ := json.Marshal(result)
		_ = a.ctx.DB.DB().Create(&models.AgentToolCall{
			ID:         "at-" + tools.RandId(),
			SessionId:  claims.SessionId,
			TenantId:   claims.TenantId,
			UserId:     claims.UserId,
			ToolName:   tool,
			Operation:  operation,
			Status:     status,
			Input:      string(input),
			Result:     truncateToolResult(string(output)),
			Error:      callError,
			DurationMs: time.Since(started).Milliseconds(),
			CreatedAt:  time.Now().Unix(),
		}).Error
	}()

	capabilities, capabilityErr := AgentService.Capabilities(claims.TenantId, claims.UserId)
	if capabilityErr != nil {
		return nil, capabilityErr
	}
	if !capabilities.Enabled || !agenttoken.Allows(claims, tool) || !containsTool(capabilities.AllowedTools, tool) {
		return nil, fmt.Errorf("当前用户无权使用 Tool %s", tool)
	}

	switch tool {
	case "alerts.search":
		return a.searchAlerts(arguments, claims.TenantId)
	case "alerts.get":
		return a.getAlert(arguments, claims.TenantId)
	case "alerts.related":
		return a.relatedAlerts(arguments, claims.TenantId)
	case "incidents.get":
		return a.getIncident(arguments, claims.TenantId)
	case "rules.get":
		return a.getRule(arguments, claims.TenantId)
	case "silences.search":
		return a.searchSilences(arguments, claims.TenantId)
	case "prometheus.datasources":
		return a.listPrometheusDatasources(claims.TenantId)
	case "prometheus.rule_query":
		return a.getRulePromQL(arguments, claims.TenantId)
	case "prometheus.query_instant":
		return a.queryPrometheus(requestCtx, arguments, claims.TenantId, false)
	case "prometheus.query_range":
		return a.queryPrometheus(requestCtx, arguments, claims.TenantId, true)
	case "silences.propose_create", "silences.propose_update", "silences.propose_delete", "alerts.propose_claim":
		return AgentService.ProposeAction(claims, tool, arguments)
	default:
		return nil, fmt.Errorf("不支持的 Agent Tool: %s", tool)
	}
}

func (a *agentToolService) searchAlerts(arguments map[string]interface{}, tenantId string) (interface{}, error) {
	var request types.RequestAlertCurEventQuery
	if err := decodeAgentArguments(arguments, &request); err != nil {
		return nil, err
	}
	request.TenantId = tenantId
	request.Page = safeAgentPage(request.Page)
	data, err := EventService.ListCurrentEvent(&request)
	if err != nil {
		return nil, err.(error)
	}
	return data, nil
}

func (a *agentToolService) getAlert(arguments map[string]interface{}, tenantId string) (interface{}, error) {
	fingerprint := stringArgument(arguments, "fingerprint")
	if fingerprint == "" {
		return nil, fmt.Errorf("fingerprint 不能为空")
	}
	return a.searchAlerts(map[string]interface{}{"fingerprint": fingerprint, "index": 1, "size": 1}, tenantId)
}

func (a *agentToolService) relatedAlerts(arguments map[string]interface{}, tenantId string) (interface{}, error) {
	fingerprint := stringArgument(arguments, "fingerprint")
	if fingerprint == "" {
		return nil, fmt.Errorf("fingerprint 不能为空")
	}
	baseData, err := a.getAlert(arguments, tenantId)
	if err != nil {
		return nil, err
	}
	base := baseData.(types.ResponseAlertCurEventList)
	if len(base.List) == 0 {
		return base, nil
	}
	event := base.List[0]
	allData, err := a.searchAlerts(map[string]interface{}{"faultCenterId": event.FaultCenterId, "index": 1, "size": 50}, tenantId)
	if err != nil {
		return nil, err
	}
	all := allData.(types.ResponseAlertCurEventList)
	related := make([]types.ResponseAlertCurEvent, 0)
	for _, item := range all.List {
		if item.Fingerprint != event.Fingerprint && item.RuleId == event.RuleId {
			related = append(related, item)
		}
	}
	return map[string]interface{}{"sourceAlert": event, "relatedAlerts": related}, nil
}

func (a *agentToolService) getIncident(arguments map[string]interface{}, tenantId string) (interface{}, error) {
	id := stringArgument(arguments, "id")
	if id == "" {
		return nil, fmt.Errorf("故障中心 id 不能为空")
	}
	data, err := FaultCenterService.Get(&types.RequestFaultCenterQuery{TenantId: tenantId, ID: id})
	if err != nil {
		return nil, err.(error)
	}
	return data, nil
}

func (a *agentToolService) getRule(arguments map[string]interface{}, tenantId string) (interface{}, error) {
	ruleId := stringArgument(arguments, "ruleId")
	if ruleId == "" {
		return nil, fmt.Errorf("ruleId 不能为空")
	}
	var rule models.AlertRule
	if err := a.ctx.DB.DB().Where("tenant_id = ? AND rule_id = ?", tenantId, ruleId).First(&rule).Error; err != nil {
		return nil, err
	}
	return rule, nil
}

func (a *agentToolService) searchSilences(arguments map[string]interface{}, tenantId string) (interface{}, error) {
	var request types.RequestSilenceQuery
	if err := decodeAgentArguments(arguments, &request); err != nil {
		return nil, err
	}
	request.TenantId = tenantId
	request.Page = safeAgentPage(request.Page)
	if request.Status == "" {
		request.Status = "all"
	}
	data, err := SilenceService.List(&request)
	if err != nil {
		return nil, err.(error)
	}
	return data, nil
}

func (a *agentToolService) listPrometheusDatasources(tenantId string) (interface{}, error) {
	sources, err := a.ctx.DB.Datasource().List(tenantId, "", provider.PrometheusDsProvider, "")
	if err != nil {
		return nil, err
	}
	result := make([]map[string]interface{}, 0, len(sources))
	for _, source := range sources {
		result = append(result, map[string]interface{}{
			"id": source.ID, "name": source.Name, "type": source.Type, "labels": source.Labels,
			"description": source.Description, "enabled": source.GetEnabled(),
		})
	}
	return result, nil
}

func (a *agentToolService) getRulePromQL(arguments map[string]interface{}, tenantId string) (interface{}, error) {
	data, err := a.getRule(arguments, tenantId)
	if err != nil {
		return nil, err
	}
	rule := data.(models.AlertRule)
	return map[string]interface{}{
		"ruleId": rule.RuleId, "ruleName": rule.RuleName, "datasourceIds": rule.DatasourceIdList,
		"promql": rule.PrometheusConfig.PromQL, "severityRules": rule.PrometheusConfig.Rules,
	}, nil
}

type agentPrometheusQuery struct {
	DatasourceId string `json:"datasourceId"`
	PromQL       string `json:"promql"`
	Start        int64  `json:"start"`
	End          int64  `json:"end"`
	Step         int64  `json:"step"`
}

func (a *agentToolService) queryPrometheus(_ context.Context, arguments map[string]interface{}, tenantId string, isRange bool) (interface{}, error) {
	var request agentPrometheusQuery
	if err := decodeAgentArguments(arguments, &request); err != nil {
		return nil, err
	}
	if request.DatasourceId == "" || strings.TrimSpace(request.PromQL) == "" {
		return nil, fmt.Errorf("datasourceId 和 promql 均不能为空")
	}
	if len(request.PromQL) > 4096 {
		return nil, fmt.Errorf("PromQL 超过最大长度")
	}
	if _, err := parser.ParseExpr(request.PromQL); err != nil {
		return nil, fmt.Errorf("PromQL 校验失败: %w", err)
	}
	source, err := a.ctx.DB.Datasource().GetForTenant(tenantId, request.DatasourceId)
	if err != nil {
		return nil, err
	}
	if source.Type != provider.PrometheusDsProvider || !source.GetEnabled() {
		return nil, fmt.Errorf("Prometheus 数据源不可用")
	}
	client, err := provider.NewPrometheusClient(source)
	if err != nil {
		return nil, err
	}
	if !isRange {
		metrics, err := client.Query(request.PromQL)
		if err != nil {
			return nil, err
		}
		return agentPrometheusResult(source, request, 0, 0, 0, metrics), nil
	}
	end := time.Now()
	if request.End > 0 {
		end = time.Unix(request.End, 0)
	}
	start := end.Add(-30 * time.Minute)
	if request.Start > 0 {
		start = time.Unix(request.Start, 0)
	}
	if !start.Before(end) || end.Sub(start) > agentPrometheusMaxRange {
		return nil, fmt.Errorf("Prometheus 查询时间范围必须大于 0 且不超过 %s", agentPrometheusMaxRange)
	}
	step := time.Duration(request.Step) * time.Second
	if step <= 0 {
		step = 30 * time.Second
	}
	if points := int(end.Sub(start) / step); points > agentPrometheusMaxPoints {
		return nil, fmt.Errorf("Prometheus 查询数据点超过上限 %d，请增大 step 或缩小范围", agentPrometheusMaxPoints)
	}
	metrics, err := client.QueryRange(request.PromQL, start, end, step)
	if err != nil {
		return nil, err
	}
	return agentPrometheusResult(source, request, start.Unix(), end.Unix(), int64(step.Seconds()), metrics), nil
}

func agentPrometheusResult(source models.AlertDataSource, request agentPrometheusQuery, start, end, step int64, metrics []provider.Metrics) map[string]interface{} {
	truncated := false
	if len(metrics) > agentPrometheusMaxResultRows {
		metrics = metrics[:agentPrometheusMaxResultRows]
		truncated = true
	}
	return map[string]interface{}{
		"datasource": map[string]string{"id": source.ID, "name": source.Name},
		"promql":     request.PromQL, "start": start, "end": end, "step": step,
		"resultCount": len(metrics), "truncated": truncated, "metrics": metrics,
	}
}

func decodeAgentArguments(arguments map[string]interface{}, target interface{}) error {
	data, err := json.Marshal(arguments)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, target)
}

func stringArgument(arguments map[string]interface{}, key string) string {
	value, exists := arguments[key]
	if !exists || value == nil {
		return ""
	}
	return strings.TrimSpace(fmt.Sprint(value))
}

func safeAgentPage(page models.Page) models.Page {
	if page.Index <= 0 {
		page.Index = 1
	}
	if page.Size <= 0 {
		page.Size = 20
	}
	if page.Size > 50 {
		page.Size = 50
	}
	return page
}

func containsTool(tools []string, candidate string) bool {
	for _, tool := range tools {
		if tool == candidate {
			return true
		}
	}
	return false
}

func truncateToolResult(value string) string {
	const max = 32 * 1024
	if len(value) <= max {
		return value
	}
	return value[:max] + "…"
}
