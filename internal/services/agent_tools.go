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

	"github.com/zeromicro/go-zero/core/logc"
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
		call := models.AgentToolCall{
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
		}
		if auditErr := a.saveAgentToolAudit(&call); auditErr != nil {
			// Never log raw SQL/errors here: they may include tool input/results.
			logc.Errorf(context.Background(), "Agent tool audit persistence failed: call_id=%s", call.ID)
		}
	}()

	capabilities, capabilityErr := AgentService.CapabilitiesContext(requestCtx, claims.TenantId, claims.UserId)
	if capabilityErr != nil {
		return nil, capabilityErr
	}
	if !capabilities.Enabled || !agenttoken.Allows(claims, tool) || !containsTool(capabilities.AllowedTools, tool) {
		return nil, fmt.Errorf("当前用户无权使用 Tool %s", tool)
	}
	claims, err = restrictAgentClaims(claims, capabilities.Scope)
	if err != nil {
		return nil, err
	}

	switch tool {
	case "alerts.search":
		return a.searchAlerts(requestCtx, arguments, claims)
	case "alerts.get":
		return a.getAlert(requestCtx, arguments, claims)
	case "alerts.related":
		return a.relatedAlerts(requestCtx, arguments, claims)
	case "incidents.get":
		return a.getIncident(requestCtx, arguments, claims.TenantId)
	case "rules.get":
		return a.getRule(requestCtx, arguments, claims.TenantId)
	case "silences.search":
		return a.searchSilences(requestCtx, arguments, claims.TenantId)
	case "prometheus.datasources":
		return a.listPrometheusDatasources(requestCtx, claims)
	case "prometheus.rule_query":
		return a.getRulePromQL(requestCtx, arguments, claims.TenantId)
	case "prometheus.query_instant":
		return a.queryPrometheus(requestCtx, arguments, claims, false)
	case "prometheus.query_range":
		return a.queryPrometheus(requestCtx, arguments, claims, true)
	case "silences.propose_create", "silences.propose_update", "silences.propose_delete", "alerts.propose_claim":
		return AgentService.ProposeAction(claims, tool, arguments)
	default:
		return nil, fmt.Errorf("不支持的 Agent Tool: %s", tool)
	}
}

func (a *agentToolService) searchAlerts(requestCtx context.Context, arguments map[string]interface{}, claims agenttoken.Claims) (interface{}, error) {
	var request types.RequestAlertCurEventQuery
	if err := decodeAgentArguments(arguments, &request); err != nil {
		return nil, err
	}
	request.TenantId = claims.TenantId
	// Scope filtering is applied below; never expose an unscoped aggregate.
	request.IncludeSummary = false
	request.AgentDatasourceIds = claims.DatasourceIds
	request.AgentEnvironmentLabelKey = claims.EnvironmentLabelKey
	request.AgentEnvironments = claims.Environments
	request.Page = safeAgentPage(request.Page)
	data, err := EventService.ListCurrentEventContext(requestCtx, &request)
	if err != nil {
		return nil, err.(error)
	}
	response, ok := data.(types.ResponseAlertCurEventList)
	if !ok {
		return nil, fmt.Errorf("当前告警服务返回了无法识别的数据")
	}
	return response, nil
}

func (a *agentToolService) getAlert(requestCtx context.Context, arguments map[string]interface{}, claims agenttoken.Claims) (interface{}, error) {
	fingerprint := stringArgument(arguments, "fingerprint")
	if fingerprint == "" {
		return nil, fmt.Errorf("fingerprint 不能为空")
	}
	data, err := a.searchAlerts(requestCtx, map[string]interface{}{
		"fingerprint": fingerprint, "faultCenterId": stringArgument(arguments, "faultCenterId"),
		"includeRecovered": true, "index": 1, "size": 2,
	}, claims)
	if err != nil {
		return nil, err
	}
	result := data.(types.ResponseAlertCurEventList)
	if len(result.List) == 0 {
		return nil, fmt.Errorf("告警不存在或不在当前授权范围内")
	}
	if len(result.List) != 1 || result.List[0].Fingerprint != fingerprint {
		return nil, fmt.Errorf("告警标识不唯一或不匹配，请指定故障中心")
	}
	return result, nil
}

func (a *agentToolService) relatedAlerts(requestCtx context.Context, arguments map[string]interface{}, claims agenttoken.Claims) (interface{}, error) {
	fingerprint := stringArgument(arguments, "fingerprint")
	if fingerprint == "" {
		return nil, fmt.Errorf("fingerprint 不能为空")
	}
	baseData, err := a.getAlert(requestCtx, arguments, claims)
	if err != nil {
		return nil, err
	}
	base := baseData.(types.ResponseAlertCurEventList)
	if len(base.List) == 0 {
		return base, nil
	}
	event := base.List[0]
	allData, err := a.searchAlerts(requestCtx, map[string]interface{}{"faultCenterId": event.FaultCenterId, "index": 1, "size": 50}, claims)
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
	return map[string]interface{}{"sourceAlert": event, "relatedAlerts": related,
		"relationBasis": "same_rule_in_fault_center", "sameIncidentConfirmed": false,
		"candidateLimit": 50, "truncated": all.Total > int64(len(all.List))}, nil
}

func (a *agentToolService) getIncident(requestCtx context.Context, arguments map[string]interface{}, tenantId string) (interface{}, error) {
	id := stringArgument(arguments, "id")
	if id == "" {
		return nil, fmt.Errorf("故障中心 id 不能为空")
	}
	data, err := FaultCenterService.GetContext(requestCtx, &types.RequestFaultCenterQuery{TenantId: tenantId, ID: id})
	if err != nil {
		return nil, err.(error)
	}
	return data, nil
}

func (a *agentToolService) getRule(requestCtx context.Context, arguments map[string]interface{}, tenantId string) (interface{}, error) {
	requestCtx, cancel := context.WithTimeout(requestCtx, agentDatabaseTimeout)
	defer cancel()
	ruleId := stringArgument(arguments, "ruleId")
	if ruleId == "" {
		return nil, fmt.Errorf("ruleId 不能为空")
	}
	var rule models.AlertRule
	if err := a.ctx.DB.DB().WithContext(requestCtx).Where("tenant_id = ? AND rule_id = ?", tenantId, ruleId).First(&rule).Error; err != nil {
		return nil, err
	}
	return rule, nil
}

func (a *agentToolService) searchSilences(requestCtx context.Context, arguments map[string]interface{}, tenantId string) (interface{}, error) {
	var request types.RequestSilenceQuery
	if err := decodeAgentArguments(arguments, &request); err != nil {
		return nil, err
	}
	request.TenantId = tenantId
	request.Page = safeAgentPage(request.Page)
	if request.Status == "" {
		request.Status = "all"
	}
	data, err := SilenceService.ListContext(requestCtx, &request)
	if err != nil {
		return nil, err.(error)
	}
	return data, nil
}

func (a *agentToolService) listPrometheusDatasources(requestCtx context.Context, claims agenttoken.Claims) (interface{}, error) {
	requestCtx, cancel := context.WithTimeout(requestCtx, agentDatabaseTimeout)
	defer cancel()
	sources, err := a.ctx.DB.Datasource().ListSummariesContext(requestCtx, claims.TenantId, provider.PrometheusDsProvider)
	if err != nil {
		return nil, err
	}
	result := make([]map[string]interface{}, 0, len(sources))
	for _, source := range sources {
		if !agentDatasourceAllowed(source, claims) {
			continue
		}
		result = append(result, map[string]interface{}{
			"id": source.ID, "name": source.Name, "type": source.Type, "labels": source.Labels,
			"description": source.Description, "enabled": source.GetEnabled(),
		})
	}
	return result, nil
}

func (a *agentToolService) getRulePromQL(requestCtx context.Context, arguments map[string]interface{}, tenantId string) (interface{}, error) {
	requestCtx, cancel := context.WithTimeout(requestCtx, agentDatabaseTimeout)
	defer cancel()
	ruleID := stringArgument(arguments, "ruleId")
	if ruleID == "" {
		return nil, fmt.Errorf("ruleId 不能为空")
	}
	var rule models.AlertRule
	if err := a.ctx.DB.DB().WithContext(requestCtx).
		Select("rule_id", "rule_name", "datasource_id_list", "prometheus_config").
		Where("tenant_id = ? AND rule_id = ?", tenantId, ruleID).First(&rule).Error; err != nil {
		return nil, err
	}
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

func (a *agentToolService) queryPrometheus(requestCtx context.Context, arguments map[string]interface{}, claims agenttoken.Claims, isRange bool) (interface{}, error) {
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
	if err := validateAgentPromQL(request.PromQL, claims); err != nil {
		return nil, err
	}
	source, err := a.agentQueryDatasource(requestCtx, claims.TenantId, request.DatasourceId)
	if err != nil {
		return nil, err
	}
	if source.Type != provider.PrometheusDsProvider || !source.GetEnabled() {
		return nil, fmt.Errorf("Prometheus 数据源不可用")
	}
	if len(claims.DatasourceIds) > 0 && !containsTool(claims.DatasourceIds, source.ID) {
		return nil, fmt.Errorf("当前 Copilot 环境范围不允许访问该数据源")
	}
	budget := provider.QueryBudget{MaxSamples: agentPrometheusMaxResultRows, MaxBytes: 4 << 20}
	if !isRange {
		metrics, err := provider.BoundedPrometheusQuery(requestCtx, source, request.PromQL, time.Time{}, time.Time{}, 0, budget)
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
	if request.Step < 0 || request.Step > int64(agentPrometheusMaxRange/time.Second) {
		return nil, fmt.Errorf("Prometheus step 必须介于 0 和 21600 秒之间")
	}
	step := time.Duration(request.Step) * time.Second
	if step <= 0 {
		step = 30 * time.Second
	}
	if points := int(end.Sub(start)/step) + 1; points > agentPrometheusMaxPoints {
		return nil, fmt.Errorf("Prometheus 查询数据点超过上限 %d，请增大 step 或缩小范围", agentPrometheusMaxPoints)
	}
	metrics, err := provider.BoundedPrometheusQuery(requestCtx, source, request.PromQL, start, end, step, budget)
	if err != nil {
		return nil, err
	}
	return agentPrometheusResult(source, request, start.Unix(), end.Unix(), int64(step.Seconds()), metrics), nil
}

func (a *agentToolService) agentQueryDatasource(ctx context.Context, tenantID, datasourceID string) (models.AlertDataSource, error) {
	ctx, cancel := context.WithTimeout(ctx, agentDatabaseTimeout)
	defer cancel()
	return a.ctx.DB.Datasource().GetForTenantContext(ctx, tenantID, datasourceID)
}

// Preserve the existing best-effort audit even when the caller disconnects,
// but never leave its database wait unbounded or create a background goroutine.
func (a *agentToolService) saveAgentToolAudit(call *models.AgentToolCall) error {
	ctx, cancel := context.WithTimeout(context.Background(), agentDatabaseTimeout)
	defer cancel()
	return a.ctx.DB.DB().WithContext(ctx).Create(call).Error
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

// filterAgentAlertScope is deliberately applied after the regular service
// query as a second safety boundary. A browser, model or future connector
// cannot widen the range simply by omitting an environment filter.
func filterAgentAlertScope(data types.ResponseAlertCurEventList, claims agenttoken.Claims) types.ResponseAlertCurEventList {
	if len(claims.DatasourceIds) == 0 && (claims.EnvironmentLabelKey == "" || len(claims.Environments) == 0) {
		return data
	}
	filtered := make([]types.ResponseAlertCurEvent, 0, len(data.List))
	for _, item := range data.List {
		if containsTool(claims.DatasourceIds, item.DatasourceId) || len(claims.DatasourceIds) == 0 {
			if agentEnvironmentAllowed(item, claims) {
				filtered = append(filtered, item)
			}
		}
	}
	data.List = filtered
	data.Total = int64(len(filtered))
	return data
}

func agentEnvironmentAllowed(item types.ResponseAlertCurEvent, claims agenttoken.Claims) bool {
	if claims.EnvironmentLabelKey == "" || len(claims.Environments) == 0 {
		return true
	}
	value, ok := item.Labels[claims.EnvironmentLabelKey].(string)
	return ok && containsTool(claims.Environments, value)
}

func agentDatasourceAllowed(source models.AlertDataSource, claims agenttoken.Claims) bool {
	// The connection may serve several environments. Metric isolation is enforced
	// by validateAgentPromQL, not by descriptive labels on the connection.
	return len(claims.DatasourceIds) == 0 || containsTool(claims.DatasourceIds, source.ID)
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
