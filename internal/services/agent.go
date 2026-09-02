package services

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
	"watchAlert/config"
	"watchAlert/internal/ctx"
	"watchAlert/internal/models"
	"watchAlert/internal/types"
	"watchAlert/pkg/agenttoken"
	"watchAlert/pkg/secretbox"
	"watchAlert/pkg/tools"
)

type agentService struct {
	ctx *ctx.Context
}

type InterAgentService interface {
	CreateSession(tenantId, userId string, req *types.RequestAgentSessionCreate) (models.AgentSession, error)
	GetSession(tenantId, userId, sessionId string) (types.ResponseAgentSessionDetail, error)
	ListSessions(tenantId, userId string) ([]models.AgentSession, error)
	Capabilities(tenantId, userId string) (types.AgentCapabilities, error)
	SendMessage(ctx context.Context, tenantId, userId string, req *types.RequestAgentSessionMessage) (models.AgentMessage, error)
	StreamMessage(ctx context.Context, tenantId, userId string, req *types.RequestAgentSessionMessage, emit func(types.AgentStreamEvent)) error
	ProposeAction(claims agenttoken.Claims, tool string, arguments map[string]interface{}) (models.AgentPendingAction, error)
	ConfirmAction(tenantId, userId string, req *types.RequestAgentActionConfirm) (models.AgentPendingAction, error)
}

// StreamMessage persists the user message before calling the isolated Agent
// service and persists the final assistant reply only after a terminal done
// event. The browser receives no provider credential or Tool result.
func (a *agentService) StreamMessage(requestCtx context.Context, tenantId, userId string, req *types.RequestAgentSessionMessage, emit func(types.AgentStreamEvent)) error {
	if strings.TrimSpace(req.Content) == "" {
		return fmt.Errorf("对话内容不能为空")
	}
	detail, err := a.GetSession(tenantId, userId, req.SessionId)
	if err != nil {
		return err
	}
	capabilities, err := a.Capabilities(tenantId, userId)
	if err != nil {
		return err
	}
	if !capabilities.Enabled {
		return fmt.Errorf("WatchAlert Copilot 尚未启用")
	}
	if config.Application.Agent.URL == "" || config.Application.Agent.InternalToken == "" {
		return fmt.Errorf("Copilot Agent 服务尚未配置")
	}

	userMessage := models.AgentMessage{
		ID: "am-" + tools.RandId(), SessionId: req.SessionId, TenantId: tenantId,
		Role: "user", Content: strings.TrimSpace(req.Content), CreatedAt: time.Now().Unix(),
	}
	if err := a.ctx.DB.DB().Create(&userMessage).Error; err != nil {
		return err
	}
	runToken, err := agenttoken.Sign(agenttoken.Claims{
		SessionId: req.SessionId, TenantId: tenantId, UserId: userId, Tools: capabilities.AllowedTools,
		DatasourceIds: capabilities.Scope.DatasourceIds, EnvironmentLabelKey: capabilities.Scope.EnvironmentLabelKey,
		Environments: capabilities.Scope.Environments, ExpiresAt: time.Now().Add(5 * time.Minute).Unix(),
	}, config.Application.Agent.InternalToken)
	if err != nil {
		return err
	}
	payload := types.AgentRunRequest{
		SessionId: req.SessionId, RunToken: runToken, Message: userMessage.Content,
		Messages: append(detail.Messages, userMessage), Context: req.Context,
		AllowedTools: capabilities.AllowedTools, Scope: capabilities.Scope,
	}
	modelConfig, err := a.modelRuntimeConfig()
	if err != nil {
		return err
	}
	payload.ModelConfig = modelConfig
	response, err := callAgentServiceStream(requestCtx, payload, emit)
	if err != nil {
		return err
	}
	if strings.TrimSpace(response.Content) == "" {
		return fmt.Errorf("Agent 服务未返回分析内容")
	}
	assistantMessage := models.AgentMessage{
		ID: "am-" + tools.RandId(), SessionId: req.SessionId, TenantId: tenantId,
		Role: "assistant", Content: response.Content, Evidence: response.Evidence, CreatedAt: time.Now().Unix(),
	}
	if err := a.ctx.DB.DB().Create(&assistantMessage).Error; err != nil {
		return err
	}
	_ = a.ctx.DB.DB().Model(&models.AgentSession{}).Where("id = ? AND tenant_id = ?", req.SessionId, tenantId).
		Updates(map[string]interface{}{"updated_at": assistantMessage.CreatedAt, "title": sessionTitle(detail.Session.Title, userMessage.Content)}).Error
	return nil
}

func newInterAgentService(ctx *ctx.Context) InterAgentService {
	return &agentService{ctx: ctx}
}

func (a *agentService) CreateSession(tenantId, userId string, req *types.RequestAgentSessionCreate) (models.AgentSession, error) {
	title := strings.TrimSpace(req.Title)
	if title == "" {
		title = "新的 Copilot 对话"
	}
	now := time.Now().Unix()
	session := models.AgentSession{
		ID:        "as-" + tools.RandId(),
		TenantId:  tenantId,
		UserId:    userId,
		Title:     truncateRunes(title, 120),
		Status:    "active",
		CreatedAt: now,
		UpdatedAt: now,
	}
	if err := a.ctx.DB.DB().Create(&session).Error; err != nil {
		return session, err
	}
	return session, nil
}

func (a *agentService) GetSession(tenantId, userId, sessionId string) (types.ResponseAgentSessionDetail, error) {
	var session models.AgentSession
	db := a.ctx.DB.DB().Where("id = ? AND tenant_id = ?", sessionId, tenantId)
	if userId != "admin" {
		db = db.Where("user_id = ?", userId)
	}
	if err := db.First(&session).Error; err != nil {
		return types.ResponseAgentSessionDetail{}, err
	}
	var messages []models.AgentMessage
	if err := a.ctx.DB.DB().Where("session_id = ? AND tenant_id = ?", sessionId, tenantId).Order("created_at asc").Find(&messages).Error; err != nil {
		return types.ResponseAgentSessionDetail{}, err
	}
	return types.ResponseAgentSessionDetail{Session: session, Messages: messages}, nil
}

func (a *agentService) ListSessions(tenantId, userId string) ([]models.AgentSession, error) {
	var sessions []models.AgentSession
	db := a.ctx.DB.DB().Where("tenant_id = ?", tenantId)
	if userId != "admin" {
		db = db.Where("user_id = ?", userId)
	}
	err := db.Order("updated_at desc").Limit(50).Find(&sessions).Error
	return sessions, err
}

func (a *agentService) Capabilities(tenantId, userId string) (types.AgentCapabilities, error) {
	settings, err := a.ctx.DB.Setting().Get()
	if err != nil {
		return types.AgentCapabilities{}, err
	}
	allowedTools, err := a.allowedToolsForUser(tenantId, userId, settings.AgentConfig.AllowedTools)
	if err != nil {
		return types.AgentCapabilities{}, err
	}
	return types.AgentCapabilities{
		Enabled:      settings.AgentConfig.GetEnable(),
		AllowedTools: allowedTools,
		CanWrite:     hasWriteTool(allowedTools),
		Scope:        agentScopeFromSettings(settings.AgentConfig.Scope),
	}, nil
}

func (a *agentService) ProposeAction(claims agenttoken.Claims, tool string, arguments map[string]interface{}) (models.AgentPendingAction, error) {
	capabilities, err := a.Capabilities(claims.TenantId, claims.UserId)
	if err != nil {
		return models.AgentPendingAction{}, err
	}
	if !capabilities.Enabled || !agenttoken.Allows(claims, tool) || !containsTool(capabilities.AllowedTools, tool) {
		return models.AgentPendingAction{}, fmt.Errorf("当前用户无权提出操作 %s", tool)
	}
	settings, err := a.ctx.DB.Setting().Get()
	if err != nil {
		return models.AgentPendingAction{}, err
	}
	if agentScopeContainsProduction(capabilities.Scope) && (settings.AgentConfig.AllowProductionWrite == nil || !*settings.AgentConfig.AllowProductionWrite) {
		return models.AgentPendingAction{}, fmt.Errorf("当前 Copilot 范围包含生产环境，但未允许生产环境写操作")
	}

	payload, preview, risk, err := a.buildActionPreview(claims, tool, arguments)
	if err != nil {
		return models.AgentPendingAction{}, err
	}
	payloadBytes, _ := json.Marshal(payload)
	previewBytes, _ := json.Marshal(preview)
	now := time.Now().Unix()
	action := models.AgentPendingAction{
		ID:          "aa-" + tools.RandId(),
		TenantId:    claims.TenantId,
		UserId:      claims.UserId,
		SessionId:   claims.SessionId,
		ActionType:  tool,
		Status:      "pending_confirmation",
		RiskLevel:   risk,
		Payload:     string(payloadBytes),
		PayloadHash: fmt.Sprintf("%x", sha256.Sum256(payloadBytes)),
		Preview:     string(previewBytes),
		ExpiresAt:   time.Now().Add(10 * time.Minute).Unix(),
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	if err := a.ctx.DB.DB().Create(&action).Error; err != nil {
		return models.AgentPendingAction{}, err
	}
	return action, nil
}

func (a *agentService) ConfirmAction(tenantId, userId string, req *types.RequestAgentActionConfirm) (models.AgentPendingAction, error) {
	if req.ActionId == "" || req.PayloadHash == "" {
		return models.AgentPendingAction{}, fmt.Errorf("操作 ID 和确认摘要不能为空")
	}
	var action models.AgentPendingAction
	if err := a.ctx.DB.DB().Where("id = ? AND tenant_id = ? AND user_id = ?", req.ActionId, tenantId, userId).First(&action).Error; err != nil {
		return models.AgentPendingAction{}, err
	}
	if action.Status != "pending_confirmation" {
		return action, fmt.Errorf("该操作当前不可确认: %s", action.Status)
	}
	if action.ExpiresAt <= time.Now().Unix() {
		_ = a.ctx.DB.DB().Model(&models.AgentPendingAction{}).Where("id = ?", action.ID).Updates(map[string]interface{}{"status": "expired", "updated_at": time.Now().Unix()}).Error
		return action, fmt.Errorf("操作预览已过期，请重新生成")
	}
	if action.PayloadHash != req.PayloadHash {
		return action, fmt.Errorf("操作内容已变化，请重新确认")
	}
	capabilities, err := a.Capabilities(tenantId, userId)
	if err != nil || !containsTool(capabilities.AllowedTools, action.ActionType) {
		return action, fmt.Errorf("当前用户已不具备该操作权限")
	}
	claimed := a.ctx.DB.DB().Model(&models.AgentPendingAction{}).
		Where("id = ? AND status = ?", action.ID, "pending_confirmation").
		Updates(map[string]interface{}{"status": "executing", "confirmed_at": time.Now().Unix(), "updated_at": time.Now().Unix()})
	if claimed.Error != nil || claimed.RowsAffected != 1 {
		return action, fmt.Errorf("操作正在被处理或已失效")
	}

	result, executeErr := a.executeConfirmedAction(action, tenantId, userId)
	resultBytes, _ := json.Marshal(result)
	updated := map[string]interface{}{"updated_at": time.Now().Unix(), "result": string(resultBytes)}
	if executeErr != nil {
		updated["status"] = "failed"
		updated["result"] = fmt.Sprintf("%s", executeErr.Error())
	} else {
		updated["status"] = "executed"
	}
	_ = a.ctx.DB.DB().Model(&models.AgentPendingAction{}).Where("id = ?", action.ID).Updates(updated).Error
	_ = a.ctx.DB.DB().Where("id = ?", action.ID).First(&action).Error
	return action, executeErr
}

func (a *agentService) SendMessage(requestCtx context.Context, tenantId, userId string, req *types.RequestAgentSessionMessage) (models.AgentMessage, error) {
	if strings.TrimSpace(req.Content) == "" {
		return models.AgentMessage{}, fmt.Errorf("对话内容不能为空")
	}
	detail, err := a.GetSession(tenantId, userId, req.SessionId)
	if err != nil {
		return models.AgentMessage{}, err
	}
	capabilities, err := a.Capabilities(tenantId, userId)
	if err != nil {
		return models.AgentMessage{}, err
	}
	if !capabilities.Enabled {
		return models.AgentMessage{}, fmt.Errorf("WatchAlert Copilot 尚未启用")
	}
	if config.Application.Agent.URL == "" || config.Application.Agent.InternalToken == "" {
		return models.AgentMessage{}, fmt.Errorf("Copilot Agent 服务尚未配置")
	}

	now := time.Now().Unix()
	userMessage := models.AgentMessage{
		ID:        "am-" + tools.RandId(),
		SessionId: req.SessionId,
		TenantId:  tenantId,
		Role:      "user",
		Content:   strings.TrimSpace(req.Content),
		CreatedAt: now,
	}
	if err := a.ctx.DB.DB().Create(&userMessage).Error; err != nil {
		return models.AgentMessage{}, err
	}

	runToken, err := agenttoken.Sign(agenttoken.Claims{
		SessionId:           req.SessionId,
		TenantId:            tenantId,
		UserId:              userId,
		Tools:               capabilities.AllowedTools,
		DatasourceIds:       capabilities.Scope.DatasourceIds,
		EnvironmentLabelKey: capabilities.Scope.EnvironmentLabelKey,
		Environments:        capabilities.Scope.Environments,
		ExpiresAt:           time.Now().Add(5 * time.Minute).Unix(),
	}, config.Application.Agent.InternalToken)
	if err != nil {
		return models.AgentMessage{}, err
	}

	payload := types.AgentRunRequest{
		SessionId:    req.SessionId,
		RunToken:     runToken,
		Message:      userMessage.Content,
		Messages:     append(detail.Messages, userMessage),
		Context:      req.Context,
		AllowedTools: capabilities.AllowedTools,
		Scope:        capabilities.Scope,
	}
	modelConfig, err := a.modelRuntimeConfig()
	if err != nil {
		return models.AgentMessage{}, err
	}
	payload.ModelConfig = modelConfig
	response, err := callAgentService(requestCtx, payload)
	if err != nil {
		return models.AgentMessage{}, err
	}

	assistantMessage := models.AgentMessage{
		ID:        "am-" + tools.RandId(),
		SessionId: req.SessionId,
		TenantId:  tenantId,
		Role:      "assistant",
		Content:   response.Content,
		Evidence:  response.Evidence,
		CreatedAt: time.Now().Unix(),
	}
	if assistantMessage.Content == "" {
		return models.AgentMessage{}, fmt.Errorf("Agent 服务未返回分析内容")
	}
	if err := a.ctx.DB.DB().Create(&assistantMessage).Error; err != nil {
		return models.AgentMessage{}, err
	}
	_ = a.ctx.DB.DB().Model(&models.AgentSession{}).Where("id = ? AND tenant_id = ?", req.SessionId, tenantId).
		Updates(map[string]interface{}{"updated_at": assistantMessage.CreatedAt, "title": sessionTitle(detail.Session.Title, userMessage.Content)}).Error
	return assistantMessage, nil
}

func (a *agentService) modelRuntimeConfig() (types.AgentModelRuntime, error) {
	settings, err := a.ctx.DB.Setting().Get()
	if err != nil {
		return types.AgentModelRuntime{}, err
	}
	model := settings.AgentConfig.Model
	if model.APIKeyEncrypted == "" {
		// Keep the deployment-environment fallback for existing installations.
		return types.AgentModelRuntime{}, nil
	}
	apiKey, err := secretbox.Decrypt(model.APIKeyEncrypted, config.Application.Agent.CredentialKey)
	if err != nil {
		return types.AgentModelRuntime{}, err
	}
	if model.Provider != "deepseek" || model.BaseURL == "" || model.Model == "" {
		return types.AgentModelRuntime{}, fmt.Errorf("Copilot 模型配置不完整")
	}
	return types.AgentModelRuntime{Provider: model.Provider, BaseURL: model.BaseURL, Model: model.Model, APIKey: apiKey}, nil
}

func agentScopeFromSettings(scope models.AgentScope) types.AgentScope {
	return types.AgentScope{
		DatasourceIds:       uniqueNonEmpty(scope.DatasourceIds),
		EnvironmentLabelKey: strings.TrimSpace(scope.EnvironmentLabelKey),
		Environments:        uniqueNonEmpty(scope.Environments),
	}
}

func uniqueNonEmpty(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, exists := seen[value]; !exists {
			seen[value] = struct{}{}
			result = append(result, value)
		}
	}
	return result
}

func callAgentService(ctx context.Context, payload types.AgentRunRequest) (types.AgentRunResponse, error) {
	var result types.AgentRunResponse
	body, err := json.Marshal(payload)
	if err != nil {
		return result, err
	}
	timeout := config.Application.Agent.Timeout
	if timeout <= 0 {
		timeout = 60
	}
	requestCtx, cancel := context.WithTimeout(ctx, time.Duration(timeout)*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(requestCtx, http.MethodPost, strings.TrimRight(config.Application.Agent.URL, "/")+"/v1/runs", bytes.NewReader(body))
	if err != nil {
		return result, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-WatchAlert-Agent-Token", config.Application.Agent.InternalToken)
	resp, err := (&http.Client{}).Do(req)
	if err != nil {
		return result, fmt.Errorf("调用 Copilot Agent 服务失败: %w", err)
	}
	defer resp.Body.Close()
	content, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if err != nil {
		return result, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return result, fmt.Errorf("Copilot Agent 服务返回 %d: %s", resp.StatusCode, strings.TrimSpace(string(content)))
	}
	if err := json.Unmarshal(content, &result); err != nil {
		return result, fmt.Errorf("解析 Copilot Agent 响应失败: %w", err)
	}
	return result, nil
}

func callAgentServiceStream(ctx context.Context, payload types.AgentRunRequest, emit func(types.AgentStreamEvent)) (types.AgentRunResponse, error) {
	var result types.AgentRunResponse
	body, err := json.Marshal(payload)
	if err != nil {
		return result, err
	}
	timeout := config.Application.Agent.Timeout
	if timeout <= 0 {
		timeout = 60
	}
	requestCtx, cancel := context.WithTimeout(ctx, time.Duration(timeout)*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(requestCtx, http.MethodPost, strings.TrimRight(config.Application.Agent.URL, "/")+"/v1/runs/stream", bytes.NewReader(body))
	if err != nil {
		return result, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("X-WatchAlert-Agent-Token", config.Application.Agent.InternalToken)
	resp, err := (&http.Client{}).Do(req)
	if err != nil {
		return result, fmt.Errorf("调用 Copilot Agent 流服务失败: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		content, _ := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
		return result, fmt.Errorf("Copilot Agent 流服务返回 %d: %s", resp.StatusCode, strings.TrimSpace(string(content)))
	}

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 4096), 2<<20)
	eventType, data := "message", ""
	dispatch := func() error {
		if data == "" {
			return nil
		}
		var event types.AgentStreamEvent
		if err := json.Unmarshal([]byte(data), &event); err != nil {
			return fmt.Errorf("解析 Copilot 流事件失败: %w", err)
		}
		event.Type = eventType
		switch eventType {
		case "delta", "status":
			emit(event)
		case "done":
			result.Content, result.Evidence = event.Content, event.Evidence
			emit(event)
		case "error":
			return fmt.Errorf("Copilot Agent 运行失败: %s", event.Message)
		}
		return nil
	}
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			if err := dispatch(); err != nil {
				return result, err
			}
			eventType, data = "message", ""
			continue
		}
		if strings.HasPrefix(line, "event:") {
			eventType = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
		} else if strings.HasPrefix(line, "data:") {
			data += strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		}
	}
	if err := scanner.Err(); err != nil {
		return result, err
	}
	if err := dispatch(); err != nil {
		return result, err
	}
	if result.Content == "" {
		return result, fmt.Errorf("Copilot Agent 流服务提前结束")
	}
	return result, nil
}

func defaultReadTools() []string {
	return []string{
		"alerts.search", "alerts.get", "alerts.related", "incidents.get",
		"rules.get", "silences.search", "prometheus.datasources",
		"prometheus.rule_query", "prometheus.query_instant", "prometheus.query_range",
	}
}

func (a *agentService) allowedToolsForUser(tenantId, userId string, configured []string) ([]string, error) {
	ceiling := configured
	if len(ceiling) == 0 {
		ceiling = defaultReadTools()
	}
	if userId == "admin" {
		return ceiling, nil
	}

	linked, err := a.ctx.DB.Tenant().GetTenantLinkedUserInfo(tenantId, userId)
	if err != nil {
		return nil, err
	}
	var role models.UserRole
	if err := a.ctx.DB.DB().Where("id = ?", linked.UserRole).First(&role).Error; err != nil {
		return nil, err
	}
	permissions := make(map[string]struct{}, len(role.Permissions))
	for _, permission := range role.Permissions {
		permissions[permission.API] = struct{}{}
	}

	allowed := make([]string, 0, len(ceiling))
	for _, tool := range ceiling {
		required, exists := toolRequiredPermission[tool]
		if !exists {
			continue
		}
		if _, ok := permissions[required]; ok {
			allowed = append(allowed, tool)
		}
	}
	return allowed, nil
}

var toolRequiredPermission = map[string]string{
	"alerts.search":            "/api/w8t/event/curEvent",
	"alerts.get":               "/api/w8t/event/curEvent",
	"alerts.related":           "/api/w8t/event/curEvent",
	"incidents.get":            "/api/w8t/faultCenter/faultCenterSearch",
	"rules.get":                "/api/w8t/rule/ruleSearch",
	"silences.search":          "/api/w8t/silence/silenceList",
	"prometheus.datasources":   "/api/w8t/datasource/dataSourceList",
	"prometheus.rule_query":    "/api/w8t/rule/ruleSearch",
	"prometheus.query_instant": "/api/w8t/datasource/dataSourceList",
	"prometheus.query_range":   "/api/w8t/datasource/dataSourceList",
	"silences.propose_create":  "/api/w8t/silence/silenceCreate",
	"silences.propose_update":  "/api/w8t/silence/silenceUpdate",
	"silences.propose_delete":  "/api/w8t/silence/silenceDelete",
	"alerts.propose_claim":     "/api/w8t/event/process",
}

func truncateRunes(value string, max int) string {
	runes := []rune(value)
	if len(runes) <= max {
		return value
	}
	return string(runes[:max]) + "…"
}

func sessionTitle(current, firstMessage string) string {
	if current != "" && current != "新的 Copilot 对话" {
		return current
	}
	return truncateRunes(firstMessage, 60)
}

func hasWriteTool(tools []string) bool {
	for _, tool := range tools {
		if strings.HasPrefix(tool, "silences.propose_") || tool == "alerts.propose_claim" {
			return true
		}
	}
	return false
}

func (a *agentService) buildActionPreview(claims agenttoken.Claims, tool string, arguments map[string]interface{}) (interface{}, interface{}, string, error) {
	now := time.Now().Unix()
	switch tool {
	case "silences.propose_create":
		var payload types.RequestSilenceCreate
		if err := decodeAgentArguments(arguments, &payload); err != nil {
			return nil, nil, "", err
		}
		payload.TenantId = claims.TenantId
		payload.UpdateBy = claims.UserId
		if payload.Name == "" || len(payload.Labels) == 0 {
			return nil, nil, "", fmt.Errorf("静默名称和至少一个 Label 条件不能为空")
		}
		if err := validateSilenceActionScope(payload.Labels, claims); err != nil {
			return nil, nil, "", err
		}
		if payload.StartsAt == 0 {
			payload.StartsAt = now
		}
		if payload.EndsAt <= payload.StartsAt || payload.EndsAt-payload.StartsAt > int64((30*24*time.Hour).Seconds()) {
			return nil, nil, "", fmt.Errorf("静默结束时间必须晚于开始时间且最长不超过 30 天")
		}
		if payload.FaultCenterId != "" && !a.faultCenterExists(claims.TenantId, payload.FaultCenterId) {
			return nil, nil, "", fmt.Errorf("目标故障中心不存在或不属于当前租户")
		}
		return payload, map[string]interface{}{"action": "创建静默", "name": payload.Name, "labels": payload.Labels, "startsAt": payload.StartsAt, "endsAt": payload.EndsAt, "faultCenterId": payload.FaultCenterId, "comment": payload.Comment}, "medium", nil
	case "silences.propose_update":
		var payload types.RequestSilenceUpdate
		if err := decodeAgentArguments(arguments, &payload); err != nil {
			return nil, nil, "", err
		}
		if payload.ID == "" {
			return nil, nil, "", fmt.Errorf("静默 ID 不能为空")
		}
		var before models.AlertSilences
		if err := a.ctx.DB.DB().Where("tenant_id = ? AND id = ?", claims.TenantId, payload.ID).First(&before).Error; err != nil {
			return nil, nil, "", err
		}
		payload.TenantId = claims.TenantId
		payload.UpdateBy = claims.UserId
		if payload.StartsAt == 0 {
			payload.StartsAt = before.StartsAt
		}
		if payload.EndsAt == 0 {
			payload.EndsAt = before.EndsAt
		}
		if len(payload.Labels) == 0 {
			payload.Labels = before.Labels
		}
		if payload.EndsAt <= payload.StartsAt {
			return nil, nil, "", fmt.Errorf("静默结束时间必须晚于开始时间")
		}
		if err := validateSilenceActionScope(payload.Labels, claims); err != nil {
			return nil, nil, "", err
		}
		return payload, map[string]interface{}{"action": "修改静默", "before": before, "after": payload}, "medium", nil
	case "silences.propose_delete":
		id := stringArgument(arguments, "id")
		if id == "" {
			return nil, nil, "", fmt.Errorf("静默 ID 不能为空")
		}
		var before models.AlertSilences
		if err := a.ctx.DB.DB().Where("tenant_id = ? AND id = ?", claims.TenantId, id).First(&before).Error; err != nil {
			return nil, nil, "", err
		}
		if err := validateSilenceActionScope(before.Labels, claims); err != nil {
			return nil, nil, "", err
		}
		return types.RequestSilenceQuery{TenantId: claims.TenantId, ID: id, FaultCenterId: before.FaultCenterId}, map[string]interface{}{"action": "删除静默", "before": before, "reversible": false}, "high", nil
	case "alerts.propose_claim":
		var payload types.RequestProcessAlertEvent
		if err := decodeAgentArguments(arguments, &payload); err != nil {
			return nil, nil, "", err
		}
		if payload.FaultCenterId == "" || len(payload.Fingerprints) == 0 || len(payload.Fingerprints) > 50 {
			return nil, nil, "", fmt.Errorf("认领需要故障中心和 1 至 50 条告警")
		}
		if !a.faultCenterExists(claims.TenantId, payload.FaultCenterId) {
			return nil, nil, "", fmt.Errorf("目标故障中心不存在或不属于当前租户")
		}
		if err := a.validateAlertActionScope(claims, payload.Fingerprints); err != nil {
			return nil, nil, "", err
		}
		payload.TenantId, payload.Username, payload.Time = claims.TenantId, claims.UserId, now
		return payload, map[string]interface{}{"action": "认领告警", "faultCenterId": payload.FaultCenterId, "fingerprints": payload.Fingerprints, "count": len(payload.Fingerprints)}, "medium", nil
	default:
		return nil, nil, "", fmt.Errorf("不支持的写操作 Tool: %s", tool)
	}
}

func agentScopeContainsProduction(scope types.AgentScope) bool {
	for _, environment := range scope.Environments {
		value := strings.ToLower(strings.TrimSpace(environment))
		if value == "production" || value == "prod" {
			return true
		}
	}
	return false
}

func validateSilenceActionScope(labels []models.SilenceLabel, claims agenttoken.Claims) error {
	if claims.EnvironmentLabelKey != "" && len(claims.Environments) > 0 {
		matched := false
		for _, label := range labels {
			if label.Key == claims.EnvironmentLabelKey && (label.Operator == "=" || label.Operator == "==") && containsTool(claims.Environments, label.Value) {
				matched = true
			}
		}
		if !matched {
			return fmt.Errorf("受限环境中的静默必须包含精确 %s Label 条件", claims.EnvironmentLabelKey)
		}
	}
	if len(claims.DatasourceIds) > 0 {
		matched := false
		for _, label := range labels {
			if label.Key == "datasource_id" && (label.Operator == "=" || label.Operator == "==") && containsTool(claims.DatasourceIds, label.Value) {
				matched = true
			}
		}
		if !matched {
			return fmt.Errorf("受限数据源中的静默必须包含精确 datasource_id Label 条件")
		}
	}
	return nil
}

func (a *agentService) validateAlertActionScope(claims agenttoken.Claims, fingerprints []string) error {
	for _, fingerprint := range fingerprints {
		var event models.AlertCurEvent
		if err := a.ctx.DB.DB().Where("tenant_id = ? AND fingerprint = ?", claims.TenantId, fingerprint).First(&event).Error; err != nil {
			return fmt.Errorf("待认领告警不存在或不属于当前租户: %s", fingerprint)
		}
		if len(claims.DatasourceIds) > 0 && !containsTool(claims.DatasourceIds, event.DatasourceId) {
			return fmt.Errorf("待认领告警不在允许的数据源范围内: %s", fingerprint)
		}
		if claims.EnvironmentLabelKey != "" && len(claims.Environments) > 0 {
			environment := ""
			if event.Labels != nil {
				environment = strings.TrimSpace(fmt.Sprint(event.Labels[claims.EnvironmentLabelKey]))
			}
			if !containsTool(claims.Environments, environment) {
				return fmt.Errorf("待认领告警不在允许的环境范围内: %s", fingerprint)
			}
		}
	}
	return nil
}

func (a *agentService) executeConfirmedAction(action models.AgentPendingAction, tenantId, userId string) (interface{}, error) {
	var member models.Member
	_ = a.ctx.DB.DB().Where("user_id = ?", userId).First(&member).Error
	username := member.UserName
	if username == "" {
		username = userId
	}
	switch action.ActionType {
	case "silences.propose_create":
		var payload types.RequestSilenceCreate
		if err := json.Unmarshal([]byte(action.Payload), &payload); err != nil {
			return nil, err
		}
		payload.TenantId, payload.UpdateBy = tenantId, username
		data, err := SilenceService.Create(&payload)
		if err != nil {
			return nil, err.(error)
		}
		return data, nil
	case "silences.propose_update":
		var payload types.RequestSilenceUpdate
		if err := json.Unmarshal([]byte(action.Payload), &payload); err != nil {
			return nil, err
		}
		payload.TenantId, payload.UpdateBy = tenantId, username
		data, err := SilenceService.Update(&payload)
		if err != nil {
			return nil, err.(error)
		}
		return data, nil
	case "silences.propose_delete":
		var payload types.RequestSilenceQuery
		if err := json.Unmarshal([]byte(action.Payload), &payload); err != nil {
			return nil, err
		}
		payload.TenantId = tenantId
		data, err := SilenceService.Delete(&payload)
		if err != nil {
			return nil, err.(error)
		}
		return data, nil
	case "alerts.propose_claim":
		var payload types.RequestProcessAlertEvent
		if err := json.Unmarshal([]byte(action.Payload), &payload); err != nil {
			return nil, err
		}
		payload.TenantId, payload.Username, payload.Time = tenantId, username, time.Now().Unix()
		data, err := EventService.ProcessAlertEvent(&payload)
		if err != nil {
			return nil, err.(error)
		}
		return data, nil
	default:
		return nil, fmt.Errorf("不支持的确认操作: %s", action.ActionType)
	}
}

func (a *agentService) faultCenterExists(tenantId, id string) bool {
	var count int64
	return a.ctx.DB.DB().Model(&models.FaultCenter{}).Where("tenant_id = ? AND id = ?", tenantId, id).Count(&count).Error == nil && count == 1
}
