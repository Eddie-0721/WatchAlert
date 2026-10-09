package services

import (
	"context"
	"watchAlert/internal/models"
	"watchAlert/internal/types"
)

// Only the model's working history is bounded here. The conversation view
// keeps its existing API and historical evidence remains stored unchanged.
func (a *agentService) loadRunHistory(ctx context.Context, tenantID, userID, sessionID string) (types.ResponseAgentSessionDetail, error) {
	ctx, cancel := context.WithTimeout(ctx, agentDatabaseTimeout)
	defer cancel()
	var result types.ResponseAgentSessionDetail
	db := a.ctx.DB.DB().WithContext(ctx)
	sessionQuery := db.Where("id = ? AND tenant_id = ?", sessionID, tenantID)
	if userID != "admin" {
		sessionQuery = sessionQuery.Where("user_id = ?", userID)
	}
	if err := sessionQuery.First(&result.Session).Error; err != nil {
		return result, err
	}
	var recent []models.AgentMessage
	// SUBSTR is supported by both deployed database drivers. Do not fetch large
	// evidence payloads or unlimited historical content just to discard them.
	if err := db.Select("id, role, SUBSTR(content, 1, 8001) AS content").
		Where("session_id = ? AND tenant_id = ?", sessionID, tenantID).
		Order("created_at desc").Order("id desc").Limit(11).Find(&recent).Error; err != nil {
		return result, err
	}
	remaining := 24000
	for _, message := range recent {
		if remaining <= 0 {
			break
		}
		content := []rune(message.Content)
		limit := 8000
		if remaining < limit {
			limit = remaining
		}
		if len(content) > limit {
			content = content[:limit]
			message.Content = string(content) + "\n[历史消息已截断]"
		}
		remaining -= len(content)
		result.Messages = append(result.Messages, message)
	}
	for i, j := 0, len(result.Messages)-1; i < j; i, j = i+1, j-1 {
		result.Messages[i], result.Messages[j] = result.Messages[j], result.Messages[i]
	}
	return result, nil
}
