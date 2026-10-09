package services

import (
	"context"
	"time"
	"watchAlert/internal/models"

	"gorm.io/gorm"
)

const agentDatabaseTimeout = 10 * time.Second

func (a *agentService) saveAgentUserMessage(ctx context.Context, message *models.AgentMessage) error {
	ctx, cancel := context.WithTimeout(ctx, agentDatabaseTimeout)
	defer cancel()
	return a.ctx.DB.DB().WithContext(ctx).Create(message).Error
}

// No transaction/connection is held during the model call. Commit the reply
// and session metadata together before reporting success to either API client.
func (a *agentService) saveAgentReply(ctx context.Context, message *models.AgentMessage, title string) error {
	ctx, cancel := context.WithTimeout(ctx, agentDatabaseTimeout)
	defer cancel()
	return a.ctx.DB.DB().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(message).Error; err != nil {
			return err
		}
		return tx.Model(&models.AgentSession{}).Where("id = ? AND tenant_id = ?", message.SessionId, message.TenantId).
			Updates(map[string]interface{}{"updated_at": message.CreatedAt, "title": title}).Error
	})
}
