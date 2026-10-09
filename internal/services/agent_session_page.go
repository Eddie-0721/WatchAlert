package services

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"
	"watchAlert/internal/models"
	"watchAlert/internal/types"
)

type agentMessageCursor struct {
	Session string `json:"s"`
	Time    int64  `json:"t"`
	ID      string `json:"id"`
}

// GetSession bounds browser history independently of the model's context budget.
// A stable keyset avoids OFFSET scans and duplicates when newer replies arrive.
func (a *agentService) GetSession(ctx context.Context, tenantID, userID string, req *types.RequestAgentSessionQuery) (types.ResponseAgentSessionDetail, error) {
	result := types.ResponseAgentSessionDetail{Messages: []models.AgentMessage{}}
	if req == nil || req.SessionId == "" || req.Limit < 0 || req.Limit > 100 {
		return result, fmt.Errorf("无效的会话分页参数，每页最多 100 条")
	}
	limit := req.Limit
	if limit == 0 {
		limit = 50
	}
	var cursor agentMessageCursor
	if req.Before != "" {
		if len(req.Before) > 512 {
			return result, fmt.Errorf("无效的会话分页游标")
		}
		decoded, err := base64.RawURLEncoding.DecodeString(req.Before)
		if err != nil || json.Unmarshal(decoded, &cursor) != nil || cursor.Session != req.SessionId || cursor.ID == "" || len(cursor.ID) > 64 || cursor.Time < 0 {
			return result, fmt.Errorf("无效的会话分页游标")
		}
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	db := a.ctx.DB.DB().WithContext(ctx)
	owner := db.Where("id = ? AND tenant_id = ?", req.SessionId, tenantID)
	if userID != "admin" {
		owner = owner.Where("user_id = ?", userID)
	}
	if err := owner.First(&result.Session).Error; err != nil {
		return result, err
	}
	query := db.Where("session_id = ? AND tenant_id = ?", req.SessionId, tenantID)
	if req.Before != "" {
		query = query.Where("(created_at < ? OR (created_at = ? AND id < ?))", cursor.Time, cursor.Time, cursor.ID)
	}
	if err := query.Order("created_at desc").Order("id desc").Limit(limit + 1).Find(&result.Messages).Error; err != nil {
		return result, err
	}
	result.HasMore = len(result.Messages) > limit
	if result.HasMore {
		result.Messages = result.Messages[:limit]
		oldest := result.Messages[len(result.Messages)-1]
		encoded, _ := json.Marshal(agentMessageCursor{Session: req.SessionId, Time: oldest.CreatedAt, ID: oldest.ID})
		result.NextCursor = base64.RawURLEncoding.EncodeToString(encoded)
	}
	for l, r := 0, len(result.Messages)-1; l < r; l, r = l+1, r-1 {
		result.Messages[l], result.Messages[r] = result.Messages[r], result.Messages[l]
	}
	// Only load current statuses for action IDs actually referenced on this page.
	// Keep nested evidence opaque: decoding numbers through interface{} loses
	// precision and needlessly allocates every query result/preview field.
	evidence := make([][]map[string]json.RawMessage, len(result.Messages))
	ids := []string{}
	seen := map[string]bool{}
	for i, message := range result.Messages {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		if json.Unmarshal([]byte(message.Evidence), &evidence[i]) != nil {
			evidence[i] = nil
			continue
		}
		for _, item := range evidence[i] {
			if id := agentEvidenceString(item["actionId"]); id != "" && !seen[id] {
				ids = append(ids, id)
				seen[id] = true
			}
		}
	}
	statuses := map[string]string{}
	now := time.Now().Unix()
	for start := 0; start < len(ids); start += 100 {
		end := start + 100
		if end > len(ids) {
			end = len(ids)
		}
		var actions []models.AgentPendingAction
		if err := db.Select("id", "status", "expires_at").Where("session_id = ? AND tenant_id = ? AND id IN ?", req.SessionId, tenantID, ids[start:end]).Find(&actions).Error; err != nil {
			return result, err
		}
		for _, action := range actions {
			status := action.Status
			if status == "pending_confirmation" && action.ExpiresAt <= now {
				status = "expired"
			}
			statuses[action.ID] = status
		}
	}
	for i, items := range evidence {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		if items == nil {
			continue
		}
		changed := false
		for _, item := range items {
			if status := statuses[agentEvidenceString(item["actionId"])]; status != "" && agentEvidenceString(item["status"]) != status {
				encoded, err := json.Marshal(status)
				if err != nil {
					return result, err
				}
				item["status"] = encoded
				changed = true
			}
		}
		// Preserve the original bytes when there is no authoritative status change.
		if changed {
			encoded, err := json.Marshal(items)
			if err != nil {
				return result, err
			}
			result.Messages[i].Evidence = string(encoded)
		}
	}
	return result, ctx.Err()
}

func agentEvidenceString(raw json.RawMessage) string {
	var value string
	if json.Unmarshal(raw, &value) != nil {
		return ""
	}
	return value
}
