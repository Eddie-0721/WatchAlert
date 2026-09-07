package services

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
	"watchAlert/internal/models"
	"watchAlert/internal/types"
)

func (ass alertSilenceService) Preview(req interface{}) (interface{}, interface{}) {
	return ass.preview(req.(*types.RequestSilencePreview))
}

func (ass alertSilenceService) preview(r *types.RequestSilencePreview) (*types.ResponseSilencePreview, error) {
	now := time.Now().Unix()
	if strings.TrimSpace(r.Name) == "" || strings.TrimSpace(r.Comment) == "" {
		return nil, fmt.Errorf("请填写静默名称与处置原因")
	}
	if r.FaultCenterId == "" {
		return nil, fmt.Errorf("必须指定故障中心，不能隐式扩大范围")
	}
	if r.StartsAt <= 0 || r.EndsAt <= r.StartsAt || r.EndsAt <= now || r.EndsAt-r.StartsAt > 30*24*3600 {
		return nil, fmt.Errorf("静默时间无效，持续时间最多 30 天")
	}
	match, err := models.CompileSilenceMatchers(r.Labels)
	if err != nil {
		return nil, err
	}
	var count int64
	if err := ass.ctx.DB.DB().Model(&models.FaultCenter{}).Where("tenant_id = ? AND id = ?", r.TenantId, r.FaultCenterId).Count(&count).Error; err != nil {
		return nil, err
	}
	if count != 1 {
		return nil, fmt.Errorf("故障中心不存在或无权访问")
	}
	var before *models.AlertSilences
	if r.ID != "" {
		before = &models.AlertSilences{}
		if err := ass.ctx.DB.DB().Where("tenant_id = ? AND id = ?", r.TenantId, r.ID).First(before).Error; err != nil {
			return nil, fmt.Errorf("静默规则不存在或无权访问")
		}
	}
	events, err := ass.ctx.Redis.Alert().GetAllEvents(models.BuildAlertEventCacheKey(r.TenantId, r.FaultCenterId))
	if err != nil {
		return nil, fmt.Errorf("读取告警失败，无法确认静默影响范围")
	}
	matches := make([]types.SilencePreviewSample, 0)
	for _, event := range events {
		if event == nil || event.TenantId != r.TenantId || event.FaultCenterId != r.FaultCenterId || event.Status == models.StateRecovered || !match(event.Labels) {
			continue
		}
		matches = append(matches, types.SilencePreviewSample{Fingerprint: event.Fingerprint, RuleName: event.RuleName, Scope: buildAlertScope(event.Labels)})
	}
	sort.Slice(matches, func(i, j int) bool { return matches[i].Fingerprint < matches[j].Fingerprint })
	ids := make([]string, len(matches))
	for i, event := range matches {
		ids[i] = event.Fingerprint
	}
	// This digest is a freshness check, not an authorization token. Permissions
	// are independently enforced for preview AND the actual write endpoint.
	payload, _ := json.Marshal(struct {
		Tenant  string
		Request *types.RequestSilencePreview
		Before  *models.AlertSilences
		IDs     []string
	}{r.TenantId, r, before, ids})
	result := &types.ResponseSilencePreview{PreviewHash: fmt.Sprintf("%x", sha256.Sum256(payload)), PreviewAt: now, Total: len(matches), Samples: matches, Truncated: len(matches) > 5}
	if len(matches) > 5 {
		result.Samples = matches[:5]
	}
	return result, nil
}

func validateSilencePreview(expected string, at int64, current *types.ResponseSilencePreview, now int64) error {
	if expected == "" && at == 0 {
		return nil
	} // Compatibility for existing API clients.
	if expected == "" || at <= 0 || at > now+5 || now-at > 300 {
		return fmt.Errorf("静默预览已过期，请重新预览并确认")
	}
	if expected != current.PreviewHash {
		return fmt.Errorf("静默条件、原配置或匹配告警已变化，请重新预览并确认")
	}
	return nil
}
