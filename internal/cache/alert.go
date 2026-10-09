package cache

import (
	"context"
	"fmt"
	"sync"
	"watchAlert/internal/models"

	"github.com/bytedance/sonic"
	"github.com/go-redis/redis"
	"github.com/zeromicro/go-zero/core/logc"
	"golang.org/x/sync/singleflight"
)

type (
	// AlertCache 用于管理告警事件缓存操作
	AlertCache struct {
		rc *redis.Client
		sync.RWMutex
		indexReady sync.Map
		indexRetry sync.Map
		indexBuild singleflight.Group
	}

	// AlertCacheInterface 定义了事件缓存的操作接口
	AlertCacheInterface interface {
		PushAlertEvent(event *models.AlertCurEvent) error
		ConfirmAlertEvent(context.Context, models.AlertCurEvent, string, int64) (bool, error)
		UpdateNotificationTime(context.Context, models.AlertCurEvent, int64, bool) (bool, error)
		RemoveRecoveredEvent(context.Context, models.AlertCurEvent) (bool, error)
		RemoveAlertEvent(tenantId, faultCenterId, fingerprint string)
		GetFingerprintsByRuleId(tenantId, faultCenterId, ruleId string) []string
		GetAllEvents(key models.AlertEventCacheKey) (map[string]*models.AlertCurEvent, error)
		GetAllEventsContext(context.Context, models.AlertEventCacheKey) (map[string]*models.AlertCurEvent, error)
		GetEventsByFingerprintContext(context.Context, models.AlertEventCacheKey, string) (map[string]*models.AlertCurEvent, error)
		GetRuleEvents(context.Context, models.AlertEventCacheKey, string, string) (map[string]*models.AlertCurEvent, error)
		GetEventFromCache(tenantId, faultCenterId, fingerprint string) (models.AlertCurEvent, error)
	}
)

// newAlertCacheInterface 创建一个新的 AlertCache 实例
func newAlertCacheInterface(r *redis.Client) AlertCacheInterface {
	return &AlertCache{
		rc: r,
	}
}

// PushAlertEvent 将事件推送到故障中心的缓存中
func (a *AlertCache) PushAlertEvent(event *models.AlertCurEvent) error {
	return a.pushEvaluationEvent(event)
}

// RemoveAlertEvent 从故障中心的缓存中移除事件
func (a *AlertCache) RemoveAlertEvent(tenantId, faultCenterId, fingerprint string) {
	key := models.BuildAlertEventCacheKey(tenantId, faultCenterId)
	a.deleteEventCacheHash(key, fingerprint)
}

// GetAllEvents 获取故障中心的所有事件
func (a *AlertCache) GetAllEvents(key models.AlertEventCacheKey) (map[string]*models.AlertCurEvent, error) {
	return a.GetAllEventsContext(context.Background(), key)
}

// Redis v6 cannot interrupt an in-flight socket read through WithContext.
// Check cancellation before I/O, after it, and during decoding instead.
func (a *AlertCache) GetAllEventsContext(ctx context.Context, key models.AlertEventCacheKey) (map[string]*models.AlertCurEvent, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	a.RLock()
	defer a.RUnlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	result, err := a.getEventCacheHashAll(key)
	if err != nil {
		return nil, err
	}
	return decodeEventRows(ctx, result)
}

// Event writers use the fingerprint as the hash field. Exact queries need not
// transfer/decode unrelated rows. Keep the same decoding and filtering semantics
// as full reads; a missing field is an empty selection, not a storage failure.
func (a *AlertCache) GetEventsByFingerprintContext(ctx context.Context, key models.AlertEventCacheKey, fingerprint string) (map[string]*models.AlertCurEvent, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if fingerprint == "" {
		return nil, fmt.Errorf("empty event fingerprint")
	}
	a.RLock()
	defer a.RUnlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	raw, err := a.rc.HGet(string(key), fingerprint).Result()
	if canceled := ctx.Err(); canceled != nil {
		return nil, canceled
	}
	if err == redis.Nil {
		return map[string]*models.AlertCurEvent{}, nil
	}
	if err != nil {
		return nil, err
	}
	return decodeEventRows(ctx, map[string]string{fingerprint: raw})
}

func decodeEventRows(ctx context.Context, result map[string]string) (map[string]*models.AlertCurEvent, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	events := make(map[string]*models.AlertCurEvent, len(result))
	for fingerprint, eventJSON := range result {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var event models.AlertCurEvent
		if err := sonic.UnmarshalString(eventJSON, &event); err != nil {
			logc.Error(context.Background(), fmt.Sprintf("unmarshal event json error: %s, event json: %s", err.Error(), eventJSON))
			continue
		}
		events[fingerprint] = &event
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return events, nil
}

// GetFingerprintsByRuleId 获取与指定规则 ID 相关的指纹列表
func (a *AlertCache) GetFingerprintsByRuleId(tenantId, faultCenterId, ruleId string) []string {
	key := models.BuildAlertEventCacheKey(tenantId, faultCenterId)
	events, err := a.GetRuleEvents(context.Background(), key, tenantId, ruleId)
	if err != nil {
		logc.Error(context.Background(), err.Error())
		return nil
	}

	var fingerprints []string
	for fingerprint, event := range events {
		if event.RuleId == ruleId {
			fingerprints = append(fingerprints, fingerprint)
		}
	}
	return fingerprints
}

// GetEventFromCache 从缓存中获取事件数据
func (a *AlertCache) GetEventFromCache(tenantId, faultCenterId, fingerprint string) (models.AlertCurEvent, error) {
	key := models.BuildAlertEventCacheKey(tenantId, faultCenterId)
	data, err := a.getEventCacheHash(key, fingerprint)
	if err != nil {
		return models.AlertCurEvent{}, err
	}

	var event models.AlertCurEvent
	if err := sonic.Unmarshal([]byte(data), &event); err != nil {
		return models.AlertCurEvent{}, err
	}

	return event, nil
}

// 封装 Redis 操作
func (a *AlertCache) deleteEventCacheHash(key models.AlertEventCacheKey, field string) {
	if err := deleteIndexedEvent.Run(a.rc, []string{string(key), ruleIndexKey(string(key))}, field).Err(); err != nil {
		logc.Errorf(context.Background(), "Delete alert cache failed: %v", err)
	}
}

func (a *AlertCache) getEventCacheHash(key models.AlertEventCacheKey, field string) (string, error) {
	return a.rc.HGet(string(key), field).Result()
}

func (a *AlertCache) getEventCacheHashAll(key models.AlertEventCacheKey) (map[string]string, error) {
	return a.rc.HGetAll(string(key)).Result()
}
