package cache

import (
	"fmt"
	"github.com/bytedance/sonic"
	"github.com/go-redis/redis"
	"sync"
	"watchAlert/internal/models"
	"watchAlert/pkg/tools"
)

type (
	// SilenceCache 用于管理告警静默的缓存操作
	SilenceCache struct {
		rc *redis.Client
		sync.RWMutex
	}

	// SilenceCacheInterface 定义了告警静默缓存的操作接口
	SilenceCacheInterface interface {
		PushAlertMute(mute models.AlertSilences) error
		RemoveAlertMute(tenantId, faultCenterId, id string) error
		GetAlertMutes(tenantId, faultCenterId string) ([]string, error)
		ListAlertMutes(tenantId, faultCenterId string) ([]models.AlertSilences, error)
		ListSilenceSnapshots(tenantId, faultCenterId string) ([]SilenceSnapshot, error)
		CompareAndSwapSilenceStatus(SilenceSnapshot, int) (bool, error)
		WithIdGetMuteFromCache(tenantId, faultCenterId, id string) (*models.AlertSilences, error)
	}
)

// newSilenceCacheInterface 创建一个新的 SilenceCache 实例
func newSilenceCacheInterface(r *redis.Client) SilenceCacheInterface {
	return &SilenceCache{
		rc: r,
	}
}

// PushAlertMute 将静默规则推送到故障中心的缓存中
func (sc *SilenceCache) PushAlertMute(mute models.AlertSilences) error {
	sc.Lock()
	defer sc.Unlock()

	key := models.BuildAlertMuteCacheKey(mute.TenantId, mute.FaultCenterId)
	return sc.setRedisHash(key, mute.ID, tools.JsonMarshalToString(mute))
}

// RemoveAlertMute 从故障中心的缓存中移除静默规则
func (sc *SilenceCache) RemoveAlertMute(tenantId, faultCenterId, id string) error {
	sc.Lock()
	defer sc.Unlock()

	key := models.BuildAlertMuteCacheKey(tenantId, faultCenterId)
	return sc.deleteRedisHash(key, id)
}

// ListAlertMutes reads a request-local snapshot with one Redis command.
func (sc *SilenceCache) ListAlertMutes(tenantId, faultCenterId string) ([]models.AlertSilences, error) {
	snapshots, err := sc.ListSilenceSnapshots(tenantId, faultCenterId)
	if err != nil {
		return nil, err
	}
	rules := make([]models.AlertSilences, 0, len(snapshots))
	for _, snapshot := range snapshots {
		rules = append(rules, snapshot.Rule)
	}
	return rules, nil
}

// The original wire value is retained so CAS also works with older JSON field
// order and whitespace. Never re-encode the expected value for comparison.
type SilenceSnapshot struct {
	Rule       models.AlertSilences
	key        models.AlertMuteCacheKey
	field, raw string
}

func (sc *SilenceCache) ListSilenceSnapshots(tenantID, centerID string) ([]SilenceSnapshot, error) {
	key := models.BuildAlertMuteCacheKey(tenantID, centerID)
	mapping, err := sc.getRedisAllHashMap(key)
	if err != nil {
		return nil, err
	}
	snapshots := make([]SilenceSnapshot, 0, len(mapping))
	for field, raw := range mapping {
		var rule models.AlertSilences
		if err := sonic.UnmarshalString(raw, &rule); err != nil {
			return nil, err
		}
		if rule.TenantId != tenantID || rule.FaultCenterId != centerID || rule.ID != field {
			return nil, fmt.Errorf("invalid silence cache identity")
		}
		snapshots = append(snapshots, SilenceSnapshot{Rule: rule, key: key, field: field, raw: raw})
	}
	return snapshots, nil
}

var silenceStatusCAS = redis.NewScript(`
if redis.call('HGET', KEYS[1], ARGV[1]) ~= ARGV[2] then return 0 end
if ARGV[4] == '2' then
  redis.call('HDEL', KEYS[1], ARGV[1])
else
  redis.call('HSET', KEYS[1], ARGV[1], ARGV[3])
end
return 1
`)

func (sc *SilenceCache) CompareAndSwapSilenceStatus(before SilenceSnapshot, status int) (bool, error) {
	if before.key == "" || before.field == "" || before.raw == "" || status < 0 || status > 2 {
		return false, fmt.Errorf("invalid silence transition")
	}
	next := before.Rule
	next.Status = status
	raw, err := sonic.MarshalString(next)
	if err != nil {
		return false, err
	}
	updated, err := silenceStatusCAS.Run(sc.rc, []string{string(before.key)}, before.field, before.raw, raw, status).Int()
	return updated == 1, err
}

func (sc *SilenceCache) GetAlertMutes(tenantId, faultCenterId string) ([]string, error) {
	sc.RLock()
	defer sc.RUnlock()

	key := models.BuildAlertMuteCacheKey(tenantId, faultCenterId)
	mapping, err := sc.getRedisAllHashMap(key)
	if err != nil {
		return nil, err
	}
	var ids []string
	for id := range mapping {
		ids = append(ids, id)
	}
	return ids, nil
}

// WithIdGetMuteFromCache 从缓存中获取静默规则
func (sc *SilenceCache) WithIdGetMuteFromCache(tenantId, faultCenterId, id string) (*models.AlertSilences, error) {
	key := models.BuildAlertMuteCacheKey(tenantId, faultCenterId)
	cache, err := sc.getRedisHash(key, id)
	if err != nil {
		return nil, err
	}

	var mute models.AlertSilences
	if err := sonic.Unmarshal(cache, &mute); err != nil {
		return nil, err
	}

	return &mute, nil
}

// setRedisHash 设置 Redis 哈希表中的值
func (sc *SilenceCache) setRedisHash(key models.AlertMuteCacheKey, field string, value interface{}) error {
	return sc.rc.HSet(string(key), field, value).Err()
}

// deleteRedisHash 删除 Redis 哈希表中的值
func (sc *SilenceCache) deleteRedisHash(key models.AlertMuteCacheKey, field string) error {
	return sc.rc.HDel(string(key), field).Err()
}

// getRedisHash 获取 Redis 哈希表中的值
func (sc *SilenceCache) getRedisHash(key models.AlertMuteCacheKey, field string) ([]byte, error) {
	return sc.rc.HGet(string(key), field).Bytes()
}

// getRedisAllMap 获取 Redis 哈希表Map
func (sc *SilenceCache) getRedisAllHashMap(key models.AlertMuteCacheKey) (map[string]string, error) {
	return sc.rc.HGetAll(string(key)).Result()
}
