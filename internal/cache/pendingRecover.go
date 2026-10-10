package cache

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"strconv"
	"sync"
)

type (
	// PendingRecoverCache 用于管理待恢复的告警事件
	PendingRecoverCache struct {
		rc    *redis.Client
		mutex sync.RWMutex
	}

	// PendingRecoverCacheInterface 定义了待恢复的告警事件缓存的操作接口
	PendingRecoverCacheInterface interface {
		Set(tenantId, ruleId, fingerprint string, time int64) error
		Get(tenantId, ruleId, fingerprint string) (int64, error)
		Delete(tenantId, ruleId, fingerprint string)
		List(tenantId, ruleId string) map[string]int64
		ListWithError(tenantId, ruleId string) (map[string]int64, error)
	}

	PendingRecoverCacheKey string

	PendingRecoverCacheData struct {
		Fingerprint string
		Time        int64
	}
)

// newPendingRecoverCacheInterface 创建一个新的 PendingRecoverCache 实例
func newPendingRecoverCacheInterface(r *redis.Client) PendingRecoverCacheInterface {
	return &PendingRecoverCache{
		rc: r,
	}
}

func (p *PendingRecoverCache) Set(tenantId, ruleId, fingerprint string, time int64) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	return p.rc.HSet(context.Background(), string(BuildPendingRecoverCacheKey(tenantId, ruleId)), fingerprint, time).Err()
}

func (p *PendingRecoverCache) Get(tenantId, ruleId, fingerprint string) (int64, error) {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	return p.rc.HGet(context.Background(), string(BuildPendingRecoverCacheKey(tenantId, ruleId)), fingerprint).Int64()
}

func (p *PendingRecoverCache) Delete(tenantId, ruleId, fingerprint string) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	p.rc.HDel(context.Background(), string(BuildPendingRecoverCacheKey(tenantId, ruleId)), fingerprint)
}

func (p *PendingRecoverCache) List(tenantId, ruleId string) map[string]int64 {
	result, _ := p.ListWithError(tenantId, ruleId)
	return result
}

func (p *PendingRecoverCache) ListWithError(tenantId, ruleId string) (map[string]int64, error) {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	result, err := p.rc.HGetAll(context.Background(), string(BuildPendingRecoverCacheKey(tenantId, ruleId))).Result()
	if err != nil {
		return nil, err
	}

	var newMap = make(map[string]int64)
	for k, v := range result {
		parsed, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid recovery timestamp for fingerprint %s", k)
		}
		newMap[k] = parsed
	}

	return newMap, nil
}

func BuildPendingRecoverCacheKey(tenantId, ruleId string) PendingRecoverCacheKey {
	return PendingRecoverCacheKey(fmt.Sprintf("w8t:%s:pendingRecover:%s.fingerprints", tenantId, ruleId))
}
