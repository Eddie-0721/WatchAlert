package cache

import (
	"watchAlert/pkg/client"

	"github.com/go-redis/redis"
)

type (
	entryCache struct {
		redis    *redis.Client
		provider *ProviderPoolStore
		alert    AlertCacheInterface
	}

	InterEntryCache interface {
		Redis() *redis.Client
		Silence() SilenceCacheInterface
		Alert() AlertCacheInterface
		ProviderPools() *ProviderPoolStore
		FaultCenter() FaultCenterCacheInterface
		PendingRecover() PendingRecoverCacheInterface
	}
)

func NewEntryCache() InterEntryCache {
	r := client.InitRedis()
	p := NewClientPoolStore()

	return &entryCache{
		redis:    r,
		provider: p,
		alert:    newAlertCacheInterface(r),
	}
}

func (e entryCache) Redis() *redis.Client              { return e.redis }
func (e entryCache) Silence() SilenceCacheInterface    { return newSilenceCacheInterface(e.redis) }
func (e entryCache) Alert() AlertCacheInterface        { return e.alert }
func (e entryCache) ProviderPools() *ProviderPoolStore { return e.provider }
func (e entryCache) FaultCenter() FaultCenterCacheInterface {
	return newFaultCenterCacheInterface(e.redis)
}
func (e entryCache) PendingRecover() PendingRecoverCacheInterface {
	return newPendingRecoverCacheInterface(e.redis)
}
