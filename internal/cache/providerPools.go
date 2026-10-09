package cache

import (
	"context"
	"fmt"
	"github.com/zeromicro/go-zero/core/logc"
	"io"
	"sync"
)

// ProviderPoolStore 提供商客户端存储池
type ProviderPoolStore struct {
	clients map[string]*providerEntry
	mux     sync.RWMutex
}

type providerEntry struct {
	client  interface{}
	users   int
	retired bool
}

// NewClientPoolStore 创建一个新的 ProviderPoolStore 实例
func NewClientPoolStore() *ProviderPoolStore {
	return &ProviderPoolStore{
		clients: make(map[string]*providerEntry),
	}
}

// SetClient takes ownership of a newly constructed client. Replacements are
// retired immediately, but close only after their final in-flight lease ends.
func (p *ProviderPoolStore) SetClient(key string, client interface{}) {
	p.mux.Lock()
	old := p.clients[key]
	p.clients[key] = &providerEntry{client: client}
	closeOld := retireProvider(old)
	p.mux.Unlock()
	if closeOld {
		closeProvider(old.client)
	}
}

// AcquireClient returns an idempotent release function, including on error.
// Every successful acquisition must be released after its last client use.
func (p *ProviderPoolStore) AcquireClient(key string) (interface{}, func(), error) {
	p.mux.Lock()
	entry, exists := p.clients[key]
	if !exists {
		p.mux.Unlock()
		return nil, func() {}, fmt.Errorf("获取客户端错误, 客户端在缓存中不存在, datasourceId: %s", key)
	}
	entry.users++
	p.mux.Unlock()
	var once sync.Once
	return entry.client, func() {
		once.Do(func() {
			p.mux.Lock()
			entry.users--
			closeOld := entry.retired && entry.users == 0
			p.mux.Unlock()
			if closeOld {
				closeProvider(entry.client)
			}
		})
	}, nil
}

// RemoveClient 移除通用客户端
func (p *ProviderPoolStore) RemoveClient(key string) {
	p.mux.Lock()
	old := p.clients[key]
	delete(p.clients, key)
	closeOld := retireProvider(old)
	p.mux.Unlock()
	if closeOld {
		closeProvider(old.client)
	}
}

// Called under the pool mutex. A retired entry is no longer in the map.
func retireProvider(entry *providerEntry) bool {
	if entry == nil {
		return false
	}
	entry.retired = true
	return entry.users == 0
}

func closeProvider(client interface{}) {
	if closer, ok := client.(io.Closer); ok {
		if err := closer.Close(); err != nil {
			logc.Error(context.Background(), "Failed to close retired datasource client")
		}
	}
}
