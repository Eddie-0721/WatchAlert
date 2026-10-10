package cache

import (
	"github.com/redis/go-redis/v9"
	"testing"
)

func TestRecoverySnapshotPropagatesRedisFailure(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	_ = client.Close()
	if _, err := newPendingRecoverCacheInterface(client).ListWithError("t", "r"); err == nil {
		t.Fatal("unavailable snapshot treated as empty")
	}
}
