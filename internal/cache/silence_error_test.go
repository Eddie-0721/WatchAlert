package cache

import (
	"github.com/redis/go-redis/v9"
	"testing"
	"watchAlert/internal/models"
)

func TestSilenceWritesReturnRedisErrors(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", MaxRetries: 0})
	_ = client.Close()
	target := newSilenceCacheInterface(client)
	if target.PushAlertMute(models.AlertSilences{TenantId: "t", ID: "s", FaultCenterId: "fc"}) == nil {
		t.Fatal("HSET error swallowed")
	}
	if target.RemoveAlertMute("t", "fc", "s") == nil {
		t.Fatal("HDEL error swallowed")
	}
	if newAlertCacheInterface(client).PushAlertEvent(&models.AlertCurEvent{TenantId: "t", FaultCenterId: "fc", Fingerprint: "fp"}) == nil {
		t.Fatal("alert HSET error swallowed")
	}
}
