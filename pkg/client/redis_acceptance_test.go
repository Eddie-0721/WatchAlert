package client

import (
	"context"
	"errors"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// Opt-in only. Use a dedicated acceptance instance, never the production Redis.
// No FLUSHDB/KEYS or application keys: each run owns an expiring random prefix.
func TestRedisRealAcceptance(t *testing.T) {
	if os.Getenv("WATCHALERT_REDIS_ACCEPTANCE") != "1" {
		t.Skip("requires explicit opt-in and a dedicated real Redis instance")
	}
	addr := os.Getenv("WATCHALERT_REDIS_TEST_ADDR")
	db, err := strconv.Atoi(os.Getenv("WATCHALERT_REDIS_TEST_DB"))
	if addr == "" || err != nil || db < 0 {
		t.Fatal("explicit WATCHALERT_REDIS_TEST_ADDR and nonnegative WATCHALERT_REDIS_TEST_DB required")
	}
	o := RedisOptions()
	o.Addr, o.DB = addr, db
	o.Username = os.Getenv("WATCHALERT_REDIS_TEST_USER")
	o.Password = os.Getenv("WATCHALERT_REDIS_TEST_PASSWORD")
	r := redis.NewClient(o)
	defer r.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := r.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	key := "w8t:acceptance:" + uuid.NewString()
	defer func() {
		cleanupCtx, stop := context.WithTimeout(context.Background(), 3*time.Second)
		defer stop()
		if err := r.Del(cleanupCtx, key).Err(); err != nil {
			t.Errorf("test-key cleanup failed: %v", err)
		}
	}()
	// MULTI/EXEC initializes the only key with a TTL so interrupted runs expire.
	if _, err := r.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.HSet(ctx, key, "event", "中文告警")
		p.Expire(ctx, key, time.Minute)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if value, err := r.HGet(ctx, key, "event").Result(); err != nil || value != "中文告警" {
		t.Fatalf("hash read: %q %v", value, err)
	}
	cas := redis.NewScript(`if redis.call('HGET', KEYS[1], 'event') == ARGV[1] then return redis.call('HSET', KEYS[1], 'event', ARGV[2]) + 1 else return 0 end`)
	if result, err := cas.Run(ctx, r, []string{key}, "中文告警", "已认领").Int(); err != nil || result != 1 {
		t.Fatalf("Lua CAS: %d %v", result, err)
	}
	if result, err := cas.Run(ctx, r, []string{key}, "中文告警", "不得覆盖").Int(); err != nil || result != 0 {
		t.Fatalf("stale CAS: %d %v", result, err)
	}
	// A concurrent write must invalidate WATCH, not silently overwrite it.
	err = r.Watch(ctx, func(tx *redis.Tx) error {
		if err := r.HSet(ctx, key, "event", "concurrent").Err(); err != nil {
			return err
		}
		_, err := tx.TxPipelined(ctx, func(p redis.Pipeliner) error { p.HSet(ctx, key, "event", "stale"); return nil })
		return err
	}, key)
	if !errors.Is(err, redis.TxFailedErr) {
		t.Fatalf("WATCH conflict was not preserved: %v", err)
	}
	if ttl, err := r.TTL(ctx, key).Result(); err != nil || ttl <= 0 || ttl > time.Minute {
		t.Fatalf("TTL changed: %v %v", ttl, err)
	}
	sub := r.Subscribe(ctx, key)
	defer sub.Close()
	if _, err := sub.ReceiveTimeout(ctx, time.Second); err != nil {
		t.Fatal(err)
	}
	if err := r.Publish(ctx, key, "reload").Err(); err != nil {
		t.Fatal(err)
	}
	msg, err := sub.ReceiveTimeout(ctx, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	message, ok := msg.(*redis.Message)
	if !ok || message.Payload != "reload" {
		t.Fatalf("unexpected PubSub reply: %T", msg)
	}
	canceled, stop := context.WithCancel(ctx)
	stop()
	if err := r.Ping(canceled).Err(); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled command: %v", err)
	}
	if err := r.Ping(ctx).Err(); err != nil {
		t.Fatal("client not usable after cancellation", err)
	}
}
