package cache

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"watchAlert/internal/models"
	"watchAlert/internal/testutil"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestExactEventReadMatchesFullDecode(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { client.Close() })
	cache := &AlertCache{rc: client}
	key := models.BuildAlertEventCacheKey("t", "fc")
	server.HSet(string(key), "wanted", `{"tenantId":"t","faultCenterId":"fc","fingerprint":"wanted","labels":{"env":"prod"}}`)
	server.HSet(string(key), "other", `{"fingerprint":"other"}`)
	server.HSet(string(key), "invalid", `not-json`)
	all, err := cache.GetAllEventsContext(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	for _, fp := range []string{"wanted", "other", "invalid", "missing"} {
		selected, err := cache.GetEventsByFingerprintContext(context.Background(), key, fp)
		if err != nil {
			t.Fatal(err)
		}
		want := map[string]*models.AlertCurEvent{}
		if row, ok := all[fp]; ok {
			want[fp] = row
		}
		if !reflect.DeepEqual(selected, want) {
			t.Fatal("exact decoding diverged", fp, selected, want)
		}
	}
	before := server.CommandCount()
	if _, err := cache.GetEventsByFingerprintContext(context.Background(), key, ""); err == nil || server.CommandCount() != before {
		t.Fatal("empty fingerprint queried storage")
	}
	server.Del(string(key))
	server.Set(string(key), "wrong-type")
	if rows, err := cache.GetEventsByFingerprintContext(context.Background(), key, "wanted"); err == nil || rows != nil {
		t.Fatal("storage failure reported as missing")
	}
}

func TestExactEventReadCancellation(t *testing.T) {
	for _, phase := range []string{"before", "after-found", "after-missing"} {
		t.Run(phase, func(t *testing.T) {
			server := miniredis.RunT(t)
			client := redis.NewClient(&redis.Options{Addr: server.Addr()})
			t.Cleanup(func() { client.Close() })
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			key := models.BuildAlertEventCacheKey("t", "fc")
			if phase == "after-found" {
				server.HSet(string(key), "fp", `{"fingerprint":"fp"}`)
			}
			if phase == "before" {
				cancel()
			} else {
				testutil.WrapRedisProcess(client, func(next func(redis.Cmder) error) func(redis.Cmder) error {
					return func(cmd redis.Cmder) error { err := next(cmd); cancel(); return err }
				})
			}
			rows, err := (&AlertCache{rc: client}).GetEventsByFingerprintContext(ctx, key, "fp")
			if rows != nil || !errors.Is(err, context.Canceled) {
				t.Fatal("canceled exact lookup succeeded", rows, err)
			}
			if phase == "before" && server.CommandCount() != 0 {
				t.Fatal("canceled request touched Redis")
			}
		})
	}
}
