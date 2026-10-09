package services

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"watchAlert/config"
	"watchAlert/internal/cache"
	appctx "watchAlert/internal/ctx"
	"watchAlert/internal/models"
	"watchAlert/internal/types"
	"watchAlert/pkg/client"

	"github.com/alicebob/miniredis/v2"
)

// Exercises the actual go-redis client and production JSON/cache adapters. The
// server is miniredis and the center list is a fixture, not production Redis/SQL.
func redisEventFixture(t testing.TB, n int) (eventService, eventService, *miniredis.Miniredis) {
	t.Helper()
	server, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	previousConfig, previousClient := config.Application.Redis, client.Redis
	t.Cleanup(func() { config.Application.Redis = previousConfig; client.Redis = previousClient; server.Close() })
	config.Application.Redis = config.Redis{Host: server.Host(), Port: server.Port()}
	entry := cache.NewEntryCache()
	t.Cleanup(func() { entry.Redis().Close() })
	reference := eventBenchmarkFixture(n, 100)
	fixture := reference.ctx.Redis.(benchmarkEventCache)
	for key, group := range fixture.alerts.events {
		for fp, event := range group {
			raw, err := json.Marshal(event)
			if err != nil {
				t.Fatal(err)
			}
			server.HSet(string(key), fp, string(raw))
		}
	}
	for _, center := range []string{"fc", "fc2"} {
		for _, rule := range fixture.rules {
			rule.TenantId = "t"
			rule.FaultCenterId = center
			raw, err := json.Marshal(rule)
			if err != nil {
				t.Fatal(err)
			}
			server.HSet(string(models.BuildAlertMuteCacheKey("t", center)), rule.ID, string(raw))
		}
	}
	return eventService{ctx: &appctx.Context{DB: eventFixtureDB{}, Redis: entry}}, reference, server
}

func TestCurrentEventRedisPipelineReadsAllCentersOnce(t *testing.T) {
	service, reference, server := redisEventFixture(t, 1000)
	for _, queue := range []string{"all", "attention", "suppressed"} {
		query := &types.RequestAlertCurEventQuery{TenantId: "t", Queue: queue, IncludeSummary: true, Page: models.Page{Index: 2, Size: 30}}
		before := server.CommandCount()
		data, err := service.ListCurrentEvent(query)
		if err != nil {
			t.Fatal(err)
		}
		if calls := server.CommandCount() - before; calls != 4 {
			t.Fatalf("expected two event + two silence HGETALL calls, got %d", calls)
		}
		if !reflect.DeepEqual(data, referenceCurrentList(reference, query)) {
			t.Fatal("real cache pipeline changed list semantics")
		}
	}
	// A full first-page heap must not stop scanning or hide later center failures.
	key := string(models.BuildAlertEventCacheKey("t", "fc2"))
	server.Del(key)
	server.Set(key, "wrong-type")
	value, err := service.ListCurrentEvent(&types.RequestAlertCurEventQuery{TenantId: "t", Page: models.Page{Index: 1, Size: 1}})
	if err == nil || value != nil {
		t.Fatal("returned an apparently complete page despite a late read failure")
	}
}

func BenchmarkCurrentEventListRedis(b *testing.B) {
	for _, n := range []int{1000, 10000} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			service, _, _ := redisEventFixture(b, n)
			query := &types.RequestAlertCurEventQuery{TenantId: "t", Queue: "all", IncludeSummary: true, Page: models.Page{Index: 1, Size: 30}}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := service.ListCurrentEvent(query); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkExactCurrentEventRedis(b *testing.B) {
	for _, n := range []int{1000, 10000} {
		for _, center := range []string{"fc", ""} {
			b.Run(fmt.Sprintf("events=%d/center=%s", n, center), func(b *testing.B) {
				service, _, _ := redisEventFixture(b, n)
				query := &types.RequestAlertCurEventQuery{TenantId: "t", FaultCenterId: center, Fingerprint: "fp-00000002", IncludeRecovered: true, Page: models.Page{Index: 1, Size: 2}}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					data, err := service.ListCurrentEvent(query)
					if err != nil || data.(types.ResponseAlertCurEventList).Total != 1 {
						b.Fatal(data, err)
					}
				}
			})
		}
	}
}
