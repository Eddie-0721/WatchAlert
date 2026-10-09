package services

import (
	"fmt"
	"os"
	"sort"
	"testing"
	"time"
	"watchAlert/internal/cache"
	appctx "watchAlert/internal/ctx"
	"watchAlert/internal/models"
	"watchAlert/internal/types"
)

type benchmarkSilences struct {
	cache.SilenceCacheInterface
	rules []models.AlertSilences
}

func (s benchmarkSilences) ListAlertMutes(string, string) ([]models.AlertSilences, error) {
	return s.rules, nil
}

type benchmarkEventCache struct {
	eventFixtureCache
	rules []models.AlertSilences
}

func (c benchmarkEventCache) Silence() cache.SilenceCacheInterface {
	return benchmarkSilences{rules: c.rules}
}

// Predecoded, deterministic fixture: isolates list CPU/allocation from Redis/SQL.
// It intentionally does not pretend these timings are production request latency.
func eventBenchmarkFixture(n, silenceCount int) eventService {
	groups := map[models.AlertEventCacheKey]map[string]*models.AlertCurEvent{}
	for _, center := range []string{"fc", "fc2"} {
		groups[models.BuildAlertEventCacheKey("t", center)] = map[string]*models.AlertCurEvent{}
	}
	for i := 0; i < n; i++ {
		center := "fc"
		if i%2 == 1 {
			center = "fc2"
		}
		fp := fmt.Sprintf("fp-%08d", i)
		labels := map[string]interface{}{"env": "prod", "service": fmt.Sprintf("service-%d", i%200), "cluster": "cn-prod", "namespace": "payments", "instance": fmt.Sprintf("10.0.%d.%d", i/256, i%256), "owner": "platform"}
		for k := 0; k < 14; k++ {
			labels[fmt.Sprintf("extra_%d", k)] = fmt.Sprintf("value-%d", i+k)
		}
		status := models.StateAlerting
		if i%17 == 0 {
			status = models.StatePreAlert
		}
		if i%31 == 0 {
			status = models.StateRecovered
		}
		groups[models.BuildAlertEventCacheKey("t", center)][fp] = &models.AlertCurEvent{TenantId: "t", FaultCenterId: center, Fingerprint: fp, RuleId: fmt.Sprintf("rule-%d", i%100), RuleName: "Service request latency", Annotations: "p95 latency above threshold", Status: status, Labels: labels, FirstTriggerTime: 1700000000 + int64(i%1000), LastEvalTime: 1700003600 + int64(i%37), ConfirmState: models.ConfirmState{IsOk: i%7 == 0}}
	}
	rules := make([]models.AlertSilences, silenceCount)
	for i := range rules {
		rules[i] = models.AlertSilences{ID: fmt.Sprint(i), Status: 1, StartsAt: 1, EndsAt: 1 << 62, Labels: []models.SilenceLabel{{Key: "service", Operator: "=", Value: fmt.Sprintf("service-%d", i)}}}
	}
	return eventService{ctx: &appctx.Context{DB: eventFixtureDB{}, Redis: benchmarkEventCache{eventFixtureCache: eventFixtureCache{alerts: eventFixtureAlerts{events: groups}}, rules: rules}}}
}

func BenchmarkCurrentEventList(b *testing.B) {
	for _, n := range []int{1000, 10000} {
		for _, queue := range []string{"all", "attention"} {
			b.Run(fmt.Sprintf("events=%d/silences=100/queue=%s", n, queue), func(b *testing.B) {
				service := eventBenchmarkFixture(n, 100)
				query := &types.RequestAlertCurEventQuery{TenantId: "t", Queue: queue, IncludeSummary: true, Page: models.Page{Index: 1, Size: 30}}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					value, err := service.ListCurrentEvent(query)
					if err != nil || len(value.(types.ResponseAlertCurEventList).List) != 30 {
						b.Fatal(value, err)
					}
				}
			})
		}
	}
}

func TestCurrentEventLatencyDistribution(t *testing.T) {
	if os.Getenv("WATCHALERT_PERF_SAMPLES") != "1" {
		t.Skip("explicit local CPU latency sampling only")
	}
	for _, n := range []int{1000, 10000} {
		service := eventBenchmarkFixture(n, 100)
		query := &types.RequestAlertCurEventQuery{TenantId: "t", Queue: "all", IncludeSummary: true, Page: models.Page{Index: 1, Size: 30}}
		for i := 0; i < 5; i++ {
			if _, err := service.ListCurrentEvent(query); err != nil {
				t.Fatal(err)
			}
		}
		values := make([]time.Duration, 100)
		for i := range values {
			start := time.Now()
			if _, err := service.ListCurrentEvent(query); err != nil {
				t.Fatal(err)
			}
			values[i] = time.Since(start)
		}
		sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
		t.Logf("predecoded CPU only: events=%d silences=100 samples=100 p50=%s p95=%s", n, values[49], values[94])
	}
}
