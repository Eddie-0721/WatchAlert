package services

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"watchAlert/internal/cache"
	appctx "watchAlert/internal/ctx"
	"watchAlert/internal/models"
	"watchAlert/internal/types"
)

// Retain the old read/materialize/count path as an independent oracle and local
// benchmark baseline. Do not replace it with the new counter under test.
func referenceCenterStatistics(service faultCenterService, request *types.RequestFaultCenterQuery) ([]models.FaultCenter, error) {
	centers, err := service.ctx.DB.FaultCenter().List(request.TenantId, request.Query)
	if err != nil {
		return nil, err
	}
	for i, center := range centers {
		events, err := service.ctx.Redis.Alert().GetAllEvents(models.BuildAlertEventCacheKey(center.TenantId, center.ID))
		if err != nil {
			return nil, err
		}
		for _, event := range events {
			switch event.Status {
			case models.StatePreAlert:
				centers[i].CurrentPreAlertNumber++
			case models.StateAlerting:
				centers[i].CurrentAlertNumber++
			case models.StatePendingRecovery:
				centers[i].CurrentRecoverNumber++
			}
		}
	}
	return centers, nil
}

func TestCenterStatisticsMatchesLegacyAndRejectsPartialResults(t *testing.T) {
	events, _, server := redisEventFixture(t, 1000)
	service := faultCenterService{ctx: &appctx.Context{DB: optionDB{}, Redis: events.ctx.Redis}}
	request := &types.RequestFaultCenterQuery{TenantId: "t"}
	server.HSet(string(models.BuildAlertEventCacheKey("t", "fc")), "pending", `{"status":"pending_recovery"}`)
	want, err := referenceCenterStatistics(service, request)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		actual, err := service.ListContext(context.Background(), request)
		if err != nil || !reflect.DeepEqual(actual, want) {
			t.Fatal("center statistics changed", actual, want, err)
		}
	}
	// Failure of a later center must discard already computed first-center data.
	key := string(models.BuildAlertEventCacheKey("t", "fc2"))
	server.Del(key)
	server.Set(key, "wrong-type")
	if data, err := service.ListContext(context.Background(), request); data != nil || err == nil {
		t.Fatal("late storage failure returned partial statistics", data, err)
	}
}

type cancelingCenterCounts struct {
	cache.AlertCacheInterface
	cancel context.CancelFunc
	calls  int
	onCall int
}

func (c *cancelingCenterCounts) CountEventStates(ctx context.Context, _ models.AlertEventCacheKey) (cache.EventStateCounts, error) {
	if err := ctx.Err(); err != nil {
		return cache.EventStateCounts{}, err
	}
	c.calls++
	if c.calls == c.onCall {
		c.cancel()
	}
	return cache.EventStateCounts{Alerting: 3}, nil
}

type centerCountsCache struct {
	cache.InterEntryCache
	counts *cancelingCenterCounts
}

func (c centerCountsCache) Alert() cache.AlertCacheInterface { return c.counts }

func TestCenterStatisticsCancellationDiscardsResults(t *testing.T) {
	for _, onCall := range []int{0, 1, 2} {
		t.Run(fmt.Sprint(onCall), func(t *testing.T) {
			requestCtx, cancel := context.WithCancel(context.Background())
			defer cancel()
			counts := &cancelingCenterCounts{cancel: cancel, onCall: onCall}
			service := faultCenterService{ctx: &appctx.Context{DB: optionDB{}, Redis: centerCountsCache{counts: counts}}}
			if onCall == 0 {
				cancel()
			}
			data, err := service.ListContext(requestCtx, &types.RequestFaultCenterQuery{TenantId: "t"})
			if data != nil || err == nil || !errors.Is(err.(error), context.Canceled) || counts.calls != onCall {
				t.Fatal("canceled request returned partial statistics or kept scanning", data, err, counts.calls)
			}
		})
	}
}

func BenchmarkCenterStatistics(b *testing.B) {
	for _, n := range []int{1000, 10000} {
		for _, retained := range []bool{true, false} {
			b.Run(fmt.Sprintf("events=%d/retained=%t", n, retained), func(b *testing.B) {
				events, _, _ := redisEventFixture(b, n)
				service := faultCenterService{ctx: &appctx.Context{DB: optionDB{}, Redis: events.ctx.Redis}}
				request := &types.RequestFaultCenterQuery{TenantId: "t"}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					var err interface{}
					if retained {
						_, err = referenceCenterStatistics(service, request)
					} else {
						_, err = service.ListContext(context.Background(), request)
					}
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
