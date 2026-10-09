package services

import (
	"context"
	"errors"
	"testing"
	"watchAlert/internal/cache"
	appctx "watchAlert/internal/ctx"
	"watchAlert/internal/models"
	"watchAlert/internal/types"
	"watchAlert/pkg/agenttoken"
)

type cancelingEventCache struct {
	cache.AlertCacheInterface
	cancel context.CancelFunc
	calls  int
}

func (c *cancelingEventCache) GetAllEventsContext(ctx context.Context, key models.AlertEventCacheKey) (map[string]*models.AlertCurEvent, error) {
	c.calls++
	if _, ok := ctx.Deadline(); !ok {
		panic("missing query budget")
	}
	c.cancel()
	return map[string]*models.AlertCurEvent{"fp": {TenantId: "t", FaultCenterId: "fc", Fingerprint: "fp", Status: models.StateAlerting}}, nil
}

type cancelingCacheEntry struct {
	cache.InterEntryCache
	alerts *cancelingEventCache
}

func (c cancelingCacheEntry) Alert() cache.AlertCacheInterface { return c.alerts }

func TestCurrentEventCancellationDoesNotReturnPartialList(t *testing.T) {
	query := &types.RequestAlertCurEventQuery{TenantId: "t", IncludeSummary: true, Page: models.Page{Index: 1, Size: 30}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	// No dependencies: an already canceled request must not touch SQL/Redis.
	data, failure := (eventService{}).ListCurrentEventContext(ctx, query)
	if data != nil || failure != context.Canceled {
		t.Fatal(data, failure)
	}
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	alerts := &cancelingEventCache{cancel: cancel}
	svc := eventService{ctx: &appctx.Context{DB: eventFixtureDB{}, Redis: cancelingCacheEntry{alerts: alerts}}}
	data, failure = svc.ListCurrentEventContext(ctx, query)
	if data != nil || failure != context.Canceled || alerts.calls != 1 {
		t.Fatal("cancellation continued scanning", data, failure, alerts.calls)
	}
}

func TestAgentAlertQueriesForwardCancellation(t *testing.T) {
	previous := EventService
	EventService = &eventService{}
	t.Cleanup(func() { EventService = previous })
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	svc := &agentToolService{}
	for _, query := range []func(context.Context, map[string]interface{}, agenttoken.Claims) (interface{}, error){svc.searchAlerts, svc.getAlert, svc.relatedAlerts} {
		data, err := query(ctx, map[string]interface{}{"fingerprint": "fp"}, agenttoken.Claims{TenantId: "t"})
		if data != nil || !errors.Is(err, context.Canceled) {
			t.Fatal("Agent discarded cancellation", data, err)
		}
	}
}
