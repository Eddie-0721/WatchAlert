package services

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
	"watchAlert/internal/cache"
	appctx "watchAlert/internal/ctx"
	"watchAlert/internal/models"
	"watchAlert/internal/types"
)

type countedSilences struct {
	cache.SilenceCacheInterface
	calls   map[string]int
	err     error
	now     int64
	invalid bool
}

func (s *countedSilences) ListAlertMutesContext(_ context.Context, tenant, center string) ([]models.AlertSilences, error) {
	return s.ListAlertMutes(tenant, center)
}

func (s *countedSilences) ListAlertMutes(tenant, center string) ([]models.AlertSilences, error) {
	s.calls[tenant+"/"+center]++
	if s.invalid {
		return []models.AlertSilences{{Status: 1, StartsAt: s.now - 10, EndsAt: s.now + 100}}, nil
	}
	return []models.AlertSilences{{Status: 1, StartsAt: s.now - 10, EndsAt: s.now + 100,
		Labels: []models.SilenceLabel{{Key: "env", Operator: "=~", Value: "^prod$"}}}}, s.err
}

type performanceCache struct {
	eventFixtureCache
	silences *countedSilences
}

func (c performanceCache) Silence() cache.SilenceCacheInterface { return c.silences }

func TestEventListReadsSilencesOncePerCenterBeforePagination(t *testing.T) {
	events := make(map[models.AlertEventCacheKey]map[string]*models.AlertCurEvent)
	for _, center := range []string{"fc", "fc2"} {
		group := make(map[string]*models.AlertCurEvent)
		for i := 0; i < 500; i++ {
			fp := fmt.Sprint(i)
			env := "prod"
			if i%2 == 0 {
				env = "test"
			}
			group[fp] = &models.AlertCurEvent{TenantId: "t", FaultCenterId: center, Fingerprint: fp, Status: models.StateAlerting, Labels: map[string]interface{}{"env": env}}
		}
		events[models.BuildAlertEventCacheKey("t", center)] = group
	}
	mutes := &countedSilences{calls: map[string]int{}, now: time.Now().Unix()}
	caches := performanceCache{eventFixtureCache: eventFixtureCache{alerts: eventFixtureAlerts{events: events}}, silences: mutes}
	service := eventService{ctx: &appctx.Context{DB: eventFixtureDB{}, Redis: caches}}
	query := &types.RequestAlertCurEventQuery{TenantId: "t", Queue: "suppressed", IncludeSummary: true, Page: models.Page{Index: 1, Size: 30}}
	value, err := service.ListCurrentEvent(query)
	if err != nil {
		t.Fatal(err)
	}
	result := value.(types.ResponseAlertCurEventList)
	if result.Total != 500 || len(result.List) != 30 || result.Summary.Queues["all"] != 1000 {
		t.Fatalf("incorrect paginated result: %+v", result.Page)
	}
	for _, center := range []string{"fc", "fc2"} {
		if mutes.calls["t/"+center] != 1 {
			t.Fatal("silence snapshot not reused", mutes.calls)
		}
	}
	// A new request must not reuse stale rules; failures cannot look unsilenced.
	mutes.err = errors.New("redis unavailable")
	if _, err := service.ListCurrentEvent(query); err == nil {
		t.Fatal("silence read failure hidden")
	}
	mutes.err = nil
	mutes.invalid = true
	if _, err := service.ListCurrentEvent(query); err == nil {
		t.Fatal("invalid active silence reported as unmuted")
	}
}
