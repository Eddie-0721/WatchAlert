package services

import (
	"testing"
	"watchAlert/internal/cache"
	appctx "watchAlert/internal/ctx"
	"watchAlert/internal/models"
	"watchAlert/internal/repo"
	"watchAlert/internal/types"
	"watchAlert/pkg/agenttoken"
)

type eventFixtureCenters struct{ repo.InterFaultCenterRepo }

func (eventFixtureCenters) List(string, string) ([]models.FaultCenter, error) {
	return []models.FaultCenter{{ID: "fc"}, {ID: "fc2"}}, nil
}

type eventFixtureDB struct{ repo.InterEntryRepo }

func (eventFixtureDB) FaultCenter() repo.InterFaultCenterRepo { return eventFixtureCenters{} }

type eventFixtureAlerts struct {
	cache.AlertCacheInterface
	events map[models.AlertEventCacheKey]map[string]*models.AlertCurEvent
}

func (e eventFixtureAlerts) GetAllEvents(key models.AlertEventCacheKey) (map[string]*models.AlertCurEvent, error) {
	return e.events[key], nil
}

type eventFixtureMutes struct{ cache.SilenceCacheInterface }

func (eventFixtureMutes) GetAlertMutes(string, string) ([]string, error) { return nil, nil }
func (eventFixtureMutes) ListAlertMutes(string, string) ([]models.AlertSilences, error) {
	return nil, nil
}

type eventFixtureCache struct {
	cache.InterEntryCache
	alerts eventFixtureAlerts
}

func (e eventFixtureCache) Alert() cache.AlertCacheInterface   { return e.alerts }
func (eventFixtureCache) Silence() cache.SilenceCacheInterface { return eventFixtureMutes{} }

func TestAgentEventIdentityAndScope(t *testing.T) {
	makeEvent := func(fp, center, env, source, tenant string) *models.AlertCurEvent {
		return &models.AlertCurEvent{TenantId: tenant, FaultCenterId: center, Fingerprint: fp, RuleId: "rule", DatasourceId: source, Status: models.StateAlerting, Labels: map[string]interface{}{"env": env}}
	}
	events := map[models.AlertEventCacheKey]map[string]*models.AlertCurEvent{
		models.BuildAlertEventCacheKey("t", "fc"): {
			"wanted":         makeEvent("wanted", "fc", "prod", "p", "t"),
			"other":          makeEvent("other", "fc", "prod", "p", "t"),
			"dev":            makeEvent("dev", "fc", "dev", "p", "t"),
			"foreign_source": makeEvent("foreign_source", "fc", "prod", "other", "t"),
			"foreign_tenant": makeEvent("foreign_tenant", "fc", "prod", "p", "other"),
			"nil":            nil,
		},
		models.BuildAlertEventCacheKey("t", "fc2"): {"wanted": makeEvent("wanted", "fc2", "prod", "p", "t")},
	}
	caches := eventFixtureCache{alerts: eventFixtureAlerts{events: events}}
	originalService, originalCache := EventService, appctx.Redis
	t.Cleanup(func() { EventService = originalService; appctx.Redis = originalCache })
	appctx.Redis = caches
	EventService = &eventService{ctx: &appctx.Context{DB: eventFixtureDB{}, Redis: caches}}
	service := &agentToolService{}
	claims := agenttoken.Claims{TenantId: "t", DatasourceIds: []string{"p"}, EnvironmentLabelKey: "env", Environments: []string{"prod"}}
	for _, tc := range []struct {
		fp, center string
		ok         bool
	}{
		{"wanted", "fc", true}, {"missing", "fc", false}, {"wanted", "", false},
		{"dev", "fc", false}, {"foreign_source", "fc", false}, {"foreign_tenant", "fc", false},
	} {
		t.Run(tc.fp+"_"+tc.center, func(t *testing.T) {
			data, err := service.getAlert(map[string]interface{}{"fingerprint": tc.fp, "faultCenterId": tc.center}, claims)
			if (err == nil) != tc.ok {
				t.Fatal("incorrect identity/scope", err)
			}
			if tc.ok {
				r := data.(types.ResponseAlertCurEventList)
				if len(r.List) != 1 || r.List[0].Fingerprint != tc.fp || r.Total != 1 {
					t.Fatal("filter must run before pagination", r)
				}
			}
		})
	}
	related, err := service.relatedAlerts(map[string]interface{}{"fingerprint": "wanted", "faultCenterId": "fc"}, claims)
	if err != nil {
		t.Fatal(err)
	}
	result := related.(map[string]interface{})
	if result["sameIncidentConfirmed"] != false || result["relationBasis"] != "same_rule_in_fault_center" {
		t.Fatal("correlation presented as incident fact")
	}
	data, err2 := EventService.ListCurrentEvent(&types.RequestAlertCurEventQuery{TenantId: "t", RuleId: "no-such-rule", Page: models.Page{Index: 1, Size: 10}})
	if err2 != nil || data.(types.ResponseAlertCurEventList).Total != 0 {
		t.Fatal("rule filter ignored")
	}
}
