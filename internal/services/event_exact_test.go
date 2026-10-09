package services

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"watchAlert/internal/models"
	"watchAlert/internal/types"
	"watchAlert/pkg/agenttoken"

	"github.com/go-redis/redis"
)

func TestExactEventSelectionPreservesFiltersSummaryAndPagination(t *testing.T) {
	service, reference, _ := redisEventFixture(t, 1000)
	for _, center := range []string{"fc", "fc2", "", "missing"} {
		for _, fp := range []string{"fp-00000002", "fp-00000000", "fp-00000031", "missing"} {
			for _, queue := range []string{"all", "attention", "suppressed", ""} {
				for _, page := range []int64{1, 2} {
					query := &types.RequestAlertCurEventQuery{TenantId: "t", FaultCenterId: center, Fingerprint: fp, Queue: queue, IncludeRecovered: true, IncludeSummary: true, Page: models.Page{Index: page, Size: 1}}
					data, err := service.ListCurrentEvent(query)
					if err != nil || !reflect.DeepEqual(data, referenceCurrentList(reference, query)) {
						t.Fatal("exact query changed semantics", center, fp, queue, page, data, err)
					}
				}
			}
		}
	}
	for _, extra := range []types.RequestAlertCurEventQuery{
		{Environment: "test"}, {Service: "service-2"}, {Service: "absent"},
		{RuleId: "rule-2"}, {RuleId: "absent"}, {AgentDatasourceIds: []string{"unauthorized"}},
		{AgentEnvironmentLabelKey: "env", AgentEnvironments: []string{"prod"}},
		{AgentEnvironmentLabelKey: "env", AgentEnvironments: []string{"test"}},
	} {
		extra.TenantId, extra.Fingerprint, extra.Page = "t", "fp-00000002", models.Page{Index: 1, Size: 2}
		data, err := service.ListCurrentEvent(&extra)
		if err != nil || !reflect.DeepEqual(data, referenceCurrentList(reference, &extra)) {
			t.Fatal("scope/filter changed", extra, data, err)
		}
	}
	query := &types.RequestAlertCurEventQuery{TenantId: "t", Fingerprint: "fp-00000000", IncludeSummary: true, Page: models.Page{Index: 1, Size: 2}}
	data, err := service.ListCurrentEvent(query)
	if err != nil || !reflect.DeepEqual(data, referenceCurrentList(reference, query)) || data.(types.ResponseAlertCurEventList).Total != 0 {
		t.Fatal("recovered event surfaced without opt-in", data, err)
	}
}

func TestExactEventReadsOnlyRequestedHashField(t *testing.T) {
	service, _, _ := redisEventFixture(t, 1000)
	key := string(models.BuildAlertEventCacheKey("t", "fc"))
	var eventCommands []string
	service.ctx.Redis.Redis().WrapProcess(func(next func(redis.Cmder) error) func(redis.Cmder) error {
		return func(cmd redis.Cmder) error {
			if len(cmd.Args()) > 1 && fmt.Sprint(cmd.Args()[1]) == key {
				eventCommands = append(eventCommands, cmd.Name())
			}
			return next(cmd)
		}
	})
	query := &types.RequestAlertCurEventQuery{TenantId: "t", FaultCenterId: "fc", Fingerprint: "fp-00000002", IncludeSummary: true, Page: models.Page{Index: 1, Size: 2}}
	data, err := service.ListCurrentEvent(query)
	if err != nil || data.(types.ResponseAlertCurEventList).Total != 1 {
		t.Fatal(data, err)
	}
	if !reflect.DeepEqual(eventCommands, []string{"hget"}) {
		t.Fatal("exact query fetched unrelated events", eventCommands)
	}
	eventCommands = nil
	query.Fingerprint = ""
	if _, err := service.ListCurrentEvent(query); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(eventCommands, []string{"hgetall"}) {
		t.Fatal("broad list no longer scans all rows", eventCommands)
	}
}

func TestExactAgentEventKeepsAmbiguityAndLateFailureChecks(t *testing.T) {
	service, _, server := redisEventFixture(t, 1000)
	previous := EventService
	EventService = &service
	t.Cleanup(func() { EventService = previous })
	agent := &agentToolService{}
	claims := agenttoken.Claims{TenantId: "t", EnvironmentLabelKey: "env", Environments: []string{"prod"}}
	args := map[string]interface{}{"fingerprint": "fp-00000002"}
	if _, err := agent.getAlert(context.Background(), args, claims); err != nil {
		t.Fatal(err)
	}
	key := string(models.BuildAlertEventCacheKey("t", "fc2"))
	duplicate := models.AlertCurEvent{TenantId: "t", FaultCenterId: "fc2", Fingerprint: "fp-00000002", Status: models.StateAlerting, Labels: map[string]interface{}{"env": "prod"}}
	raw, _ := json.Marshal(duplicate)
	server.HSet(key, duplicate.Fingerprint, string(raw))
	if _, err := agent.getAlert(context.Background(), args, claims); err == nil {
		t.Fatal("duplicate centers silently resolved to first result")
	}
	args["faultCenterId"] = "fc"
	if _, err := agent.getAlert(context.Background(), args, claims); err != nil {
		t.Fatal(err)
	}
	delete(args, "faultCenterId")
	server.Del(key)
	server.Set(key, "wrong-type")
	if result, err := agent.getAlert(context.Background(), args, claims); err == nil || result != nil {
		t.Fatal("late storage error returned first result", result, err)
	}
}

func TestExactEventStillChecksPayloadIdentityAndSilenceFailures(t *testing.T) {
	service, _, server := redisEventFixture(t, 1000)
	query := &types.RequestAlertCurEventQuery{TenantId: "t", FaultCenterId: "fc", Fingerprint: "fp-00000002", Page: models.Page{Index: 1, Size: 2}}
	key := string(models.BuildAlertEventCacheKey("t", "fc"))
	for _, invalid := range []models.AlertCurEvent{
		{TenantId: "other", FaultCenterId: "fc", Fingerprint: query.Fingerprint},
		{TenantId: "t", FaultCenterId: "fc2", Fingerprint: query.Fingerprint},
		{TenantId: "t", FaultCenterId: "fc", Fingerprint: "other"},
	} {
		raw, _ := json.Marshal(invalid)
		server.HSet(key, query.Fingerprint, string(raw))
		data, err := service.ListCurrentEvent(query)
		if err != nil || data.(types.ResponseAlertCurEventList).Total != 0 {
			t.Fatal("hash lookup bypassed payload filtering", data, err)
		}
	}
	valid := models.AlertCurEvent{TenantId: "t", FaultCenterId: "fc", Fingerprint: query.Fingerprint, Status: models.StateAlerting, Labels: map[string]interface{}{"env": "prod"}}
	raw, _ := json.Marshal(valid)
	server.HSet(key, query.Fingerprint, string(raw))
	muteKey := string(models.BuildAlertMuteCacheKey("t", "fc"))
	server.Del(muteKey)
	server.Set(muteKey, "wrong-type")
	if data, err := service.ListCurrentEvent(query); err == nil || data != nil {
		t.Fatal("silence failure reported as an unsilenced event", data, err)
	}
}
