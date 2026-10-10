package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"watchAlert/config"
	"watchAlert/internal/models"
	"watchAlert/internal/testutil"

	"github.com/alicebob/miniredis/v2/server"
	"github.com/redis/go-redis/v9"
)

func enableRuleIndex(t *testing.T) {
	t.Helper()
	previous := config.Application.Evaluation.RuleEventIndex
	config.Application.Evaluation.RuleEventIndex = true
	t.Cleanup(func() { config.Application.Evaluation.RuleEventIndex = previous })
}

func TestRuleIndexWarmReadOnlyTransfersSelectedRule(t *testing.T) {
	enableRuleIndex(t)
	c, s, base := eventStateFixture(t)
	key := models.BuildAlertEventCacheKey("t", "fc")
	for i := 0; i < 1000; i++ {
		event := base
		event.Fingerprint = fmt.Sprint(i)
		event.RuleId = fmt.Sprintf("rule-%d", i%20)
		raw, _ := json.Marshal(event)
		s.HSet(string(key), event.Fingerprint, string(raw))
	}
	got, err := c.GetRuleEvents(context.Background(), key, "t", "rule-3")
	if err != nil || len(got) != 50 {
		t.Fatal("legacy bootstrap lost members", len(got), err)
	}
	var commands []string
	replyValues := 0
	testutil.WrapRedisProcess(c.rc, func(next func(redis.Cmder) error) func(redis.Cmder) error {
		return func(cmd redis.Cmder) error {
			commands = append(commands, cmd.Name())
			err := next(cmd)
			if result, ok := cmd.(*redis.Cmd); ok {
				if values, ok := result.Val().([]interface{}); ok {
					replyValues = len(values)
				}
			}
			return err
		}
	})
	got, err = c.GetRuleEvents(context.Background(), key, "t", "rule-3")
	if err != nil || len(got) != 50 || len(commands) != 1 || commands[0] != "evalsha" || replyValues != 101 {
		t.Fatal("warm read scanned/transferred unrelated events", len(got), commands, replyValues, err)
	}
	for _, event := range got {
		if event.RuleId != "rule-3" {
			t.Fatal("foreign rule returned")
		}
	}
}

func TestRuleIndexTracksCreatesClaimsAndBothDeletePaths(t *testing.T) {
	enableRuleIndex(t)
	c, _, event := eventStateFixture(t)
	key := models.BuildAlertEventCacheKey("t", "fc")
	if _, err := c.GetRuleEvents(context.Background(), key, "t", "rule"); err != nil {
		t.Fatal(err)
	}
	newEvent := event
	newEvent.Fingerprint = "new"
	if err := c.PushAlertEvent(&newEvent); err != nil {
		t.Fatal(err)
	}
	if ok, err := c.ConfirmAlertEvent(context.Background(), newEvent, "operator", 100); !ok || err != nil {
		t.Fatal(ok, err)
	}
	if ok, err := c.UpdateNotificationTime(context.Background(), newEvent, 110, false); !ok || err != nil {
		t.Fatal(ok, err)
	}
	got, err := c.GetRuleEvents(context.Background(), key, "t", "rule")
	if err != nil || len(got) != 2 || got["new"].ConfirmState.ConfirmUsername != "operator" || got["new"].LastSendTime != 110 {
		t.Fatal(got, err)
	}
	c.RemoveAlertEvent("t", "fc", "new")
	event.Status = models.StateRecovered
	event.IsRecovered = true
	event.RecoverTime = 200
	if err := c.PushAlertEvent(&event); err != nil {
		t.Fatal(err)
	}
	if ok, err := c.RemoveRecoveredEvent(context.Background(), event); !ok || err != nil {
		t.Fatal(ok, err)
	}
	got, err = c.GetRuleEvents(context.Background(), key, "t", "rule")
	if err != nil || len(got) != 0 {
		t.Fatal("deleted members remain", got, err)
	}
	if count := c.rc.ZCard(context.Background(), ruleIndexKey(string(key))).Val(); count != 1 {
		t.Fatal("orphaned index members", count)
	}
}

func TestRuleIndexRebuildsMissingPartialAndRestartedDerivedData(t *testing.T) {
	enableRuleIndex(t)
	c, s, event := eventStateFixture(t)
	key := models.BuildAlertEventCacheKey("t", "fc")
	index := ruleIndexKey(string(key))
	if _, err := c.GetRuleEvents(context.Background(), key, "t", "rule"); err != nil {
		t.Fatal(err)
	}
	for _, damage := range []func(){
		func() { s.Del(index) },
		func() { c.rc.ZRem(context.Background(), index, ruleIndexMember("rule", "fp")) },
		func() {
			extra := event
			extra.Fingerprint = "legacy-added"
			raw, _ := json.Marshal(extra)
			s.HSet(string(key), extra.Fingerprint, string(raw))
		},
	} {
		damage()
		got, err := c.GetRuleEvents(context.Background(), key, "t", "rule")
		if err != nil || got["fp"] == nil {
			t.Fatal("partial index became empty result", got, err)
		}
	}
	// Same-cardinality out-of-band mutations are not detectable from counts;
	// a fresh process must rebuild even when a completion marker survived.
	c.rc.ZRem(context.Background(), index, ruleIndexMember("rule", "fp"))
	c.rc.ZAdd(context.Background(), index, redis.Z{Member: ruleIndexMember("wrong", "fp")})
	restarted := &AlertCache{rc: c.rc}
	got, err := restarted.GetRuleEvents(context.Background(), key, "t", "rule")
	if err != nil || len(got) != 2 {
		t.Fatal("restart trusted stale derived data", got, err)
	}
}

func TestRuleIndexFailuresNeverBecomeEmptySuccessfulSnapshots(t *testing.T) {
	enableRuleIndex(t)
	c, s, _ := eventStateFixture(t)
	key := models.BuildAlertEventCacheKey("t", "fc")
	if _, err := c.GetRuleEvents(context.Background(), key, "t", "rule"); err != nil {
		t.Fatal(err)
	}
	s.Del(ruleIndexKey(string(key)))
	if err := s.Set(ruleIndexKey(string(key)), "wrong-type"); err != nil {
		t.Fatal(err)
	}
	if got, err := c.GetRuleEvents(context.Background(), key, "t", "rule"); err != nil || len(got) != 1 {
		t.Fatal("index failure did not fall back to source", got, err)
	}
	s.SetError("ERR unavailable")
	if _, err := c.GetRuleEvents(context.Background(), key, "t", "rule"); err == nil {
		t.Fatal("Redis failure hidden")
	}
	s.SetError("")
	s.HSet(string(key), "fp", "invalid-json")
	if _, err := c.GetRuleEvents(context.Background(), key, "t", "rule"); err == nil {
		t.Fatal("damaged source skipped")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.GetRuleEvents(ctx, key, "t", "rule"); err == nil {
		t.Fatal("cancellation ignored")
	}
}

func TestRuleIndexDisabledStillUsesCompleteFreshSource(t *testing.T) {
	before := config.Application.Evaluation.RuleEventIndex
	config.Application.Evaluation.RuleEventIndex = false
	t.Cleanup(func() { config.Application.Evaluation.RuleEventIndex = before })
	c, s, event := eventStateFixture(t)
	key := models.BuildAlertEventCacheKey("t", "fc")
	for _, rule := range []string{"rule", "changed-by-old-writer"} {
		event.RuleId = rule
		raw, _ := json.Marshal(event)
		s.HSet(string(key), "fp", string(raw))
		got, err := c.GetRuleEvents(context.Background(), key, "t", rule)
		if err != nil || len(got) != 1 {
			t.Fatal("disabled mode trusted index", got, err)
		}
	}
	if _, err := c.GetRuleEvents(context.Background(), key, "other", "rule"); err == nil {
		t.Fatal("foreign tenant cache accepted")
	}
}

func TestRuleIndexSeparatesArbitraryRulePrefixes(t *testing.T) {
	enableRuleIndex(t)
	c, _, base := eventStateFixture(t)
	key := models.BuildAlertEventCacheKey("t", "fc")
	for i, rule := range []string{"r", "r:", "r\x00", "规则"} {
		event := base
		event.Fingerprint = fmt.Sprint(i)
		event.RuleId = rule
		if err := c.PushAlertEvent(&event); err != nil {
			t.Fatal(err)
		}
	}
	for _, rule := range []string{"r", "r:", "r\x00", "规则"} {
		got, err := c.GetRuleEvents(context.Background(), key, "t", rule)
		if err != nil || len(got) != 1 {
			t.Fatal(rule, got, err)
		}
	}
}

func TestConcurrentRuleIndexBootstrapAndQueries(t *testing.T) {
	enableRuleIndex(t)
	c, _, _ := eventStateFixture(t)
	key := models.BuildAlertEventCacheKey("t", "fc")
	var wg sync.WaitGroup
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, err := c.GetRuleEvents(context.Background(), key, "t", "rule")
			if err != nil || len(got) != 1 {
				t.Error(got, err)
			}
		}()
	}
	wg.Wait()
}

func TestRuleIndexBootstrapConflictFallsBackFreshAndBacksOff(t *testing.T) {
	enableRuleIndex(t)
	c, s, event := eventStateFixture(t)
	key := models.BuildAlertEventCacheKey("t", "fc")
	conflicts := 0
	s.Server().SetPreHook(func(_ *server.Peer, cmd string, _ ...string) bool {
		if cmd == "EXEC" {
			conflicts++
			fresh := event
			fresh.LastEvalTime = int64(100 + conflicts)
			raw, _ := json.Marshal(fresh)
			s.HSet(string(key), "fp", string(raw))
		}
		return false
	})
	defer s.Server().SetPreHook(nil)
	got, err := c.GetRuleEvents(context.Background(), key, "t", "rule")
	if err != nil || len(got) != 1 || got["fp"].LastEvalTime != 104 || conflicts != eventCASAttempts {
		t.Fatal("conflicted bootstrap lost fresh source", got, conflicts, err)
	}
	if _, ready := c.indexReady.Load(string(key)); ready {
		t.Fatal("failed transaction marked index complete")
	}
	if got, err = c.GetRuleEvents(context.Background(), key, "t", "rule"); err != nil || len(got) != 1 || conflicts != eventCASAttempts {
		t.Fatal("rebuild backoff replayed scans or returned empty", got, conflicts, err)
	}
}

func TestRuleIndexFailureCannotPartiallyWriteSource(t *testing.T) {
	c, s, event := eventStateFixture(t)
	key := models.BuildAlertEventCacheKey("t", "fc")
	before := s.HGet(string(key), "fp")
	s.Del(ruleIndexKey(string(key)))
	if err := s.Set(ruleIndexKey(string(key)), "wrong type"); err != nil {
		t.Fatal(err)
	}
	event.LastEvalTime++
	if err := c.PushAlertEvent(&event); err == nil {
		t.Fatal("invalid index type not reported")
	}
	after := s.HGet(string(key), "fp")
	if before != after {
		t.Fatal("Lua error partially changed source")
	}
}
