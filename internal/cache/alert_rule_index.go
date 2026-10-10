package cache

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"
	"time"
	"watchAlert/config"
	"watchAlert/internal/models"

	"github.com/bytedance/sonic"
	"github.com/redis/go-redis/v9"
	"github.com/zeromicro/go-zero/core/logc"
)

const ruleIndexMarker = "!complete-v1"

func ruleIndexKey(source string) string        { return source + ":rule-index:v1" }
func ruleIndexPrefix(ruleID string) string     { return hex.EncodeToString([]byte(ruleID)) + ":" }
func ruleIndexMember(ruleID, fp string) string { return ruleIndexPrefix(ruleID) + fp }

// Read membership and source values atomically; never interpret a partial
// index or dangling membership as an empty rule. All members have score zero.
var readRuleIndex = redis.NewScript(`
if not redis.call('ZSCORE', KEYS[2], '!complete-v1') then return {'rebuild'} end
if redis.call('ZCARD', KEYS[2]) ~= redis.call('HLEN', KEYS[1]) + 1 then return {'rebuild'} end
local members = redis.call('ZRANGEBYLEX', KEYS[2], '[' .. ARGV[1], '[' .. ARGV[1] .. string.char(255))
local result = {'ready'}
for _, member in ipairs(members) do
  local field = string.sub(member, string.len(ARGV[1]) + 1)
  local value = redis.call('HGET', KEYS[1], field)
  if not value then return {'rebuild'} end
  table.insert(result, field)
  table.insert(result, value)
end
return result
`)

// Existing unconditional deletes remain unconditional, but keep the derived
// membership consistent. cjson is used only to read rule_id, never to rewrite
// event JSON (which would corrupt empty arrays/large numeric values).
var deleteIndexedEvent = redis.NewScript(`
local raw = redis.call('HGET', KEYS[1], ARGV[1])
if not raw then return 0 end
local indexType = redis.call('TYPE', KEYS[2]).ok
if indexType ~= 'none' and indexType ~= 'zset' then return redis.error_reply('invalid rule index type') end
local ok, event = pcall(cjson.decode, raw)
if ok and type(event) == 'table' and type(event.rule_id) == 'string' then
  local encoded = string.gsub(event.rule_id, '.', function(c) return string.format('%02x', string.byte(c)) end)
  redis.call('ZREM', KEYS[2], encoded .. ':' .. ARGV[1])
else
  redis.call('ZREM', KEYS[2], '!complete-v1')
end
return redis.call('HDEL', KEYS[1], ARGV[1])
`)

func parseScopedEvent(source, tenant, fp, raw string) (*models.AlertCurEvent, error) {
	var event models.AlertCurEvent
	if err := sonic.Unmarshal([]byte(raw), &event); err != nil {
		return nil, fmt.Errorf("invalid alert cache JSON for fingerprint %s", fp)
	}
	if fp == "" || event.Fingerprint != fp || event.TenantId != tenant || string(models.BuildAlertEventCacheKey(event.TenantId, event.FaultCenterId)) != source {
		return nil, fmt.Errorf("alert cache scope mismatch for fingerprint %s", fp)
	}
	return &event, nil
}

// Opt-in read path for homogeneous upgraded writers. Startup always rebuilds
// a center once, so derived data from rollback/restore is never trusted merely
// because its marker survived. No TTL reuse of event data or empty results.
func (a *AlertCache) GetRuleEvents(requestCtx context.Context, key models.AlertEventCacheKey, tenant, rule string) (map[string]*models.AlertCurEvent, error) {
	if err := requestCtx.Err(); err != nil {
		return nil, err
	}
	if key == "" || tenant == "" || rule == "" {
		return nil, fmt.Errorf("missing rule event scope")
	}
	source := string(key)
	if !strings.HasPrefix(source, fmt.Sprintf("w8t:%s:%s:", tenant, models.FaultCenterPrefix)) {
		return nil, fmt.Errorf("rule event tenant scope mismatch")
	}
	if config.Application.Evaluation.RuleEventIndex {
		if _, ready := a.indexReady.Load(source); !ready {
			if err := a.rebuildRuleIndex(requestCtx, source, tenant); err != nil {
				return a.readRuleEventsFull(requestCtx, source, tenant, rule)
			}
		}
		for attempt := 0; attempt < 2; attempt++ {
			value, err := readRuleIndex.Run(requestCtx, a.rc, []string{source, ruleIndexKey(source)}, ruleIndexPrefix(rule)).Result()
			if err != nil {
				a.indexReady.Delete(source)
				logc.Errorf(requestCtx, "Rule index read failed, falling back to source: %v", err)
				break
			}
			result, ok := value.([]interface{})
			if !ok || len(result) == 0 || len(result)%2 == 0 {
				return nil, fmt.Errorf("invalid rule index response")
			}
			if len(result) > 0 && result[0] == "ready" {
				events := make(map[string]*models.AlertCurEvent, (len(result)-1)/2)
				for i := 1; i+1 < len(result); i += 2 {
					if err := requestCtx.Err(); err != nil {
						return nil, err
					}
					fp, ok := result[i].(string)
					if !ok {
						return nil, fmt.Errorf("invalid rule index field")
					}
					raw, ok := result[i+1].(string)
					if !ok {
						return nil, fmt.Errorf("invalid rule index value")
					}
					event, err := parseScopedEvent(source, tenant, fp, raw)
					if err != nil {
						return nil, err
					}
					if event.RuleId != rule {
						a.indexReady.Delete(source)
						return a.readRuleEventsFull(requestCtx, source, tenant, rule)
					}
					events[fp] = event
				}
				return events, nil
			}
			a.indexReady.Delete(source)
			if err := a.rebuildRuleIndex(requestCtx, source, tenant); err != nil {
				break
			}
		}
	}
	return a.readRuleEventsFull(requestCtx, source, tenant, rule)
}

func (a *AlertCache) readRuleEventsFull(requestCtx context.Context, source, tenant, rule string) (map[string]*models.AlertCurEvent, error) {
	if err := requestCtx.Err(); err != nil {
		return nil, err
	}
	rows, err := a.rc.HGetAll(requestCtx, source).Result()
	if err != nil {
		return nil, err
	}
	events := make(map[string]*models.AlertCurEvent)
	for fp, raw := range rows {
		if err := requestCtx.Err(); err != nil {
			return nil, err
		}
		event, err := parseScopedEvent(source, tenant, fp, raw)
		if err != nil {
			return nil, err
		}
		if event.RuleId == rule {
			events[fp] = event
		}
	}
	return events, nil
}

func (a *AlertCache) rebuildRuleIndex(requestCtx context.Context, source, tenant string) error {
	result := a.indexBuild.DoChan(source, func() (_ any, buildErr error) {
		if _, ready := a.indexReady.Load(source); ready {
			return nil, nil
		}
		if retry, ok := a.indexRetry.Load(source); ok && time.Now().Before(retry.(time.Time)) {
			return nil, fmt.Errorf("rule index rebuild cooling down")
		}
		defer func() {
			if buildErr != nil {
				a.indexRetry.Store(source, time.Now().Add(time.Second))
				logc.Errorf(requestCtx, "Rule index rebuild failed; using fresh full reads: %v", buildErr)
			} else {
				a.indexRetry.Delete(source)
			}
		}()
		for attempt := 0; attempt < eventCASAttempts; attempt++ {
			if err := requestCtx.Err(); err != nil {
				return nil, err
			}
			err := a.rc.Watch(requestCtx, func(tx *redis.Tx) error {
				rows, err := tx.HGetAll(requestCtx, source).Result()
				if err != nil {
					return err
				}
				members := make([]redis.Z, 0, len(rows)+1)
				members = append(members, redis.Z{Member: ruleIndexMarker})
				for fp, raw := range rows {
					if err := requestCtx.Err(); err != nil {
						return err
					}
					event, err := parseScopedEvent(source, tenant, fp, raw)
					if err != nil {
						return err
					}
					members = append(members, redis.Z{Member: ruleIndexMember(event.RuleId, fp)})
				}
				if err := requestCtx.Err(); err != nil {
					return err
				}
				_, err = tx.TxPipelined(requestCtx, func(pipe redis.Pipeliner) error {
					pipe.Del(requestCtx, ruleIndexKey(source))
					pipe.ZAdd(requestCtx, ruleIndexKey(source), members...)
					return nil
				})
				return err
			}, source)
			if err == redis.TxFailedErr {
				continue
			}
			if err != nil {
				return nil, err
			}
			a.indexReady.Store(source, true)
			return nil, nil
		}
		return nil, redis.TxFailedErr
	})
	select {
	case <-requestCtx.Done():
		return requestCtx.Err()
	case outcome := <-result:
		return outcome.Err
	}
}
