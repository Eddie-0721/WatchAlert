package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"watchAlert/internal/models"

	"github.com/redis/go-redis/v9"
)

var ErrEventChanged = errors.New("alert changed concurrently; refresh and retry")

// Compare original bytes, not a re-encoded model. Lua must not re-encode JSON:
// that changes empty arrays and can round numeric label values.
var eventValueCAS = redis.NewScript(`
local current = redis.call('HGET', KEYS[1], ARGV[1])
if ARGV[2] == 'create' then
  if current then return 0 end
else
  if current ~= ARGV[3] then return 0 end
end
local indexType = redis.call('TYPE', KEYS[2]).ok
if indexType ~= 'none' and indexType ~= 'zset' then return redis.error_reply('invalid rule index type') end
redis.call('ZADD', KEYS[2], 0, ARGV[5])
redis.call('HSET', KEYS[1], ARGV[1], ARGV[4])
return 1
`)

const eventCASAttempts = 4

func eventIdentityMatches(current, expected models.AlertCurEvent) bool {
	return current.TenantId == expected.TenantId && current.FaultCenterId == expected.FaultCenterId && current.Fingerprint == expected.Fingerprint && current.RuleId == expected.RuleId && current.EventId == expected.EventId && current.FirstTriggerTime == expected.FirstTriggerTime
}

func validEventIdentity(event models.AlertCurEvent) bool {
	return event.TenantId != "" && event.FaultCenterId != "" && event.Fingerprint != ""
}

func decodeEvent(raw string) (models.AlertCurEvent, map[string]json.RawMessage, error) {
	var event models.AlertCurEvent
	var fields map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &event); err != nil {
		return event, nil, fmt.Errorf("invalid cached alert JSON")
	}
	if err := json.Unmarshal([]byte(raw), &fields); err != nil || fields == nil {
		return event, nil, fmt.Errorf("invalid cached alert object")
	}
	return event, fields, nil
}

func confirmFields(fields map[string]json.RawMessage) (map[string]json.RawMessage, error) {
	result := make(map[string]json.RawMessage)
	if raw := fields["confirmState"]; len(raw) > 0 && string(raw) != "null" {
		if err := json.Unmarshal(raw, &result); err != nil {
			return nil, err
		}
	}
	return result, nil
}

// updateExisting never resurrects a deleted event and never crosses a new
// incident's identity, even when it shares the same rule fingerprint.
func (a *AlertCache) updateExisting(requestCtx context.Context, expected models.AlertCurEvent, change func(models.AlertCurEvent, map[string]json.RawMessage) (bool, error)) (bool, error) {
	if !validEventIdentity(expected) {
		return false, fmt.Errorf("invalid alert identity")
	}
	key := string(models.BuildAlertEventCacheKey(expected.TenantId, expected.FaultCenterId))
	for attempt := 0; attempt < eventCASAttempts; attempt++ {
		if err := requestCtx.Err(); err != nil {
			return false, err
		}
		raw, err := a.rc.HGet(requestCtx, key, expected.Fingerprint).Result()
		if err == redis.Nil {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		current, fields, err := decodeEvent(raw)
		if err != nil {
			return false, err
		}
		if !eventIdentityMatches(current, expected) {
			return false, nil
		}
		ok, err := change(current, fields)
		if err != nil || !ok {
			return ok, err
		}
		updated, err := json.Marshal(fields)
		if err != nil {
			return false, err
		}
		if string(updated) == raw {
			return true, nil
		}
		if err := requestCtx.Err(); err != nil {
			return false, err
		}
		result, err := eventValueCAS.Run(requestCtx, a.rc, []string{key, ruleIndexKey(key)}, expected.Fingerprint, "update", raw, string(updated), ruleIndexMember(expected.RuleId, expected.Fingerprint)).Int()
		if err != nil {
			return false, err
		}
		if result == 1 {
			return true, nil
		}
	}
	return false, ErrEventChanged
}

// ConfirmAlertEvent is first-writer-wins. Repeated claims keep the first owner.
func (a *AlertCache) ConfirmAlertEvent(requestCtx context.Context, expected models.AlertCurEvent, username string, at int64) (bool, error) {
	if username == "" || at <= 0 {
		return false, fmt.Errorf("invalid claim")
	}
	return a.updateExisting(requestCtx, expected, func(current models.AlertCurEvent, fields map[string]json.RawMessage) (bool, error) {
		if current.IsRecovered || current.Status == models.StateRecovered {
			return false, nil
		}
		if current.ConfirmState.IsOk {
			return true, nil
		}
		confirm, err := confirmFields(fields)
		if err != nil {
			return false, err
		}
		confirm["isOk"] = json.RawMessage("true")
		confirm["confirmActionTime"] = json.RawMessage(strconv.FormatInt(at, 10))
		confirm["confirmUsername"], err = json.Marshal(username)
		if err != nil {
			return false, err
		}
		fields["confirmState"], err = json.Marshal(confirm)
		return true, err
	})
}

// Notification time means attempt throttling, not confirmed delivery. Change
// only that timestamp, never a cached snapshot's labels/state/claim fields.
func (a *AlertCache) UpdateNotificationTime(requestCtx context.Context, expected models.AlertCurEvent, at int64, upgrade bool) (bool, error) {
	if at <= 0 {
		return false, fmt.Errorf("invalid notification time")
	}
	return a.updateExisting(requestCtx, expected, func(current models.AlertCurEvent, fields map[string]json.RawMessage) (bool, error) {
		if current.IsRecovered || current.Status == models.StateRecovered || current.Status != expected.Status {
			return false, nil
		}
		if !upgrade {
			if at > current.LastSendTime {
				fields["last_send_time"] = json.RawMessage(strconv.FormatInt(at, 10))
			}
			return true, nil
		}
		if current.ConfirmState.IsOk {
			return false, nil
		}
		if at <= current.ConfirmState.ConfirmTimeoutSendTime {
			return true, nil
		}
		confirm, err := confirmFields(fields)
		if err != nil {
			return false, err
		}
		confirm["confirmTimeoutSendTime"] = json.RawMessage(strconv.FormatInt(at, 10))
		fields["confirmState"], err = json.Marshal(confirm)
		return true, err
	})
}

// Evaluation owns monitoring fields, not operator claims or notification clocks.
// A stale evaluation must not undo either when publishing its newer metrics.
func (a *AlertCache) pushEvaluationEvent(event *models.AlertCurEvent) error {
	if event == nil || !validEventIdentity(*event) {
		return fmt.Errorf("invalid alert identity")
	}
	incoming, err := json.Marshal(event)
	if err != nil {
		return err
	}
	key := string(models.BuildAlertEventCacheKey(event.TenantId, event.FaultCenterId))
	for attempt := 0; attempt < eventCASAttempts; attempt++ {
		raw, err := a.rc.HGet(context.Background(), key, event.Fingerprint).Result()
		if err != nil && err != redis.Nil {
			return err
		}
		mode := "create"
		updated := incoming
		if err == redis.Nil && (event.IsRecovered || event.Status == models.StateRecovered || event.Status == models.StatePendingRecovery) {
			return ErrEventChanged
		}
		if err == nil {
			mode = "update"
			current, existing, decodeErr := decodeEvent(raw)
			if decodeErr != nil {
				return decodeErr
			}
			if !eventIdentityMatches(current, *event) {
				// Older cached events may lack the ID/time populated by the
				// evaluator's GetEventId/GetFirstTime compatibility helpers.
				legacy := current
				if legacy.EventId == "" {
					legacy.EventId = event.EventId
				}
				if legacy.FirstTriggerTime == 0 {
					legacy.FirstTriggerTime = event.FirstTriggerTime
				}
				if !eventIdentityMatches(legacy, *event) {
					return ErrEventChanged
				}
			}
			if event.LastEvalTime < current.LastEvalTime {
				return ErrEventChanged
			}
			if (current.IsRecovered || current.Status == models.StateRecovered) && (!event.IsRecovered || event.Status != models.StateRecovered) {
				return ErrEventChanged
			}
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(incoming, &fields); err != nil {
				return err
			}
			for name, value := range existing {
				if _, known := fields[name]; !known {
					fields[name] = value
				}
			}
			if value, ok := existing["confirmState"]; ok {
				fields["confirmState"] = value
			}
			// Recovery deliberately resets the regular notification clock.
			if !event.IsRecovered && event.Status != models.StateRecovered && current.LastSendTime > event.LastSendTime {
				fields["last_send_time"] = existing["last_send_time"]
			}
			updated, err = json.Marshal(fields)
			if err != nil {
				return err
			}
		}
		result, err := eventValueCAS.Run(context.Background(), a.rc, []string{key, ruleIndexKey(key)}, event.Fingerprint, mode, raw, string(updated), ruleIndexMember(event.RuleId, event.Fingerprint)).Int()
		if err != nil {
			return err
		}
		if result == 1 {
			return nil
		}
	}
	return ErrEventChanged
}
