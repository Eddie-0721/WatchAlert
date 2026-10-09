package cache

import (
	"context"
	"fmt"
	"github.com/go-redis/redis"
	"watchAlert/internal/models"
)

var recoveredEventDeleteCAS = redis.NewScript(`
if redis.call('HGET', KEYS[1], ARGV[1]) ~= ARGV[2] then return 0 end
return redis.call('HDEL', KEYS[1], ARGV[1])
`)

// Only the successful delete owner may continue into recovery notification.
// No unconditional HDEL and no resurrection or deletion of a new incident.
func (a *AlertCache) RemoveRecoveredEvent(requestCtx context.Context, expected models.AlertCurEvent) (bool, error) {
	if !validEventIdentity(expected) || !expected.IsRecovered || expected.Status != models.StateRecovered {
		return false, fmt.Errorf("invalid recovered event")
	}
	key := string(models.BuildAlertEventCacheKey(expected.TenantId, expected.FaultCenterId))
	for attempt := 0; attempt < eventCASAttempts; attempt++ {
		if err := requestCtx.Err(); err != nil {
			return false, err
		}
		raw, err := a.rc.HGet(key, expected.Fingerprint).Result()
		if err == redis.Nil {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		current, _, err := decodeEvent(raw)
		if err != nil {
			return false, err
		}
		if !eventIdentityMatches(current, expected) || !current.IsRecovered || current.Status != models.StateRecovered || current.RecoverTime != expected.RecoverTime {
			return false, nil
		}
		if err := requestCtx.Err(); err != nil {
			return false, err
		}
		removed, err := recoveredEventDeleteCAS.Run(a.rc, []string{key}, expected.Fingerprint, raw).Int()
		if err != nil {
			return false, err
		}
		if removed == 1 {
			return true, nil
		}
	}
	return false, ErrEventChanged
}
