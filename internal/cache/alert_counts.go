package cache

import (
	"context"
	"fmt"
	"watchAlert/internal/models"

	"github.com/bytedance/sonic"
	"github.com/zeromicro/go-zero/core/logc"
)

type EventStateCounts struct {
	PreAlert        int64
	Alerting        int64
	PendingRecovery int64
}

// CountEventStates keeps the full typed decoder's validity rules, but neither
// hash fields nor a retained map of decoded events are needed for a count.
// HVALS still reads the complete hash atomically; this is not a cached index or
// a partial HSCAN snapshot. The caller's deadline bounds Redis network I/O.
func (a *AlertCache) CountEventStates(ctx context.Context, key models.AlertEventCacheKey) (EventStateCounts, error) {
	if err := ctx.Err(); err != nil {
		return EventStateCounts{}, err
	}
	a.RLock()
	defer a.RUnlock()
	if err := ctx.Err(); err != nil {
		return EventStateCounts{}, err
	}
	values, err := a.rc.HVals(ctx, string(key)).Result()
	if canceled := ctx.Err(); canceled != nil {
		return EventStateCounts{}, canceled
	}
	if err != nil {
		return EventStateCounts{}, err
	}
	return countEventValues(ctx, values)
}

func countEventValues(ctx context.Context, values []string) (EventStateCounts, error) {
	var counts EventStateCounts
	var event models.AlertCurEvent
	for _, raw := range values {
		if err := ctx.Err(); err != nil {
			return EventStateCounts{}, err
		}
		// Reuse only the outer allocation; absent/null fields must never inherit
		// the previous row's status, labels, or partially decoded invalid data.
		event = models.AlertCurEvent{}
		if err := sonic.UnmarshalString(raw, &event); err != nil {
			logc.Error(context.Background(), fmt.Sprintf("unmarshal event json error: %s, event json: %s", err.Error(), raw))
			continue
		}
		switch event.Status {
		case models.StatePreAlert:
			counts.PreAlert++
		case models.StateAlerting:
			counts.Alerting++
		case models.StatePendingRecovery:
			counts.PendingRecovery++
		}
	}
	if err := ctx.Err(); err != nil {
		return EventStateCounts{}, err
	}
	return counts, nil
}
