package consumer

import (
	"context"
	"fmt"
	"time"
	"watchAlert/alert/mute"
	appctx "watchAlert/internal/ctx"
	"watchAlert/internal/models"
)

// Match every source event before aggregation and before changing throttle
// clocks. A read/compile failure is an error, never permission to send.
func prepareNotificationMembers(requestCtx context.Context, app *appctx.Context, processType string, fc models.FaultCenter, members []*models.AlertCurEvent) ([]*models.AlertCurEvent, error) {
	if err := requestCtx.Err(); err != nil {
		return nil, err
	}
	candidates := make([]*models.AlertCurEvent, 0, len(members))
	for _, event := range members {
		if event == nil || event.Fingerprint == "" {
			continue
		}
		if event.TenantId != fc.TenantId || event.FaultCenterId != fc.ID {
			return nil, fmt.Errorf("notification event scope mismatch")
		}
		if mute.RecoverNotify(mute.MuteParams{IsRecovered: event.IsRecovered, RecoverNotify: fc.RecoverNotify}) {
			continue
		}
		if processType == "upgrade" && (event.IsRecovered || event.ConfirmState.IsOk) {
			continue
		}
		candidates = append(candidates, event)
	}
	if len(candidates) == 0 {
		return nil, nil
	}
	now := time.Now().Unix()
	match, err := mute.LoadSnapshot(app.Redis.Silence(), fc.TenantId, fc.ID, now)
	if err != nil {
		return nil, err
	}
	ready := make([]*models.AlertCurEvent, 0, len(candidates))
	var firstError error
	for _, event := range candidates {
		if err := requestCtx.Err(); err != nil {
			return nil, err
		}
		if match(event.Labels) {
			continue
		}
		if !event.IsRecovered {
			upgrade := processType == "upgrade"
			ok, err := app.Redis.Alert().UpdateNotificationTime(requestCtx, *event, now, upgrade)
			if err != nil {
				if firstError == nil {
					firstError = fmt.Errorf("update notification clock: %w", err)
				}
				continue
			}
			if !ok {
				continue
			}
			if upgrade {
				event.ConfirmState.ConfirmTimeoutSendTime = max(event.ConfirmState.ConfirmTimeoutSendTime, now)
			} else {
				event.LastSendTime = max(event.LastSendTime, now)
			}
		}
		ready = append(ready, event)
	}
	return ready, firstError
}
