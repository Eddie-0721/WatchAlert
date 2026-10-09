package consumer

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"
	"watchAlert/internal/models"

	"github.com/zeromicro/go-zero/core/logc"
)

const notificationGroupWorkers = 4
const notificationSendLimit = 8

// Process-wide cap shared by ordinary and upgrade notifications. Slots cover
// preparation as well as IO, so a queued job does not pre-stamp its clocks or
// carry a stale silence snapshot while waiting. No unbounded task queue.
var notificationSlots = make(chan struct{}, notificationSendLimit)

func withNotificationSlot(requestCtx context.Context, work func() error) (err error) {
	if err := requestCtx.Err(); err != nil {
		return err
	}
	select {
	case notificationSlots <- struct{}{}:
	case <-requestCtx.Done():
		return requestCtx.Err()
	}
	defer func() {
		<-notificationSlots
		if recovered := recover(); recovered != nil {
			logc.Errorf(requestCtx, "Notification worker panicked: %v\n%s", recovered, debug.Stack())
			err = fmt.Errorf("notification worker panicked")
		}
	}()
	if err := requestCtx.Err(); err != nil {
		return err
	}
	return work()
}

func dispatchNotificationGroups(requestCtx context.Context, groups *AlertGroups, send func(EventsGroup)) {
	if requestCtx.Err() != nil {
		return
	}
	workerCount := 0
	for _, rule := range groups.Rules {
		workerCount += len(rule.Groups)
		if workerCount >= notificationGroupWorkers {
			workerCount = notificationGroupWorkers
			break
		}
	}
	if workerCount == 0 {
		return
	}
	jobs := make(chan EventsGroup)
	var workers sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for group := range jobs {
				if requestCtx.Err() != nil {
					return
				}
				// Source events can belong to several notification objects.
				// Copy only when scheduled, not the entire backlog. Nested maps
				// are read-only throughout notification preparation/templates.
				local := EventsGroup{NoticeID: group.NoticeID, Events: make([]*models.AlertCurEvent, 0, len(group.Events))}
				for _, event := range group.Events {
					if event == nil {
						continue
					}
					copy := *event
					local.Events = append(local.Events, &copy)
				}
				func() {
					defer func() {
						if recovered := recover(); recovered != nil {
							logc.Errorf(requestCtx, "Notification group panicked: %v\n%s", recovered, debug.Stack())
						}
					}()
					send(local)
				}()
			}
		}()
	}
schedule:
	for _, rule := range groups.Rules {
		for _, group := range rule.Groups {
			if requestCtx.Err() != nil {
				break schedule
			}
			select {
			case jobs <- group:
			case <-requestCtx.Done():
				break schedule
			}
		}
	}
	close(jobs)
	workers.Wait() // No overlapping next tick or upgrade stage for this center.
}
