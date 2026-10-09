package provider

import (
	"context"
	"errors"
	"sync"
	"watchAlert/config"
)

var ErrProbeBusy = errors.New("拨测任务繁忙，请稍后重试")

var probeSlotsOnce sync.Once
var probeSlots chan struct{}

func probeConcurrencyLimit(value int) int {
	if value <= 0 {
		return 32
	}
	if value > 256 {
		return 256
	}
	return value
}

func sharedProbeSlots() chan struct{} {
	probeSlotsOnce.Do(func() {
		probeSlots = make(chan struct{}, probeConcurrencyLimit(config.Application.Probing.MaxConcurrentRuns))
	})
	return probeSlots
}

// AcquireProbeSlot waits without spawning another goroutine. Scheduled rules
// have one worker each, so missed ticks do not create more queued jobs.
func AcquireProbeSlot(ctx context.Context) (func(), error) {
	return acquireProbeSlot(ctx, sharedProbeSlots(), true)
}

// TryAcquireProbeSlot rejects interactive overload instead of retaining a
// potentially unbounded queue of HTTP requests behind scheduled work.
func TryAcquireProbeSlot(ctx context.Context) (func(), error) {
	return acquireProbeSlot(ctx, sharedProbeSlots(), false)
}

func acquireProbeSlot(ctx context.Context, slots chan struct{}, wait bool) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if wait {
		select {
		case slots <- struct{}{}:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	} else {
		select {
		case slots <- struct{}{}:
		default:
			return nil, ErrProbeBusy
		}
	}
	if err := ctx.Err(); err != nil {
		<-slots
		return nil, err
	}
	var once sync.Once
	return func() { once.Do(func() { <-slots }) }, nil
}
