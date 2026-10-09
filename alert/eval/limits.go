package eval

import (
	"context"
	"sync"
	"watchAlert/config"
)

var queryLimitOnce sync.Once
var querySlots chan struct{}

func boundedSetting(value, fallback, maximum int) int {
	if value <= 0 {
		return fallback
	}
	if value > maximum {
		return maximum
	}
	return value
}

// Shared by alert and recording rule evaluators in this process. Configuration
// is loaded once at startup. Waiting workers are cancelled with their rule;
// there is no queue of ticks accumulating behind a slow datasource.
func sharedQuerySlots() chan struct{} {
	queryLimitOnce.Do(func() {
		querySlots = make(chan struct{}, boundedSetting(config.Application.Evaluation.MaxConcurrentQueries, 32, 256))
	})
	return querySlots
}

func acquireQuery(ctx context.Context, slots chan struct{}) bool {
	if ctx.Err() != nil {
		return false
	}
	select {
	case slots <- struct{}{}:
		if ctx.Err() != nil {
			<-slots
			return false
		}
		return true
	case <-ctx.Done():
		return false
	}
}
