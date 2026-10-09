package consumer

import (
	"context"
	"runtime/debug"
	"time"
	"watchAlert/internal/models"

	"github.com/zeromicro/go-zero/core/logc"
)

// All fields except channels are protected by Consume's mutex. A stopped
// worker remains registered until its in-flight cycle has actually returned.
// A reload during that drain reuses it rather than overlapping old/new loops.
type consumerWorker struct {
	center  models.FaultCenter
	enabled bool
	wake    chan struct{}
	done    chan struct{}
	cancel  context.CancelFunc
}

func (w *consumerWorker) signal() {
	select {
	case w.wake <- struct{}{}:
	default:
	}
}
func (w *consumerWorker) stop() {
	w.enabled = false
	if w.cancel != nil {
		w.cancel()
	}
	w.signal()
}

func (c *Consume) submit(center models.FaultCenter, execute func(context.Context, models.FaultCenter)) {
	c.Lock()
	defer c.Unlock()
	if c.ctx.Ctx.Err() != nil {
		return
	}
	if c.workers == nil {
		c.workers = make(map[string]*consumerWorker)
	}
	if worker := c.workers[center.ID]; worker != nil {
		worker.center, worker.enabled = center, true
		if worker.cancel != nil {
			worker.cancel()
		}
		worker.signal()
		return
	}
	worker := &consumerWorker{center: center, enabled: true, wake: make(chan struct{}, 1), done: make(chan struct{})}
	c.workers[center.ID] = worker
	go c.runWorker(worker, execute)
}

func (c *Consume) runWorker(worker *consumerWorker, execute func(context.Context, models.FaultCenter)) {
	ticker := time.NewTicker(time.Second * DefaultProcessTime)
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Ctx.Done():
		case <-worker.wake:
		case <-ticker.C:
		}
		c.Lock()
		if !worker.enabled || c.ctx.Ctx.Err() != nil {
			delete(c.workers, worker.center.ID)
			close(worker.done)
			c.Unlock()
			return
		}
		center := worker.center
		requestCtx, cancel := context.WithCancel(c.ctx.Ctx)
		worker.cancel = cancel
		c.Unlock()

		c.executeSafely(requestCtx, center, execute)
		cancel()
	}
}

func (c *Consume) executeSafely(requestCtx context.Context, center models.FaultCenter, execute func(context.Context, models.FaultCenter)) {
	defer func() {
		if recovered := recover(); recovered != nil {
			logc.Errorf(c.ctx.Ctx, "Consumer cycle panicked, center: %s, error: %v\n%s", center.ID, recovered, debug.Stack())
		}
	}()
	if requestCtx.Err() == nil {
		execute(requestCtx, center)
	}
}
