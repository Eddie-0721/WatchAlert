package consumer

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	appctx "watchAlert/internal/ctx"
	"watchAlert/internal/models"
)

func wakeConsumer(c *Consume, id string) *consumerWorker {
	c.Lock()
	defer c.Unlock()
	w := c.workers[id]
	w.signal()
	return w
}

func TestConsumerReloadCoalescesAndWaitsForOldCycleEvenAfterPanic(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := &Consume{ctx: &appctx.Context{Ctx: parent}}
	started := make(chan string, 10)
	oldCanceled := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	var active, peak atomic.Int32
	run := func(ctx context.Context, fc models.FaultCenter) {
		n := active.Add(1)
		if n > peak.Load() {
			peak.Store(n)
		}
		defer active.Add(-1)
		started <- fc.Name
		if fc.Name == "old" {
			<-ctx.Done()
			close(oldCanceled)
			<-release
			panic("old generation failed")
		}
		<-ctx.Done()
	}
	c.submit(models.FaultCenter{ID: "fc", Name: "old"}, run)
	worker := wakeConsumer(c, "fc")
	select {
	case name := <-started:
		if name != "old" {
			t.Fatal(name)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("old not started")
	}
	for _, name := range []string{"middle", "latest"} {
		c.submit(models.FaultCenter{ID: "fc", Name: name}, run)
	}
	awaitSignal(t, oldCanceled)
	select {
	case name := <-started:
		t.Fatal("overlapping generation", name)
	default:
	}
	once.Do(func() { close(release) })
	select {
	case name := <-started:
		if name != "latest" {
			t.Fatal("stale configuration used", name)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("latest not started")
	}
	c.Stop("fc")
	awaitSignal(t, worker.done)
	if peak.Load() != 1 {
		t.Fatal("overlapping cycles", peak.Load())
	}
	c.RLock()
	remaining := len(c.workers)
	c.RUnlock()
	if remaining != 0 {
		t.Fatal("stopped worker not removed")
	}
}

func TestStopThenSubmitReusesDrainingWorkerAndStopAllIsScoped(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	foreign, foreignCancel := context.WithCancel(context.Background())
	defer foreignCancel()
	c := &Consume{ctx: &appctx.Context{Ctx: parent, ContextMap: map[string]context.CancelFunc{"rule": foreignCancel}}}
	started := make(chan struct{}, 10)
	release := make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	var count atomic.Int32
	run := func(ctx context.Context, _ models.FaultCenter) {
		count.Add(1)
		started <- struct{}{}
		<-ctx.Done()
		<-release
	}
	c.submit(models.FaultCenter{ID: "fc"}, run)
	w := wakeConsumer(c, "fc")
	awaitSignal(t, started)
	c.Stop("fc")
	c.submit(models.FaultCenter{ID: "fc", Name: "new"}, run)
	c.RLock()
	current := c.workers["fc"]
	c.RUnlock()
	if current != w || count.Load() != 1 {
		t.Fatal("replacement overlapped draining worker")
	}
	once.Do(func() { close(release) })
	awaitSignal(t, started)
	c.StopAllConsumers()
	awaitSignal(t, w.done)
	if foreign.Err() != nil {
		t.Fatal("consumer shutdown canceled another subsystem")
	}
}

func TestConsumerParentCancellationStopsWorkerAndBlocksNewSubmit(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	c := &Consume{ctx: &appctx.Context{Ctx: parent}}
	started := make(chan struct{}, 1)
	c.submit(models.FaultCenter{ID: "fc"}, func(ctx context.Context, _ models.FaultCenter) { started <- struct{}{}; <-ctx.Done() })
	w := wakeConsumer(c, "fc")
	awaitSignal(t, started)
	cancel()
	awaitSignal(t, w.done)
	c.submit(models.FaultCenter{ID: "other"}, func(context.Context, models.FaultCenter) { t.Error("started after shutdown") })
	c.RLock()
	remaining := len(c.workers)
	c.RUnlock()
	if remaining != 0 {
		t.Fatal("accepted work after parent cancellation")
	}
}
