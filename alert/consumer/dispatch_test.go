package consumer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"watchAlert/internal/models"
)

func awaitSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for worker")
	}
}

func notificationTestGroups(count int, event *models.AlertCurEvent) *AlertGroups {
	groups := make(map[string]EventsGroup, count)
	for i := 0; i < count; i++ {
		id := fmt.Sprint(i)
		groups[id] = EventsGroup{NoticeID: id, Events: []*models.AlertCurEvent{event}}
	}
	return &AlertGroups{Rules: map[string]RulesGroup{"rule": {Groups: groups}}}
}

func TestGroupDispatcherBoundsConcurrencyAndCopiesSharedSources(t *testing.T) {
	const jobs = 100
	source := notificationEvent("same", "prod")
	started := make(chan struct{}, jobs)
	release := make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	var active, peak, completed atomic.Int32
	done := make(chan struct{})
	go func() {
		defer close(done)
		dispatchNotificationGroups(context.Background(), notificationTestGroups(jobs, source), func(group EventsGroup) {
			current := active.Add(1)
			for old := peak.Load(); current > old; old = peak.Load() {
				if peak.CompareAndSwap(old, current) {
					break
				}
			}
			defer active.Add(-1)
			if group.Events[0] == source {
				t.Error("shared mutable event passed to concurrent groups")
			}
			group.Events[0].LastSendTime = 123
			group.Events[0].ConfirmState.IsOk = true
			started <- struct{}{}
			<-release
			completed.Add(1)
		})
	}()
	for i := 0; i < notificationGroupWorkers; i++ {
		awaitSignal(t, started)
	}
	if active.Load() != notificationGroupWorkers {
		t.Fatal("groups stayed serial", active.Load())
	}
	select {
	case <-started:
		t.Fatal("unbounded group dispatch")
	default:
	}
	once.Do(func() { close(release) })
	awaitSignal(t, done)
	if completed.Load() != jobs || peak.Load() > notificationGroupWorkers || source.LastSendTime != 0 || source.ConfirmState.IsOk {
		t.Fatal("dropped work, excess concurrency or source mutation", completed.Load(), peak.Load(), source)
	}
}

func TestSendSlotsAreGlobalBoundedAndCancellationReleasesWaiters(t *testing.T) {
	requestCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{}, notificationSendLimit)
	release := make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	var workers sync.WaitGroup
	for i := 0; i < notificationSendLimit; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			if err := withNotificationSlot(context.Background(), func() error { started <- struct{}{}; <-release; return nil }); err != nil {
				t.Error(err)
			}
		}()
	}
	for i := 0; i < notificationSendLimit; i++ {
		awaitSignal(t, started)
	}
	result := make(chan error, 1)
	go func() {
		result <- withNotificationSlot(requestCtx, func() error { t.Error("canceled waiter acquired work"); return nil })
	}()
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("slot wait ignored cancellation")
	}
	once.Do(func() { close(release) })
	workers.Wait()
	if len(notificationSlots) != 0 {
		t.Fatal("slots leaked")
	}
	if err := withNotificationSlot(context.Background(), func() error { panic("bad template") }); err == nil {
		t.Fatal("panic not reported")
	}
	if len(notificationSlots) != 0 {
		t.Fatal("panic leaked slot")
	}
}

func TestDispatcherCancellationDoesNotDrainBacklogIntoSends(t *testing.T) {
	requestCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{}, notificationGroupWorkers)
	var sent atomic.Int32
	done := make(chan struct{})
	go func() {
		defer close(done)
		dispatchNotificationGroups(requestCtx, notificationTestGroups(100, notificationEvent("same", "prod")), func(EventsGroup) { sent.Add(1); started <- struct{}{}; <-requestCtx.Done() })
	}()
	for i := 0; i < notificationGroupWorkers; i++ {
		awaitSignal(t, started)
	}
	cancel()
	awaitSignal(t, done)
	if sent.Load() != notificationGroupWorkers {
		t.Fatal("canceled backlog sent", sent.Load())
	}
}

func TestConsumerParallelGroupsSendRealLocalHTTPWithoutMutatingSource(t *testing.T) {
	f := newNotificationFixture(t)
	started := make(chan struct{}, 12)
	release := make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	f.afterSend = func() { started <- struct{}{}; <-release }
	source := notificationEvent("same", "prod")
	c := &Consume{ctx: f.app}
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.sendAlerts(context.Background(), f.center, notificationTestGroups(12, source))
	}()
	for i := 0; i < notificationGroupWorkers; i++ {
		awaitSignal(t, started)
	}
	once.Do(func() { close(release) })
	awaitSignal(t, done)
	if len(f.payloads) != 12 || f.clock.calls != 12 || source.LastSendTime != 0 || source.DutyUser != "" || source.Annotations != "original" {
		t.Fatal("parallel send changed source or lost work", len(f.payloads), f.clock.calls)
	}
}

func TestGroupPanicDoesNotTerminateRemainingWork(t *testing.T) {
	var completed atomic.Int32
	dispatchNotificationGroups(context.Background(), notificationTestGroups(20, notificationEvent("same", "prod")), func(group EventsGroup) {
		if group.NoticeID == "0" {
			panic("fixture")
		}
		completed.Add(1)
	})
	if completed.Load() != 19 {
		t.Fatal("panic stopped other groups", completed.Load())
	}
}

func TestQueuedNotificationDoesNotReadSilencesOrAdvanceClock(t *testing.T) {
	f := newNotificationFixture(t)
	for i := 0; i < notificationSendLimit; i++ {
		notificationSlots <- struct{}{}
	}
	defer func() {
		for i := 0; i < notificationSendLimit; i++ {
			<-notificationSlots
		}
	}()
	requestCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		result <- handleAlert(requestCtx, f.app, "alarm", f.center, "notice", []*models.AlertCurEvent{notificationEvent("one", "prod")})
	}()
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("queued send did not cancel")
	}
	if f.clock.calls != 0 || f.silence.reads != 0 || len(f.payloads) != 0 {
		t.Fatal("queued job mutated state or sent")
	}
}
