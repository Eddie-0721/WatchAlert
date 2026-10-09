package probe

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	appctx "watchAlert/internal/ctx"
	"watchAlert/internal/models"
	"watchAlert/pkg/provider"
)

func awaitProbeDone(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("probe worker did not exit")
	}
}

func TestProbeReloadCoalescesWhileOldCycleDrains(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := NewProbeService(&appctx.Context{Ctx: parent})
	started := make(chan string, 4)
	finishOld := make(chan struct{})
	var finishOnce sync.Once
	releaseOld := func() { finishOnce.Do(func() { close(finishOld) }) }
	var active, peak atomic.Int32
	execute := func(ctx context.Context, rule models.ProbeRule) {
		n := active.Add(1)
		defer active.Add(-1)
		for previous := peak.Load(); n > previous && !peak.CompareAndSwap(previous, n); previous = peak.Load() {
		}
		started <- rule.RuleName
		if rule.RuleName == "old" {
			<-finishOld // Deliberately ignores cancellation, then panics.
			panic("old generation cleanup fixture")
		}
		<-ctx.Done()
	}
	rule := validProbeRule("unused.invalid")
	rule.RuleName = "old"
	if err := s.add(rule, execute); err != nil {
		t.Fatal(err)
	}
	select {
	case name := <-started:
		if name != "old" {
			t.Fatal(name)
		}
	case <-time.After(time.Second):
		t.Fatal("initial cycle missing")
	}
	s.mu.RLock()
	worker := s.workers[rule.RuleId]
	s.mu.RUnlock()
	defer func() { cancel(); releaseOld(); awaitProbeDone(t, worker.done) }()
	for _, name := range []string{"intermediate", "latest"} {
		if err := s.Stop(rule.RuleId); err != nil {
			t.Fatal(err)
		}
		rule.RuleName = name
		if err := s.add(rule, execute); err != nil {
			t.Fatal(err)
		}
	}
	s.mu.RLock()
	sameWorker := s.workers[rule.RuleId] == worker
	s.mu.RUnlock()
	if !sameWorker || active.Load() != 1 || s.GetActiveRules() != 1 {
		t.Fatal("reload replaced a draining worker")
	}
	if err := s.add(rule, execute); err == nil {
		t.Fatal("active duplicate Add must remain an error")
	}
	select {
	case name := <-started:
		t.Fatal("new cycle overlapped old generation", name)
	default:
	}
	releaseOld()
	select {
	case name := <-started:
		if name != "latest" {
			t.Fatal("stale reload executed", name)
		}
	case <-time.After(time.Second):
		t.Fatal("latest generation did not execute")
	}
	if peak.Load() != 1 {
		t.Fatal("same-rule execution overlapped", peak.Load())
	}
	if err := s.Stop(rule.RuleId); err != nil {
		t.Fatal(err)
	}
	awaitProbeDone(t, worker.done)
	if s.GetActiveRules() != 0 {
		t.Fatal("stopped worker still counted")
	}
	s.mu.RLock()
	remaining := len(s.workers)
	s.mu.RUnlock()
	if remaining != 0 {
		t.Fatal("worker registry leaked")
	}
}

func TestProbeReloadWhileWaitingRunsOnlyLatestConfiguration(t *testing.T) {
	var leases []func()
	defer func() {
		for _, release := range leases {
			release()
		}
	}()
	for i := 0; i < 32; i++ {
		release, err := provider.TryAcquireProbeSlot(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		leases = append(leases, release)
	}
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := NewProbeService(&appctx.Context{Ctx: parent})
	started := make(chan string, 2)
	execute := func(ctx context.Context, rule models.ProbeRule) { started <- rule.RuleName; <-ctx.Done() }
	rule := validProbeRule("unused.invalid")
	rule.RuleName = "old"
	if err := s.add(rule, execute); err != nil {
		t.Fatal(err)
	}
	s.mu.RLock()
	worker := s.workers[rule.RuleId]
	s.mu.RUnlock()
	defer func() { cancel(); awaitProbeDone(t, worker.done) }()
	deadline := time.Now().Add(time.Second)
	for {
		s.mu.RLock()
		waiting := worker.cancel != nil
		s.mu.RUnlock()
		if waiting {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("worker not initialized")
		}
		time.Sleep(time.Millisecond)
	}
	if err := s.Stop(rule.RuleId); err != nil {
		t.Fatal(err)
	}
	rule.RuleName = "latest"
	if err := s.add(rule, execute); err != nil {
		t.Fatal(err)
	}
	leases[0]()
	select {
	case name := <-started:
		if name != "latest" {
			t.Fatal("canceled waiter executed stale configuration", name)
		}
	case <-time.After(time.Second):
		t.Fatal("latest config remained blocked")
	}
}

func TestProbeRealScheduledHTTPRequestsAreBounded(t *testing.T) {
	started := make(chan struct{}, 40)
	cleanup := make(chan struct{})
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		started <- struct{}{}
		select {
		case <-r.Context().Done():
		case <-cleanup:
		}
	}))
	defer server.Close()
	defer close(cleanup)
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := NewProbeService(&appctx.Context{Ctx: parent})
	for i := 0; i < 40; i++ {
		rule := validProbeRule(server.URL)
		rule.RuleId = fmt.Sprintf("http-%d", i)
		if err := s.Add(rule); err != nil {
			t.Fatal(err)
		}
	}
	s.mu.RLock()
	var done []<-chan struct{}
	for _, worker := range s.workers {
		done = append(done, worker.done)
	}
	s.mu.RUnlock()
	defer func() {
		cancel()
		for _, ch := range done {
			awaitProbeDone(t, ch)
		}
	}()
	for i := 0; i < 32; i++ {
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			t.Fatal("HTTP capacity was not reached")
		}
	}
	if calls.Load() != 32 {
		t.Fatal("scheduled HTTP concurrency exceeded capacity", calls.Load())
	}
	if release, err := provider.TryAcquireProbeSlot(parent); err != provider.ErrProbeBusy {
		if release != nil {
			release()
		}
		t.Fatal("HTTP cycles did not hold shared capacity", err)
	}
	if err := s.StopAll(); err != nil {
		t.Fatal(err)
	}
	for _, ch := range done {
		awaitProbeDone(t, ch)
	}
}

func TestProbeScheduledCyclesShareBoundedSlotsAndCancelWaiters(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := NewProbeService(&appctx.Context{Ctx: parent})
	started := make(chan struct{}, 80)
	var active, peak atomic.Int32
	execute := func(ctx context.Context, rule models.ProbeRule) {
		n := active.Add(1)
		defer active.Add(-1)
		for previous := peak.Load(); n > previous && !peak.CompareAndSwap(previous, n); previous = peak.Load() {
		}
		started <- struct{}{}
		<-ctx.Done()
	}
	for i := 0; i < 80; i++ {
		rule := validProbeRule("unused.invalid")
		rule.RuleId = fmt.Sprintf("probe-%d", i)
		if err := s.add(rule, execute); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 32; i++ {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("default probe capacity was not reached")
		}
	}
	if release, err := provider.TryAcquireProbeSlot(context.Background()); err != provider.ErrProbeBusy {
		if release != nil {
			release()
		}
		t.Fatal("interactive calls did not share scheduled capacity", err)
	}
	s.mu.RLock()
	done := make([]<-chan struct{}, 0, len(s.workers))
	for _, worker := range s.workers {
		done = append(done, worker.done)
	}
	s.mu.RUnlock()
	if err := s.StopAll(); err != nil {
		t.Fatal(err)
	}
	for _, ch := range done {
		awaitProbeDone(t, ch)
	}
	if active.Load() != 0 || peak.Load() > 32 || s.GetActiveRules() != 0 {
		t.Fatal("capacity/cancellation invariant failed", active.Load(), peak.Load())
	}
	// Every lease was released, including workers canceled while waiting.
	var releases []func()
	defer func() {
		for _, release := range releases {
			release()
		}
	}()
	for i := 0; i < 32; i++ {
		release, err := provider.TryAcquireProbeSlot(context.Background())
		if err != nil {
			t.Fatal("slot leaked", err)
		}
		releases = append(releases, release)
	}
}

func TestProbeParentCancellationCleansIdleWorkerAndRejectsNewTasks(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := NewProbeService(&appctx.Context{Ctx: parent})
	completed := make(chan struct{})
	rule := validProbeRule("unused.invalid")
	if err := s.add(rule, func(context.Context, models.ProbeRule) { close(completed) }); err != nil {
		t.Fatal(err)
	}
	awaitProbeDone(t, completed)
	s.mu.RLock()
	worker := s.workers[rule.RuleId]
	s.mu.RUnlock()
	cancel()
	awaitProbeDone(t, worker.done)
	if s.GetActiveRules() != 0 {
		t.Fatal("parent-canceled worker leaked")
	}
	if err := s.Add(rule); err != context.Canceled {
		t.Fatal("add ignored parent cancellation", err)
	}
}
