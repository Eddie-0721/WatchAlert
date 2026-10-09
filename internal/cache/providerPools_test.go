package cache

import (
	"sync"
	"sync/atomic"
	"testing"
)

type trackedProvider struct{ closed atomic.Int32 }

func (p *trackedProvider) Close() error { p.closed.Add(1); return nil }

func TestProviderReplacementWaitsForLastLease(t *testing.T) {
	p := NewClientPoolStore()
	old, current := &trackedProvider{}, &trackedProvider{}
	p.SetClient("ds", old)
	_, first, err := p.AcquireClient("ds")
	if err != nil {
		t.Fatal(err)
	}
	_, second, _ := p.AcquireClient("ds")
	p.SetClient("ds", current)
	if old.closed.Load() != 0 {
		t.Fatal("closed in-flight client")
	}
	first()
	first()
	if old.closed.Load() != 0 {
		t.Fatal("closed before final lease")
	}
	second()
	if old.closed.Load() != 1 {
		t.Fatal("old client not closed exactly once")
	}
	got, release, err := p.AcquireClient("ds")
	if err != nil || got != current {
		t.Fatal("replacement not visible", err)
	}
	p.RemoveClient("ds")
	if current.closed.Load() != 0 {
		t.Fatal("removed in-flight client closed")
	}
	if _, noop, err := p.AcquireClient("ds"); err == nil {
		t.Fatal("removed client still visible")
	} else {
		noop()
	}
	release()
	release()
	p.RemoveClient("ds")
	if current.closed.Load() != 1 {
		t.Fatal("removed client not closed exactly once")
	}
}

func TestConcurrentProviderRefreshAndAcquisition(t *testing.T) {
	p := NewClientPoolStore()
	var created []*trackedProvider
	var writers sync.Mutex
	var wg sync.WaitGroup
	for n := 0; n < 12; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				client := &trackedProvider{}
				writers.Lock()
				created = append(created, client)
				writers.Unlock()
				p.SetClient("ds", client)
				got, release, err := p.AcquireClient("ds")
				if err != nil {
					t.Error(err)
					return
				}
				if got.(*trackedProvider).closed.Load() != 0 {
					t.Error("acquired a closed client")
				}
				release()
			}
		}()
	}
	wg.Wait()
	p.RemoveClient("ds")
	for _, client := range created {
		if client.closed.Load() != 1 {
			t.Fatal("leaked/double-closed client", client.closed.Load())
		}
	}
}
