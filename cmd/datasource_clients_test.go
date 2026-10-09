package main

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"
	"watchAlert/internal/models"
)

func TestStartupClientsAreBoundedAndAwaited(t *testing.T) {
	enabled, disabled := true, false
	list := make([]models.AlertDataSource, 65)
	for i := range list {
		list[i].Enabled = &enabled
	}
	list[64].Enabled = &disabled
	var active, peak, completed atomic.Int32
	err := initializeDatasourceClients(list, func(models.AlertDataSource) error {
		now := active.Add(1)
		defer active.Add(-1)
		for old := peak.Load(); now > old && !peak.CompareAndSwap(old, now); old = peak.Load() {
		}
		time.Sleep(time.Millisecond)
		n := completed.Add(1)
		if n == 1 {
			return errors.New("one invalid configuration")
		}
		return nil
	})
	if err == nil || active.Load() != 0 || completed.Load() != 64 || peak.Load() > 8 || peak.Load() < 2 {
		t.Fatal(err, active.Load(), completed.Load(), peak.Load())
	}
}
