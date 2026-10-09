package provider

import (
	"context"
	"testing"
	"time"
)

func TestProbeBudgetDefaultsAndBounds(t *testing.T) {
	for _, tc := range []struct{ input, want int }{{0, 32}, {-1, 32}, {1, 1}, {12, 12}, {256, 256}, {1000, 256}} {
		if got := probeConcurrencyLimit(tc.input); got != tc.want {
			t.Fatal(tc, got)
		}
	}
}

func TestProbeSlotsCancelRejectAndReleaseExactlyOnce(t *testing.T) {
	slots := make(chan struct{}, 2)
	first, err := acquireProbeSlot(context.Background(), slots, true)
	if err != nil {
		t.Fatal(err)
	}
	defer first()
	second, err := acquireProbeSlot(context.Background(), slots, false)
	if err != nil {
		t.Fatal(err)
	}
	defer second()
	if release, err := acquireProbeSlot(context.Background(), slots, false); release != nil || err != ErrProbeBusy {
		t.Fatal("overload was not rejected", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if release, err := acquireProbeSlot(ctx, slots, true); release != nil || err != context.DeadlineExceeded {
		t.Fatal("waiting did not cancel", err)
	}
	if len(slots) != 2 {
		t.Fatal("canceled waiter stole a slot")
	}
	first()
	first()
	if len(slots) != 1 {
		t.Fatal("release was not idempotent")
	}
	ctx, cancel = context.WithCancel(context.Background())
	cancel()
	if release, err := acquireProbeSlot(ctx, slots, false); release != nil || err != context.Canceled {
		t.Fatal("pre-cancel ignored", err)
	}
	if len(slots) != 1 {
		t.Fatal("pre-canceled caller acquired slot")
	}
}
