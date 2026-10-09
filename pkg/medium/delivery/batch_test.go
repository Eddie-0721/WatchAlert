package delivery

import (
	"context"
	"errors"
	"testing"
	"time"
	"watchAlert/internal/models"
	"watchAlert/internal/types"
)

func testMessage() *types.Message {
	return &types.Message{ToUsers: []models.Member{{Phone: "10001"}, {Phone: "10002"}, {Phone: "10003"}}, Labels: map[string]any{"service": "test"}}
}

func TestBatchCancellationReportsPartialResultAndStopsNewCalls(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	result, err := SendBatch(ctx, testMessage(), func(requestCtx context.Context, content, phone string) error {
		calls++
		deadline, ok := requestCtx.Deadline()
		if !ok || time.Until(deadline) > RequestTimeout {
			t.Fatal("missing batch budget")
		}
		if content != `{"service":"test"}` {
			t.Fatal(content)
		}
		if calls == 2 {
			cancel()
			return requestCtx.Err()
		}
		return nil
	})
	if !errors.Is(err, context.Canceled) || result.Success || calls != 2 {
		t.Fatalf("%v %+v calls=%d", err, result, calls)
	}
	if got := result.Data.(BatchSummary); got != (BatchSummary{Accepted: 1, Failed: 1, NotAttempted: 1}) {
		t.Fatal(got)
	}
}

func TestBatchDoesNotRetryFailedRecipientsOrSkipOtherRecipients(t *testing.T) {
	calls := 0
	result, err := SendBatch(context.Background(), testMessage(), func(context.Context, string, string) error {
		calls++
		if calls == 2 {
			return errors.New("provider rejected")
		}
		return nil
	})
	if err == nil || calls != 3 || result.Success {
		t.Fatalf("%v %+v calls=%d", err, result, calls)
	}
	if got := result.Data.(BatchSummary); got != (BatchSummary{Accepted: 2, Failed: 1}) {
		t.Fatal(got)
	}
}

func TestBatchEmptyOrCanceledDoesNotSend(t *testing.T) {
	for _, msg := range []*types.Message{nil, {}, {ToUsers: []models.Member{{Phone: "  "}}}} {
		if _, err := SendBatch(context.Background(), msg, func(context.Context, string, string) error { t.Fatal("unexpected send"); return nil }); err == nil {
			t.Fatal("accepted empty recipients")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r, err := SendBatch(ctx, testMessage(), func(context.Context, string, string) error { t.Fatal("sent after cancellation"); return nil })
	if !errors.Is(err, context.Canceled) || r.Success || r.Data.(BatchSummary).NotAttempted != 3 {
		t.Fatalf("%v %+v", err, r)
	}
}

func TestSlowRecipientDoesNotExhaustLaterRecipientBudget(t *testing.T) {
	calls := 0
	result, err := sendBatch(context.Background(), testMessage(), 20*time.Millisecond, func(ctx context.Context, _ string, _ string) error {
		calls++
		if calls == 1 {
			<-ctx.Done()
			return ctx.Err()
		}
		if ctx.Err() != nil {
			t.Fatal("previous recipient consumed next budget")
		}
		return nil
	})
	if !errors.Is(err, context.DeadlineExceeded) || calls != 3 || result.Data.(BatchSummary) != (BatchSummary{Accepted: 2, Failed: 1}) {
		t.Fatalf("%v %+v calls=%d", err, result, calls)
	}
}
