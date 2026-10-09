package phone

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
	"watchAlert/internal/models"
	"watchAlert/internal/types"
	"watchAlert/pkg/medium/delivery"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func recipients() *types.Message {
	return &types.Message{ToUsers: []models.Member{{Phone: "10001"}, {Phone: "10002"}}, Labels: map[string]any{"service": "test"}}
}

func TestAliyunPhoneUsesSDKAndStrictResultWithoutRetries(t *testing.T) {
	for _, tc := range []struct {
		body   string
		status int
		fail   bool
	}{
		{`{"Code":"OK","Message":"accepted"}`, 200, false},
		{`{"Code":"Rejected","Message":"OK secret-token"}`, 200, true},
		{`{"Message":"OK"}`, 200, true},
		{`{"Code":"OK","Message":"secret-token"}`, 500, true},
		{strings.Repeat("x", delivery.MaxResponseBytes+1), 200, true},
	} {
		n := NewAliyunPhoneNotifier("test-id", "test-secret", "caller", "template")
		calls := 0
		n.transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
			calls++
			if r.Context().Err() != nil {
				return nil, r.Context().Err()
			}
			if !strings.Contains(r.URL.Host, "aliyuncs.com") {
				t.Fatalf("unexpected SDK endpoint %s", r.URL.Host)
			}
			// Returning a fixture here means no real service is contacted.
			return &http.Response{StatusCode: tc.status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(tc.body))}, nil
		})
		result, err := n.Notify(context.Background(), recipients())
		if (err != nil) != tc.fail || result.Success == tc.fail || calls != 2 {
			t.Fatalf("body=%q err=%v result=%+v calls=%d", tc.body[:min(len(tc.body), 100)], err, result, calls)
		}
		if err != nil && strings.Contains(err.Error(), "secret-token") {
			t.Fatal("provider payload leaked", err)
		}
	}
}

func TestAliyunPhoneCancelInterruptsSDKAndStopsRemainingRecipients(t *testing.T) {
	n := NewAliyunPhoneNotifier("test-id", "test-secret", "caller", "template")
	started := make(chan struct{})
	calls := 0
	n.transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		calls++
		close(started)
		<-r.Context().Done()
		return nil, r.Context().Err()
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan struct{})
	var result *types.NotificationResult
	var err error
	go func() { defer close(done); result, err = n.Notify(ctx, recipients()) }()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("SDK never sent request")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("SDK ignored cancellation")
	}
	if !errors.Is(err, context.Canceled) || calls != 1 || result.Success || result.Data.(delivery.BatchSummary).NotAttempted != 1 {
		t.Fatalf("%v %+v calls=%d", err, result, calls)
	}
}

func TestTencentPhoneRejectsUnknownAndFailedResponses(t *testing.T) {
	for _, tc := range []struct {
		body   string
		status int
		fail   bool
	}{
		{`{"result":0}`, 200, false},
		{`{"result":1,"errmsg":"unsuccessful secret-token"}`, 200, true},
		{`{"errmsg":"success"}`, 200, true},
		{`{"result":0}`, 500, true},
		{strings.Repeat("x", delivery.MaxResponseBytes+1), 200, true},
	} {
		n := NewTencentPhoneNotifier("test-id", "test-key", "test-app")
		calls := 0
		n.postHTTP = func(ctx context.Context, url string, body *bytes.Reader) (*http.Response, error) {
			calls++
			if _, ok := ctx.Deadline(); !ok {
				t.Fatal("missing batch deadline")
			}
			return &http.Response{StatusCode: tc.status, Body: io.NopCloser(strings.NewReader(tc.body))}, nil
		}
		msg := recipients()
		msg.ToUsers = msg.ToUsers[:1]
		result, err := n.Notify(context.Background(), msg)
		if (err != nil) != tc.fail || result.Success == tc.fail || calls != 1 {
			t.Fatalf("%v %+v calls=%d", err, result, calls)
		}
		if err != nil && strings.Contains(err.Error(), "secret-token") {
			t.Fatal(err)
		}
	}
}

func TestTencentPhoneCancelStopsRemainingRecipients(t *testing.T) {
	n := NewTencentPhoneNotifier("test-id", "test-key", "test-app")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	n.postHTTP = func(requestCtx context.Context, url string, body *bytes.Reader) (*http.Response, error) {
		calls++
		cancel()
		return nil, requestCtx.Err()
	}
	result, err := n.Notify(ctx, recipients())
	if !errors.Is(err, context.Canceled) || calls != 1 || result.Success || result.Data.(delivery.BatchSummary).NotAttempted != 1 {
		t.Fatalf("%v %+v calls=%d", err, result, calls)
	}
}
