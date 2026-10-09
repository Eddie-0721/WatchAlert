package medium

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func notificationSenders() map[string]SendInter {
	return map[string]SendInter{
		"webhook": NewWebHookSender(), "sreflow": NewSREFlowSender(),
		"slack": NewSlackSender(), "wechat": NewWeChatSender(),
		"dingding": NewDingSender(), "feishu": NewFeiShuSender(),
	}
}

func TestNotificationRejectsHTTPFailuresAndOversizedBodies(t *testing.T) {
	for name, sender := range notificationSenders() {
		for _, method := range []string{"send", "test"} {
			for _, tc := range []struct {
				name, body string
				status     int
			}{
				{"server_failure", `{"code":0,"errcode":0,"private":"never echo this"}`, 500},
				{"rate_limited", "never echo this", 429},
				{"large_success", strings.Repeat("x", maxNotificationResponseBytes+1), 200},
				{"large_failure", strings.Repeat("x", maxNotificationResponseBytes+1), 500},
			} {
				t.Run(name+"/"+method+"/"+tc.name, func(t *testing.T) {
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						w.WriteHeader(tc.status)
						_, _ = io.WriteString(w, tc.body)
					}))
					defer server.Close()
					params := SendParams{Hook: server.URL, Content: "{}"}
					call := sender.Send
					if method == "test" {
						call = sender.Test
					}
					err := call(params)
					if err == nil {
						t.Fatal("invalid response accepted")
					}
					if strings.Contains(err.Error(), "never echo this") {
						t.Fatal("upstream body leaked")
					}
				})
			}
		}
	}
}

func TestRobotAcknowledgementRequiresExplicitIntegerZero(t *testing.T) {
	for _, field := range []string{"code", "errcode"} {
		for _, body := range []string{"{}", "null", "[]", "invalid", `{"` + field + `":null}`, `{"` + field + `":"0"}`, `{"` + field + `":1}`, `{"` + field + `":0} trailing`} {
			response := &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(body))}
			if err := checkRobotResponse(response, field); err == nil {
				t.Fatalf("accepted %s", body)
			}
		}
		response := &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"` + field + `":0}`))}
		if err := checkRobotResponse(response, field); err != nil {
			t.Fatal(err)
		}
	}
}

func TestNotificationCancellationInterruptsHeadersAndBody(t *testing.T) {
	for name, sender := range notificationSenders() {
		for _, flush := range []bool{false, true} {
			t.Run(name+map[bool]string{true: "/body", false: "/headers"}[flush], func(t *testing.T) {
				started := make(chan struct{})
				disconnected := make(chan struct{})
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					_, _ = io.Copy(io.Discard, r.Body)
					if flush {
						w.WriteHeader(200)
						w.(http.Flusher).Flush()
					}
					close(started)
					<-r.Context().Done()
					close(disconnected)
				}))
				defer server.Close()
				requestCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				result := make(chan error, 1)
				go func() { result <- sender.Send(SendParams{RequestContext: requestCtx, Hook: server.URL, Content: "{}"}) }()
				select {
				case <-started:
				case <-requestCtx.Done():
					t.Fatal("request never started")
				}
				cancel()
				select {
				case err := <-result:
					if !errors.Is(err, context.Canceled) {
						t.Fatalf("wanted cancellation, got %v", err)
					}
				case <-time.After(time.Second):
					t.Fatal("canceled request remained blocked")
				}
				select {
				case <-disconnected:
				case <-time.After(time.Second):
					t.Fatal("underlying request still running")
				}
			})
		}
	}
}

type countingResponseBody struct {
	read   int
	closed bool
}

func (b *countingResponseBody) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 'x'
	}
	b.read += len(p)
	return len(p), nil
}
func (b *countingResponseBody) Close() error { b.closed = true; return nil }

func TestNotificationResponseReadHasHardByteLimit(t *testing.T) {
	body := &countingResponseBody{}
	_, err := readNotificationResponse(&http.Response{StatusCode: 200, Body: body})
	if err == nil || !body.closed || body.read != maxNotificationResponseBytes+1 {
		t.Fatalf("err=%v closed=%v bytes=%d", err, body.closed, body.read)
	}
}
