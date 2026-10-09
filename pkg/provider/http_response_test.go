package provider

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func queryHTTPProvider(kind, url string, ctx context.Context) error {
	switch kind {
	case "Loki":
		_, _, err := (LokiProvider{Url: url, Timeout: 5}).QueryContext(ctx, LogQueryOptions{Loki: Loki{Query: `{app="test"}`}})
		return err
	case "VictoriaLogs":
		_, _, err := (VictoriaLogsProvider{URL: url, Timeout: 5}).QueryContext(ctx, LogQueryOptions{VictoriaLogs: VictoriaLogs{Query: "*"}})
		return err
	case "Jaeger":
		_, err := (JaegerDsProvider{url: url}).QueryContext(ctx, TraceQueryOptions{Service: "test"})
		return err
	}
	return fmt.Errorf("unknown provider")
}

func TestHTTPProvidersValidateResults(t *testing.T) {
	for _, tc := range []struct {
		name, provider, body string
		status               int
		wantError            bool
	}{
		{"loki-empty", "Loki", `{"status":"success","data":{"resultType":"streams","result":[]}}`, 200, false},
		{"loki-error-status", "Loki", `{"status":"error"}`, 200, true},
		{"loki-wrong-type", "Loki", `{"status":"success","data":{"resultType":"vector","result":[]}}`, 200, true},
		{"loki-missing-result", "Loki", `{"status":"success","data":{"resultType":"streams"}}`, 200, true},
		{"loki-http-error", "Loki", `{"data":{"result":[]}}`, 503, true},
		{"victoria-empty", "VictoriaLogs", "", 200, false},
		{"victoria-lines", "VictoriaLogs", "{\"message\":\"one\"}\n{\"message\":\"two\"}\n", 200, false},
		{"victoria-long-line", "VictoriaLogs", `{"message":"` + strings.Repeat("x", 100000) + `"}`, 200, false},
		{"victoria-invalid-line", "VictoriaLogs", "{}\nnot-json\n", 200, true},
		{"victoria-http-error", "VictoriaLogs", "", 500, true},
		{"jaeger-empty", "Jaeger", `{"data":[],"errors":null}`, 200, false},
		{"jaeger-null", "Jaeger", `{"data":null,"errors":null}`, 200, false},
		{"jaeger-valid", "Jaeger", `{"data":[{"traceID":"abc"}]}`, 200, false},
		{"jaeger-partial", "Jaeger", `{"data":[{"traceID":"abc"}],"errors":[{"msg":"timeout"}]}`, 200, true},
		{"jaeger-missing", "Jaeger", `{}`, 200, true},
		{"jaeger-http-error", "Jaeger", `{"data":[]}`, 401, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.body)
			}))
			defer s.Close()
			if err := queryHTTPProvider(tc.provider, s.URL, context.Background()); (err != nil) != tc.wantError {
				t.Fatal("unexpected error state", err)
			}
		})
	}
}

func TestHTTPProvidersRejectTruncatedBodiesAndCancel(t *testing.T) {
	for _, kind := range []string{"Loki", "VictoriaLogs", "Jaeger"} {
		t.Run(kind+"/truncated", func(t *testing.T) {
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Length", "1000")
				_, _ = io.WriteString(w, "{}")
			}))
			defer s.Close()
			if err := queryHTTPProvider(kind, s.URL, context.Background()); err == nil {
				t.Fatal("truncation reported success")
			}
		})
		t.Run(kind+"/cancel", func(t *testing.T) {
			started := make(chan struct{})
			cleanup := make(chan struct{})
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				close(started)
				select {
				case <-r.Context().Done():
				case <-cleanup:
				}
			}))
			defer s.Close()
			defer close(cleanup)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- queryHTTPProvider(kind, s.URL, ctx) }()
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("request not started")
			}
			cancel()
			select {
			case err := <-done:
				if err == nil {
					t.Fatal("cancelled query succeeded")
				}
			case <-time.After(time.Second):
				t.Fatal("query did not stop")
			}
		})
	}
}

type trackingBody struct {
	io.Reader
	closed bool
}

func (b *trackingBody) Close() error { b.closed = true; return nil }

func TestQueryBodyBudgetAndClose(t *testing.T) {
	for _, tc := range []struct {
		status, size int
		wantError    bool
	}{{200, 0, false}, {200, maxQueryBodyBytes, false}, {200, maxQueryBodyBytes + 1, true}, {503, 10, true}} {
		body := &trackingBody{Reader: strings.NewReader(strings.Repeat("x", tc.size))}
		_, err := readQueryBody(&http.Response{StatusCode: tc.status, Body: body})
		if !body.closed || (err != nil) != tc.wantError {
			t.Fatal("body not closed or budget ignored", err, body.closed)
		}
	}
}
