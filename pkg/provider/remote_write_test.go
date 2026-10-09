package provider

import (
	"bytes"
	"context"
	"errors"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
	"watchAlert/internal/models"
)

type remoteWriteTransport func(*http.Request) (*http.Response, error)

func (f remoteWriteTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type remoteWriteBody struct {
	reader io.Reader
	read   int
	closed bool
}

func TestRemoteWriteRealSlowErrorBodyCanBeCancelledOrExpire(t *testing.T) {
	for _, ownTimeout := range []bool{false, true} {
		name := "caller-cancel"
		if ownTimeout {
			name = "configured-timeout"
		}
		t.Run(name, func(t *testing.T) {
			started, stopped, cleanup := make(chan struct{}), make(chan struct{}), make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				w.WriteHeader(503)
				_, _ = io.WriteString(w, "partial diagnostic")
				w.(http.Flusher).Flush()
				close(started)
				select {
				case <-r.Context().Done():
					close(stopped)
				case <-cleanup:
				}
			}))
			defer server.Close()
			defer close(cleanup)
			timeout := int64(0)
			if ownTimeout {
				timeout = 1
			}
			client, err := NewPrometheusClient(models.AlertDataSource{HTTP: models.HTTP{URL: server.URL, Timeout: timeout}, Write: models.Write{URL: server.URL}})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- client.Write(ctx, []Metrics{{Name: "probe", Value: 1}}, nil) }()
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("write not started")
			}
			want := context.DeadlineExceeded
			if !ownTimeout {
				want = context.Canceled
				cancel()
			}
			select {
			case err := <-done:
				if !errors.Is(err, want) {
					t.Fatal("error body lost cancellation/timeout", err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("error response remained blocked")
			}
			select {
			case <-stopped:
			case <-time.After(time.Second):
				t.Fatal("server did not observe cancellation")
			}
		})
	}
}

func TestRemoteWriteSuccessfulPayloadAndCredentialsUnchanged(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		username, password, ok := r.BasicAuth()
		if !ok || username != "fixture-user" || password != "fixture-password" || r.Header.Get("X-Fixture") != "present" {
			t.Error("request-local credentials/headers changed")
		}
		if r.Method != "POST" || r.Header.Get("Content-Type") != "application/x-protobuf" || r.Header.Get("Content-Encoding") != "snappy" || r.Header.Get("X-Prometheus-Remote-Write-Version") != "0.1.0" {
			t.Error("write protocol changed")
		}
		encoded, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			w.WriteHeader(500)
			return
		}
		decoded, err := snappy.Decode(nil, encoded)
		if err != nil {
			t.Error(err)
			w.WriteHeader(500)
			return
		}
		var payload prompb.WriteRequest
		if err := proto.Unmarshal(decoded, &payload); err != nil {
			t.Error(err)
			w.WriteHeader(500)
			return
		}
		if len(payload.Timeseries) != 1 {
			t.Error("timeseries lost")
			w.WriteHeader(500)
			return
		}
		series := payload.Timeseries[0]
		labels := make(map[string]string)
		for _, label := range series.Labels {
			labels[label.Name] = label.Value
		}
		if labels["__name__"] != "probe" || labels["env"] != "test" || labels["team"] != "sre" || len(series.Samples) != 1 || series.Samples[0].Value != 1 || series.Samples[0].Timestamp <= 0 {
			t.Error("samples/labels changed")
		}
		w.WriteHeader(204)
	}))
	defer server.Close()
	client, err := NewPrometheusClient(models.AlertDataSource{HTTP: models.HTTP{URL: server.URL, Timeout: 3, Headers: map[string]string{"X-Fixture": "present"}}, Auth: models.Auth{User: "fixture-user", Pass: "fixture-password"}, Write: models.Write{URL: server.URL}})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Write(context.Background(), []Metrics{{Name: "probe", Value: 1, Labels: map[string]any{"env": "test"}}}, map[string]string{"team": "sre"}); err != nil {
		t.Fatal(err)
	}
	if err := (PrometheusProvider{}).Write(context.Background(), nil, nil); err != nil {
		t.Fatal("empty write did not remain no-op", err)
	}
}

type remoteWriteCancelLabel struct{ cancel context.CancelFunc }

func (label remoteWriteCancelLabel) String() string { label.cancel(); return "canceled" }

func TestRemoteWriteCancellationDuringConversionStopsBeforeIO(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := (PrometheusProvider{Timeout: 3}).Write(ctx, []Metrics{{Name: "probe", Labels: map[string]any{"cancel": remoteWriteCancelLabel{cancel: cancel}}}}, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatal("canceled conversion proceeded", err)
	}
}

func (b *remoteWriteBody) Read(p []byte) (int, error) {
	n, err := b.reader.Read(p)
	b.read += n
	return n, err
}
func (b *remoteWriteBody) Close() error { b.closed = true; return nil }

type remoteWriteFailReader struct{}

func (remoteWriteFailReader) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }

func TestRemoteWriteBoundsErrorResponse(t *testing.T) {
	const limit = 64 << 10
	for _, size := range []int{0, 128, limit, limit + 1, 2 << 20} {
		body := &remoteWriteBody{reader: strings.NewReader(strings.Repeat("x", size))}
		v := PrometheusProvider{WriteURL: "http://unused.invalid", Timeout: 1, httpClient: &http.Client{Transport: remoteWriteTransport(func(r *http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 503, Header: make(http.Header), Body: body}, nil
		})}}
		err := v.Write(context.Background(), []Metrics{{Name: "probe", Value: 1}}, nil)
		if err == nil || !strings.Contains(err.Error(), "503") {
			t.Fatal("HTTP failure lost", err)
		}
		if !body.closed {
			t.Fatal("response not closed")
		}
		if body.read > limit+1 {
			t.Fatalf("error response read %d bytes, limit %d", body.read, limit+1)
		}
		if size > limit && strings.Contains(err.Error(), strings.Repeat("x", 128)) {
			t.Fatal("oversized response was echoed instead of reported")
		}
		if size == 128 && !strings.Contains(err.Error(), strings.Repeat("x", 128)) {
			t.Fatal("ordinary diagnostic was lost")
		}
	}
}

func TestRemoteWritePreservesReadFailureCause(t *testing.T) {
	body := &remoteWriteBody{reader: remoteWriteFailReader{}}
	v := PrometheusProvider{WriteURL: "http://unused.invalid", Timeout: 1, httpClient: &http.Client{Transport: remoteWriteTransport(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 500, Header: make(http.Header), Body: body}, nil
	})}}
	err := v.Write(context.Background(), []Metrics{{Name: "probe", Value: 1}}, nil)
	if !errors.Is(err, io.ErrUnexpectedEOF) || !body.closed {
		t.Fatal("read failure was hidden", err, body.closed)
	}
}

func TestRemoteWriteTimeoutAndPreCancellation(t *testing.T) {
	for _, seconds := range []int64{0, -1, 3} {
		want := 10 * time.Second
		if seconds > 0 {
			want = time.Duration(seconds) * time.Second
		}
		v := PrometheusProvider{WriteURL: "http://unused.invalid", Timeout: seconds, httpClient: &http.Client{Transport: remoteWriteTransport(func(r *http.Request) (*http.Response, error) {
			deadline, ok := r.Context().Deadline()
			left := time.Until(deadline)
			if !ok || left <= 0 || left > want || left < want-time.Second {
				t.Errorf("timeout=%d budget missing/incorrect: %v %v", seconds, ok, left)
			}
			return &http.Response{StatusCode: 204, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(nil))}, nil
		})}}
		if err := v.Write(context.Background(), []Metrics{{Name: "probe", Value: 1}}, nil); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	// Nil client proves canceled work never reaches transport setup.
	if err := (PrometheusProvider{}).Write(ctx, []Metrics{{Name: "probe", Value: 1}}, nil); !errors.Is(err, context.Canceled) {
		t.Fatal("pre-cancel ignored", err)
	}
	if err := (PrometheusProvider{Timeout: math.MaxInt64}).Write(context.Background(), []Metrics{{Name: "probe", Value: 1}}, nil); err == nil {
		t.Fatal("overflow timeout accepted")
	}
}
