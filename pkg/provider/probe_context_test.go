package provider

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestProbeHTTPAndSSLCancelInFlightWithoutFailureMetrics(t *testing.T) {
	for _, ssl := range []bool{false, true} {
		name := "http"
		if ssl {
			name = "ssl"
		}
		t.Run(name, func(t *testing.T) {
			started, stopped, cleanup := make(chan struct{}), make(chan struct{}), make(chan struct{})
			s := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				close(started)
				select {
				case <-r.Context().Done():
					close(stopped)
				case <-cleanup:
				}
			}))
			if ssl {
				s.StartTLS()
			} else {
				s.Start()
			}
			defer s.Close()
			defer close(cleanup)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var prober MetricsAwareProbe = HTTPer{}
			endpoint := s.URL
			if ssl {
				prober = Ssler{}
				endpoint = strings.TrimPrefix(s.URL, "https://")
			}
			done := make(chan []Metrics, 1)
			go func() {
				done <- prober.PilotWithMetricsContext(ctx, EndpointOption{Endpoint: endpoint, Timeout: 30, HTTP: Ehttp{Method: "GET"}}, ProbeRuleInfo{})
			}()
			select {
			case <-started:
			case <-time.After(2 * time.Second):
				t.Fatal("probe did not start")
			}
			cancel()
			select {
			case metrics := <-done:
				if metrics != nil {
					t.Fatalf("cancellation produced target metrics: %+v", metrics)
				}
			case <-time.After(time.Second):
				t.Fatal("probe ignored cancellation")
			}
			select {
			case <-stopped:
			case <-time.After(time.Second):
				t.Fatal("target did not see cancellation")
			}
		})
	}
}

func TestProbeAlreadyCancelledAndNormalResults(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, p := range []MetricsAwareProbe{HTTPer{}, Tcper{}, Ssler{}, Pinger{}} {
		if got := p.PilotWithMetricsContext(ctx, EndpointOption{Endpoint: "not-a-network-target", Timeout: 30}, ProbeRuleInfo{}); got != nil {
			t.Fatalf("%T produced metrics for cancelled request: %+v", p, got)
		}
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	metrics := (Tcper{}).PilotWithMetricsContext(context.Background(), EndpointOption{Endpoint: listener.Addr().String(), Timeout: 3}, ProbeRuleInfo{})
	if len(metrics) != 2 || metrics[0].Value != 1 {
		t.Fatalf("TCP success changed: %+v", metrics)
	}
	conn, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := conn.Read(make([]byte, 1)); err == nil {
		t.Fatal("probe connection was not closed")
	}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) }))
	defer server.Close()
	metrics = (Ssler{}).PilotWithMetricsContext(context.Background(), EndpointOption{Endpoint: strings.TrimPrefix(server.URL, "https://"), Timeout: 3}, ProbeRuleInfo{})
	if len(metrics) != 3 || metrics[0].Value <= 0 || metrics[2].Value != 1 {
		t.Fatalf("SSL certificate metrics changed: %+v", metrics)
	}
}

func TestHTTPProbeTimeoutStillProducesFailureMetrics(t *testing.T) {
	cleanup := make(chan struct{})
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-cleanup:
		}
	}))
	defer s.Close()
	defer close(cleanup)
	metrics := (HTTPer{}).PilotWithMetricsContext(context.Background(), EndpointOption{Endpoint: s.URL, Timeout: 1, HTTP: Ehttp{Method: "GET"}}, ProbeRuleInfo{})
	if len(metrics) != 3 || metrics[2].Value != 0 {
		t.Fatalf("real target timeout was hidden: %+v", metrics)
	}
}

type blockingProbePing struct {
	started, stopped, release chan struct{}
	stops                     atomic.Int32
}

func (p *blockingProbePing) Run() error { close(p.started); <-p.release; return nil }
func (p *blockingProbePing) Stop() {
	if p.stops.Add(1) == 1 {
		close(p.stopped)
	}
}

func TestProbePingCancellationStopsAndWaitsForRun(t *testing.T) {
	p := &blockingProbePing{started: make(chan struct{}), stopped: make(chan struct{}), release: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runProbePing(ctx, p) }()
	<-p.started
	cancel()
	select {
	case <-p.stopped:
	case <-time.After(time.Second):
		close(p.release)
		t.Fatal("Stop not called")
	}
	select {
	case <-done:
		close(p.release)
		t.Fatal("returned before ping cleanup completed")
	default:
	}
	close(p.release)
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not finish")
	}
	if p.stops.Load() != 1 {
		t.Fatal("unexpected repeated Stop")
	}
	if err := runProbePing(ctx, p); !errors.Is(err, context.Canceled) {
		t.Fatal("pre-cancel did not bypass Run", err)
	}
}

func TestProbeIPResolutionPreservesLiteralAddresses(t *testing.T) {
	for _, endpoint := range []string{"127.0.0.1", "::1", "fe80::1%probe-test"} {
		addr, err := resolveProbeIP(context.Background(), net.DefaultResolver, endpoint)
		if err != nil || addr.String() != endpoint {
			t.Fatalf("literal address changed: %s %v %v", endpoint, addr, err)
		}
	}
}

func TestProbeIPResolutionCancellation(t *testing.T) {
	started := make(chan struct{})
	var once sync.Once
	resolver := &net.Resolver{PreferGo: true, Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
		once.Do(func() { close(started) })
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := resolveProbeIP(ctx, resolver, "probe-cancellation.invalid"); done <- err }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("resolver was not called")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal("DNS cancellation lost", err)
		}
	case <-time.After(time.Second):
		t.Fatal("DNS did not cancel")
	}
}
