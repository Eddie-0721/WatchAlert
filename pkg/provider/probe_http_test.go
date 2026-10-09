package provider

import (
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// Count real server-side sockets instead of global goroutines, which include
// unrelated test/runtime work. No request leaves the local test servers.
func trackedProbeServer(t *testing.T, tls bool, handler http.Handler) (*httptest.Server, *atomic.Int32, *atomic.Int32) {
	t.Helper()
	active, created := new(atomic.Int32), new(atomic.Int32)
	s := httptest.NewUnstartedServer(handler)
	s.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		switch state {
		case http.StateNew:
			active.Add(1)
			created.Add(1)
		case http.StateClosed:
			active.Add(-1)
		}
	}
	if tls {
		s.StartTLS()
	} else {
		s.Start()
	}
	t.Cleanup(s.Close)
	return s, active, created
}

func requireNoProbeConnections(t *testing.T, active *atomic.Int32) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for active.Load() != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if n := active.Load(); n != 0 {
		t.Fatalf("completed probes retained %d connections", n)
	}
}

func TestHTTPProbeClosesPrivateTransport(t *testing.T) {
	for _, useTLS := range []bool{false, true} {
		name := "http"
		if useTLS {
			name = "https"
		}
		t.Run(name, func(t *testing.T) {
			s, active, created := trackedProbeServer(t, useTLS, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusNoContent)
			}))
			for i := 0; i < 10; i++ {
				result := (HTTPer{}).executeHTTPProbe(EndpointOption{Endpoint: s.URL, Timeout: 3, HTTP: Ehttp{Method: "GET"}})
				if result.Error != "" || result.StatusCode != http.StatusNoContent {
					t.Fatalf("unexpected probe result: %+v", result)
				}
			}
			if created.Load() != 10 {
				t.Fatalf("probes must retain fresh-connection semantics, got %d sockets", created.Load())
			}
			requireNoProbeConnections(t, active)
		})
	}
}

func TestHTTPProbeClosesRedirectConnectionsOnSuccessAndError(t *testing.T) {
	target, targetActive, _ := trackedProbeServer(t, false, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	for _, destination := range []string{target.URL, "unsupported://probe.invalid"} {
		t.Run(destination, func(t *testing.T) {
			s, active, _ := trackedProbeServer(t, false, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Location", destination)
				w.WriteHeader(http.StatusFound)
			}))
			result := (HTTPer{}).executeHTTPProbe(EndpointOption{Endpoint: s.URL, Timeout: 3, HTTP: Ehttp{Method: "GET"}})
			if (result.Error == "") != (destination == target.URL) {
				t.Fatalf("redirect outcome changed: %+v", result)
			}
			requireNoProbeConnections(t, active)
			requireNoProbeConnections(t, targetActive)
		})
	}
}

func TestHTTPProbePreservesRequestAndReachabilityMetrics(t *testing.T) {
	s, active, _ := trackedProbeServer(t, false, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if r.Method != "POST" || string(body) != `{"probe":true}` || r.Header.Get("X-Probe-Test") != "present" || r.Header.Get("Content-Type") != "application/json" || r.UserAgent() != "WatchAlert-Probe/1.0" {
			t.Error("probe request fields changed")
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	metrics := (HTTPer{}).PilotWithMetrics(EndpointOption{
		Endpoint: s.URL, Timeout: 3,
		HTTP: Ehttp{Method: "post", Body: `{"probe":true}`, Header: map[string]string{"X-Probe-Test": "present"}},
	}, ProbeRuleInfo{TenantID: "tenant", RuleID: "rule", RuleType: HTTPEndpointProvider, Endpoint: s.URL, Labels: map[string]string{"env": "test"}})
	if len(metrics) != 3 || metrics[1].Name != "probe_http_status_code" || metrics[1].Value != 503 || metrics[2].Name != "probe_http_success" || metrics[2].Value != 1 {
		t.Fatalf("HTTP error status must still mean reachable: %+v", metrics)
	}
	for _, metric := range metrics {
		if metric.Labels["tenant_id"] != "tenant" || metric.Labels["env"] != "test" {
			t.Fatalf("labels changed: %+v", metric.Labels)
		}
	}
	requireNoProbeConnections(t, active)
}
