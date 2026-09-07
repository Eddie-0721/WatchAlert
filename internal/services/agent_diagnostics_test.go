package services

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"watchAlert/internal/types"
)

func TestDiagnosticProbeRejectsUntrustedOutput(t *testing.T) {
	for _, body := range []string{
		`{"checks":[{"id":"sdk","status":"ok"},{"id":"gateway","status":"ok"},{"id":"model","status":"ok"}]}`,
		`{"checks":[{"id":"model","status":"secret-key"}]}`,
		`{"checks":[]}`,
		strings.Repeat("x", 8193),
	} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "POST" || r.URL.Path != "/v1/diagnostics" || r.Header.Get("X-WatchAlert-Agent-Token") != "internal" {
				t.Error("wrong diagnostic request")
			}
			w.Write([]byte(body))
		}))
		checks, err := probeAgentRuntime(context.Background(), server.URL, "internal", types.AgentModelRuntime{})
		server.Close()
		if len(body) < 200 && strings.Contains(body, `"sdk"`) {
			if err != nil || len(checks) != 3 {
				t.Fatal(err)
			}
		} else if err == nil || strings.Contains(err.Error(), "secret-key") {
			t.Fatal("invalid output accepted or leaked")
		}
	}
}
