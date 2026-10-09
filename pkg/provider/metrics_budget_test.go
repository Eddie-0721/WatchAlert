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
	"watchAlert/internal/models"
)

func budgetSource(url string) models.AlertDataSource {
	var source models.AlertDataSource
	source.HTTP.URL = url
	source.HTTP.Timeout = 5
	return source
}
func TestPrometheusBudgetRejectsOversizedDecodedAndWireResults(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		bytes      int64
		samples    int
		rangeQuery bool
	}{
		{"vector", `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1,"1"]},{"metric":{},"value":[1,"2"]}]}}`, 4096, 1, false},
		{"matrix", `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{},"values":[[1,"1"],[2,"2"]]},{"metric":{},"values":[[1,"1"],[2,"2"]]}]}}`, 4096, 3, true},
		{"bytes", strings.Repeat(" ", 4097), 4096, 100, false},
		{"partial", `{"status":"success","warnings":["partial"],"data":{"resultType":"matrix","result":[]}}`, 4096, 100, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if err := r.ParseForm(); err != nil {
					t.Error(err)
				}
				if r.Form.Get("limit") != fmt.Sprint(tc.samples+1) {
					t.Error("upstream series limit missing", r.Form)
				}
				_, _ = io.WriteString(w, tc.body)
			}))
			defer server.Close()
			step := time.Duration(0)
			if tc.rangeQuery {
				step = time.Second
			}
			_, err := BoundedPrometheusQuery(context.Background(), budgetSource(server.URL), "up", time.Unix(1, 0), time.Unix(2, 0), step, QueryBudget{MaxSamples: tc.samples, MaxBytes: tc.bytes})
			if err == nil {
				t.Fatal("oversize/incomplete result accepted")
			}
		})
	}
}

func TestPrometheusCancellationReachesServer(t *testing.T) {
	started, stopped := make(chan struct{}), make(chan struct{})
	cleanup := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		close(started)
		select {
		case <-r.Context().Done():
			close(stopped)
		case <-cleanup:
		}
	}))
	defer server.Close()
	defer close(cleanup)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := BoundedPrometheusQuery(ctx, budgetSource(server.URL), "up", time.Time{}, time.Time{}, 0, QueryBudget{MaxSamples: 100, MaxBytes: 4096})
		done <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("query never started")
	}
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("cancellation lost")
		}
	case <-time.After(time.Second):
		t.Fatal("query did not cancel")
	}
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("upstream still running")
	}
}

func TestBudgetBodyAcceptsExactSize(t *testing.T) {
	reader := &budgetBody{ReadCloser: io.NopCloser(strings.NewReader("1234")), remaining: 4}
	result, err := io.ReadAll(reader)
	if err != nil || string(result) != "1234" {
		t.Fatal(string(result), err)
	}
}

func TestPrometheusConnectionReuseKeepsCredentialsRequestLocal(t *testing.T) {
	var ports []string
	var users []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		user, pass, ok := r.BasicAuth()
		if !ok || pass != user+"-password" || r.Header.Get("X-Scope") != user {
			t.Error("credentials crossed request boundary")
		}
		ports = append(ports, r.RemoteAddr)
		users = append(users, user)
		_, _ = io.WriteString(w, `{"status":"success","data":{"resultType":"vector","result":[]}}`)
	}))
	defer server.Close()
	for _, user := range []string{"tenant-a", "tenant-b"} {
		source := budgetSource(server.URL)
		source.Auth.User, source.Auth.Pass = user, user+"-password"
		source.HTTP.Headers = map[string]string{"X-Scope": user}
		_, err := BoundedPrometheusQuery(context.Background(), source, "up", time.Time{}, time.Time{}, 0, QueryBudget{MaxSamples: 10, MaxBytes: 4096})
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(ports) != 2 || ports[0] != ports[1] || users[0] == users[1] {
		t.Fatal("expected reused connection with different credentials", ports, users)
	}
}
