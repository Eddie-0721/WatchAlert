package provider

import (
	"context"
	"errors"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"watchAlert/internal/models"
)

type temporaryChecker struct {
	closed int
	failed bool
}

func (c *temporaryChecker) Check() (bool, error) {
	if c.failed {
		return false, errors.New("failed check")
	}
	return true, nil
}
func (c *temporaryChecker) Close() error { c.closed++; return nil }

func TestHealthCheckReleasesTemporaryClient(t *testing.T) {
	const kind = "lifecycle-test"
	defer delete(datasourceFactories, kind)
	for _, failed := range []bool{false, true} {
		c := &temporaryChecker{failed: failed}
		datasourceFactories[kind] = func(models.AlertDataSource) (HealthChecker, error) { return c, nil }
		ok, err := CheckDatasourceHealth(models.AlertDataSource{Type: kind})
		if ok == failed || (err != nil) != failed || c.closed != 1 {
			t.Fatal(ok, err, c.closed)
		}
	}
}

func TestElasticConstructorAndQueryBudgets(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		status     int
		wantError  bool
	}{
		{"empty", `{"hits":{"hits":[]},"timed_out":false,"_shards":{"failed":0}}`, 200, false},
		{"one", `{"hits":{"hits":[{"_source":{"message":"one","number":1}}]}}`, 200, false},
		{"http-error", `{"error":"failed"}`, 503, true},
		{"timeout", `{"hits":{"hits":[]},"timed_out":true}`, 200, true},
		{"shard-error", `{"hits":{"hits":[]},"_shards":{"failed":1}}`, 200, true},
		{"terminated", `{"hits":{"hits":[]},"terminated_early":true}`, 200, true},
		{"missing", `{}`, 200, true},
		{"too-large", `{"hits":{"hits":[{"_source":{"message":"` + strings.Repeat("x", maxQueryBodyBytes) + `"}}]}}`, 200, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var requests atomic.Int32
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				_, _ = io.Copy(io.Discard, r.Body)
				if r.URL.Path != "/logs/_search" {
					t.Error("unexpected health/startup request", r.URL.Path)
				}
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.body)
			}))
			defer s.Close()
			cli, err := NewElasticSearchClient(context.Background(), models.AlertDataSource{HTTP: models.HTTP{URL: s.URL, Timeout: 2}})
			if err != nil {
				t.Fatal(err)
			}
			e := cli.(ElasticSearchDsProvider)
			defer e.Close()
			if requests.Load() != 0 {
				t.Fatal("constructor performed network I/O")
			}
			logs, count, err := e.Query(LogQueryOptions{ElasticSearch: Elasticsearch{Index: "logs", QueryType: models.EsQueryTypeRawJson, RawJson: `{"match_all":{}}`}})
			if (err != nil) != tc.wantError {
				t.Fatal("unexpected query result", err)
			}
			if tc.name == "one" && (count != 1 || logs.Message[0]["message"] != "one" || logs.Message[0]["number"] != float64(1)) {
				t.Fatal("source conversion changed", logs, count)
			}
			if requests.Load() != 1 {
				t.Fatal("duplicate query", requests.Load())
			}
		})
	}
}

func TestElasticInflightCancellation(t *testing.T) {
	started, cleanup := make(chan struct{}), make(chan struct{})
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		close(started)
		select {
		case <-r.Context().Done():
		case <-cleanup:
		}
	}))
	defer s.Close()
	defer close(cleanup)
	cli, err := NewElasticSearchClient(context.Background(), models.AlertDataSource{HTTP: models.HTTP{URL: s.URL, Timeout: 5}})
	if err != nil {
		t.Fatal(err)
	}
	e := cli.(ElasticSearchDsProvider)
	defer e.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, _, err := e.QueryContext(ctx, LogQueryOptions{ElasticSearch: Elasticsearch{Index: "logs", QueryType: models.EsQueryTypeRawJson, RawJson: `{"match_all":{}}`}})
		done <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("query did not start")
	}
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("cancel ignored")
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled query did not stop")
	}
}

func TestClickHouseSQLLifecycleAndQueryBounds(t *testing.T) {
	// Isolated SQLite exercises the database/sql lifecycle without a real CH server.
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, _ := db.DB()
	defer sqlDB.Close()
	provider := ClickHouseProvider{client: sqlDB}
	query := LogQueryOptions{ClickHouse: ClickHouse{Query: "SELECT 'first' AS name, 1 AS value UNION ALL SELECT 'second', 2"}}
	logs, count, err := provider.Query(query)
	if err != nil || count != 2 || logs.Message[0]["name"] != "first" || logs.Message[1]["name"] != "second" {
		t.Fatal(logs, count, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := provider.QueryContext(ctx, query); err == nil {
		t.Fatal("cancel ignored")
	}
	if _, _, err := provider.Query(LogQueryOptions{ClickHouse: ClickHouse{Query: "WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM n WHERE x<10001) SELECT x FROM n"}}); err == nil {
		t.Fatal("row budget ignored")
	}
	if err := provider.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := provider.Query(query); err == nil {
		t.Fatal("closed SQL client remained usable")
	}
	configured, err := NewClickHouseClient(context.Background(), models.AlertDataSource{})
	if err != nil {
		t.Fatal(err)
	}
	actual := configured.(ClickHouseProvider)
	defer actual.Close()
	if actual.client.Stats().MaxOpenConnections != 8 {
		t.Fatal("unbounded ClickHouse connection pool")
	}
}
