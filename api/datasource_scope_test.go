package api

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	appctx "watchAlert/internal/ctx"
	"watchAlert/internal/models"
	"watchAlert/internal/repo"
)

type datasourceScopeSpy struct {
	repo.InterDatasourceRepo
	tenant, id string
	source     *models.AlertDataSource
}

func (s *datasourceScopeSpy) GetForTenant(tenant, id string) (models.AlertDataSource, error) {
	s.tenant, s.id = tenant, id
	if s.source != nil && s.source.TenantId == tenant && s.source.ID == id {
		return *s.source, nil
	}
	return models.AlertDataSource{}, fmt.Errorf("not found")
}

func TestPingUsesStoredCredentialsWithoutReturningThem(t *testing.T) {
	called := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		user, pass, ok := r.BasicAuth()
		if !ok || user != "reader" || pass != "server-only-secret" || r.Header.Get("X-Token") != "server-header-secret" {
			t.Error("stored credentials not used")
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	source := models.AlertDataSource{TenantId: "trusted", ID: "local", Type: "Prometheus",
		HTTP: models.HTTP{URL: server.URL, Timeout: 2, Headers: map[string]string{"X-Token": "server-header-secret"}},
		Auth: models.Auth{User: "reader", Pass: "server-only-secret"}}
	old := appctx.DB
	t.Cleanup(func() { appctx.DB = old })
	appctx.DB = datasourceScopeDB{ds: &datasourceScopeSpy{source: &source}}
	router := gin.New()
	router.POST("/", func(c *gin.Context) { c.Set("TenantID", "trusted") }, DatasourceController.Ping)
	body := fmt.Sprintf(`{"id":"local","type":"Prometheus","http":{"url":%q,"timeout":2,"headers":{"X-Token":""}},"auth":{"user":"reader","pass":""}}`, server.URL)
	request := httptest.NewRequest("POST", "/", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	writer := httptest.NewRecorder()
	router.ServeHTTP(writer, request)
	if writer.Code != 200 || !called || strings.Contains(writer.Body.String(), "secret") {
		t.Fatalf("ping failed: %d %s", writer.Code, writer.Body.String())
	}
}

type datasourceScopeDB struct {
	repo.InterEntryRepo
	ds *datasourceScopeSpy
}

func (s datasourceScopeDB) Datasource() repo.InterDatasourceRepo { return s.ds }

func TestExternalDatasourceQueriesUseTrustedTenant(t *testing.T) {
	old := appctx.DB
	t.Cleanup(func() { appctx.DB = old })
	for _, tc := range []struct {
		name, body string
		handler    gin.HandlerFunc
	}{
		{"ping", `{"tenantId":"foreign","id":"ds-foreign","type":"Prometheus"}`, DatasourceController.Ping},
		{"logs", `{"tenantId":"foreign","datasourceId":"ds-foreign","type":"Loki","query":"dXA="}`, DatasourceController.SearchViewLogsContent},
	} {
		spy := &datasourceScopeSpy{}
		appctx.DB = datasourceScopeDB{ds: spy}
		router := gin.New()
		router.POST("/", func(c *gin.Context) { c.Set("TenantID", "trusted") }, tc.handler)
		request := httptest.NewRequest("POST", "/", strings.NewReader(tc.body))
		request.Header.Set("Content-Type", "application/json")
		writer := httptest.NewRecorder()
		router.ServeHTTP(writer, request)
		if writer.Code != 400 || spy.tenant != "trusted" || spy.id != "ds-foreign" {
			t.Fatalf("%s crossed tenant boundary: %d %q %q", tc.name, writer.Code, spy.tenant, spy.id)
		}
	}
}
