package api

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	appctx "watchAlert/internal/ctx"
	"watchAlert/internal/models"
	"watchAlert/internal/repo"
	"watchAlert/internal/services"

	"github.com/gin-gonic/gin"
)

type settingsKeys struct{ repo.InterApiKeyRepo }

func (settingsKeys) GetByKey(key string) (models.ApiKey, bool, error) {
	if key == "test-admin-key" {
		return models.ApiKey{UserId: "admin"}, true, nil
	}
	if key == "test-user-key" {
		return models.ApiKey{UserId: "ordinary-user"}, true, nil
	}
	return models.ApiKey{}, false, nil
}

type settingsAudit struct{ repo.InterAuditLogRepo }

func (settingsAudit) Create(models.AuditLog) error { return nil }

type settingsEntry struct{ repo.InterEntryRepo }

func (settingsEntry) ApiKey() repo.InterApiKeyRepo     { return settingsKeys{} }
func (settingsEntry) AuditLog() repo.InterAuditLogRepo { return settingsAudit{} }

type settingsServiceSpy struct{ calls int }

func (s *settingsServiceSpy) Get() (interface{}, interface{})             { s.calls++; return "settings", nil }
func (s *settingsServiceSpy) Save(interface{}) (interface{}, interface{}) { s.calls++; return nil, nil }

func TestSettingsRoutesEnforcePlatformAdmin(t *testing.T) {
	oldDB, oldCtx, oldService := appctx.DB, appctx.Ctx, services.SettingService
	spy := &settingsServiceSpy{}
	appctx.DB, appctx.Ctx, services.SettingService = settingsEntry{}, context.Background(), spy
	t.Cleanup(func() { appctx.DB, appctx.Ctx, services.SettingService = oldDB, oldCtx, oldService })
	r := gin.New()
	SettingsController.API(r.Group("/api/w8t"))
	for _, route := range []struct{ method, path string }{
		{"GET", "getSystemSetting"}, {"POST", "saveSystemSetting"}, {"POST", "syncLdapUser"},
	} {
		for _, tenant := range []string{"", "null", "tenant-a"} {
			w := httptest.NewRecorder()
			req := httptest.NewRequest(route.method, "/api/w8t/setting/"+route.path, strings.NewReader(`{}`))
			req.Header.Set("X-API-Key", "test-user-key")
			req.Header.Set("TenantID", tenant)
			req.Header.Set("UserId", "admin")
			r.ServeHTTP(w, req)
			if w.Code != 403 {
				t.Fatalf("%s tenant=%q status=%d", route.path, tenant, w.Code)
			}
		}
	}
	if spy.calls != 0 {
		t.Fatal("ordinary user reached settings service")
	}
	for _, route := range []struct{ method, path string }{
		{"GET", "getSystemSetting"}, {"POST", "saveSystemSetting"},
	} {
		w := httptest.NewRecorder()
		req := httptest.NewRequest(route.method, "/api/w8t/setting/"+route.path, strings.NewReader(`{}`))
		req.Header.Set("X-API-Key", "test-admin-key")
		req.Header.Set("TenantID", "tenant-a")
		req.Header.Set("Content-Type", "application/json")
		r.ServeHTTP(w, req)
		if w.Code != 200 {
			t.Fatalf("admin %s status=%d body=%s", route.path, w.Code, w.Body.String())
		}
	}
	if spy.calls != 2 {
		t.Fatalf("admin calls=%d", spy.calls)
	}
}
