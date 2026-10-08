package middleware

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	appctx "watchAlert/internal/ctx"

	"github.com/gin-gonic/gin"
)

func TestPermissionRejectsInvalidTenantBeforeHandler(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, user := range []string{"admin", "regular-user"} {
		for _, tenant := range []string{"", "null", "NULL", "undefined", " ", " tenant-a "} {
			t.Run(user+"/"+tenant, func(t *testing.T) {
				called := false
				r := gin.New()
				r.Use(func(c *gin.Context) { c.Set("UserId", user) }, Permission())
				r.GET("/", func(c *gin.Context) { called = true; c.Status(204) })
				w := httptest.NewRecorder()
				req := httptest.NewRequest("GET", "/", nil)
				req.Header.Set(TenantIDHeaderKey, tenant)
				r.ServeHTTP(w, req)
				if called || w.Code != http.StatusForbidden {
					t.Fatalf("called=%v status=%d", called, w.Code)
				}
			})
		}
	}
}

func TestPermissionAllowsAdminWithValidTenant(t *testing.T) {
	r := gin.New()
	r.Use(func(c *gin.Context) { c.Set("UserId", "admin") }, Permission())
	r.GET("/", func(c *gin.Context) { c.Status(204) })
	w := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set(TenantIDHeaderKey, "tenant-a")
	r.ServeHTTP(w, req)
	if w.Code != 204 {
		t.Fatalf("status=%d", w.Code)
	}
}

func TestPlatformAdminRequiresTrustedIdentity(t *testing.T) {
	for _, identity := range []interface{}{nil, "", "regular-user", 12, "admin"} {
		r := gin.New()
		r.Use(func(c *gin.Context) {
			if identity != nil {
				c.Set("UserId", identity)
			}
		}, PlatformAdmin())
		r.GET("/", func(c *gin.Context) { c.Status(204) })
		w := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/?userId=admin", nil)
		req.Header.Set("UserId", "admin")
		r.ServeHTTP(w, req)
		want := 403
		if identity == "admin" {
			want = 204
		}
		if w.Code != want {
			t.Errorf("identity=%v status=%d want=%d", identity, w.Code, want)
		}
	}
}

func TestParseTenantRejectsInvalidHeaderWithoutDatabase(t *testing.T) {
	for _, tenant := range []string{"", "null", "undefined", " "} {
		r := gin.New()
		r.Use(ParseTenant())
		r.GET("/", func(c *gin.Context) { t.Fatal("invalid tenant reached handler") })
		w := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/", nil)
		req.Header.Set(TenantIDHeaderKey, tenant)
		r.ServeHTTP(w, req)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("status=%d", w.Code)
		}
	}
}

func TestIsTokenValidRejectsMalformedHeaders(t *testing.T) {
	ctx := &appctx.Context{Ctx: context.Background()}
	for _, header := range []string{"", "x", "bearer", "Bearer ", "Basic abc", "Bearer not-a-jwt", "Bearer a b"} {
		if IsTokenValid(ctx, header) {
			t.Errorf("accepted %q", header)
		}
	}
}
