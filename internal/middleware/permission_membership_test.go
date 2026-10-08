package middleware

import (
	"context"
	"net/http/httptest"
	"testing"
	appctx "watchAlert/internal/ctx"
	"watchAlert/internal/models"
	"watchAlert/internal/repo"

	"github.com/gin-gonic/gin"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

// The member/role queries use SQLite; the repository membership boundary is
// stubbed to exercise both a valid link and its existing empty-link behavior.
type membershipRepo struct{ repo.InterTenantRepo }

func (membershipRepo) GetTenantLinkedUserInfo(tenant, user string) (models.TenantUser, error) {
	if tenant == "tenant-a" && user == "alice" {
		return models.TenantUser{UserID: user, UserRole: "reader"}, nil
	}
	if tenant == "tenant-b" && user == "bob" {
		return models.TenantUser{UserID: user, UserRole: "reader"}, nil
	}
	return models.TenantUser{}, nil
}

type permissionEntry struct {
	repo.InterEntryRepo
	db *gorm.DB
}

func (e permissionEntry) DB() *gorm.DB                 { return e.db }
func (e permissionEntry) Tenant() repo.InterTenantRepo { return membershipRepo{} }

func TestPermissionSeparatesMembershipFromEndpointPermission(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	defer sqlDB.Close()
	if err := db.AutoMigrate(&models.Member{}, &models.UserRole{}); err != nil {
		t.Fatal(err)
	}
	for _, user := range []string{"alice", "bob"} {
		if err := db.Create(&models.Member{UserId: user}).Error; err != nil {
			t.Fatal(err)
		}
	}
	for _, role := range []models.UserRole{
		{ID: "reader", Permissions: []models.UserPermissions{{API: "/read"}}},
		// An empty-ID role must never accidentally grant non-members access.
		{ID: "", Permissions: []models.UserPermissions{{API: "/read"}}},
	} {
		if err := db.Create(&role).Error; err != nil {
			t.Fatal(err)
		}
	}
	oldDB, oldCtx := appctx.DB, appctx.Ctx
	appctx.DB, appctx.Ctx = permissionEntry{db: db}, context.Background()
	t.Cleanup(func() { appctx.DB, appctx.Ctx = oldDB, oldCtx })
	for _, tc := range []struct {
		user, tenant, path string
		want               int
	}{
		{"alice", "tenant-a", "/read", 204}, {"bob", "tenant-b", "/read", 204},
		{"alice", "tenant-b", "/read", 403}, {"bob", "tenant-a", "/read", 403},
		{"alice", "missing", "/read", 403}, {"alice", "tenant-a", "/write", 403},
	} {
		r := gin.New()
		r.Use(func(c *gin.Context) { c.Set("UserId", tc.user) }, Permission())
		r.GET(tc.path, func(c *gin.Context) { c.Status(204) })
		w := httptest.NewRecorder()
		req := httptest.NewRequest("GET", tc.path, nil)
		req.Header.Set(TenantIDHeaderKey, tc.tenant)
		r.ServeHTTP(w, req)
		if w.Code != tc.want {
			t.Errorf("%+v got=%d", tc, w.Code)
		}
	}
}
