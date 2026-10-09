package repo

import (
	"context"
	"errors"
	"testing"
	"time"
	"watchAlert/internal/models"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

func TestAgentConfigurationAndMembershipContext(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	pool, _ := db.DB()
	pool.SetMaxOpenConns(1)
	defer pool.Close()
	if err := db.AutoMigrate(&models.Settings{}, &models.TenantLinkedUsers{}); err != nil {
		t.Fatal(err)
	}
	enabled, disabled := true, false
	for _, row := range []models.Settings{{IsInit: 0, AgentConfig: models.AgentConfig{Enable: &disabled}}, {IsInit: 1, AgentConfig: models.AgentConfig{Enable: &enabled}}} {
		if err := db.Create(&row).Error; err != nil {
			t.Fatal(err)
		}
	}
	for _, row := range []models.TenantLinkedUsers{{ID: "a", Users: []models.TenantUser{{UserID: "u", UserRole: "role-a"}}}, {ID: "b", Users: []models.TenantUser{{UserID: "u", UserRole: "role-b"}}}} {
		if err := db.Create(&row).Error; err != nil {
			t.Fatal(err)
		}
	}
	settings := newSettingRepoInterface(db, NewInterGormDBCli(db))
	tenant := newTenantInterface(db, NewInterGormDBCli(db))
	for _, get := range []func() (models.Settings, error){settings.Get, func() (models.Settings, error) { return settings.GetContext(context.Background()) }} {
		row, err := get()
		if err != nil || row.IsInit != 1 || !row.AgentConfig.GetEnable() {
			t.Fatal("initialized settings selection changed", row.IsInit, err)
		}
	}
	for _, id := range []string{"a", "b"} {
		legacy, err := tenant.GetTenantLinkedUserInfo(id, "u")
		current, contextErr := tenant.GetTenantLinkedUserInfoContext(context.Background(), id, "u")
		if err != nil || contextErr != nil || current != legacy || current.UserRole != "role-"+id {
			t.Fatal("tenant scope changed", current, err, contextErr)
		}
	}
	if user, err := tenant.GetTenantLinkedUserInfoContext(context.Background(), "a", "missing"); err != nil || user.UserID != "" {
		t.Fatal("missing member semantics changed", user, err)
	}
	if _, err := tenant.GetTenantLinkedUserInfoContext(context.Background(), "missing", "u"); !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatal("missing tenant was accepted", err)
	}
	for _, operation := range []string{"settings", "membership"} {
		t.Run(operation, func(t *testing.T) {
			connection, err := pool.Conn(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer connection.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
			defer cancel()
			result := make(chan error, 1)
			go func() {
				var err error
				if operation == "settings" {
					_, err = settings.GetContext(ctx)
				} else {
					_, err = tenant.GetTenantLinkedUserInfoContext(ctx, "a", "u")
				}
				result <- err
			}()
			select {
			case err := <-result:
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatal("pool wait ignored request deadline", err)
				}
			case <-time.After(2 * time.Second):
				connection.Close()
				<-result
				t.Fatal("pool wait did not stop")
			}
		})
	}
}
