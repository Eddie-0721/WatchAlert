package repo

import (
	"errors"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"testing"
	"watchAlert/internal/models"
)

func TestDatasourceTenantQueriesAndCredentialClear(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	conn, _ := db.DB()
	defer conn.Close()
	if err := db.AutoMigrate(&models.AlertDataSource{}); err != nil {
		t.Fatal(err)
	}
	for _, tenant := range []string{"a", "b"} {
		row := models.AlertDataSource{TenantId: tenant, ID: "ds-" + tenant, Name: "shared", KubeConfig: "secret"}
		row.Auth.Pass = "password"
		if err := db.Create(&row).Error; err != nil {
			t.Fatal(err)
		}
	}
	repo := DatasourceRepo{entryRepo: entryRepo{db: db}}
	for _, tenant := range []string{"", " ", "null", "undefined", "b"} {
		if _, err := repo.GetForTenant(tenant, "ds-a"); err == nil {
			t.Fatal("unscoped read allowed", tenant)
		}
	}
	rows, err := repo.List("a", "", "", "shared")
	if err != nil || len(rows) != 1 || rows[0].TenantId != "a" {
		t.Fatal("list isolation failed", err)
	}
	row, err := repo.GetForTenant("a", "ds-a")
	if err != nil {
		t.Fatal(err)
	}
	row.KubeConfig, row.Auth.Pass = "", ""
	if err := repo.Update(row); err != nil {
		t.Fatal(err)
	}
	saved, _ := repo.GetForTenant("a", "ds-a")
	if saved.KubeConfig != "" || saved.Auth.Pass != "" {
		t.Fatal("clear not persisted")
	}
	row.ID = "ds-b"
	if err := repo.Update(row); err != nil {
		t.Fatal(err)
	}
	foreign, _ := repo.GetForTenant("b", "ds-b")
	if foreign.KubeConfig != "secret" || foreign.Auth.Pass != "password" {
		t.Fatal("foreign row changed")
	}
}

func TestSettingsClearPersistsAndReadErrorPropagates(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	conn, _ := db.DB()
	defer conn.Close()
	if err := db.AutoMigrate(&models.Settings{}); err != nil {
		t.Fatal(err)
	}
	repo := settingRepo{entryRepo: entryRepo{db: db}}
	if _, err := repo.Get(); !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatal("missing settings not reported")
	}
	original := models.Settings{IsInit: 1}
	original.OidcConfig.ClientSecret = "secret"
	original.LdapConfig.AdminPass = "secret"
	if err := db.Create(&original).Error; err != nil {
		t.Fatal(err)
	}
	if err := repo.Update(models.Settings{}); err != nil {
		t.Fatal(err)
	}
	saved, err := repo.Get()
	if err != nil || saved.OidcConfig.ClientSecret != "" || saved.LdapConfig.AdminPass != "" {
		t.Fatal("zero config clear failed", err)
	}
	conn.Close()
	if _, err := repo.Get(); err == nil {
		t.Fatal("database error swallowed")
	}
}
