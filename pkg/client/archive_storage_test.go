package client

import (
	"testing"
	"watchAlert/internal/models"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

func TestArchiveStorageAddsReceiptWithoutRewritingHistory(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	conn, _ := db.DB()
	conn.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = conn.Close() })
	if err := db.AutoMigrate(&models.AlertHisEvent{}); err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&models.AlertHisEvent{TenantId: "legacy", Annotations: "unchanged"}).Error; err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := initArchiveStorage(db); err != nil {
			t.Fatal(err)
		}
	}
	if !db.Migrator().HasTable(&models.AlertArchiveReceipt{}) {
		t.Fatal("receipt migration missing")
	}
	var history []models.AlertHisEvent
	if err := db.Find(&history).Error; err != nil || len(history) != 1 || history[0].Annotations != "unchanged" {
		t.Fatal("history migration rewrote legacy data", history, err)
	}
}
