package repo

import (
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"testing"
	"watchAlert/internal/models"
)

func TestSilenceUpdatePersistsFutureStatusAndLabels(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	conn, _ := db.DB()
	defer conn.Close()
	if err = db.AutoMigrate(&models.AlertSilences{}); err != nil {
		t.Fatal(err)
	}
	original := models.AlertSilences{TenantId: "t", ID: "s", Status: 1, Name: "old", Labels: []models.SilenceLabel{{Key: "env", Operator: "==", Value: "prod"}}}
	if err = db.Create(&original).Error; err != nil {
		t.Fatal(err)
	}
	original.Status = 0
	original.Labels[0].Value = "test"
	repository := SilenceRepo{entryRepo: entryRepo{db: db}}
	if err = repository.Update(original); err != nil {
		t.Fatal(err)
	}
	var saved models.AlertSilences
	if err = db.First(&saved, "id = ?", "s").Error; err != nil {
		t.Fatal(err)
	}
	if saved.Status != 0 || len(saved.Labels) != 1 || saved.Labels[0].Value != "test" {
		t.Fatalf("state or label not persisted: %#v", saved)
	}
}
