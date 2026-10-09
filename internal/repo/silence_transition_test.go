package repo

import (
	"context"
	"testing"
	"watchAlert/internal/models"
)

func TestSilenceStatusTransitionProtectsConfiguration(t *testing.T) {
	db := eventTestDB(t)
	if err := db.AutoMigrate(&models.AlertSilences{}); err != nil {
		t.Fatal(err)
	}
	before := models.AlertSilences{TenantId: "t", ID: "s", FaultCenterId: "fc", StartsAt: 10, EndsAt: 100, UpdateAt: 1, Status: 0, Comment: "original"}
	if err := db.Create(&before).Error; err != nil {
		t.Fatal(err)
	}
	r := SilenceRepo{entryRepo: entryRepo{db: db}}
	// An edit in the same second must not lose its comment/labels to a full-row update.
	if err := db.Model(&models.AlertSilences{}).Where("tenant_id = ? AND id = ?", "t", "s").UpdateColumn("comment", "new comment").Error; err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if ok, err := r.TransitionStatus(context.Background(), before, 1); err != nil || !ok {
			t.Fatal("transition/retry failed", ok, err)
		}
	}
	var row models.AlertSilences
	if err := db.First(&row).Error; err != nil || row.Comment != "new comment" || row.Status != 1 {
		t.Fatal(row, err)
	}
	before.Status = 1
	if err := db.Model(&models.AlertSilences{}).Where("tenant_id = ? AND id = ?", "t", "s").Updates(map[string]interface{}{"ends_at": 200, "fault_center_id": "moved"}).Error; err != nil {
		t.Fatal(err)
	}
	if ok, err := r.TransitionStatus(context.Background(), before, 2); err != nil || ok {
		t.Fatal("edited schedule overwritten", ok, err)
	}
	other := before
	other.TenantId = "other"
	if ok, err := r.TransitionStatus(context.Background(), other, 2); err != nil || ok {
		t.Fatal("tenant escaped", ok, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := r.TransitionStatus(ctx, before, 2); err == nil {
		t.Fatal("cancel ignored")
	}
}
