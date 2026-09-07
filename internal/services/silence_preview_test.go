package services

import (
	"errors"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"testing"
	"time"
	"watchAlert/internal/cache"
	"watchAlert/internal/ctx"
	"watchAlert/internal/models"
	"watchAlert/internal/types"
)

type previewEvents struct {
	cache.AlertCacheInterface
	events map[string]*models.AlertCurEvent
	err    error
}

func (p *previewEvents) GetAllEvents(models.AlertEventCacheKey) (map[string]*models.AlertCurEvent, error) {
	return p.events, p.err
}

type previewCache struct {
	cache.InterEntryCache
	events *previewEvents
}

func (p previewCache) Alert() cache.AlertCacheInterface { return p.events }

func TestSilencePreviewFreshnessAndIsolation(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	sql, _ := db.DB()
	defer sql.Close()
	if err = db.AutoMigrate(&models.FaultCenter{}, &models.AlertSilences{}); err != nil {
		t.Fatal(err)
	}
	if err = db.Create(&models.FaultCenter{TenantId: "t", ID: "fc", Name: "center"}).Error; err != nil {
		t.Fatal(err)
	}
	events := &previewEvents{events: map[string]*models.AlertCurEvent{}}
	for _, id := range []string{"1", "2", "3", "4", "5", "6"} {
		events.events[id] = &models.AlertCurEvent{TenantId: "t", FaultCenterId: "fc", Fingerprint: id, Labels: map[string]interface{}{"env": "prod"}}
	}
	events.events["other"] = &models.AlertCurEvent{TenantId: "other", FaultCenterId: "fc", Fingerprint: "other", Labels: map[string]interface{}{"env": "prod"}}
	events.events["recovered"] = &models.AlertCurEvent{TenantId: "t", FaultCenterId: "fc", Status: models.StateRecovered, Labels: map[string]interface{}{"env": "prod"}}
	service := alertSilenceService{ctx: &ctx.Context{DB: policyRepo{db: db}, Redis: previewCache{events: events}}}
	req := &types.RequestSilencePreview{TenantId: "t", FaultCenterId: "fc", Name: "maintenance", Comment: "release", StartsAt: time.Now().Unix(), EndsAt: time.Now().Add(time.Hour).Unix(), Labels: []models.SilenceLabel{{Key: "env", Operator: "==", Value: "prod"}}}
	before, err := service.preview(req)
	if err != nil {
		t.Fatal(err)
	}
	if before.Total != 6 || len(before.Samples) != 5 || !before.Truncated {
		t.Fatalf("unexpected preview: %#v", before)
	}
	again, err := service.preview(req)
	if err != nil || again.PreviewHash != before.PreviewHash {
		t.Fatal("hash must be deterministic")
	}
	if err = validateSilencePreview(before.PreviewHash, before.PreviewAt, again, time.Now().Unix()); err != nil {
		t.Fatal(err)
	}
	delete(events.events, "6")
	changed, _ := service.preview(req)
	if err = validateSilencePreview(before.PreviewHash, before.PreviewAt, changed, time.Now().Unix()); err == nil {
		t.Fatal("changed matches accepted")
	}
	// No write repository/cache methods exist on this fixture. A stale create
	// must return before attempting any database or cache write.
	_, failure := service.Create(&types.RequestSilenceCreate{TenantId: req.TenantId, FaultCenterId: req.FaultCenterId, Name: req.Name, Comment: req.Comment, StartsAt: req.StartsAt, EndsAt: req.EndsAt, Labels: req.Labels, PreviewHash: before.PreviewHash, PreviewAt: before.PreviewAt})
	if failure == nil {
		t.Fatal("stale create accepted")
	}
	for _, at := range []int64{0, before.PreviewAt - 301, before.PreviewAt + 60} {
		if validateSilencePreview(changed.PreviewHash, at, changed, before.PreviewAt) == nil {
			t.Fatal("invalid preview time accepted")
		}
	}
	// Existing configuration edits invalidate an update preview too.
	req.ID = "s"
	if err = db.Create(&models.AlertSilences{TenantId: "t", ID: "s", Name: "old", FaultCenterId: "fc"}).Error; err != nil {
		t.Fatal(err)
	}
	updatePreview, err := service.preview(req)
	if err != nil {
		t.Fatal(err)
	}
	if err = db.Model(&models.AlertSilences{}).Where("id = ?", "s").Update("comment", "another user changed it").Error; err != nil {
		t.Fatal(err)
	}
	updatedPreview, err := service.preview(req)
	if err != nil {
		t.Fatal(err)
	}
	if validateSilencePreview(updatePreview.PreviewHash, updatePreview.PreviewAt, updatedPreview, time.Now().Unix()) == nil {
		t.Fatal("changed original configuration accepted")
	}
	req.ID = ""
	req.TenantId = "other"
	if _, err = service.preview(req); err == nil {
		t.Fatal("cross-tenant center accepted")
	}
	req.TenantId = "t"
	events.err = errors.New("cache unavailable")
	if _, err = service.preview(req); err == nil {
		t.Fatal("cache failure must block preview")
	}
}
