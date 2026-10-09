package repo

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
	"watchAlert/internal/models"
)

func TestCenterIdentitiesScopeAndProjection(t *testing.T) {
	db := eventTestDB(t)
	if err := db.AutoMigrate(&models.FaultCenter{}); err != nil {
		t.Fatal(err)
	}
	for _, row := range []models.FaultCenter{
		{TenantId: "t", ID: "mine", Name: "Production", Description: "not needed"},
		{TenantId: "other", ID: "foreign", Name: "Private"},
	} {
		if err := db.Create(&row).Error; err != nil {
			t.Fatal(err)
		}
	}
	r := faultCenterRepo{entryRepo: entryRepo{db: db}}
	rows, err := r.ListIdentities(context.Background(), "t")
	if err != nil || len(rows) != 1 || rows[0].ID != "mine" || rows[0].Name != "Production" || rows[0].Description != "" {
		t.Fatal("scope/projection incorrect", rows, err)
	}
	// An empty tenant must not expand into an all-tenant query.
	rows, err = r.ListIdentities(context.Background(), "")
	if err != nil || len(rows) != 0 {
		t.Fatal("empty tenant leaked data", rows, err)
	}

	pool, _ := db.DB()
	conn, err := pool.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	_, err = r.ListIdentities(ctx, "t")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("pool wait ignored context", err)
	}
}

func TestCenterOptionsProjectionSearchAndCancellation(t *testing.T) {
	db := eventTestDB(t)
	if err := db.AutoMigrate(&models.FaultCenter{}); err != nil {
		t.Fatal(err)
	}
	for _, row := range []models.FaultCenter{
		{TenantId: "t", ID: "mine", Name: "Production", Description: "payments"},
		{TenantId: "other", ID: "foreign", Name: "Production", Description: "payments"},
	} {
		if err := db.Create(&row).Error; err != nil {
			t.Fatal(err)
		}
	}
	// Configuration need not be decoded for a selector. This would fail if the
	// query accidentally loaded the full model instead of its id/name columns.
	if err := db.Model(&models.FaultCenter{}).Where("id = ?", "mine").Update("noticeRoutes", "invalid-json").Error; err != nil {
		t.Fatal(err)
	}
	r := faultCenterRepo{entryRepo: entryRepo{db: db}}
	for _, query := range []string{"", "Production", "mine", "payments"} {
		rows, err := r.ListOptions(context.Background(), "t", query)
		if err != nil || len(rows) != 1 || rows[0].ID != "mine" || rows[0].Name != "Production" {
			t.Fatal("scope/search/projection incorrect", query, rows, err)
		}
		raw, err := json.Marshal(rows)
		if err != nil || string(raw) != `[{"id":"mine","name":"Production"}]` {
			t.Fatal("option response must not include fake counts/config", string(raw), err)
		}
	}
	for _, pair := range [][2]string{{"", ""}, {"t", "missing"}, {"t", "' OR 1=1 --"}} {
		rows, err := r.ListOptions(context.Background(), pair[0], pair[1])
		if err != nil || len(rows) != 0 || rows == nil {
			t.Fatal("empty result must be scoped and encoded as []", rows, err)
		}
	}
	pool, _ := db.DB()
	conn, err := pool.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	if _, err := r.ListOptions(ctx, "t", ""); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("options SQL must stop waiting for canceled requests", err)
	}
}
