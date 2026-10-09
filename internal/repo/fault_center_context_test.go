package repo

import (
	"context"
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
