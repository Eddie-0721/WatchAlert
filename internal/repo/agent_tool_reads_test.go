package repo

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
	"watchAlert/internal/models"

	"gorm.io/gorm"
)

func TestAgentToolReadContextsAndFilters(t *testing.T) {
	db := eventTestDB(t)
	if err := db.AutoMigrate(&models.FaultCenter{}, &models.AlertDataSource{}, &models.AlertSilences{}); err != nil {
		t.Fatal(err)
	}
	for _, row := range []models.FaultCenter{{TenantId: "t", ID: "fc", Name: "production"}, {TenantId: "other", ID: "foreign", Name: "production"}} {
		if err := db.Create(&row).Error; err != nil {
			t.Fatal(err)
		}
	}
	for _, row := range []models.AlertDataSource{{TenantId: "t", ID: "ds", Name: "target", Type: "Prometheus"}, {TenantId: "t", ID: "loki", Name: "target", Type: "Loki"}, {TenantId: "other", ID: "foreign", Name: "target", Type: "Prometheus"}} {
		if err := db.Create(&row).Error; err != nil {
			t.Fatal(err)
		}
	}
	for _, row := range []models.AlertSilences{{TenantId: "t", ID: "s01", FaultCenterId: "fc", Status: 1, Comment: "target"}, {TenantId: "t", ID: "s02", FaultCenterId: "fc", Status: 1, Comment: "target"}, {TenantId: "t", ID: "s03", FaultCenterId: "elsewhere", Status: 1, Comment: "target"}, {TenantId: "other", ID: "s04", FaultCenterId: "fc", Status: 1, Comment: "target"}} {
		if err := db.Create(&row).Error; err != nil {
			t.Fatal(err)
		}
	}
	entry := entryRepo{db: db}
	center, source, silence := faultCenterRepo{entryRepo: entry}, DatasourceRepo{entryRepo: entry}, SilenceRepo{entryRepo: entry}
	ctx := context.Background()
	if row, err := center.GetContext(ctx, "t", "", "production"); err != nil || row.ID != "fc" {
		t.Fatal("center filters changed", row, err)
	}
	if _, err := center.GetContext(ctx, "t", "foreign", ""); !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatal("foreign center exposed", err)
	}
	if rows, err := source.ListContext(ctx, "t", "", "Prometheus", "target"); err != nil || len(rows) != 1 || rows[0].ID != "ds" {
		t.Fatal("datasource filters changed", rows, err)
	}
	if rows, err := source.ListContext(ctx, "t", "", "Prometheus,Loki", "target"); err != nil || len(rows) != 2 {
		t.Fatal("multi-type filters changed", rows, err)
	}
	if rows, count, err := silence.ListContext(ctx, "t", "fc", "target", "1", models.Page{Index: 2, Size: 1}); err != nil || count != 2 || len(rows) != 1 || rows[0].ID != "s02" {
		t.Fatal("silence filtering/paging changed", rows, count, err)
	}
	for name, read := range map[string]func(context.Context) error{
		"center": func(ctx context.Context) error { _, err := center.GetContext(ctx, "t", "fc", ""); return err },
		"datasources": func(ctx context.Context) error {
			_, err := source.ListContext(ctx, "t", "", "Prometheus", "")
			return err
		},
		"summaries": func(ctx context.Context) error {
			_, err := source.ListSummariesContext(ctx, "t", "Prometheus")
			return err
		},
		"datasource": func(ctx context.Context) error { _, err := source.GetForTenantContext(ctx, "t", "ds"); return err },
		"silences": func(ctx context.Context) error {
			_, _, err := silence.ListContext(ctx, "t", "fc", "", "all", models.Page{Index: 1, Size: 20})
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			pool, _ := db.DB()
			connection, err := pool.Conn(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer connection.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
			defer cancel()
			result := make(chan error, 1)
			go func() { result <- read(ctx) }()
			select {
			case err := <-result:
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatal("database wait ignored cancellation", err)
				}
			case <-time.After(2 * time.Second):
				connection.Close()
				<-result
				t.Fatal("database wait was not bounded")
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := db.Callback().Query().After("gorm:query").Register("test:cancel_after_count", func(tx *gorm.DB) {
		if strings.Contains(strings.ToLower(tx.Statement.SQL.String()), "count(*)") {
			cancel()
		}
	}); err != nil {
		t.Fatal(err)
	}
	if rows, count, err := silence.ListContext(ctx, "t", "fc", "", "all", models.Page{Index: 1, Size: 20}); !errors.Is(err, context.Canceled) || rows != nil || count != 0 {
		t.Fatal("canceled list exposed partial count", rows, count, err)
	}
}
