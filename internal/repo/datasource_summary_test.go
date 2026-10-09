package repo

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"watchAlert/internal/models"

	"gorm.io/gorm"
)

func TestDatasourceSummaryProjection(t *testing.T) {
	db := eventTestDB(t)
	if err := db.AutoMigrate(&models.AlertDataSource{}); err != nil {
		t.Fatal(err)
	}
	enabled := true
	for _, row := range []models.AlertDataSource{
		{TenantId: "t", ID: "ds", Type: "Prometheus", Name: "Production", Description: "display", Labels: map[string]interface{}{"env": "prod"}, Enabled: &enabled, Auth: models.Auth{User: "hidden", Pass: "hidden"}, HTTP: models.HTTP{URL: "https://hidden.invalid"}, KubeConfig: "hidden"},
		{TenantId: "other", ID: "foreign", Type: "Prometheus"},
		{TenantId: "t", ID: "logs", Type: "Loki"},
	} {
		if err := db.Create(&row).Error; err != nil {
			t.Fatal(err)
		}
	}
	r := DatasourceRepo{entryRepo: entryRepo{db: db}}
	full, err := r.ListContext(context.Background(), "t", "", "Prometheus", "")
	if err != nil {
		t.Fatal(err)
	}
	var statements []string
	if err := db.Callback().Query().After("gorm:query").Register("test:summary_sql", func(tx *gorm.DB) { statements = append(statements, tx.Statement.SQL.String()) }); err != nil {
		t.Fatal(err)
	}
	summary, err := r.ListSummariesContext(context.Background(), "t", "Prometheus")
	if err != nil || len(summary) != 1 {
		t.Fatal(summary, err)
	}
	want := full[0]
	want.TenantId = ""
	want.Auth = models.Auth{}
	want.HTTP = models.HTTP{}
	want.KubeConfig = ""
	if !reflect.DeepEqual(summary[0], want) {
		t.Fatal("metadata changed or connection fields fetched", summary[0])
	}
	if len(statements) != 1 || strings.Contains(statements[0], "SELECT *") || strings.Contains(statements[0], "auth") || strings.Contains(statements[0], "kube_config") {
		t.Fatal("not a metadata-only query", statements)
	}
	if err := db.Model(&models.AlertDataSource{}).Where("id = ?", "ds").Update("auth", "invalid-json").Error; err != nil {
		t.Fatal(err)
	}
	if _, err := r.ListSummariesContext(context.Background(), "t", "Prometheus"); err != nil {
		t.Fatal("metadata decoded unrelated auth", err)
	}
	if _, err := r.GetForTenantContext(context.Background(), "t", "ds"); err == nil {
		t.Fatal("actual query no longer validates its connection configuration")
	}
	for _, tenant := range []string{"", " ", "null", "undefined"} {
		if _, err := r.ListSummariesContext(context.Background(), tenant, "Prometheus"); err == nil {
			t.Fatal("empty tenant allowed")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := r.ListSummariesContext(ctx, "t", "Prometheus"); !errors.Is(err, context.Canceled) {
		t.Fatal("cancellation lost", err)
	}
}

func BenchmarkDatasourceMetadataRead(b *testing.B) {
	for _, size := range []int{20, 200} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			db := eventTestDB(b)
			if err := db.AutoMigrate(&models.AlertDataSource{}); err != nil {
				b.Fatal(err)
			}
			for i := 0; i < size; i++ {
				row := models.AlertDataSource{TenantId: "t", ID: fmt.Sprint(i), Name: "Prometheus", Type: "Prometheus", Labels: map[string]interface{}{"env": "prod", "cluster": "test"}, Auth: models.Auth{User: "test-user", Pass: strings.Repeat("x", 128)}, HTTP: models.HTTP{URL: "https://test.invalid", Headers: map[string]string{"Authorization": strings.Repeat("x", 128)}, Timeout: 10}}
				if err := db.Create(&row).Error; err != nil {
					b.Fatal(err)
				}
			}
			r := DatasourceRepo{entryRepo: entryRepo{db: db}}
			for _, projection := range []bool{false, true} {
				b.Run(fmt.Sprintf("summary=%t", projection), func(b *testing.B) {
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						var rows []models.AlertDataSource
						var err error
						if projection {
							rows, err = r.ListSummariesContext(context.Background(), "t", "Prometheus")
						} else {
							rows, err = r.ListContext(context.Background(), "t", "", "Prometheus", "")
						}
						if err != nil || len(rows) != size {
							b.Fatal(len(rows), err)
						}
					}
				})
			}
		})
	}
}
