package repo

import (
	"context"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"strings"
	"testing"
	"time"
	"watchAlert/internal/models"
)

type countSelectLogger struct {
	logger.Interface
	selects int
}

func (l *countSelectLogger) Trace(_ context.Context, _ time.Time, fc func() (string, int64), _ error) {
	sql, _ := fc()
	if strings.HasPrefix(strings.ToUpper(sql), "SELECT") {
		l.selects++
	}
}

func TestNoticeTrendUsesOneGroupedQuery(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	conn, _ := db.DB()
	defer conn.Close()
	if err = db.AutoMigrate(&models.NoticeRecord{}); err != nil {
		t.Fatal(err)
	}
	rows := []models.NoticeRecord{
		{TenantId: "t", Date: "2026-10-09", Severity: "P0"}, {TenantId: "t", Date: "2026-10-09", Severity: "P0"},
		{TenantId: "t", Date: "2026-10-10", Severity: "P1"}, {TenantId: "other", Date: "2026-10-09", Severity: "P0"},
		{TenantId: "t", Date: "2026-01-01", Severity: "P0"}, {TenantId: "t", Date: "2026-10-09", Severity: "unknown"},
	}
	if err = db.Create(&rows).Error; err != nil {
		t.Fatal(err)
	}
	log := &countSelectLogger{Interface: logger.Default}
	target := NoticeRepo{entryRepo: entryRepo{db: db.Session(&gorm.Session{Logger: log})}}
	result, err := target.CountRecordsByDate("t", []string{"2026-10-09", "2026-10-10"})
	if err != nil {
		t.Fatal(err)
	}
	if log.selects != 1 || len(result) != 2 {
		t.Fatalf("queries=%d rows=%+v", log.selects, result)
	}
	totals := map[string]int64{}
	for _, row := range result {
		totals[row.Date+row.Severity] = row.Count
	}
	if totals["2026-10-09P0"] != 2 || totals["2026-10-10P1"] != 1 {
		t.Fatal(totals)
	}
	result, err = target.CountRecordsByDate("empty", []string{"2026-10-09"})
	if err != nil || len(result) != 0 {
		t.Fatal("empty tenant", result, err)
	}
}

func TestNoticePageFiltersLimitsAndCancellation(t *testing.T) {
	db := eventTestDB(t)
	if err := db.AutoMigrate(&models.NoticeRecord{}); err != nil {
		t.Fatal(err)
	}
	rows := []models.NoticeRecord{
		{TenantId: "t", EventId: "a", CreateAt: 1, AlarmMsg: "match"},
		{TenantId: "t", EventId: "b", CreateAt: 2, ErrMsg: "match"},
		{TenantId: "other", EventId: "c", CreateAt: 3, ErrMsg: "match"},
		{TenantId: "t", EventId: "d", CreateAt: 4, AlarmMsg: "excluded"},
	}
	if err := db.Create(&rows).Error; err != nil {
		t.Fatal(err)
	}
	r := NoticeRepo{entryRepo: entryRepo{db: db}}
	result, err := r.ListRecord(context.Background(), "t", "", "", "", "", "match", models.Page{Index: 2, Size: 1})
	if err != nil || result.Total != 2 || len(result.List) != 1 || result.List[0].EventId != "a" {
		t.Fatal(result, err)
	}
	result, err = r.ListRecord(context.Background(), "t", "", "", "", "", "", models.Page{})
	if err != nil || result.Size != 20 || result.Index != 1 || result.Total != 3 {
		t.Fatal(result, err)
	}
	if _, err = r.ListRecord(context.Background(), "t", "", "", "", "", "", models.Page{Size: 101}); err == nil {
		t.Fatal("unbounded page allowed")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = r.ListRecord(ctx, "t", "", "", "", "", "", models.Page{}); err == nil {
		t.Fatal("cancelled request succeeded")
	}
}

func TestDatasourceLookupHonorsCancellationAndTenant(t *testing.T) {
	db := eventTestDB(t)
	if err := db.AutoMigrate(&models.AlertDataSource{}); err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&models.AlertDataSource{TenantId: "t", ID: "source"}).Error; err != nil {
		t.Fatal(err)
	}
	r := DatasourceRepo{entryRepo: entryRepo{db: db}}
	if _, err := r.GetForTenantContext(context.Background(), "t", "source"); err != nil {
		t.Fatal(err)
	}
	if _, err := r.GetForTenantContext(context.Background(), "other", "source"); err == nil {
		t.Fatal("tenant filter missing")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := r.GetForTenantContext(ctx, "t", "source"); err == nil {
		t.Fatal("cancelled lookup succeeded")
	}
}
