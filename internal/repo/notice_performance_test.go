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
