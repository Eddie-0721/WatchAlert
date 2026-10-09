package repo

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"watchAlert/internal/models"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func archiveTestRepo(t *testing.T) EventRepo {
	db := eventTestDB(t)
	if err := db.AutoMigrate(&models.AlertArchiveReceipt{}); err != nil {
		t.Fatal(err)
	}
	return EventRepo{entryRepo: entryRepo{db: db}}
}

func TestMySQLReceiptConflictDoesNotOverwriteOwner(t *testing.T) {
	// Dry-run only: no connection, SQL execution or server version query.
	db, err := gorm.Open(mysql.New(mysql.Config{DSN: "unused:unused@tcp(127.0.0.1:1)/unused", SkipInitializeWithVersion: true}), &gorm.Config{DryRun: true, DisableAutomaticPing: true, SkipDefaultTransaction: true})
	if err != nil {
		t.Fatal(err)
	}
	conn, _ := db.DB()
	t.Cleanup(func() { _ = conn.Close() })
	result := db.Clauses(clause.OnConflict{DoNothing: true}).Create(&models.AlertArchiveReceipt{ArchiveKey: "key", Owner: "owner"})
	if result.Error != nil {
		t.Fatal(result.Error)
	}
	sql := result.Statement.SQL.String()
	if !strings.Contains(sql, "ON DUPLICATE KEY UPDATE `archive_key`=`archive_key`") || strings.Contains(sql, "`owner`=") {
		t.Fatal("MySQL upsert would replace archive ownership", sql)
	}
}
func archiveTestEvent() models.AlertHisEvent {
	return models.AlertHisEvent{TenantId: "t", FaultCenterId: "fc", Fingerprint: "fp", RuleId: "rule", EventId: "incident", FirstTriggerTime: 10, RecoverTime: 20, Annotations: "first snapshot"}
}
func assertArchiveCounts(t *testing.T, r EventRepo, history, receipts int64) {
	t.Helper()
	for _, tc := range []struct {
		model any
		want  int64
	}{{&models.AlertHisEvent{}, history}, {&models.AlertArchiveReceipt{}, receipts}} {
		var got int64
		if err := r.db.Model(tc.model).Count(&got).Error; err != nil || got != tc.want {
			t.Fatalf("count=%d want=%d error=%v", got, tc.want, err)
		}
	}
}

func TestArchiveRetriesAndConcurrentCallersCreateOneHistory(t *testing.T) {
	r := archiveTestRepo(t)
	event := archiveTestEvent()
	if err := r.CreateHistoryEvent(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	event.Annotations = "retry must not rewrite history"
	event.RecoverTime++
	var wg sync.WaitGroup
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := r.CreateHistoryEvent(context.Background(), event); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	assertArchiveCounts(t, r, 1, 1)
	var stored models.AlertHisEvent
	if err := r.db.Take(&stored).Error; err != nil {
		t.Fatal(err)
	}
	if stored.Annotations != "first snapshot" || stored.RecoverTime != 20 {
		t.Fatal("history mutated on retry", stored)
	}
}

func TestArchiveHistoryFailureRollsBackReceiptAndCanRetry(t *testing.T) {
	r := archiveTestRepo(t)
	if err := r.db.Callback().Create().Before("gorm:create").Register("test:reject-history", func(tx *gorm.DB) {
		if tx.Statement.Schema != nil && tx.Statement.Schema.Table == "alert_his_events" {
			tx.AddError(errors.New("history write failed"))
		}
	}); err != nil {
		t.Fatal(err)
	}
	if err := r.CreateHistoryEvent(context.Background(), archiveTestEvent()); err == nil {
		t.Fatal("history failure hidden")
	}
	assertArchiveCounts(t, r, 0, 0)
	if err := r.db.Callback().Create().Remove("test:reject-history"); err != nil {
		t.Fatal(err)
	}
	if err := r.CreateHistoryEvent(context.Background(), archiveTestEvent()); err != nil {
		t.Fatal(err)
	}
	assertArchiveCounts(t, r, 1, 1)
}

func TestArchiveCanceledInvalidAndMissingReceiptTableFailWithoutHistory(t *testing.T) {
	r := archiveTestRepo(t)
	c, cancel := context.WithCancel(context.Background())
	cancel()
	if err := r.CreateHistoryEvent(c, archiveTestEvent()); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	event := archiveTestEvent()
	event.FirstTriggerTime = 0
	if err := r.CreateHistoryEvent(context.Background(), event); !errors.Is(err, models.ErrInvalidRecovery) {
		t.Fatal(err)
	}
	assertArchiveCounts(t, r, 0, 0)
	// Separate fresh DB simulates a missing migration, not a production delete.
	missing := EventRepo{entryRepo: entryRepo{db: eventTestDB(t)}}
	if err := missing.CreateHistoryEvent(context.Background(), archiveTestEvent()); err == nil {
		t.Fatal("missing receipt table ignored")
	}
	var count int64
	missing.db.Model(&models.AlertHisEvent{}).Count(&count)
	if count != 0 {
		t.Fatal("history written without transaction receipt")
	}
}

func TestArchiveIdentitySeparatesTenantsCentersAndIncidents(t *testing.T) {
	r := archiveTestRepo(t)
	base := archiveTestEvent()
	events := []models.AlertHisEvent{base, base, base, base, base, base}
	events[1].TenantId = "other"
	events[2].FaultCenterId = "other"
	events[3].EventId = "new"
	events[4].FirstTriggerTime = 11
	events[5].EventId = "" // legacy identity
	for _, event := range events {
		for i := 0; i < 2; i++ {
			if err := r.CreateHistoryEvent(context.Background(), event); err != nil {
				t.Fatal(err)
			}
		}
	}
	assertArchiveCounts(t, r, int64(len(events)), int64(len(events)))
}
