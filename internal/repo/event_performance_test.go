package repo

import (
	"context"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"math"
	"testing"
	"time"
	"watchAlert/internal/models"
	"watchAlert/internal/types"
)

func eventTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	conn, _ := db.DB()
	conn.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = conn.Close() })
	if err := db.AutoMigrate(&models.AlertHisEvent{}); err != nil {
		t.Fatal(err)
	}
	return db
}

func TestDailySLOAggregatesMatchOriginalSemantics(t *testing.T) {
	db := eventTestDB(t)
	start := time.Date(2026, 10, 1, 0, 0, 0, 0, time.FixedZone("CST", 8*3600))
	boundaries := make([]int64, 8)
	for i := range boundaries {
		boundaries[i] = start.AddDate(0, 0, i).Unix()
	}
	rows := []models.AlertHisEvent{
		{TenantId: "t", FaultCenterId: "fc", FirstTriggerTime: boundaries[0] - 100, RecoverTime: boundaries[0], ConfirmState: models.ConfirmState{ConfirmActionTime: boundaries[0] - 1}},
		{TenantId: "t", FaultCenterId: "fc", FirstTriggerTime: boundaries[1], RecoverTime: boundaries[1] + 200, ConfirmState: models.ConfirmState{ConfirmActionTime: boundaries[1]}},
		{TenantId: "t", FaultCenterId: "fc", FirstTriggerTime: boundaries[0] - 200, RecoverTime: boundaries[0] - 100, ConfirmState: models.ConfirmState{ConfirmActionTime: boundaries[6]}},
		{TenantId: "t", FaultCenterId: "fc", FirstTriggerTime: boundaries[7] - 100, RecoverTime: boundaries[7], ConfirmState: models.ConfirmState{ConfirmActionTime: boundaries[7]}},
	}
	for i := 0; i < 1000; i++ {
		row := models.AlertHisEvent{TenantId: "t", FaultCenterId: "fc", FirstTriggerTime: boundaries[0] + int64(i)*799, RecoverTime: boundaries[0] + int64(i)*811, ConfirmState: models.ConfirmState{ConfirmActionTime: boundaries[0] + int64(i)*803}}
		if i%5 == 0 {
			row.TenantId = "other"
		}
		if i%7 == 0 {
			row.FaultCenterId = "other"
		}
		rows = append(rows, row)
	}
	if err := db.CreateInBatches(rows, 50).Error; err != nil {
		t.Fatal(err)
	}
	log := &countSelectLogger{}
	r := EventRepo{entryRepo: entryRepo{db: db.Session(&gorm.Session{Logger: log})}}
	days, err := r.DailySLO(context.Background(), "t", "fc", boundaries)
	if err != nil {
		t.Fatal(err)
	}
	if log.selects != 1 || len(days) != 7 {
		t.Fatal("expected one aggregate query", log.selects)
	}
	for i, day := range days {
		var sumRepair, sumAck float64
		var repairCount, ackCount int
		for _, row := range rows {
			if row.TenantId != "t" || row.FaultCenterId != "fc" {
				continue
			}
			if row.RecoverTime >= boundaries[i] && row.RecoverTime < boundaries[i+1] && row.RecoverTime > 0 && row.RecoverTime > row.FirstTriggerTime {
				sumRepair += float64(row.RecoverTime - row.FirstTriggerTime)
				repairCount++
			}
			ack := row.ConfirmState.ConfirmActionTime
			if ack >= boundaries[i] && ack < boundaries[i+1] && ack > 0 && row.FirstTriggerTime > 0 && ack >= row.FirstTriggerTime {
				sumAck += float64(ack - row.FirstTriggerTime)
				ackCount++
			}
		}
		if repairCount > 0 {
			sumRepair /= float64(repairCount)
		}
		if ackCount > 0 {
			sumAck /= float64(ackCount)
		}
		if math.Abs(sumRepair-day.MTTR) > 0.00001 || math.Abs(sumAck-day.MTTA) > 0.00001 {
			t.Fatal("changed SLO semantics", i, day, sumRepair, sumAck)
		}
	}
	c, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := r.DailySLO(c, "t", "fc", boundaries); err == nil {
		t.Fatal("cancelled statistics reported success")
	}
}

func TestHistoryPaginationAndCancellation(t *testing.T) {
	db := eventTestDB(t)
	r := EventRepo{entryRepo: entryRepo{db: db}}
	rows := []models.AlertHisEvent{{TenantId: "t", EventId: "a", RecoverTime: 1}, {TenantId: "other", EventId: "b", RecoverTime: 2}, {TenantId: "t", EventId: "c", RecoverTime: 3}}
	if err := db.Create(&rows).Error; err != nil {
		t.Fatal(err)
	}
	result, err := r.GetHistoryEvent(context.Background(), types.RequestAlertHisEventQuery{TenantId: "t", Page: models.Page{Index: 1, Size: 1}})
	if err != nil || result.Total != 2 || len(result.List) != 1 || result.List[0].EventId != "c" {
		t.Fatal(result, err)
	}
	if _, err := r.GetHistoryEvent(context.Background(), types.RequestAlertHisEventQuery{Page: models.Page{Index: 1, Size: 1000000}}); err == nil {
		t.Fatal("unbounded page allowed")
	}
	if result, err := r.GetHistoryEvent(context.Background(), types.RequestAlertHisEventQuery{TenantId: "t", Page: models.Page{Index: 1, Size: 10000}}); err != nil || result.Total != 2 {
		t.Fatal("existing HTML export broken", result, err)
	}
	c, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := r.GetHistoryEvent(c, types.RequestAlertHisEventQuery{TenantId: "t"}); err == nil {
		t.Fatal("cancellation swallowed")
	}
}
