package perf

import (
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"strings"
	"testing"
	"watchAlert/internal/models"
)

func TestIndexPlanIsReadOnlyAndSQLiteQueriesUseIndexes(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	conn, _ := db.DB()
	conn.SetMaxOpenConns(1)
	defer conn.Close()
	if err := db.AutoMigrate(&models.AlertRule{}, &models.AlertHisEvent{}, &models.NoticeRecord{}, &models.AgentMessage{}, &models.AgentSession{}, &models.AuditLog{}); err != nil {
		t.Fatal(err)
	}
	plan, err := PlanIndexes(db)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan) != len(Indexes) {
		t.Fatal("incomplete index plan")
	}
	for _, index := range Indexes {
		if db.Migrator().HasIndex(index.Table, index.Name) {
			t.Fatal("planning executed DDL")
		}
	}
	for _, statement := range plan {
		if err := db.Exec(statement).Error; err != nil {
			t.Fatal(err)
		}
	}
	for _, tc := range []struct{ query, index string }{
		{"SELECT enabled FROM alert_rules WHERE tenant_id='t' AND rule_id='r' LIMIT 1", "idx_perf_rule_lookup"},
		{"SELECT * FROM alert_his_events WHERE tenant_id='t' ORDER BY recover_time DESC LIMIT 20", "idx_perf_history_tenant_recovery"},
		{"SELECT * FROM alert_his_events WHERE tenant_id='t' AND fault_center_id='fc' ORDER BY recover_time DESC LIMIT 20", "idx_perf_history_center_recovery"},
		{"SELECT date,severity,COUNT(*) FROM notice_records WHERE tenant_id='t' AND date IN ('2026-10-10') GROUP BY date,severity", "idx_perf_notice_trend"},
		{"SELECT id,role,SUBSTR(content,1,8001) FROM w8t_agent_messages WHERE tenant_id='t' AND session_id='s' ORDER BY created_at DESC,id DESC LIMIT 11", "idx_perf_agent_history"},
	} {
		var rows []struct{ Detail string }
		if err := db.Raw("EXPLAIN QUERY PLAN " + tc.query).Scan(&rows).Error; err != nil {
			t.Fatal(err)
		}
		var detail string
		for _, row := range rows {
			detail += row.Detail
		}
		if !strings.Contains(detail, tc.index) || strings.Contains(detail, "TEMP B-TREE") {
			t.Fatal("query did not use intended index", tc.index, detail)
		}
	}
	again, err := PlanIndexes(db)
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range again {
		if !strings.HasPrefix(line, "-- Existing") {
			t.Fatal("duplicate DDL proposed", line)
		}
	}
}
