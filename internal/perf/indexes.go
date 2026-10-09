// Package perf contains explicit, operator-reviewed database maintenance plans.
// It is not called by the application's startup migration.
package perf

import (
	"fmt"
	"gorm.io/gorm"
	"strings"
)

type Index struct {
	Name, Table string
	Columns     []string
}

var Indexes = []Index{
	{"idx_perf_rule_lookup", "alert_rules", []string{"tenant_id", "rule_id", "enabled"}},
	{"idx_perf_history_tenant_recovery", "alert_his_events", []string{"tenant_id", "recover_time"}},
	{"idx_perf_history_center_recovery", "alert_his_events", []string{"tenant_id", "fault_center_id", "recover_time"}},
	{"idx_perf_notice_trend", "notice_records", []string{"tenant_id", "date", "severity"}},
	{"idx_perf_notice_recent", "notice_records", []string{"tenant_id", "create_at"}},
	{"idx_perf_notice_retention", "notice_records", []string{"create_at"}},
	{"idx_perf_agent_history", "w8t_agent_messages", []string{"tenant_id", "session_id", "created_at", "id"}},
	{"idx_perf_agent_sessions", "w8t_agent_sessions", []string{"tenant_id", "user_id", "updated_at"}},
	{"idx_perf_audit_recent", "audit_logs", []string{"tenant_id", "created_at"}},
}

// PlanIndexes only inspects metadata and returns DDL for review. It does not
// create indexes, alter column types, backfill values, or delete old indexes.
func PlanIndexes(db *gorm.DB) ([]string, error) {
	dialect := db.Dialector.Name()
	if dialect != "mysql" && dialect != "sqlite" {
		return nil, fmt.Errorf("unsupported database dialect")
	}
	var plan []string
	for _, index := range Indexes {
		if !db.Migrator().HasTable(index.Table) {
			return nil, fmt.Errorf("missing table %s; use an initialized WatchAlert database", index.Table)
		}
		if db.Migrator().HasIndex(index.Table, index.Name) {
			plan = append(plan, "-- Existing "+index.Name+": verify its columns before treating this migration as complete.")
			continue
		}
		types, err := db.Migrator().ColumnTypes(index.Table)
		if err != nil {
			return nil, err
		}
		byName := map[string]gorm.ColumnType{}
		for _, column := range types {
			byName[column.Name()] = column
		}
		var columns []string
		for _, name := range index.Columns {
			column, ok := byName[name]
			if !ok {
				return nil, fmt.Errorf("missing column %s.%s", index.Table, name)
			}
			part := "`" + name + "`"
			if dialect == "mysql" {
				typeName := strings.ToLower(column.DatabaseTypeName())
				length, known := column.Length()
				// Legacy identifiers were TEXT. Do not implicitly shrink them to
				// VARCHAR just to add indexes, or assume every install has that type.
				if strings.Contains(typeName, "text") || (strings.Contains(typeName, "char") && (!known || length > 64)) {
					part += "(64)"
				}
			}
			columns = append(columns, part)
		}
		if dialect == "mysql" {
			plan = append(plan, "ALTER TABLE `"+index.Table+"` ADD INDEX `"+index.Name+"` ("+strings.Join(columns, ", ")+"), ALGORITHM=INPLACE, LOCK=NONE;")
		} else {
			plan = append(plan, "CREATE INDEX `"+index.Name+"` ON `"+index.Table+"` ("+strings.Join(columns, ", ")+");")
		}
	}
	return plan, nil
}
