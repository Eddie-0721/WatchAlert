package repo

import (
	"context"
	"fmt"
	"strings"
	"watchAlert/internal/models"
)

type DailySLO struct{ MTTR, MTTA float64 }

// DailySLO returns only aggregates, never full event/label/annotation records.
// Boundaries are local calendar midnights supplied by the caller; grouping
// cannot silently switch the original UI statistics to UTC calendar days.
func (e EventRepo) DailySLO(ctx context.Context, tenantID, centerID string, boundaries []int64) ([]DailySLO, error) {
	if len(boundaries) != 8 {
		return nil, fmt.Errorf("SLO requires seven calendar days")
	}
	for i := 1; i < len(boundaries); i++ {
		if boundaries[i] <= boundaries[i-1] {
			return nil, fmt.Errorf("invalid SLO boundaries")
		}
	}
	ack := "CAST(JSON_EXTRACT(NULLIF(confirm_state, ''), '$.confirmActionTime') AS INTEGER)"
	switch e.db.Dialector.Name() {
	case "mysql":
		ack = "CAST(JSON_UNQUOTE(JSON_EXTRACT(NULLIF(confirm_state, ''), '$.confirmActionTime')) AS SIGNED)"
	case "sqlite":
	default:
		return nil, fmt.Errorf("unsupported SLO database")
	}
	var selections []string
	var args []interface{}
	for i := 0; i < 7; i++ {
		for _, item := range []struct{ condition, duration string }{
			{"recover_time >= ? AND recover_time < ? AND recover_time > 0 AND recover_time > first_trigger_time", "recover_time - first_trigger_time"},
			{ack + " >= ? AND " + ack + " < ? AND " + ack + " > 0 AND first_trigger_time > 0 AND " + ack + " >= first_trigger_time", ack + " - first_trigger_time"},
		} {
			selections = append(selections, "COALESCE(SUM(CASE WHEN "+item.condition+" THEN "+item.duration+" ELSE 0 END), 0)", "COALESCE(SUM(CASE WHEN "+item.condition+" THEN 1 ELSE 0 END), 0)")
			args = append(args, boundaries[i], boundaries[i+1], boundaries[i], boundaries[i+1])
		}
	}
	query := e.db.WithContext(ctx).Model(&models.AlertHisEvent{}).Where("tenant_id = ?", tenantID)
	if centerID != "" {
		query = query.Where("fault_center_id = ?", centerID)
	}
	query = query.Where("((recover_time >= ? AND recover_time < ?) OR ("+ack+" >= ? AND "+ack+" < ?))", boundaries[0], boundaries[7], boundaries[0], boundaries[7])
	values := make([]float64, 28)
	dest := make([]interface{}, len(values))
	for i := range values {
		dest[i] = &values[i]
	}
	if err := query.Select(strings.Join(selections, ", "), args...).Row().Scan(dest...); err != nil {
		return nil, err
	}
	result := make([]DailySLO, 7)
	for i := range result {
		if values[i*4+1] > 0 {
			result[i].MTTR = values[i*4] / values[i*4+1]
		}
		if values[i*4+3] > 0 {
			result[i].MTTA = values[i*4+2] / values[i*4+3]
		}
	}
	return result, nil
}
