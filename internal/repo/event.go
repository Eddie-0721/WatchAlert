package repo

import (
	"context"
	"gorm.io/gorm"
	"watchAlert/internal/models"
	"watchAlert/internal/types"
)

type (
	EventRepo struct {
		entryRepo
	}

	InterEventRepo interface {
		GetHistoryEvent(context.Context, types.RequestAlertHisEventQuery) (types.ResponseHistoryEventList, error)
		CreateHistoryEvent(r models.AlertHisEvent) error
		DailySLO(context.Context, string, string, []int64) ([]DailySLO, error)
	}
)

func newEventInterface(db *gorm.DB, g InterGormDBCli) InterEventRepo {
	return &EventRepo{
		entryRepo{
			g:  g,
			db: db,
		},
	}
}

func (e EventRepo) GetHistoryEvent(requestCtx context.Context, r types.RequestAlertHisEventQuery) (types.ResponseHistoryEventList, error) {
	var data []models.AlertHisEvent
	var count int64

	var err error
	// The existing HTML export requests at most 10,000 rows through this API.
	// Preserve it until a separate paginated/streaming export is available.
	r.Page, err = historyPage(r.Page, 10000)
	if err != nil {
		return types.ResponseHistoryEventList{}, err
	}
	db := e.DB().WithContext(requestCtx).Model(&models.AlertHisEvent{}).Where("tenant_id = ?", r.TenantId)
	if r.FaultCenterId != "" {
		db = db.Where("fault_center_id = ?", r.FaultCenterId)
	}

	if r.Query != "" {
		db = db.Where("(rule_name LIKE ? OR severity LIKE ? OR annotations LIKE ? OR fingerprint LIKE ?)", "%"+r.Query+"%", "%"+r.Query+"%", "%"+r.Query+"%", "%"+r.Query+"%")
	}

	if r.DatasourceType != "" {
		db = db.Where("datasource_type = ?", r.DatasourceType)
	}

	if r.Severity != "" {
		db = db.Where("severity = ?", r.Severity)
	}

	if r.StartAt != 0 && r.EndAt != 0 {
		db = db.Where("first_trigger_time > ? and first_trigger_time < ?", r.StartAt, r.EndAt)
	}

	if err := db.Count(&count).Error; err != nil {
		return types.ResponseHistoryEventList{}, err
	}

	switch r.SortOrder {
	case models.SortOrderASC:
		db = db.Order("alarm_duration asc")
	case models.SortOrderDesc:
		db = db.Order("alarm_duration desc")
	default:
		db = db.Order("recover_time desc")
	}

	if err := db.Limit(int(r.Page.Size)).Offset(int((r.Page.Index - 1) * r.Page.Size)).Find(&data).Error; err != nil {
		return types.ResponseHistoryEventList{}, err
	}

	return types.ResponseHistoryEventList{
		List: data,
		Page: models.Page{
			Index: r.Page.Index,
			Size:  r.Page.Size,
			Total: count,
		},
	}, nil
}

func (e EventRepo) CreateHistoryEvent(r models.AlertHisEvent) error {
	err := e.g.Create(models.AlertHisEvent{}, r)
	if err != nil {
		return err
	}

	return nil
}
