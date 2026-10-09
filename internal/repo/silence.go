package repo

import (
	"context"
	"fmt"
	"watchAlert/internal/models"

	"gorm.io/gorm"
)

type (
	SilenceRepo struct {
		entryRepo
	}

	InterSilenceRepo interface {
		List(tenantId, faultCenterId, query string, status string, page models.Page) ([]models.AlertSilences, int64, error)
		ListContext(context.Context, string, string, string, string, models.Page) ([]models.AlertSilences, int64, error)
		Create(r models.AlertSilences) error
		Update(r models.AlertSilences) error
		TransitionStatus(context.Context, models.AlertSilences, int) (bool, error)
		Delete(tenantId, id string) error
	}
)

func newSilenceInterface(db *gorm.DB, g InterGormDBCli) InterSilenceRepo {
	return &SilenceRepo{
		entryRepo{
			g:  g,
			db: db,
		},
	}
}

// Automatic lifecycle synchronization updates only status, never a stale copy
// of labels/comment/times. Matching the original schedule protects edits/moves.
func (sr SilenceRepo) TransitionStatus(ctx context.Context, before models.AlertSilences, status int) (bool, error) {
	if before.TenantId == "" || before.ID == "" || status < 0 || status > 2 {
		return false, fmt.Errorf("invalid silence transition")
	}
	base := func() *gorm.DB {
		return sr.db.WithContext(ctx).Model(&models.AlertSilences{}).
			Where("tenant_id = ? AND id = ? AND fault_center_id = ? AND starts_at = ? AND ends_at = ? AND update_at = ?", before.TenantId, before.ID, before.FaultCenterId, before.StartsAt, before.EndsAt, before.UpdateAt)
	}
	updated := base().Where("status = ?", before.Status).UpdateColumn("status", status)
	if updated.Error != nil {
		return false, updated.Error
	}
	if updated.RowsAffected > 0 {
		return true, nil
	}
	// A prior SQL success followed by a failed cache write must be retryable.
	var count int64
	if err := base().Where("status = ?", status).Count(&count).Error; err != nil {
		return false, err
	}
	return count > 0, nil
}

func (sr SilenceRepo) List(tenantId, faultCenterId, query string, status string, page models.Page) ([]models.AlertSilences, int64, error) {
	return sr.ListContext(context.Background(), tenantId, faultCenterId, query, status, page)
}

func (sr SilenceRepo) ListContext(ctx context.Context, tenantId, faultCenterId, query string, status string, page models.Page) ([]models.AlertSilences, int64, error) {
	var (
		silenceList []models.AlertSilences
		count       int64
	)
	db := sr.db.WithContext(ctx).Model(models.AlertSilences{})
	if tenantId != "" {
		db.Where("tenant_id = ?", tenantId)
	}

	if faultCenterId != "" {
		db.Where("fault_center_id = ?", faultCenterId)
	}

	if query != "" {
		db.Where("id LIKE ? OR comment LIKE ?", "%"+query+"%", "%"+query+"%")
	}

	if status != "all" {
		db.Where("status = ?", status)
	}

	if err := db.Count(&count).Error; err != nil {
		return nil, 0, err
	}
	db.Order("id ASC")
	db.Limit(int(page.Size)).Offset(int((page.Index - 1) * page.Size))
	err := db.Find(&silenceList).Error
	if err != nil {
		return nil, 0, err
	}

	return silenceList, count, nil
}

func (sr SilenceRepo) Create(r models.AlertSilences) error {
	err := sr.g.Create(models.AlertSilences{}, r)
	if err != nil {
		return err
	}

	return nil
}

func (sr SilenceRepo) Update(r models.AlertSilences) error {
	// Select includes zero-valued status (future silences) while retaining
	// GORM's JSON serializer for Label conditions.
	return sr.db.Model(&models.AlertSilences{}).Where("tenant_id = ? AND id = ?", r.TenantId, r.ID).
		Select("Name", "Labels", "StartsAt", "EndsAt", "UpdateAt", "UpdateBy", "FaultCenterId", "Comment", "Status").Updates(r).Error
}

func (sr SilenceRepo) Delete(tenantId, id string) error {
	var silence models.AlertSilences
	db := sr.db.Where("tenant_id = ? AND id = ?", tenantId, id)
	err := db.First(&silence).Error
	if err != nil {
		return err
	}

	del := Delete{
		Table: models.AlertSilences{},
		Where: map[string]interface{}{
			"tenant_id = ?": tenantId,
			"id = ?":        id,
		},
	}
	err = sr.g.Delete(del)
	if err != nil {
		return err
	}

	return nil
}
