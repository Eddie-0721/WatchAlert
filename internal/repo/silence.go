package repo

import (
	"watchAlert/internal/models"

	"gorm.io/gorm"
)

type (
	SilenceRepo struct {
		entryRepo
	}

	InterSilenceRepo interface {
		List(tenantId, faultCenterId, query string, status string, page models.Page) ([]models.AlertSilences, int64, error)
		Create(r models.AlertSilences) error
		Update(r models.AlertSilences) error
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

func (sr SilenceRepo) List(tenantId, faultCenterId, query string, status string, page models.Page) ([]models.AlertSilences, int64, error) {
	var (
		silenceList []models.AlertSilences
		count       int64
	)
	db := sr.db.Model(models.AlertSilences{})
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

	db.Count(&count)
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
