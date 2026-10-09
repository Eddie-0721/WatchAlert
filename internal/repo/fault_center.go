package repo

import (
	"context"
	"gorm.io/gorm"
	"watchAlert/internal/models"
)

type (
	faultCenterRepo struct {
		entryRepo
	}

	InterFaultCenterRepo interface {
		Create(params models.FaultCenter) error
		Update(params models.FaultCenter) error
		Delete(tenantId, id string) error
		List(tenantId, query string) ([]models.FaultCenter, error)
		ListContext(context.Context, string, string) ([]models.FaultCenter, error)
		ListIdentities(context.Context, string) ([]models.FaultCenter, error)
		ListOptions(context.Context, string, string) ([]models.FaultCenterOption, error)
		Get(tenantId, id, name string) (models.FaultCenter, error)
		GetContext(context.Context, string, string, string) (models.FaultCenter, error)
		Reset(tenantId, id, name, description, aggregationType string) error
	}
)

func newInterFaultCenterRepo(db *gorm.DB, g InterGormDBCli) InterFaultCenterRepo {
	return &faultCenterRepo{
		entryRepo{
			g:  g,
			db: db,
		},
	}
}

func (f faultCenterRepo) Create(params models.FaultCenter) error {
	err := f.g.Create(&models.FaultCenter{}, params)
	if err != nil {
		return err
	}
	return nil
}

func (f faultCenterRepo) Update(params models.FaultCenter) error {
	u := Updates{
		Table: &models.FaultCenter{},
		Where: map[string]interface{}{
			"tenant_id = ?": params.TenantId,
			"id = ?":        params.ID,
		},
		Updates: params,
	}
	err := f.g.Updates(u)
	if err != nil {
		return err
	}
	return nil
}

func (f faultCenterRepo) Delete(tenantId, id string) error {
	del := Delete{
		Table: &models.FaultCenter{},
		Where: map[string]interface{}{
			"tenant_id = ?": tenantId,
			"id = ?":        id,
		},
	}
	err := f.g.Delete(del)
	if err != nil {
		return err
	}
	return nil
}

// ListIdentities avoids loading routing/notification configuration for event lists.
func (f faultCenterRepo) ListIdentities(ctx context.Context, tenantID string) ([]models.FaultCenter, error) {
	var centers []models.FaultCenter
	err := f.db.WithContext(ctx).Model(&models.FaultCenter{}).Select("id", "name").Where("tenant_id = ?", tenantID).Find(&centers).Error
	return centers, err
}

func (f faultCenterRepo) ListOptions(ctx context.Context, tenantID, query string) ([]models.FaultCenterOption, error) {
	data := make([]models.FaultCenterOption, 0)
	db := f.db.WithContext(ctx).Model(&models.FaultCenter{}).Select("id", "name").Where("tenant_id = ?", tenantID)
	if query != "" {
		db = db.Where("name LIKE ? OR id LIKE ? OR description LIKE ?", "%"+query+"%", "%"+query+"%", "%"+query+"%")
	}
	err := db.Find(&data).Error
	return data, err
}

func (f faultCenterRepo) List(tenantId, query string) ([]models.FaultCenter, error) {
	return f.ListContext(context.Background(), tenantId, query)
}

func (f faultCenterRepo) ListContext(ctx context.Context, tenantId, query string) ([]models.FaultCenter, error) {
	var (
		data []models.FaultCenter
		db   = f.db.WithContext(ctx).Model(&models.FaultCenter{})
	)

	if tenantId != "" {
		db.Where("tenant_id = ?", tenantId)
	}
	if query != "" {
		db.Where("name LIKE ? OR id LIKE ? OR description LIKE ?", "%"+query+"%", "%"+query+"%", "%"+query+"%")
	}

	err := db.Find(&data).Error
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (f faultCenterRepo) Get(tenantId, id, name string) (models.FaultCenter, error) {
	return f.GetContext(context.Background(), tenantId, id, name)
}

func (f faultCenterRepo) GetContext(ctx context.Context, tenantId, id, name string) (models.FaultCenter, error) {
	var (
		data models.FaultCenter
		db   = f.db.WithContext(ctx).Model(&models.FaultCenter{})
	)

	if tenantId != "" {
		db.Where("tenant_id = ?", tenantId)
	}
	if name != "" {
		db.Where("name = ?", name)
	}
	if id != "" {
		db.Where("id = ?", id)
	}

	err := db.First(&data).Error
	if err != nil {
		return data, err
	}
	return data, nil
}

func (f faultCenterRepo) Reset(tenantId, id, name, description, aggregationType string) error {
	var update []string

	if name != "" {
		update = []string{"name", name}
	}
	if description != "" {
		update = []string{"description", description}
	}
	if aggregationType != "" {
		update = []string{"aggregation_type", aggregationType}
	}

	if update != nil {
		err := f.g.Update(Update{
			Table: &models.FaultCenter{},
			Where: map[string]interface{}{
				"tenant_id = ?": tenantId,
				"id = ?":        id,
			},
			Update: update,
		})
		if err != nil {
			return err
		}
	}

	return nil
}
