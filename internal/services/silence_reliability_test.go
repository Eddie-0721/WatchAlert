package services

import (
	"errors"
	"fmt"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"strings"
	"testing"
	"time"
	"watchAlert/internal/cache"
	"watchAlert/internal/ctx"
	"watchAlert/internal/models"
	"watchAlert/internal/repo"
	"watchAlert/internal/types"
)

type silenceWriteCache struct {
	cache.SilenceCacheInterface
	removed []string
	pushed  []models.AlertSilences
	failure error
}

func (s *silenceWriteCache) RemoveAlertMute(tenant, center, id string) error {
	s.removed = append(s.removed, tenant+"/"+center+"/"+id)
	return s.failure
}
func (s *silenceWriteCache) PushAlertMute(row models.AlertSilences) error {
	s.pushed = append(s.pushed, row)
	return s.failure
}

type silenceEntryCache struct {
	cache.InterEntryCache
	silence *silenceWriteCache
}

func (s silenceEntryCache) Silence() cache.SilenceCacheInterface { return s.silence }
func (s silenceEntryCache) Alert() cache.AlertCacheInterface {
	return &previewEvents{events: map[string]*models.AlertCurEvent{}}
}

type silenceWriteRepo struct {
	repo.InterSilenceRepo
	db      *gorm.DB
	failure error
}

func (s silenceWriteRepo) Create(row models.AlertSilences) error { return s.db.Create(&row).Error }
func (s silenceWriteRepo) Update(row models.AlertSilences) error { return s.db.Save(&row).Error }

func (s silenceWriteRepo) Delete(tenant, id string) error {
	if s.failure != nil {
		return s.failure
	}
	return s.db.Where("tenant_id = ? AND id = ?", tenant, id).Delete(&models.AlertSilences{}).Error
}

type silenceWriteDB struct {
	repo.InterEntryRepo
	db      *gorm.DB
	silence silenceWriteRepo
}

func (s silenceWriteDB) DB() *gorm.DB                   { return s.db }
func (s silenceWriteDB) Silence() repo.InterSilenceRepo { return s.silence }

func TestSilenceSaveDoesNotClaimCacheSuccess(t *testing.T) {
	for _, operation := range []string{"create", "update"} {
		t.Run(operation, func(t *testing.T) {
			db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
			if err != nil {
				t.Fatal(err)
			}
			sql, _ := db.DB()
			defer sql.Close()
			if err := db.AutoMigrate(&models.FaultCenter{}, &models.AlertSilences{}); err != nil {
				t.Fatal(err)
			}
			if err := db.Create(&models.FaultCenter{TenantId: "t", ID: "fc"}).Error; err != nil {
				t.Fatal(err)
			}
			c := &silenceWriteCache{failure: errors.New("cache unavailable")}
			service := alertSilenceService{ctx: &ctx.Context{DB: silenceWriteDB{db: db, silence: silenceWriteRepo{db: db}}, Redis: silenceEntryCache{silence: c}}}
			labels := []models.SilenceLabel{{Key: "env", Operator: "==", Value: "test"}}
			start, end := time.Now().Unix(), time.Now().Add(time.Hour).Unix()
			var data, failure interface{}
			if operation == "create" {
				data, failure = service.Create(&types.RequestSilenceCreate{TenantId: "t", FaultCenterId: "fc", Name: "maintenance", Comment: "release", StartsAt: start, EndsAt: end, Labels: labels})
			} else {
				if err := db.Create(&models.AlertSilences{TenantId: "t", ID: "s", FaultCenterId: "fc", Name: "old"}).Error; err != nil {
					t.Fatal(err)
				}
				data, failure = service.Update(&types.RequestSilenceUpdate{TenantId: "t", ID: "s", FaultCenterId: "fc", Name: "maintenance", Comment: "release", StartsAt: start, EndsAt: end, Labels: labels})
			}
			if failure == nil || !strings.Contains(failure.(error).Error(), "尚未确认生效") {
				t.Fatal("unconfirmed cache reported success", failure)
			}
			saved := data.(models.AlertSilences)
			var stored models.AlertSilences
			if err := db.First(&stored, "id = ?", saved.ID).Error; err != nil || stored.Name != "maintenance" {
				t.Fatal("partial write missing", stored, err)
			}
			if !strings.Contains(failure.(error).Error(), saved.ID) {
				t.Fatal("cannot locate partially saved silence")
			}
		})
	}
}
func TestSilenceDeleteUsesStoredCenterAndReportsFailures(t *testing.T) {
	for _, mode := range []string{"success", "cache_error", "db_error", "foreign"} {
		t.Run(mode, func(t *testing.T) {
			db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
			if err != nil {
				t.Fatal(err)
			}
			sql, _ := db.DB()
			defer sql.Close()
			if err := db.AutoMigrate(&models.AlertSilences{}); err != nil {
				t.Fatal(err)
			}
			original := models.AlertSilences{TenantId: "t", ID: "s", FaultCenterId: "real-center"}
			if err := db.Create(&original).Error; err != nil {
				t.Fatal(err)
			}
			cached := &silenceWriteCache{}
			repository := silenceWriteRepo{db: db}
			if mode == "cache_error" {
				cached.failure = errors.New("redis unavailable")
			}
			if mode == "db_error" {
				repository.failure = errors.New("db unavailable")
			}
			service := alertSilenceService{ctx: &ctx.Context{DB: silenceWriteDB{db: db, silence: repository}, Redis: silenceEntryCache{silence: cached}}}
			request := &types.RequestSilenceQuery{TenantId: "t", ID: "s", FaultCenterId: "wrong-or-empty-center"}
			if mode == "foreign" {
				request.TenantId = "other"
			}
			_, failure := service.Delete(request)
			if (failure == nil) != (mode == "success") {
				t.Fatal("wrong deletion result", failure)
			}
			var count int64
			db.Model(&models.AlertSilences{}).Count(&count)
			if (count == 0) != (mode == "success") {
				t.Fatal("database changed despite rejection")
			}
			if mode == "foreign" {
				if len(cached.removed) > 0 {
					t.Fatal("foreign request touched cache")
				}
				return
			}
			if len(cached.removed) != 1 || cached.removed[0] != "t/real-center/s" {
				t.Fatal("request center used instead of stored object", cached.removed)
			}
			if mode == "db_error" && (len(cached.pushed) != 1 || cached.pushed[0].ID != "s") {
				t.Fatal("cache not compensated after database failure")
			}
		})
	}
}

type silencePageRepo struct {
	repo.InterSilenceRepo
	total    int
	failPage int64
	pages    []int64
}

func (s *silencePageRepo) List(tenant, center, query, status string, page models.Page) ([]models.AlertSilences, int64, error) {
	s.pages = append(s.pages, page.Index)
	if page.Index == s.failPage {
		return nil, 0, errors.New("read failed")
	}
	rows := []models.AlertSilences{}
	start := int((page.Index - 1) * page.Size)
	for i := start; i < start+int(page.Size) && i < s.total; i++ {
		rows = append(rows, models.AlertSilences{ID: fmt.Sprintf("s-%d", i)})
	}
	return rows, int64(s.total), nil
}
func TestSilenceCacheLoadsBeyondFirstPageAndReportsPartialFailure(t *testing.T) {
	for _, mode := range []string{"complete", "read_error", "cache_error"} {
		t.Run(mode, func(t *testing.T) {
			r := &silencePageRepo{total: 2501}
			c := &silenceWriteCache{}
			if mode == "read_error" {
				r.failPage = 2
			}
			if mode == "cache_error" {
				c.failure = errors.New("write failed")
			}
			loaded, err := LoadSilenceCache(r, c)
			if mode == "complete" {
				if err != nil || loaded != 2501 || len(r.pages) != 3 || len(c.pushed) != 2501 {
					t.Fatal("incomplete startup loading", loaded, err)
				}
			} else if err == nil {
				t.Fatal("failed cache sync reported success")
			}
			if mode == "read_error" && loaded != 1000 {
				t.Fatal("wrong partial count")
			}
			if mode == "cache_error" && (loaded != 0 || !strings.Contains(err.Error(), "缓存")) {
				t.Fatal("wrong failed write result")
			}
		})
	}
}
