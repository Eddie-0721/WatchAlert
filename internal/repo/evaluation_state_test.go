package repo

import (
	"context"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"testing"
	"watchAlert/internal/models"
)

func TestEvaluationEnabledLookupIsTenantScopedAndFresh(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	conn, _ := db.DB()
	defer conn.Close()
	if err := db.AutoMigrate(&models.AlertRule{}, &models.RecordingRule{}); err != nil {
		t.Fatal(err)
	}
	on := true
	if err := db.Create(&models.AlertRule{TenantId: "t", RuleId: "alert", Enabled: &on}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&models.RecordingRule{TenantId: "t", RuleId: "recording", Enabled: &on}).Error; err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		id    string
		model interface{}
		check func(context.Context, string, string) (bool, error)
	}{
		{"alert", &models.AlertRule{}, RuleRepo{entryRepo: entryRepo{db: db}}.IsEnabled},
		{"recording", &models.RecordingRule{}, RecordingRuleRepo{entryRepo: entryRepo{db: db}}.IsEnabled},
	} {
		t.Run(tc.id, func(t *testing.T) {
			if on, err := tc.check(context.Background(), "t", tc.id); err != nil || !on {
				t.Fatal(on, err)
			}
			if on, err := tc.check(context.Background(), "foreign", tc.id); err == nil || on {
				t.Fatal("foreign tenant enabled")
			}
			if err := db.Model(tc.model).Where("tenant_id = ? AND rule_id = ?", "t", tc.id).Update("enabled", false).Error; err != nil {
				t.Fatal(err)
			}
			if on, err := tc.check(context.Background(), "t", tc.id); err != nil || on {
				t.Fatal("stale enabled state", on, err)
			}
			c, cancel := context.WithCancel(context.Background())
			cancel()
			if on, err := tc.check(c, "t", tc.id); err == nil || on {
				t.Fatal("cancelled lookup accepted")
			}
		})
	}
}
