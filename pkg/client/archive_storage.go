package client

import (
	"context"
	"fmt"
	"strings"
	"time"
	"watchAlert/internal/models"

	"gorm.io/gorm"
)

// A receipt is safe only when both inserts can roll back together. Do not
// silently run this protocol on a legacy non-transactional MySQL table.
func initArchiveStorage(db *gorm.DB) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	db = db.WithContext(ctx)
	if db.Dialector.Name() == "mysql" {
		if err := checkArchiveTableEngine(db, "alert_his_events"); err != nil {
			return err
		}
		if err := db.Set("gorm:table_options", "ENGINE=InnoDB").AutoMigrate(&models.AlertArchiveReceipt{}); err != nil {
			return err
		}
		return checkArchiveTableEngine(db, "alert_archive_receipts")
	}
	return db.AutoMigrate(&models.AlertArchiveReceipt{})
}

func checkArchiveTableEngine(db *gorm.DB, table string) error {
	var engine string
	if err := db.Raw("SELECT ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?", table).Scan(&engine).Error; err != nil {
		return err
	}
	if !strings.EqualFold(engine, "InnoDB") {
		return fmt.Errorf("%s must use InnoDB for atomic recovery archives; found %q; review migration before upgrading", table, engine)
	}
	return nil
}
