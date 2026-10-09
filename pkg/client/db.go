package client

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"watchAlert/config"
	"watchAlert/internal/models"

	"github.com/glebarez/sqlite"
	"github.com/zeromicro/go-zero/core/logc"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type DBConfig struct {
	Type                   string // 数据库类型: mysql 或 sqlite
	Host                   string // MySQL 主机地址
	Port                   string // MySQL 端口
	User                   string // MySQL 用户名
	Pass                   string // MySQL 密码
	DBName                 string // MySQL 数据库名
	Timeout                string // MySQL 连接超时
	Path                   string // SQLite 数据库文件路径
	MaxOpenConns           int
	MaxIdleConns           *int
	ConnMaxLifetimeSeconds int
	ConnMaxIdleTimeSeconds int
}

// OpenDB connects without migrating schemas or rewriting permission records.
// Maintenance commands must use this rather than the application initializer.
func OpenDB(dbcfg DBConfig) (*gorm.DB, error) {
	if _, err := resolvePoolLimits(dbcfg); err != nil {
		return nil, err
	}
	var db *gorm.DB
	var err error

	// 设置默认数据库类型为 mysql
	if dbcfg.Type == "" {
		dbcfg.Type = "mysql"
	}

	switch dbcfg.Type {
	case "sqlite":
		db, err = initSQLiteDB(dbcfg)
	case "mysql":
		db, err = initMySQLDB(dbcfg)
	default:
		return nil, fmt.Errorf("unsupported database type: %s", dbcfg.Type)
	}

	if err != nil {
		return nil, err
	}
	if err := configurePool(db, dbcfg); err != nil {
		closeDB(db)
		return nil, err
	}
	return db, nil
}

func closeDB(db *gorm.DB) {
	if conn, err := db.DB(); err == nil {
		_ = conn.Close()
	}
}

func NewDBClient(dbcfg DBConfig) *gorm.DB {
	db, err := OpenDB(dbcfg)
	if err != nil {
		logc.Errorf(context.Background(), "failed to initialize database: %s", err.Error())
		return nil
	}

	// 检查 Product 结构是否变化，变化则进行迁移
	err = db.AutoMigrate(
		&models.DutySchedule{},
		&models.DutyManagement{},
		&models.DutyCalendarInfo{},
		&models.AlertNotice{},
		&models.AlertDataSource{},
		&models.AlertRule{},
		&models.AlertCurEvent{},
		&models.AlertHisEvent{},
		&models.AlertSilences{},
		&models.Member{},
		&models.UserRole{},
		&models.UserPermissions{},
		&models.NoticeTemplateExample{},
		&models.RuleGroups{},
		&models.RuleTemplateGroup{},
		&models.RuleTemplate{},
		&models.Tenant{},
		&models.Dashboard{},
		&models.AuditLog{},
		&models.Settings{},
		&models.TenantLinkedUsers{},
		&models.DashboardFolders{},
		&models.AlertSubscribe{},
		&models.NoticeRecord{},
		&models.ProbeRule{},
		&models.FaultCenter{},
		&models.AiContentRecord{},
		&models.Comment{},
		&models.ApiKey{},
		&models.RecordingRuleGroup{},
		&models.RecordingRule{},
		&models.PrometheusTargetGroup{},
		&models.PrometheusTarget{},
		&models.PrometheusTargetVersion{},
		&models.AgentSession{},
		&models.AgentMessage{},
		&models.AgentToolCall{},
		&models.AgentPendingAction{},
	)
	if err != nil {
		logc.Error(context.Background(), err.Error())
		closeDB(db)
		return nil
	}

	if config.Application.Server.Mode == "debug" {
		db.Debug()
	} else {
		db.Logger = logger.Default.LogMode(logger.Silent)
	}

	initPermissionsSQL(db)

	return db
}

// initMySQLDB 初始化 MySQL 数据库连接
func initMySQLDB(config DBConfig) (*gorm.DB, error) {
	if config.Timeout == "" {
		config.Timeout = "10s"
	}
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8mb4,utf8&parseTime=True&loc=Local&timeout=%s",
		config.User,
		config.Pass,
		config.Host,
		config.Port,
		config.DBName,
		config.Timeout)

	return gorm.Open(mysql.Open(dsn), &gorm.Config{})
}

// initSQLiteDB 初始化 SQLite 数据库连接
func initSQLiteDB(config DBConfig) (*gorm.DB, error) {
	// 设置默认 SQLite 文件路径
	if config.Path == "" {
		config.Path = "data/watchalert.db"
	}

	// 确保目录存在
	dir := filepath.Dir(config.Path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	return gorm.Open(sqlite.Open(config.Path), &gorm.Config{})
}

func initPermissionsSQL(db *gorm.DB) {
	var psData []models.UserPermissions

	for _, v := range models.PermissionsInfo() {
		psData = append(psData, v)
	}

	db.Session(&gorm.Session{AllowGlobalUpdate: true}).Delete(&models.UserPermissions{})
	db.Model(&models.UserPermissions{}).Create(&psData)
}
