package client

import (
	"fmt"
	"gorm.io/gorm"
	"time"
)

type poolLimits struct {
	open, idle         int
	lifetime, idleTime time.Duration
}

func resolvePoolLimits(c DBConfig) (poolLimits, error) {
	if c.MaxOpenConns > 4096 || c.ConnMaxLifetimeSeconds > 604800 || c.ConnMaxIdleTimeSeconds > 604800 {
		return poolLimits{}, fmt.Errorf("database pool limits exceed supported bounds (4096 connections, 7 days)")
	}
	if c.MaxOpenConns < 0 || c.ConnMaxLifetimeSeconds < 0 || c.ConnMaxIdleTimeSeconds < 0 || (c.MaxIdleConns != nil && *c.MaxIdleConns < 0) {
		return poolLimits{}, fmt.Errorf("database pool limits must not be negative")
	}
	limits := poolLimits{open: 32, idle: 8, lifetime: 30 * time.Minute, idleTime: 5 * time.Minute}
	if c.Type == "sqlite" {
		// SQLite permits one writer. One connection also preserves :memory:
		// databases; recycling the final connection would discard all data.
		limits = poolLimits{open: 1, idle: 1}
		if c.MaxOpenConns > 1 || (c.MaxIdleConns != nil && *c.MaxIdleConns != 1) || c.ConnMaxLifetimeSeconds > 0 || c.ConnMaxIdleTimeSeconds > 0 {
			return poolLimits{}, fmt.Errorf("sqlite requires one retained connection with no connection expiry")
		}
		return limits, nil
	}
	if c.MaxOpenConns > 0 {
		limits.open = c.MaxOpenConns
	}
	if c.MaxIdleConns != nil {
		limits.idle = *c.MaxIdleConns
	}
	if limits.idle > limits.open {
		limits.idle = limits.open
	}
	if c.ConnMaxLifetimeSeconds > 0 {
		limits.lifetime = time.Duration(c.ConnMaxLifetimeSeconds) * time.Second
	}
	if c.ConnMaxIdleTimeSeconds > 0 {
		limits.idleTime = time.Duration(c.ConnMaxIdleTimeSeconds) * time.Second
	}
	return limits, nil
}

func configurePool(db *gorm.DB, c DBConfig) error {
	limits, err := resolvePoolLimits(c)
	if err != nil {
		return err
	}
	conn, err := db.DB()
	if err != nil {
		return err
	}
	conn.SetMaxOpenConns(limits.open)
	conn.SetMaxIdleConns(limits.idle)
	conn.SetConnMaxLifetime(limits.lifetime)
	conn.SetConnMaxIdleTime(limits.idleTime)
	return nil
}
