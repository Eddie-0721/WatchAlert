package client

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestPoolDefaultsAndValidation(t *testing.T) {
	limits, err := resolvePoolLimits(DBConfig{})
	if err != nil || limits.open != 32 || limits.idle != 8 || limits.lifetime != 30*time.Minute {
		t.Fatal(limits, err)
	}
	zero := 0
	limits, err = resolvePoolLimits(DBConfig{MaxOpenConns: 4, MaxIdleConns: &zero})
	if err != nil || limits.idle != 0 || limits.open != 4 {
		t.Fatal(limits, err)
	}
	for _, c := range []DBConfig{{MaxOpenConns: -1}, {MaxOpenConns: 5000}, {ConnMaxIdleTimeSeconds: -1}, {ConnMaxLifetimeSeconds: 1 << 62}, {Type: "sqlite", MaxOpenConns: 2}, {Type: "sqlite", MaxIdleConns: &zero}} {
		if _, err := resolvePoolLimits(c); err == nil {
			t.Fatal("invalid pool config accepted", c.Type)
		}
	}
}

func TestSQLitePoolWaitIsCancellableAndDatabaseSurvives(t *testing.T) {
	db, err := OpenDB(DBConfig{Type: "sqlite", Path: ":memory:"})
	if err != nil {
		t.Fatal(err)
	}
	conn, _ := db.DB()
	defer conn.Close()
	if conn.Stats().MaxOpenConnections != 1 {
		t.Fatal("sqlite pool must be bounded")
	}
	var tableCount int
	if err := db.Raw("SELECT count(*) FROM sqlite_master WHERE type = 'table'").Scan(&tableCount).Error; err != nil || tableCount != 0 {
		t.Fatal("OpenDB performed a migration", err)
	}
	if err := db.Exec("CREATE TABLE pool_test (n INTEGER)").Error; err != nil {
		t.Fatal(err)
	}
	held, err := conn.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	c, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := conn.ExecContext(c, "SELECT 1"); err == nil {
		t.Fatal("blocked query ignored cancellation")
	}
	_ = held.Close()
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := db.Exec("INSERT INTO pool_test VALUES (1)").Error; err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	var count int64
	if err := db.Raw("SELECT COUNT(*) FROM pool_test").Scan(&count).Error; err != nil || count != 20 {
		t.Fatal("in-memory database lost", count, err)
	}
}
