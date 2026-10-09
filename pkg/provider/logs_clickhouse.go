package provider

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
	"watchAlert/internal/models"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/zeromicro/go-zero/core/logc"
)

type ClickHouseProvider struct {
	client         *sql.DB
	ExternalLabels map[string]interface{}
	Ctx            context.Context
}

func NewClickHouseClient(ctx context.Context, ds models.AlertDataSource) (LogsFactoryProvider, error) {
	conn := clickhouse.OpenDB(&clickhouse.Options{
		Addr: []string{ds.ClickHouseConfig.Addr},
		Auth: clickhouse.Auth{
			Username: ds.Auth.User,
			Password: ds.Auth.Pass,
		},
		Settings: clickhouse.Settings{
			"max_execution_time":   60,
			"max_result_rows":      10000,
			"max_result_bytes":     maxQueryBodyBytes,
			"result_overflow_mode": "throw",
		},
		DialTimeout: time.Second * time.Duration(ds.ClickHouseConfig.Timeout),
	})
	if conn == nil {
		return nil, errors.New("clickhouse connection failed")
	}
	conn.SetMaxOpenConns(8)
	conn.SetMaxIdleConns(2)
	conn.SetConnMaxIdleTime(5 * time.Minute)
	conn.SetConnMaxLifetime(30 * time.Minute)

	return ClickHouseProvider{
		client:         conn,
		ExternalLabels: ds.Labels,
		Ctx:            ctx,
	}, nil
}

func (c ClickHouseProvider) Query(options LogQueryOptions) (Logs, int, error) {
	requestCtx := c.Ctx
	if requestCtx == nil {
		requestCtx = context.Background()
	}
	return c.QueryContext(requestCtx, options)
}

func (c ClickHouseProvider) QueryContext(requestCtx context.Context, options LogQueryOptions) (Logs, int, error) {
	queryCtx, cancel := context.WithTimeout(requestCtx, 60*time.Second)
	defer cancel()
	rows, err := c.client.QueryContext(queryCtx, options.ClickHouse.Query)
	if err != nil {
		return Logs{}, 0, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return Logs{}, 0, err
	}

	var messages []map[string]interface{}
	values := make([]interface{}, len(columns))
	dest := make([]interface{}, len(columns))
	for i := range values {
		dest[i] = &values[i]
	}

	for rows.Next() {
		// 扫描数据到 values
		if err := rows.Scan(dest...); err != nil {
			logc.Error(context.Background(), "clickhouse scan error:", err)
			return Logs{}, 0, err
		}

		entry := make(map[string]interface{})
		for i, col := range columns {
			// 取出指针指向的实际数据
			val := values[i]
			if val == nil {
				entry[col] = ""
				continue
			}
			if b, ok := val.([]byte); ok {
				entry[col] = string(b)
			} else {
				entry[col] = val
			}
		}
		messages = append(messages, entry)
		if len(messages) > 10000 {
			return Logs{}, 0, fmt.Errorf("ClickHouse result exceeds 10000 rows")
		}
	}

	if err := rows.Err(); err != nil {
		return Logs{}, 0, err
	}

	return Logs{
		ProviderName: ClickHouseDsProviderName,
		Message:      messages,
	}, len(messages), nil
}

func (c ClickHouseProvider) Check() (bool, error) {
	checkCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := c.client.PingContext(checkCtx)
	if err != nil {
		return false, err
	}

	return true, nil
}

func (c ClickHouseProvider) Close() error { return c.client.Close() }

func (c ClickHouseProvider) GetExternalLabels() map[string]interface{} {
	return c.ExternalLabels
}
