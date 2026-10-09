package provider

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/url"
	"time"
	"watchAlert/internal/models"
	"watchAlert/pkg/tools"

	"github.com/bytedance/sonic"

	"github.com/zeromicro/go-zero/core/logc"
)

type (
	VictoriaLogsProvider struct {
		URL            string         `json:"url"`
		Timeout        int64          `json:"timeout"`
		ExternalLabels map[string]any `json:"external_labels"`
		Ctx            context.Context
		Username       string            `json:"username"`
		Password       string            `json:"password"`
		Headers        map[string]string `json:"headers"`
	}
)

// NewVictoriaLogsClient 创建一个新的 VictoriaLogsProvider 实例。
func NewVictoriaLogsClient(ctx context.Context, datasource models.AlertDataSource) (LogsFactoryProvider, error) {
	return VictoriaLogsProvider{
		URL:            datasource.HTTP.URL,
		Timeout:        datasource.HTTP.Timeout,
		ExternalLabels: datasource.Labels,
		Username:       datasource.Auth.User,
		Password:       datasource.Auth.Pass,
		Ctx:            ctx,
		Headers:        datasource.HTTP.Headers,
	}, nil
}

func (v VictoriaLogsProvider) Query(options LogQueryOptions) (Logs, int, error) {
	requestCtx := v.Ctx
	if requestCtx == nil {
		requestCtx = context.Background()
	}
	return v.QueryContext(requestCtx, options)
}

func (v VictoriaLogsProvider) QueryContext(requestCtx context.Context, options LogQueryOptions) (Logs, int, error) {
	curTime := time.Now()

	if options.StartAt == "" || options.StartAt == nil {
		options.StartAt = int32(tools.ParserDuration(curTime, 30, "m").Unix())
	}

	if options.EndAt == "" || options.EndAt == nil {
		options.EndAt = int32(curTime.Unix())
	}

	if options.VictoriaLogs.Limit == 0 {
		options.VictoriaLogs.Limit = 500
	}

	args := fmt.Sprintf("/select/logsql/query?query=%s&limit=%d&start=%d&end=%d", url.QueryEscape(options.VictoriaLogs.Query), options.VictoriaLogs.Limit, options.StartAt.(int32), options.EndAt.(int32))
	requestURL := v.URL + args

	var headers = make(map[string]string)
	for key, value := range v.Headers {
		headers[key] = value
	}
	for key, value := range tools.CreateBasicAuthHeader(v.Username, v.Password) {
		headers[key] = value
	}

	res, err := tools.GetContext(requestCtx, headers, requestURL, int(v.Timeout))

	if err != nil {
		return Logs{}, 0, err
	}

	respBody, err := readQueryBody(res)
	if err != nil {
		return Logs{}, 0, err
	}

	var (
		message []map[string]interface{}
		count   int
	)
	scanner := bufio.NewScanner(bytes.NewReader(respBody))
	scanner.Buffer(make([]byte, 4096), maxQueryBodyBytes+1)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var msg map[string]interface{}
		if err := sonic.Unmarshal(line, &msg); err != nil {
			return Logs{}, 0, fmt.Errorf("invalid VictoriaLogs JSON line")
		}
		if msg == nil {
			return Logs{}, 0, fmt.Errorf("invalid VictoriaLogs empty record")
		}
		message = append(message, msg)
		count++
	}
	if err := scanner.Err(); err != nil {
		return Logs{}, 0, fmt.Errorf("incomplete VictoriaLogs result: %w", err)
	}

	return Logs{
		ProviderName: VictoriaLogsDsProviderName,
		Message:      message,
	}, count, nil
}

func (v VictoriaLogsProvider) Check() (bool, error) {
	var headers = make(map[string]string)
	for key, value := range v.Headers {
		headers[key] = value
	}
	for key, value := range tools.CreateBasicAuthHeader(v.Username, v.Password) {
		headers[key] = value
	}

	res, err := tools.Get(headers, v.URL+"/health", int(v.Timeout))
	if err != nil {
		return false, err
	}
	defer closeHealthBody(res)

	if res.StatusCode != http.StatusOK {
		logc.Error(v.Ctx, fmt.Errorf("unhealthy status: %d", res.StatusCode))
		return false, fmt.Errorf("unhealthy status: %d", res.StatusCode)
	}
	return true, nil
}

func (v VictoriaLogsProvider) GetExternalLabels() map[string]interface{} {
	return v.ExternalLabels
}
