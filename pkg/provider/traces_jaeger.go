package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/bytedance/sonic"
	"net/url"
	"strconv"
	"time"
	"watchAlert/internal/models"
	"watchAlert/pkg/tools"
)

type JaegerDsProvider struct {
	ExternalLabels map[string]interface{}
	url            string
}

func NewJaegerClient(datasource models.AlertDataSource) (TracesFactoryProvider, error) {
	return JaegerDsProvider{
		url:            datasource.HTTP.URL,
		ExternalLabels: datasource.Labels,
	}, nil
}

type JaegerResult struct {
	Data   json.RawMessage `json:"data"`
	Errors []interface{}   `json:"errors"`
}

type JaegerData struct {
	TraceId string `json:"traceID"`
}

func (j JaegerDsProvider) Query(options TraceQueryOptions) ([]Traces, error) {
	return j.QueryContext(context.Background(), options)
}

func (j JaegerDsProvider) QueryContext(requestCtx context.Context, options TraceQueryOptions) ([]Traces, error) {
	curTime := time.Now()

	if options.Limit == 0 {
		options.Limit = 100
	}

	if options.StartAt == 0 {
		duration, _ := time.ParseDuration(strconv.Itoa(1) + "h")
		options.StartAt = curTime.Add(-duration).UnixNano()
	}

	if options.EndAt == 0 {
		options.EndAt = curTime.UnixNano()
	}

	args := fmt.Sprintf("/api/traces?service=%s&start=%d&end=%d&limit=%d&tags=%s", url.QueryEscape(options.Service), options.StartAt, options.EndAt, options.Limit, url.QueryEscape(options.Tags))
	requestURL := j.url + args
	res, err := tools.GetContext(requestCtx, nil, requestURL, 10)
	if err != nil {
		return nil, err
	}

	var jaegerResult JaegerResult
	body, err := readQueryBody(res)
	if err != nil {
		return nil, err
	}
	if err := sonic.Unmarshal(body, &jaegerResult); err != nil {
		return nil, fmt.Errorf("invalid Jaeger JSON response")
	}
	if len(jaegerResult.Errors) != 0 || len(jaegerResult.Data) == 0 {
		return nil, fmt.Errorf("Jaeger query did not return a complete result")
	}
	var traces []JaegerData
	if err := sonic.Unmarshal(jaegerResult.Data, &traces); err != nil {
		return nil, fmt.Errorf("invalid Jaeger traces")
	}

	var data []Traces
	for _, t := range traces {
		if t.TraceId == "" {
			return nil, fmt.Errorf("Jaeger result missing trace ID")
		}
		data = append(data, Traces{
			Service: options.Service,
			TraceId: t.TraceId,
		})
	}

	return data, nil
}

func (j JaegerDsProvider) Check() (bool, error) {
	res, err := tools.Get(nil, j.url, 10)
	if err != nil {
		return false, err
	}
	defer closeHealthBody(res)

	if res.StatusCode != 200 {
		return false, fmt.Errorf("unhealthy status: %d", res.StatusCode)
	}
	return true, nil
}

type JaegerServiceData struct {
	Data []string `json:"data"`
}

func (j JaegerDsProvider) GetJaegerService() (JaegerServiceData, error) {
	url := j.url + "/api/services"
	res, err := tools.Get(nil, url, 10)
	if err != nil {
		return JaegerServiceData{}, err
	}

	body, err := readQueryBody(res)
	if err != nil {
		return JaegerServiceData{}, err
	}

	var resData JaegerServiceData
	if err := sonic.Unmarshal(body, &resData); err != nil {
		return JaegerServiceData{}, fmt.Errorf("invalid Jaeger services response")
	}

	return resData, nil
}

func (j JaegerDsProvider) GetExternalLabels() map[string]interface{} {
	return j.ExternalLabels
}
