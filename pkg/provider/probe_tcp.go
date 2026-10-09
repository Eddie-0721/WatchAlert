package provider

import (
	"context"
	"net"
	"time"
)

type Tcper struct{}

// NewMetricsAwareTcper 创建支持指标的TCP探测器
func NewMetricsAwareTcper() MetricsAwareProbe {
	return Tcper{}
}

// PilotWithMetrics 执行TCP探测并直接返回指标
func (p Tcper) PilotWithMetrics(option EndpointOption, ruleInfo ProbeRuleInfo) []Metrics {
	return p.PilotWithMetricsContext(context.Background(), option, ruleInfo)
}

func (p Tcper) PilotWithMetricsContext(ctx context.Context, option EndpointOption, ruleInfo ProbeRuleInfo) []Metrics {
	if ctx.Err() != nil {
		return nil
	}
	startTime := time.Now()

	// 尝试拨测指定地址和端口
	dialer := net.Dialer{Timeout: time.Duration(option.Timeout) * time.Second}
	conn, err := dialer.DialContext(ctx, "tcp", option.Endpoint)
	responseTime := time.Since(startTime)
	if conn != nil {
		_ = conn.Close()
	}
	if ctx.Err() != nil {
		return nil
	}

	// 创建基础标签
	baseLabels := map[string]any{
		"tenant_id":  ruleInfo.TenantID,
		"probe_id":   ruleInfo.RuleID,
		"probe_name": ruleInfo.RuleName,
		"probe_type": ruleInfo.RuleType,
		"endpoint":   ruleInfo.Endpoint,
	}

	for key, value := range ruleInfo.Labels {
		baseLabels[key] = value
	}

	// 确定成功状态
	isSuccessful := err == nil

	// 创建TCP指标
	metrics := []Metrics{
		{
			Name:   "probe_tcp_success",
			Help:   "TCP probe success (1 for success, 0 for failure)",
			Labels: copyLabelsMap(baseLabels),
			Value:  BoolToFloat(isSuccessful),
		},
		{
			Name:   "probe_tcp_response_time_ms",
			Help:   "TCP connection response time in milliseconds",
			Labels: copyLabelsMap(baseLabels),
			Value:  float64(responseTime.Milliseconds()),
		},
	}

	return metrics
}
