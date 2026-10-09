package probe

import (
	"fmt"
	"math"
	"strings"
	"time"
	"watchAlert/internal/models"
	"watchAlert/pkg/provider"
)

// ValidateProbeRule 验证拨测规则
func ValidateProbeRule(rule models.ProbeRule) error {
	if rule.RuleId == "" {
		return fmt.Errorf("rule ID cannot be empty")
	}

	if rule.RuleName == "" {
		return fmt.Errorf("rule name cannot be empty")
	}

	return ValidateProbeConfig(rule.RuleType, rule.ProbingEndpointConfig, true)
}

// Validate time conversions before timers/network work. Instant probes do not
// use an evaluation interval, but have the same per-target timeout constraints.
func ValidateProbeConfig(ruleType string, config models.ProbingEndpointConfig, scheduled bool) error {
	const maxSeconds = math.MaxInt64 / int64(time.Second)
	validSeconds := func(value int64) bool { return value > 0 && value <= maxSeconds }
	if !validSeconds(int64(config.Strategy.Timeout)) {
		return fmt.Errorf("probe timeout must be positive seconds within the duration range")
	}
	if scheduled && !validSeconds(config.Strategy.EvalInterval) {
		return fmt.Errorf("probe eval interval must be positive seconds within the duration range")
	}
	for _, endpoint := range strings.Split(config.Endpoint, ",") {
		if strings.TrimSpace(endpoint) == "" {
			return fmt.Errorf("probe endpoint cannot be empty")
		}
	}
	switch ruleType {
	case provider.HTTPEndpointProvider:
		method := strings.ToUpper(config.HTTP.Method)
		if method != provider.GetHTTPMethod && method != provider.PostHTTPMethod {
			return fmt.Errorf("unsupported probe HTTP method")
		}
	case provider.ICMPEndpointProvider:
		if !validSeconds(int64(config.ICMP.Interval)) || config.ICMP.Count <= 0 {
			return fmt.Errorf("ICMP interval must be positive seconds within the duration range and count must be positive")
		}
	case provider.TCPEndpointProvider, provider.SSLEndpointProvider:
	default:
		return fmt.Errorf("unsupported probe type: %s", ruleType)
	}
	return nil
}

// FormatProbeMetricName 格式化探测指标名称
func FormatProbeMetricName(name string) string {
	// 确保指标名称符合Prometheus命名规范
	return name
}

// MergeProbeLabels 合并探测标签
func MergeProbeLabels(base, additional map[string]any) map[string]any {
	result := make(map[string]any)

	// 复制基础标签
	for k, v := range base {
		result[k] = v
	}

	// 添加额外标签
	for k, v := range additional {
		result[k] = v
	}

	return result
}

// CopyLabels 复制标签映射
func CopyLabels(labels map[string]any) map[string]any {
	copied := make(map[string]any)
	for k, v := range labels {
		copied[k] = v
	}
	return copied
}

// BoolToFloat 将布尔值转换为浮点数
func BoolToFloat(b bool) float64 {
	if b {
		return 1.0
	}
	return 0.0
}
