package provider

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/go-ping/ping"
)

type Pinger struct{}

// NewMetricsAwarePinger 创建支持指标的ICMP探测器
func NewMetricsAwarePinger() MetricsAwareProbe {
	return Pinger{}
}

// PilotWithMetrics 执行ICMP探测并直接返回指标
func (p Pinger) PilotWithMetrics(option EndpointOption, ruleInfo ProbeRuleInfo) []Metrics {
	return p.PilotWithMetricsContext(context.Background(), option, ruleInfo)
}

func (p Pinger) PilotWithMetricsContext(ctx context.Context, option EndpointOption, ruleInfo ProbeRuleInfo) []Metrics {
	if ctx.Err() != nil {
		return nil
	}
	timestamp := time.Now().Unix()

	// 执行ICMP探测
	var detail PingerInformation
	// Resolve with the caller's cancellation and a finite DNS budget instead of
	// NewPinger's blocking ResolveIPAddr. Prefer IPv4, as ResolveIPAddr("ip") does.
	resolveCtx, cancelResolve := context.WithTimeout(ctx, time.Duration(option.Timeout)*time.Second)
	addr, err := resolveProbeIP(resolveCtx, net.DefaultResolver, option.Endpoint)
	cancelResolve()
	if ctx.Err() != nil {
		return nil
	}
	if err != nil {
		// 返回失败指标
		return p.createFailureMetrics(ruleInfo, timestamp, fmt.Sprintf("New pinger error: %s", err.Error()))
	}
	pinger := ping.New(option.Endpoint)
	pinger.SetIPAddr(addr)
	pinger.SetPrivileged(true)

	// 请求次数
	pinger.Count = option.ICMP.Count
	// 请求间隔
	pinger.Interval = time.Second * time.Duration(option.ICMP.Interval)
	// 超时时间
	pinger.Timeout = time.Second * time.Duration(option.Timeout)

	pinger.OnFinish = func(stats *ping.Statistics) {
		detail = PingerInformation{
			Address:     stats.Addr,
			PacketsSent: stats.PacketsSent,
			PacketsRecv: stats.PacketsRecv,
			PacketLoss:  stats.PacketLoss,
			Addr:        stats.Addr,
			IPAddr:      stats.IPAddr.String(),
			MinRtt:      float64(stats.MinRtt.Milliseconds()),
			MaxRtt:      float64(stats.MaxRtt.Milliseconds()),
			AvgRtt:      float64(stats.AvgRtt.Milliseconds()),
		}
	}

	err = runProbePing(ctx, pinger)
	if ctx.Err() != nil {
		return nil
	}
	if err != nil {
		// 返回失败指标
		return p.createFailureMetrics(ruleInfo, timestamp, fmt.Sprintf("Ping error: %s", err.Error()))
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

	// 创建ICMP指标
	metrics := []Metrics{
		{
			Name:   "probe_icmp_packet_loss_percent",
			Help:   "ICMP packet loss percentage",
			Labels: copyLabelsMap(baseLabels),
			Value:  detail.PacketLoss,
		},
		{
			Name:   "probe_icmp_rtt_min_ms",
			Help:   "ICMP minimum round trip time in milliseconds",
			Labels: copyLabelsMap(baseLabels),
			Value:  detail.MinRtt,
		},
		{
			Name:   "probe_icmp_rtt_max_ms",
			Help:   "ICMP maximum round trip time in milliseconds",
			Labels: copyLabelsMap(baseLabels),
			Value:  detail.MaxRtt,
		},
		{
			Name:   "probe_icmp_rtt_avg_ms",
			Help:   "ICMP average round trip time in milliseconds",
			Labels: copyLabelsMap(baseLabels),
			Value:  detail.AvgRtt,
		},
		{
			Name:   "probe_icmp_packets_sent_total",
			Help:   "Total ICMP packets sent",
			Labels: copyLabelsMap(baseLabels),
			Value:  float64(detail.PacketsSent),
		},
		{
			Name:   "probe_icmp_packets_received_total",
			Help:   "Total ICMP packets received",
			Labels: copyLabelsMap(baseLabels),
			Value:  float64(detail.PacketsRecv),
		},
	}

	return metrics
}

func resolveProbeIP(ctx context.Context, resolver *net.Resolver, endpoint string) (*net.IPAddr, error) {
	addresses, err := resolver.LookupIPAddr(ctx, endpoint)
	if err != nil {
		return nil, err
	}
	if len(addresses) == 0 {
		return nil, fmt.Errorf("no address for probe endpoint")
	}
	for _, address := range addresses {
		if address.IP.To4() != nil {
			return &address, nil
		}
	}
	return &addresses[0], nil
}

// Run remains synchronous: do not return and leave a ping goroutine running.
// go-ping's Stop is concurrency-safe and wakes its receive/send loops.
func runProbePing(ctx context.Context, pinger interface {
	Run() error
	Stop()
}) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	stop := context.AfterFunc(ctx, pinger.Stop)
	defer stop()
	err := pinger.Run()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

// createFailureMetrics 创建失败时的指标
func (p Pinger) createFailureMetrics(ruleInfo ProbeRuleInfo, timestamp int64, errorMsg string) []Metrics {
	baseLabels := map[string]any{
		"tenant_id":  ruleInfo.TenantID,
		"probe_id":   ruleInfo.RuleID,
		"probe_name": ruleInfo.RuleName,
		"probe_type": ruleInfo.RuleType,
		"endpoint":   ruleInfo.Endpoint,
		"error":      errorMsg,
	}

	return []Metrics{
		{
			Name:      "probe_icmp_packet_loss_percent",
			Help:      "ICMP packet loss percentage",
			Labels:    copyLabelsMap(baseLabels),
			Value:     100.0, // 完全失败
			Timestamp: timestamp,
		},
		{
			Name:      "probe_icmp_success",
			Help:      "ICMP probe success (1 for success, 0 for failure)",
			Labels:    copyLabelsMap(baseLabels),
			Value:     0.0, // 失败
			Timestamp: timestamp,
		},
	}
}
