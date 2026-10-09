package probe

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"
	"watchAlert/internal/ctx"
	"watchAlert/internal/models"
	"watchAlert/pkg/provider"

	"github.com/zeromicro/go-zero/core/logc"
	"golang.org/x/sync/errgroup"
)

// ProbeService 拨测服务
type ProbeService struct {
	ctx         *ctx.Context
	watchCtxMap map[string]context.CancelFunc
	mu          sync.RWMutex
}

// NewProbeService 创建新的拨测服务
func NewProbeService(ctx *ctx.Context) *ProbeService {
	return &ProbeService{
		ctx:         ctx,
		watchCtxMap: make(map[string]context.CancelFunc),
	}
}

// Add 添加拨测规则
func (s *ProbeService) Add(rule models.ProbeRule) error {
	if err := ValidateProbeRule(rule); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ctx.Ctx.Err(); err != nil {
		return err
	}

	// 检查规则是否已存在
	if _, exists := s.watchCtxMap[rule.RuleId]; exists {
		return fmt.Errorf("rule %s already exists", rule.RuleId)
	}

	c, cancel := context.WithCancel(s.ctx.Ctx)
	s.watchCtxMap[rule.RuleId] = cancel

	// 启动拨测协程
	go s.runProbing(c, rule)

	logc.Infof(s.ctx.Ctx, "Added probing rule: %s (%s)", rule.RuleName, rule.RuleType)
	return nil
}

// Stop 停止指定规则的拨测
func (s *ProbeService) Stop(ruleID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	cancel, exists := s.watchCtxMap[ruleID]
	if !exists {
		return fmt.Errorf("rule %s not found", ruleID)
	}

	cancel()
	delete(s.watchCtxMap, ruleID)
	return nil
}

// StopAll 停止所有拨测任务
func (s *ProbeService) StopAll() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	count := len(s.watchCtxMap)
	if count == 0 {
		return nil
	}

	logc.Infof(s.ctx.Ctx, "Stopping %d probing tasks...", count)

	// 取消所有拨测任务
	for ruleID, cancel := range s.watchCtxMap {
		cancel()
		delete(s.watchCtxMap, ruleID)
	}

	logc.Infof(s.ctx.Ctx, "Cancellation requested for all probing tasks")
	return nil
}

// GetActiveRules 获取活跃规则数量
func (s *ProbeService) GetActiveRules() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.watchCtxMap)
}

// runProbing 运行拨测
func (s *ProbeService) runProbing(ctx context.Context, rule models.ProbeRule) {
	if ctx.Err() != nil {
		return
	}
	timer := time.NewTicker(time.Second * time.Duration(rule.ProbingEndpointConfig.Strategy.EvalInterval))
	defer timer.Stop()

	// 立即执行一次
	s.executeProbing(ctx, rule)

	for {
		select {
		case <-timer.C:
			s.executeProbing(ctx, rule)
		case <-ctx.Done():
			logc.Infof(s.ctx.Ctx, "Probing stopped for rule: %s", rule.RuleId)
			return
		}
	}
}

// executeProbing 执行拨测
func (s *ProbeService) executeProbing(requestCtx context.Context, rule models.ProbeRule) {
	// 执行拨测并获取指标
	metrics, err := s.executeProbeWithMetrics(requestCtx, rule)
	if requestCtx.Err() != nil {
		return
	}
	if err != nil {
		logc.Errorf(s.ctx.Ctx, "Probing failed for rule %s: %v", rule.RuleId, err)
		return
	}

	// 写入指标到数据源
	if len(metrics) > 0 && rule.DatasourceId != "" {
		pools := s.ctx.Redis.ProviderPools()
		cli, release, err := pools.AcquireClient(rule.DatasourceId)
		defer release()
		if err != nil {
			logc.Errorf(ctx.Ctx, "获取数据源客户端失败, 规则ID: %s, 规则名称: %s, 数据源ID: %s, 错误: %v", rule.RuleId, rule.RuleName, rule.DatasourceId, err)
			return
		}

		if requestCtx.Err() != nil {
			return
		}
		writer, ok := cli.(provider.PrometheusProvider)
		if !ok {
			logc.Errorf(s.ctx.Ctx, "Invalid probing datasource type, rule: %s", rule.RuleId)
			return
		}
		err = writer.Write(requestCtx, metrics, nil)
		if err != nil && requestCtx.Err() == nil {
			logc.Errorf(s.ctx.Ctx, "写入指标失败, 规则ID: %s, 规则名称: %s, 数据源ID: %s, 错误: %v", rule.RuleId, rule.RuleName, rule.DatasourceId, err)
		}
	}
}

// executeProbeWithMetrics 执行拨测并获取指标
func (s *ProbeService) executeProbeWithMetrics(requestCtx context.Context, rule models.ProbeRule) ([]provider.Metrics, error) {
	if err := requestCtx.Err(); err != nil {
		return nil, err
	}
	var metrics []provider.Metrics
	config := rule.ProbingEndpointConfig
	endpoints := strings.Split(config.Endpoint, ",")

	baseInfo := provider.ProbeRuleInfo{
		TenantID: rule.TenantId,
		RuleID:   rule.RuleId,
		RuleName: rule.RuleName,
		Labels:   rule.Labels,
		RuleType: rule.RuleType,
	}

	var prober provider.MetricsAwareProbe
	switch rule.RuleType {
	case provider.HTTPEndpointProvider:
		prober = provider.NewMetricsAwareHTTPer()
	case provider.ICMPEndpointProvider:
		prober = provider.NewMetricsAwarePinger()
	case provider.TCPEndpointProvider:
		prober = provider.NewMetricsAwareTcper()
	case provider.SSLEndpointProvider:
		prober = provider.NewMetricsAwareSSLer()
	default:
		return nil, fmt.Errorf("unsupported rule type: %s", rule.RuleType)
	}
	for _, endpoint := range endpoints {
		if err := requestCtx.Err(); err != nil {
			return nil, err
		}
		baseInfo.Endpoint = endpoint
		result := prober.PilotWithMetricsContext(requestCtx, provider.EndpointOption{
			Endpoint: endpoint, Timeout: config.Strategy.Timeout,
			HTTP: provider.Ehttp{Method: config.HTTP.Method, Header: config.HTTP.Header, Body: config.HTTP.Body},
			ICMP: provider.Eicmp{Interval: config.ICMP.Interval, Count: config.ICMP.Count},
		}, baseInfo)
		if err := requestCtx.Err(); err != nil {
			return nil, err
		}
		metrics = append(metrics, result...)
	}

	return metrics, nil
}

// RePushRule 重新推送规则
func (s *ProbeService) RePushRule() error {
	var ruleList []models.ProbeRule
	if err := s.ctx.DB.DB().Where("enabled = ?", true).Find(&ruleList).Error; err != nil {
		return fmt.Errorf("failed to fetch rules: %w", err)
	}

	g := new(errgroup.Group)
	for _, rule := range ruleList {
		rule := rule
		g.Go(func() error {
			if err := s.Add(rule); err != nil {
				return fmt.Errorf("failed to add rule %s: %w", rule.RuleId, err)
			}
			return nil
		})
	}

	return g.Wait()
}
