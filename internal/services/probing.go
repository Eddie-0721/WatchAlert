package services

import (
	"context"
	"fmt"
	"watchAlert/alert"
	"watchAlert/alert/probe"
	"watchAlert/internal/ctx"
	"watchAlert/internal/models"
	"watchAlert/internal/types"
	"watchAlert/pkg/client"
	"watchAlert/pkg/provider"
	"watchAlert/pkg/tools"

	"time"

	"github.com/zeromicro/go-zero/core/logc"
)

type (
	probingService struct {
		ctx *ctx.Context
	}

	InterProbingService interface {
		Create(req interface{}) (interface{}, interface{})
		Update(req interface{}) (interface{}, interface{})
		Delete(req interface{}) (interface{}, interface{})
		List(req interface{}) (interface{}, interface{})
		Search(req interface{}) (interface{}, interface{})
		Once(req interface{}) (interface{}, interface{})
		OnceContext(requestCtx context.Context, req interface{}) (interface{}, interface{})
		ChangeState(req interface{}) (interface{}, interface{})
	}
)

func newInterProbingService(ctx *ctx.Context) InterProbingService {
	return &probingService{
		ctx: ctx,
	}
}

func (m probingService) Create(req interface{}) (interface{}, interface{}) {
	r := req.(*types.RequestProbingRuleCreate)
	data := models.ProbeRule{
		TenantId:              r.TenantId,
		RuleName:              r.RuleName,
		RuleId:                "r-" + tools.RandId(),
		RuleType:              r.RuleType,
		Labels:                r.Labels,
		ProbingEndpointConfig: r.ProbingEndpointConfig,
		DatasourceId:          r.DatasourceId,
		UpdateAt:              time.Now().Unix(),
		UpdateBy:              r.UpdateBy,
		Enabled:               r.Enabled,
	}

	if err := probe.ValidateProbeRule(data); err != nil {
		return nil, err
	}
	err := m.ctx.DB.Probing().Create(data)
	if err != nil {
		return nil, err
	}

	// 判断当前节点角色
	if *r.GetEnabled() {
		if alert.IsLeader() {
			// Leader: 直接启动拨测协程
			if err := alert.Probe.Add(data); err != nil {
				logc.Errorf(m.ctx.Ctx, "启动拨测任务失败: %v", err)
			}
		} else {
			// Follower: 发布消息通知 Leader
			tools.PublishReloadMessage(m.ctx.Ctx, client.Redis, tools.ChannelProbingReload, tools.ReloadMessage{
				Action:   tools.ActionCreate,
				ID:       data.RuleId,
				TenantID: data.TenantId,
				Name:     data.RuleName,
			})
		}
	}

	return nil, nil
}

func (m probingService) Update(req interface{}) (interface{}, interface{}) {
	r := req.(*types.RequestProbingRuleUpdate)
	data := models.ProbeRule{
		TenantId:              r.TenantId,
		RuleName:              r.RuleName,
		RuleId:                r.RuleId,
		RuleType:              r.RuleType,
		Labels:                r.Labels,
		ProbingEndpointConfig: r.ProbingEndpointConfig,
		DatasourceId:          r.DatasourceId,
		UpdateAt:              time.Now().Unix(),
		UpdateBy:              r.UpdateBy,
		Enabled:               r.Enabled,
	}

	if err := probe.ValidateProbeRule(data); err != nil {
		return nil, err
	}
	_, err := m.ctx.DB.Probing().Search(r.TenantId, r.RuleId)
	if err != nil {
		return nil, err
	}

	err = m.ctx.DB.Probing().Update(data)
	if err != nil {
		return nil, err
	}

	// 判断当前节点角色
	if alert.LeaderElector != nil && alert.LeaderElector.IsLeader() {
		// Leader: 直接重启拨测协程
		if err := alert.Probe.Stop(r.RuleId); err != nil {
			logc.Errorf(m.ctx.Ctx, "停止拨测任务失败: %v", err)
		}
		if *r.GetEnabled() {
			if err := alert.Probe.Add(data); err != nil {
				logc.Errorf(m.ctx.Ctx, "启动拨测任务失败: %v", err)
			}
		}
	} else {
		// Follower: 发布消息通知 Leader
		tools.PublishReloadMessage(m.ctx.Ctx, client.Redis, tools.ChannelProbingReload, tools.ReloadMessage{
			Action:   tools.ActionUpdate,
			ID:       r.RuleId,
			TenantID: r.TenantId,
			Name:     r.RuleName,
		})
	}

	return nil, nil
}

func (m probingService) Delete(req interface{}) (interface{}, interface{}) {
	r := req.(*types.RequestProbingRuleQuery)
	res, err := m.ctx.DB.Probing().Search(r.TenantId, r.RuleId)
	if err != nil {
		return nil, err
	}

	err = m.ctx.DB.Probing().Delete(r.TenantId, r.RuleId)
	if err != nil {
		return nil, err
	}

	// 判断当前节点角色
	if alert.LeaderElector != nil && alert.LeaderElector.IsLeader() {
		// Leader: 直接停止拨测协程
		if err := alert.Probe.Stop(r.RuleId); err != nil {
			logc.Errorf(m.ctx.Ctx, "停止拨测任务失败: %v", err)
		}
	} else {
		// Follower: 发布消息通知 Leader
		tools.PublishReloadMessage(m.ctx.Ctx, client.Redis, tools.ChannelProbingReload, tools.ReloadMessage{
			Action:   tools.ActionDelete,
			ID:       r.RuleId,
			TenantID: r.TenantId,
			Name:     res.RuleName,
		})
	}

	return nil, nil
}

func (m probingService) List(req interface{}) (interface{}, interface{}) {
	r := req.(*types.RequestProbingRuleQuery)
	data, err := m.ctx.DB.Probing().List(r.TenantId, r.RuleType, r.Query)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (m probingService) Search(req interface{}) (interface{}, interface{}) {
	r := req.(*types.RequestProbingRuleQuery)
	data, err := m.ctx.DB.Probing().Search(r.TenantId, r.RuleId)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (m probingService) Once(req interface{}) (interface{}, interface{}) {
	return m.OnceContext(context.Background(), req)
}

func (m probingService) OnceContext(requestCtx context.Context, req interface{}) (interface{}, interface{}) {
	if err := requestCtx.Err(); err != nil {
		return nil, err
	}
	r := req.(*types.RequestProbingOnce)
	ruleConfig := r.ProbingEndpointConfig
	if err := probe.ValidateProbeConfig(r.RuleType, ruleConfig, false); err != nil {
		return nil, err
	}

	// 准备规则信息用于指标标签
	ruleInfo := provider.ProbeRuleInfo{
		RuleID:   "once-" + tools.RandId(), // 临时ID
		RuleName: "Once Probe",
		RuleType: r.RuleType,
		Endpoint: ruleConfig.Endpoint,
	}

	// 根据探测类型执行相应的探测并获取指标
	switch r.RuleType {
	case provider.HTTPEndpointProvider:
		httper := provider.NewMetricsAwareHTTPer()
		metrics := httper.PilotWithMetricsContext(requestCtx, provider.EndpointOption{
			Endpoint: ruleConfig.Endpoint,
			Timeout:  ruleConfig.Strategy.Timeout,
			HTTP: provider.Ehttp{
				Method: ruleConfig.HTTP.Method,
				Header: ruleConfig.HTTP.Header,
				Body:   ruleConfig.HTTP.Body,
			},
		}, ruleInfo)

		return probeOnceResult(requestCtx, r.RuleType, metrics)

	case provider.ICMPEndpointProvider:
		pinger := provider.NewMetricsAwarePinger()
		metrics := pinger.PilotWithMetricsContext(requestCtx, provider.EndpointOption{
			Endpoint: ruleConfig.Endpoint,
			Timeout:  ruleConfig.Strategy.Timeout,
			ICMP: provider.Eicmp{
				Interval: ruleConfig.ICMP.Interval,
				Count:    ruleConfig.ICMP.Count,
			},
		}, ruleInfo)

		return probeOnceResult(requestCtx, r.RuleType, metrics)

	case provider.TCPEndpointProvider:
		tcper := provider.NewMetricsAwareTcper()
		metrics := tcper.PilotWithMetricsContext(requestCtx, provider.EndpointOption{
			Endpoint: ruleConfig.Endpoint,
			Timeout:  ruleConfig.Strategy.Timeout,
		}, ruleInfo)

		return probeOnceResult(requestCtx, r.RuleType, metrics)

	case provider.SSLEndpointProvider:
		ssler := provider.NewMetricsAwareSSLer()
		metrics := ssler.PilotWithMetricsContext(requestCtx, provider.EndpointOption{
			Endpoint: ruleConfig.Endpoint,
			Timeout:  ruleConfig.Strategy.Timeout,
		}, ruleInfo)

		return probeOnceResult(requestCtx, r.RuleType, metrics)

	default:
		err := fmt.Errorf("不支持的探测类型: %s", r.RuleType)
		logc.Errorf(m.ctx.Ctx, "%v", err)
		return nil, err
	}
}

func probeOnceResult(ctx context.Context, ruleType string, metrics []provider.Metrics) (interface{}, interface{}) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	logc.Infof(ctx, "%s即时拨测完成，返回 %d 个指标", ruleType, len(metrics))
	return metrics, nil
}

func (m probingService) ChangeState(req interface{}) (interface{}, interface{}) {
	r := req.(*types.RequestProbeChangeState)
	if r.TenantId == "" || r.RuleId == "" {
		return nil, fmt.Errorf("tenant and probe rule ID are required")
	}
	// Validate persisted legacy configurations before enabling them. An invalid
	// configuration can still be disabled without starting a timer or network IO.
	rule, err := m.ctx.DB.Probing().Search(r.TenantId, r.RuleId)
	if err != nil {
		return nil, err
	}
	if rule.RuleId == "" {
		return nil, fmt.Errorf("probe rule not found")
	}
	if *r.GetEnabled() {
		if err := probe.ValidateProbeRule(rule); err != nil {
			return nil, err
		}
	}
	var action string
	switch *r.GetEnabled() {
	case true:
		action = tools.ActionEnable
	case false:
		action = tools.ActionDisable
	}

	err = m.ctx.DB.Probing().ChangeState(r.TenantId, r.RuleId, r.GetEnabled())
	if err != nil {
		return nil, err
	}

	// 判断当前节点角色
	if alert.LeaderElector != nil && alert.LeaderElector.IsLeader() {
		// Leader: 直接操作协程
		switch *r.GetEnabled() {
		case true:
			if err := alert.Probe.Add(rule); err != nil {
				logc.Errorf(m.ctx.Ctx, "启动拨测任务失败: %v", err)
			}
		case false:
			if err := alert.Probe.Stop(r.RuleId); err != nil {
				logc.Errorf(m.ctx.Ctx, "停止拨测任务失败: %v", err)
			}
		}
	} else {
		// Follower: 发布消息通知 Leader
		tools.PublishReloadMessage(m.ctx.Ctx, client.Redis, tools.ChannelProbingReload, tools.ReloadMessage{
			Action:   action,
			ID:       r.RuleId,
			TenantID: r.TenantId,
			Name:     rule.RuleName,
		})
	}

	return nil, nil
}
