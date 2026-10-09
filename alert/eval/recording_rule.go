package eval

import (
	"context"
	"fmt"
	"runtime/debug"
	"time"
	"watchAlert/internal/ctx"
	"watchAlert/internal/models"
	"watchAlert/pkg/provider"

	"github.com/zeromicro/go-zero/core/logc"
)

type (
	// RecordingRuleEval 记录规则评估
	RecordingRuleEval interface {
		Submit(rule models.RecordingRule)
		Stop(ruleId string)
		Eval(ctx context.Context, rule models.RecordingRule)
		RestartAllEvals()
		StopAllEvals()
	}

	// RecordingRule 记录规则
	RecordingRule struct {
		ctx *ctx.Context
	}
)

func NewRecordingRuleEval(ctx *ctx.Context) RecordingRuleEval {
	return &RecordingRule{
		ctx: ctx,
	}
}

// Submit 提交记录规则评估任务
func (t *RecordingRule) Submit(rule models.RecordingRule) {
	t.ctx.Mux.Lock()
	defer t.ctx.Mux.Unlock()

	if previous, exists := t.ctx.ContextMap[rule.RuleId]; exists {
		previous()
	}
	c, cancel := context.WithCancel(t.ctx.Ctx)
	t.ctx.ContextMap[rule.RuleId] = cancel
	go t.Eval(c, rule)
}

// Stop 停止记录规则评估任务
func (t *RecordingRule) Stop(ruleId string) {
	t.ctx.Mux.Lock()
	defer t.ctx.Mux.Unlock()

	if cancel, exists := t.ctx.ContextMap[ruleId]; exists {
		cancel()
		delete(t.ctx.ContextMap, ruleId)
	}
}

// Eval 执行记录规则评估
func (t *RecordingRule) Eval(ctx context.Context, rule models.RecordingRule) {
	err := rule.Validate()
	if err != nil {
		logc.Errorf(t.ctx.Ctx, "Recording rule validation failed, MetricName: %s, RuleId: %s, Error: %v", rule.MetricName, rule.RuleId, err)
		return
	}

	taskChan := make(chan struct{}, TaskChannelBufferSize)
	timer := time.NewTicker(t.getEvalTimeDuration(rule.EvalInterval))
	defer func() {
		timer.Stop()
		if r := recover(); r != nil {
			// 获取调用栈信息
			stack := debug.Stack()
			logc.Errorf(t.ctx.Ctx, "Recovered from recording rule eval goroutine panic: %s, MetricName: %s, RuleId: %s\n%s", r, rule.MetricName, rule.RuleId, stack)
			if ctx.Err() == nil {
				t.Restart(rule)
			}
		}
	}()

	for {
		select {
		case <-timer.C:
			// 处理任务信号量
			taskChan <- struct{}{}
			logc.Infof(t.ctx.Ctx, fmt.Sprintf("Handle recoding eval task, RuleId: %v, MetricName: %s", rule.RuleId, rule.MetricName))
			t.executeTask(ctx, rule, taskChan)
		case <-ctx.Done():
			logc.Infof(t.ctx.Ctx, fmt.Sprintf("Stop recoding eval task, RuleId: %v, MetricName: %s", rule.RuleId, rule.MetricName))
			return
		}
		timer.Reset(t.getEvalTimeDuration(rule.EvalInterval))
	}
}

// executeTask 执行评估任务
func (t *RecordingRule) executeTask(requestCtx context.Context, rule models.RecordingRule, taskChan chan struct{}) {
	defer func() {
		// 释放任务信号量
		<-taskChan
	}()

	// 在规则评估前检查是否仍然启用
	if requestCtx.Err() != nil || !t.isRuleEnabled(requestCtx, rule.TenantId, rule.RuleId) {
		return
	}

	// 处理数据源
	t.processSingleDatasource(requestCtx, rule)
}

// processSingleDatasource 处理数据源
func (t *RecordingRule) processSingleDatasource(requestCtx context.Context, rule models.RecordingRule) {
	slots := sharedQuerySlots()
	if !acquireQuery(requestCtx, slots) {
		return
	}
	defer func() { <-slots }()
	instance, err := t.ctx.DB.Datasource().GetForTenantContext(requestCtx, rule.TenantId, rule.DatasourceId)
	if err != nil {
		logc.Errorf(t.ctx.Ctx, "Failed to get datasource instance %s: %v", rule.DatasourceId, err)
		return
	}

	if instance.Type != provider.PrometheusDsProvider || !instance.GetEnabled() {
		logc.Errorf(t.ctx.Ctx, "Recording datasource is disabled or has invalid type, RuleId: %s", rule.RuleId)
		return
	}
	t.processPrometheus(requestCtx, rule, instance)
}

// processPrometheus 处理 Prometheus 数据源
func (t *RecordingRule) processPrometheus(requestCtx context.Context, rule models.RecordingRule, instance models.AlertDataSource) {
	// 创建 Prometheus 客户端
	cli, err := provider.NewPrometheusClient(instance)
	if err != nil {
		logc.Errorf(t.ctx.Ctx, "Failed to create Prometheus client: %v", err)
		return
	}

	// 执行 PromQL 查询
	results, err := cli.(provider.PrometheusProvider).QueryContext(requestCtx, rule.PromQL)
	if err != nil {
		logc.Errorf(t.ctx.Ctx, "Failed to execute PromQL query: %v", err)
		return
	}

	newResults := make([]provider.Metrics, len(results))
	for i, result := range results {
		newResults[i] = provider.Metrics{
			Name:   rule.MetricName,
			Help:   result.Help,
			Labels: result.Labels,
			Value:  result.Value,
		}
		delete(newResults[i].Labels, "__name__")
	}

	// 将结果写入 Prometheus 远程写入端点
	if requestCtx.Err() != nil {
		return
	}
	err = cli.Write(requestCtx, newResults, rule.Labels)
	if err != nil {
		logc.Errorf(t.ctx.Ctx, "Failed to write recording rule result: %v", err)
		return
	}
}

// Restart 重启记录规则评估
func (t *RecordingRule) Restart(rule models.RecordingRule) {
	t.Stop(rule.RuleId)
	t.Submit(rule)
}

// RestartAllEvals 重启所有记录规则评估器
func (t *RecordingRule) RestartAllEvals() {
	ruleList, err := t.getRuleList()
	if err != nil {
		logc.Error(t.ctx.Ctx, fmt.Sprintf("Failed to get recording rule list: %v", err))
		return
	}

	count := len(ruleList)
	if count == 0 {
		return
	}

	logc.Info(t.ctx.Ctx, fmt.Sprintf("获取到 %d 个状态为启用的记录规则", count))

	// Submit only installs a timer. Query concurrency is controlled at runtime.
	for _, rule := range ruleList {
		t.Submit(rule)
	}
	logc.Info(t.ctx.Ctx, "所有记录规则评估器启动成功！")
}

// isRuleEnabled 检查记录规则是否启用
func (t *RecordingRule) isRuleEnabled(requestCtx context.Context, tenantID, ruleID string) bool {
	enabled, err := t.ctx.DB.RecordingRule().IsEnabled(requestCtx, tenantID, ruleID)
	return err == nil && enabled
}

// getRuleList 获取记录规则列表
func (t *RecordingRule) getRuleList() ([]models.RecordingRule, error) {
	var ruleList []models.RecordingRule
	if err := t.ctx.DB.DB().Where("enabled = ?", "1").Find(&ruleList).Error; err != nil {
		return nil, fmt.Errorf("获取 Recording Rule List 失败: %w", err)
	}
	return ruleList, nil
}

// StopAllEvals 停止所有记录规则评估器
func (t *RecordingRule) StopAllEvals() {
	t.ctx.Mux.Lock()
	defer t.ctx.Mux.Unlock()

	count := len(t.ctx.ContextMap)
	if count == 0 {
		return
	}

	logc.Infof(t.ctx.Ctx, "停止 %d 个记录规则评估器...", count)

	// 取消所有评估任务
	for ruleId, cancel := range t.ctx.ContextMap {
		cancel()
		delete(t.ctx.ContextMap, ruleId)
	}

	logc.Infof(t.ctx.Ctx, "所有记录规则评估器已停止")
}

// getEvalTimeDuration 获取评估时间间隔
func (t *RecordingRule) getEvalTimeDuration(evalInterval int64) time.Duration {
	return time.Duration(evalInterval) * time.Second
}
