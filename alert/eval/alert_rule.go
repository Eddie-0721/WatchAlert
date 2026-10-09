package eval

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"
	"time"
	"watchAlert/config"
	"watchAlert/internal/ctx"
	"watchAlert/internal/models"
	"watchAlert/pkg/provider"
	"watchAlert/pkg/tools"

	"github.com/zeromicro/go-zero/core/logc"
)

const (
	// 数据源类型
	DatasourceTypePrometheus      = "Prometheus"
	DatasourceTypeAliCloudSLS     = "AliCloudSLS"
	DatasourceTypeLoki            = "Loki"
	DatasourceTypeElasticSearch   = "ElasticSearch"
	DatasourceTypeVictoriaLogs    = "VictoriaLogs"
	DatasourceTypeClickHouse      = "ClickHouse"
	DatasourceTypeJaeger          = "Jaeger"
	DatasourceTypeCloudWatch      = "CloudWatch"
	DatasourceTypeKubernetesEvent = "KubernetesEvent"

	// 默认恢复等待时间
	DefaultRecoverWaitTime = 1

	// 任务通道缓冲区大小
	TaskChannelBufferSize = 1
)

// 数据源处理器映射
var datasourceHandlers = map[string]func(context.Context, *ctx.Context, string, string, models.AlertRule) evaluationResult{
	DatasourceTypePrometheus:      metrics,
	DatasourceTypeAliCloudSLS:     logs,
	DatasourceTypeLoki:            logs,
	DatasourceTypeElasticSearch:   logs,
	DatasourceTypeVictoriaLogs:    logs,
	DatasourceTypeClickHouse:      logs,
	DatasourceTypeJaeger:          traces,
	DatasourceTypeCloudWatch:      cloudWatch,
	DatasourceTypeKubernetesEvent: kubernetesEvent,
}

type (
	// AlertRuleEval 告警规则评估
	AlertRuleEval interface {
		Submit(rule models.AlertRule)
		Stop(ruleId string)
		Eval(ctx context.Context, rule models.AlertRule)
		Recover(tenantId, ruleId string, eventCacheKey models.AlertEventCacheKey, faultCenterInfoKey models.FaultCenterInfoCacheKey, curFingerprints []string)
		RestartAllEvals()
		StopAllEvals()
	}

	// AlertRule 告警规则
	AlertRule struct {
		ctx          *ctx.Context
		lastComplete sync.Map
		querySlots   chan struct{}
	}
)

func NewAlertRuleEval(ctx *ctx.Context) AlertRuleEval {
	return &AlertRule{
		ctx: ctx,
	}
}

func (t *AlertRule) Submit(rule models.AlertRule) {
	t.ctx.Mux.Lock()
	defer t.ctx.Mux.Unlock()

	if previous, exists := t.ctx.ContextMap[rule.RuleId]; exists {
		previous()
	}
	t.lastComplete.Delete(rule.RuleId)
	c, cancel := context.WithCancel(t.ctx.Ctx)
	t.ctx.ContextMap[rule.RuleId] = cancel
	go t.Eval(c, rule)
}

func (t *AlertRule) Stop(ruleId string) {
	t.lastComplete.Delete(ruleId)
	t.ctx.Mux.Lock()
	defer t.ctx.Mux.Unlock()

	if cancel, exists := t.ctx.ContextMap[ruleId]; exists {
		cancel()
		delete(t.ctx.ContextMap, ruleId)
	}
}

func (t *AlertRule) Restart(rule models.AlertRule) {
	t.Stop(rule.RuleId)
	t.Submit(rule)
}

func (t *AlertRule) Eval(ctx context.Context, rule models.AlertRule) {
	err := rule.Validate()
	if err != nil {
		logc.Errorf(t.ctx.Ctx, "Rule validation failed, RuleName: %s, RuleId: %s, Error: %v", rule.RuleName, rule.RuleId, err)
		return
	}

	taskChan := make(chan struct{}, TaskChannelBufferSize)
	timer := time.NewTicker(t.getEvalTimeDuration(rule.EvalInterval))
	defer func() {
		timer.Stop()
		if r := recover(); r != nil {
			// 获取调用栈信息
			stack := debug.Stack()
			logc.Errorf(t.ctx.Ctx, "Recovered from rule eval goroutine panic: %s, RuleName: %s, RuleId: %s\n%s", r, rule.RuleName, rule.RuleId, stack)
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
			logc.Infof(t.ctx.Ctx, fmt.Sprintf("Handle eval task, RuleId: %v, RuleName: %s", rule.RuleId, rule.RuleName))
			t.executeTask(ctx, rule, taskChan)
		case <-ctx.Done():
			logc.Infof(t.ctx.Ctx, fmt.Sprintf("Stop eval task, RuleId: %v, RuleName: %s", rule.RuleId, rule.RuleName))
			return
		}
		timer.Reset(t.getEvalTimeDuration(rule.EvalInterval))
	}
}

// executeTask 执行评估任务
func (t *AlertRule) executeTask(requestCtx context.Context, rule models.AlertRule, taskChan chan struct{}) {
	defer func() {
		// 释放任务信号量
		<-taskChan
	}()

	// 在规则评估前检查是否仍然启用
	if requestCtx.Err() != nil || !t.isRuleEnabled(requestCtx, rule.TenantId, rule.RuleId) {
		t.lastComplete.Delete(rule.RuleId)
		return
	}

	// 并发处理数据源
	result := t.processDatasources(requestCtx, rule)
	if requestCtx.Err() != nil || result.Status != "complete" {
		t.lastComplete.Delete(rule.RuleId)
		logc.Errorf(t.ctx.Ctx, "Rule evaluation incomplete; recovery skipped. RuleId: %s, Reason: %s", rule.RuleId, result.Reason)
		return
	}

	// 处理恢复逻辑
	_, consecutive := t.lastComplete.LoadOrStore(rule.RuleId, true)
	t.recoverCompleteContext(requestCtx, rule.TenantId, rule.RuleId,
		models.BuildAlertEventCacheKey(rule.TenantId, rule.FaultCenterId),
		models.BuildFaultCenterInfoCacheKey(rule.TenantId, rule.FaultCenterId),
		result.Fingerprints, !consecutive)
}

// processDatasources 处理数据源
func (t *AlertRule) processDatasources(requestCtx context.Context, rule models.AlertRule) evaluationResult {
	results := make([]evaluationResult, len(rule.DatasourceIdList))
	jobs := make(chan int)
	var wg sync.WaitGroup
	workers := min(len(rule.DatasourceIdList), boundedSetting(config.Application.Evaluation.MaxDatasourcesPerRule, 4, 32))
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for index := range jobs {
				results[index] = t.processSingleDatasource(requestCtx, rule.DatasourceIdList[index], rule)
			}
		}()
	}
	for index := range rule.DatasourceIdList {
		select {
		case jobs <- index:
		case <-requestCtx.Done():
			close(jobs)
			wg.Wait()
			return failedEvaluation("cancelled")
		}
	}
	close(jobs)
	wg.Wait()
	return combineEvaluations(results)
}

// processSingleDatasource 处理单个数据源
func (t *AlertRule) processSingleDatasource(requestCtx context.Context, dsId string, rule models.AlertRule) (result evaluationResult) {
	slots := t.querySlots
	if slots == nil {
		slots = sharedQuerySlots()
	}
	if !acquireQuery(requestCtx, slots) {
		return failedEvaluation("cancelled")
	}
	defer func() { <-slots }()
	defer func() {
		if recover() != nil {
			// A worker panic must neither terminate the process nor look like recovery.
			result = failedEvaluation("datasource_worker_panic")
			logc.Errorf(t.ctx.Ctx, "Datasource evaluation panic, RuleId: %s, DatasourceId: %s", rule.RuleId, dsId)
		}
	}()
	instance, err := t.ctx.DB.Datasource().GetForTenantContext(requestCtx, rule.TenantId, dsId)
	if err != nil {
		logc.Errorf(t.ctx.Ctx, "Failed to get datasource instance %s: %v", dsId, err)
		return failedEvaluation("datasource_unavailable")
	}

	if !instance.GetEnabled() {
		return failedEvaluation("datasource_disabled")
	}
	if instance.Type != rule.DatasourceType && !(instance.Type == "Kubernetes" && rule.DatasourceType == DatasourceTypeKubernetesEvent) {
		return failedEvaluation("datasource_type_mismatch")
	}

	// These providers validate HTTP/decoding failures in the actual query.
	// Other providers retain preflight until their error semantics are audited.
	switch instance.Type {
	case DatasourceTypePrometheus, provider.LokiDsProviderName, provider.VictoriaLogsDsProviderName, provider.JaegerDsProviderName:
	default:
		if ok, _ := provider.CheckDatasourceHealth(instance); !ok {
			logc.Errorf(t.ctx.Ctx, "Datasource %s is unhealthy", dsId)
			return failedEvaluation("datasource_unhealthy")
		}
	}

	// 调用处理器
	handler, exists := datasourceHandlers[rule.DatasourceType]
	if !exists {
		logc.Errorf(t.ctx.Ctx, "Unsupported datasource type: %s", rule.DatasourceType)
		return failedEvaluation("unsupported_datasource")
	}

	if requestCtx.Err() != nil {
		return failedEvaluation("cancelled")
	}
	return handler(requestCtx, t.ctx, dsId, instance.Type, rule)
}

// getEvalTimeDuration 获取评估时间间隔
func (t *AlertRule) getEvalTimeDuration(evalInterval int64) time.Duration {
	return time.Duration(evalInterval) * time.Second
}

func (t *AlertRule) Recover(tenantId, ruleId string, eventCacheKey models.AlertEventCacheKey, faultCenterInfoKey models.FaultCenterInfoCacheKey, curFingerprints []string) {
	t.recoverComplete(tenantId, ruleId, eventCacheKey, faultCenterInfoKey, curFingerprints, false)
}

func (t *AlertRule) recoverComplete(tenantId, ruleId string, eventCacheKey models.AlertEventCacheKey, faultCenterInfoKey models.FaultCenterInfoCacheKey, curFingerprints []string, restartWindow bool) {
	t.recoverCompleteContext(context.Background(), tenantId, ruleId, eventCacheKey, faultCenterInfoKey, curFingerprints, restartWindow)
}

func (t *AlertRule) recoverCompleteContext(requestCtx context.Context, tenantId, ruleId string, eventCacheKey models.AlertEventCacheKey, faultCenterInfoKey models.FaultCenterInfoCacheKey, curFingerprints []string, restartWindow bool) {
	if requestCtx.Err() != nil {
		t.lastComplete.Delete(ruleId)
		return
	}
	// 过滤空指纹
	var filteredCurFingerprints []string
	for _, fp := range curFingerprints {
		if fp != "" {
			filteredCurFingerprints = append(filteredCurFingerprints, fp)
		}
	}
	curFingerprints = filteredCurFingerprints
	current := make(map[string]struct{}, len(curFingerprints))
	for _, fingerprint := range curFingerprints {
		current[fingerprint] = struct{}{}
	}

	// 校验 key 非空
	if eventCacheKey == "" || faultCenterInfoKey == "" {
		logc.Errorf(t.ctx.Ctx, "AlertRule.Recover: eventCacheKey or faultCenterInfoKey is empty")
		return
	}

	// 获取所有的故障中心告警事件
	events, err := t.ctx.Redis.Alert().GetAllEvents(eventCacheKey)
	if err != nil {
		t.lastComplete.Delete(ruleId)
		logc.Errorf(t.ctx.Ctx, "AlertRule.Recover: Failed to get all events: %v", err)
		return
	}

	// Load the recovery timestamps once. A read/decode failure is not an empty
	// recovery window and must not cause state transitions.
	pendingFingerprints, err := t.ctx.Redis.PendingRecover().ListWithError(tenantId, ruleId)
	if err != nil {
		t.lastComplete.Delete(ruleId)
		logc.Errorf(t.ctx.Ctx, "Failed to read recovery timestamps, RuleId: %s", ruleId)
		return
	}
	// 存储当前规则下所有活动的指纹
	var activeRuleFingerprints []string

	// 筛选当前规则相关的指纹，并处理预告警状态
	for fingerprint, event := range events {
		if requestCtx.Err() != nil {
			t.lastComplete.Delete(ruleId)
			return
		}
		if fingerprint == "" {
			continue
		}

		if event == nil || event.RuleId != ruleId || event.TenantId != tenantId {
			continue
		}

		// 移除状态为预告警且当前告警列表中不存在的事件
		_, stillActive := current[fingerprint]
		if event.Status == models.StatePreAlert && !stillActive {
			t.ctx.Redis.Alert().RemoveAlertEvent(event.TenantId, event.FaultCenterId, event.Fingerprint)
			continue
		}

		activeRuleFingerprints = append(activeRuleFingerprints, fingerprint)
	}

	/*
		从待恢复状态转换成告警状态（即在 Redis 中存在待恢复 且在 curFingerprints 存在告警的事件）
	*/

	// 获取当前待恢复的告警指纹列表
	if len(pendingFingerprints) != 0 {
		for _, fingerprint := range curFingerprints {
			if requestCtx.Err() != nil {
				t.lastComplete.Delete(ruleId)
				return
			}
			if _, exists := pendingFingerprints[fingerprint]; !exists {
				continue
			}
			event, ok := events[fingerprint]
			if !ok || event == nil || event.RuleId != ruleId || event.TenantId != tenantId {
				continue
			}

			newEvent := event
			// 转换成告警状态
			err := newEvent.TransitionStatus(models.StateAlerting)
			if err != nil {
				logc.Errorf(t.ctx.Ctx, "Failed to transition to「alerting」state for fingerprint %s: %v", fingerprint, err)
				continue
			}
			if err := t.ctx.Redis.Alert().PushAlertEvent(newEvent); err != nil {
				t.lastComplete.Delete(ruleId)
				continue
			}
			t.ctx.Redis.PendingRecover().Delete(tenantId, ruleId, fingerprint)
		}
	}

	/*
		从待恢复状态转换成已恢复状态
	*/

	// 计算需要恢复的指纹列表 (即在 Redis 中存在但在当前活动列表中不存在的指纹)
	recoverFingerprints := tools.GetSliceDifference(activeRuleFingerprints, curFingerprints)
	curTime := time.Now().Unix()
	recoverWaitTime := t.getRecoverWaitTime(faultCenterInfoKey)
	for _, fingerprint := range recoverFingerprints {
		if requestCtx.Err() != nil {
			t.lastComplete.Delete(ruleId)
			return
		}
		event, ok := events[fingerprint]
		if !ok {
			continue
		}

		newEvent := event
		// 获取待恢复状态的时间戳
		wTime, exists := pendingFingerprints[fingerprint]
		if !exists {
			// 转换状态, 标记为待恢复
			if err := newEvent.TransitionStatus(models.StatePendingRecovery); err != nil {
				logc.Errorf(t.ctx.Ctx, "Failed to transition to「pending_recovery」state for fingerprint %s: %v", fingerprint, err)
				continue
			}
			// 记录当前时间
			if err := t.ctx.Redis.PendingRecover().Set(tenantId, ruleId, fingerprint, curTime); err != nil {
				t.lastComplete.Delete(ruleId)
				continue
			}
			if err := t.ctx.Redis.Alert().PushAlertEvent(newEvent); err != nil {
				t.lastComplete.Delete(ruleId)
			}
			continue
		}

		if restartWindow && newEvent.Status == models.StatePendingRecovery {
			// Unknown/failed time must not count towards a continuous recovery window.
			if err := t.ctx.Redis.PendingRecover().Set(tenantId, ruleId, fingerprint, curTime); err != nil {
				t.lastComplete.Delete(ruleId)
			}
			continue
		}

		// 判断是否在等待时间内
		recoverThreshold := wTime + recoverWaitTime
		// 当前时间超过预期等待时间，并且状态是 PendingRecovery 时才执行恢复逻辑
		if curTime >= recoverThreshold && newEvent.Status == models.StatePendingRecovery {
			// 已恢复状态
			if err := newEvent.TransitionStatus(models.StateRecovered); err != nil {
				logc.Errorf(t.ctx.Ctx, "Failed to transition to recovered state for fingerprint %s: %v", fingerprint, err)
				continue
			}
			// 更新告警事件
			if err := t.ctx.Redis.Alert().PushAlertEvent(newEvent); err != nil {
				t.lastComplete.Delete(ruleId)
				continue
			}
			// 恢复后继续处理下一个事件
			t.ctx.Redis.PendingRecover().Delete(tenantId, ruleId, fingerprint)
			continue
		}
	}
}

// getRecoverWaitTime 获取恢复等待时间
func (t *AlertRule) getRecoverWaitTime(faultCenterInfoKey models.FaultCenterInfoCacheKey) int64 {
	faultCenter := t.ctx.Redis.FaultCenter().GetFaultCenterInfo(faultCenterInfoKey)
	if faultCenter.RecoverWaitTime == 0 {
		return DefaultRecoverWaitTime
	}
	return faultCenter.RecoverWaitTime
}

// RestartAllEvals 重启所有评估器
func (t *AlertRule) RestartAllEvals() {
	ruleList, err := t.getRuleList()
	if err != nil {
		logc.Error(t.ctx.Ctx, fmt.Sprintf("Failed to get rule list: %v", err))
		return
	}

	count := len(ruleList)
	if count == 0 {
		return
	}

	logc.Info(t.ctx.Ctx, fmt.Sprintf("获取到 %d 个状态为启用的规则", count))

	// Submit only installs a timer. Query concurrency is controlled at runtime.
	for _, rule := range ruleList {
		t.Submit(rule)
	}
	logc.Info(t.ctx.Ctx, "所有规则评估器启动成功！")
}

// isRuleEnabled 检查规则是否启用
func (t *AlertRule) isRuleEnabled(requestCtx context.Context, tenantID, ruleID string) bool {
	enabled, err := t.ctx.DB.Rule().IsEnabled(requestCtx, tenantID, ruleID)
	return err == nil && enabled
}

// getRuleList 获取规则列表
func (t *AlertRule) getRuleList() ([]models.AlertRule, error) {
	var ruleList []models.AlertRule
	if err := t.ctx.DB.DB().Where("enabled = ?", "1").Find(&ruleList).Error; err != nil {
		return nil, fmt.Errorf("获取 Rule List 失败: %w", err)
	}
	return ruleList, nil
}

// StopAllEvals 停止所有评估器
func (t *AlertRule) StopAllEvals() {
	t.ctx.Mux.Lock()
	defer t.ctx.Mux.Unlock()

	count := len(t.ctx.ContextMap)
	if count == 0 {
		return
	}

	logc.Infof(t.ctx.Ctx, "停止 %d 个规则评估器...", count)

	// 取消所有评估任务
	for ruleId, cancel := range t.ctx.ContextMap {
		cancel()
		t.lastComplete.Delete(ruleId)
		delete(t.ctx.ContextMap, ruleId)
	}

	logc.Infof(t.ctx.Ctx, "所有规则评估器已停止")
}
