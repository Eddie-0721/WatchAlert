package mute

import (
	"time"
	"watchAlert/internal/ctx"
	models "watchAlert/internal/models"

	"github.com/zeromicro/go-zero/core/logc"
)

type MuteParams struct {
	RecoverNotify *bool
	IsRecovered   bool
	TenantId      string
	Labels        map[string]interface{}
	FaultCenterId string
}

func IsMuted(mute MuteParams) bool {
	if IsSilence(mute) {
		return true
	}

	if RecoverNotify(mute) {
		return true
	}

	return false
}

// RecoverNotify 判断是否推送恢复通知
func RecoverNotify(mp MuteParams) bool {
	return mp.IsRecovered && (mp.RecoverNotify == nil || !*mp.RecoverNotify)
}

// IsSilence 判断是否静默
func IsSilence(mute MuteParams) bool {
	silenceCtx := ctx.Redis.Silence()
	// 获取静默列表中所有的id
	rules, err := silenceCtx.ListAlertMutes(mute.TenantId, mute.FaultCenterId)
	if err != nil {
		logc.Errorf(ctx.Ctx, "%s", err.Error())
		return false
	}

	// Snapshot lives for this decision only, not across sends or consumer ticks.
	for _, muteRule := range rules {
		if muteRule.Status != 1 || time.Now().Unix() < muteRule.StartsAt || time.Now().Unix() >= muteRule.EndsAt {
			continue
		}

		if evalCondition(mute.Labels, muteRule.Labels) {
			return true
		}
	}

	return false
}

// CompileSnapshot reuses compiled matchers only within a single read operation.
// Each new request observes updated rules and evaluates their time boundaries again.
func CompileSnapshot(rules []models.AlertSilences, now int64) func(map[string]interface{}) bool {
	var matches []func(map[string]interface{}) bool
	for _, rule := range rules {
		if rule.Status != 1 || now < rule.StartsAt || now >= rule.EndsAt {
			continue
		}
		match, err := models.CompileSilenceMatchers(rule.Labels)
		if err == nil {
			matches = append(matches, match)
		}
	}
	return func(labels map[string]interface{}) bool {
		for _, match := range matches {
			if match(labels) {
				return true
			}
		}
		return false
	}
}

func evalCondition(metrics map[string]interface{}, muteLabels []models.SilenceLabel) bool {
	match, err := models.CompileSilenceMatchers(muteLabels)
	return err == nil && match(metrics)
}
