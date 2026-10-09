package mute

import (
	"fmt"
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
		return true
	}

	match, err := CompileSnapshotChecked(rules, time.Now().Unix())
	if err != nil {
		logc.Errorf(ctx.Ctx, "Cannot evaluate silence rules: %v", err)
		return true
	}
	return match(mute.Labels)
}

// CompileSnapshot reuses compiled matchers only within a single read operation.
// Each new request observes updated rules and evaluates their time boundaries again.
func CompileSnapshot(rules []models.AlertSilences, now int64) func(map[string]interface{}) bool {
	match, err := CompileSnapshotChecked(rules, now)
	if err != nil {
		return func(map[string]interface{}) bool { return true }
	}
	return match
}

// Production callers propagate errors instead of reporting an unknown silence
// state as either an active silence or an unmuted event.
func CompileSnapshotChecked(rules []models.AlertSilences, now int64) (func(map[string]interface{}) bool, error) {
	var matches []func(map[string]interface{}) bool
	for _, rule := range rules {
		if rule.Status != 1 || now < rule.StartsAt || now >= rule.EndsAt {
			continue
		}
		match, err := models.CompileSilenceMatchers(rule.Labels)
		if err != nil {
			return nil, fmt.Errorf("invalid active silence %s: %w", rule.ID, err)
		}
		matches = append(matches, match)
	}
	return func(labels map[string]interface{}) bool {
		for _, match := range matches {
			if match(labels) {
				return true
			}
		}
		return false
	}, nil
}

type SilenceReader interface {
	ListAlertMutes(string, string) ([]models.AlertSilences, error)
}

// Read once per preparation, never cache across slow sends or consumer ticks.
func LoadSnapshot(reader SilenceReader, tenantID, centerID string, now int64) (func(map[string]interface{}) bool, error) {
	rules, err := reader.ListAlertMutes(tenantID, centerID)
	if err != nil {
		return nil, fmt.Errorf("read silence state: %w", err)
	}
	return CompileSnapshotChecked(rules, now)
}

func evalCondition(metrics map[string]interface{}, muteLabels []models.SilenceLabel) bool {
	match, err := models.CompileSilenceMatchers(muteLabels)
	return err == nil && match(metrics)
}
