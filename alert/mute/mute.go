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
	return mp.IsRecovered && !*mp.RecoverNotify
}

// IsSilence 判断是否静默
func IsSilence(mute MuteParams) bool {
	silenceCtx := ctx.Redis.Silence()
	// 获取静默列表中所有的id
	ids, err := silenceCtx.GetAlertMutes(mute.TenantId, mute.FaultCenterId)
	if err != nil {
		logc.Errorf(ctx.Ctx, "%s", err.Error())
		return false
	}

	// 根据ID获取到详细的静默规则
	for _, id := range ids {
		muteRule, err := silenceCtx.WithIdGetMuteFromCache(mute.TenantId, mute.FaultCenterId, id)
		if err != nil {
			logc.Errorf(ctx.Ctx, "%s", err.Error())
			return false
		}

		if muteRule == nil || muteRule.Status != 1 || time.Now().Unix() < muteRule.StartsAt || time.Now().Unix() >= muteRule.EndsAt {
			continue
		}

		if evalCondition(mute.Labels, muteRule.Labels) {
			return true
		}
	}

	return false
}

func evalCondition(metrics map[string]interface{}, muteLabels []models.SilenceLabel) bool {
	match, err := models.CompileSilenceMatchers(muteLabels)
	return err == nil && match(metrics)
}
