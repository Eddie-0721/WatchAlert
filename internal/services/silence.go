package services

import (
	"context"
	"fmt"
	"time"
	"watchAlert/internal/ctx"
	models "watchAlert/internal/models"
	"watchAlert/internal/types"
	"watchAlert/pkg/tools"
)

type alertSilenceService struct {
	alertEvent models.AlertCurEvent
	ctx        *ctx.Context
}

type InterSilenceService interface {
	Preview(req interface{}) (interface{}, interface{})
	Create(req interface{}) (interface{}, interface{})
	Update(req interface{}) (interface{}, interface{})
	Delete(req interface{}) (interface{}, interface{})
	List(req interface{}) (interface{}, interface{})
	ListContext(context.Context, interface{}) (interface{}, interface{})
}

func newInterSilenceService(ctx *ctx.Context) InterSilenceService {
	return &alertSilenceService{
		ctx: ctx,
	}
}

func (ass alertSilenceService) Create(req interface{}) (interface{}, interface{}) {
	r := req.(*types.RequestSilenceCreate)
	p, err := ass.preview(&types.RequestSilencePreview{TenantId: r.TenantId, Name: r.Name, Comment: r.Comment, Labels: r.Labels, StartsAt: r.StartsAt, EndsAt: r.EndsAt, FaultCenterId: r.FaultCenterId})
	if err != nil {
		return nil, err
	}
	if err := validateSilencePreview(r.PreviewHash, r.PreviewAt, p, time.Now().Unix()); err != nil {
		return nil, err
	}
	updateAt := time.Now().Unix()
	silence := models.AlertSilences{
		TenantId:      r.TenantId,
		Name:          r.Name,
		ID:            "s-" + tools.RandId(),
		StartsAt:      r.StartsAt,
		EndsAt:        r.EndsAt,
		UpdateAt:      updateAt,
		UpdateBy:      r.UpdateBy,
		FaultCenterId: r.FaultCenterId,
		Labels:        r.Labels,
		Comment:       r.Comment,
		Status:        1,
	}

	if r.StartsAt > updateAt {
		silence.Status = 0
	}

	err = ass.ctx.DB.Silence().Create(silence)
	if err != nil {
		return nil, err
	}

	if err := ass.ctx.Redis.Silence().PushAlertMute(silence); err != nil {
		return silence, fmt.Errorf("静默已保存（%s），但缓存同步失败，尚未确认生效；请勿重复创建，请核对后重试更新", silence.ID)
	}
	return silence, nil
}

func (ass alertSilenceService) Update(req interface{}) (interface{}, interface{}) {
	r := req.(*types.RequestSilenceUpdate)
	p, err := ass.preview(&types.RequestSilencePreview{TenantId: r.TenantId, ID: r.ID, Name: r.Name, Comment: r.Comment, Labels: r.Labels, StartsAt: r.StartsAt, EndsAt: r.EndsAt, FaultCenterId: r.FaultCenterId})
	if err != nil {
		return nil, err
	}
	if err := validateSilencePreview(r.PreviewHash, r.PreviewAt, p, time.Now().Unix()); err != nil {
		return nil, err
	}
	var before models.AlertSilences
	if err := ass.ctx.DB.DB().Where("tenant_id = ? AND id = ?", r.TenantId, r.ID).First(&before).Error; err != nil {
		return nil, err
	}
	silence := models.AlertSilences{
		TenantId:      r.TenantId,
		Name:          r.Name,
		ID:            r.ID,
		StartsAt:      r.StartsAt,
		EndsAt:        r.EndsAt,
		UpdateAt:      time.Now().Unix(),
		UpdateBy:      r.UpdateBy,
		FaultCenterId: r.FaultCenterId,
		Labels:        r.Labels,
		Comment:       r.Comment,
		Status:        1,
	}

	if r.StartsAt > silence.UpdateAt {
		silence.Status = 0
	} else {
		silence.Status = 1
	}

	if before.FaultCenterId != silence.FaultCenterId {
		if err := ass.ctx.Redis.Silence().RemoveAlertMute(before.TenantId, before.FaultCenterId, before.ID); err != nil {
			return nil, fmt.Errorf("旧故障中心缓存清理失败，静默配置未修改，请核对后重试")
		}
	}
	err = ass.ctx.DB.Silence().Update(silence)
	if err != nil {
		if before.FaultCenterId != silence.FaultCenterId {
			if restoreErr := ass.ctx.Redis.Silence().PushAlertMute(before); restoreErr != nil {
				return nil, fmt.Errorf("静默更新失败且旧缓存回补失败，请人工核对")
			}
		}
		return nil, err
	}
	if err := ass.ctx.Redis.Silence().PushAlertMute(silence); err != nil {
		return silence, fmt.Errorf("静默已保存（%s），但缓存同步失败，尚未确认生效；请核对后重试更新", silence.ID)
	}
	return silence, nil
}

func (ass alertSilenceService) Delete(req interface{}) (interface{}, interface{}) {
	r := req.(*types.RequestSilenceQuery)
	if r.TenantId == "" || r.ID == "" {
		return nil, fmt.Errorf("租户和静默ID不能为空")
	}
	var stored models.AlertSilences
	if err := ass.ctx.DB.DB().Where("tenant_id = ? AND id = ?", r.TenantId, r.ID).First(&stored).Error; err != nil {
		return nil, err
	}
	if err := ass.ctx.Redis.Silence().RemoveAlertMute(stored.TenantId, stored.FaultCenterId, stored.ID); err != nil {
		return nil, fmt.Errorf("静默缓存删除失败，数据库记录已保留；请核对静默实际状态后重试")
	}
	err := ass.ctx.DB.Silence().Delete(r.TenantId, r.ID)
	if err != nil {
		if restoreErr := ass.ctx.Redis.Silence().PushAlertMute(stored); restoreErr != nil {
			return nil, fmt.Errorf("静默删除未完成，数据库记录仍保留且缓存回补失败，请人工核对")
		}
		return nil, fmt.Errorf("静默数据库删除失败，已尝试回补缓存，请核对后重试")
	}

	return nil, nil
}

func (ass alertSilenceService) List(req interface{}) (interface{}, interface{}) {
	return ass.ListContext(context.Background(), req)
}

func (ass alertSilenceService) ListContext(requestCtx context.Context, req interface{}) (interface{}, interface{}) {
	requestCtx, cancel := context.WithTimeout(requestCtx, 10*time.Second)
	defer cancel()
	r := req.(*types.RequestSilenceQuery)
	data, count, err := ass.ctx.DB.Silence().ListContext(requestCtx, r.TenantId, r.FaultCenterId, r.Query, r.Status, r.Page)
	if err != nil {
		return nil, err
	}

	return types.ResponseSilenceList{
		List: data,
		Page: models.Page{
			Total: count,
			Index: r.Page.Index,
			Size:  r.Page.Size,
		},
	}, nil
}
