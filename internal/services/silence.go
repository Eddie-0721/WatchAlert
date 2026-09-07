package services

import (
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

	ass.ctx.Redis.Silence().PushAlertMute(silence)
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

	err = ass.ctx.DB.Silence().Update(silence)
	if err != nil {
		return nil, err
	}

	if before.FaultCenterId != silence.FaultCenterId {
		ass.ctx.Redis.Silence().RemoveAlertMute(before.TenantId, before.FaultCenterId, before.ID)
	}
	ass.ctx.Redis.Silence().PushAlertMute(silence)
	return silence, nil
}

func (ass alertSilenceService) Delete(req interface{}) (interface{}, interface{}) {
	r := req.(*types.RequestSilenceQuery)
	ass.ctx.Redis.Silence().RemoveAlertMute(r.TenantId, r.FaultCenterId, r.ID)
	err := ass.ctx.DB.Silence().Delete(r.TenantId, r.ID)
	if err != nil {
		return nil, err
	}

	return nil, nil
}

func (ass alertSilenceService) List(req interface{}) (interface{}, interface{}) {
	r := req.(*types.RequestSilenceQuery)
	data, count, err := ass.ctx.DB.Silence().List(r.TenantId, r.FaultCenterId, r.Query, r.Status, r.Page)
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
