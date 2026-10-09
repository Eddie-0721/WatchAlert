package services

import (
	"context"
	"time"
	"watchAlert/alert"
	"watchAlert/internal/ctx"
	"watchAlert/internal/models"
	"watchAlert/internal/types"
	"watchAlert/pkg/client"
	"watchAlert/pkg/tools"
)

type (
	faultCenterService struct {
		ctx *ctx.Context
	}

	InterFaultCenterService interface {
		Create(req interface{}) (data interface{}, err interface{})
		Update(req interface{}) (data interface{}, err interface{})
		Delete(req interface{}) (data interface{}, err interface{})
		List(req interface{}) (data interface{}, err interface{})
		ListContext(context.Context, interface{}) (interface{}, interface{})
		ListOptions(context.Context, interface{}) (interface{}, interface{})
		Get(req interface{}) (data interface{}, err interface{})
		Reset(req interface{}) (data interface{}, err interface{})
		Slo(context.Context, interface{}) (data interface{}, err interface{})
	}
)

func newInterFaultCenterService(ctx *ctx.Context) InterFaultCenterService {
	return &faultCenterService{
		ctx: ctx,
	}
}

func (f faultCenterService) Create(req interface{}) (data interface{}, err interface{}) {
	r := req.(*types.RequestFaultCenterCreate)
	fc := models.FaultCenter{
		TenantId:             r.TenantId,
		ID:                   "fc-" + tools.RandId(),
		Name:                 r.Name,
		Description:          r.Description,
		NoticeIds:            r.NoticeIds,
		NoticeRoutes:         r.NoticeRoutes,
		RepeatNoticeInterval: r.RepeatNoticeInterval,
		RecoverNotify:        r.RecoverNotify,
		AggregationType:      r.AggregationType,
		CreateAt:             time.Now().Unix(),
		RecoverWaitTime:      r.RecoverWaitTime,
		IsUpgradeEnabled:     r.IsUpgradeEnabled,
		UpgradableSeverity:   r.UpgradableSeverity,
		UpgradeStrategy:      r.UpgradeStrategy,
	}

	err = f.ctx.DB.FaultCenter().Create(fc)
	if err != nil {
		return nil, err
	}

	f.ctx.Redis.FaultCenter().PushFaultCenterInfo(fc)

	// 判断当前节点角色
	if alert.IsLeader() {
		// Leader: 直接启动消费协程
		alert.ConsumerWork.Submit(fc)
	} else {
		// Follower: 发布消息通知 Leader
		tools.PublishReloadMessage(f.ctx.Ctx, client.Redis, tools.ChannelFaultCenterReload, tools.ReloadMessage{
			Action:   tools.ActionCreate,
			ID:       fc.ID,
			TenantID: fc.TenantId,
			Name:     fc.Name,
		})
	}

	return nil, nil
}

func (f faultCenterService) Update(req interface{}) (data interface{}, err interface{}) {
	r := req.(*types.RequestFaultCenterUpdate)
	fc := models.FaultCenter{
		TenantId:             r.TenantId,
		ID:                   r.ID,
		Name:                 r.Name,
		Description:          r.Description,
		NoticeIds:            r.NoticeIds,
		NoticeRoutes:         r.NoticeRoutes,
		RepeatNoticeInterval: r.RepeatNoticeInterval,
		RecoverNotify:        r.RecoverNotify,
		AggregationType:      r.AggregationType,
		CreateAt:             r.CreateAt,
		RecoverWaitTime:      r.RecoverWaitTime,
		IsUpgradeEnabled:     r.IsUpgradeEnabled,
		UpgradableSeverity:   r.UpgradableSeverity,
		UpgradeStrategy:      r.UpgradeStrategy,
	}

	err = f.ctx.DB.FaultCenter().Update(fc)
	if err != nil {
		return nil, err
	}

	f.ctx.Redis.FaultCenter().PushFaultCenterInfo(fc)

	// 判断当前节点角色
	if alert.IsLeader() {
		// Leader: 直接重启消费协程
		alert.ConsumerWork.Stop(r.ID)
		alert.ConsumerWork.Submit(fc)
	} else {
		// Follower: 发布消息通知 Leader
		tools.PublishReloadMessage(f.ctx.Ctx, client.Redis, tools.ChannelFaultCenterReload, tools.ReloadMessage{
			Action:   tools.ActionUpdate,
			ID:       fc.ID,
			TenantID: fc.TenantId,
			Name:     fc.Name,
		})
	}

	return nil, nil
}

func (f faultCenterService) Delete(req interface{}) (data interface{}, err interface{}) {
	r := req.(*types.RequestFaultCenterQuery)
	err = f.ctx.DB.FaultCenter().Delete(r.TenantId, r.ID)
	if err != nil {
		return nil, err
	}

	f.ctx.Redis.FaultCenter().RemoveFaultCenterInfo(models.BuildFaultCenterInfoCacheKey(r.TenantId, r.ID))

	// 判断当前节点角色
	if alert.IsLeader() {
		// Leader: 直接停止消费协程
		alert.ConsumerWork.Stop(r.ID)
	} else {
		// Follower: 发布消息通知 Leader
		tools.PublishReloadMessage(f.ctx.Ctx, client.Redis, tools.ChannelFaultCenterReload, tools.ReloadMessage{
			Action:   tools.ActionDelete,
			ID:       r.ID,
			TenantID: r.TenantId,
			Name:     r.ID,
		})
	}

	return nil, nil
}

// Selectors only need center identity, never event counts. Keep this explicit so
// management screens retain their existing statistics and failure semantics.
func (f faultCenterService) ListOptions(requestCtx context.Context, req interface{}) (interface{}, interface{}) {
	r := req.(*types.RequestFaultCenterQuery)
	queryCtx, cancel := context.WithTimeout(requestCtx, 30*time.Second)
	defer cancel()
	data, err := f.ctx.DB.FaultCenter().ListOptions(queryCtx, r.TenantId, r.Query)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (f faultCenterService) List(req interface{}) (data interface{}, err interface{}) {
	return f.ListContext(context.Background(), req)
}

func (f faultCenterService) ListContext(requestCtx context.Context, req interface{}) (interface{}, interface{}) {
	queryCtx, cancel := context.WithTimeout(requestCtx, 30*time.Second)
	defer cancel()
	if err := queryCtx.Err(); err != nil {
		return nil, err
	}
	r := req.(*types.RequestFaultCenterQuery)
	faultCenters, err := f.ctx.DB.FaultCenter().ListContext(queryCtx, r.TenantId, r.Query)
	if err != nil {
		return nil, err
	}
	for index, fc := range faultCenters {
		counts, err := f.ctx.Redis.Alert().CountEventStates(queryCtx, models.BuildAlertEventCacheKey(fc.TenantId, fc.ID))
		if err != nil {
			return nil, err
		}
		faultCenters[index].CurrentPreAlertNumber += counts.PreAlert
		faultCenters[index].CurrentAlertNumber += counts.Alerting
		faultCenters[index].CurrentRecoverNumber += counts.PendingRecovery
	}
	if err := queryCtx.Err(); err != nil {
		return nil, err
	}

	return faultCenters, nil
}

func (f faultCenterService) Get(req interface{}) (data interface{}, err interface{}) {
	r := req.(*types.RequestFaultCenterQuery)
	data, err = f.ctx.DB.FaultCenter().Get(r.TenantId, r.ID, r.Name)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (f faultCenterService) Reset(req interface{}) (data interface{}, err interface{}) {
	r := req.(*types.RequestFaultCenterReset)
	err = f.ctx.DB.FaultCenter().Reset(r.TenantId, r.ID, r.Name, r.Description, r.AggregationType)
	if err != nil {
		return nil, err
	}

	data, err = f.ctx.DB.FaultCenter().Get(r.TenantId, r.ID, r.Name)
	if err != nil {
		return nil, err
	}
	f.ctx.Redis.FaultCenter().PushFaultCenterInfo(data.(models.FaultCenter))

	// 判断当前节点角色
	if alert.IsLeader() {
		// Leader: 直接重启消费协程
		alert.ConsumerWork.Stop(r.ID)
		alert.ConsumerWork.Submit(data.(models.FaultCenter))
	} else {
		// Follower: 发布消息通知 Leader
		tools.PublishReloadMessage(f.ctx.Ctx, client.Redis, tools.ChannelFaultCenterReload, tools.ReloadMessage{
			Action:   tools.ActionUpdate,
			ID:       r.ID,
			TenantID: r.TenantId,
			Name:     r.Name,
		})
	}

	return nil, nil
}

func (f faultCenterService) Slo(requestCtx context.Context, req interface{}) (data interface{}, err interface{}) {
	r := req.(*types.RequestFaultCenterQuery)
	now := time.Now()
	start := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location()).AddDate(0, 0, -6)
	boundaries := make([]int64, 8)
	for i := range boundaries {
		boundaries[i] = start.AddDate(0, 0, i).Unix()
	}
	queryCtx, cancel := context.WithTimeout(requestCtx, 30*time.Second)
	defer cancel()
	days, queryErr := f.ctx.DB.Event().DailySLO(queryCtx, r.TenantId, r.ID, boundaries)
	if queryErr != nil {
		return nil, queryErr
	}
	result := types.RequestFaultCenterSLO{MTTR: make([]float64, 7), MTTA: make([]float64, 7)}
	for i, day := range days {
		result.MTTR[i], result.MTTA[i] = day.MTTR, day.MTTA
	}
	return result, nil
}
