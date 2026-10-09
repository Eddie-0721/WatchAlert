package api

import (
	"watchAlert/internal/ctx"
	"watchAlert/internal/middleware"
	"watchAlert/internal/models"
	"watchAlert/internal/types"
	"watchAlert/pkg/response"

	"github.com/gin-gonic/gin"
	"github.com/zeromicro/go-zero/core/logc"
)

type dashboardInfoController struct{}

var DashboardInfoController = new(dashboardInfoController)

func (dashboardInfoController dashboardInfoController) API(gin *gin.RouterGroup) {
	system := gin.Group("system")
	system.Use(
		middleware.Auth(),
		middleware.ParseTenant(),
	)
	{
		system.GET("getDashboardInfo", dashboardInfoController.GetDashboardInfo)
	}
}

func (dashboardInfoController dashboardInfoController) GetDashboardInfo(context *gin.Context) {
	var c = ctx.DO()

	tid, _ := context.Get("TenantID")
	tidString := tid.(string)

	faultCenter, err := c.DB.FaultCenter().Get(tidString, context.Query("faultCenterId"), "")
	if err != nil {
		logc.Error(c.Ctx, err.Error())
		response.Fail(context, "读取故障中心失败", "failed")
		return
	}

	data, err := loadDashboardInfo(c, tidString, faultCenter)
	if err != nil {
		logc.Error(c.Ctx, err.Error())
		response.Fail(context, "读取告警态势失败", "failed")
		return
	}
	response.Success(context, data, "success")
}

func loadDashboardInfo(c *ctx.Context, tenantId string, faultCenter models.FaultCenter) (types.ResponseDashboardInfo, error) {
	var data types.ResponseDashboardInfo
	db := c.DB.DB()
	if err := db.Model(&models.AlertRule{}).Where("tenant_id = ?", tenantId).Count(&data.CountAlertRules).Error; err != nil {
		return data, err
	}
	if err := db.Model(&models.FaultCenter{}).Where("tenant_id = ?", tenantId).Count(&data.FaultCenterNumber).Error; err != nil {
		return data, err
	}
	// Preserve the existing platform-wide user-count meaning.
	if err := db.Model(&models.Member{}).Count(&data.UserNumber).Error; err != nil {
		return data, err
	}
	events, err := c.Redis.Alert().GetAllEvents(models.BuildAlertEventCacheKey(tenantId, faultCenter.ID))
	if err != nil {
		return data, err
	}
	uniq := make(map[string]struct{})
	for _, event := range events {
		if event == nil {
			continue
		}
		switch event.Severity {
		case "P0":
			data.AlarmDistribution.P0++
		case "P1":
			data.AlarmDistribution.P1++
		case "P2":
			data.AlarmDistribution.P2++
		}
		if _, ok := uniq[event.RuleName]; ok {
			continue
		}
		data.CurAlertList = append(data.CurAlertList, types.AlertList{Severity: event.Severity, RuleName: event.RuleName, FaultCenterId: event.FaultCenterId, TiggerTime: event.FirstTriggerTime})
		uniq[event.RuleName] = struct{}{}
	}
	return data, nil
}
