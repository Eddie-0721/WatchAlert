package api

import (
	"time"
	"watchAlert/internal/middleware"
	"watchAlert/internal/services"
	"watchAlert/internal/types"
	"watchAlert/pkg/response"
	utils "watchAlert/pkg/tools"

	"github.com/gin-gonic/gin"
)

type alertEventController struct{}

var AlertEventController = new(alertEventController)

/*
告警事件 API
/api/w8t/event
*/
func (alertEventController alertEventController) API(gin *gin.RouterGroup) {
	a := gin.Group("event")
	a.Use(
		middleware.Auth(),
		middleware.Permission(),
		middleware.ParseTenant(),
	)
	{
		a.POST("delete", alertEventController.DeleteAlertEvent)
		a.POST("addComment", alertEventController.AddComment)
		a.GET("listComments", alertEventController.ListComment)
		a.POST("deleteComment", alertEventController.DeleteComment)
	}

	b := gin.Group("event")
	b.Use(
		middleware.Auth(),
		middleware.Permission(),
		middleware.ParseTenant(),
	)
	{
		b.POST("process", alertEventController.ProcessAlertEvent)
		b.GET("curEvent", alertEventController.ListCurrentEvent)
		b.GET("hisEvent", alertEventController.ListHistoryEvent)
	}
}

func (alertEventController alertEventController) ProcessAlertEvent(ctx *gin.Context) {
	r := new(types.RequestProcessAlertEvent)
	if !BindJson(ctx, r) {
		return
	}

	tid, _ := ctx.Get("TenantID")
	r.TenantId = tid.(string)
	r.Time = time.Now().Unix()

	tokenStr := ctx.Request.Header.Get("Authorization")
	if tokenStr == "" {
		response.Fail(ctx, "未知的用户", "failed")
		return
	}

	r.Username = utils.GetUser(tokenStr)

	Service(ctx, func() (interface{}, interface{}) {
		return services.EventService.ProcessAlertEvent(r)
	})
}

func (alertEventController alertEventController) DeleteAlertEvent(ctx *gin.Context) {
	r := new(types.RequestProcessAlertEvent)
	if !BindJson(ctx, r) {
		return
	}

	tid, _ := ctx.Get("TenantID")
	r.TenantId = tid.(string)
	r.Time = time.Now().Unix()

	Service(ctx, func() (interface{}, interface{}) {
		return services.EventService.DeleteAlertEvent(r)
	})
}

func (alertEventController alertEventController) ListCurrentEvent(ctx *gin.Context) {
	r := new(types.RequestAlertCurEventQuery)
	if !BindQuery(ctx, r) {
		return
	}

	tid, _ := ctx.Get("TenantID")
	r.TenantId = tid.(string)

	Service(ctx, func() (interface{}, interface{}) {
		return services.EventService.ListCurrentEventContext(ctx.Request.Context(), r)
	})
}

func (alertEventController alertEventController) ListHistoryEvent(ctx *gin.Context) {
	r := new(types.RequestAlertHisEventQuery)
	if !BindQuery(ctx, r) {
		return
	}

	tid, _ := ctx.Get("TenantID")
	r.TenantId = tid.(string)

	Service(ctx, func() (interface{}, interface{}) {
		return services.EventService.ListHistoryEvent(ctx.Request.Context(), r)
	})
}

func (alertEventController alertEventController) ListComment(ctx *gin.Context) {
	r := new(types.RequestListEventComments)
	if !BindQuery(ctx, r) {
		return
	}

	tid, _ := ctx.Get("TenantID")
	r.TenantId = tid.(string)

	Service(ctx, func() (interface{}, interface{}) {
		return services.EventService.ListComments(r)
	})
}

func (alertEventController alertEventController) AddComment(ctx *gin.Context) {
	r := new(types.RequestAddEventComment)
	if !BindJson(ctx, r) {
		return
	}

	tid, _ := ctx.Get("TenantID")
	r.TenantId = tid.(string)

	token := ctx.Request.Header.Get("Authorization")
	r.Username = utils.GetUser(token)
	r.UserId = utils.GetUserID(token)

	Service(ctx, func() (interface{}, interface{}) {
		return services.EventService.AddComment(r)
	})
}

func (alertEventController alertEventController) DeleteComment(ctx *gin.Context) {
	r := new(types.RequestDeleteEventComment)
	if !BindJson(ctx, r) {
		return
	}

	tid, _ := ctx.Get("TenantID")
	r.TenantId = tid.(string)

	Service(ctx, func() (interface{}, interface{}) {
		return services.EventService.DeleteComment(r)
	})
}
