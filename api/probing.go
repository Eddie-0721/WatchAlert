package api

import (
	"errors"
	middleware "watchAlert/internal/middleware"
	"watchAlert/internal/services"
	"watchAlert/internal/types"
	"watchAlert/pkg/tools"

	"github.com/gin-gonic/gin"
)

type probingController struct{}

var ProbingController = new(probingController)

func (probingController probingController) API(gin *gin.RouterGroup) {
	a := gin.Group("probing")
	a.Use(
		middleware.Auth(),
		middleware.Permission(),
		middleware.ParseTenant(),
		middleware.AuditingLog(),
	)
	{
		a.POST("createProbing", probingController.Create)
		a.POST("updateProbing", probingController.Update)
		a.POST("deleteProbing", probingController.Delete)
	}

	b := gin.Group("probing")
	b.Use(
		middleware.Auth(),
		middleware.Permission(),
		middleware.ParseTenant(),
	)
	{
		b.GET("listProbing", probingController.List)
		b.GET("searchProbing", probingController.Search)
	}

	c := gin.Group("probing")
	c.Use(
		middleware.Auth(),
		middleware.ParseTenant(),
	)
	{
		c.POST("onceProbing", probingController.Once)
		c.POST("changeState", probingController.ChangeState)
	}
}

func (probingController probingController) List(ctx *gin.Context) {
	r := new(types.RequestProbingRuleQuery)
	if !BindQuery(ctx, r) {
		return
	}

	tid, _ := ctx.Get("TenantID")
	r.TenantId = tid.(string)

	Service(ctx, func() (interface{}, interface{}) {
		return services.ProbingService.List(r)
	})
}

func (probingController probingController) Search(ctx *gin.Context) {
	r := new(types.RequestProbingRuleQuery)
	if !BindQuery(ctx, r) {
		return
	}

	tid, _ := ctx.Get("TenantID")
	r.TenantId = tid.(string)

	Service(ctx, func() (interface{}, interface{}) {
		return services.ProbingService.Search(r)
	})
}

func (probingController probingController) Create(ctx *gin.Context) {
	r := new(types.RequestProbingRuleCreate)
	if !BindJson(ctx, r) {
		return
	}

	Service(ctx, func() (interface{}, interface{}) {
		tokenStr := ctx.Request.Header.Get("Authorization")
		if len(tokenStr) <= 0 {
			return nil, errors.New("用户未登录")
		}
		r.UpdateBy = tools.GetUser(tokenStr)

		tid, _ := ctx.Get("TenantID")
		r.TenantId = tid.(string)

		return services.ProbingService.Create(r)
	})
}

func (probingController probingController) Update(ctx *gin.Context) {
	r := new(types.RequestProbingRuleUpdate)
	if !BindJson(ctx, r) {
		return
	}

	Service(ctx, func() (interface{}, interface{}) {
		tokenStr := ctx.Request.Header.Get("Authorization")
		if len(tokenStr) <= 0 {
			return nil, errors.New("用户未登录")
		}
		r.UpdateBy = tools.GetUser(tokenStr)

		tid, _ := ctx.Get("TenantID")
		r.TenantId = tid.(string)

		return services.ProbingService.Update(r)
	})
}

func (probingController probingController) Delete(ctx *gin.Context) {
	r := new(types.RequestProbingRuleQuery)
	if !BindJson(ctx, r) {
		return
	}

	tid, _ := ctx.Get("TenantID")
	r.TenantId = tid.(string)

	Service(ctx, func() (interface{}, interface{}) {
		return services.ProbingService.Delete(r)
	})
}

func (probingController probingController) Once(ctx *gin.Context) {
	r := new(types.RequestProbingOnce)
	if !BindJson(ctx, r) {
		return
	}

	Service(ctx, func() (interface{}, interface{}) {
		return services.ProbingService.Once(r)
	})
}

func (probingController probingController) ChangeState(ctx *gin.Context) {
	r := new(types.RequestProbeChangeState)
	if !BindJson(ctx, r) {
		return
	}

	Service(ctx, func() (interface{}, interface{}) {
		return services.ProbingService.ChangeState(r)
	})
}
