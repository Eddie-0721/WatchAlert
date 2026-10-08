package api

import (
	"github.com/gin-gonic/gin"
	middleware "watchAlert/internal/middleware"
	"watchAlert/internal/services"
	"watchAlert/internal/types"
)

type ruleTmplController struct{}

var RuleTmplController = new(ruleTmplController)

/*
规则模版 API
/api/w8t/ruleTmpl
*/
func (ruleTmplController ruleTmplController) API(gin *gin.RouterGroup) {
	a := gin.Group("ruleTmpl")
	a.Use(
		middleware.Auth(),
		middleware.Permission(),
		middleware.ParseTenant(),
		middleware.AuditingLog(),
	)
	{
		a.POST("ruleTmplCreate", ruleTmplController.Create)
		a.POST("ruleTmplUpdate", ruleTmplController.Update)
		a.POST("ruleTmplDelete", ruleTmplController.Delete)
	}

	b := gin.Group("ruleTmpl")
	b.Use(
		middleware.Auth(),
		middleware.Permission(),
		middleware.ParseTenant(),
	)
	{
		b.GET("ruleTmplList", ruleTmplController.List)
	}
}

func (ruleTmplController ruleTmplController) Create(ctx *gin.Context) {
	r := new(types.RequestRuleTemplateCreate)
	if !BindJson(ctx, r) {
		return
	}

	Service(ctx, func() (interface{}, interface{}) {
		return services.RuleTmplService.Create(r)
	})
}

func (ruleTmplController ruleTmplController) Update(ctx *gin.Context) {
	r := new(types.RequestRuleTemplateUpdate)
	if !BindJson(ctx, r) {
		return
	}

	Service(ctx, func() (interface{}, interface{}) {
		return services.RuleTmplService.Update(r)
	})
}

func (ruleTmplController ruleTmplController) Delete(ctx *gin.Context) {
	r := new(types.RequestRuleTemplateQuery)
	if !BindJson(ctx, r) {
		return
	}

	Service(ctx, func() (interface{}, interface{}) {
		return services.RuleTmplService.Delete(r)
	})
}

func (ruleTmplController ruleTmplController) List(ctx *gin.Context) {
	r := new(types.RequestRuleTemplateQuery)
	if !BindQuery(ctx, r) {
		return
	}

	Service(ctx, func() (interface{}, interface{}) {
		return services.RuleTmplService.List(r)
	})
}
