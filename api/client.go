package api

import (
	"github.com/gin-gonic/gin"
	middleware "watchAlert/internal/middleware"
	"watchAlert/internal/services"
	"watchAlert/internal/types"
)

type clientController struct{}

var ClientController = new(clientController)

func (clientController clientController) API(gin *gin.RouterGroup) {
	a := gin.Group("c")
	a.Use(
		middleware.Auth(),
		middleware.Permission(),
		middleware.ParseTenant(),
	)
	{
		a.GET("getJaegerService", clientController.GetJaegerService)
	}
}

func (clientController clientController) GetJaegerService(ctx *gin.Context) {
	r := new(types.RequestDatasourceQuery)
	if !BindQuery(ctx, r) {
		return
	}

	Service(ctx, func() (interface{}, interface{}) {
		r.TenantId = ctx.GetString("TenantID")
		return services.ClientService.GetJaegerService(r)
	})
}
