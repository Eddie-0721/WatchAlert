package api

import (
	"net/http"
	middleware "watchAlert/internal/middleware"
	"watchAlert/internal/services"
	"watchAlert/internal/types"

	"github.com/gin-gonic/gin"
)

type prometheusController struct{}

var PrometheusController = new(prometheusController)

/*
Prometheus Target API
/api/w8t/prometheus
*/

func (prometheusController prometheusController) API(gin *gin.RouterGroup) {
	prom := gin.Group("prometheus")
	prom.Use(
		middleware.Auth(),
		middleware.Permission(),
		middleware.ParseTenant(),
		middleware.AuditingLog(),
	)
	{
		// 服务组
		prom.POST("targetGroupCreate", prometheusController.CreateTargetGroup)
		prom.POST("targetGroupUpdate", prometheusController.UpdateTargetGroup)
		prom.POST("targetGroupDelete", prometheusController.DeleteTargetGroup)
		// 服务
		prom.POST("targetCreate", prometheusController.CreateTarget)
		prom.POST("targetUpdate", prometheusController.UpdateTarget)
		prom.POST("targetDelete", prometheusController.DeleteTarget)
		// 版本回滚
		prom.POST("targetVersionRollback", prometheusController.RollbackTargetVersion)
	}

	// Prometheus HTTP Service Discovery（无需登录鉴权）
	promSD := gin.Group("prometheus")
	{
		promSD.GET("targets", prometheusController.ListTargetsForSD)
	}

	promRead := gin.Group("prometheus")
	promRead.Use(
		middleware.Auth(),
		middleware.ParseTenant(),
	)
	{
		// 服务组
		promRead.GET("targetGroupList", prometheusController.ListTargetGroup)
		promRead.GET("targetGroupGet", prometheusController.GetTargetGroup)
		// 服务
		promRead.GET("targetList", prometheusController.ListTarget)
		promRead.GET("targetGet", prometheusController.GetTarget)
		// 版本
		promRead.GET("targetVersionList", prometheusController.ListTargetVersion)
		promRead.GET("targetVersionGet", prometheusController.GetTargetVersion)
	}
}

// -----------------------------------------------------------------------
// Service Discovery
// -----------------------------------------------------------------------

func (prometheusController prometheusController) ListTargetsForSD(c *gin.Context) {
	r := new(types.RequestPrometheusSDTargets)
	if err := c.ShouldBindQuery(r); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	data, err := services.PrometheusService.ListTargetsForSD(r)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.(error).Error()})
		return
	}

	c.JSON(http.StatusOK, data)
}

// -----------------------------------------------------------------------
// TargetGroup
// -----------------------------------------------------------------------

func (prometheusController prometheusController) CreateTargetGroup(c *gin.Context) {
	r := new(types.RequestPrometheusCreateTargetGroup)
	if !BindJson(c, r) {
		return
	}

	tid, _ := c.Get("TenantID")
	r.TenantId = tid.(string)

	Service(c, func() (interface{}, interface{}) {
		return services.PrometheusService.CreateTargetGroup(r)
	})
}

func (prometheusController prometheusController) UpdateTargetGroup(c *gin.Context) {
	r := new(types.RequestPrometheusUpdateTargetGroup)
	if !BindJson(c, r) {
		return
	}

	tid, _ := c.Get("TenantID")
	r.TenantId = tid.(string)

	Service(c, func() (interface{}, interface{}) {
		return services.PrometheusService.UpdateTargetGroup(r)
	})
}

func (prometheusController prometheusController) DeleteTargetGroup(c *gin.Context) {
	r := new(types.RequestPrometheusDeleteTargetGroup)
	if !BindJson(c, r) {
		return
	}

	tid, _ := c.Get("TenantID")
	r.TenantId = tid.(string)

	Service(c, func() (interface{}, interface{}) {
		return services.PrometheusService.DeleteTargetGroup(r)
	})
}

func (prometheusController prometheusController) ListTargetGroup(c *gin.Context) {
	r := new(types.RequestPrometheusListTargetGroup)
	if !BindQuery(c, r) {
		return
	}

	tid, _ := c.Get("TenantID")
	r.TenantId = tid.(string)

	Service(c, func() (interface{}, interface{}) {
		return services.PrometheusService.ListTargetGroup(r)
	})
}

func (prometheusController prometheusController) GetTargetGroup(c *gin.Context) {
	r := new(types.RequestPrometheusGetTargetGroup)
	if !BindQuery(c, r) {
		return
	}

	tid, _ := c.Get("TenantID")
	r.TenantId = tid.(string)

	Service(c, func() (interface{}, interface{}) {
		return services.PrometheusService.GetTargetGroup(r)
	})
}

// -----------------------------------------------------------------------
// Target
// -----------------------------------------------------------------------

func (prometheusController prometheusController) CreateTarget(c *gin.Context) {
	r := new(types.RequestPrometheusCreateTarget)
	if !BindJson(c, r) {
		return
	}

	tid, _ := c.Get("TenantID")
	r.TenantId = tid.(string)

	Service(c, func() (interface{}, interface{}) {
		return services.PrometheusService.CreateTarget(r)
	})
}

func (prometheusController prometheusController) UpdateTarget(c *gin.Context) {
	r := new(types.RequestPrometheusUpdateTarget)
	if !BindJson(c, r) {
		return
	}

	tid, _ := c.Get("TenantID")
	r.TenantId = tid.(string)

	Service(c, func() (interface{}, interface{}) {
		return services.PrometheusService.UpdateTarget(r)
	})
}

func (prometheusController prometheusController) DeleteTarget(c *gin.Context) {
	r := new(types.RequestPrometheusDeleteTarget)
	if !BindJson(c, r) {
		return
	}

	tid, _ := c.Get("TenantID")
	r.TenantId = tid.(string)

	Service(c, func() (interface{}, interface{}) {
		return services.PrometheusService.DeleteTarget(r)
	})
}

func (prometheusController prometheusController) ListTarget(c *gin.Context) {
	r := new(types.RequestPrometheusListTarget)
	if !BindQuery(c, r) {
		return
	}

	tid, _ := c.Get("TenantID")
	r.TenantId = tid.(string)

	Service(c, func() (interface{}, interface{}) {
		return services.PrometheusService.ListTarget(r)
	})
}

func (prometheusController prometheusController) GetTarget(c *gin.Context) {
	r := new(types.RequestPrometheusGetTarget)
	if !BindQuery(c, r) {
		return
	}

	tid, _ := c.Get("TenantID")
	r.TenantId = tid.(string)

	Service(c, func() (interface{}, interface{}) {
		return services.PrometheusService.GetTarget(r)
	})
}

// -----------------------------------------------------------------------
// TargetVersion
// -----------------------------------------------------------------------

func (prometheusController prometheusController) ListTargetVersion(c *gin.Context) {
	r := new(types.RequestPrometheusListTargetVersion)
	if !BindQuery(c, r) {
		return
	}

	tid, _ := c.Get("TenantID")
	r.TenantId = tid.(string)

	Service(c, func() (interface{}, interface{}) {
		return services.PrometheusService.ListTargetVersion(r)
	})
}

func (prometheusController prometheusController) GetTargetVersion(c *gin.Context) {
	r := new(types.RequestPrometheusGetTargetVersion)
	if !BindQuery(c, r) {
		return
	}

	tid, _ := c.Get("TenantID")
	r.TenantId = tid.(string)

	Service(c, func() (interface{}, interface{}) {
		return services.PrometheusService.GetTargetVersion(r)
	})
}

func (prometheusController prometheusController) RollbackTargetVersion(c *gin.Context) {
	r := new(types.RequestPrometheusRollbackTargetVersion)
	if !BindJson(c, r) {
		return
	}

	tid, _ := c.Get("TenantID")
	r.TenantId = tid.(string)

	Service(c, func() (interface{}, interface{}) {
		return services.PrometheusService.RollbackTargetVersion(r)
	})
}
