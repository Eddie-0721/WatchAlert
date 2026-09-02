package api

import (
	"fmt"
	"watchAlert/internal/middleware"
	"watchAlert/internal/services"
	"watchAlert/internal/types"

	"github.com/gin-gonic/gin"
)

// agentController is the browser-facing Copilot BFF. The browser never talks
// to the model provider or the Agent service directly.
type agentController struct{}

var AgentController = new(agentController)

func (agentController agentController) API(gin *gin.RouterGroup) {
	read := gin.Group("agent")
	read.Use(middleware.Auth(), middleware.Permission(), middleware.ParseTenant())
	{
		read.GET("capabilities", agentController.Capabilities)
		read.GET("sessionList", agentController.ListSessions)
		read.GET("sessionGet", agentController.GetSession)
	}

	write := gin.Group("agent")
	write.Use(middleware.Auth(), middleware.Permission(), middleware.ParseTenant(), middleware.AuditingLog())
	{
		write.POST("sessionCreate", agentController.CreateSession)
		write.POST("sessionMessage", agentController.SendMessage)
		write.POST("sessionMessageStream", agentController.StreamMessage)
		write.POST("actionConfirm", agentController.ConfirmAction)
	}
}

func (agentController agentController) ConfirmAction(ctx *gin.Context) {
	r := new(types.RequestAgentActionConfirm)
	BindJson(ctx, r)
	Service(ctx, func() (interface{}, interface{}) {
		tenantId, userId, err := agentRequestScope(ctx)
		if err != nil {
			return nil, err
		}
		return services.AgentService.ConfirmAction(tenantId, userId, r)
	})
}

func (agentController agentController) Capabilities(ctx *gin.Context) {
	Service(ctx, func() (interface{}, interface{}) {
		tenantId, userId, err := agentRequestScope(ctx)
		if err != nil {
			return nil, err
		}
		return services.AgentService.Capabilities(tenantId, userId)
	})
}

func (agentController agentController) ListSessions(ctx *gin.Context) {
	Service(ctx, func() (interface{}, interface{}) {
		tenantId, userId, err := agentRequestScope(ctx)
		if err != nil {
			return nil, err
		}
		return services.AgentService.ListSessions(tenantId, userId)
	})
}

func (agentController agentController) GetSession(ctx *gin.Context) {
	r := new(types.RequestAgentSessionQuery)
	BindQuery(ctx, r)
	Service(ctx, func() (interface{}, interface{}) {
		tenantId, userId, err := agentRequestScope(ctx)
		if err != nil {
			return nil, err
		}
		return services.AgentService.GetSession(tenantId, userId, r.SessionId)
	})
}

func (agentController agentController) CreateSession(ctx *gin.Context) {
	r := new(types.RequestAgentSessionCreate)
	BindJson(ctx, r)
	Service(ctx, func() (interface{}, interface{}) {
		tenantId, userId, err := agentRequestScope(ctx)
		if err != nil {
			return nil, err
		}
		return services.AgentService.CreateSession(tenantId, userId, r)
	})
}

func (agentController agentController) SendMessage(ctx *gin.Context) {
	r := new(types.RequestAgentSessionMessage)
	BindJson(ctx, r)
	Service(ctx, func() (interface{}, interface{}) {
		tenantId, userId, err := agentRequestScope(ctx)
		if err != nil {
			return nil, err
		}
		return services.AgentService.SendMessage(ctx.Request.Context(), tenantId, userId, r)
	})
}

func (agentController agentController) StreamMessage(ctx *gin.Context) {
	r := new(types.RequestAgentSessionMessage)
	if err := ctx.ShouldBindJSON(r); err != nil {
		ctx.SSEvent("error", types.AgentStreamEvent{Type: "error", Message: err.Error()})
		return
	}
	tenantId, userId, err := agentRequestScope(ctx)
	if err != nil {
		ctx.SSEvent("error", types.AgentStreamEvent{Type: "error", Message: err.Error()})
		return
	}
	ctx.Header("Content-Type", "text/event-stream")
	ctx.Header("Cache-Control", "no-cache")
	ctx.Header("X-Accel-Buffering", "no")
	ctx.Status(200)
	ctx.Writer.Flush()
	err = services.AgentService.StreamMessage(ctx.Request.Context(), tenantId, userId, r, func(event types.AgentStreamEvent) {
		ctx.SSEvent(event.Type, event)
		ctx.Writer.Flush()
	})
	if err != nil {
		ctx.SSEvent("error", types.AgentStreamEvent{Type: "error", Message: err.Error()})
		ctx.Writer.Flush()
	}
}

func agentRequestScope(ctx *gin.Context) (string, string, error) {
	tenantId, exists := ctx.Get(middleware.TenantIDHeaderKey)
	if !exists {
		return "", "", fmt.Errorf("租户上下文不存在")
	}
	userId, exists := ctx.Get("UserId")
	if !exists {
		return "", "", fmt.Errorf("用户上下文不存在")
	}
	tenantValue, ok := tenantId.(string)
	if !ok || tenantValue == "" {
		return "", "", fmt.Errorf("租户上下文无效")
	}
	userValue, ok := userId.(string)
	if !ok || userValue == "" {
		return "", "", fmt.Errorf("用户上下文无效")
	}
	return tenantValue, userValue, nil
}
