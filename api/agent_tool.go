package api

import (
	"crypto/subtle"
	"fmt"
	"net/http"
	"watchAlert/config"
	"watchAlert/internal/services"
	"watchAlert/internal/types"
	"watchAlert/pkg/agenttoken"
	"watchAlert/pkg/response"

	"github.com/gin-gonic/gin"
)

// AgentToolController is an internal-only gateway. It deliberately does not
// use browser JWT authentication: callers present both the service token and a
// short-lived, user-scoped run token issued by the Go BFF.
type agentToolController struct{}

var AgentToolController = new(agentToolController)

func (agentToolController agentToolController) API(gin *gin.RouterGroup) {
	internal := gin.Group("internal/agent")
	internal.Use(requireAgentServiceToken())
	{
		internal.POST("tools/execute", agentToolController.Execute)
	}
}

func (agentToolController agentToolController) Execute(ctx *gin.Context) {
	r := new(types.RequestAgentToolCall)
	if err := ctx.ShouldBindJSON(r); err != nil {
		response.Fail(ctx, err.Error(), "failed")
		return
	}
	claims, err := agenttoken.Verify(r.RunToken, config.Application.Agent.InternalToken)
	if err != nil {
		response.Fail(ctx, err.Error(), "failed")
		return
	}
	if !agenttoken.Allows(claims, r.ToolName) {
		response.Fail(ctx, fmt.Sprintf("当前运行无权调用 Tool %s", r.ToolName), "failed")
		return
	}
	data, err := services.AgentToolService.Execute(ctx.Request.Context(), claims, r.ToolName, r.Arguments)
	if err != nil {
		response.Fail(ctx, err.Error(), "failed")
		return
	}
	response.Success(ctx, data, "success")
}

func requireAgentServiceToken() gin.HandlerFunc {
	return func(ctx *gin.Context) {
		expected := config.Application.Agent.InternalToken
		provided := ctx.GetHeader("X-WatchAlert-Agent-Token")
		if expected == "" || provided == "" || subtle.ConstantTimeCompare([]byte(expected), []byte(provided)) != 1 {
			response.Fail(ctx, "Agent 服务身份验证失败", "failed")
			ctx.AbortWithStatus(http.StatusUnauthorized)
			return
		}
		ctx.Next()
	}
}
