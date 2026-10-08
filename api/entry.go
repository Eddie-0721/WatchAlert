package api

import (
	"context"
	"github.com/gin-gonic/gin"
	"github.com/zeromicro/go-zero/core/logc"
	"watchAlert/pkg/response"
)

func Service(ctx *gin.Context, fu func() (interface{}, interface{})) {
	// Abort stops Gin's handler chain, not the caller's Go function.
	if ctx.IsAborted() {
		return
	}
	data, err := fu()
	if err != nil {
		logc.Error(context.Background(), err)
		response.Fail(ctx, err.(error).Error(), "failed")
		ctx.Abort()
		return
	} else {
		response.Success(ctx, data, "success")
	}
}

func BindJson(ctx *gin.Context, req interface{}) bool {
	if ctx.IsAborted() {
		return false
	}
	err := ctx.ShouldBindJSON(req)
	if err != nil {
		response.Fail(ctx, "请求 JSON 格式或字段类型不正确", "failed")
		ctx.Abort()
		return false
	}
	return true
}

func BindQuery(ctx *gin.Context, req interface{}) bool {
	if ctx.IsAborted() {
		return false
	}
	err := ctx.ShouldBindQuery(req)
	if err != nil {
		response.Fail(ctx, "请求查询参数格式或字段类型不正确", "failed")
		ctx.Abort()
		return false
	}
	return true
}
