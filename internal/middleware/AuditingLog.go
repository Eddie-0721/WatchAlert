package middleware

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"time"
	"watchAlert/internal/ctx"
	"watchAlert/internal/models"
	"watchAlert/pkg/response"
	"watchAlert/pkg/tools"

	"github.com/gin-gonic/gin"
	"github.com/zeromicro/go-zero/core/logc"
)

// Bound memory consumption while retaining enough room for rule imports.
const maxAuditedRequestBody = 8 << 20

func AuditingLog() gin.HandlerFunc {
	return func(context *gin.Context) {
		username := context.GetString("UserId")
		if username == "" {
			response.TokenFail(context)
			context.Abort()
			return
		}
		tid := context.Request.Header.Get(TenantIDHeaderKey)
		if !validTenantID(tid) {
			response.Fail(context, "租户ID无效", "failed")
			context.Abort()
			return
		}

		// Response log
		body := http.MaxBytesReader(context.Writer, context.Request.Body, maxAuditedRequestBody)
		readBody, err := io.ReadAll(body)
		_ = body.Close()
		if err != nil {
			var tooLarge *http.MaxBytesError
			if errors.As(err, &tooLarge) {
				response.Response(context, http.StatusRequestEntityTooLarge, http.StatusRequestEntityTooLarge, "请求体超过 8 MiB 限制", "failed")
			} else {
				response.Fail(context, "请求体读取失败", "failed")
			}
			context.Abort()
			return
		}
		// 将 body 数据放回请求中
		context.Request.Body = io.NopCloser(bytes.NewBuffer(readBody))
		summary := auditBodySummary(readBody)

		// 获取请求的完整API路径
		fullPath := context.Request.URL.Path

		// 当请求处理完成后才会执行 Next() 后面的代码
		context.Next()

		ps := models.AuditEventMap
		auditLog := models.AuditLog{
			TenantId:   tid,
			ID:         tools.RandId(),
			Username:   username,
			IPAddress:  context.ClientIP(),
			Method:     context.Request.Method,
			Path:       fullPath,
			CreatedAt:  time.Now().Unix(),
			StatusCode: context.Writer.Status(),
			Body:       summary,
			AuditType:  ps[fullPath],
		}

		c := ctx.DO()
		err = c.DB.AuditLog().Create(auditLog)
		if err != nil {
			// The business response may already have been sent (including SSE).
			// Do not append another JSON document or expose a database error.
			logc.Error(context.Request.Context(), "审计日志写入数据库失败")
			_ = context.Error(errors.New("审计日志写入数据库失败"))
			return
		}
	}
}
