package medium

import (
	"bytes"
	"context"
	"fmt"
	"watchAlert/pkg/tools"
)

type (
	// WebHookSender 自定义Hook发送策略
	WebHookSender struct{}
)

var WebhookTestContent = fmt.Sprintf(`{
  "text": "%s"
}`, RobotTestContent)

func NewWebHookSender() SendInter { return &WebHookSender{} }

func (w *WebHookSender) Send(params SendParams) error {
	return w.postContext(params.Context(), params.Hook, params.Headers, params.Content)
}

func (w *WebHookSender) Test(params SendParams) error {
	return w.postContext(params.Context(), params.Hook, params.Headers, WebhookTestContent)
}

func (w *WebHookSender) post(hook string, headers map[string]string, content string) error {
	return w.postContext(context.Background(), hook, headers, content)
}

func (w *WebHookSender) postContext(requestCtx context.Context, hook string, headers map[string]string, content string) error {
	res, err := tools.PostContext(requestCtx, headers, hook, bytes.NewReader([]byte(content)), 10)
	if err != nil {
		return err
	}
	_, err = readNotificationResponse(res)
	return err
}
