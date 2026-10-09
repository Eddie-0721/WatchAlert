package medium

import (
	"bytes"
	"errors"
	"fmt"
	"io"
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
	return w.post(params.Hook, params.Headers, params.Content)
}

func (w *WebHookSender) Test(params SendParams) error {
	return w.post(params.Hook, params.Headers, WebhookTestContent)
}

func (w *WebHookSender) post(hook string, headers map[string]string, content string) error {
	res, err := tools.Post(headers, hook, bytes.NewReader([]byte(content)), 10)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode != 200 {
		bodyByte, err := io.ReadAll(res.Body)
		if err != nil {
			return fmt.Errorf("读取 Body 失败, err: %s", err.Error())
		}
		return errors.New(string(bodyByte))
	}

	_, _ = io.Copy(io.Discard, io.LimitReader(res.Body, 4096))
	return nil
}
