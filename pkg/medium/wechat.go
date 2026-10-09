package medium

import (
	"bytes"
	"context"
	"fmt"
	"watchAlert/pkg/tools"
)

type (
	// WeChatSender 企业微信发送策略
	WeChatSender struct{}

	WeChatResponse struct {
		Code int    `json:"errcode"`
		Msg  string `json:"errmsg"`
	}
)

var WechatTestContent = fmt.Sprintf(`{
	"msgtype": "text",
	"text": {
	    "content": "%s"
	}
}`, RobotTestContent)

func NewWeChatSender() SendInter { return &WeChatSender{} }

func (w *WeChatSender) Send(params SendParams) error {
	return w.postContext(params.Context(), params.Hook, params.Content)
}

func (w *WeChatSender) Test(params SendParams) error {
	return w.postContext(params.Context(), params.Hook, WechatTestContent)
}

func (w *WeChatSender) post(hook, content string) error {
	return w.postContext(context.Background(), hook, content)
}

func (w *WeChatSender) postContext(requestCtx context.Context, hook, content string) error {
	res, err := tools.PostContext(requestCtx, nil, hook, bytes.NewReader([]byte(content)), 10)
	if err != nil {
		return err
	}
	return checkRobotResponse(res, "errcode")
}
