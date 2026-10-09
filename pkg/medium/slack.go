package medium

import (
	"bytes"
	"context"
	"errors"
	"watchAlert/internal/models"
	"watchAlert/pkg/tools"
)

type (
	// SlackSender Slack 发送策略
	SlackSender struct{}
)

func NewSlackSender() SendInter { return &SlackSender{} }

func (f *SlackSender) Send(params SendParams) error {
	msg := params.GetSendMsg()
	return f.postContext(params.Context(), params.Hook, tools.JsonMarshalToString(msg))
}

func (f *SlackSender) Test(params SendParams) error {
	msg := models.SlackMsgTemplate{
		Text: RobotTestContent,
	}
	return f.postContext(params.Context(), params.Hook, tools.JsonMarshalToString(msg))
}

func (f *SlackSender) post(hook, content string) error {
	return f.postContext(context.Background(), hook, content)
}

func (f *SlackSender) postContext(requestCtx context.Context, hook, content string) error {
	res, err := tools.PostContext(requestCtx, nil, hook, bytes.NewReader([]byte(content)), 10)
	if err != nil {
		return err
	}
	bodyByte, err := readNotificationResponse(res)
	if err != nil {
		return err
	}

	if string(bodyByte) != "ok" {
		return errors.New("Slack did not acknowledge notification with ok")
	}

	return nil
}
