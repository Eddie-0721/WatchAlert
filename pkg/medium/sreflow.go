package medium

import (
	"bytes"
	"context"
	"fmt"
	"watchAlert/pkg/tools"
)

type (
	// SREFlowSender
	SREFlowSender struct{}
)

var SREFlowTestContent = fmt.Sprintf(`{
  "text": "%s"
}`, RobotTestContent)

func NewSREFlowSender() SendInter { return &SREFlowSender{} }

func (w *SREFlowSender) Send(params SendParams) error {
	return w.postContext(params.Context(), params.Hook, params.Headers, params.Content)
}

func (w *SREFlowSender) Test(params SendParams) error {
	return w.postContext(params.Context(), params.Hook, params.Headers, SREFlowTestContent)
}

func (w *SREFlowSender) post(hook string, headers map[string]string, content string) error {
	return w.postContext(context.Background(), hook, headers, content)
}

func (w *SREFlowSender) postContext(requestCtx context.Context, hook string, headers map[string]string, content string) error {
	res, err := tools.PostContext(requestCtx, headers, hook, bytes.NewReader([]byte(content)), 10)
	if err != nil {
		return err
	}
	_, err = readNotificationResponse(res)
	return err
}
