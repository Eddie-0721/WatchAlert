package medium

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"strconv"
	"time"
	"watchAlert/internal/ctx"

	"github.com/bytedance/sonic"
	"github.com/zeromicro/go-zero/core/logc"

	"watchAlert/pkg/tools"
)

type (
	// FeiShuSender 飞书发送策略
	FeiShuSender struct{}

	FeiShuResponse struct {
		Code int    `json:"code"`
		Msg  string `json:"msg"`
	}
)

var FeiShuTestContent = fmt.Sprintf(`{
  "msg_type": "text",
  "content": {
  "text": "%s"
  }
}`, RobotTestContent)

func NewFeiShuSender() SendInter { return &FeiShuSender{} }

func (f *FeiShuSender) Send(params SendParams) error {
	return f.postContext(params.Context(), params.Hook, params.Sign, params.GetSendMsg())
}

func (f *FeiShuSender) Test(params SendParams) error {
	msg := make(map[string]any)
	err := sonic.Unmarshal([]byte(FeiShuTestContent), &msg)
	if err != nil {
		logc.Errorf(ctx.Ctx, "发送的内容解析失败, err: %s", err.Error())
		return err
	}

	return f.postContext(params.Context(), params.Hook, params.Sign, msg)
}

func (f *FeiShuSender) post(hook, sign string, msg map[string]any) error {
	return f.postContext(context.Background(), hook, sign, msg)
}

func (f *FeiShuSender) postContext(requestCtx context.Context, hook, sign string, msg map[string]any) error {
	if sign != "" {
		signature, timestamp := generateFeishuSignature(sign)
		msg["sign"] = signature
		msg["timestamp"] = timestamp
	}

	msgByte := bytes.NewReader(tools.JsonMarshalToByte(msg))
	res, err := tools.PostContext(requestCtx, nil, hook, msgByte, 10)
	if err != nil {
		return err
	}
	return checkRobotResponse(res, "code")
}

// generateFeishuSignature 生成 Feishu 签名
func generateFeishuSignature(secret string) (string, string) {
	// 1. Get timestamp
	timestamp := strconv.FormatInt(time.Now().Unix(), 10)

	// 2. Prepare the string to sign: {timestamp}\n{secret}
	stringToSign := fmt.Sprintf("%s\n%s", timestamp, secret)

	var data []byte
	h := hmac.New(sha256.New, []byte(stringToSign))
	h.Write(data)

	return base64.StdEncoding.EncodeToString(h.Sum(nil)), timestamp
}
