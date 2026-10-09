package medium

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"net/url"
	"time"
	"watchAlert/pkg/tools"
)

type (
	// DingDingSender 钉钉发送策略
	DingDingSender struct{}

	DingResponse struct {
		Code int    `json:"errcode"`
		Msg  string `json:"errmsg"`
	}
)

var DingTestContent = fmt.Sprintf(`{
	"msgtype": "text",
    "text": {
        "content": "%s"
    }
}`, RobotTestContent)

func NewDingSender() SendInter { return &DingDingSender{} }

func (d *DingDingSender) Send(params SendParams) error {
	return d.postContext(params.Context(), params.Hook, params.Sign, params.Content)
}

func (d *DingDingSender) Test(params SendParams) error {
	return d.postContext(params.Context(), params.Hook, params.Sign, DingTestContent)
}

func (d *DingDingSender) post(hook, sign, content string) error {
	return d.postContext(context.Background(), hook, sign, content)
}

func (d *DingDingSender) postContext(requestCtx context.Context, hook, sign, content string) error {
	if sign != "" {
		signature, timestamp := generateDingSignature(sign)
		hook = fmt.Sprintf("%s&timestamp=%s&sign=%s", hook, timestamp, signature)
	}

	cardContentByte := bytes.NewReader([]byte(content))
	res, err := tools.PostContext(requestCtx, nil, hook, cardContentByte, 10)
	if err != nil {
		return err
	}
	return checkRobotResponse(res, "errcode")
}

// generateDingSignature 生成 Ding 签名
func generateDingSignature(secret string) (string, string) {
	// 1. Get millisecond timestamp
	timestamp := fmt.Sprintf("%d", time.Now().UnixNano()/int64(time.Millisecond))

	// 2. Prepare the string to sign: {timestamp}\n{secret}
	stringToSign := fmt.Sprintf("%s\n%s", timestamp, secret)

	// 3. Create HMAC-SHA256 hash
	h := hmac.New(sha256.New, []byte(secret))
	h.Write([]byte(stringToSign))
	hmacCode := h.Sum(nil)

	return url.QueryEscape(base64.StdEncoding.EncodeToString(hmacCode)), timestamp
}
