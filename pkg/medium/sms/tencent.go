package sms

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"net/http"
	"watchAlert/internal/types"
	"watchAlert/pkg/medium/delivery"
	"watchAlert/pkg/tools"
)

// TencentSMSNotifier 腾讯云短信通知器
type TencentSMSNotifier struct {
	AppKey     string
	SdkAppId   string
	TemplateId int
	Sign       string
	postHTTP   func(context.Context, string, *bytes.Reader) (*http.Response, error)
}

// Mobiles 手机号码结构
type Mobiles struct {
	Mobile     string `json:"mobile"`
	Nationcode string `json:"nationcode"`
}

// TXmessage 腾讯云短信消息结构
type TXmessage struct {
	Ext    string    `json:"ext"`
	Extend string    `json:"extend"`
	Params []string  `json:"params"`
	Sig    string    `json:"sig"`
	Sign   string    `json:"sign"`
	Tel    []Mobiles `json:"tel"`
	Time   int       `json:"time"`
	Tpl_id int       `json:"tpl_id"`
}

// NewTencentSMSNotifier 创建腾讯云短信通知器
func NewTencentSMSNotifier(appKey, sdkAppId string, templateId int, sign string) *TencentSMSNotifier {
	return &TencentSMSNotifier{
		AppKey:     appKey,
		SdkAppId:   sdkAppId,
		TemplateId: templateId,
		Sign:       sign,
	}
}

// GetType 获取通知器类型
func (t *TencentSMSNotifier) GetType() types.NotificationType {
	return types.SMS
}

// GetProvider 获取通知器提供商
func (t *TencentSMSNotifier) GetProvider() types.NotificationProvider {
	return types.TencentSms
}

// Notify 发送腾讯云短信通知
func (t *TencentSMSNotifier) Notify(ctx context.Context, message *types.Message) (*types.NotificationResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := t.validate(); err != nil {
		return nil, err
	}
	return delivery.SendBatch(ctx, message, func(requestCtx context.Context, content, number string) error {
		_, err := t.postContext(requestCtx, content, number, "sms")
		return err
	})
}

// Post 发送腾讯云短信
func (t *TencentSMSNotifier) Post(Messages, PhoneNumbers, logsign string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), delivery.RequestTimeout)
	defer cancel()
	return t.postContext(ctx, Messages, PhoneNumbers, logsign)
}

func (t *TencentSMSNotifier) postContext(ctx context.Context, Messages, PhoneNumbers, logsign string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	// 检查配置是否完整
	if t.AppKey == "" || t.SdkAppId == "" || t.TemplateId == 0 || t.Sign == "" {
		return "", fmt.Errorf("腾讯云短信接口配置不完整")
	}

	TXmobiles := []Mobiles{}
	mobiles := splitPhoneNumbers(PhoneNumbers)
	for _, m := range mobiles {
		TXmobiles = append(TXmobiles, Mobiles{
			Mobile:     m,
			Nationcode: "86",
		})
	}

	strRand := "7226249334"
	strTime := strconv.FormatInt(time.Now().Unix(), 10)
	intTime, _ := strconv.Atoi(strTime)
	sig := t.getSha256Code("appkey=" + t.AppKey + "&random=" + strRand + "&time=" + strTime + "&mobile=" + PhoneNumbers)
	url := "https://yun.tim.qq.com/v5/tlssmssvr/sendmultisms2?sdkappid=" + t.SdkAppId + "&random=" + strRand

	u := TXmessage{
		Ext:    logsign,
		Extend: "",
		Params: []string{Messages},
		Sig:    sig,
		Sign:   t.Sign,
		Tel:    TXmobiles,
		Time:   intTime,
		Tpl_id: t.TemplateId,
	}

	payload, err := json.Marshal(u)
	if err != nil {
		return "", err
	}
	post := t.postHTTP
	if post == nil {
		post = func(ctx context.Context, url string, body *bytes.Reader) (*http.Response, error) {
			return tools.PostContext(ctx, nil, url, body, 10)
		}
	}
	response, err := post(ctx, url, bytes.NewReader(payload))
	if err != nil {
		return "", delivery.RequestError(ctx, err)
	}
	body, err := delivery.ReadResponse(response)
	if err != nil {
		return "", err
	}
	if err := delivery.CheckTencent(body, mobiles); err != nil {
		return "", err
	}
	return "OK", nil
}

// getSha256Code 计算SHA256摘要
func (t *TencentSMSNotifier) getSha256Code(data string) string {
	hash := sha256.New()
	hash.Write([]byte(data))
	return fmt.Sprintf("%x", hash.Sum(nil))
}

// validate 验证配置参数
func (t *TencentSMSNotifier) validate() error {
	if t.AppKey == "" {
		return fmt.Errorf("AppKey 不能为空")
	}
	if t.SdkAppId == "" {
		return fmt.Errorf("SdkAppId 不能为空")
	}
	if t.Sign == "" {
		return fmt.Errorf("Sign 不能为空")
	}
	if t.TemplateId == 0 {
		return fmt.Errorf("TemplateCode 不能为空")
	}
	return nil
}

// splitPhoneNumbers 分割手机号码
func splitPhoneNumbers(phoneNumbers string) []string {
	if phoneNumbers == "" {
		return nil
	}

	// 去重和清理
	unique := make(map[string]bool)
	var result []string

	parts := strings.Split(phoneNumbers, ",")
	for _, phone := range parts {
		phone = strings.TrimSpace(phone)
		if phone != "" && !unique[phone] {
			unique[phone] = true
			result = append(result, phone)
		}
	}

	return result
}
