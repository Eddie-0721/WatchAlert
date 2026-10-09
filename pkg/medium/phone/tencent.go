package phone

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"net/http"
	"watchAlert/internal/types"
	"watchAlert/pkg/medium/delivery"
	"watchAlert/pkg/tools"
)

// TencentPhoneNotifier 腾讯云电话通知器
type TencentPhoneNotifier struct {
	SecretID  string
	SecretKey string
	AppID     string
	postHTTP  func(context.Context, string, *bytes.Reader) (*http.Response, error)
}

// NewTencentPhoneNotifier 创建腾讯云电话通知器
func NewTencentPhoneNotifier(secretID, secretKey, appID string) *TencentPhoneNotifier {
	return &TencentPhoneNotifier{
		SecretID:  secretID,
		SecretKey: secretKey,
		AppID:     appID,
	}
}

// GetType 获取通知器类型
func (t *TencentPhoneNotifier) GetType() types.NotificationType {
	return types.Phone
}

// GetProvider 获取通知器提供商
func (t *TencentPhoneNotifier) GetProvider() types.NotificationProvider {
	return types.TencentPhone
}

// Notify 发送腾讯云电话通知
func (t *TencentPhoneNotifier) Notify(ctx context.Context, message *types.Message) (*types.NotificationResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := t.validate(); err != nil {
		return nil, err
	}
	return delivery.SendBatch(ctx, message, func(requestCtx context.Context, content, number string) error {
		return t.call(requestCtx, content, number)
	})
}

// Call 拨打腾讯云电话的内部方法
func (t *TencentPhoneNotifier) Call(parent context.Context, content, phoneNumber string) string {
	ctx, cancel := context.WithTimeout(parent, delivery.RequestTimeout)
	defer cancel()
	if err := t.call(ctx, content, phoneNumber); err != nil {
		return err.Error()
	}
	return "success"
}

func (t *TencentPhoneNotifier) call(ctx context.Context, content, phoneNumber string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := t.validate(); err != nil {
		return err
	}

	// 腾讯云语音通知API接口地址
	url := "https://cloud.tim.qq.com/v5/tlsvoicesvr/sendtvoice?sdkappid=" + t.AppID + "&random=7226249334"

	// 准备请求数据
	reqData := map[string]interface{}{
		"tel": map[string]string{
			"nationcode": "86", // 国家码，中国为86
			"mobile":     phoneNumber,
		},
		"prompttype": 2,       // 语音通知类型，2表示播放文本转语音
		"promptfile": content, // 语音内容，如果是文本转语音则传入文本内容
		"playtimes":  2,       // 播放次数
	}

	// 计算签名
	timeStr := strconv.FormatInt(time.Now().Unix(), 10)
	timeInt, _ := strconv.Atoi(timeStr)

	// 计算签名，腾讯云语音通知的签名方式与短信类似
	strRand := "7226249334"
	sigContent := "appkey=" + t.SecretKey + "&random=" + strRand + "&time=" + timeStr + "&mobile=" + phoneNumber
	sig := getSha256Code(sigContent)
	reqData["sig"] = sig
	reqData["time"] = timeInt

	jsonData, err := json.Marshal(reqData)
	if err != nil {
		return err
	}
	post := t.postHTTP
	if post == nil {
		post = func(ctx context.Context, url string, body *bytes.Reader) (*http.Response, error) {
			return tools.PostContext(ctx, nil, url, body, 10)
		}
	}
	response, err := post(ctx, url, bytes.NewReader(jsonData))
	if err != nil {
		return delivery.RequestError(ctx, err)
	}
	body, err := delivery.ReadResponse(response)
	if err != nil {
		return err
	}
	return delivery.CheckTencent(body, nil)
}

// getSha256Code 计算SHA256摘要
func getSha256Code(data string) string {
	hash := sha256.New()
	hash.Write([]byte(data))
	return fmt.Sprintf("%x", hash.Sum(nil))
}

// validate 验证配置参数
func (t *TencentPhoneNotifier) validate() error {
	if t.SecretID == "" {
		return fmt.Errorf("SecretID 不能为空")
	}
	if t.SecretKey == "" {
		return fmt.Errorf("SecretKey 不能为空")
	}
	if t.AppID == "" {
		return fmt.Errorf("AppID 不能为空")
	}
	return nil
}
