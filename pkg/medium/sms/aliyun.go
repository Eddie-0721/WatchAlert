package sms

import (
	"context"
	"fmt"
	"net/http"
	"time"
	"watchAlert/internal/types"
	"watchAlert/pkg/medium/delivery"

	"github.com/aliyun/alibaba-cloud-sdk-go/sdk"
	"github.com/aliyun/alibaba-cloud-sdk-go/sdk/auth/credentials"
	"github.com/aliyun/alibaba-cloud-sdk-go/services/dysmsapi"
)

type AliyunSMSNotifier struct {
	AccessKeyId     string
	AccessKeySecret string
	SignName        string
	TemplateCode    string
	transport       http.RoundTripper
}

func NewAliyunSMSNotifier(id, secret, sign, template string) *AliyunSMSNotifier {
	return &AliyunSMSNotifier{AccessKeyId: id, AccessKeySecret: secret, SignName: sign, TemplateCode: template}
}
func (a *AliyunSMSNotifier) GetType() types.NotificationType         { return types.SMS }
func (a *AliyunSMSNotifier) GetProvider() types.NotificationProvider { return types.AliyunSms }

func (a *AliyunSMSNotifier) newClient() (*dysmsapi.Client, error) {
	config := sdk.NewConfig().WithAutoRetry(false).WithMaxRetryTime(0).WithTimeout(10 * time.Second)
	return dysmsapi.NewClientWithOptions("cn-hangzhou", config, credentials.NewAccessKeyCredential(a.AccessKeyId, a.AccessKeySecret))
}

func (a *AliyunSMSNotifier) Notify(ctx context.Context, message *types.Message) (*types.NotificationResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := a.validate(); err != nil {
		return nil, err
	}
	client, err := a.newClient()
	if err != nil {
		return nil, delivery.RequestError(ctx, err)
	}
	return delivery.SendBatch(ctx, message, func(requestCtx context.Context, content, number string) error {
		client.SetTransport(delivery.SDKTransport{Parent: requestCtx, Base: a.transport})
		return a.send(client, requestCtx, content, number)
	})
}

func (a *AliyunSMSNotifier) send(client *dysmsapi.Client, ctx context.Context, content, number string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	request := dysmsapi.CreateSendSmsRequest()
	request.Scheme = "https"
	request.PhoneNumbers = number
	request.SignName = a.SignName
	request.TemplateCode = a.TemplateCode
	request.TemplateParam = content
	response, err := client.SendSms(request)
	if err != nil {
		return delivery.RequestError(ctx, err)
	}
	return delivery.CheckAliyunCode(response.Code)
}

// Post is retained for existing direct callers. Consumer paths use Notify.
func (a *AliyunSMSNotifier) Post(content, number, logsign string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), delivery.RequestTimeout)
	defer cancel()
	if err := a.validate(); err != nil {
		return "", err
	}
	client, err := a.newClient()
	if err != nil {
		return "", delivery.RequestError(ctx, err)
	}
	client.SetTransport(delivery.SDKTransport{Parent: ctx, Base: a.transport})
	if err := a.send(client, ctx, content, number); err != nil {
		return "", err
	}
	return "OK", nil
}

func (a *AliyunSMSNotifier) validate() error {
	if a.AccessKeyId == "" {
		return fmt.Errorf("AccessKeyId 不能为空")
	}
	if a.AccessKeySecret == "" {
		return fmt.Errorf("AccessKeySecret 不能为空")
	}
	if a.SignName == "" {
		return fmt.Errorf("SignName 不能为空")
	}
	if a.TemplateCode == "" {
		return fmt.Errorf("TemplateCode 不能为空")
	}
	return nil
}
