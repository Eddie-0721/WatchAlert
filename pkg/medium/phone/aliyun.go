package phone

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aliyun/alibaba-cloud-sdk-go/sdk"
	"github.com/aliyun/alibaba-cloud-sdk-go/sdk/auth/credentials"
	"github.com/aliyun/alibaba-cloud-sdk-go/services/dyvmsapi"
	"net/http"
	"time"
	"watchAlert/internal/types"
	"watchAlert/pkg/medium/delivery"
)

type AliyunPhoneNotifier struct {
	AccessKeyId      string
	AccessKeySecret  string
	CalledShowNumber string
	TtsCode          string
	Region           string
	transport        http.RoundTripper
}

func NewAliyunPhoneNotifier(id, secret, caller, tts string) *AliyunPhoneNotifier {
	return &AliyunPhoneNotifier{AccessKeyId: id, AccessKeySecret: secret, CalledShowNumber: caller, TtsCode: tts, Region: "cn-hangzhou"}
}
func (a *AliyunPhoneNotifier) GetType() types.NotificationType         { return types.Phone }
func (a *AliyunPhoneNotifier) GetProvider() types.NotificationProvider { return types.AliyunPhone }

func (a *AliyunPhoneNotifier) newClient() (*dyvmsapi.Client, error) {
	config := sdk.NewConfig().WithAutoRetry(false).WithMaxRetryTime(0).WithTimeout(10 * time.Second)
	return dyvmsapi.NewClientWithOptions(a.Region, config, credentials.NewAccessKeyCredential(a.AccessKeyId, a.AccessKeySecret))
}

func (a *AliyunPhoneNotifier) Notify(ctx context.Context, message *types.Message) (*types.NotificationResult, error) {
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
		return a.call(client, requestCtx, content, number)
	})
}

func (a *AliyunPhoneNotifier) call(client *dyvmsapi.Client, ctx context.Context, content, number string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	request := dyvmsapi.CreateSingleCallByTtsRequest()
	request.Scheme = "https"
	request.CalledShowNumber = a.CalledShowNumber
	request.CalledNumber = number
	request.TtsCode = a.TtsCode
	payload, err := json.Marshal(map[string]string{"content": content})
	if err != nil {
		return err
	}
	request.TtsParam = string(payload)
	response, err := client.SingleCallByTts(request)
	if err != nil {
		return delivery.RequestError(ctx, err)
	}
	return delivery.CheckAliyunCode(response.Code)
}

// Call keeps its historical signature; internal delivery uses typed errors.
func (a *AliyunPhoneNotifier) Call(parent context.Context, content, number string) string {
	ctx, cancel := context.WithTimeout(parent, delivery.RequestTimeout)
	defer cancel()
	if err := a.validate(); err != nil {
		return err.Error()
	}
	client, err := a.newClient()
	if err != nil {
		return delivery.RequestError(ctx, err).Error()
	}
	client.SetTransport(delivery.SDKTransport{Parent: ctx, Base: a.transport})
	if err := a.call(client, ctx, content, number); err != nil {
		return err.Error()
	}
	return "OK"
}

func (a *AliyunPhoneNotifier) validate() error {
	if a.AccessKeyId == "" {
		return fmt.Errorf("AccessKeyId 不能为空")
	}
	if a.AccessKeySecret == "" {
		return fmt.Errorf("AccessKeySecret 不能为空")
	}
	if a.CalledShowNumber == "" {
		return fmt.Errorf("CalledShowNumber 不能为空")
	}
	if a.TtsCode == "" {
		return fmt.Errorf("TtsCode 不能为空")
	}
	return nil
}
