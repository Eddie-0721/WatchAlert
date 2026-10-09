package medium

import (
	"context"
	"errors"
	"strings"
	"testing"
	"watchAlert/internal/models"
	"watchAlert/internal/types"
)

type contextNotifier struct {
	expected context.Context
	t        *testing.T
}

func (n contextNotifier) GetType() types.NotificationType         { return types.SMS }
func (n contextNotifier) GetProvider() types.NotificationProvider { return types.AliyunSms }
func (n contextNotifier) Notify(ctx context.Context, _ *types.Message) (*types.NotificationResult, error) {
	if ctx != n.expected {
		n.t.Fatal("caller context lost")
	}
	return &types.NotificationResult{Success: false, Message: "已受理 1，未尝试 2"}, context.Canceled
}
func TestCommunicationAdaptersPreserveCancellationAndPartialCounts(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, sender := range []SendInter{&SMSAdapter{notifier: contextNotifier{ctx, t}}, &PhoneAdapter{notifier: contextNotifier{ctx, t}}} {
		params := SendParams{RequestContext: ctx, Content: "{}", SMS: models.SMS{To: []string{"10001"}}, Phone: models.Phone{To: []string{"10001"}}}
		for _, call := range []func(SendParams) error{sender.Send, sender.Test} {
			err := call(params)
			if !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "未尝试 2") {
				t.Fatal(err)
			}
		}
	}
	if err := notificationResultError("SMS", nil, nil); err == nil {
		t.Fatal("nil response accepted")
	}
}
