// Package delivery contains the bounded IO shared by SMS and phone providers.
package delivery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"watchAlert/internal/types"
)

const RequestTimeout = 10 * time.Second

type BatchSummary struct {
	Accepted     int `json:"accepted"`
	Failed       int `json:"failed"`
	NotAttempted int `json:"notAttempted"`
}

// Each recipient is attempted once. A timeout never triggers automatic replay:
// the provider may have accepted a request whose acknowledgement was lost.
func SendBatch(parent context.Context, message *types.Message, send func(context.Context, string, string) error) (*types.NotificationResult, error) {
	return sendBatch(parent, message, RequestTimeout, send)
}

func sendBatch(parent context.Context, message *types.Message, timeout time.Duration, send func(context.Context, string, string) error) (*types.NotificationResult, error) {
	if message == nil {
		return nil, errors.New("notification message is required")
	}
	var summary BatchSummary
	for _, user := range message.ToUsers {
		if strings.TrimSpace(user.Phone) != "" {
			summary.NotAttempted++
		}
	}
	if summary.NotAttempted == 0 {
		return nil, errors.New("电话号码不能为空")
	}
	content, err := json.Marshal(message.Labels)
	if err != nil {
		return nil, fmt.Errorf("encode notification labels: %w", err)
	}
	contentText := string(content)
	var firstError error
	for _, user := range message.ToUsers {
		if err := parent.Err(); err != nil {
			firstError = errors.Join(firstError, err)
			break
		}
		phone := strings.TrimSpace(user.Phone)
		if phone == "" {
			continue
		}
		summary.NotAttempted--
		// A fresh budget per recipient prevents tail starvation in large lists.
		requestCtx, cancel := context.WithTimeout(parent, timeout)
		err := send(requestCtx, contentText, phone)
		cancel()
		if err != nil {
			summary.Failed++
			if firstError == nil {
				firstError = err
			}
		} else {
			summary.Accepted++
		}
	}
	result := &types.NotificationResult{
		Success: firstError == nil && summary.Accepted > 0 && summary.NotAttempted == 0,
		Message: fmt.Sprintf("服务商已受理 %d，失败或结果未知 %d，未尝试 %d", summary.Accepted, summary.Failed, summary.NotAttempted),
		Data:    summary,
	}
	return result, firstError
}
