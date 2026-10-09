package medium

import (
	"fmt"
	"watchAlert/internal/types"
)

func notificationResultError(channel string, result *types.NotificationResult, err error) error {
	if err != nil {
		if result != nil {
			return fmt.Errorf("%s：%s；%w", channel, result.Message, err)
		}
		return fmt.Errorf("%s：%w", channel, err)
	}
	if result == nil {
		return fmt.Errorf("%s：服务商没有返回发送结果", channel)
	}
	if !result.Success {
		return fmt.Errorf("%s：%s", channel, result.Message)
	}
	return nil
}
