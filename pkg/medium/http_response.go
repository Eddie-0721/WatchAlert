package medium

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// Notification acknowledgements are small. Bound both successful and failed
// responses; never copy an upstream response (which can contain secrets) to logs.
const maxNotificationResponseBytes = 64 << 10

func readNotificationResponse(res *http.Response) ([]byte, error) {
	defer res.Body.Close()
	body, err := io.ReadAll(io.LimitReader(res.Body, maxNotificationResponseBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read notification response: %w", err)
	}
	if len(body) > maxNotificationResponseBytes {
		return nil, fmt.Errorf("notification response exceeds %d bytes (HTTP %d)", maxNotificationResponseBytes, res.StatusCode)
	}
	// Preserve existing webhook success contract: HTTP 200, not arbitrary 2xx.
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("notification endpoint returned HTTP %d", res.StatusCode)
	}
	return body, nil
}

func checkRobotResponse(res *http.Response, codeField string) error {
	body, err := readNotificationResponse(res)
	if err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil {
		return fmt.Errorf("invalid notification acknowledgement JSON")
	}
	var code *int
	if err := json.Unmarshal(fields[codeField], &code); err != nil || code == nil {
		return fmt.Errorf("notification acknowledgement missing integer %s", codeField)
	}
	if *code != 0 {
		return fmt.Errorf("notification provider rejected request (%s=%d)", codeField, *code)
	}
	return nil
}
