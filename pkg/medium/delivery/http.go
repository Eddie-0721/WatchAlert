package delivery

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"
)

const MaxResponseBytes = 64 << 10

type responseError struct{ message string }

func (e *responseError) Error() string { return e.message }

// The SDK mutates a concrete *http.Transport per call. Hide the shared socket
// pool behind RoundTripper, and bind cancellation only to this request.
var sdkTransport = func() *http.Transport {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.DialContext = (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext
	t.MaxIdleConns = 64
	t.MaxIdleConnsPerHost = 8
	t.IdleConnTimeout = 90 * time.Second
	return t
}()

type SDKTransport struct {
	Parent context.Context
	Base   http.RoundTripper
}

func (t SDKTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// Keep the SDK/http.Client per-request timeout AND the batch cancellation.
	requestCtx, cancel := context.WithCancel(req.Context())
	defer cancel()
	stop := context.AfterFunc(t.Parent, cancel)
	defer stop()
	if err := t.Parent.Err(); err != nil {
		return nil, err
	}
	base := t.Base
	if base == nil {
		base = sdkTransport
	}
	response, err := base.RoundTrip(req.Clone(requestCtx))
	if err != nil {
		return nil, err
	}
	body, err := ReadResponse(response)
	if err != nil {
		return nil, err
	}
	// The SDK decoder may read without a limit. It receives only bounded bytes.
	response.Body = io.NopCloser(bytes.NewReader(body))
	response.ContentLength = int64(len(body))
	return response, nil
}

func ReadResponse(response *http.Response) ([]byte, error) {
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, MaxResponseBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read notification response: %w", err)
	}
	if len(body) > MaxResponseBytes {
		return nil, &responseError{"notification response exceeds 64 KiB"}
	}
	if response.StatusCode != http.StatusOK {
		return nil, &responseError{fmt.Sprintf("notification endpoint returned HTTP %d", response.StatusCode)}
	}
	return body, nil
}

func CheckAliyunCode(code string) error {
	if code == "OK" {
		return nil
	}
	return fmt.Errorf("Aliyun rejected notification (Code=%s)", safeCode(code))
}

func safeCode(code string) string {
	if len(code) == 0 || len(code) > 128 {
		return "missing-or-invalid"
	}
	for _, r := range code {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '.' || r == '_' || r == '-') {
			return "invalid"
		}
	}
	return code
}

// Do not log SDK error strings: some include signed requests and credentials.
func RequestError(requestCtx context.Context, err error) error {
	if requestCtx.Err() != nil {
		return requestCtx.Err()
	}
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	var responseErr *responseError
	if errors.As(err, &responseErr) {
		return responseErr
	}
	var timeout net.Error
	if errors.As(err, &timeout) && timeout.Timeout() {
		return context.DeadlineExceeded
	}
	return errors.New("notification request failed; delivery is not confirmed")
}

// Tencent's legacy bulk-SMS endpoint reports per-recipient failures in detail.
// For phone calls only the top-level result is required (expected == nil).
func CheckTencent(body []byte, expected []string) error {
	var response struct {
		Result *int `json:"result"`
		Detail []struct {
			Result *int   `json:"result"`
			Mobile string `json:"mobile"`
		} `json:"detail"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		return errors.New("invalid Tencent acknowledgement JSON")
	}
	if response.Result == nil {
		return errors.New("Tencent acknowledgement missing result")
	}
	if *response.Result != 0 {
		return fmt.Errorf("Tencent rejected notification (result=%d)", *response.Result)
	}
	if expected == nil {
		return nil
	}
	if len(response.Detail) != len(expected) {
		return errors.New("Tencent SMS acknowledgement has incomplete recipient results")
	}
	remaining := make(map[string]int, len(expected))
	for _, number := range expected {
		remaining[number]++
	}
	for _, detail := range response.Detail {
		if detail.Result == nil {
			return errors.New("Tencent SMS detail missing result")
		}
		if *detail.Result != 0 {
			return fmt.Errorf("Tencent SMS recipient rejected (result=%d)", *detail.Result)
		}
		if remaining[detail.Mobile] <= 0 {
			return errors.New("Tencent SMS acknowledgement has unexpected recipient")
		}
		remaining[detail.Mobile]--
	}
	return nil
}
