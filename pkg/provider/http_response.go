package provider

import (
	"fmt"
	"io"
	"net/http"
)

const maxQueryBodyBytes = 8 << 20

// A failed, truncated or oversized upstream response must not become an empty
// successful evaluation (which could recover an active alert).
func readQueryBody(res *http.Response) ([]byte, error) {
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		// Do not log error bodies: they may contain credentials or sensitive logs.
		return nil, fmt.Errorf("datasource query HTTP status %d", res.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(res.Body, maxQueryBodyBytes+1))
	if err != nil {
		return nil, fmt.Errorf("incomplete datasource response: %w", err)
	}
	if len(body) > maxQueryBodyBytes {
		return nil, fmt.Errorf("datasource response exceeds %d bytes", maxQueryBodyBytes)
	}
	return body, nil
}

// Explicit connection tests do not need the entire response payload.
func closeHealthBody(res *http.Response) {
	_, _ = io.Copy(io.Discard, io.LimitReader(res.Body, 4096))
	_ = res.Body.Close()
}
