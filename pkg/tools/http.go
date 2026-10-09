package tools

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/bytedance/sonic"
)

// Transport owns only sockets, never credentials/cookies. Keep the legacy TLS
// behavior here; enabling certificate verification requires its own rollout.
var sharedHTTPTransport = func() *http.Transport {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	t.MaxIdleConns = 100
	t.MaxIdleConnsPerHost = 10
	t.IdleConnTimeout = 90 * time.Second
	return t
}()

var freshHTTPTransport = func() *http.Transport {
	t := sharedHTTPTransport.Clone()
	t.DisableKeepAlives = true
	return t
}()

func Get(headers map[string]string, url string, timeout int) (*http.Response, error) {
	return GetContext(context.Background(), headers, url, timeout)
}

func GetContext(ctx context.Context, headers map[string]string, url string, timeout int) (*http.Response, error) {
	return doRequest(ctx, http.MethodGet, headers, url, nil, timeout, sharedHTTPTransport)
}

// Certificate probes need a new handshake, not a pooled connection's old certificate.
func GetFreshConnection(headers map[string]string, url string, timeout int) (*http.Response, error) {
	return doRequest(context.Background(), http.MethodGet, headers, url, nil, timeout, freshHTTPTransport)
}

func Post(headers map[string]string, url string, bodyReader *bytes.Reader, timeout int) (*http.Response, error) {
	return PostContext(context.Background(), headers, url, bodyReader, timeout)
}

func PostContext(ctx context.Context, headers map[string]string, url string, bodyReader *bytes.Reader, timeout int) (*http.Response, error) {
	return doRequest(ctx, http.MethodPost, headers, url, bodyReader, timeout, sharedHTTPTransport)
}

// Callers must close response.Body; reading successful bodies to EOF permits
// connection reuse. Client timeout still covers reading the returned body.
func doRequest(ctx context.Context, method string, headers map[string]string, url string, body io.Reader, timeout int, transport *http.Transport) (*http.Response, error) {
	request, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return nil, err
	}
	if method == http.MethodPost {
		request.Header.Set("Content-Type", "application/json")
	}
	for k, v := range headers {
		request.Header.Set(k, v)
	}
	if timeout <= 0 {
		timeout = 10
	}
	client := http.Client{Timeout: time.Duration(timeout) * time.Second, Transport: transport}
	return client.Do(request)
}

// CreateBasicAuthHeader 创建带认证的HTTP头
func CreateBasicAuthHeader(username, password string) map[string]string {
	headers := make(map[string]string)
	if username != "" && password != "" {
		headers["Authorization"] = "Basic " + basicAuth(username, password)
	}
	return headers
}

func basicAuth(username, password string) string {
	auth := username + ":" + password
	return base64.StdEncoding.EncodeToString([]byte(auth))
}

// MergeHeaders 合并HTTP头
func MergeHeaders(headers1, headers2 map[string]string) map[string]string {
	mergedHeaders := make(map[string]string)
	for k, v := range headers1 {
		mergedHeaders[k] = v
	}
	for k, v := range headers2 {
		mergedHeaders[k] = v
	}
	return mergedHeaders
}

// ParseReaderBody 处理请求Body
func ParseReaderBody(body io.Reader, req interface{}) error {
	newBody := body
	bodyByte, err := io.ReadAll(newBody)
	if err != nil {
		return fmt.Errorf("读取 Body 失败, err: %s", err.Error())
	}
	if err := sonic.Unmarshal(bodyByte, &req); err != nil {
		return fmt.Errorf("解析 Body 失败, body: %s, err: %s", string(bodyByte), err.Error())
	}
	return nil
}
