package medium

import (
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestNotificationHTTPResponsesReleaseReusableConnections(t *testing.T) {
	for _, tc := range []struct {
		name, response string
		send           func(string) error
	}{
		{"webhook", "accepted", func(url string) error { return (&WebHookSender{}).post(url, nil, "{}") }},
		{"sreflow", "accepted", func(url string) error { return (&SREFlowSender{}).post(url, nil, "{}") }},
		{"slack", "ok", func(url string) error { return (&SlackSender{}).post(url, "{}") }},
		{"wechat", `{"errcode":0}`, func(url string) error { return (&WeChatSender{}).post(url, "{}") }},
		{"dingding", `{"errcode":0}`, func(url string) error { return (&DingDingSender{}).post(url, "", "{}") }},
		{"feishu", `{"code":0}`, func(url string) error { return (&FeiShuSender{}).post(url, "", map[string]any{"text": "test"}) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var connections atomic.Int32
			s := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				_, _ = io.WriteString(w, tc.response)
			}))
			s.Config.ConnState = func(_ net.Conn, state http.ConnState) {
				if state == http.StateNew {
					connections.Add(1)
				}
			}
			s.Start()
			defer s.Close()
			for i := 0; i < 3; i++ {
				if err := tc.send(s.URL); err != nil {
					t.Fatal(err)
				}
			}
			if connections.Load() != 1 {
				t.Fatal("connection not reused", connections.Load())
			}
		})
	}
}
