package delivery

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestTencentResultNotSuccessSubstring(t *testing.T) {
	for _, body := range []string{`{"result":1,"errmsg":"unsuccessful"}`, `{"errmsg":"success"}`, `null`, `{"result":null}`, `{"result":"0"}`, `not json`} {
		if err := CheckTencent([]byte(body), nil); err == nil {
			t.Fatal("accepted", body)
		}
	}
	if err := CheckTencent([]byte(`{"result":0,"errmsg":"accepted"}`), nil); err != nil {
		t.Fatal(err)
	}
}

func TestTencentSMSRequiresCompleteSuccessfulRecipientResults(t *testing.T) {
	for _, body := range []string{
		`{"result":0}`, `{"result":0,"detail":[]}`,
		`{"result":0,"detail":[{"result":1,"mobile":"10001"}]}`,
		`{"result":0,"detail":[{"mobile":"10001"}]}`,
		`{"result":0,"detail":[{"result":0,"mobile":"other"}]}`,
	} {
		if err := CheckTencent([]byte(body), []string{"10001"}); err == nil {
			t.Fatal("accepted", body)
		}
	}
	if err := CheckTencent([]byte(`{"result":0,"detail":[{"result":0,"mobile":"10001"}]}`), []string{"10001"}); err != nil {
		t.Fatal(err)
	}
}

func TestAliyunCodeAndErrorsDoNotExposePayload(t *testing.T) {
	if err := CheckAliyunCode("OK"); err != nil {
		t.Fatal(err)
	}
	for _, code := range []string{"", "Invalid.Template", "OK\nsecret-token", strings.Repeat("x", 129)} {
		err := CheckAliyunCode(code)
		if err == nil || strings.Contains(err.Error(), "secret-token") {
			t.Fatalf("%q: %v", code, err)
		}
	}
	if err := RequestError(context.Background(), errors.New("signed request secret-token")); strings.Contains(err.Error(), "secret-token") {
		t.Fatal(err)
	}
}

func TestSDKTransportPreservesClientTimeoutAndParentCancellation(t *testing.T) {
	for _, flush := range []bool{false, true} {
		for _, useParent := range []bool{false, true} {
			started := make(chan struct{})
			disconnected := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if flush {
					w.WriteHeader(200)
					w.(http.Flusher).Flush()
				}
				close(started)
				<-r.Context().Done()
				close(disconnected)
			}))
			ctx, cancel := context.WithCancel(context.Background())
			client := &http.Client{Transport: SDKTransport{Parent: ctx}, Timeout: 100 * time.Millisecond}
			if useParent {
				client.Timeout = 3 * time.Second
			}
			result := make(chan error, 1)
			go func() {
				response, err := client.Get(server.URL)
				if response != nil {
					response.Body.Close()
				}
				result <- err
			}()
			select {
			case <-started:
			case <-time.After(time.Second):
				cancel()
				server.Close()
				t.Fatal("request never started")
			}
			if useParent {
				cancel()
			}
			select {
			case err := <-result:
				if err == nil {
					t.Fatal("canceled request succeeded")
				}
			case <-time.After(time.Second):
				cancel()
				server.Close()
				t.Fatal("request did not stop")
			}
			select {
			case <-disconnected:
			case <-time.After(time.Second):
				t.Error("underlying request still running")
			}
			cancel()
			server.Close()
		}
	}
}

func TestSDKTransportReusesConnectionsAndLimitsBody(t *testing.T) {
	var connections atomic.Int32
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/large" {
			_, _ = io.WriteString(w, strings.Repeat("x", MaxResponseBytes+1))
			return
		}
		_, _ = io.WriteString(w, `{"Code":"OK"}`)
	}))
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			connections.Add(1)
		}
	}
	server.Start()
	defer server.Close()
	for i := 0; i < 3; i++ {
		client := &http.Client{Transport: SDKTransport{Parent: context.Background()}, Timeout: time.Second}
		response, err := client.Get(server.URL)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(response.Body)
		response.Body.Close()
		if err != nil || string(body) != `{"Code":"OK"}` {
			t.Fatalf("%s %v", body, err)
		}
	}
	if connections.Load() != 1 {
		t.Fatal("new clients did not reuse sockets", connections.Load())
	}
	client := &http.Client{Transport: SDKTransport{Parent: context.Background()}, Timeout: time.Second}
	if _, err := client.Get(server.URL + "/large"); err == nil {
		t.Fatal("unbounded body accepted")
	}
}
