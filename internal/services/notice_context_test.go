package services

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	appctx "watchAlert/internal/ctx"
	"watchAlert/internal/types"
)

func TestNoticeTestPropagatesRequestCancellation(t *testing.T) {
	started := make(chan struct{})
	disconnected := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		close(started)
		<-r.Context().Done()
		close(disconnected)
	}))
	defer server.Close()
	requestCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	service := noticeService{ctx: &appctx.Context{Ctx: context.Background()}}
	result := make(chan interface{}, 1)
	go func() {
		_, err := service.Test(requestCtx, &types.RequestNoticeTest{NoticeType: "WebHook", Hook: server.URL})
		result <- err
	}()
	select {
	case <-started:
	case <-requestCtx.Done():
		t.Fatal("request did not start")
	}
	cancel()
	select {
	case err := <-result:
		e, ok := err.(error)
		if !ok || !strings.Contains(e.Error(), "context canceled") {
			t.Fatalf("got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("notice test did not cancel")
	}
	select {
	case <-disconnected:
	case <-time.After(time.Second):
		t.Fatal("notification still running")
	}
}
