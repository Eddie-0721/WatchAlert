package tools

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestHTTPReusesConnectionsWithoutSharingCredentials(t *testing.T) {
	var connections atomic.Int32
	s := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Set-Cookie", "private=first-user")
		if r.Header.Get("Cookie") != "" {
			t.Error("cookies leaked between calls")
		}
		_, _ = io.WriteString(w, r.Header.Get("Authorization"))
	}))
	s.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			connections.Add(1)
		}
	}
	s.Start()
	defer s.Close()
	for _, auth := range []string{"Bearer first", "Bearer second", ""} {
		res, err := Post(map[string]string{"Authorization": auth}, s.URL, bytes.NewReader([]byte("{}")), 5)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(res.Body)
		_ = res.Body.Close()
		if err != nil || string(body) != auth {
			t.Fatal("credentials changed", err)
		}
	}
	if connections.Load() != 1 {
		t.Fatal("connection pool not reused", connections.Load())
	}
	for i := 0; i < 2; i++ {
		res, err := GetFreshConnection(nil, s.URL, 5)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, res.Body)
		_ = res.Body.Close()
	}
	if connections.Load() != 3 {
		t.Fatal("fresh probe reused a connection", connections.Load())
	}
}

func TestHTTPInvalidURLAndCancellation(t *testing.T) {
	if _, err := Get(map[string]string{"Authorization": "test"}, ":bad", 5); err == nil {
		t.Fatal("invalid GET accepted")
	}
	if _, err := Post(nil, ":bad", bytes.NewReader(nil), 5); err == nil {
		t.Fatal("invalid POST accepted")
	}
	started, stopped := make(chan struct{}), make(chan struct{})
	cleanup := make(chan struct{})
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		select {
		case <-r.Context().Done():
			close(stopped)
		case <-cleanup:
		}
	}))
	defer s.Close()
	defer close(cleanup)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := GetContext(ctx, nil, s.URL, 5); done <- err }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("request not started")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("request not cancelled")
	}
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("upstream did not see cancellation")
	}
}
