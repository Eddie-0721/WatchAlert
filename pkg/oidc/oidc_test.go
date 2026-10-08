package oidc

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestTokenExchangeUsesServerCredential(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Error(err)
		}
		if r.Form.Get("client_secret") != "server-only-secret" || r.Form.Get("code") != "test-code" || r.Form.Get("client_id") != "public-client" {
			t.Error("server exchange credentials missing")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"access_token":"token","refresh_token":"refresh"}`))
	}))
	defer server.Close()
	token, err := GetOauthToken(server.URL, "test-code", "public-client", "server-only-secret")
	if err != nil || token.AccessToken != "token" {
		t.Fatal("token exchange failed", err)
	}
}
