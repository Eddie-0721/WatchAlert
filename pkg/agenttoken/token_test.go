package agenttoken

import (
	"testing"
	"time"
)

func TestSignVerifyAndAllow(t *testing.T) {
	secret := "test-secret"
	token, err := Sign(Claims{
		SessionId:           "as-1",
		TenantId:            "tenant-1",
		UserId:              "user-1",
		Tools:               []string{"alerts.search"},
		DatasourceIds:       []string{"prom-prod"},
		EnvironmentLabelKey: "environment",
		Environments:        []string{"production"},
		ExpiresAt:           time.Now().Add(time.Minute).Unix(),
	}, secret)
	if err != nil {
		t.Fatalf("sign failed: %v", err)
	}
	claims, err := Verify(token, secret)
	if err != nil {
		t.Fatalf("verify failed: %v", err)
	}
	if claims.TenantId != "tenant-1" || !Allows(claims, "alerts.search") || Allows(claims, "rules.get") || claims.DatasourceIds[0] != "prom-prod" || claims.Environments[0] != "production" {
		t.Fatalf("unexpected claims: %#v", claims)
	}
}

func TestVerifyRejectsInvalidOrExpiredToken(t *testing.T) {
	secret := "test-secret"
	if _, err := Verify("not-a-token", secret); err == nil {
		t.Fatal("invalid token should fail")
	}
	token, err := Sign(Claims{ExpiresAt: time.Now().Add(-time.Minute).Unix()}, secret)
	if err != nil {
		t.Fatalf("sign failed: %v", err)
	}
	if _, err := Verify(token, secret); err == nil {
		t.Fatal("expired token should fail")
	}
}
