package agenttoken

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// Claims are intentionally limited to execution scope. They never contain a
// JWT, a password, model credentials, or any external-system credential.
type Claims struct {
	SessionId           string   `json:"sessionId"`
	TenantId            string   `json:"tenantId"`
	UserId              string   `json:"userId"`
	Tools               []string `json:"tools"`
	DatasourceIds       []string `json:"datasourceIds"`
	EnvironmentLabelKey string   `json:"environmentLabelKey"`
	Environments        []string `json:"environments"`
	ExpiresAt           int64    `json:"expiresAt"`
}

func Sign(claims Claims, secret string) (string, error) {
	if secret == "" {
		return "", fmt.Errorf("Agent internal token is not configured")
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}
	encoded := base64.RawURLEncoding.EncodeToString(payload)
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(encoded))
	signature := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return encoded + "." + signature, nil
}

func Verify(token, secret string) (Claims, error) {
	var claims Claims
	if secret == "" {
		return claims, fmt.Errorf("Agent internal token is not configured")
	}
	parts := strings.Split(token, ".")
	if len(parts) != 2 {
		return claims, fmt.Errorf("invalid Agent run token")
	}
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(parts[0]))
	expected, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil || !hmac.Equal(expected, mac.Sum(nil)) {
		return claims, fmt.Errorf("invalid Agent run token signature")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil || json.Unmarshal(payload, &claims) != nil {
		return claims, fmt.Errorf("invalid Agent run token payload")
	}
	if claims.ExpiresAt <= time.Now().Unix() {
		return claims, fmt.Errorf("Agent run token has expired")
	}
	return claims, nil
}

func Allows(claims Claims, tool string) bool {
	for _, allowed := range claims.Tools {
		if allowed == tool {
			return true
		}
	}
	return false
}
