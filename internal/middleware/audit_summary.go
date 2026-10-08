package middleware

import (
	"encoding/json"
	"sort"
)

// Summaries intentionally store no request values. A secret-key denylist is
// insufficient for arbitrary headers, labels, templates and malformed JSON.
func auditBodySummary(body []byte) string {
	if len(body) == 0 {
		return `{"body":"empty"}`
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil || fields == nil {
		return `{"body":"omitted","reason":"not_json_object"}`
	}
	// Only known field names are retained: an arbitrary key may itself contain
	// a credential. Nested values and free-form keys are never traversed.
	known := make([]string, 0)
	for _, key := range []string{
		"id", "tenantId", "ruleId", "ruleGroupId", "faultCenterId", "datasourceId",
		"name", "ruleName", "type", "enabled", "status", "labels", "externalLabels",
		"description", "comment", "startsAt", "endsAt", "fingerprints",
		"authType", "communicationConfig", "ldapConfig", "oidcConfig", "aiConfig", "agentConfig",
		"HTTP", "http", "Auth", "auth", "kubeConfig", "dsAliCloudConfig", "awsCloudwatch",
		"sessionId", "actionId", "content", "context", "password", "pass", "apiKey",
	} {
		if _, exists := fields[key]; exists {
			known = append(known, key)
		}
	}
	sort.Strings(known)
	summary, _ := json.Marshal(struct {
		Fields     []string `json:"fields"`
		FieldCount int      `json:"fieldCount"`
		Values     string   `json:"values"`
	}{known, len(fields), "omitted"})
	return string(summary)
}
