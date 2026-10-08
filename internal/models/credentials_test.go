package models

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestSettingsCredentialsPublicRetainReplaceClear(t *testing.T) {
	var stored Settings
	for _, value := range stored.credentialFields() {
		*value = "synthetic-secret"
	}
	stored.AgentConfig.Model.APIKeyEncrypted = "synthetic-cipher"
	public := stored.PublicSettings()
	raw, _ := json.Marshal(public)
	if strings.Contains(string(raw), "synthetic-") {
		t.Fatal("public settings leaked credentials")
	}
	for name, value := range stored.credentialFields() {
		if *value != "synthetic-secret" || !public.CredentialsSet[name] {
			t.Fatal("source mutated or missing configured state", name)
		}
	}
	if !public.AgentConfig.Model.APIKeySet {
		t.Fatal("missing agent state")
	}
	if err := public.MergeCredentials(&stored); err != nil {
		t.Fatal(err)
	}
	for name, value := range public.credentialFields() {
		if *value != "synthetic-secret" {
			t.Fatal("blank did not preserve", name)
		}
	}
	for name := range stored.credentialFields() {
		t.Run(name, func(t *testing.T) {
			next := stored.PublicSettings()
			next.ClearCredentials = []string{name}
			previous := stored
			if err := next.MergeCredentials(&previous); err != nil {
				t.Fatal(err)
			}
			if *next.credentialFields()[name] != "" {
				t.Fatal("clear failed")
			}
			if name == "agentConfig.model.apiKey" && previous.AgentConfig.Model.APIKeyEncrypted != "" {
				t.Fatal("encrypted key retained after clear")
			}
			next = stored.PublicSettings()
			*next.credentialFields()[name] = "replacement"
			if err := next.MergeCredentials(&previous); err != nil {
				t.Fatal(err)
			}
			if *next.credentialFields()[name] != "replacement" {
				t.Fatal("replace failed")
			}
			next.ClearCredentials = []string{name}
			if err := next.MergeCredentials(&previous); err == nil {
				t.Fatal("clear + replace accepted")
			}
		})
	}
	next := stored.PublicSettings()
	next.AgentConfig.Model.APIKeyEncrypted = "browser-forged"
	next.ClearCredentials = []string{"unknown"}
	if next.MergeCredentials(&stored) == nil {
		t.Fatal("unknown field accepted")
	}
	next.ClearCredentials = nil
	if err := next.MergeCredentials(&stored); err != nil {
		t.Fatal(err)
	}
	if next.AgentConfig.Model.APIKeyEncrypted != "" {
		t.Fatal("browser ciphertext accepted")
	}
}

func TestDatasourceCredentialsRoundtrip(t *testing.T) {
	var stored AlertDataSource
	for _, value := range stored.credentialFields() {
		*value = "synthetic-secret"
	}
	stored.HTTP.URL = "https://service.invalid/?token=synthetic-secret"
	stored.Write.URL = "https://user:synthetic-secret@service.invalid/write"
	stored.HTTP.Headers = map[string]string{"Authorization": "synthetic-secret", "X-Cluster": "private"}
	public := stored.PublicDatasource()
	raw, _ := json.Marshal(public)
	if strings.Contains(string(raw), "synthetic-secret") || strings.Contains(string(raw), "private") {
		t.Fatal("public datasource leaked credential")
	}
	if stored.HTTP.Headers["Authorization"] != "synthetic-secret" {
		t.Fatal("redaction mutated original headers")
	}
	if err := public.MergeCredentials(stored, nil); err != nil {
		t.Fatal(err)
	}
	if public.Auth.Pass != stored.Auth.Pass || public.HTTP.URL != stored.HTTP.URL || public.Write.URL != stored.Write.URL || public.HTTP.Headers["Authorization"] != "synthetic-secret" {
		t.Fatal("edit lost credentials")
	}
	for name := range stored.credentialFields() {
		next := stored.PublicDatasource()
		if err := next.MergeCredentials(stored, []string{name}); err != nil {
			t.Fatal(err)
		}
		if *next.credentialFields()[name] != "" {
			t.Fatal("explicit clear failed", name)
		}
	}
	next := stored.PublicDatasource()
	next.HTTP.Headers = map[string]string{"authorization": "", "X-New": "new"}
	if err := next.MergeCredentials(stored, nil); err != nil {
		t.Fatal(err)
	}
	if next.HTTP.Headers["authorization"] != "synthetic-secret" || len(next.HTTP.Headers) != 2 {
		t.Fatal("header retain/delete failed")
	}
	next = stored.PublicDatasource()
	clear := []string{"http.headers", "http.url", "write.url"}
	for name := range stored.credentialFields() {
		clear = append(clear, name)
	}
	if err := next.MergeCredentials(stored, clear); err != nil {
		t.Fatal(err)
	}
	if len(next.HTTP.Headers) != 0 || next.HTTP.URL != "" || next.Write.URL != "" {
		t.Fatal("header/url clear failed")
	}
	next.HTTP.Headers["Authorization"] = "new"
	if next.MergeCredentials(stored, []string{"http.headers"}) == nil {
		t.Fatal("conflicting header clear accepted")
	}
}

func TestRetainedCredentialsCannotBeForwardedToChangedDestination(t *testing.T) {
	stored := AlertDataSource{Type: "Prometheus"}
	stored.HTTP.URL = "https://metrics.invalid"
	stored.Auth.Pass = "secret"
	next := stored.PublicDatasource()
	next.HTTP.URL = "https://another.invalid"
	if next.MergeCredentials(stored, nil) == nil {
		t.Fatal("stored secret could be forwarded to another endpoint")
	}
	next = stored.PublicDatasource()
	next.HTTP.URL = "https://another.invalid"
	if err := next.MergeCredentials(stored, []string{"auth.pass"}); err != nil {
		t.Fatal("explicit credential clear should allow a destination change", err)
	}
}
