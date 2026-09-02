package services

import (
	"testing"
	"watchAlert/config"
	"watchAlert/internal/models"
	"watchAlert/pkg/secretbox"
)

func TestPrepareDeepSeekModelConfigEncryptsAndPreservesKey(t *testing.T) {
	previous := config.Application.Agent.CredentialKey
	config.Application.Agent.CredentialKey = "settings-test-key"
	defer func() { config.Application.Agent.CredentialKey = previous }()

	model := models.AgentModelConfig{Provider: "deepseek", Model: "deepseek-v4-flash", APIKey: "sk-test"}
	if err := prepareAgentModelConfig(&model, models.AgentModelConfig{}); err != nil {
		t.Fatal(err)
	}
	if model.APIKey != "" || model.APIKeyEncrypted == "" || model.APIKeyEncrypted == "sk-test" {
		t.Fatalf("unexpected persisted model config: %#v", model)
	}
	plaintext, err := secretbox.Decrypt(model.APIKeyEncrypted, "settings-test-key")
	if err != nil || plaintext != "sk-test" {
		t.Fatalf("unexpected encrypted key: %q, %v", plaintext, err)
	}

	updated := models.AgentModelConfig{Provider: "deepseek", Model: "deepseek-v4-pro"}
	if err := prepareAgentModelConfig(&updated, model); err != nil {
		t.Fatal(err)
	}
	if updated.APIKeyEncrypted != model.APIKeyEncrypted {
		t.Fatal("blank browser input must retain the existing encrypted key")
	}
}
