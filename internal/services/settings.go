package services

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"watchAlert/config"
	"watchAlert/internal/ctx"
	"watchAlert/internal/models"
	"watchAlert/pkg/ai"
	"watchAlert/pkg/secretbox"
)

type (
	settingService struct {
		ctx *ctx.Context
	}

	InterSettingService interface {
		Save(req interface{}) (interface{}, interface{})
		Get() (interface{}, interface{})
	}
)

func newInterSettingService(ctx *ctx.Context) InterSettingService {
	return settingService{
		ctx: ctx,
	}
}

func (a settingService) Save(req interface{}) (interface{}, interface{}) {
	r := req.(*models.Settings)
	var current models.Settings
	if a.ctx.DB.Setting().Check() {
		var err error
		current, err = a.ctx.DB.Setting().Get()
		if err != nil {
			return nil, err
		}
	}
	if err := prepareAgentModelConfig(&r.AgentConfig.Model, current.AgentConfig.Model); err != nil {
		return nil, err
	}

	if a.ctx.DB.Setting().Check() {
		err := a.ctx.DB.Setting().Update(*r)
		if err != nil {
			return nil, err
		}
	} else {
		err := a.ctx.DB.Setting().Create(*r)
		if err != nil {
			return nil, err
		}
	}

	const mark = "SyncLdapUserJob"
	if cancel, exists := a.ctx.ContextMap[mark]; exists {
		cancel()
		delete(a.ctx.ContextMap, mark)
	}

	if r.AuthType != nil && *r.AuthType == models.SettingLdapAuth {
		c, cancel := context.WithCancel(context.Background())
		a.ctx.ContextMap[mark] = cancel
		go LdapService.SyncUsersCronjob(c, r.LdapConfig)
	}

	if r.AiConfig.GetEnable() {
		client, err := ai.NewAiClient(&r.AiConfig)
		if err != nil {
			return nil, err
		}
		a.ctx.Redis.ProviderPools().SetClient("AiClient", client)
	}

	return nil, nil
}

func (a settingService) Get() (interface{}, interface{}) {
	get, err := a.ctx.DB.Setting().Get()
	if err != nil {
		return nil, err
	}
	get.AppVersion = config.Version
	get.AgentConfig.Model.APIKeySet = get.AgentConfig.Model.APIKeyEncrypted != ""
	get.AgentConfig.Model.APIKey = ""
	get.AgentConfig.Model.APIKeyEncrypted = ""

	return get, nil
}

func prepareAgentModelConfig(model *models.AgentModelConfig, current models.AgentModelConfig) error {
	model.Provider = strings.ToLower(strings.TrimSpace(model.Provider))
	if model.Provider == "" && (model.Model != "" || model.BaseURL != "" || model.APIKey != "" || current.APIKeyEncrypted != "") {
		model.Provider = "deepseek"
	}
	if model.Provider == "" {
		model.APIKeyEncrypted = current.APIKeyEncrypted
		return nil
	}
	if model.Provider != "deepseek" {
		return fmt.Errorf("当前仅支持 DeepSeek 模型供应商")
	}
	if model.BaseURL == "" {
		model.BaseURL = "https://api.deepseek.com"
	}
	endpoint, err := url.Parse(model.BaseURL)
	if err != nil || endpoint.Scheme != "https" || endpoint.Host != "api.deepseek.com" {
		return fmt.Errorf("DeepSeek 接口地址必须为 https://api.deepseek.com")
	}
	model.BaseURL = strings.TrimRight(model.BaseURL, "/")
	if model.Model == "" {
		model.Model = "deepseek-v4-flash"
	}
	if model.Model != "deepseek-v4-flash" && model.Model != "deepseek-v4-pro" {
		return fmt.Errorf("不支持的 DeepSeek 模型")
	}
	if model.APIKey != "" {
		ciphertext, err := secretbox.Encrypt(strings.TrimSpace(model.APIKey), config.Application.Agent.CredentialKey)
		if err != nil {
			return err
		}
		model.APIKeyEncrypted = ciphertext
	} else {
		model.APIKeyEncrypted = current.APIKeyEncrypted
	}
	model.APIKey = ""
	model.APIKeySet = false
	return nil
}
