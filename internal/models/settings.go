package models

import (
	"encoding/json"
	"strconv"
)

const (
	SettingSystemAuth = 0
	SettingLdapAuth   = 1
)

type Settings struct {
	IsInit int `json:"isInit"`
	// 0 = 系统认证，1 = LDAP 认证
	AuthType            *int                `json:"authType"`
	AppVersion          string              `json:"appVersion" gorm:"-"`
	CommunicationConfig communicationConfig `json:"communicationConfig" gorm:"communicationConfig;serializer:json"`
	AiConfig            AiConfig            `json:"aiConfig" gorm:"aiConfig;serializer:json"`
	AgentConfig         AgentConfig         `json:"agentConfig" gorm:"agentConfig;serializer:json"`
	LdapConfig          LdapConfig          `json:"ldapConfig" gorm:"ldapConfig;serializer:json"`
	OidcConfig          OidcConfig          `json:"oidcConfig" gorm:"oidcConfig;serializer:json"`
}

type communicationConfig struct {
	Email emailConfig `json:"email"`
	Phone phoneConfig `json:"phone"`
	SMS   smsConfig   `json:"sms"`
}

type emailConfig struct {
	ServerAddress string `json:"serverAddress"`
	Port          int    `json:"port"`
	Email         string `json:"email"`
	Token         string `json:"token"`
}

// UnmarshalJSON implements custom JSON unmarshaling for emailConfig
func (e *emailConfig) UnmarshalJSON(data []byte) error {
	type Alias emailConfig
	aux := &struct {
		Port interface{} `json:"port"`
		*Alias
	}{
		Alias: (*Alias)(e),
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	switch v := aux.Port.(type) {
	case string:
		if v == "" {
			e.Port = 0
		} else {
			port, err := strconv.Atoi(v)
			if err != nil {
				e.Port = 0
			} else {
				e.Port = port
			}
		}
	case float64:
		e.Port = int(v)
	case int:
		e.Port = v
	default:
		e.Port = 0
	}

	return nil
}

type phoneConfig struct {
	Provider string             `json:"provider"`
	Aliyun   aliyunPhoneConfig  `json:"aliyun"`
	Tencent  tencentPhoneConfig `json:"tencent"`
}

type aliyunPhoneConfig struct {
	AccessKeyId      string `json:"AccessKeyId"`
	AccessKeySecret  string `json:"AccessKeySecret"`
	CalledShowNumber string `json:"CalledShowNumber"`
	TtsCode          string `json:"TtsCode"`
}

type tencentPhoneConfig struct {
	SecretID  string `json:"SecretID"`
	SecretKey string `json:"SecretKey"`
	AppID     string `json:"AppID"`
}

type smsConfig struct {
	Provider string           `json:"provider"`
	Aliyun   aliyunSMSConfig  `json:"aliyun"`
	Tencent  tencentSMSConfig `json:"tencent"`
}

type aliyunSMSConfig struct {
	AccessKeyId     string `json:"AccessKeyId"`
	AccessKeySecret string `json:"AccessKeySecret"`
	SignName        string `json:"SignName"`
	TemplateCode    string `json:"TemplateCode"`
}

type tencentSMSConfig struct {
	AppKey     string `json:"AppKey"`
	SdkAppId   string `json:"SdkAppId"`
	TemplateId string `json:"TemplateId"`
	Sign       string `json:"Sign"`
}

// AiConfig ai config
type AiConfig struct {
	Enable *bool `json:"enable"`
	//Type      string `json:"type"` // OpenAi, DeepSeek
	Url       string `json:"url"`
	AppKey    string `json:"appKey"`
	Model     string `json:"model"`
	Timeout   int    `json:"timeout"`
	MaxTokens int    `json:"maxTokens"`
	Prompt    string `json:"prompt"`
}

// AgentConfig controls which capabilities are available to the Copilot. It is
// deliberately separate from AiConfig: AiConfig describes the model provider,
// while AgentConfig describes the product-level safety boundary.
type AgentConfig struct {
	Enable                   *bool      `json:"enable"`
	AllowedTools             []string   `json:"allowedTools"`
	Scope                    AgentScope `json:"scope"`
	AllowProductionWrite     *bool      `json:"allowProductionWrite"`
	RequireWriteConfirmation *bool      `json:"requireWriteConfirmation"`
}

// AgentScope is an optional server-side data boundary for Copilot. When a
// scope is configured, it is enforced by the Tool Gateway rather than trusted
// to the model or browser.
type AgentScope struct {
	DatasourceIds       []string `json:"datasourceIds"`
	EnvironmentLabelKey string   `json:"environmentLabelKey"`
	Environments        []string `json:"environments"`
}

func (a AgentConfig) GetEnable() bool {
	return a.Enable != nil && *a.Enable
}

func (a AgentConfig) GetRequireWriteConfirmation() bool {
	return a.RequireWriteConfirmation == nil || *a.RequireWriteConfirmation
}

type LdapConfig struct {
	Address         string `json:"address"`
	BaseDN          string `json:"baseDN"`
	AdminUser       string `json:"adminUser"`
	AdminPass       string `json:"adminPass"`
	DefaultTenant   string `json:"defaultTenant"`
	DefaultUserRole string `json:"defaultUserRole"`
	Cronjob         string `json:"cronjob"`
	// Filter 用于限制允许登录的用户范围，例如: (&(objectClass=person)(memberOf=cn=jms,ou=groups,dc=test,dc=com))
	Filter string `json:"filter"`
}

type OidcConfig struct {
	ClientID     string `json:"clientID"`
	ClientSecret string `json:"clientSecret"`
	UpperURI     string `json:"upperURI"`
	RedirectURI  string `json:"redirectURI"`
	Domain       string `json:"domain"`
}

func (a AiConfig) GetEnable() bool {
	if a.Enable == nil {
		return false
	}

	return *a.Enable
}
