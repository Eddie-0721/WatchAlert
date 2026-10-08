package models

import (
	"fmt"
	"net/url"
	"strings"
)

// Explicit field lists keep credential handling independent of JSON casing,
// reflection and client-supplied metadata. Empty means retain, never delete.
func mergeCredentials(next, previous map[string]*string, clear []string) error {
	cleared := map[string]bool{}
	for _, name := range clear {
		value, ok := next[name]
		if !ok {
			return fmt.Errorf("不支持清除该凭据字段")
		}
		if *value != "" {
			return fmt.Errorf("不能同时更新和清除凭据: %s", name)
		}
		cleared[name] = true
	}
	for name, value := range next {
		if !cleared[name] && *value == "" && previous[name] != nil {
			*value = *previous[name]
		}
	}
	return nil
}

func redactCredentials(fields map[string]*string) map[string]bool {
	state := make(map[string]bool, len(fields))
	for name, value := range fields {
		state[name] = *value != ""
		*value = ""
	}
	return state
}

func (s *Settings) credentialFields() map[string]*string {
	return map[string]*string{
		"communicationConfig.email.token":                  &s.CommunicationConfig.Email.Token,
		"communicationConfig.phone.aliyun.AccessKeyId":     &s.CommunicationConfig.Phone.Aliyun.AccessKeyId,
		"communicationConfig.phone.aliyun.AccessKeySecret": &s.CommunicationConfig.Phone.Aliyun.AccessKeySecret,
		"communicationConfig.phone.tencent.SecretID":       &s.CommunicationConfig.Phone.Tencent.SecretID,
		"communicationConfig.phone.tencent.SecretKey":      &s.CommunicationConfig.Phone.Tencent.SecretKey,
		"communicationConfig.sms.aliyun.AccessKeyId":       &s.CommunicationConfig.SMS.Aliyun.AccessKeyId,
		"communicationConfig.sms.aliyun.AccessKeySecret":   &s.CommunicationConfig.SMS.Aliyun.AccessKeySecret,
		"communicationConfig.sms.tencent.AppKey":           &s.CommunicationConfig.SMS.Tencent.AppKey,
		"aiConfig.appKey":                                  &s.AiConfig.AppKey,
		"ldapConfig.adminPass":                             &s.LdapConfig.AdminPass,
		"oidcConfig.clientSecret":                          &s.OidcConfig.ClientSecret,
		"agentConfig.model.apiKey":                         &s.AgentConfig.Model.APIKey,
	}
}

func (s Settings) PublicSettings() Settings {
	s.CredentialsSet = redactCredentials(s.credentialFields())
	s.AgentConfig.Model.APIKeySet = s.AgentConfig.Model.APIKeyEncrypted != "" || s.CredentialsSet["agentConfig.model.apiKey"]
	s.CredentialsSet["agentConfig.model.apiKey"] = s.AgentConfig.Model.APIKeySet
	s.AgentConfig.Model.APIKeyEncrypted = ""
	s.ClearCredentials = nil
	return s
}

// current is a private copy of stored settings. The encrypted Agent key is
// handled by the encryption service, never copied into the plaintext field.
func (s *Settings) MergeCredentials(current *Settings) error {
	if err := mergeCredentials(s.credentialFields(), current.credentialFields(), s.ClearCredentials); err != nil {
		return err
	}
	for _, name := range s.ClearCredentials {
		if name == "agentConfig.model.apiKey" {
			current.AgentConfig.Model.APIKeyEncrypted = ""
		}
	}
	s.AgentConfig.Model.APIKeyEncrypted = "" // Never trust browser ciphertext.
	s.CredentialsSet, s.ClearCredentials = nil, nil
	return nil
}

func sensitiveURL(raw string) bool {
	u, err := url.Parse(raw)
	return err != nil || (u != nil && (u.User != nil || u.RawQuery != "" || u.Fragment != ""))
}

func (s *AlertDataSource) credentialFields() map[string]*string {
	return map[string]*string{
		"auth.pass":                   &s.Auth.Pass,
		"dsAliCloudConfig.alicloudAk": &s.DsAliCloudConfig.AliCloudAk,
		"dsAliCloudConfig.alicloudSk": &s.DsAliCloudConfig.AliCloudSk,
		"awsCloudwatch.accessKey":     &s.AWSCloudWatch.AccessKey,
		"awsCloudwatch.secretKey":     &s.AWSCloudWatch.SecretKey,
		"kubeConfig":                  &s.KubeConfig,
	}
}

func (s AlertDataSource) PublicDatasource() AlertDataSource {
	s.CredentialsSet = redactCredentials(s.credentialFields())
	// URL userinfo/query strings can also hold credentials. Preserve them only
	// on the server; a masked URL must never be written back as the real URL.
	for name, value := range map[string]*string{"http.url": &s.HTTP.URL, "write.url": &s.Write.URL} {
		if sensitiveURL(*value) {
			s.CredentialsSet[name] = *value != ""
			*value = ""
		}
	}
	headers := make(map[string]string, len(s.HTTP.Headers))
	for key, value := range s.HTTP.Headers {
		headers[key] = ""
		if value != "" {
			s.CredentialsSet["http.headers"] = true
		}
	}
	s.HTTP.Headers = headers
	return s
}

func (s *AlertDataSource) MergeCredentials(previous AlertDataSource, clear []string) error {
	nextFields, oldFields := s.credentialFields(), previous.credentialFields()
	cleared := map[string]bool{}
	for _, name := range clear {
		cleared[name] = true
	}
	retained := false
	for name, value := range nextFields {
		if *value == "" && *oldFields[name] != "" && !cleared[name] {
			retained = true
		}
	}
	if !cleared["http.headers"] {
		if s.HTTP.Headers == nil && len(previous.HTTP.Headers) > 0 {
			retained = true
		}
		for key, value := range s.HTTP.Headers {
			for oldKey, oldValue := range previous.HTTP.Headers {
				if value == "" && strings.EqualFold(key, oldKey) && oldValue != "" {
					retained = true
				}
			}
		}
	}
	// Only credential-bearing URLs are retained when blank. Ordinary endpoint
	// changes remain ordinary configuration edits.
	for name, pair := range map[string][2]*string{"http.url": {&s.HTTP.URL, &previous.HTTP.URL}, "write.url": {&s.Write.URL, &previous.Write.URL}} {
		nextFields[name] = pair[0]
		if sensitiveURL(*pair[1]) {
			oldFields[name] = pair[1]
		}
	}
	var fields []string
	clearHeaders := false
	for _, name := range clear {
		if name == "http.headers" {
			clearHeaders = true
		} else {
			fields = append(fields, name)
		}
	}
	if err := mergeCredentials(nextFields, oldFields, fields); err != nil {
		return err
	}
	if clearHeaders {
		for _, value := range s.HTTP.Headers {
			if value != "" {
				return fmt.Errorf("不能同时更新和清除请求头")
			}
		}
		s.HTTP.Headers = map[string]string{}
	} else {
		headers := map[string]string{}
		if s.HTTP.Headers == nil {
			for key, value := range previous.HTTP.Headers {
				headers[key] = value
			}
		}
		for key, value := range s.HTTP.Headers {
			if value == "" {
				for oldKey, oldValue := range previous.HTTP.Headers {
					if strings.EqualFold(key, oldKey) {
						value = oldValue
						break
					}
				}
			}
			headers[key] = value
		}
		s.HTTP.Headers = headers
	}
	s.CredentialsSet = nil
	// Reusing a stored secret must not turn a connection test/edit into a
	// credential forwarding endpoint. A destination change requires new keys
	// or an explicit clear, not silently carrying credentials to another host.
	if retained && (s.Type != previous.Type || s.HTTP.URL != previous.HTTP.URL ||
		s.ClickHouseConfig.Addr != previous.ClickHouseConfig.Addr ||
		s.DsAliCloudConfig.AliCloudEndpoint != previous.DsAliCloudConfig.AliCloudEndpoint ||
		s.AWSCloudWatch.Region != previous.AWSCloudWatch.Region) {
		return fmt.Errorf("修改连接目标时，请重新填写或明确清除已保存的凭据和请求头")
	}
	return nil
}
