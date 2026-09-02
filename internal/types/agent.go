package types

import "watchAlert/internal/models"

type RequestAgentSessionCreate struct {
	Title string `json:"title"`
}

type RequestAgentSessionQuery struct {
	SessionId string `json:"sessionId" form:"sessionId"`
}

type RequestAgentSessionMessage struct {
	SessionId string                 `json:"sessionId"`
	Content   string                 `json:"content"`
	Context   map[string]interface{} `json:"context"`
}

type RequestAgentToolCall struct {
	RunToken  string                 `json:"runToken"`
	ToolName  string                 `json:"toolName"`
	Arguments map[string]interface{} `json:"arguments"`
}

type RequestAgentActionConfirm struct {
	ActionId    string `json:"actionId"`
	PayloadHash string `json:"payloadHash"`
}

type AgentCapabilities struct {
	Enabled      bool       `json:"enabled"`
	AllowedTools []string   `json:"allowedTools"`
	CanWrite     bool       `json:"canWrite"`
	Scope        AgentScope `json:"scope"`
}

type AgentScope struct {
	DatasourceIds       []string `json:"datasourceIds"`
	EnvironmentLabelKey string   `json:"environmentLabelKey"`
	Environments        []string `json:"environments"`
}

type ResponseAgentSessionDetail struct {
	Session  models.AgentSession   `json:"session"`
	Messages []models.AgentMessage `json:"messages"`
}

type AgentRunRequest struct {
	SessionId    string                 `json:"sessionId"`
	RunToken     string                 `json:"runToken"`
	Message      string                 `json:"message"`
	Messages     []models.AgentMessage  `json:"messages"`
	Context      map[string]interface{} `json:"context"`
	AllowedTools []string               `json:"allowedTools"`
	Scope        AgentScope             `json:"scope"`
}

type AgentRunResponse struct {
	Content   string                 `json:"content"`
	Evidence  string                 `json:"evidence"`
	ToolCalls []models.AgentToolCall `json:"toolCalls"`
}

// AgentStreamEvent is sent as an SSE message by the browser-facing BFF.
// Events are limited to generated text, evidence and a final persisted reply.
type AgentStreamEvent struct {
	Type     string `json:"type"`
	Delta    string `json:"delta,omitempty"`
	Content  string `json:"content,omitempty"`
	Evidence string `json:"evidence,omitempty"`
	Message  string `json:"message,omitempty"`
}
