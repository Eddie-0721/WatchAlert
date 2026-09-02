package models

// AgentSession, AgentMessage and AgentToolCall are intentionally small. The
// system stores evidence and execution metadata rather than hidden model
// reasoning, so every user-visible conclusion can be traced to a tool call.
type AgentSession struct {
	ID        string `json:"id" gorm:"primaryKey;size:64"`
	TenantId  string `json:"tenantId" gorm:"index;size:64"`
	UserId    string `json:"userId" gorm:"index;size:64"`
	Title     string `json:"title" gorm:"size:255"`
	Status    string `json:"status" gorm:"size:32"`
	Summary   string `json:"summary" gorm:"type:text"`
	CreatedAt int64  `json:"createdAt" gorm:"index"`
	UpdatedAt int64  `json:"updatedAt" gorm:"index"`
}

func (AgentSession) TableName() string { return "w8t_agent_sessions" }

type AgentMessage struct {
	ID        string `json:"id" gorm:"primaryKey;size:64"`
	SessionId string `json:"sessionId" gorm:"index;size:64"`
	TenantId  string `json:"tenantId" gorm:"index;size:64"`
	Role      string `json:"role" gorm:"size:24"`
	Content   string `json:"content" gorm:"type:text"`
	Evidence  string `json:"evidence" gorm:"type:text"`
	CreatedAt int64  `json:"createdAt" gorm:"index"`
}

func (AgentMessage) TableName() string { return "w8t_agent_messages" }

type AgentToolCall struct {
	ID         string `json:"id" gorm:"primaryKey;size:64"`
	SessionId  string `json:"sessionId" gorm:"index;size:64"`
	TenantId   string `json:"tenantId" gorm:"index;size:64"`
	UserId     string `json:"userId" gorm:"index;size:64"`
	ToolName   string `json:"toolName" gorm:"size:128"`
	Operation  string `json:"operation" gorm:"size:16"`
	Status     string `json:"status" gorm:"size:24"`
	Input      string `json:"input" gorm:"type:text"`
	Result     string `json:"result" gorm:"type:text"`
	Error      string `json:"error" gorm:"type:text"`
	DurationMs int64  `json:"durationMs"`
	CreatedAt  int64  `json:"createdAt" gorm:"index"`
}

func (AgentToolCall) TableName() string { return "w8t_agent_tool_calls" }

type AgentPendingAction struct {
	ID          string `json:"id" gorm:"primaryKey;size:64"`
	TenantId    string `json:"tenantId" gorm:"index;size:64"`
	UserId      string `json:"userId" gorm:"index;size:64"`
	SessionId   string `json:"sessionId" gorm:"index;size:64"`
	ActionType  string `json:"actionType" gorm:"size:64"`
	Status      string `json:"status" gorm:"size:24"`
	RiskLevel   string `json:"riskLevel" gorm:"size:24"`
	Payload     string `json:"payload" gorm:"type:text"`
	PayloadHash string `json:"payloadHash" gorm:"size:128"`
	Preview     string `json:"preview" gorm:"type:text"`
	Result      string `json:"result" gorm:"type:text"`
	ExpiresAt   int64  `json:"expiresAt" gorm:"index"`
	ConfirmedAt int64  `json:"confirmedAt"`
	CreatedAt   int64  `json:"createdAt" gorm:"index"`
	UpdatedAt   int64  `json:"updatedAt"`
}

func (AgentPendingAction) TableName() string { return "w8t_agent_pending_actions" }
