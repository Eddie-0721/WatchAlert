package types

import "watchAlert/internal/models"

// RequestProcessAlertEvent 请求处理告警事件
type RequestProcessAlertEvent struct {
	TenantId      string   `json:"tenantId"`
	FaultCenterId string   `json:"faultCenterId"`
	Fingerprints  []string `json:"fingerprints"`
	Time          int64    `json:"time"`
	Username      string   `json:"username"`
}

// RequestAlertCurEventQuery 请求活跃告警事件
type RequestAlertCurEventQuery struct {
	TenantId        string `json:"tenantId" form:"tenantId"`
	RuleId          string `json:"ruleId" form:"ruleId"`
	RuleName        string `json:"ruleName" form:"ruleName"`
	DatasourceType  string `json:"datasourceType" form:"datasourceType"`
	DatasourceId    string `json:"datasourceId" form:"datasourceId"`
	Fingerprint     string `json:"fingerprint" form:"fingerprint"`
	Query           string `json:"query" form:"query"`
	Scope           int64  `json:"scope" form:"scope"`
	Severity        string `json:"severity" form:"severity"`
	FaultCenterId   string `json:"faultCenterId" form:"faultCenterId"`
	Environment     string `json:"environment" form:"environment"`
	Service         string `json:"service" form:"service"`
	Cluster         string `json:"cluster" form:"cluster"`
	Namespace       string `json:"namespace" form:"namespace"`
	Instance        string `json:"instance" form:"instance"`
	Status          string `json:"status" form:"status"`
	LifecycleStatus string `json:"lifecycleStatus" form:"lifecycleStatus"`
	Acknowledged    *bool  `json:"acknowledged" form:"acknowledged"`
	Silenced        *bool  `json:"silenced" form:"silenced"`
	IncludeRecovered bool  `json:"includeRecovered" form:"includeRecovered"`
	SortOrder       string `json:"sortOrder" form:"sortOrder"`
	models.Page
}

// AlertScope is the human-oriented identity extracted from event labels.
// Labels remain the source of truth; this view makes the common operational
// dimensions stable for alert lists, filters and response handoffs.
type AlertScope struct {
	Environment string `json:"environment"`
	Service     string `json:"service"`
	Cluster     string `json:"cluster"`
	Namespace   string `json:"namespace"`
	Resource    string `json:"resource"`
	Instance    string `json:"instance"`
	Owner       string `json:"owner"`
}

// ResponseAlertCurEvent exposes independent alert lifecycle, acknowledgement
// and suppression dimensions while keeping the legacy status field available.
type ResponseAlertCurEvent struct {
	models.AlertCurEvent
	LifecycleStatus models.AlertStatus `json:"lifecycle_status"`
	Acknowledged    bool               `json:"acknowledged"`
	Silenced        bool               `json:"silenced"`
	FaultCenterName string             `json:"faultCenterName"`
	Scope           AlertScope         `json:"scope"`
}

// ResponseAlertCurEventList 返回活跃告警列表
type ResponseAlertCurEventList struct {
	List []ResponseAlertCurEvent `json:"list"`
	models.Page
}

// RequestAlertHisEventQuery 请求查询历史事件
type RequestAlertHisEventQuery struct {
	TenantId       string `json:"tenantId" form:"tenantId"`
	DatasourceId   string `json:"datasourceId" form:"datasourceId"`
	DatasourceType string `json:"datasourceType" form:"datasourceType"`
	Fingerprint    string `json:"fingerprint" form:"fingerprint"`
	Severity       string `json:"severity" form:"severity"`
	RuleId         string `json:"ruleId" form:"ruleId"`
	RuleName       string `json:"ruleName" form:"ruleName"`
	StartAt        int64  `json:"startAt" form:"startAt"`
	EndAt          int64  `json:"endAt" form:"endAt"`
	Query          string `json:"query" form:"query"`
	FaultCenterId  string `json:"faultCenterId" form:"faultCenterId"`
	SortOrder      string `json:"sortOrder" form:"sortOrder"`
	models.Page
}

// ResponseHistoryEventList 返回历史事件列表
type ResponseHistoryEventList struct {
	List []models.AlertHisEvent `json:"list"`
	models.Page
}

// RequestAddEventComment 添加评论
type RequestAddEventComment struct {
	// 租户
	TenantId string `json:"tenantId"`
	// 故障中心
	FaultCenterId string `json:"faultCenterId"`
	// 告警指纹
	Fingerprint string `json:"fingerprint"`
	// 用户名
	Username string `json:"username"`
	// 用户 ID
	UserId string `json:"userId"`
	// 评论内容
	Content string `json:"content"`
}

// RequestDeleteEventComment 删除评论
type RequestDeleteEventComment struct {
	// 租户
	TenantId string `json:"tenantId"`
	// 评论 ID
	CommentId string `json:"commentId"`
}

// RequestListEventComments 获取评论
type RequestListEventComments struct {
	// 租户
	TenantId string `json:"tenantId" form:"tenantId"`
	// 告警指纹
	Fingerprint string `json:"fingerprint" form:"fingerprint"`
}
