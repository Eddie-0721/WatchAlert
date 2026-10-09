package consumer

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
	"watchAlert/alert/process"
	"watchAlert/internal/ctx"
	"watchAlert/internal/models"
	mediums "watchAlert/pkg/medium"
	"watchAlert/pkg/templates"
	"watchAlert/pkg/tools"

	"github.com/zeromicro/go-zero/core/logc"
	"golang.org/x/sync/errgroup"
)

// handleAlert 处理告警逻辑
func handleAlert(requestCtx context.Context, ctx *ctx.Context, processType string, faultCenter models.FaultCenter, noticeId string, alerts []*models.AlertCurEvent) error {
	if err := requestCtx.Err(); err != nil {
		return err
	}
	g := new(errgroup.Group)

	// 获取通知对象详细信息
	noticeData, err := getNoticeData(ctx, faultCenter.TenantId, noticeId)
	if err != nil {
		logc.Error(ctx.Ctx, fmt.Sprintf("Failed to get notice data: %v", err))
		return err
	}

	// 按告警等级分组
	severityGroups := make(map[string][]*models.AlertCurEvent)
	for _, alert := range alerts {
		if err := requestCtx.Err(); err != nil {
			return err
		}
		if alert == nil || alert.Fingerprint == "" {
			continue
		}
		severityGroups[alert.Severity] = append(severityGroups[alert.Severity], alert)
	}

	for severity, events := range severityGroups {
		g.Go(func() error {
			// Retain one failure, not one allocated error per event in a flood.
			var firstSendError error
			if events == nil {
				return nil
			}

			// 获取当前事件等级对应的路由配置
			routes := getNoticeRoutes(noticeData, severity)
			groupSize := 1
			if processType == "upgrade" || (processType == "alarm" && faultCenter.GetAlarmAggregationType() == "Rule") {
				groupSize = len(events)
			}
			for start := 0; start < len(events); start += groupSize {
				members := events[start:min(start+groupSize, len(events))]
				if len(routes) == 0 {
					continue
				}
				for _, route := range routes {
					if err := requestCtx.Err(); err != nil {
						return errors.Join(firstSendError, err)
					}
					// Re-read after each potentially slow route. Never carry a
					// silence decision across external notification calls.
					ready, err := prepareNotificationMembers(requestCtx, ctx, processType, faultCenter, members)
					if err != nil {
						if firstSendError == nil {
							firstSendError = err
						}
						// A partial clock failure may leave other valid members.
					}
					if len(ready) == 0 {
						continue
					}
					event := aggregateNotificationMembers(processType, ready)
					// 设置值班用户信息
					dutyUsers := getDutyUsers(ctx, noticeData, route.NoticeType)
					event.DutyUser = strings.Join(dutyUsers, " ")

					// 生成告警内容
					content := generateAlertContent(ctx, event, noticeData, route)

					// 构建邮件信息
					email := models.Email{
						Subject: route.Subject,
						To:      slices.Clone(route.To),
						CC:      slices.Clone(route.CC),
					}

					phone := models.Phone{
						To: slices.Clone(route.To),
					}

					sms := models.SMS{
						To: slices.Clone(route.To),
					}

					if len(dutyUsers) > 0 {
						switch route.NoticeType {
						case "Phone":
							phone.To = append(phone.To, dutyUsers...)
						case "SMS":
							sms.To = append(sms.To, dutyUsers...)
						case "Email":
							email.To = append(email.To, dutyUsers...)
						}
					}

					// 发送告警
					err = mediums.Sender(ctx, mediums.SendParams{
						RequestContext: requestCtx,
						TenantId:       event.TenantId,
						EventId:        event.EventId,
						RuleName:       event.RuleName,
						Severity:       event.Severity,
						NoticeType:     route.NoticeType,
						NoticeId:       noticeId,
						NoticeName:     noticeData.Name,
						IsRecovered:    event.IsRecovered,
						Hook:           route.Hook,
						Headers:        route.Headers,
						Email:          email,
						Phone:          phone,
						SMS:            sms,
						Content:        content,
						Sign:           route.Sign,
					})
					if err != nil {
						logc.Error(ctx.Ctx, fmt.Sprintf("Failed to send alert: %v", err))
						if firstSendError == nil {
							firstSendError = err
						}
					}
				}
			}

			return firstSendError
		})
	}

	return g.Wait()
}

// Aggregation changes only a local payload, not source cache events.
func aggregateNotificationMembers(processType string, alerts []*models.AlertCurEvent) *models.AlertCurEvent {
	event := *alerts[0]
	if len(alerts) > 1 {
		if processType == "upgrade" {
			event.Annotations += getContent(len(alerts))
		} else {
			event.Annotations += fmt.Sprintf("\n聚合 %d 条消息，详情请前往 WatchAlert 查看\n", len(alerts))
		}
	}
	return &event
}

// getNoticeData 获取 Notice 数据
func getNoticeData(ctx *ctx.Context, tenantId, noticeId string) (models.AlertNotice, error) {
	return ctx.DB.Notice().Get(tenantId, noticeId)
}

// getNoticeRoutes 获取事件等级对应的路由配置
func getNoticeRoutes(notice models.AlertNotice, severity string) []models.Route {
	var routes []models.Route
	if notice.Routes != nil {
		for i, route := range notice.Routes {
			if process.NotInTheEffectiveTime(route.EffectiveTime) {
				logc.Infof(ctx.Ctx, "Notice %v route [%v] is not in effective time", notice.Name, i+1)
				continue
			}
			if slices.Contains(route.Severitys, severity) {
				routes = append(routes, route)
			}
		}
	}

	return routes
}

type WebhookContent struct {
	Alarm     *models.AlertCurEvent `json:"alarm"`
	DutyUsers []models.DutyUser     `json:"dutyUsers"`
}

// generateAlertContent 生成告警内容
func generateAlertContent(ctx *ctx.Context, alert *models.AlertCurEvent, noticeData models.AlertNotice, route models.Route) string {
	if route.NoticeType == "WebHook" || route.NoticeType == "SREFlow" {
		users, ok := ctx.DB.DutyCalendar().GetDutyUserData(*noticeData.GetDutyId(), time.Now().Format("2006-1-2"))
		if !ok || len(users) == 0 {
			logc.Error(ctx.Ctx, "Failed to get duty users, noticeName: ", noticeData.Name)
		}

		var dutyUsers = []models.DutyUser{}
		for _, user := range users {
			dutyUsers = append(dutyUsers, models.DutyUser{
				Email:    user.Email,
				Mobile:   user.Phone,
				UserId:   user.UserId,
				Username: user.UserName,
			})
		}
		content := WebhookContent{
			Alarm:     alert,
			DutyUsers: dutyUsers,
		}

		return tools.JsonMarshalToString(content)
	}

	template, err := templates.NewTemplate(ctx, *alert, route)
	if err != nil {
		logc.Error(ctx.Ctx, fmt.Sprintf("Failed to create template: %v", err))
		return ""
	}
	return template.CardContentMsg
}

func getDutyUsers(ctx *ctx.Context, noticeData models.AlertNotice, noticeType string) []string {
	var us []string
	users, ok := ctx.DB.DutyCalendar().GetDutyUserData(*noticeData.GetDutyId(), time.Now().Format("2006-1-2"))
	if ok {
		switch noticeType {
		case "FeiShu":
			for _, user := range users {
				us = append(us, fmt.Sprintf("<at id=%s></at>", user.DutyUserId))
			}
			return us
		case "DingDing":
			for _, user := range users {
				us = append(us, fmt.Sprintf("@%s", user.DutyUserId))
			}
			return us
		case "Email", "WeChat", "WebHook", "SREFlow":
			for _, user := range users {
				us = append(us, fmt.Sprintf("@%s", user.UserName))
			}
			return us
		case "Slack":
			for _, user := range users {
				us = append(us, fmt.Sprintf("<@%s>", user.DutyUserId))
			}
			return us
		case "Phone", "SMS":
			for _, user := range users {
				if user.Phone == "" {
					continue
				}
				us = append(us, user.Phone)
			}
			return us
		}
	}

	return []string{"暂无"}
}
