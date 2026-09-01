package services

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
	"watchAlert/alert/mute"
	"watchAlert/internal/ctx"
	"watchAlert/internal/models"
	"watchAlert/internal/types"
)

type eventService struct {
	ctx *ctx.Context
}

type InterEventService interface {
	ListCurrentEvent(req interface{}) (interface{}, interface{})
	ListHistoryEvent(req interface{}) (interface{}, interface{})
	ProcessAlertEvent(req interface{}) (interface{}, interface{})
	DeleteAlertEvent(req interface{}) (interface{}, interface{})

	ListComments(req interface{}) (interface{}, interface{})
	AddComment(req interface{}) (interface{}, interface{})
	DeleteComment(req interface{}) (interface{}, interface{})
}

func newInterEventService(ctx *ctx.Context) InterEventService {
	return &eventService{
		ctx: ctx,
	}
}

func (e eventService) ProcessAlertEvent(req interface{}) (interface{}, interface{}) {
	r := req.(*types.RequestProcessAlertEvent)

	var wg sync.WaitGroup
	wg.Add(len(r.Fingerprints))
	for _, fingerprint := range r.Fingerprints {
		go func(fingerprint string) {
			defer wg.Done()
			cache, err := e.ctx.Redis.Alert().GetEventFromCache(r.TenantId, r.FaultCenterId, fingerprint)
			if err != nil {
				return
			}

			if cache.ConfirmState.IsOk {
				return
			}

			cache.ConfirmState.IsOk = true
			cache.ConfirmState.ConfirmUsername = r.Username
			cache.ConfirmState.ConfirmActionTime = r.Time

			e.ctx.Redis.Alert().PushAlertEvent(&cache)
		}(fingerprint)
	}

	wg.Wait()
	return nil, nil
}

func (e eventService) DeleteAlertEvent(req interface{}) (interface{}, interface{}) {
	r := req.(*types.RequestProcessAlertEvent)

	var wg sync.WaitGroup
	wg.Add(len(r.Fingerprints))
	for _, fingerprint := range r.Fingerprints {
		go func(fingerprint string) {
			defer wg.Done()
			e.ctx.Redis.Alert().RemoveAlertEvent(r.TenantId, r.FaultCenterId, fingerprint)
		}(fingerprint)
	}

	wg.Wait()
	return nil, nil
}

func (e eventService) ListCurrentEvent(req interface{}) (interface{}, interface{}) {
	r, ok := req.(*types.RequestAlertCurEventQuery)
	if !ok {
		return nil, fmt.Errorf("invalid request type: expected *models.AlertCurEventQuery")
	}

	var (
		allEvents      []models.AlertCurEvent
		filteredEvents []types.ResponseAlertCurEvent
		curTime        = time.Now()
	)

	centers, err := e.ctx.DB.FaultCenter().List(r.TenantId, "")
	if err != nil {
		return nil, err
	}
	centerNames := make(map[string]string, len(centers))
	for _, center := range centers {
		centerNames[center.ID] = center.Name
		if r.FaultCenterId != "" && center.ID != r.FaultCenterId {
			continue
		}

		events, err := e.ctx.Redis.Alert().GetAllEvents(models.BuildAlertEventCacheKey(r.TenantId, center.ID))
		if err != nil {
			return nil, err
		}
		for _, alert := range events {
			allEvents = append(allEvents, *alert)
		}
	}

	var form int64
	var to int64
	if r.Scope > 0 {
		to = curTime.Unix()
		form = curTime.Add(-time.Duration(r.Scope) * 24 * time.Hour).Unix()
	}

	for _, event := range allEvents {
		if r.DatasourceType != "" && event.DatasourceType != r.DatasourceType {
			continue
		}

		if r.Severity != "" && event.Severity != r.Severity {
			continue
		}

		if r.Scope > 0 && (event.FirstTriggerTime < form || event.FirstTriggerTime > to) {
			continue
		}

		if r.FaultCenterId != "" && event.FaultCenterId != r.FaultCenterId {
			continue
		}

		if !matchQuery(event, r.Query) {
			continue
		}

		isSilenced := mute.IsSilence(mute.MuteParams{TenantId: r.TenantId, FaultCenterId: event.FaultCenterId, Labels: event.Labels})
		view := buildCurrentEventResponse(event, isSilenced)
		view.FaultCenterName = centerNames[event.FaultCenterId]
		view.Scope = buildAlertScope(event.Labels)
		if !matchCurrentEvent(view, r) {
			continue
		}

		filteredEvents = append(filteredEvents, view)
	}

	sort.Slice(filteredEvents, func(i, j int) bool {
		a, b := &filteredEvents[i], &filteredEvents[j]

		// 按持续时间降序
		durA := a.LastEvalTime - a.FirstTriggerTime
		durB := b.LastEvalTime - b.FirstTriggerTime
		switch r.SortOrder {
		case models.SortOrderASC:
			if durA != durB {
				return durA < durB // 升序
			}
		case models.SortOrderDesc:
			if durA != durB {
				return durA > durB // 降序
			}
		default:
			if a.FirstTriggerTime != b.FirstTriggerTime {
				return a.FirstTriggerTime > b.FirstTriggerTime
			}
		}

		// 默认按指纹升序
		return a.Fingerprint < b.Fingerprint
	})

	paginatedList := pageSlice(filteredEvents, int(r.Page.Index), int(r.Page.Size))
	return types.ResponseAlertCurEventList{
		List: paginatedList,
		Page: models.Page{
			Total: int64(len(filteredEvents)),
			Index: r.Page.Index,
			Size:  r.Page.Size,
		},
	}, nil
}

func matchQuery(event models.AlertCurEvent, query string) bool {
	if query == "" {
		return true
	}

	// 检查 RuleName 和 Annotations
	if strings.Contains(event.RuleName, query) || strings.Contains(event.Annotations, query) {
		return true
	}
	// 遍历 Labels
	for k, v := range event.Labels {
		if strings.Contains(k, query) || strings.Contains(fmt.Sprint(v), query) {
			return true
		}
	}
	return false
}

func buildCurrentEventResponse(event models.AlertCurEvent, silenced bool) types.ResponseAlertCurEvent {
	lifecycleStatus := event.Status
	displayStatus := lifecycleStatus
	if event.ConfirmState.IsOk {
		displayStatus = models.AlertStatus("processing")
	}
	if silenced {
		displayStatus = models.AlertStatus("muting")
	}

	view := types.ResponseAlertCurEvent{
		AlertCurEvent:   event,
		LifecycleStatus: lifecycleStatus,
		Acknowledged:    event.ConfirmState.IsOk,
		Silenced:        silenced,
	}
	// Existing clients still receive processing/muting through status, but the
	// cached lifecycle state is never overwritten.
	view.Status = displayStatus
	return view
}

func matchCurrentEvent(event types.ResponseAlertCurEvent, query *types.RequestAlertCurEventQuery) bool {
	if event.LifecycleStatus == models.StateRecovered && !query.IncludeRecovered {
		return false
	}
	if query.LifecycleStatus != "" && string(event.LifecycleStatus) != query.LifecycleStatus {
		return false
	}
	if query.Acknowledged != nil && event.Acknowledged != *query.Acknowledged {
		return false
	}
	if query.Silenced != nil && event.Silenced != *query.Silenced {
		return false
	}
	if query.Status != "" && string(event.Status) != query.Status {
		return false
	}
	if query.Environment != "" && !matchesScope(event.Scope.Environment, query.Environment) {
		return false
	}
	if query.Service != "" && !matchesScope(event.Scope.Service, query.Service) {
		return false
	}
	if query.Cluster != "" && !matchesScope(event.Scope.Cluster, query.Cluster) {
		return false
	}
	if query.Namespace != "" && !matchesScope(event.Scope.Namespace, query.Namespace) {
		return false
	}
	if query.Instance != "" && !matchesScope(event.Scope.Instance, query.Instance) {
		return false
	}
	return true
}

func matchesScope(value, query string) bool {
	return strings.EqualFold(strings.TrimSpace(value), strings.TrimSpace(query))
}

func buildAlertScope(labels map[string]interface{}) types.AlertScope {
	return types.AlertScope{
		Environment: labelValue(labels, "environment", "env", "stage", "deployment_environment"),
		Service:     labelValue(labels, "service", "app", "application", "job"),
		Cluster:     labelValue(labels, "cluster", "cluster_name", "kubernetes_cluster"),
		Namespace:   labelValue(labels, "namespace", "kubernetes_namespace", "k8s_namespace"),
		Resource:    labelValue(labels, "resource_name", "resource", "pod", "node", "host", "instance"),
		Instance:    labelValue(labels, "instance", "pod", "node", "host", "endpoint"),
		Owner:       labelValue(labels, "owner", "team", "service_owner"),
	}
}

func labelValue(labels map[string]interface{}, aliases ...string) string {
	if len(labels) == 0 {
		return ""
	}
	for _, alias := range aliases {
		for key, value := range labels {
			if strings.EqualFold(key, alias) {
				return strings.TrimSpace(fmt.Sprint(value))
			}
		}
	}
	return ""
}

func (e eventService) ListHistoryEvent(req interface{}) (interface{}, interface{}) {
	r := req.(*types.RequestAlertHisEventQuery)
	data, err := e.ctx.DB.Event().GetHistoryEvent(*r)
	if err != nil {
		return nil, err
	}

	return data, err

}

func pageSlice(data []types.ResponseAlertCurEvent, index, size int) []types.ResponseAlertCurEvent {
	if index <= 0 {
		index = 1
	}

	if size <= 0 {
		size = 10
	}

	total := len(data)
	if total == 0 {
		return []types.ResponseAlertCurEvent{}
	}

	offset := (index - 1) * size
	if offset >= total {
		return []types.ResponseAlertCurEvent{}
	}

	limit := index * size
	if limit > total {
		limit = total
	}

	return data[offset:limit]
}

func (e eventService) ListComments(req interface{}) (interface{}, interface{}) {
	r := req.(*types.RequestListEventComments)
	comment := e.ctx.DB.Comment()
	data, err := comment.List(*r)
	if err != nil {
		return nil, fmt.Errorf("获取评论失败, %s", err.Error())
	}

	return data, nil
}

func (e eventService) AddComment(req interface{}) (interface{}, interface{}) {
	r := req.(*types.RequestAddEventComment)
	comment := e.ctx.DB.Comment()
	err := comment.Add(*r)
	if err != nil {
		return nil, fmt.Errorf("评论失败, %s", err.Error())
	}

	return "评论成功", nil
}

func (e eventService) DeleteComment(req interface{}) (interface{}, interface{}) {
	r := req.(*types.RequestDeleteEventComment)
	comment := e.ctx.DB.Comment()
	err := comment.Delete(*r)
	if err != nil {
		return nil, fmt.Errorf("删除评论失败, %s", err.Error())
	}

	return "删除评论成功", nil
}
