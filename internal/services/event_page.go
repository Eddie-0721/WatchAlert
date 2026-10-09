package services

import (
	"container/heap"
	"fmt"
	"sort"
	"watchAlert/internal/models"
	"watchAlert/internal/types"
)

// Keep only the first offset+size candidates while still visiting every row for
// exact filtering, totals and facets. No event/cache object is mutated.
type currentEventPage struct {
	items []types.ResponseAlertCurEvent
	limit int
	order string
}

func newCurrentEventPage(page models.Page, order string) (*currentEventPage, int, error) {
	index, size := page.Index, page.Size
	if index <= 0 {
		index = 1
	}
	if size <= 0 {
		size = 10
	}
	maxInt := int64(int(^uint(0) >> 1))
	if size > maxInt || index > maxInt/size {
		return nil, 0, fmt.Errorf("分页范围过大，请缩小页码或每页数量")
	}
	limit := int(index * size)
	return &currentEventPage{items: make([]types.ResponseAlertCurEvent, 0, min(limit, 64)), limit: limit, order: order}, int((index - 1) * size), nil
}

func currentEventLess(a, b *types.ResponseAlertCurEvent, order string) bool {
	durA, durB := a.LastEvalTime-a.FirstTriggerTime, b.LastEvalTime-b.FirstTriggerTime
	switch order {
	case models.SortOrderASC:
		if durA != durB {
			return durA < durB
		}
	case models.SortOrderDesc:
		if durA != durB {
			return durA > durB
		}
	default:
		if a.FirstTriggerTime != b.FirstTriggerTime {
			return a.FirstTriggerTime > b.FirstTriggerTime
		}
	}
	if a.Fingerprint != b.Fingerprint {
		return a.Fingerprint < b.Fingerprint
	}
	// Identical fingerprints can exist in different centers. Resolve formerly
	// unspecified ties so repeated page requests do not shuffle equal rows.
	if a.FaultCenterId != b.FaultCenterId {
		return a.FaultCenterId < b.FaultCenterId
	}
	if a.DatasourceId != b.DatasourceId {
		return a.DatasourceId < b.DatasourceId
	}
	return a.EventId < b.EventId
}

func (p currentEventPage) Len() int { return len(p.items) }
func (p currentEventPage) Less(i, j int) bool {
	return currentEventLess(&p.items[j], &p.items[i], p.order)
}
func (p currentEventPage) Swap(i, j int) { p.items[i], p.items[j] = p.items[j], p.items[i] }
func (p *currentEventPage) Push(value interface{}) {
	p.items = append(p.items, value.(types.ResponseAlertCurEvent))
}
func (p *currentEventPage) Pop() interface{} {
	last := len(p.items) - 1
	value := p.items[last]
	p.items[last] = types.ResponseAlertCurEvent{}
	p.items = p.items[:last]
	return value
}
func (p *currentEventPage) offer(value types.ResponseAlertCurEvent) {
	if len(p.items) < p.limit {
		p.items = append(p.items, value)
		if len(p.items) == p.limit {
			heap.Init(p)
		}
		return
	}
	if currentEventLess(&value, &p.items[0], p.order) {
		p.items[0] = value
		heap.Fix(p, 0)
	}
}
func (p *currentEventPage) page(offset int) []types.ResponseAlertCurEvent {
	if offset >= len(p.items) {
		return []types.ResponseAlertCurEvent{}
	}
	sort.Slice(p.items, func(i, j int) bool { return currentEventLess(&p.items[i], &p.items[j], p.order) })
	// Do not retain previous pages' large structs behind a small returned slice.
	return append([]types.ResponseAlertCurEvent{}, p.items[offset:]...)
}
