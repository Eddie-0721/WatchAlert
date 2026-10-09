package repo

import (
	"fmt"
	"watchAlert/internal/models"
)

// History screens use finite pages; internal statistics use aggregate queries.
func historyPage(page models.Page, maxSize int64) (models.Page, error) {
	if page.Index <= 0 {
		page.Index = 1
	}
	if page.Size <= 0 {
		page.Size = 20
	}
	if page.Size > maxSize {
		return page, fmt.Errorf("历史列表每次最多 %d 条", maxSize)
	}
	if page.Index > int64(^uint(0)>>1)/page.Size {
		return page, fmt.Errorf("分页参数过大")
	}
	return page, nil
}
