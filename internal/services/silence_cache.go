package services

import (
	"fmt"
	"watchAlert/internal/cache"
	"watchAlert/internal/models"
	"watchAlert/internal/repo"
)

// LoadSilenceCache uses bounded, stable pages, not a one-off 1000 item limit.
// It reports partial loading rather than logging an unconditional success.
func LoadSilenceCache(repository repo.InterSilenceRepo, target cache.SilenceCacheInterface) (int, error) {
	loaded := 0
	const size = 1000
	for page := int64(1); ; page++ {
		rows, _, err := repository.List("", "", "", "all", models.Page{Index: page, Size: size})
		if err != nil {
			return loaded, fmt.Errorf("读取静默分页失败: %w", err)
		}
		for _, row := range rows {
			if err := target.PushAlertMute(row); err != nil {
				return loaded, fmt.Errorf("静默 %s 缓存同步失败", row.ID)
			}
			loaded++
		}
		if len(rows) < size {
			return loaded, nil
		}
	}
}
