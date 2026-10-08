package services

import (
	"fmt"
	"watchAlert/internal/repo"
)

// Validate references before saving a rule or changing evaluator state.
func validateDatasourceReferences(repository repo.InterDatasourceRepo, tenant, kind string, ids []string) error {
	if len(ids) == 0 {
		return fmt.Errorf("请选择当前租户的数据源")
	}
	for _, id := range ids {
		ds, err := repository.GetForTenant(tenant, id)
		if err != nil {
			return fmt.Errorf("数据源不存在或不属于当前租户")
		}
		if ds.Type != kind {
			return fmt.Errorf("数据源类型与规则不匹配")
		}
	}
	return nil
}

func datasourceIDs(value interface{}) ([]string, error) {
	switch values := value.(type) {
	case []string:
		return values, nil
	case []interface{}:
		ids := make([]string, 0, len(values))
		for _, value := range values {
			id, ok := value.(string)
			if !ok {
				return nil, fmt.Errorf("datasource_ids 必须是字符串数组")
			}
			ids = append(ids, id)
		}
		return ids, nil
	default:
		return nil, fmt.Errorf("datasource_ids 必须是字符串数组")
	}
}
