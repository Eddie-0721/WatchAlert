package services

import (
	"fmt"
	"testing"
	"watchAlert/internal/models"
	"watchAlert/internal/repo"
)

type scopeDatasourceRepo struct{ repo.InterDatasourceRepo }

func (scopeDatasourceRepo) GetForTenant(tenant, id string) (models.AlertDataSource, error) {
	if tenant != "a" || id != "local" {
		return models.AlertDataSource{}, fmt.Errorf("not found")
	}
	return models.AlertDataSource{TenantId: "a", ID: "local", Type: "Prometheus"}, nil
}
func TestDatasourceReferencesRequireTenantAndType(t *testing.T) {
	for _, tc := range []struct {
		tenant, kind string
		ids          []string
		want         bool
	}{
		{"a", "Prometheus", []string{"local"}, true},
		{"b", "Prometheus", []string{"local"}, false},
		{"a", "Prometheus", []string{"foreign"}, false},
		{"", "Prometheus", []string{"local"}, false},
		{"a", "Loki", []string{"local"}, false},
		{"a", "Prometheus", nil, false},
	} {
		if err := validateDatasourceReferences(scopeDatasourceRepo{}, tc.tenant, tc.kind, tc.ids); (err == nil) != tc.want {
			t.Fatal("unexpected reference validation", tc, err)
		}
	}
}
func TestDatasourceIDsRejectMalformedValues(t *testing.T) {
	for _, value := range []interface{}{nil, "local", []interface{}{3}, []interface{}{true}} {
		if _, err := datasourceIDs(value); err == nil {
			t.Fatal("malformed IDs accepted")
		}
	}
	for _, value := range []interface{}{[]string{"local"}, []interface{}{"local"}} {
		if ids, err := datasourceIDs(value); err != nil || len(ids) != 1 || ids[0] != "local" {
			t.Fatal("valid IDs rejected")
		}
	}
}
