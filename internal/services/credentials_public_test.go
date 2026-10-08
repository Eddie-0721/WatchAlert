package services

import (
	"encoding/json"
	"strings"
	"testing"
	"watchAlert/internal/ctx"
	"watchAlert/internal/models"
	"watchAlert/internal/repo"
	"watchAlert/internal/types"
)

type credentialDSRepo struct {
	repo.InterDatasourceRepo
	source models.AlertDataSource
}

func (r credentialDSRepo) GetForTenant(tenant, id string) (models.AlertDataSource, error) {
	if tenant != r.source.TenantId || id != r.source.ID {
		return scopeDatasourceRepo{}.GetForTenant(tenant, id)
	}
	return r.source, nil
}
func (r credentialDSRepo) List(tenant, id, kind, query string) ([]models.AlertDataSource, error) {
	return []models.AlertDataSource{r.source}, nil
}

type credentialDSDB struct {
	repo.InterEntryRepo
	datasource repo.InterDatasourceRepo
}

func (r credentialDSDB) Datasource() repo.InterDatasourceRepo { return r.datasource }

func TestPublicSettingsAndOIDCServiceNeverReturnSecret(t *testing.T) {
	settings := models.Settings{}
	settings.OidcConfig.ClientSecret = "synthetic-oidc-secret"
	settings.OidcConfig.ClientID = "public-client"
	settings.LdapConfig.AdminPass = "synthetic-ldap-secret"
	c := &ctx.Context{DB: policyRepo{settings: &policySettings{settings: settings}}}
	for _, read := range []func() (interface{}, interface{}){(settingService{ctx: c}).Get, (oidcService{ctx: c}).GetOidcInfo} {
		value, err := read()
		if err != nil {
			t.Fatal(err)
		}
		body, _ := json.Marshal(value)
		if strings.Contains(string(body), "synthetic-") {
			t.Fatal("public service leaked stored credentials")
		}
	}
}

func TestDatasourceServiceUsesTenantBeforeSideEffects(t *testing.T) {
	stored := models.AlertDataSource{TenantId: "a", ID: "ds-a", KubeConfig: "synthetic-secret"}
	stored.Auth.Pass = "synthetic-secret"
	service := datasourceService{ctx: &ctx.Context{DB: credentialDSDB{datasource: credentialDSRepo{source: stored}}}}
	// A nil cache/persistence implementation would panic if rejected calls reached side effects.
	for _, call := range []func() (interface{}, interface{}){
		func() (interface{}, interface{}) {
			return service.Get(&types.RequestDatasourceQuery{TenantId: "b", ID: "ds-a"})
		},
		func() (interface{}, interface{}) {
			return service.Update(&types.RequestDatasourceUpdate{TenantId: "b", ID: "ds-a"})
		},
		func() (interface{}, interface{}) {
			return service.Delete(&types.RequestDatasourceQuery{TenantId: "b", ID: "ds-a"})
		},
	} {
		if _, err := call(); err == nil {
			t.Fatal("foreign datasource accepted")
		}
	}
	for _, call := range []func() (interface{}, interface{}){
		func() (interface{}, interface{}) {
			return service.Get(&types.RequestDatasourceQuery{TenantId: "a", ID: "ds-a"})
		},
		func() (interface{}, interface{}) { return service.List(&types.RequestDatasourceQuery{TenantId: "a"}) },
	} {
		value, err := call()
		if err != nil {
			t.Fatal(err)
		}
		body, _ := json.Marshal(value)
		if strings.Contains(string(body), "synthetic-secret") {
			t.Fatal("public datasource service leaked a credential")
		}
	}
}
