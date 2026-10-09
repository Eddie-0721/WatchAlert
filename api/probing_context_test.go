package api

import (
	"context"
	"github.com/gin-gonic/gin"
	"net/http/httptest"
	"strings"
	"testing"
	"watchAlert/internal/services"
	"watchAlert/internal/types"
)

type probeControllerFixture struct {
	services.InterProbingService
	ctx    context.Context
	tenant string
}

func (p *probeControllerFixture) OnceContext(ctx context.Context, req interface{}) (interface{}, interface{}) {
	p.ctx = ctx
	return nil, ctx.Err()
}
func (p *probeControllerFixture) ChangeState(req interface{}) (interface{}, interface{}) {
	p.tenant = req.(*types.RequestProbeChangeState).TenantId
	return nil, nil
}

func TestProbeControllerForwardsCancellationAndTrustedTenant(t *testing.T) {
	previous := services.ProbingService
	defer func() { services.ProbingService = previous }()
	fixture := &probeControllerFixture{}
	services.ProbingService = fixture
	requestCtx, cancel := context.WithCancel(context.Background())
	cancel()
	ctx, _ := gin.CreateTestContext(httptest.NewRecorder())
	ctx.Request = httptest.NewRequest("POST", "/", strings.NewReader(`{}`)).WithContext(requestCtx)
	ctx.Request.Header.Set("Content-Type", "application/json")
	ProbingController.Once(ctx)
	if fixture.ctx != requestCtx {
		t.Fatal("request context discarded")
	}
	ctx, _ = gin.CreateTestContext(httptest.NewRecorder())
	ctx.Request = httptest.NewRequest("POST", "/", strings.NewReader(`{"tenantId":"untrusted","ruleId":"p","enabled":true}`))
	ctx.Request.Header.Set("Content-Type", "application/json")
	ctx.Set("TenantID", "authorized")
	ProbingController.ChangeState(ctx)
	if fixture.tenant != "authorized" {
		t.Fatal("body supplied state change tenant")
	}
}
