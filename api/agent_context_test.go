package api

import (
	"context"
	"net/http/httptest"
	"testing"
	"watchAlert/internal/services"
	"watchAlert/internal/types"

	"github.com/gin-gonic/gin"
)

type agentCapabilityContextFixture struct {
	services.InterAgentService
	ctx          context.Context
	tenant, user string
}

func (s *agentCapabilityContextFixture) CapabilitiesContext(ctx context.Context, tenant, user string) (types.AgentCapabilities, error) {
	s.ctx, s.tenant, s.user = ctx, tenant, user
	return types.AgentCapabilities{}, ctx.Err()
}

func TestAgentCapabilitiesPassRequestContextAndTrustedScope(t *testing.T) {
	previous := services.AgentService
	defer func() { services.AgentService = previous }()
	fixture := &agentCapabilityContextFixture{}
	services.AgentService = fixture
	requestCtx, cancel := context.WithCancel(context.Background())
	cancel()
	ctx, _ := gin.CreateTestContext(httptest.NewRecorder())
	ctx.Request = httptest.NewRequest("GET", "/?tenantId=untrusted&userId=admin", nil).WithContext(requestCtx)
	ctx.Set("TenantID", "authorized")
	ctx.Set("UserId", "member")
	AgentController.Capabilities(ctx)
	if fixture.ctx != requestCtx || fixture.tenant != "authorized" || fixture.user != "member" {
		t.Fatal("capabilities lost cancellation or trusted identity", fixture)
	}
}
