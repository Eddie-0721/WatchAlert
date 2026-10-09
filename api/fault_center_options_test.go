package api

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"watchAlert/internal/models"
	"watchAlert/internal/services"
	"watchAlert/internal/types"

	"github.com/gin-gonic/gin"
)

type centerOptionService struct {
	services.InterFaultCenterService
	options, full int
	request       *types.RequestFaultCenterQuery
	ctx           context.Context
}

func (s *centerOptionService) ListOptions(ctx context.Context, req interface{}) (interface{}, interface{}) {
	s.options++
	s.request, s.ctx = req.(*types.RequestFaultCenterQuery), ctx
	return []models.FaultCenterOption{{ID: "fc", Name: "Production"}}, nil
}

func (s *centerOptionService) List(req interface{}) (interface{}, interface{}) {
	s.full++
	s.request = req.(*types.RequestFaultCenterQuery)
	return []models.FaultCenter{{ID: "fc", CurrentAlertNumber: 7}}, nil
}

func TestCenterOptionsControllerPreservesTenantContextAndDefault(t *testing.T) {
	original := services.FaultCenterService
	defer func() { services.FaultCenterService = original }()
	fixture := &centerOptionService{}
	services.FaultCenterService = fixture
	for _, view := range []string{"options", "", "unknown"} {
		recorder := httptest.NewRecorder()
		ctx, _ := gin.CreateTestContext(recorder)
		requestCtx, cancel := context.WithCancel(context.Background())
		ctx.Request = httptest.NewRequest("GET", "/?view="+view+"&query=payment&tenantId=untrusted", nil).WithContext(requestCtx)
		ctx.Set("TenantID", "authorized")
		FaultCenterController.List(ctx)
		cancel()
		if fixture.request.TenantId != "authorized" || fixture.request.Query != "payment" {
			t.Fatal("list must use middleware tenant and bind search", fixture.request)
		}
		if recorder.Code != 200 {
			t.Fatal(recorder.Body.String())
		}
		if view == "options" {
			if fixture.ctx != requestCtx || strings.Contains(recorder.Body.String(), "currentAlertNumber") || !strings.Contains(recorder.Body.String(), `"name":"Production"`) {
				t.Fatal("options context/shape mismatch", recorder.Body.String())
			}
		} else if !strings.Contains(recorder.Body.String(), `"currentAlertNumber":7`) {
			t.Fatal("default list lost counts", recorder.Body.String())
		}
	}
	if fixture.options != 1 || fixture.full != 2 {
		t.Fatal("unexpected routing", fixture)
	}
}
