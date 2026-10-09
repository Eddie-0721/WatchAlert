package services

import (
	"context"
	"errors"
	"fmt"
	"testing"
	appctx "watchAlert/internal/ctx"
	"watchAlert/internal/models"
	"watchAlert/internal/repo"
	"watchAlert/internal/types"
)

type optionCenters struct {
	repo.InterFaultCenterRepo
	err error
}

func (r optionCenters) List(string, string) ([]models.FaultCenter, error) {
	return []models.FaultCenter{{TenantId: "t", ID: "fc", Name: "Production"}, {TenantId: "t", ID: "fc2", Name: "Staging"}}, r.err
}

func (r optionCenters) ListOptions(ctx context.Context, tenant, query string) ([]models.FaultCenterOption, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if r.err != nil {
		return nil, r.err
	}
	return []models.FaultCenterOption{{ID: "fc", Name: "Production"}, {ID: "fc2", Name: "Staging"}}, nil
}

type optionDB struct {
	repo.InterEntryRepo
	centers optionCenters
}

func (d optionDB) FaultCenter() repo.InterFaultCenterRepo { return d.centers }

func TestCenterOptionsSkipEventsAndPreserveFullCounts(t *testing.T) {
	events, _, server := redisEventFixture(t, 1000)
	service := faultCenterService{ctx: &appctx.Context{DB: optionDB{}, Redis: events.ctx.Redis}}
	r := &types.RequestFaultCenterQuery{TenantId: "t"}
	before := server.CommandCount()
	data, err := service.List(r)
	if err != nil || server.CommandCount()-before != 2 {
		t.Fatal("full list must still read both centers", data, err)
	}
	for _, center := range data.([]models.FaultCenter) {
		if center.CurrentAlertNumber == 0 || center.CurrentPreAlertNumber == 0 {
			t.Fatal("management counts were lost", center)
		}
	}
	before = server.CommandCount()
	data, err = service.ListOptions(context.Background(), r)
	if err != nil || len(data.([]models.FaultCenterOption)) != 2 || server.CommandCount() != before {
		t.Fatal("options must not touch event cache", data, err)
	}
	server.SetError("cache unavailable")
	if _, err = service.List(r); err == nil {
		t.Fatal("full statistics must propagate cache errors, not return zero")
	}
	if _, err = service.ListOptions(context.Background(), r); err != nil {
		t.Fatal("identity-only options must not depend on event storage", err)
	}
	service.ctx.DB = optionDB{centers: optionCenters{err: errors.New("SQL unavailable")}}
	if data, err := service.ListOptions(context.Background(), r); data != nil || err == nil {
		t.Fatal("options SQL failure must not become an empty successful response", data, err)
	}
	requestCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if data, err := service.ListOptions(requestCtx, r); data != nil || !errors.Is(err.(error), context.Canceled) {
		t.Fatal("canceled option request succeeded", data, err)
	}
}

// Real cache adapter/miniredis; SQL fixture. Compare the old selector call with
// its identity-only replacement, not management statistics (which are retained).
func BenchmarkCenterSelector(b *testing.B) {
	for _, n := range []int{1000, 10000} {
		events, _, _ := redisEventFixture(b, n)
		service := faultCenterService{ctx: &appctx.Context{DB: optionDB{}, Redis: events.ctx.Redis}}
		request := &types.RequestFaultCenterQuery{TenantId: "t"}
		for _, options := range []bool{false, true} {
			b.Run(fmt.Sprintf("events=%d/options=%t", n, options), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					var err interface{}
					if options {
						_, err = service.ListOptions(context.Background(), request)
					} else {
						_, err = service.List(request)
					}
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
