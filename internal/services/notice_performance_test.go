package services

import (
	"errors"
	"testing"
	appctx "watchAlert/internal/ctx"
	"watchAlert/internal/models"
	"watchAlert/internal/repo"
	"watchAlert/internal/types"
)

type trendRecords struct {
	repo.InterNoticeRepo
	calls int
	err   error
}

func (r *trendRecords) CountRecordsByDate(tenant string, dates []string) ([]models.NoticeRecordCount, error) {
	r.calls++
	return []models.NoticeRecordCount{{Date: dates[0], Severity: "P0", Count: 3}, {Date: dates[6], Severity: "P2", Count: 2}}, r.err
}

type trendDB struct {
	repo.InterEntryRepo
	records *trendRecords
}

func (d trendDB) Notice() repo.InterNoticeRepo { return d.records }

func TestTrendPreservesSevenDayShapeAndZeroFills(t *testing.T) {
	records := &trendRecords{}
	service := noticeService{ctx: &appctx.Context{DB: trendDB{records: records}}}
	value, err := service.GetRecordMetric(&types.RequestNoticeQuery{TenantId: "t"})
	if err != nil {
		t.Fatal(err)
	}
	trend := value.(ResponseRecordMetric)
	if records.calls != 1 || len(trend.Date) != 7 || len(trend.Series.P0) != 7 || len(trend.Series.P1) != 7 || len(trend.Series.P2) != 7 {
		t.Fatal("invalid series shape", trend)
	}
	if trend.Series.P0[0] != 3 || trend.Series.P2[6] != 2 || trend.Series.P0[1] != 0 || trend.Series.P1[6] != 0 {
		t.Fatal("missing dates must be zero-filled", trend)
	}
	records.err = errors.New("database unavailable")
	if _, err = service.GetRecordMetric(&types.RequestNoticeQuery{TenantId: "t"}); err == nil {
		t.Fatal("failed query must not appear as zero counts")
	}
}
