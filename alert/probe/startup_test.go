package probe

import (
	"context"
	"errors"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
	appctx "watchAlert/internal/ctx"
	"watchAlert/internal/models"
	"watchAlert/internal/repo"
)

type probeStartupRepo struct {
	repo.InterEntryRepo
	db *gorm.DB
}

func (p probeStartupRepo) DB() *gorm.DB { return p.db }

func TestProbeStartupContinuesValidRulesAndHonorsCancellation(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	pool, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	pool.SetMaxOpenConns(1)
	if err := db.AutoMigrate(&models.ProbeRule{}); err != nil {
		t.Fatal(err)
	}
	called := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called <- struct{}{}; w.WriteHeader(204) }))
	defer server.Close()
	enabled, disabled := true, false
	valid := validProbeRule(server.URL)
	valid.RuleId, valid.Enabled = "valid", &enabled
	invalid := valid
	invalid.RuleId, invalid.ProbingEndpointConfig.Strategy.Timeout = "invalid", 0
	inactive := invalid
	inactive.RuleId, inactive.Enabled = "disabled", &disabled
	if err := db.Create(&[]models.ProbeRule{invalid, valid, inactive}).Error; err != nil {
		t.Fatal(err)
	}
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := NewProbeService(&appctx.Context{Ctx: parent, DB: probeStartupRepo{db: db}})
	if err := s.RePushRule(); err == nil || !strings.Contains(err.Error(), "invalid") {
		t.Fatal("invalid startup rule not reported", err)
	}
	s.mu.RLock()
	worker := s.workers["valid"]
	count := len(s.workers)
	s.mu.RUnlock()
	if worker == nil || count != 1 {
		t.Fatal("startup skipped valid rule or installed invalid rules", count)
	}
	defer func() { cancel(); awaitProbeDone(t, worker.done) }()
	select {
	case <-called:
	case <-time.After(time.Second):
		t.Fatal("valid startup rule did not run")
	}
	cancel()
	awaitProbeDone(t, worker.done)
	if err := s.RePushRule(); !errors.Is(err, context.Canceled) {
		t.Fatal("startup query discarded cancellation", err)
	}
}
