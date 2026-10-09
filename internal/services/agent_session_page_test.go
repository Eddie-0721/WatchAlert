package services

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
	appctx "watchAlert/internal/ctx"
	"watchAlert/internal/models"
	"watchAlert/internal/types"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

func pageService(t *testing.T) (*agentService, *gorm.DB) {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	pool, _ := db.DB()
	pool.SetMaxOpenConns(1)
	t.Cleanup(func() { pool.Close() })
	if err = db.AutoMigrate(&models.AgentSession{}, &models.AgentMessage{}, &models.AgentPendingAction{}); err != nil {
		t.Fatal(err)
	}
	if err = db.Create(&models.AgentSession{ID: "s", TenantId: "t", UserId: "u"}).Error; err != nil {
		t.Fatal(err)
	}
	return &agentService{ctx: &appctx.Context{DB: policyRepo{db: db}}}, db
}

func TestSessionPageBoundariesAndConcurrentAppend(t *testing.T) {
	service, db := pageService(t)
	for i := 0; i < 123; i++ {
		// Deliberately share timestamps across page boundaries.
		row := models.AgentMessage{ID: fmt.Sprintf("m%03d", i), SessionId: "s", TenantId: "t", CreatedAt: 100 + int64(i/100), Content: "message"}
		if err := db.Create(&row).Error; err != nil {
			t.Fatal(err)
		}
	}
	query := &types.RequestAgentSessionQuery{SessionId: "s"}
	seen := map[string]bool{}
	for page := 0; page < 3; page++ {
		result, err := service.GetSession(context.Background(), "t", "u", query)
		if err != nil {
			t.Fatal(err)
		}
		want := 50
		if page == 2 {
			want = 23
		}
		if len(result.Messages) != want || result.HasMore != (page < 2) {
			t.Fatalf("page %d: count=%d more=%v", page, len(result.Messages), result.HasMore)
		}
		for i, row := range result.Messages {
			if seen[row.ID] {
				t.Fatalf("duplicate %s", row.ID)
			}
			if i > 0 && result.Messages[i-1].ID >= row.ID {
				t.Fatal("not chronological")
			}
			seen[row.ID] = true
		}
		query.Before = result.NextCursor
		if page == 0 {
			if result.Messages[0].ID != "m073" || result.Messages[49].ID != "m122" {
				t.Fatal("not newest page")
			}
			if err := db.Create(&models.AgentMessage{ID: "new", SessionId: "s", TenantId: "t", CreatedAt: 200}).Error; err != nil {
				t.Fatal(err)
			}
		}
	}
	if len(seen) != 123 || seen["new"] {
		t.Fatal("cursor skipped history or included newer append")
	}
	first, err := service.GetSession(context.Background(), "t", "admin", &types.RequestAgentSessionQuery{SessionId: "s", Limit: 100})
	if err != nil || len(first.Messages) != 100 {
		t.Fatal("admin/max page", err)
	}
}

func TestSessionPageValidationIsolationCancellation(t *testing.T) {
	service, _ := pageService(t)
	for _, scope := range [][2]string{{"other", "u"}, {"t", "other"}, {"other", "admin"}} {
		if _, err := service.GetSession(context.Background(), scope[0], scope[1], &types.RequestAgentSessionQuery{SessionId: "s"}); err == nil {
			t.Fatal("scope bypass")
		}
	}
	wrongSession, _ := json.Marshal(agentMessageCursor{Session: "other", ID: "m", Time: 100})
	for _, req := range []*types.RequestAgentSessionQuery{nil, {}, {SessionId: "s", Limit: -1}, {SessionId: "s", Limit: 101}, {SessionId: "s", Before: "invalid"}, {SessionId: "s", Before: strings.Repeat("a", 513)}, {SessionId: "s", Before: base64.RawURLEncoding.EncodeToString(wrongSession)}} {
		if _, err := service.GetSession(context.Background(), "t", "u", req); err == nil {
			t.Fatal("invalid input accepted", req)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := service.GetSession(ctx, "t", "u", &types.RequestAgentSessionQuery{SessionId: "s"}); err == nil {
		t.Fatal("cancel ignored")
	}
	empty, err := service.GetSession(context.Background(), "t", "u", &types.RequestAgentSessionQuery{SessionId: "s"})
	if err != nil || empty.Messages == nil || len(empty.Messages) != 0 || empty.HasMore || empty.NextCursor != "" {
		t.Fatal("empty page", empty, err)
	}
}

func TestSessionPageOnlyLoadsReferencedActionStatuses(t *testing.T) {
	service, db := pageService(t)
	rows := []models.AgentMessage{
		{ID: "old", SessionId: "s", TenantId: "t", CreatedAt: 1, Evidence: `[{"actionId":"old-action"}]`},
		{ID: "new", SessionId: "s", TenantId: "t", CreatedAt: 2, Evidence: `[{"actionId":"a","status":"pending_confirmation"},{"actionId":"expired"},{"actionId":"foreign","status":"unchanged"}]`},
	}
	if err := db.Create(&rows).Error; err != nil {
		t.Fatal(err)
	}
	actions := []models.AgentPendingAction{
		{ID: "old-action", TenantId: "t", SessionId: "s", Status: "executed", Payload: strings.Repeat("large", 10000)},
		{ID: "a", TenantId: "t", SessionId: "s", Status: "executed"},
		{ID: "expired", TenantId: "t", SessionId: "s", Status: "pending_confirmation", ExpiresAt: time.Now().Unix() - 1},
		{ID: "foreign", TenantId: "other", SessionId: "s", Status: "executed"},
	}
	if err := db.Create(&actions).Error; err != nil {
		t.Fatal(err)
	}
	var queries []string
	if err := db.Callback().Query().After("gorm:query").Register("test:page_sql", func(tx *gorm.DB) {
		queries = append(queries, tx.Dialector.Explain(tx.Statement.SQL.String(), tx.Statement.Vars...))
	}); err != nil {
		t.Fatal(err)
	}
	result, err := service.GetSession(context.Background(), "t", "u", &types.RequestAgentSessionQuery{SessionId: "s", Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	var evidence []map[string]interface{}
	if err = json.Unmarshal([]byte(result.Messages[0].Evidence), &evidence); err != nil {
		t.Fatal(err)
	}
	if evidence[0]["status"] != "executed" || evidence[1]["status"] != "expired" || evidence[2]["status"] != "unchanged" {
		t.Fatal(evidence)
	}
	if len(queries) != 3 {
		t.Fatal("unexpected queries", queries)
	}
	actionSQL := queries[2]
	if strings.Contains(actionSQL, "old-action") || strings.Contains(actionSQL, "SELECT *") || strings.Contains(actionSQL, "payload") || !strings.Contains(actionSQL, " IN ") {
		t.Fatal("unbounded/full-payload action query", actionSQL)
	}
	if !strings.Contains(queries[1], "LIMIT 2") {
		t.Fatal("unbounded message query", queries[1])
	}
}
