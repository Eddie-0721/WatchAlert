package services

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"watchAlert/internal/models"
	"watchAlert/internal/types"

	"gorm.io/gorm"
)

func TestSessionEvidencePreservesUnrelatedData(t *testing.T) {
	service, db := pageService(t)
	queryEvidence := `[ { "status": "completed", "source": {"large":9007199254740993}, "query":{"promql":"rate(test[5m])"} } ]`
	proposalEvidence := `[{"actionId":"a","status":"pending_confirmation","preview":{"large":9007199254740993,"exact":1.2300},"unknown":{"future":[true,null,"text"]}}]`
	for i, raw := range []string{queryEvidence, proposalEvidence} {
		if err := db.Create(&models.AgentMessage{ID: fmt.Sprint(i), TenantId: "t", SessionId: "s", CreatedAt: int64(i + 100), Evidence: raw}).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Create(&models.AgentPendingAction{ID: "a", TenantId: "t", SessionId: "s", Status: "executed"}).Error; err != nil {
		t.Fatal(err)
	}
	result, err := service.GetSession(context.Background(), "t", "u", &types.RequestAgentSessionQuery{SessionId: "s"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Messages[0].Evidence != queryEvidence {
		t.Error("read-only evidence was unnecessarily rewritten", result.Messages[0].Evidence)
	}
	updated := result.Messages[1].Evidence
	if !strings.Contains(updated, `"status":"executed"`) || !strings.Contains(updated, "9007199254740993") || !strings.Contains(updated, "1.2300") {
		t.Error("status overlay changed unrelated evidence values", updated)
	}
	var stored models.AgentMessage
	if err := db.Where("id = ?", "1").First(&stored).Error; err != nil || stored.Evidence != proposalEvidence {
		t.Fatal("history source was mutated", stored, err)
	}
}

func TestSessionEvidenceEdgeCases(t *testing.T) {
	for _, raw := range []string{
		``, `not json`, `null`, `[]`, `[null, {}]`, `[{"actionId":"a"}, false]`,
		`[ { "actionId":"a", "status":"executed", "source":{"n":1e999} } ]`,
		`[{"actionId":123,"status":"old"},{"actionId":null},{"ActionId":"a"}]`,
		`[{"actionId":"foreign-session","status":"old"},{"actionId":"foreign-tenant","status":"old"},{"actionId":"missing"}]`,
	} {
		t.Run(raw, func(t *testing.T) {
			service, db := pageService(t)
			if err := db.Create(&models.AgentMessage{ID: "m", TenantId: "t", SessionId: "s", Evidence: raw}).Error; err != nil {
				t.Fatal(err)
			}
			actions := []models.AgentPendingAction{
				{ID: "a", TenantId: "t", SessionId: "s", Status: "executed"},
				{ID: "foreign-session", TenantId: "t", SessionId: "other", Status: "executed"},
				{ID: "foreign-tenant", TenantId: "other", SessionId: "s", Status: "executed"},
			}
			if err := db.Create(&actions).Error; err != nil {
				t.Fatal(err)
			}
			result, err := service.GetSession(context.Background(), "t", "u", &types.RequestAgentSessionQuery{SessionId: "s"})
			if err != nil || len(result.Messages) != 1 || result.Messages[0].Evidence != raw {
				t.Fatal("unchanged/invalid evidence must be preserved", result, err)
			}
		})
	}
}

func TestSessionEvidenceBatchesAndInvalidStatus(t *testing.T) {
	service, db := pageService(t)
	items := []map[string]json.RawMessage{nil}
	for i := 0; i < 205; i++ {
		id := fmt.Sprintf("action-%03d", i)
		if err := db.Create(&models.AgentPendingAction{ID: id, TenantId: "t", SessionId: "s", Status: "executed"}).Error; err != nil {
			t.Fatal(err)
		}
		encoded, _ := json.Marshal(id)
		items = append(items, map[string]json.RawMessage{"actionId": encoded, "status": json.RawMessage(`123`), "source": json.RawMessage(`{"number":1e999}`)})
	}
	items = append(items, items[1]) // Duplicate IDs must not increase query batches.
	raw, _ := json.Marshal(items)
	if err := db.Create(&models.AgentMessage{ID: "m", TenantId: "t", SessionId: "s", Evidence: string(raw)}).Error; err != nil {
		t.Fatal(err)
	}
	queries := 0
	if err := db.Callback().Query().After("gorm:query").Register("test:count_actions", func(tx *gorm.DB) {
		if tx.Statement.Table == (models.AgentPendingAction{}).TableName() {
			queries++
		}
	}); err != nil {
		t.Fatal(err)
	}
	result, err := service.GetSession(context.Background(), "t", "u", &types.RequestAgentSessionQuery{SessionId: "s"})
	if err != nil || queries != 3 {
		t.Fatal("action batching failed", queries, err)
	}
	if err := json.Unmarshal([]byte(result.Messages[0].Evidence), &items); err != nil {
		t.Fatal(err)
	}
	if items[0] != nil || len(items) != 207 {
		t.Fatal("changed evidence shape")
	}
	for _, item := range items[1:] {
		if string(item["status"]) != `"executed"` || string(item["source"]) != `{"number":1e999}` {
			t.Fatal("invalid overlay", item)
		}
	}
}

func TestSessionEvidenceCancellationAfterHistoryRead(t *testing.T) {
	service, db := pageService(t)
	if err := db.Create(&models.AgentMessage{ID: "m", TenantId: "t", SessionId: "s", Evidence: `[]`}).Error; err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := db.Callback().Query().After("gorm:query").Register("test:cancel_history", func(tx *gorm.DB) {
		if tx.Statement.Table == (models.AgentMessage{}).TableName() {
			cancel()
		}
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.GetSession(ctx, "t", "u", &types.RequestAgentSessionQuery{SessionId: "s"}); !errors.Is(err, context.Canceled) {
		t.Fatal("cancellation after SQL was ignored", err)
	}
}

func BenchmarkSessionEvidencePage(b *testing.B) {
	for _, proposal := range []bool{false, true} {
		b.Run(fmt.Sprintf("proposal=%t", proposal), func(b *testing.B) {
			service, db := pageService(b)
			values := make([]map[string]interface{}, 100)
			for i := range values {
				values[i] = map[string]interface{}{"timestamp": 1700000000 + i, "value": i, "labels": map[string]string{"environment": "prod", "service": "payment", "instance": fmt.Sprint(i)}}
			}
			evidence := map[string]interface{}{"status": "completed", "source": map[string]interface{}{"samples": values}, "query": map[string]string{"promql": "up"}}
			if proposal {
				evidence["actionId"] = "a"
				evidence["status"] = "pending_confirmation"
			}
			raw, err := json.Marshal([]interface{}{evidence})
			if err != nil {
				b.Fatal(err)
			}
			for i := 0; i < 50; i++ {
				if err := db.Create(&models.AgentMessage{ID: fmt.Sprint(i), TenantId: "t", SessionId: "s", CreatedAt: int64(i + 100), Evidence: string(raw)}).Error; err != nil {
					b.Fatal(err)
				}
			}
			if err := db.Create(&models.AgentPendingAction{ID: "a", TenantId: "t", SessionId: "s", Status: "executed"}).Error; err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				page, err := service.GetSession(context.Background(), "t", "u", &types.RequestAgentSessionQuery{SessionId: "s"})
				if err != nil || len(page.Messages) != 50 {
					b.Fatal(err)
				}
			}
		})
	}
}
