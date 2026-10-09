package services

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"unicode/utf8"
	"watchAlert/internal/models"
	"watchAlert/pkg/agenttoken"
)

func TestAgentAuditTruncationPreservesUTF8(t *testing.T) {
	const limit = 32 * 1024
	for _, value := range []string{"告", "警", "🔔"} {
		for cut := 1; cut < len(value); cut++ {
			input := append(bytes.Repeat([]byte("x"), limit-cut), []byte(value+"tail")...)
			result := truncateToolResult(input)
			if !utf8.ValidString(result) {
				t.Errorf("audit summary split a %d-byte character at byte %d", len(value), cut)
			}
		}
	}
}

func TestAgentAuditSummaryBoundaries(t *testing.T) {
	const limit = 32 * 1024
	for _, size := range []int{0, 1, limit - 1, limit, limit + 1, 2 * 1024 * 1024} {
		input := bytes.Repeat([]byte("x"), size)
		before := append([]byte(nil), input...)
		result := truncateToolResult(input)
		want := string(input)
		if size > limit {
			want = want[:limit] + "…"
		}
		if result != want || !bytes.Equal(before, input) {
			t.Fatal("summary bytes or source changed", size)
		}
		if size > 0 {
			input[0] = 'y'
			if result != want {
				t.Fatal("summary aliases source buffer")
			}
		}
	}
	for _, character := range []string{"é", "告", "🔔"} {
		for cut := 0; cut < len(character); cut++ {
			input := strings.Repeat("x", limit-cut) + character + "tail"
			want := strings.Repeat("x", limit-cut) + "…"
			if got := truncateToolResult([]byte(input)); got != want {
				t.Fatal("not the longest complete character prefix", cut)
			}
		}
	}
}

func TestAgentAuditSummaryDoesNotTruncateToolResponse(t *testing.T) {
	service, db := completionService(t)
	if err := db.AutoMigrate(&models.AgentToolCall{}, &models.AlertRule{}); err != nil {
		t.Fatal(err)
	}
	description := strings.Repeat("告警🔔", 20000)
	if err := db.Create(&models.AlertRule{TenantId: "t", RuleId: "r", Description: description}).Error; err != nil {
		t.Fatal(err)
	}
	previous := AgentService
	AgentService = service
	defer func() { AgentService = previous }()
	tool := &agentToolService{ctx: service.ctx}
	arguments := map[string]interface{}{"ruleId": "r"}
	result, err := tool.Execute(context.Background(), agenttoken.Claims{TenantId: "t", UserId: "admin", SessionId: "s", Tools: []string{"rules.get"}}, "rules.get", arguments)
	if err != nil || result.(models.AlertRule).Description != description {
		t.Fatal("tool response truncated", err)
	}
	var audit models.AgentToolCall
	if err := db.First(&audit).Error; err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	if audit.Result != truncateToolResult(encoded) || !utf8.ValidString(audit.Result) || len(audit.Result) > 32*1024+len("…") || audit.Input != `{"ruleId":"r"}` || audit.Status != "completed" {
		t.Fatal("audit contract changed")
	}
}

func legacyAuditSummary(value string) string {
	if len(value) <= 32*1024 {
		return value
	}
	return value[:32*1024] + "…"
}

var auditSummaryBenchmarkSink string

func BenchmarkAgentAuditSummaryCopy(b *testing.B) {
	for _, size := range []int{128, 32 * 1024, 2 * 1024 * 1024} {
		input := bytes.Repeat([]byte("x"), size)
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			b.Run("full-string-first", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					auditSummaryBenchmarkSink = legacyAuditSummary(string(input))
				}
			})
			b.Run("prefix-first", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					auditSummaryBenchmarkSink = truncateToolResult(input)
				}
			})
		})
	}
}
