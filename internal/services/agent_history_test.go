package services

import (
	"context"
	"fmt"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"strings"
	"testing"
	appctx "watchAlert/internal/ctx"
	"watchAlert/internal/models"
)

func TestRunHistoryBoundsAndOwnership(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	connection, _ := db.DB()
	defer connection.Close()
	if err = db.AutoMigrate(&models.AgentSession{}, &models.AgentMessage{}); err != nil {
		t.Fatal(err)
	}
	if err = db.Create(&models.AgentSession{ID: "s", TenantId: "t", UserId: "u"}).Error; err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		row := models.AgentMessage{ID: fmt.Sprintf("m%03d", i), SessionId: "s", TenantId: "t", CreatedAt: int64(i + 100), Role: "assistant", Content: "history", Evidence: strings.Repeat("evidence", 1000)}
		if err = db.Create(&row).Error; err != nil {
			t.Fatal(err)
		}
	}
	service := agentService{ctx: &appctx.Context{DB: policyRepo{db: db}}}
	history, err := service.loadRunHistory(context.Background(), "t", "u", "s")
	if err != nil || len(history.Messages) != 11 || history.Messages[0].ID != "m089" || history.Messages[10].ID != "m099" {
		t.Fatal(history, err)
	}
	for _, message := range history.Messages {
		if message.Evidence != "" {
			t.Fatal("evidence must not travel as model history")
		}
	}
	for _, pair := range [][2]string{{"other", "u"}, {"t", "other"}} {
		if _, err = service.loadRunHistory(context.Background(), pair[0], pair[1], "s"); err == nil {
			t.Fatal("ownership bypass")
		}
	}
	if err = db.Model(&models.AgentMessage{}).Where("session_id = ?", "s").Update("content", strings.Repeat("告", 12000)).Error; err != nil {
		t.Fatal(err)
	}
	history, err = service.loadRunHistory(context.Background(), "t", "u", "s")
	if err != nil || len(history.Messages) != 3 {
		t.Fatal("character budget not applied", len(history.Messages), err)
	}
	for _, message := range history.Messages {
		if !strings.HasSuffix(message.Content, "[历史消息已截断]") {
			t.Fatal("truncation must be explicit")
		}
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = service.loadRunHistory(cancelled, "t", "u", "s"); err == nil {
		t.Fatal("cancellation ignored")
	}
}
