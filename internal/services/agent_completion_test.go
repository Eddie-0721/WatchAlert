package services

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync/atomic"
	"testing"
	"time"
	"watchAlert/internal/models"
	"watchAlert/internal/types"

	"gorm.io/gorm"
)

func completionService(t *testing.T) (*agentService, *gorm.DB) {
	t.Helper()
	service, db := pageService(t)
	enabled := true
	service.ctx.DB = policyRepo{db: db, settings: &policySettings{settings: models.Settings{AgentConfig: models.AgentConfig{Enable: &enabled}}}}
	return service, db
}

func TestAgentCompletionPersistence(t *testing.T) {
	for _, stream := range []bool{true, false} {
		for _, failure := range []string{"", "user", "assistant", "session", "tail"} {
			t.Run(fmt.Sprintf("stream=%t/failure=%s", stream, failure), func(t *testing.T) {
				service, db := completionService(t)
				var called atomic.Bool
				agentTransportServer(t, func(w http.ResponseWriter, r *http.Request) {
					called.Store(true)
					if stream {
						// If a database transaction spans the model request, this
						// single-connection probe cannot acquire the pool slot.
						probe, cancel := context.WithTimeout(r.Context(), time.Second)
						defer cancel()
						var count int64
						if err := db.WithContext(probe).Model(&models.AgentMessage{}).Count(&count).Error; err != nil {
							t.Error("database connection held during model call", err)
						}
						fmt.Fprint(w, "event: delta\ndata: {\"delta\":\"answer\"}\n\nevent: done\ndata: {\"content\":\"answer\",\"evidence\":\"[]\"}\n\n")
						if failure == "tail" {
							fmt.Fprint(w, "event: error\ndata: {\"message\":\"late failure\"}\n\n")
						}
					} else {
						fmt.Fprint(w, `{"content":"answer","evidence":"[]"}`)
					}
				})
				injected := errors.New("injected persistence failure")
				if err := db.Callback().Create().Before("gorm:create").Register("test:fail_message", func(tx *gorm.DB) {
					if message, ok := tx.Statement.Dest.(*models.AgentMessage); ok && message.Role == failure {
						tx.AddError(injected)
					}
				}); err != nil {
					t.Fatal(err)
				}
				if err := db.Callback().Update().Before("gorm:update").Register("test:fail_session", func(tx *gorm.DB) {
					if failure == "session" && tx.Statement.Table == (models.AgentSession{}).TableName() {
						tx.AddError(injected)
					}
				}); err != nil {
					t.Fatal(err)
				}
				done := 0
				req := &types.RequestAgentSessionMessage{SessionId: "s", Content: "question"}
				var err error
				if stream {
					err = service.StreamMessage(context.Background(), "t", "admin", req, func(event types.AgentStreamEvent) {
						if event.Type != "done" {
							return
						}
						done++
						var stored models.AgentMessage
						if readErr := db.Where("role = ?", "assistant").First(&stored).Error; readErr != nil || stored.Content != event.Content || stored.Evidence != event.Evidence {
							t.Error("done emitted before reply was persisted", readErr)
						}
						var session models.AgentSession
						if readErr := db.Where("id = ?", "s").First(&session).Error; readErr != nil || session.Title != "question" || session.UpdatedAt != stored.CreatedAt {
							t.Error("done emitted before session metadata was persisted", readErr)
						}
					})
				} else {
					_, err = service.SendMessage(context.Background(), "t", "admin", req)
				}
				wantFailure := failure != "" && (stream || failure != "tail")
				if (err != nil) != wantFailure || (stream && done != boolCount(!wantFailure)) {
					t.Error("incorrect terminal outcome", done, err)
				}
				if failure == "user" && called.Load() {
					t.Error("model called after user persistence failed")
				}
				var assistants int64
				if err := db.Model(&models.AgentMessage{}).Where("role = ?", "assistant").Count(&assistants).Error; err != nil || assistants != int64(boolCount(!wantFailure)) {
					t.Fatal("partial/failed reply persisted", assistants, err)
				}
			})
		}
	}
}

func TestAgentDatabasePoolWaitCancellation(t *testing.T) {
	for _, operation := range []string{"history", "user", "reply"} {
		t.Run(operation, func(t *testing.T) {
			service, db := completionService(t)
			pool, _ := db.DB()
			connection, err := pool.Conn(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer connection.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
			defer cancel()
			result := make(chan error, 1)
			go func() {
				message := &models.AgentMessage{ID: "waiting", TenantId: "t", SessionId: "s", Role: "assistant", Content: "answer"}
				var err error
				switch operation {
				case "history":
					_, err = service.loadRunHistory(ctx, "t", "admin", "s")
				case "user":
					err = service.saveAgentUserMessage(ctx, message)
				case "reply":
					err = service.saveAgentReply(ctx, message, "title")
				}
				result <- err
			}()
			select {
			case err := <-result:
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatal("pool wait did not honor caller deadline", err)
				}
			case <-time.After(2 * time.Second):
				connection.Close()
				<-result
				t.Fatal("pool wait remained blocked after request expired")
			}
			connection.Close()
			var count int64
			if err := db.Model(&models.AgentMessage{}).Count(&count).Error; err != nil || count != 0 {
				t.Fatal("canceled wait wrote a message", count, err)
			}
		})
	}
}

func TestAgentCompletionDisconnect(t *testing.T) {
	service, db := completionService(t)
	upstreamCanceled := make(chan struct{})
	agentTransportServer(t, func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "event: delta\ndata: {\"delta\":\"partial\"}\n\n")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		close(upstreamCanceled)
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := false
	err := service.StreamMessage(ctx, "t", "admin", &types.RequestAgentSessionMessage{SessionId: "s", Content: "question"}, func(event types.AgentStreamEvent) {
		if event.Type == "delta" {
			cancel()
		}
		done = done || event.Type == "done"
	})
	if !errors.Is(err, context.Canceled) || done {
		t.Fatal("disconnect was reported as success", err, done)
	}
	select {
	case <-upstreamCanceled:
	case <-time.After(2 * time.Second):
		t.Fatal("upstream request was not canceled")
	}
	var count int64
	if err := db.Model(&models.AgentMessage{}).Where("role = ?", "assistant").Count(&count).Error; err != nil || count != 0 {
		t.Fatal("partial reply persisted", count, err)
	}
}

func boolCount(value bool) int {
	if value {
		return 1
	}
	return 0
}
