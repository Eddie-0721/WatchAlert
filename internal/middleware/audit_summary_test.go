package middleware

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	appctx "watchAlert/internal/ctx"
	"watchAlert/internal/models"
	"watchAlert/internal/repo"

	"github.com/gin-gonic/gin"
)

type auditCapture struct {
	repo.InterAuditLogRepo
	entries []models.AuditLog
	err     error
}

func (a *auditCapture) Create(entry models.AuditLog) error {
	a.entries = append(a.entries, entry)
	return a.err
}

type auditEntry struct {
	repo.InterEntryRepo
	audit *auditCapture
}

func (e auditEntry) AuditLog() repo.InterAuditLogRepo { return e.audit }

func setupAuditCapture(t *testing.T, capture *auditCapture) {
	t.Helper()
	oldDB, oldCtx := appctx.DB, appctx.Ctx
	appctx.DB, appctx.Ctx = auditEntry{audit: capture}, context.Background()
	t.Cleanup(func() { appctx.DB, appctx.Ctx = oldDB, oldCtx })
}

func TestAuditSummaryOmitsAllValuesAndUntrustedKeys(t *testing.T) {
	for _, body := range []string{
		`{"agentConfig":{"model":{"apiKey":"SYNTHETIC-SECRET","apiKeyEncrypted":"SYNTHETIC-SECRET"}},"name":"SYNTHETIC-SECRET"}`,
		`{"HTTP":{"headers":{"Authorization":"SYNTHETIC-SECRET"}},"labels":{"any":"SYNTHETIC-SECRET"},"kubeConfig":"SYNTHETIC-SECRET"}`,
		`{"SYNTHETIC-SECRET":"SYNTHETIC-SECRET","PASSWORD":"SYNTHETIC-SECRET","items":[{"token":"SYNTHETIC-SECRET"}]}`,
		`{"password":"SYNTHETIC-SECRET"`,
		`["SYNTHETIC-SECRET"]`, `"SYNTHETIC-SECRET"`, `null`, ``,
	} {
		summary := auditBodySummary([]byte(body))
		if strings.Contains(summary, "SYNTHETIC-SECRET") {
			t.Fatalf("secret in summary: %s", summary)
		}
		if !json.Valid([]byte(summary)) {
			t.Fatalf("invalid summary: %s", summary)
		}
	}
	if got := auditBodySummary([]byte(`{"name":"a","agentConfig":{},"unknown":"x"}`)); got != `{"fields":["agentConfig","name"],"fieldCount":3,"values":"omitted"}` {
		t.Fatalf("unexpected summary: %s", got)
	}
}

func TestAuditingPreservesBodyAndSingleResponse(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "saved", true: "database-failure"}[fail], func(t *testing.T) {
			capture := &auditCapture{}
			if fail {
				capture.err = errors.New("database error SYNTHETIC-SECRET")
			}
			setupAuditCapture(t, capture)
			body := `{"agentConfig":{"model":{"apiKey":"SYNTHETIC-SECRET"}}}`
			r := gin.New()
			r.Use(func(c *gin.Context) { c.Set("UserId", "api-key-user") }, AuditingLog())
			r.POST("/", func(c *gin.Context) {
				got, err := io.ReadAll(c.Request.Body)
				if err != nil || string(got) != body {
					t.Errorf("request body changed: %v", err)
				}
				c.JSON(200, gin.H{"ok": true})
			})
			w := httptest.NewRecorder()
			req := httptest.NewRequest("POST", "/", strings.NewReader(body))
			req.Header.Set(TenantIDHeaderKey, "tenant-a")
			r.ServeHTTP(w, req)
			if w.Code != 200 || w.Body.String() != `{"ok":true}` {
				t.Fatalf("corrupted response: %d %s", w.Code, w.Body.String())
			}
			if len(capture.entries) != 1 {
				t.Fatalf("audit count=%d", len(capture.entries))
			}
			entry := capture.entries[0]
			if entry.Username != "api-key-user" || entry.TenantId != "tenant-a" || entry.StatusCode != 200 {
				t.Fatalf("incorrect metadata: %+v", entry)
			}
			if strings.Contains(entry.Body, "SYNTHETIC-SECRET") {
				t.Fatal("raw credential persisted")
			}
		})
	}
}

func TestAuditingSanitizesRejectedRequest(t *testing.T) {
	capture := &auditCapture{}
	setupAuditCapture(t, capture)
	r := gin.New()
	r.Use(func(c *gin.Context) { c.Set("UserId", "user-a") }, AuditingLog())
	r.POST("/", func(c *gin.Context) { c.AbortWithStatusJSON(400, gin.H{"error": "invalid input"}) })
	w := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/", strings.NewReader(`{"token":"SYNTHETIC-SECRET"`))
	req.Header.Set(TenantIDHeaderKey, "tenant-a")
	r.ServeHTTP(w, req)
	if len(capture.entries) != 1 || capture.entries[0].StatusCode != 400 || strings.Contains(capture.entries[0].Body, "SYNTHETIC-SECRET") {
		t.Fatal("failed request not safely audited")
	}
}

type failedBody struct{}

func (failedBody) Read([]byte) (int, error) { return 0, errors.New("SYNTHETIC-SECRET read error") }
func (failedBody) Close() error             { return nil }

func TestAuditingRejectsUnreadableAndOversizedBodies(t *testing.T) {
	for _, oversized := range []bool{false, true} {
		r := gin.New()
		r.Use(func(c *gin.Context) { c.Set("UserId", "user-a") }, AuditingLog())
		r.POST("/", func(c *gin.Context) { t.Fatal("bad body reached business handler") })
		w := httptest.NewRecorder()
		req := httptest.NewRequest("POST", "/", nil)
		req.Header.Set(TenantIDHeaderKey, "tenant-a")
		want := 400
		if oversized {
			req.Body = io.NopCloser(strings.NewReader(strings.Repeat("x", maxAuditedRequestBody+1)))
			want = 413
		} else {
			req.Body = failedBody{}
		}
		r.ServeHTTP(w, req)
		if w.Code != want || strings.Contains(w.Body.String(), "SYNTHETIC-SECRET") {
			t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
		}
	}
}
