package api

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestBindingRejectsInvalidInputBeforeSideEffects(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tc := range []struct{ name, method, target, body string }{
		{"json-type", "POST", "/", `{"count":"secret-invalid-number"}`},
		{"json-malformed", "POST", "/", `{"count":`},
		{"json-empty", "POST", "/", ""},
		{"query-type", "GET", "/?count=secret-invalid-number", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			called := false
			r := gin.New()
			r.Any("/", func(c *gin.Context) {
				var input struct {
					Count int `json:"count" form:"count"`
				}
				if tc.method == "POST" {
					if !BindJson(c, &input) {
						return
					}
				} else if !BindQuery(c, &input) {
					return
				}
				called = true
				Service(c, func() (interface{}, interface{}) { return "unexpected", nil })
			})
			w := httptest.NewRecorder()
			req := httptest.NewRequest(tc.method, tc.target, strings.NewReader(tc.body))
			req.Header.Set("Content-Type", "application/json")
			r.ServeHTTP(w, req)
			if called || w.Code != http.StatusBadRequest {
				t.Fatalf("called=%v status=%d", called, w.Code)
			}
			if strings.Contains(w.Body.String(), "secret-invalid-number") {
				t.Fatal("input leaked in error")
			}
			decoder := json.NewDecoder(w.Body)
			var response map[string]interface{}
			if err := decoder.Decode(&response); err != nil {
				t.Fatal(err)
			}
			if err := decoder.Decode(&response); err != io.EOF {
				t.Fatalf("response has trailing data: %v", err)
			}
		})
	}
}

func TestBindingStillPopulatesValidRequests(t *testing.T) {
	for _, method := range []string{"GET", "POST"} {
		t.Run(method, func(t *testing.T) {
			r := gin.New()
			r.Any("/", func(c *gin.Context) {
				var input struct {
					Count int `json:"count" form:"count"`
				}
				var ok bool
				if method == "GET" {
					ok = BindQuery(c, &input)
				} else {
					ok = BindJson(c, &input)
				}
				if !ok || input.Count != 7 {
					t.Errorf("ok=%v count=%d", ok, input.Count)
					return
				}
				Service(c, func() (interface{}, interface{}) { return input, nil })
			})
			w := httptest.NewRecorder()
			req := httptest.NewRequest(method, "/?count=7", strings.NewReader(`{"count":7}`))
			req.Header.Set("Content-Type", "application/json")
			r.ServeHTTP(w, req)
			if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"count":7`) {
				t.Fatalf("%d: %s", w.Code, w.Body.String())
			}
		})
	}
}

func TestServiceDoesNotRunAfterAbort(t *testing.T) {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Abort()
	Service(c, func() (interface{}, interface{}) { t.Fatal("aborted service called"); return nil, nil })
}

func TestControllersRejectMalformedJSONWithoutInitializedServices(t *testing.T) {
	for name, handler := range map[string]gin.HandlerFunc{
		"rule":     RuleController.Create,
		"settings": SettingsController.Save,
		"agent":    AgentController.ConfirmAction,
	} {
		t.Run(name, func(t *testing.T) {
			r := gin.New()
			r.POST("/", handler)
			w := httptest.NewRecorder()
			req := httptest.NewRequest("POST", "/", strings.NewReader("{"))
			req.Header.Set("Content-Type", "application/json")
			r.ServeHTTP(w, req)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("status=%d", w.Code)
			}
		})
	}
}

func TestControllersMustCheckBindingResult(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	for _, path := range files {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(file, func(n ast.Node) bool {
			statement, ok := n.(*ast.ExprStmt)
			if !ok {
				return true
			}
			call, ok := statement.X.(*ast.CallExpr)
			if !ok {
				return true
			}
			name, ok := call.Fun.(*ast.Ident)
			if ok && (name.Name == "BindJson" || name.Name == "BindQuery") {
				t.Errorf("unchecked binding at %s", fset.Position(call.Pos()))
			}
			return true
		})
	}
}
