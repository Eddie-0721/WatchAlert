package services

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"testing"
	"time"
	"watchAlert/internal/ctx"
	"watchAlert/internal/models"
	"watchAlert/internal/types"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

func silencePreviewFixture(t testing.TB, n int) (alertSilenceService, *types.RequestSilencePreview, *previewEvents) {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { sqlDB.Close() })
	if err := db.AutoMigrate(&models.FaultCenter{}, &models.AlertSilences{}); err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&models.FaultCenter{TenantId: "t", ID: "fc", Name: "center"}).Error; err != nil {
		t.Fatal(err)
	}
	source := eventBenchmarkFixture(n, 0).ctx.Redis.(benchmarkEventCache).alerts.events
	events := &previewEvents{events: make(map[string]*models.AlertCurEvent, n)}
	for _, group := range source {
		for fp, event := range group {
			event.FaultCenterId = "fc"
			event.Status = models.StateAlerting
			events.events[fp] = event
		}
	}
	request := &types.RequestSilencePreview{TenantId: "t", FaultCenterId: "fc", Name: "maintenance", Comment: "release", StartsAt: time.Now().Unix(), EndsAt: time.Now().Add(time.Hour).Unix(), Labels: []models.SilenceLabel{{Key: "env", Operator: "==", Value: "prod"}}}
	return alertSilenceService{ctx: &ctx.Context{DB: policyRepo{db: db}, Redis: previewCache{events: events}}}, request, events
}

// Independent reference of the original full-materialization implementation.
// The hash still includes EVERY matching fingerprint, not only visible samples.
func referenceSilencePreview(t testing.TB, r *types.RequestSilencePreview, events map[string]*models.AlertCurEvent) *types.ResponseSilencePreview {
	t.Helper()
	match, err := models.CompileSilenceMatchers(r.Labels)
	if err != nil {
		t.Fatal(err)
	}
	matches := make([]types.SilencePreviewSample, 0)
	for _, event := range events {
		if event == nil || event.TenantId != r.TenantId || event.FaultCenterId != r.FaultCenterId || event.Status == models.StateRecovered || !match(event.Labels) {
			continue
		}
		matches = append(matches, types.SilencePreviewSample{Fingerprint: event.Fingerprint, RuleName: event.RuleName, Scope: buildAlertScope(event.Labels)})
	}
	sort.Slice(matches, func(i, j int) bool { return matches[i].Fingerprint < matches[j].Fingerprint })
	ids := make([]string, len(matches))
	for i, event := range matches {
		ids[i] = event.Fingerprint
	}
	payload, err := json.Marshal(struct {
		Tenant  string
		Request *types.RequestSilencePreview
		Before  *models.AlertSilences
		IDs     []string
	}{r.TenantId, r, nil, ids})
	if err != nil {
		t.Fatal(err)
	}
	result := &types.ResponseSilencePreview{PreviewHash: fmt.Sprintf("%x", sha256.Sum256(payload)), Total: len(matches), Samples: matches, Truncated: len(matches) > 5}
	if len(matches) > 5 {
		result.Samples = matches[:5]
	}
	return result
}

func TestSilencePreviewMatchesReference(t *testing.T) {
	service, request, fixture := silencePreviewFixture(t, 1000)
	rng := rand.New(rand.NewSource(42))
	// Unique fingerprints follow the writer's contract; malformed/foreign rows
	// are mixed in to ensure the optimization preserves filtering, not just top 5.
	keys := make([]string, 0, len(fixture.events))
	for key := range fixture.events {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		event := fixture.events[key]
		switch rng.Intn(9) {
		case 0:
			event.TenantId = "other"
		case 1:
			event.FaultCenterId = "other"
		case 2:
			event.Status = models.StateRecovered
		case 3:
			event.Labels["env"] = "test"
		case 4:
			event.Status = models.StatePendingRecovery
		case 5:
			event.Status = models.StatePreAlert
		}
	}
	fixture.events["nil"] = nil
	for i := 0; i < 100; i++ {
		request.Labels = []models.SilenceLabel{{Key: "service", Operator: "=~", Value: fmt.Sprintf("service-%d.*", i)}}
		actual, err := service.preview(request)
		if err != nil {
			t.Fatal(err)
		}
		actual.PreviewAt = 0
		want := referenceSilencePreview(t, request, fixture.events)
		if !reflect.DeepEqual(actual, want) {
			t.Fatalf("query %d differs: got %#v, want %#v", i, actual, want)
		}
	}
}

func TestSilencePreviewSampleBoundaries(t *testing.T) {
	for _, n := range []int{0, 1, 4, 5, 6, 1000} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			service, request, fixture := silencePreviewFixture(t, n)
			actual, err := service.preview(request)
			if err != nil {
				t.Fatal(err)
			}
			actual.PreviewAt = 0
			if want := referenceSilencePreview(t, request, fixture.events); !reflect.DeepEqual(actual, want) {
				t.Fatalf("boundary %d differs: got %#v, want %#v", n, actual, want)
			}
		})
	}
}

type previewScopeCounter struct{ calls *int }

func (c previewScopeCounter) String() string {
	(*c.calls)++
	return "platform"
}

func TestSilencePreviewOnlyBuildsVisibleScopes(t *testing.T) {
	service, request, fixture := silencePreviewFixture(t, 1000)
	calls := 0
	for _, event := range fixture.events {
		event.Labels["owner"] = previewScopeCounter{calls: &calls}
	}
	reference := referenceSilencePreview(t, request, fixture.events)
	if calls != 1000 {
		t.Fatalf("reference must materialize all 1000 scopes, got %d", calls)
	}
	calls = 0
	actual, err := service.preview(request)
	if err != nil {
		t.Fatal(err)
	}
	actual.PreviewAt = 0
	if calls != 5 || !reflect.DeepEqual(actual, reference) {
		t.Fatalf("expected same preview with only 5 scope builds, got %d", calls)
	}
	// Removing a match outside the sample must still invalidate confirmation.
	delete(fixture.events, "fp-00000999")
	changed, err := service.preview(request)
	if err != nil {
		t.Fatal(err)
	}
	if changed.Total != 999 || !reflect.DeepEqual(changed.Samples, actual.Samples) || changed.PreviewHash == actual.PreviewHash {
		t.Fatal("hidden match changes must alter the digest, not the visible samples")
	}
}

func BenchmarkSilencePreview(b *testing.B) {
	for _, n := range []int{5, 1000, 10000} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			service, request, _ := silencePreviewFixture(b, n)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				result, err := service.preview(request)
				if err != nil || result.Total != n || len(result.Samples) != min(n, 5) {
					b.Fatal(result, err)
				}
			}
		})
	}
}
