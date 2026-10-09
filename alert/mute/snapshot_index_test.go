package mute

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"watchAlert/internal/models"
)

// Independent oracle retains the former ordered, exhaustive scan. Do not call
// CompileSnapshotChecked here: the optimized index must not verify itself.
func linearSnapshot(rules []models.AlertSilences, now int64) (func(map[string]interface{}) bool, error) {
	var matches []func(map[string]interface{}) bool
	for _, rule := range rules {
		if rule.Status != 1 || now < rule.StartsAt || now >= rule.EndsAt {
			continue
		}
		match, err := models.CompileSilenceMatchers(rule.Labels)
		if err != nil {
			return nil, fmt.Errorf("invalid active silence %s: %w", rule.ID, err)
		}
		matches = append(matches, match)
	}
	return func(labels map[string]interface{}) bool {
		for _, match := range matches {
			if match(labels) {
				return true
			}
		}
		return false
	}, nil
}

func batchSnapshot(rules []models.AlertSilences, now int64) (func(map[string]interface{}) bool, error) {
	return CompileBatchSnapshotChecked(rules, now, 1000)
}

func snapshotFixture(n int, shape string) []models.AlertSilences {
	rules := make([]models.AlertSilences, n)
	for i := range rules {
		conditions := []models.SilenceLabel{{Key: "env", Operator: "=", Value: "prod"}, {Key: "service", Operator: "==", Value: fmt.Sprintf("service-%d", i)}}
		if shape == "regex" {
			conditions = []models.SilenceLabel{{Key: "service", Operator: "=~", Value: fmt.Sprintf("^service-%d$", i)}}
		}
		if shape == "mixed" && i%10 == 0 {
			conditions = []models.SilenceLabel{{Key: "service", Operator: "=~", Value: fmt.Sprintf("^service-%d$", i)}}
		}
		rules[i] = models.AlertSilences{ID: fmt.Sprint(i), Status: 1, StartsAt: 100, EndsAt: 200, Labels: conditions}
	}
	return rules
}

func TestSnapshotMatchesIndependentLinearOracle(t *testing.T) {
	rng := rand.New(rand.NewSource(20261010))
	keys := []string{"env", "service", "Env", "环境", "region"}
	values := []string{"prod", "test", "payment", "payment-api", "生产", "["}
	ops := []string{"=", "==", "!=", "=~", "!~"}
	for trial := 0; trial < 300; trial++ {
		rules := snapshotFixture(20, "mixed")
		for i := 0; i < 30; i++ {
			conditions := make([]models.SilenceLabel, 1+rng.Intn(4))
			for j := range conditions {
				op := ops[rng.Intn(len(ops))]
				value := values[rng.Intn(len(values))]
				if op == "=~" || op == "!~" {
					value = []string{"^prod$", "pay", "^$", "生产", "["}[rng.Intn(5)]
				}
				conditions[j] = models.SilenceLabel{Key: keys[rng.Intn(len(keys))], Operator: op, Value: value}
			}
			rules = append(rules, models.AlertSilences{ID: fmt.Sprint(i), Status: rng.Intn(3), StartsAt: 100, EndsAt: 200, Labels: conditions})
		}
		for _, now := range []int64{99, 100, 199, 200} {
			want, wantErr := linearSnapshot(rules, now)
			got, err := batchSnapshot(rules, now)
			if fmt.Sprint(err) != fmt.Sprint(wantErr) {
				t.Fatalf("trial=%d time=%d error differs: %v / %v", trial, now, err, wantErr)
			}
			if err != nil {
				if got != nil {
					t.Fatal("invalid snapshot returned matcher")
				}
				continue
			}
			for n := 0; n < 80; n++ {
				labels := map[string]interface{}{}
				for _, key := range keys {
					switch rng.Intn(5) {
					case 0:
						continue
					case 1:
						labels[key] = 123
					case 2:
						labels[key] = nil
					default:
						labels[key] = values[rng.Intn(len(values))]
					}
				}
				if got(labels) != want(labels) {
					t.Fatalf("trial=%d time=%d mismatch labels=%v", trial, now, labels)
				}
			}
		}
	}
}

func TestSnapshotIndexNeverHidesInvalidActiveRule(t *testing.T) {
	rules := snapshotFixture(30, "equality")
	// Even a rule whose equality anchor cannot match must be fully validated.
	rules = append(rules, models.AlertSilences{ID: "invalid", Status: 1, StartsAt: 100, EndsAt: 200, Labels: []models.SilenceLabel{{Key: "env", Operator: "=", Value: "never"}, {Key: "service", Operator: "=~", Value: "["}}})
	if match, err := batchSnapshot(rules, 150); err == nil || match != nil {
		t.Fatal("invalid active rule hidden", err)
	}
	if !CompileSnapshot(rules, 150)(nil) {
		t.Fatal("legacy fail-closed behavior changed")
	}
}

func TestBatchSnapshotPlanningHintsDoNotChangeResults(t *testing.T) {
	for _, size := range []int{0, 1, 8, 9, 100} {
		rules := snapshotFixture(size, "mixed")
		want, _ := linearSnapshot(rules, 150)
		for _, hint := range []int{-1, 0, 1, 31, 32, 10000} {
			match, err := CompileBatchSnapshotChecked(rules, 150, hint)
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 150; i++ {
				labels := map[string]interface{}{"env": "prod", "service": fmt.Sprintf("service-%d", i)}
				if match(labels) != want(labels) {
					t.Fatal("hint changed semantics", size, hint, labels)
				}
			}
		}
	}
}

func TestSnapshotIndexPositiveCandidatesAndFallbacks(t *testing.T) {
	rules := snapshotFixture(30, "equality")
	for _, conditions := range [][]models.SilenceLabel{
		{{Key: "negative", Operator: "!=", Value: "blocked"}},
		{{Key: "regex", Operator: "=~", Value: "pay"}},
		{{Key: "neg_regex", Operator: "!~", Value: "^blocked$"}},
		{{Key: "环境", Operator: "=", Value: "生产"}, {Key: "service", Operator: "=~", Value: "^pay"}},
		{{Key: "service", Operator: "=", Value: "contradiction"}, {Key: "service", Operator: "==", Value: "other"}},
	} {
		rules = append(rules, models.AlertSilences{Status: 1, StartsAt: 100, EndsAt: 200, Labels: conditions})
	}
	want, _ := linearSnapshot(rules, 150)
	got, err := batchSnapshot(rules, 150)
	if err != nil {
		t.Fatal(err)
	}
	cases := []map[string]interface{}{
		nil, {}, {"env": "prod"}, {"env": 1, "service": "service-0"},
		{"env": "test", "service": "service-0"}, {"env": "prod", "service": nil},
		{"env": "prod", "Service": "service-0"}, {"negative": "allowed"}, {"negative": "blocked"},
		{"negative": 123}, {"regex": "repayment"}, {"neg_regex": "allowed"}, {"neg_regex": "blocked"},
		{"环境": "生产", "service": "payment"}, {"环境": "生产", "service": "orders"},
		{"service": "contradiction"},
	}
	for i := 0; i < 35; i++ {
		cases = append(cases, map[string]interface{}{"env": "prod", "service": fmt.Sprintf("service-%d", i)})
	}
	for _, labels := range cases {
		if got(labels) != want(labels) {
			t.Fatal("candidate/fallback semantics changed", labels)
		}
	}
	// The compiled snapshot must not retain mutable matcher configuration.
	rules[0].Labels[1].Value = "changed"
	if !got(map[string]interface{}{"env": "prod", "service": "service-0"}) {
		t.Fatal("snapshot mutated with source slice")
	}
	var workers sync.WaitGroup
	for i := 0; i < 8; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for j := 0; j < 100; j++ {
				for _, labels := range cases {
					if got(labels) != want(labels) {
						t.Error("shared snapshot changed", labels)
						return
					}
				}
			}
		}()
	}
	workers.Wait()
}

func TestSnapshotIndexSelectsRareEqualityButStillChecksFullRule(t *testing.T) {
	var compiled []compiledSilence
	calls := 0
	for _, rule := range snapshotFixture(1000, "equality") {
		full, err := models.CompileSilenceMatchers(rule.Labels)
		if err != nil {
			t.Fatal(err)
		}
		compiled = append(compiled, compiledSilence{labels: rule.Labels, match: func(labels map[string]interface{}) bool { calls++; return full(labels) }})
	}
	match := indexSnapshot(compiled)
	if match(map[string]interface{}{"env": "prod", "service": "missing"}) || calls != 0 {
		t.Fatal("unrelated rules evaluated", calls)
	}
	if match(map[string]interface{}{"env": "test", "service": "service-999"}) || calls != 1 {
		t.Fatal("anchor bypassed full rule", calls)
	}
	if !match(map[string]interface{}{"env": "prod", "service": "service-999"}) || calls != 2 {
		t.Fatal("candidate not selected exactly once", calls)
	}
}

func TestSnapshotIndexSupportsManyDistinctKeys(t *testing.T) {
	var rules []models.AlertSilences
	for i := 0; i < 100; i++ {
		rules = append(rules, models.AlertSilences{Status: 1, StartsAt: 100, EndsAt: 200, Labels: []models.SilenceLabel{{Key: fmt.Sprint(i), Operator: "=", Value: "wanted"}}})
	}
	got, err := batchSnapshot(rules, 150)
	if err != nil {
		t.Fatal(err)
	}
	want, _ := linearSnapshot(rules, 150)
	for _, labels := range []map[string]interface{}{nil, {"0": "wanted"}, {"99": "wanted"}, {"100": "wanted"}, {"99": 1}} {
		if got(labels) != want(labels) {
			t.Fatal("event-key lookup changed result", labels)
		}
	}
}

func BenchmarkSilenceSnapshotMatch(b *testing.B) {
	for _, n := range []int{1, 100, 1000} {
		for _, shape := range []string{"equality", "mixed", "regex"} {
			rules := snapshotFixture(n, shape)
			for _, impl := range []struct {
				name    string
				compile func([]models.AlertSilences, int64) (func(map[string]interface{}) bool, error)
			}{{"linear", linearSnapshot}, {"snapshot", batchSnapshot}} {
				b.Run(fmt.Sprintf("rules=%d/%s/%s", n, shape, impl.name), func(b *testing.B) {
					match, err := impl.compile(rules, 150)
					if err != nil {
						b.Fatal(err)
					}
					// A missing candidate forces exhaustive scan in the old implementation.
					labels := map[string]interface{}{"env": "prod", "service": "not-found", "cluster": "cn", "instance": "10.0.0.1"}
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						if match(labels) {
							b.Fatal("unexpected match")
						}
					}
				})
			}
		}
	}
}

func BenchmarkSilenceSnapshotCompile(b *testing.B) {
	for _, n := range []int{1, 100, 1000} {
		for _, impl := range []struct {
			name    string
			compile func([]models.AlertSilences, int64) (func(map[string]interface{}) bool, error)
		}{{"linear", linearSnapshot}, {"snapshot", batchSnapshot}, {"notification", CompileSnapshotChecked}} {
			b.Run(fmt.Sprintf("rules=%d/%s", n, impl.name), func(b *testing.B) {
				rules := snapshotFixture(n, "equality")
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, err := impl.compile(rules, 150); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
