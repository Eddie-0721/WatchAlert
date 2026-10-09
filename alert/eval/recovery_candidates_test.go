package eval

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
	"watchAlert/pkg/tools"
)

func TestRecoveryCandidatesKeepOrderDuplicatesAndDoNotChangeInputs(t *testing.T) {
	cached := []string{"keep", "gone", "gone", "last"}
	current := map[string]struct{}{"keep": {}, "new": {}}
	want := []string{"gone", "gone", "last"}
	if got := recoveryCandidates(cached, current); !reflect.DeepEqual(got, want) {
		t.Fatal(got)
	}
	if cached[0] != "keep" || len(current) != 2 {
		t.Fatal("inputs mutated")
	}
	if got := recoveryCandidates(nil, current); len(got) != 0 {
		t.Fatal(got)
	}
}

func TestRuleEventReadFailureCannotRecoverOrResetCache(t *testing.T) {
	e, r, events, pending := fixture(t, "complete")
	events.ruleReadError = errors.New("rule snapshot unavailable")
	runOnce(e, r)
	if events.writes != 0 || len(events.events) != 4 || pending.listReads != 0 {
		t.Fatal("recovery used incomplete rule snapshot")
	}
}

func BenchmarkRecoveryCandidates(b *testing.B) {
	for _, size := range []int{1000, 10000} {
		cached := make([]string, size)
		current := make(map[string]struct{}, size)
		for i := range cached {
			cached[i] = fmt.Sprint(i)
			current[cached[i]] = struct{}{}
		}
		b.Run(fmt.Sprintf("legacy-%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if len(tools.GetSliceDifference(cached, cached)) != 0 {
					b.Fatal("unexpected difference")
				}
			}
		})
		b.Run(fmt.Sprintf("indexed-set-%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if len(recoveryCandidates(cached, current)) != 0 {
					b.Fatal("unexpected difference")
				}
			}
		})
	}
}
