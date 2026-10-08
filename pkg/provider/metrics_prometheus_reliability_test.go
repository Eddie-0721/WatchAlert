package provider

import (
	"context"
	"errors"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"testing"
	"time"
)

type queryFixture struct {
	v1.API
	value    model.Value
	warnings v1.Warnings
	err      error
}

func (q queryFixture) Query(context.Context, string, time.Time, ...v1.Option) (model.Value, v1.Warnings, error) {
	return q.value, q.warnings, q.err
}
func TestInstantQueryRejectsIncompleteResults(t *testing.T) {
	for _, tc := range []struct {
		name    string
		fixture queryFixture
		ok      bool
	}{
		{"empty_vector", queryFixture{value: model.Vector{}}, true},
		{"sample", queryFixture{value: model.Vector{&model.Sample{Value: 1}}}, true},
		{"warning", queryFixture{value: model.Vector{}, warnings: v1.Warnings{"partial data"}}, false},
		{"timeout", queryFixture{err: context.DeadlineExceeded}, false},
		{"query_error", queryFixture{err: errors.New("failed")}, false},
		{"scalar", queryFixture{value: &model.Scalar{Value: 1}}, false},
		{"nil_sample", queryFixture{value: model.Vector{nil}}, false},
		{"nil_result", queryFixture{}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := (PrometheusProvider{client: tc.fixture, Timeout: 1}).Query("up")
			if (err == nil) != tc.ok {
				t.Fatalf("unexpected result: %v", err)
			}
		})
	}
	for _, raw := range []string{"NaN", "+Inf", "-Inf"} {
		var value model.SampleValue
		if err := value.UnmarshalJSON([]byte(`"` + raw + `"`)); err != nil {
			t.Fatal(err)
		}
		p := PrometheusProvider{client: queryFixture{value: model.Vector{&model.Sample{Value: value}}}, Timeout: 1}
		if _, err := p.Query("up"); err == nil {
			t.Fatalf("accepted %s", raw)
		}
	}
}
