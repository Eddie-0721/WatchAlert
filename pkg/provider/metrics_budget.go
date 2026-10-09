package provider

import (
	"context"
	"fmt"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"io"
	"time"
	"watchAlert/internal/models"
)

type QueryBudget struct {
	MaxSamples int
	MaxBytes   int64
}
type prometheusBodyBudgetKey struct{}
type budgetBody struct {
	io.ReadCloser
	remaining int64
}

func (b *budgetBody) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	// Read at most one extra byte to distinguish an exact-size response from overflow.
	if int64(len(p)) > b.remaining+1 {
		p = p[:b.remaining+1]
	}
	n, err := b.ReadCloser.Read(p)
	if int64(n) > b.remaining {
		return 0, fmt.Errorf("Prometheus response exceeds byte budget; narrow the query")
	}
	b.remaining -= int64(n)
	return n, err
}
func (b QueryBudget) options(timeout int64) []v1.Option {
	options := []v1.Option{v1.WithTimeout(time.Duration(timeout) * time.Second)}
	if b.MaxSamples > 0 {
		options = append(options, v1.WithLimit(uint64(b.MaxSamples)+1))
	}
	return options
}
func (v PrometheusProvider) queryContext(parent context.Context, budget QueryBudget) (context.Context, context.CancelFunc) {
	if budget.MaxBytes > 0 {
		parent = context.WithValue(parent, prometheusBodyBudgetKey{}, budget.MaxBytes)
	}
	return context.WithTimeout(parent, time.Duration(v.Timeout)*time.Second)
}

// BoundedPrometheusQuery limits response bytes before JSON decoding and total
// samples before flattening. Exceeding a budget is an error, never empty data.
func BoundedPrometheusQuery(ctx context.Context, source models.AlertDataSource, query string, start, end time.Time, step time.Duration, budget QueryBudget) ([]Metrics, error) {
	if budget.MaxSamples <= 0 || budget.MaxBytes <= 0 {
		return nil, fmt.Errorf("query budget must be positive")
	}
	client, err := newPrometheusProvider(source)
	if err != nil {
		return nil, err
	}
	if step == 0 {
		return client.query(ctx, query, budget)
	}
	return client.queryRange(ctx, query, start, end, step, budget)
}
