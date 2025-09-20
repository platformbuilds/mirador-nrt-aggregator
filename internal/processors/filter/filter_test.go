package filter

import (
	"context"
	"testing"

	"github.com/platformbuilds/mirador-nrt-aggregator/internal/config"
	"github.com/platformbuilds/mirador-nrt-aggregator/internal/model"
)

func TestFilterMetricsPreStageDropsNonMatching(t *testing.T) {
	cfg := config.ProcessorCfg{Extra: map[string]any{
		"stage":             "pre",
		"on":                "metrics",
		"expr":              "attrs[\"env\"] == \"prod\"",
		"drop_non_matching": true,
	}}
	p := New(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	in := make(chan any, 2)
	out := make(chan any, 2)
	done := make(chan struct{})
	go func() {
		_ = p.Start(ctx, in, out)
		close(done)
	}()

	in <- model.Envelope{Kind: model.KindMetrics, Attrs: map[string]string{"env": "prod"}}
	in <- model.Envelope{Kind: model.KindMetrics, Attrs: map[string]string{"env": "dev"}}
	close(in)
	<-done

	var kept []model.Envelope
	for v := range out {
		if env, ok := v.(model.Envelope); ok {
			kept = append(kept, env)
		}
	}

	if len(kept) != 1 {
		t.Fatalf("expected only prod envelope kept, got %d", len(kept))
	}
	if kept[0].Attrs["env"] != "prod" {
		t.Fatalf("unexpected envelope kept: %+v", kept[0])
	}
}

func TestFilterAggregatesPostStageKeepsWhenMatching(t *testing.T) {
	cfg := config.ProcessorCfg{Extra: map[string]any{
		"stage":             "post",
		"on":                "aggregates",
		"expr":              "error_rate > 0.1",
		"drop_non_matching": true,
	}}
	p := New(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	in := make(chan any, 2)
	out := make(chan any, 2)
	done := make(chan struct{})
	go func() {
		_ = p.Start(ctx, in, out)
		close(done)
	}()

	in <- model.Aggregate{Service: "svc", ErrorRate: 0.2}
	in <- model.Aggregate{Service: "svc", ErrorRate: 0.05}
	close(in)
	<-done

	var kept []model.Aggregate
	for v := range out {
		if agg, ok := v.(model.Aggregate); ok {
			kept = append(kept, agg)
		}
	}
	if len(kept) != 1 {
		t.Fatalf("expected one aggregate kept, got %d", len(kept))
	}
	if kept[0].ErrorRate <= 0.1 {
		t.Fatalf("non-matching aggregate kept: %+v", kept[0])
	}
}
