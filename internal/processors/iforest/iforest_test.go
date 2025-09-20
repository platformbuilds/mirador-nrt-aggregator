package iforest

import (
	"context"
	"testing"

	"github.com/platformbuilds/mirador-nrt-aggregator/internal/config"
	"github.com/platformbuilds/mirador-nrt-aggregator/internal/model"
)

func TestProcessorScoresAggregates(t *testing.T) {
	cfg := config.ProcessorCfg{Features: []string{"p99"}}
	p := New(cfg)
	p.forest = &Forest{Trees: []Tree{{Nodes: []Node{
		{F: 0, T: 0.1, L: 1, R: 2},
		{Leaf: true},
		{Leaf: true},
	}}}}
	p.subsampleN = 32

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	in := make(chan any, 1)
	out := make(chan any, 1)
	done := make(chan struct{})
	go func() {
		_ = p.Start(ctx, in, out)
		close(done)
	}()

	in <- model.Aggregate{Service: "svc", WindowEnd: 1, P99: 0.05}
	close(in)
	<-done

	v, ok := <-out
	if !ok {
		t.Fatalf("expected output aggregate")
	}
	agg, ok := v.(model.Aggregate)
	if !ok {
		t.Fatalf("unexpected type %T", v)
	}
	if agg.AnomalyScore <= 0 {
		t.Fatalf("expected positive anomaly score, got %f", agg.AnomalyScore)
	}
}

func TestFeaturesOfMapsKnownFields(t *testing.T) {
	p := New(config.ProcessorCfg{Features: []string{"p50", "p95", "unknown"}})
	agg := model.Aggregate{P50: 1, P95: 2}

	vec := p.featuresOf(agg)
	if len(vec) != 3 {
		t.Fatalf("expected 3 features, got %d", len(vec))
	}
	if vec[0] != agg.P50 || vec[1] != agg.P95 {
		t.Fatalf("feature mapping incorrect: %v", vec)
	}
	if vec[2] != 0 {
		t.Fatalf("unknown feature should map to 0")
	}
}

func TestScoreReturnsZeroWithoutForest(t *testing.T) {
	p := New(config.ProcessorCfg{})
	if got := p.score([]float64{1, 2, 3}); got != 0 {
		t.Fatalf("expected zero score without forest, got %f", got)
	}
}
