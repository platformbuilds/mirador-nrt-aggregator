package logsum

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/platformbuilds/mirador-nrt-aggregator/internal/config"
	"github.com/platformbuilds/mirador-nrt-aggregator/internal/model"
)

func TestProcessorConsumeAndFlush(t *testing.T) {
	cfg := config.ProcessorCfg{
		WindowSeconds: 60,
		Extra: map[string]any{
			"quantile_field": "latency_ms",
			"topk_fields":    []any{"endpoint"},
			"user_id_fields": []any{"user_id"},
			"error_levels":   []any{"error"},
		},
	}

	p := New(cfg)
	winStart := time.Now().Unix()

	logEvent := map[string]any{
		"service":    "orders",
		"level":      "error",
		"latency_ms": 150.0,
		"endpoint":   "/checkout",
		"user_id":    "u-1",
	}
	raw, _ := json.Marshal(logEvent)
	p.consume(raw, winStart)

	out := make(chan any, 1)
	p.flush(out, winStart)

	select {
	case item := <-out:
		agg, ok := item.(model.Aggregate)
		if !ok {
			t.Fatalf("flush emitted unexpected type %T", item)
		}
		if agg.Service != "orders" {
			t.Fatalf("unexpected service %q", agg.Service)
		}
		if agg.Count == 0 {
			t.Fatalf("expected count > 0")
		}
		if agg.ErrorRate < 0.9 {
			t.Fatalf("expected high error rate, got %f", agg.ErrorRate)
		}
		if agg.P95 == 0 {
			t.Fatalf("latency quantiles not recorded")
		}
		if got := agg.Labels["top_endpoint"]; got == "" {
			t.Fatalf("top endpoint label missing")
		}
		if got := agg.Labels["unique_users"]; got != "1" {
			t.Fatalf("expected 1 unique user, got %q", got)
		}
	default:
		t.Fatalf("flush did not emit aggregate")
	}
}
