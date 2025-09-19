package vectorizer

import (
	"strings"
	"testing"
	"time"

	"github.com/platformbuilds/mirador-nrt-aggregator/internal/config"
	"github.com/platformbuilds/mirador-nrt-aggregator/internal/model"
)

func TestBuildMetricsVectorNormalizesOutput(t *testing.T) {
	cfg := config.ProcessorCfg{
		Extra: map[string]any{
			"metrics": map[string]any{
				"p90_approx": false,
			},
		},
	}
	p := New(cfg)
	agg := model.Aggregate{
		Service:     "svc",
		WindowStart: time.Now().Unix(),
		WindowEnd:   time.Now().Unix() + 60,
		P50:         0.1,
		P95:         0.3,
		P99:         0.5,
		RPS:         100,
		ErrorRate:   0.05,
		Count:       600,
	}

	vec := p.buildMetricsVector(agg)
	if len(vec) == 0 {
		t.Fatal("vector should not be empty")
	}
	var sum float64
	for _, v := range vec {
		sum += float64(v) * float64(v)
	}
	if sum == 0 {
		t.Fatalf("vector not normalised")
	}
}

func TestBuildLogsTextIncludesSummary(t *testing.T) {
	cfg := config.ProcessorCfg{
		Extra: map[string]any{
			"logs": map[string]any{
				"include_fields": []any{"status"},
			},
		},
	}
	p := New(cfg)
	agg := model.Aggregate{
		Service:     "svc",
		WindowStart: 10,
		WindowEnd:   70,
		SummaryText: "summary text",
		Labels: map[string]string{
			"source":  "logs",
			"status":  "200",
			"success": "true",
		},
	}

	text := p.buildLogsText(agg)
	if text == "" {
		t.Fatalf("logs text should not be empty")
	}
	if !contains(text, "status=200") {
		t.Fatalf("expected status field in text: %s", text)
	}
	if !contains(text, "summary=summary text") {
		t.Fatalf("expected summary in text: %s", text)
	}
}

func contains(s, sub string) bool {
	return strings.Contains(s, sub)
}
