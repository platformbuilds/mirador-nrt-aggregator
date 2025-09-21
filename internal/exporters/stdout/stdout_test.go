package stdout

import (
	"bytes"
	"context"
	"log"
	"math"
	"strings"
	"testing"

	"github.com/platformbuilds/mirador-nrt-aggregator/internal/config"
	"github.com/platformbuilds/mirador-nrt-aggregator/internal/model"
)

func TestNewDefaults(t *testing.T) {
	exp := New(config.ExporterCfg{})
	if exp.pretty {
		t.Fatalf("expected pretty=false by default")
	}
	if !exp.includeVector {
		t.Fatalf("expected includeVector=true by default")
	}
}

func TestNewHonorsExtras(t *testing.T) {
	exp := New(config.ExporterCfg{Extra: map[string]any{
		"pretty":         true,
		"include_vector": false,
	}})
	if !exp.pretty {
		t.Fatalf("expected pretty=true")
	}
	if exp.includeVector {
		t.Fatalf("expected includeVector=false")
	}
}

func TestStartLogsAggregate(t *testing.T) {
	exp := New(config.ExporterCfg{})
	var buf bytes.Buffer
	exp.logger = log.New(&buf, "", 0)

	ch := make(chan model.Aggregate, 1)
	ch <- model.Aggregate{
		Service:     "svc",
		SummaryText: "hello",
		WindowStart: 1,
		WindowEnd:   2,
		Vector:      []float32{0.1, 0.2},
	}
	close(ch)

	if err := exp.Start(context.Background(), ch); err != nil {
		t.Fatalf("start returned error: %v", err)
	}

	out := buf.String()
	if !strings.Contains(out, "svc") {
		t.Fatalf("expected log output to contain service, got %q", out)
	}
	if !strings.Contains(out, "hello") {
		t.Fatalf("expected log output to contain summary, got %q", out)
	}
	if !strings.Contains(out, "0.1") {
		t.Fatalf("expected log output to contain vector components, got %q", out)
	}
}

func TestPrintAggregateHonorsPrettyAndVectorFlags(t *testing.T) {
	exp := New(config.ExporterCfg{Extra: map[string]any{
		"pretty":         true,
		"include_vector": false,
	}})
	var buf bytes.Buffer
	exp.logger = log.New(&buf, "", 0)

	exp.printAggregate(model.Aggregate{Service: "svc", SummaryText: "ok"})

	out := buf.String()
	if !strings.Contains(out, "\n  \"service\"") {
		t.Fatalf("expected pretty output, got %q", out)
	}
	if strings.Contains(out, "vector") {
		t.Fatalf("vector should be omitted when include_vector=false: %q", out)
	}
}

func TestPrintAggregateLogsMarshalErrors(t *testing.T) {
	exp := New(config.ExporterCfg{})
	var buf bytes.Buffer
	exp.logger = log.New(&buf, "", 0)

	agg := model.Aggregate{Service: "svc", P99: math.NaN()}
	exp.printAggregate(agg)

	out := buf.String()
	if !strings.Contains(out, "marshal failed") {
		t.Fatalf("expected marshal failure to be logged, got %q", out)
	}
}

func TestStartStopsOnContextCancel(t *testing.T) {
	exp := New(config.ExporterCfg{})
	var buf bytes.Buffer
	exp.logger = log.New(&buf, "", 0)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := make(chan model.Aggregate)

	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := exp.Start(ctx, ch); err != nil {
			t.Errorf("start returned error: %v", err)
		}
	}()

	cancel()

	<-done
}

func TestExtraBoolCoversStringValues(t *testing.T) {
	cases := []struct {
		in       any
		expected bool
	}{
		{true, true},
		{false, false},
		{"true", true},
		{"false", false},
		{"other", false},
	}

	for _, tc := range cases {
		if got := extraBool(map[string]any{"k": tc.in}, "k", false); got != tc.expected {
			t.Fatalf("extraBool(%v) = %v, want %v", tc.in, got, tc.expected)
		}
	}

	if extraBool(nil, "k", true) != true {
		t.Fatalf("expected default value when map is nil")
	}
}
