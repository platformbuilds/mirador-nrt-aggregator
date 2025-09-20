package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/platformbuilds/mirador-nrt-aggregator/internal/config"
	"github.com/platformbuilds/mirador-nrt-aggregator/internal/model"
	kafkago "github.com/segmentio/kafka-go"
)

func TestNewDefaults(t *testing.T) {
	rc := config.ReceiverCfg{
		Brokers: []string{"b1"},
		Topic:   "t",
		Extra:   map[string]any{"ndjson": true},
	}
	r := New(rc, "jsonlogs")
	if r.maxBytes != 10*1024*1024 {
		t.Fatalf("unexpected default maxBytes: %d", r.maxBytes)
	}
	if !r.ndjson {
		t.Fatal("expected ndjson true from extras")
	}
	if r.kind != "json_logs" {
		t.Fatalf("expected normalized kind json_logs, got %s", r.kind)
	}
}

func TestNewOverrides(t *testing.T) {
	rc := config.ReceiverCfg{
		Brokers: []string{"b1"},
		Topic:   "t",
		Extra:   map[string]any{"max_bytes": 123},
	}
	r := New(rc, "metrics")
	if r.maxBytes != 123 {
		t.Fatalf("expected maxBytes 123, got %d", r.maxBytes)
	}
}

func TestNormalizeKind(t *testing.T) {
	cases := map[string]string{
		"metrics":               "metrics",
		"traces":                "traces",
		"promremotewrite":       "prom_rw",
		"json":                  "json_logs",
		"unknown":               "metrics",
		"PrometheusRemoteWrite": "prom_rw",
	}
	for in, want := range cases {
		if got := normalizeKind(in); got != want {
			t.Fatalf("normalizeKind(%q)=%q, want %q", in, got, want)
		}
	}
}

func TestGroupOrDefault(t *testing.T) {
	r := &Receiver{group: "  custom  "}
	if g := r.groupOrDefault(); g != "custom" {
		t.Fatalf("expected trimmed custom, got %q", g)
	}
	r.group = ""
	if g := r.groupOrDefault(); g != "mirador-nrt-aggregator" {
		t.Fatalf("expected default group, got %q", g)
	}
}

func TestHeadersToMap(t *testing.T) {
	hdrs := []kafkago.Header{{Key: "k1", Value: []byte("v1")}}
	m := headersToMap(hdrs)
	if m["k1"] != "v1" {
		t.Fatalf("expected v1, got %q", m["k1"])
	}
	empty := headersToMap(nil)
	if len(empty) != 0 {
		t.Fatalf("expected empty map, got %v", empty)
	}
}

func TestStartValidatesConfiguration(t *testing.T) {
	r := New(config.ReceiverCfg{}, "metrics")
	if err := r.Start(context.Background(), make(chan model.Envelope)); err == nil {
		t.Fatal("expected error for missing brokers/topic")
	}
}

func TestStartStopsOnCancelledContext(t *testing.T) {
	rc := config.ReceiverCfg{
		Brokers: []string{"localhost:1"},
		Topic:   "topic",
	}
	r := New(rc, "metrics")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := r.Start(ctx, make(chan model.Envelope)); err != nil {
			t.Errorf("Start returned error: %v", err)
		}
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not return after cancellation")
	}
}
