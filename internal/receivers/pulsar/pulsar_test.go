package pulsar

import (
	"testing"

	"github.com/platformbuilds/mirador-nrt-aggregator/internal/config"
)

func TestNewSetsDefaultsAndKindNormalization(t *testing.T) {
	rc := config.ReceiverCfg{
		Endpoint: "pulsar://host:6650",
		Topic:    "topic",
		Group:    "sub",
		Extra: map[string]any{
			"subscription_type": "exclusive",
			"ndjson":            true,
		},
	}
	r := New(rc, "metrics")
	if r.serviceURL != "pulsar://host:6650" {
		t.Fatalf("unexpected serviceURL %q", r.serviceURL)
	}
	if r.kind != "metrics" {
		t.Fatalf("expected kind metrics, got %q", r.kind)
	}
	if !r.ndjson {
		t.Fatalf("expected ndjson true")
	}
}

func TestNormalizeKind(t *testing.T) {
	cases := map[string]string{
		"metrics":         "metrics",
		"traces":          "traces",
		"promremotewrite": "prom_rw",
		"json":            "json_logs",
		"unknown":         "metrics",
	}
	for in, expected := range cases {
		if got := normalizeKind(in); got != expected {
			t.Fatalf("normalizeKind(%q)=%q want %q", in, got, expected)
		}
	}
}

func TestPropsToMap(t *testing.T) {
	props := map[string]string{"k1": "v1", "k2": "v2"}
	out := propsToMap(props)
	if len(out) != 2 || out["k1"] != "v1" || out["k2"] != "v2" {
		t.Fatalf("unexpected props map: %#v", out)
	}
	if len(propsToMap(nil)) != 0 {
		t.Fatalf("expected empty map for nil input")
	}
}
