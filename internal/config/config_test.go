package config_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/platformbuilds/mirador-nrt-aggregator/internal/config"
)

func TestLoadNormalizesCompositeKeys(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "cfg.yaml")
	yaml := `receivers:
  otlpgrpc:
    endpoint: ":4317"
  jsonlogs/http:
    endpoint: "0.0.0.0:9000"
processors:
  spanmetrics/default:
    histogram_buckets: [0.1, 0.3]
exporters:
  weaviate:
    endpoint: "http://weaviate:8080"
pipelines:
  traces:
    receivers: [otlpgrpc]
    processors: [spanmetrics/default]
    exporters: [weaviate]
`
	if err := os.WriteFile(cfgPath, []byte(yaml), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := config.Load(cfgPath)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}

	rc, ok := cfg.Receivers["otlpgrpc"]
	if !ok {
		t.Fatalf("receiver otlpgrpc not found")
	}
	if rc.Type != "otlpgrpc" {
		t.Fatalf("want receiver type otlpgrpc, got %q", rc.Type)
	}
	if rc.Name != "otlpgrpc" {
		t.Fatalf("want receiver name otlpgrpc, got %q", rc.Name)
	}

	jl, ok := cfg.Receivers["jsonlogs/http"]
	if !ok {
		t.Fatalf("composite receiver key not loaded")
	}
	if jl.Type != "jsonlogs" {
		t.Fatalf("want type jsonlogs, got %q", jl.Type)
	}
	if jl.Name != "http" {
		t.Fatalf("want name http, got %q", jl.Name)
	}

	proc, ok := cfg.Processors["spanmetrics/default"]
	if !ok {
		t.Fatalf("processor key missing")
	}
	if proc.Type != "spanmetrics" || proc.Name != "default" {
		t.Fatalf("receiver type/name normalization failed: %+v", proc)
	}

	exp, ok := cfg.Exporters["weaviate"]
	if !ok || exp.Type != "weaviate" || exp.Name != "weaviate" {
		t.Fatalf("exporter normalization failed: %+v", exp)
	}

	if got := len(cfg.Pipelines); got != 1 {
		t.Fatalf("expected 1 pipeline, got %d", got)
	}
}

func TestProcessorExtrasHelpers(t *testing.T) {
	pc := config.ProcessorCfg{Extra: map[string]any{
		"string_val": "hello",
		"bool_true":  true,
	}}

	if got := pc.ExtraString("string_val", ""); got != "hello" {
		t.Fatalf("ExtraString returned %q", got)
	}
	if got := pc.ExtraString("missing", "fallback"); got != "fallback" {
		t.Fatalf("ExtraString default failed, got %q", got)
	}
	if got := pc.ExtraBool("bool_true", false); !got {
		t.Fatalf("ExtraBool expected true")
	}
	if got := pc.ExtraBool("missing", true); !got {
		t.Fatalf("ExtraBool default failed")
	}
}

func TestResolvePath(t *testing.T) {
	cfgPath := filepath.Join("/etc", "mirador", "config.yaml")
	if got := config.ResolvePath(cfgPath, "relative.txt"); got != filepath.Join("/etc", "mirador", "relative.txt") {
		t.Fatalf("ResolvePath relative mismatch: %q", got)
	}
	if got := config.ResolvePath(cfgPath, "/tmp/absolute.txt"); got != "/tmp/absolute.txt" {
		t.Fatalf("ResolvePath absolute mismatch: %q", got)
	}
}
