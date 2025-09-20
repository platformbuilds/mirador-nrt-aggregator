package otlplogs

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/platformbuilds/mirador-nrt-aggregator/internal/config"
	"github.com/platformbuilds/mirador-nrt-aggregator/internal/model"

	colllog "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	com "go.opentelemetry.io/proto/otlp/common/v1"
	logs "go.opentelemetry.io/proto/otlp/logs/v1"
	res "go.opentelemetry.io/proto/otlp/resource/v1"
)

func TestFlattenAndEmitProducesJSONEnvelope(t *testing.T) {
	cfg := config.ProcessorCfg{Extra: map[string]any{
		"resource_attrs": true,
		"scope_attrs":    true,
		"level_alias":    "level",
	}}
	p := New(cfg)

	req := &colllog.ExportLogsServiceRequest{
		ResourceLogs: []*logs.ResourceLogs{
			{
				Resource: &res.Resource{Attributes: []*com.KeyValue{{
					Key:   "service.name",
					Value: &com.AnyValue{Value: &com.AnyValue_StringValue{StringValue: "checkout"}},
				}}},
				ScopeLogs: []*logs.ScopeLogs{
					{
						Scope: &com.InstrumentationScope{Name: "scope", Version: "v1"},
						LogRecords: []*logs.LogRecord{
							{
								TimeUnixNano:   123,
								SeverityText:   "ERROR",
								SeverityNumber: logs.SeverityNumber_SEVERITY_NUMBER_ERROR,
								TraceId:        []byte{0x01, 0x02},
								SpanId:         []byte{0x03, 0x04},
								Attributes: []*com.KeyValue{{
									Key:   "customer.id",
									Value: &com.AnyValue{Value: &com.AnyValue_StringValue{StringValue: "123"}},
								}},
							},
						},
					},
				},
			},
		},
	}

	src := model.Envelope{Kind: model.KindJSONLogs, Attrs: map[string]string{"header": "value"}}
	out := make(chan any, 1)

	p.flattenAndEmit(context.Background(), req, src, out)

	select {
	case v := <-out:
		env, ok := v.(model.Envelope)
		if !ok {
			t.Fatalf("unexpected type %T", v)
		}
		if env.Kind != model.KindJSONLogs {
			t.Fatalf("expected json logs kind")
		}
		if env.Attrs["header"] != "value" {
			t.Fatalf("expected attrs to be preserved")
		}

		var m map[string]any
		if err := json.Unmarshal(env.Bytes, &m); err != nil {
			t.Fatalf("unmarshal flattened json: %v", err)
		}
		if got := m["severity_text"]; got != "ERROR" {
			t.Fatalf("severity missing: %v", got)
		}
		if got := m["level"]; got != "error" {
			t.Fatalf("level alias not applied: %v", got)
		}
		if got := m["resource.service.name"]; got != "checkout" {
			t.Fatalf("resource attribute missing: %v", got)
		}
		if got := m["scope.name"]; got != "scope" {
			t.Fatalf("scope name missing: %v", got)
		}
		if got := m["trace_id"]; got != "0102" {
			t.Fatalf("trace id not hex encoded: %v", got)
		}
	default:
		t.Fatalf("expected envelope output")
	}
}
