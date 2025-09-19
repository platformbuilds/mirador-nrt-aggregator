package spanmetrics

import (
	"testing"

	"github.com/platformbuilds/mirador-nrt-aggregator/internal/config"

	coll "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	com "go.opentelemetry.io/proto/otlp/common/v1"
	res "go.opentelemetry.io/proto/otlp/resource/v1"
	tr "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

func TestTracesToResourceMetricsGeneratesMetrics(t *testing.T) {
	cfg := config.ProcessorCfg{
		HistogramBuckets: []float64{0.1, 0.3},
		Dimensions:       []string{"service.name", "span.kind"},
	}
	p := New(cfg)

	span := &tr.Span{
		Name:              "GET /checkout",
		StartTimeUnixNano: 1,
		EndTimeUnixNano:   1000000,
		Kind:              tr.Span_SPAN_KIND_SERVER,
		Status:            &tr.Status{Code: tr.Status_STATUS_CODE_ERROR},
		Events: []*tr.Span_Event{
			{Name: "exception"},
		},
	}

	rs := &coll.ExportTraceServiceRequest{
		ResourceSpans: []*tr.ResourceSpans{
			{
				Resource: &res.Resource{
					Attributes: []*com.KeyValue{
						{Key: "service.name", Value: &com.AnyValue{Value: &com.AnyValue_StringValue{StringValue: "checkout"}}},
					},
				},
				ScopeSpans: []*tr.ScopeSpans{{Spans: []*tr.Span{span}}},
			},
		},
	}

	payload, err := proto.Marshal(rs)
	if err != nil {
		t.Fatalf("marshal traces: %v", err)
	}

	rms := p.tracesToResourceMetrics(payload)
	if len(rms) != 1 {
		t.Fatalf("expected one ResourceMetrics, got %d", len(rms))
	}

	totalMetrics := 0
	hasErrorsMetric := false
	for _, sm := range rms[0].ScopeMetrics {
		totalMetrics += len(sm.Metrics)
		for _, m := range sm.Metrics {
			if m.GetName() == "errors_total" {
				hasErrorsMetric = true
			}
		}
	}

	if totalMetrics == 0 {
		t.Fatalf("expected generated metrics")
	}
	if !hasErrorsMetric {
		t.Fatalf("expected errors_total metric when error span present")
	}
}
