package summarizer

import (
	"testing"

	"github.com/caio/go-tdigest/v4"
	"github.com/platformbuilds/mirador-nrt-aggregator/internal/config"
	"github.com/platformbuilds/mirador-nrt-aggregator/internal/model"

	coll "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	com "go.opentelemetry.io/proto/otlp/common/v1"
	met "go.opentelemetry.io/proto/otlp/metrics/v1"
	res "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/protobuf/proto"

	prompb "github.com/prometheus/prometheus/prompb"
)

func TestConsumeOTLPMetricsProducesAggregate(t *testing.T) {
	p := New(config.ProcessorCfg{WindowSeconds: 60})

	metric := &met.Metric{
		Name: "http_server_duration_seconds",
		Data: &met.Metric_Histogram{
			Histogram: &met.Histogram{
				AggregationTemporality: met.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA,
				DataPoints: []*met.HistogramDataPoint{
					{
						ExplicitBounds: []float64{0.5, 1.0},
						BucketCounts:   []uint64{3, 2, 1},
					},
				},
			},
		},
	}

	resourceMetrics := &coll.ExportMetricsServiceRequest{
		ResourceMetrics: []*met.ResourceMetrics{
			{
				Resource: &res.Resource{
					Attributes: []*com.KeyValue{
						{Key: "service.name", Value: &com.AnyValue{Value: &com.AnyValue_StringValue{StringValue: "checkout"}}},
					},
				},
				ScopeMetrics: []*met.ScopeMetrics{
					{Metrics: []*met.Metric{metric}},
				},
			},
		},
	}

	body, err := proto.Marshal(resourceMetrics)
	if err != nil {
		t.Fatalf("marshal metrics: %v", err)
	}

	p.consumeOTLPMetrics(body, 0)
	out := make(chan any, 1)
	p.flush(out, 0)

	agg, ok := (<-out).(model.Aggregate)
	if !ok {
		t.Fatalf("expected aggregate from flush")
	}
	if agg.Service != "checkout" {
		t.Fatalf("service mismatch: %v", agg.Service)
	}
	if agg.P50 == 0 && agg.P95 == 0 {
		t.Fatalf("expected histogram quantiles to be populated")
	}
}

func TestConsumePromRWUpdatesCounters(t *testing.T) {
	p := New(config.ProcessorCfg{WindowSeconds: 60})
	d, _ := tdigest.New()
	p.state["svc"] = &svc{td: d}

	wr := &prompb.WriteRequest{Timeseries: []prompb.TimeSeries{
		{
			Labels: []prompb.Label{
				{Name: "__name__", Value: "http_requests_total"},
				{Name: "service.name", Value: "svc"},
			},
			Samples: []prompb.Sample{{Value: 5}},
		},
		{
			Labels: []prompb.Label{
				{Name: "__name__", Value: "http_errors_total"},
				{Name: "service.name", Value: "svc"},
			},
			Samples: []prompb.Sample{{Value: 2}},
		},
	}}
	body, err := wr.Marshal()
	if err != nil {
		t.Fatalf("marshal write request: %v", err)
	}

	p.consumePromRW(body, 0)
	st := p.state["svc"]
	if st.req <= 0 || st.err <= 0 {
		t.Fatalf("expected counters to increment: %+v", st)
	}
	if st.td == nil {
		t.Fatalf("tdigest should remain initialised")
	}
}
