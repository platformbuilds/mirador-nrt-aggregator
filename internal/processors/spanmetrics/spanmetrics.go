package spanmetrics

import (
	"context"
	"log"
	"math"
	"strings"
	"time"

	"github.com/platformbuilds/mirador-nrt-aggregator/internal/config"
	"github.com/platformbuilds/mirador-nrt-aggregator/internal/model"

	collmet "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	colltr "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	com "go.opentelemetry.io/proto/otlp/common/v1"
	met "go.opentelemetry.io/proto/otlp/metrics/v1"
	res "go.opentelemetry.io/proto/otlp/resource/v1"
	tr "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

// processor converts OTLP Traces → OTLP Metrics (RED-style) and also emits errors_total.
type processor struct {
	dimensions         []string // attribute keys to propagate as metric labels
	errorEventAttrDims []string // event attribute keys to pull into error labels (e.g., "exception.type")
	errorEventNames    []string // names treated as error events, default ["exception"]
	errFromStatus      bool
	errFromEvents      bool
	histBounds         []float64 // seconds
	defaultSvcAttr     string
}

func New(cfg config.ProcessorCfg) *processor {
	bounds := cfg.HistogramBuckets
	if len(bounds) == 0 {
		bounds = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}
	}

	// Error handling config
	errFromStatus := true
	if v := cfg.Extra["error_from_status"]; v != nil {
		if b, ok := v.(bool); ok {
			errFromStatus = b
		}
	}
	errFromEvents := true
	if v := cfg.Extra["error_from_events"]; v != nil {
		if b, ok := v.(bool); ok {
			errFromEvents = b
		}
	}
	evtNames := []string{"exception"}
	if xs, ok := cfg.Extra["error_event_names"].([]any); ok && len(xs) > 0 {
		evtNames = make([]string, 0, len(xs))
		for _, it := range xs {
			if s, ok := it.(string); ok && s != "" {
				evtNames = append(evtNames, s)
			}
		}
	}
	evtAttrDims := []string{}
	if xs, ok := cfg.Extra["error_event_attr_dims"].([]any); ok && len(xs) > 0 {
		for _, it := range xs {
			if s, ok := it.(string); ok && s != "" {
				evtAttrDims = append(evtAttrDims, s)
			}
		}
	}

	return &processor{
		dimensions:         cfg.Dimensions,
		errorEventAttrDims: evtAttrDims,
		errorEventNames:    lowerSlice(evtNames),
		errFromStatus:      errFromStatus,
		errFromEvents:      errFromEvents,
		histBounds:         bounds,
		defaultSvcAttr:     "service.name",
	}
}

func (p *processor) Start(ctx context.Context, in <-chan any, out chan<- any) error {
	defer close(out)
	for {
		select {
		case <-ctx.Done():
			return nil
		case v, ok := <-in:
			if !ok {
				return nil
			}
			env, ok := v.(model.Envelope)
			if !ok || env.Kind != model.KindTraces {
				out <- v
				continue
			}
			rmList := p.tracesToResourceMetrics(env.Bytes)
			if len(rmList) == 0 {
				out <- v
				continue
			}
			em := &collmet.ExportMetricsServiceRequest{ResourceMetrics: rmList}
			b, err := proto.Marshal(em)
			if err != nil {
				log.Printf("[spanmetrics] marshal metrics error: %v", err)
				out <- v
				continue
			}
			out <- model.Envelope{Kind: model.KindMetrics, Bytes: b, Attrs: env.Attrs, TSUnix: env.TSUnix}
		}
	}
}

func (p *processor) tracesToResourceMetrics(raw []byte) []*met.ResourceMetrics {
	var et colltr.ExportTraceServiceRequest
	if err := proto.Unmarshal(raw, &et); err != nil {
		log.Printf("[spanmetrics] cannot unmarshal traces: %v", err)
		return nil
	}

	var out []*met.ResourceMetrics
	for _, rt := range et.ResourceSpans {
		rattrs := rt.GetResource()
		rAttrMap := attrsToMapRes(rattrs)

		rm := &met.ResourceMetrics{
			Resource: copyResource(rattrs),
			ScopeMetrics: []*met.ScopeMetrics{
				{Scope: &com.InstrumentationScope{Name: "mirador.spanmetrics", Version: "0.2.0"}},
			},
		}
		sm := &met.ScopeMetrics{Scope: &com.InstrumentationScope{Name: "mirador.spanmetrics", Version: "0.2.0"}}
		rm.ScopeMetrics = append(rm.ScopeMetrics, sm)

		for _, ss := range rt.ScopeSpans {
			for _, sp := range ss.Spans {
				start := uint64(sp.GetStartTimeUnixNano())
				end := safeEnd(start, sp.GetEndTimeUnixNano())
				durSec := float64(end-start) / 1e9

				// Build base labels: configured dimensions from span/resource
				baseLabels := p.buildDimensions(sp, rAttrMap)

				// requests_total (always 1 per span)
				sm.Metrics = append(sm.Metrics, p.buildRequestsTotal(int64(start), int64(end), baseLabels))

				// duration_seconds histogram
				sm.Metrics = append(sm.Metrics, p.buildDurationHistogram(int64(start), int64(end), durSec, baseLabels))

				// errors_total?
				if p.isErrorSpan(sp) {
					errLabels := baseLabels
					// If error from events, try to augment labels with requested event attributes
					if p.errFromEvents {
						if evL := p.errorEventLabels(sp); len(evL) > 0 {
							errLabels = append(copyLabels(baseLabels), evL...)
						}
					}
					sm.Metrics = append(sm.Metrics, p.buildErrorsTotal(int64(start), int64(end), errLabels))
				}
			}
		}
		if len(sm.Metrics) > 0 {
			out = append(out, rm)
		}
	}
	return out
}

// ---- error detection ----

func (p *processor) isErrorSpan(sp *tr.Span) bool {
	if p.errFromStatus && strings.EqualFold(sp.GetStatus().GetCode().String(), "ERROR") {
		return true
	}
	if p.errFromEvents {
		for _, ev := range sp.Events {
			if inLower(ev.GetName(), p.errorEventNames) {
				return true
			}
		}
	}
	return false
}

func (p *processor) errorEventLabels(sp *tr.Span) []*com.KeyValue {
	if len(p.errorEventAttrDims) == 0 {
		return nil
	}
	// Take the first matching event; this mirrors a simple heuristic to avoid cardinality blow-up
	for _, ev := range sp.Events {
		if !inLower(ev.GetName(), p.errorEventNames) {
			continue
		}
		evMap := attrsToMapSpan(ev.Attributes)
		var out []*com.KeyValue
		for _, k := range p.errorEventAttrDims {
			if v, ok := evMap[k]; ok && v != "" {
				out = append(out, strKV(k, v))
			}
		}
		if len(out) > 0 {
			return out
		}
	}
	return nil
}

// ---- metric builders ----

func (p *processor) buildRequestsTotal(tsStart, tsEnd int64, labels []*com.KeyValue) *met.Metric {
	dp := &met.NumberDataPoint{
		TimeUnixNano:      uint64(tsEnd),
		StartTimeUnixNano: uint64(tsStart),
		Attributes:        labels,
		Value:             &met.NumberDataPoint_AsDouble{AsDouble: 1},
	}
	return &met.Metric{
		Name:        "requests_total",
		Description: "Total number of spans (requests) derived from traces",
		Unit:        "1",
		Data: &met.Metric_Sum{
			Sum: &met.Sum{
				AggregationTemporality: met.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA,
				IsMonotonic:            true,
				DataPoints:             []*met.NumberDataPoint{dp},
			},
		},
	}
}

func (p *processor) buildErrorsTotal(tsStart, tsEnd int64, labels []*com.KeyValue) *met.Metric {
	dp := &met.NumberDataPoint{
		TimeUnixNano:      uint64(tsEnd),
		StartTimeUnixNano: uint64(tsStart),
		Attributes:        labels,
		Value:             &met.NumberDataPoint_AsDouble{AsDouble: 1},
	}
	return &met.Metric{
		Name:        "errors_total",
		Description: "Total number of error spans (via status or error events)",
		Unit:        "1",
		Data: &met.Metric_Sum{
			Sum: &met.Sum{
				AggregationTemporality: met.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA,
				IsMonotonic:            true,
				DataPoints:             []*met.NumberDataPoint{dp},
			},
		},
	}
}

func (p *processor) buildDurationHistogram(tsStart, tsEnd int64, durSec float64, labels []*com.KeyValue) *met.Metric {
	bidx := bucketIndex(p.histBounds, durSec)
	bCounts := make([]uint64, len(p.histBounds)+1)
	if bidx >= 0 && bidx < len(bCounts) {
		bCounts[bidx] = 1
	} else {
		bCounts[len(bCounts)-1] = 1
	}
	dp := &met.HistogramDataPoint{
		TimeUnixNano:      uint64(tsEnd),
		StartTimeUnixNano: uint64(tsStart),
		Attributes:        labels,
		ExplicitBounds:    p.histBounds,
		BucketCounts:      bCounts,
		Count:             uint64(1),
		Sum:               protoFloat64(durSec),
	}
	return &met.Metric{
		Name:        "duration_seconds",
		Description: "Span duration in seconds (histogram)",
		Unit:        "s",
		Data: &met.Metric_Histogram{
			Histogram: &met.Histogram{
				AggregationTemporality: met.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA,
				DataPoints:             []*met.HistogramDataPoint{dp},
			},
		},
	}
}

// ---- label/dimension helpers ----

func (p *processor) buildDimensions(sp *tr.Span, resAttrs map[string]string) []*com.KeyValue {
	labels := make([]*com.KeyValue, 0, len(p.dimensions)+4)
	spanAttr := attrsToMapSpan(sp.Attributes)

	// Always include span.kind/status.code if requested via dimensions list
	if in(p.dimensions, "span.kind") {
		labels = append(labels, strKV("span.kind", strings.ToLower(sp.Kind.String())))
	}
	if in(p.dimensions, "status.code") {
		labels = append(labels, strKV("status.code", strings.ToLower(sp.Status.GetCode().String())))
	}

	for _, k := range p.dimensions {
		kn := strings.TrimSpace(k)
		if kn == "" || kn == "span.kind" || kn == "status.code" {
			continue
		}
		if v, ok := spanAttr[kn]; ok && v != "" {
			labels = append(labels, strKV(kn, v))
			continue
		}
		if v, ok := resAttrs[kn]; ok && v != "" {
			labels = append(labels, strKV(kn, v))
			continue
		}
	}

	// Ensure service.name if present in resource
	if svc, ok := resAttrs[p.defaultSvcAttr]; ok && svc != "" {
		labels = append(labels, strKV(p.defaultSvcAttr, svc))
	}
	return labels
}

func attrsToMapRes(r *res.Resource) map[string]string {
	out := map[string]string{}
	if r == nil {
		return out
	}
	for _, a := range r.Attributes {
		if s := a.GetValue().GetStringValue(); s != "" {
			out[a.Key] = s
		}
	}
	return out
}

func attrsToMapSpan(xs []*com.KeyValue) map[string]string {
	out := map[string]string{}
	for _, a := range xs {
		if s := a.GetValue().GetStringValue(); s != "" {
			out[a.Key] = s
		}
	}
	return out
}

func strKV(k, v string) *com.KeyValue {
	return &com.KeyValue{
		Key: k,
		Value: &com.AnyValue{
			Value: &com.AnyValue_StringValue{StringValue: v},
		},
	}
}

func copyLabels(in []*com.KeyValue) []*com.KeyValue {
	if len(in) == 0 {
		return nil
	}
	out := make([]*com.KeyValue, len(in))
	copy(out, in)
	return out
}

func bucketIndex(bounds []float64, v float64) int {
	for i := 0; i < len(bounds); i++ {
		if v <= bounds[i] || math.IsNaN(v) {
			return i
		}
	}
	return len(bounds) // +Inf
}

func copyResource(r *res.Resource) *res.Resource {
	if r == nil {
		return nil
	}
	out := &res.Resource{Attributes: make([]*com.KeyValue, 0, len(r.Attributes))}
	for _, a := range r.Attributes {
		out.Attributes = append(out.Attributes, &com.KeyValue{
			Key: a.Key,
			Value: &com.AnyValue{
				Value: &com.AnyValue_StringValue{StringValue: a.GetValue().GetStringValue()},
			},
		})
	}
	return out
}

func protoFloat64(v float64) *float64 { x := v; return &x }

func safeEnd(start, end uint64) uint64 {
	if end == 0 {
		return uint64(time.Now().UnixNano())
	}
	if end < start {
		return start
	}
	return end
}

func in(xs []string, target string) bool {
	for _, s := range xs {
		if s == target {
			return true
		}
	}
	return false
}

func inLower(s string, xs []string) bool {
	ls := strings.ToLower(s)
	for _, t := range xs {
		if ls == t {
			return true
		}
	}
	return false
}

func lowerSlice(xs []string) []string {
	out := make([]string, len(xs))
	for i, s := range xs {
		out[i] = strings.ToLower(s)
	}
	return out
}
