package summarizer

import (
	"context"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/caio/go-tdigest"
	"github.com/yourorg/mirador-nrt-aggregator/internal/config"
	"github.com/yourorg/mirador-nrt-aggregator/internal/model"

	prompb "github.com/prometheus/prometheus/prompb"
	"google.golang.org/protobuf/proto"

	coll "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	com "go.opentelemetry.io/proto/otlp/common/v1"
	met "go.opentelemetry.io/proto/otlp/metrics/v1"
	resv1 "go.opentelemetry.io/proto/otlp/resource/v1"
)

// processor aggregates metrics into fixed-size windows using a t-digest
// for latency percentiles and simple counters for RPS & error-rate.
type processor struct {
	windowSec        int
	svcAttr          string          // attribute to identify service (default "service.name")
	bucketSampleCap  int             // cap synthetic samples per bucket to bound cost (default 50)
	state            map[string]*svc // per service state for current window
	last             map[string]float64
	acceptOTLP       bool
	acceptPromRemote bool
}

type svc struct {
	td    *tdigest.TDigest
	req   float64
	ok    float64
	err   float64
	count uint64
	// You can stash useful facets here in future (e.g., top routes) and pass in Labels.
	labels map[string]string
}

func New(cfg config.ProcessorCfg) *processor {
	w := cfg.WindowSeconds
	if w <= 0 {
		w = 60
	}
	svcAttr := cfg.ExtraString("service_attribute", "service.name")
	cap := 50
	if v, ok := cfg.Extra["bucket_sample_cap"]; ok {
		switch t := v.(type) {
		case int:
			if t > 0 {
				cap = t
			}
		case float64:
			if t > 0 {
				cap = int(t)
			}
		case string:
			if n, err := strconv.Atoi(t); err == nil && n > 0 {
				cap = n
			}
		}
	}
	return &processor{
		windowSec:        w,
		svcAttr:          svcAttr,
		bucketSampleCap:  cap,
		state:            map[string]*svc{},
		last:             map[string]float64{},
		acceptOTLP:       true,
		acceptPromRemote: true,
	}
}

func (p *processor) Start(ctx context.Context, in <-chan any, out chan<- any) error {
	defer close(out)

	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	winStart := trunc(time.Now().Unix(), int64(p.windowSec))

	for {
		select {
		case <-ctx.Done():
			return nil

		case v, ok := <-in:
			if !ok {
				return nil
			}
			env, ok := v.(model.Envelope)
			if !ok {
				// pass through unknown items
				out <- v
				continue
			}
			switch env.Kind {
			case model.KindMetrics:
				if p.acceptOTLP {
					p.consumeOTLPMetrics(env.Bytes, winStart)
				}
			case model.KindPromRW:
				if p.acceptPromRemote {
					p.consumePromRW(env.Bytes, winStart)
				}
			default:
				// Not a metrics envelope → pass along
				out <- v
			}

		case now := <-ticker.C:
			if now.Unix() >= winStart+int64(p.windowSec) {
				p.flush(out, winStart)
				winStart = trunc(now.Unix(), int64(p.windowSec))
			}
		}
	}
}

func (p *processor) flush(out chan<- any, winStart int64) {
	winEnd := winStart + int64(p.windowSec)
	for svcName, st := range p.state {
		// Quantiles
		var p50, p95, p99 float64
		if st.td != nil && st.td.Count() > 0 {
			p50 = st.td.Quantile(0.50)
			p95 = st.td.Quantile(0.95)
			p99 = st.td.Quantile(0.99)
		}
		// Rates
		rps := st.req / float64(p.windowSec)
		errRate := 0.0
		if total := st.ok + st.err; total > 0 {
			errRate = st.err / total
		}

		agg := model.Aggregate{
			Service:     svcName,
			WindowStart: winStart,
			WindowEnd:   winEnd,
			Count:       st.count,
			P50:         p50,
			P95:         p95,
			P99:         p99,
			RPS:         rps,
			ErrorRate:   errRate,
			Labels:      st.labels,
			Locator:     "{}",
			SummaryText: buildSummaryText(svcName, rps, errRate, st.count),
		}
		out <- agg
	}
	// reset window state (not the delta map)
	p.state = map[string]*svc{}
}

// ---------------- OTLP Metrics ----------------

func (p *processor) consumeOTLPMetrics(raw []byte, winStart int64) {
	var em coll.ExportMetricsServiceRequest
	if err := proto.Unmarshal(raw, &em); err != nil {
		log.Printf("[summarizer] failed to unmarshal OTLP metrics: %v", err)
		return
	}
	for _, rm := range em.ResourceMetrics {
		resAttrs := attrsToMap(rm.GetResource())
		svcName := firstNonEmpty(resAttrs[p.svcAttr], resAttrs["service"], resAttrs["service.name"], "unknown")
		st := p.ensureSvc(svcName)

		for _, sm := range rm.ScopeMetrics {
			for _, m := range sm.Metrics {
				switch d := m.Data.(type) {
				case *met.Metric_Sum:
					p.consumeSum(m.GetName(), resAttrs, d.Sum, st)
				case *met.Metric_Histogram:
					p.consumeHistogram(m.GetName(), resAttrs, d.Histogram, st)
				case *met.Metric_ExponentialHistogram:
					// Not supported yet: skip
				default:
					// Gauge or Summary → ignore for NRT RED
				}
			}
		}
	}
}

func (p *processor) consumeSum(name string, res map[string]string, s *met.Sum, st *svc) {
	// We treat SUM datapoints as counters, compute delta by series key
	isDelta := s.GetAggregationTemporality() == met.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA
	for _, dp := range s.GetDataPoints() {
		val := numberOf(dp)
		if !isDelta {
			key := seriesKey(res, name, dp.GetAttributes())
			val = delta(p, key, val)
		}
		if strings.HasSuffix(name, "requests_total") {
			st.req += val
			// error OR ok decision based on status code attr if present
			code := getAttr(dp.GetAttributes(), "status.code", "")
			if code == "" {
				code = getAttr(dp.GetAttributes(), "status_code", "")
			}
			if code != "" && !isOKStatus(code) {
				st.err += val
			} else {
				st.ok += val
			}
			st.count += uint64(maxf(val, 0))
		}
		if strings.HasSuffix(name, "errors_total") {
			st.err += val
			st.req += val // errors imply requests too, to keep rate stable
			st.count += uint64(maxf(val, 0))
		}
	}
}

func (p *processor) consumeHistogram(name string, res map[string]string, h *met.Histogram, st *svc) {
	isDelta := h.GetAggregationTemporality() == met.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA
	for _, dp := range h.GetDataPoints() {
		bounds := dp.GetExplicitBounds()
		counts := make([]float64, len(dp.GetBucketCounts()))
		if isDelta {
			for i, c := range dp.GetBucketCounts() {
				counts[i] = float64(c)
			}
		} else {
			// cumulative → delta using per-bucket series key
			for i, c := range dp.GetBucketCounts() {
				key := seriesKey(res, name+":bucket:"+strconv.Itoa(i), dp.GetAttributes())
				counts[i] = delta(p, key, float64(c))
			}
		}
		// Add capped number of representative samples per bucket upper bound
		for i := 0; i < len(counts) && i < len(bounds); i++ {
			reps := int(minf(counts[i], float64(p.bucketSampleCap)))
			if reps <= 0 {
				continue
			}
			ub := bounds[i]
			for j := 0; j < reps; j++ {
				st.td.Add(ub, 1)
			}
			st.count += uint64(reps)
		}
	}
}

// ---------------- Prometheus Remote Write ----------------

func (p *processor) consumePromRW(raw []byte, winStart int64) {
	var wr prompb.WriteRequest
	if err := proto.Unmarshal(raw, &wr); err != nil {
		log.Printf("[summarizer] failed to unmarshal PromRW: %v", err)
		return
	}
	for _, ts := range wr.Timeseries {
		lbls := labelsToMap(ts.Labels)
		name := lbls["__name__"]
		svcName := firstNonEmpty(lbls[p.svcAttr], lbls["service.name"], lbls["service"], lbls["job"], "unknown")
		st := p.ensureSvc(svcName)

		switch {
		case strings.HasSuffix(name, "_duration_seconds_bucket"):
			le := lbls["le"]
			if le == "" {
				continue
			}
			ub, err := strconv.ParseFloat(le, 64)
			if err != nil {
				continue
			}
			// Treat sample values as deltas (common with VM/Agent); cap to avoid hot loops
			for _, s := range ts.Samples {
				reps := int(minf(s.Value, float64(p.bucketSampleCap)))
				for j := 0; j < reps; j++ {
					st.td.Add(ub, 1)
				}
				st.count += uint64(maxf(s.Value, 0))
			}

		case strings.HasSuffix(name, "_requests_total"):
			code := firstNonEmpty(lbls["status_code"], lbls["code"])
			for _, s := range ts.Samples {
				v := maxf(s.Value, 0)
				st.req += v
				if code != "" && !isOKStatus(code) {
					st.err += v
				} else {
					st.ok += v
				}
				st.count += uint64(v)
			}

		case strings.HasSuffix(name, "_errors_total"):
			for _, s := range ts.Samples {
				v := maxf(s.Value, 0)
				st.err += v
				st.req += v
				st.count += uint64(v)
			}
		}
	}
}

// ---------------- helpers ----------------

func (p *processor) ensureSvc(name string) *svc {
	if s, ok := p.state[name]; ok {
		return s
	}
	ns := &svc{
		td:     tdigest.New(),
		labels: map[string]string{},
	}
	p.state[name] = ns
	return ns
}

func trunc(ts int64, win int64) int64 { return ts - (ts % win) }

func buildSummaryText(svc string, rps, errRate float64, count uint64) string {
	return "summary service=" + svc +
		" rps=" + ftoa(rps) +
		" error_rate=" + ftoa(errRate) +
		" count=" + strconv.FormatUint(count, 10)
}

func ftoa(f float64) string {
	// compact float format
	return strconv.FormatFloat(f, 'f', 6, 64)
}

func attrsToMap(r *resv1.Resource) map[string]string {
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

func getAttr(xs []*com.KeyValue, key, def string) string {
	for _, a := range xs {
		if a.Key == key {
			if s := a.GetValue().GetStringValue(); s != "" {
				return s
			}
		}
	}
	return def
}

func numberOf(dp *met.NumberDataPoint) float64 {
	switch v := dp.Value.(type) {
	case *met.NumberDataPoint_AsInt:
		return float64(v.AsInt)
	case *met.NumberDataPoint_AsDouble:
		return v.AsDouble
	default:
		return 0
	}
}

func seriesKey(res map[string]string, name string, attrs []*com.KeyValue) string {
	var b strings.Builder
	b.WriteString(name)
	b.WriteByte('|')
	if s := res["service.name"]; s != "" {
		b.WriteString(s)
	}
	for _, a := range attrs {
		b.WriteByte('|')
		b.WriteString(a.Key)
		b.WriteByte('=')
		b.WriteString(a.GetValue().GetStringValue())
	}
	return b.String()
}

func delta(p *processor, key string, cur float64) float64 {
	prev := p.last[key]
	d := cur - prev
	if d < 0 {
		d = 0
	}
	p.last[key] = cur
	return d
}

func firstNonEmpty(ss ...string) string {
	for _, s := range ss {
		if s != "" {
			return s
		}
	}
	return ""
}

func labelsToMap(lbls []prompb.Label) map[string]string {
	m := make(map[string]string, len(lbls))
	for _, l := range lbls {
		m[l.Name] = l.Value
	}
	return m
}

func isOKStatus(code string) bool {
	// normalize a few common encodings
	c := strings.ToLower(code)
	switch c {
	case "ok", "unset", "2xx", "200", "201", "202", "204":
		return true
	default:
		// treat anything starting with "2" as ok
		return strings.HasPrefix(c, "2")
	}
}

func maxf(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}
func minf(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}
