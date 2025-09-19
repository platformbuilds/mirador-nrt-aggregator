package model

// Envelope is the generic container for any inbound signal emitted by receivers.
// It carries the raw, undecoded payload plus minimal metadata, so downstream
// processors (summarizer, logsum, spanmetrics, filters) can decode/inspect as needed.
type Envelope struct {
	// Kind identifies the payload type.
	// Supported values:
	//   "metrics"   - OTLP ExportMetricsServiceRequest (protobuf bytes)
	//   "traces"    - OTLP ExportTraceServiceRequest (protobuf bytes)
	//   "prom_rw"   - Prometheus Remote Write (prompb.WriteRequest protobuf bytes)
	//   "json_logs" - One JSON log event (raw JSON bytes)
	Kind string `json:"kind"`

	// Bytes holds the raw request payload for the given Kind.
	// - For OTLP kinds, this is the marshaled protobuf of the Export*ServiceRequest.
	// - For prom_rw, this is the marshaled prompb.WriteRequest protobuf (already unsnappied).
	// - For json_logs, this is the raw JSON object for a single log record.
	Bytes []byte `json:"-"`

	// Attrs is an optional bag of lightweight attributes receivers may attach
	// (e.g., resource hints, remote addr, auth principal). Most processors
	// are free to ignore this and decode from Bytes instead.
	Attrs map[string]string `json:"attrs,omitempty"`

	// TSUnix is the receiver-observed arrival time in Unix seconds.
	TSUnix int64 `json:"ts_unix"`
}

// Aggregate is the per-window summary produced by summarizers (or logsum).
// It is the canonical, compact record we vectorize and store in Weaviate.
type Aggregate struct {
	// Identity & window
	Service     string            `json:"service"`
	WindowStart int64             `json:"window_start"`     // Unix seconds (inclusive)
	WindowEnd   int64             `json:"window_end"`       // Unix seconds (exclusive)
	Labels      map[string]string `json:"labels,omitempty"` // extra facets/top-Ks/etc.
	Locator     string            `json:"locator"`          // optional opaque locator or deep-link JSON

	// Counts & rates
	Count     uint64  `json:"count"`      // number of contributing samples/points (approx)
	RPS       float64 `json:"rps"`        // requests/events per second over the window
	ErrorRate float64 `json:"error_rate"` // fraction in [0,1]

	// Latency quantiles (seconds). For logs without latency, these may be zero.
	P50 float64 `json:"p50"`
	P95 float64 `json:"p95"`
	P99 float64 `json:"p99"`

	// Anomaly scoring (filled by iforest processor)
	AnomalyScore float64 `json:"anomaly_score"`

	// Human-readable short summary; also used as input to embedding.
	SummaryText string `json:"summary"`

	// Vector embedding produced by the vectorizer (not serialized to JSON).
	Vector []float32 `json:"-"`
}

// Known Envelope.Kind constants to help avoid typos.
const (
	KindMetrics  = "metrics"
	KindTraces   = "traces"
	KindPromRW   = "prom_rw"
	KindJSONLogs = "json_logs"
)
