package kafka

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"log"
	"strings"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/yourorg/mirador-nrt-aggregator/internal/config"
	"github.com/yourorg/mirador-nrt-aggregator/internal/model"
)

// Receiver consumes binary payloads from Kafka and forwards them as model.Envelope.
// Supported kinds:
//   - "metrics":   OTLP ExportMetricsServiceRequest
//   - "traces":    OTLP ExportTracesServiceRequest
//   - "prom_rw":   Prometheus Remote Write (prompb.WriteRequest)
//   - "json_logs": JSON payload per message (or NDJSON if extra.ndjson = true)
type Receiver struct {
	brokers []string
	topic   string
	group   string
	kind    string

	maxBytes int  // per message fetch cap
	ndjson   bool // if true and kind=json_logs, split message by lines
}

// New builds a Kafka receiver.
func New(rc config.ReceiverCfg, kind string) *Receiver {
	maxBytes := 10 * 1024 * 1024
	if v, ok := rc.Extra["max_bytes"].(int); ok && v > 0 {
		maxBytes = v
	}
	ndjson := false
	if v, ok := rc.Extra["ndjson"].(bool); ok {
		ndjson = v
	}
	return &Receiver{
		brokers:  rc.Brokers,
		topic:    rc.Topic,
		group:    rc.Group,
		kind:     normalizeKind(kind),
		maxBytes: maxBytes,
		ndjson:   ndjson,
	}
}

func (r *Receiver) Start(ctx context.Context, out chan<- model.Envelope) error {
	if len(r.brokers) == 0 || strings.TrimSpace(r.topic) == "" {
		return errors.New("kafka receiver: missing brokers or topic")
	}

	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:  r.brokers,
		GroupID:  r.groupOrDefault(),
		Topic:    r.topic,
		MaxBytes: r.maxBytes,
	})
	defer func() { _ = reader.Close() }()

	log.Printf("[kafka/%s] consuming topic=%s group=%s brokers=%v", r.kind, r.topic, r.groupOrDefault(), r.brokers)

	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			// graceful exit on context cancellation
			if err == context.Canceled || err == context.DeadlineExceeded {
				return nil
			}
			select {
			case <-ctx.Done():
				return nil
			default:
				log.Printf("[kafka/%s] read error: %v", r.kind, err)
				time.Sleep(500 * time.Millisecond)
				continue
			}
		}

		attrs := headersToMap(msg.Headers)
		ts := time.Now().Unix()

		switch r.kind {
		case "json_logs":
			// Either emit the whole message as one JSON event,
			// or split NDJSON into multiple envelopes if configured.
			if r.ndjson {
				sc := bufio.NewScanner(bytes.NewReader(msg.Value))
				// allow reasonably long lines (up to ~10MB)
				buf := make([]byte, 0, 64*1024)
				sc.Buffer(buf, r.maxBytes)
				for sc.Scan() {
					line := strings.TrimSpace(sc.Text())
					if line == "" {
						continue
					}
					out <- model.Envelope{
						Kind:   model.KindJSONLogs,
						Bytes:  []byte(line),
						Attrs:  attrs,
						TSUnix: ts,
					}
				}
				if err := sc.Err(); err != nil {
					log.Printf("[kafka/json_logs] scan error: %v", err)
				}
			} else {
				out <- model.Envelope{
					Kind:   model.KindJSONLogs,
					Bytes:  msg.Value,
					Attrs:  attrs,
					TSUnix: ts,
				}
			}

		case "metrics":
			out <- model.Envelope{
				Kind:   model.KindMetrics,
				Bytes:  msg.Value,
				Attrs:  attrs,
				TSUnix: ts,
			}

		case "traces":
			out <- model.Envelope{
				Kind:   model.KindTraces,
				Bytes:  msg.Value,
				Attrs:  attrs,
				TSUnix: ts,
			}

		case "prom_rw":
			out <- model.Envelope{
				Kind:   model.KindPromRW,
				Bytes:  msg.Value,
				Attrs:  attrs,
				TSUnix: ts,
			}

		default:
			// Fallback to metrics for safety
			out <- model.Envelope{
				Kind:   model.KindMetrics,
				Bytes:  msg.Value,
				Attrs:  attrs,
				TSUnix: ts,
			}
		}
	}
}

func (r *Receiver) groupOrDefault() string {
	g := strings.TrimSpace(r.group)
	if g == "" {
		return "mirador-nrt-aggregator"
	}
	return g
}

func headersToMap(hdrs []kafkago.Header) map[string]string {
	if len(hdrs) == 0 {
		return map[string]string{}
	}
	m := make(map[string]string, len(hdrs))
	for _, h := range hdrs {
		m[h.Key] = string(h.Value)
	}
	return m
}

func normalizeKind(k string) string {
	k = strings.ToLower(strings.TrimSpace(k))
	switch k {
	case "metrics", "traces", "prom_rw", "json_logs":
		return k
	case "promremotewrite", "prometheusremotewrite", "prom-remote-write":
		return "prom_rw"
	case "json", "jsonlogs", "logs":
		return "json_logs"
	default:
		return "metrics"
	}
}
