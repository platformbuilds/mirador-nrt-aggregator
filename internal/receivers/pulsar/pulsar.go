package pulsar

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"log"
	"strings"
	"time"

	ps "github.com/apache/pulsar-client-go/pulsar"

	"github.com/platformbuilds/mirador-nrt-aggregator/internal/config"
	"github.com/platformbuilds/mirador-nrt-aggregator/internal/model"
)

// Receiver consumes messages from Apache Pulsar and forwards them as model.Envelope.
// Supported kinds:
//   - "metrics":   OTLP ExportMetricsServiceRequest
//   - "traces":    OTLP ExportTraceServiceRequest
//   - "prom_rw":   Prometheus Remote Write (prompb.WriteRequest)
//   - "json_logs": JSON payload per message (or NDJSON if extra.ndjson = true)
//
// Config mapping (config.ReceiverCfg):
//   - Endpoint OR Brokers[0]  => Pulsar serviceURL (e.g., pulsar://host:6650, pulsar+ssl://host:6651)
//   - Topic                   => topic to subscribe
//   - Group                   => subscription name
//   - Extra:
//     ndjson: bool (default false)          // split messages by newline for json_logs
//     subscription_type: string             // "exclusive" | "shared" | "failover" | "key_shared" (default "shared")
//     auth_token: string                    // static token
//     auth_token_file: string               // read token from file (if auth_token empty)
//     tls_allow_insecure: bool              // default false
//     tls_trust_certs_file: string          // path to CA bundle for TLS
//     message_chan_buffer: int              // consumer buffer (default 32)
//     receiver_queue_size: int              // prefetch queue per consumer (default 1000)
//     kind: string                          // override of constructor kind
type Receiver struct {
	serviceURL string
	topic      string
	subName    string
	kind       string

	ndjson bool

	// Pulsar client/consumer options
	subType           ps.SubscriptionType
	authToken         string
	authTokenFile     string
	tlsAllowInsecure  bool
	tlsTrustCertsPath string
	msgChanBuffer     int
	receiverQueueSize int
}

// New builds a Pulsar receiver. 'kind' should be "metrics" | "traces" | "prom_rw" | "json_logs".
func New(rc config.ReceiverCfg, kind string) *Receiver {
	// serviceURL: prefer rc.Endpoint if set; else first Brokers entry (kept for parity with other receivers)
	svc := strings.TrimSpace(rc.Endpoint)
	if svc == "" && len(rc.Brokers) > 0 {
		svc = strings.TrimSpace(rc.Brokers[0]) // e.g., pulsar://pulsar:6650
	}

	// Kind override via extra.kind if present
	if v, ok := rc.Extra["kind"].(string); ok && v != "" {
		kind = v
	}

	// NDJSON splitting for json_logs
	ndjson := false
	if v, ok := rc.Extra["ndjson"].(bool); ok {
		ndjson = v
	}

	// Subscription type
	subType := ps.Shared
	if s, ok := rc.Extra["subscription_type"].(string); ok && s != "" {
		switch strings.ToLower(strings.TrimSpace(s)) {
		case "exclusive":
			subType = ps.Exclusive
		case "failover":
			subType = ps.Failover
		case "key_shared", "keyshared", "key-shared":
			subType = ps.KeyShared
		default:
			subType = ps.Shared
		}
	}

	// Auth / TLS
	authToken := ""
	if s, ok := rc.Extra["auth_token"].(string); ok {
		authToken = s
	}
	authTokenFile := ""
	if s, ok := rc.Extra["auth_token_file"].(string); ok {
		authTokenFile = s
	}
	tlsAllowInsecure := false
	if b, ok := rc.Extra["tls_allow_insecure"].(bool); ok {
		tlsAllowInsecure = b
	}
	tlsTrustPath := ""
	if s, ok := rc.Extra["tls_trust_certs_file"].(string); ok {
		tlsTrustPath = s
	}

	msgBuf := 32
	if v, ok := rc.Extra["message_chan_buffer"].(int); ok && v > 0 {
		msgBuf = v
	}
	recvQ := 1000
	if v, ok := rc.Extra["receiver_queue_size"].(int); ok && v > 0 {
		recvQ = v
	}

	return &Receiver{
		serviceURL:        svc,
		topic:             rc.Topic,
		subName:           rc.Group, // mirrors Kafka group → Pulsar subscription name
		kind:              normalizeKind(kind),
		ndjson:            ndjson,
		subType:           subType,
		authToken:         authToken,
		authTokenFile:     authTokenFile,
		tlsAllowInsecure:  tlsAllowInsecure,
		tlsTrustCertsPath: tlsTrustPath,
		msgChanBuffer:     msgBuf,
		receiverQueueSize: recvQ,
	}
}

func (r *Receiver) Start(ctx context.Context, out chan<- model.Envelope) error {
	if r.serviceURL == "" || strings.TrimSpace(r.topic) == "" || strings.TrimSpace(r.subName) == "" {
		return errors.New("pulsar receiver: missing serviceURL, topic, or subscription name")
	}

	cliOpts := ps.ClientOptions{
		URL:                        r.serviceURL,
		TLSAllowInsecureConnection: r.tlsAllowInsecure,
		TLSTrustCertsFilePath:      r.tlsTrustCertsPath,
	}

	// Authentication
	if r.authToken != "" {
		cliOpts.Authentication = ps.NewAuthenticationToken(r.authToken)
	} else if r.authTokenFile != "" {
		cliOpts.Authentication = ps.NewAuthenticationTokenFromFile(r.authTokenFile)
	}

	client, err := ps.NewClient(cliOpts)
	if err != nil {
		return err
	}
	defer client.Close()

	consOpts := ps.ConsumerOptions{
		Topic:            r.topic,
		SubscriptionName: r.subName,
		Type:             r.subType,
		// Buffer sizes
		MessageChannel:    make(chan ps.ConsumerMessage, r.msgChanBuffer),
		ReceiverQueueSize: r.receiverQueueSize,
	}

	consumer, err := client.Subscribe(consOpts)
	if err != nil {
		return err
	}
	defer consumer.Close()

	log.Printf("[pulsar/%s] consuming topic=%s subscription=%s url=%s", r.kind, r.topic, r.subName, r.serviceURL)

	// Receive loop using the consumer's MessageChannel to avoid blocking Receive calls.
	msgCh := consumer.Chan()

	for {
		select {
		case <-ctx.Done():
			return nil

		case cm, ok := <-msgCh:
			if !ok {
				// channel closed (consumer closed); exit gracefully
				return nil
			}
			msg := cm.Message
			ts := time.Now().Unix()
			attrs := propsToMap(msg.Properties())

			switch r.kind {
			case "json_logs":
				if r.ndjson {
					sc := bufio.NewScanner(bytes.NewReader(msg.Payload()))
					// allow long lines (up to ~10MB)
					buf := make([]byte, 0, 64*1024)
					sc.Buffer(buf, 10*1024*1024)
					for sc.Scan() {
						line := strings.TrimSpace(sc.Text())
						if line == "" {
							continue
						}
						env := model.Envelope{
							Kind:   model.KindJSONLogs,
							Bytes:  []byte(line),
							Attrs:  attrs,
							TSUnix: ts,
						}
						select {
						case out <- env:
						default:
							log.Printf("[pulsar/json_logs] dropping NDJSON line due to backpressure")
						}
					}
					if err := sc.Err(); err != nil {
						log.Printf("[pulsar/json_logs] scan error: %v", err)
					}
				} else {
					env := model.Envelope{
						Kind:   model.KindJSONLogs,
						Bytes:  msg.Payload(),
						Attrs:  attrs,
						TSUnix: ts,
					}
					select {
					case out <- env:
					default:
						log.Printf("[pulsar/json_logs] dropping message due to backpressure")
					}
				}

			case "metrics":
				select {
				case out <- model.Envelope{Kind: model.KindMetrics, Bytes: msg.Payload(), Attrs: attrs, TSUnix: ts}:
				default:
					log.Printf("[pulsar/metrics] dropping message due to backpressure")
				}

			case "traces":
				select {
				case out <- model.Envelope{Kind: model.KindTraces, Bytes: msg.Payload(), Attrs: attrs, TSUnix: ts}:
				default:
					log.Printf("[pulsar/traces] dropping message due to backpressure")
				}

			case "prom_rw":
				select {
				case out <- model.Envelope{Kind: model.KindPromRW, Bytes: msg.Payload(), Attrs: attrs, TSUnix: ts}:
				default:
					log.Printf("[pulsar/prom_rw] dropping message due to backpressure")
				}

			default:
				// Fallback to metrics to match kafka receiver’s behavior
				select {
				case out <- model.Envelope{Kind: model.KindMetrics, Bytes: msg.Payload(), Attrs: attrs, TSUnix: ts}:
				default:
					log.Printf("[pulsar/%s] dropping message due to backpressure", r.kind)
				}
			}

			// Ack regardless of downstream backpressure to avoid redelivery loops.
			consumer.Ack(msg)
		}
	}
}

func propsToMap(p map[string]string) map[string]string {
	if len(p) == 0 {
		return map[string]string{}
	}
	out := make(map[string]string, len(p))
	for k, v := range p {
		out[k] = v
	}
	return out
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
