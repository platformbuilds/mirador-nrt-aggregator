package jsonlogs

import (
	"bufio"
	"compress/gzip"
	"context"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"time"

	kafka "github.com/segmentio/kafka-go"

	"github.com/platformbuilds/mirador-nrt-aggregator/internal/config"
	"github.com/platformbuilds/mirador-nrt-aggregator/internal/model"
)

const (
	defaultPath = "/v1/logs"
)

// ----------------------------- HTTP receiver -----------------------------

type HTTPReceiver struct {
	addr string
	path string
}

func NewHTTP(rc config.ReceiverCfg) *HTTPReceiver {
	path := defaultPath
	if p, ok := rc.Extra["path"].(string); ok && strings.TrimSpace(p) != "" {
		path = p
	}
	return &HTTPReceiver{
		addr: rc.Endpoint, // e.g. "0.0.0.0:9428"
		path: path,
	}
}

func (r *HTTPReceiver) Start(ctx context.Context, out chan<- model.Envelope) error {
	if r.addr == "" {
		r.addr = "0.0.0.0:9428"
	}

	mux := http.NewServeMux()
	mux.HandleFunc(r.path, func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		var reader io.Reader = req.Body
		defer req.Body.Close()

		// Support gzip-encoded payloads
		if enc := req.Header.Get("Content-Encoding"); strings.Contains(strings.ToLower(enc), "gzip") {
			gr, err := gzip.NewReader(reader)
			if err != nil {
				http.Error(w, "bad gzip", http.StatusBadRequest)
				return
			}
			defer gr.Close()
			reader = gr
		}

		ct := strings.ToLower(req.Header.Get("Content-Type"))
		switch {
		case strings.Contains(ct, "ndjson"), strings.Contains(ct, "x-ndjson"):
			// newline-delimited JSON: one event per line
			sc := bufio.NewScanner(reader)
			// allow reasonably long lines
			buf := make([]byte, 0, 64*1024)
			sc.Buffer(buf, 10*1024*1024)
			now := time.Now().Unix()
			n := 0
			for sc.Scan() {
				line := strings.TrimSpace(sc.Text())
				if line == "" {
					continue
				}
				out <- model.Envelope{
					Kind:   model.KindJSONLogs,
					Bytes:  []byte(line),
					Attrs:  map[string]string{}, // could copy query/header attrs here
					TSUnix: now,
				}
				n++
			}
			if err := sc.Err(); err != nil && err != io.EOF {
				http.Error(w, "read error", http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte("ok"))
			log.Printf("[jsonlogs/http] accepted %d events", n)

		default:
			// treat as a single JSON object or array; we’ll try to split by newline first,
			// falling back to whole-body as one event if not NDJSON.
			body, err := io.ReadAll(reader)
			if err != nil {
				http.Error(w, "bad body", http.StatusBadRequest)
				return
			}
			payload := strings.TrimSpace(string(body))
			now := time.Now().Unix()

			// If it looks like NDJSON (has newlines), stream per line; else single event
			if strings.Contains(payload, "\n") {
				n := 0
				for _, line := range strings.Split(payload, "\n") {
					line = strings.TrimSpace(line)
					if line == "" {
						continue
					}
					out <- model.Envelope{
						Kind:   model.KindJSONLogs,
						Bytes:  []byte(line),
						Attrs:  map[string]string{},
						TSUnix: now,
					}
					n++
				}
				w.WriteHeader(http.StatusAccepted)
				_, _ = w.Write([]byte("ok"))
				log.Printf("[jsonlogs/http] accepted %d events (auto-split)", n)
				return
			}

			// Single JSON document
			if payload != "" {
				out <- model.Envelope{
					Kind:   model.KindJSONLogs,
					Bytes:  []byte(payload),
					Attrs:  map[string]string{},
					TSUnix: now,
				}
			}
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte("ok"))
		}
	})

	srv := &http.Server{
		Addr:              r.addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	// listener to detect address in logs
	ln, err := net.Listen("tcp", r.addr)
	if err != nil {
		return err
	}
	log.Printf("[jsonlogs/http] listening on %s path=%s", r.addr, r.path)

	// Serve in background
	errCh := make(chan error, 1)
	go func() {
		if serveErr := srv.Serve(ln); serveErr != nil && serveErr != http.ErrServerClosed {
			errCh <- serveErr
		}
	}()

	// Shutdown when ctx is done
	select {
	case <-ctx.Done():
		shctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(shctx)
		return nil
	case e := <-errCh:
		return e
	}
}

// ----------------------------- Kafka receiver -----------------------------

type KafkaReceiver struct {
	brokers []string
	topic   string
	group   string

	// optional: max bytes, etc. via Extra (not strictly required)
}

func NewKafka(rc config.ReceiverCfg) *KafkaReceiver {
	return &KafkaReceiver{
		brokers: rc.Brokers,
		topic:   rc.Topic,
		group:   rc.Group,
	}
}

func (r *KafkaReceiver) Start(ctx context.Context, out chan<- model.Envelope) error {
	if len(r.brokers) == 0 || r.topic == "" {
		return ErrKafkaConfig
	}
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  r.brokers,
		GroupID:  r.groupOrDefault(),
		Topic:    r.topic,
		MaxBytes: 10 * 1024 * 1024, // 10MB per message
	})

	defer func() {
		_ = reader.Close()
	}()

	log.Printf("[jsonlogs/kafka] consuming topic=%s group=%s brokers=%v", r.topic, r.groupOrDefault(), r.brokers)

	for {
		m, err := reader.ReadMessage(ctx)
		if err != nil {
			// context canceled → graceful exit
			if err == context.Canceled || err == context.DeadlineExceeded {
				return nil
			}
			// kafka reader returns error when closing; also treat as graceful on ctx done
			select {
			case <-ctx.Done():
				return nil
			default:
				// transient fetch error; keep going (small backoff)
				log.Printf("[jsonlogs/kafka] read error: %v", err)
				time.Sleep(500 * time.Millisecond)
				continue
			}
		}

		attrs := headersToMap(m.Headers)
		out <- model.Envelope{
			Kind:   model.KindJSONLogs,
			Bytes:  m.Value,
			Attrs:  attrs,
			TSUnix: time.Now().Unix(), // receiver timestamp
		}
	}
}

func (r *KafkaReceiver) groupOrDefault() string {
	if strings.TrimSpace(r.group) == "" {
		return "mirador-jsonlogs"
	}
	return r.group
}

var ErrKafkaConfig = &configError{"jsonlogs/kafka: missing brokers or topic"}

type configError struct{ s string }

func (e *configError) Error() string { return e.s }

// ----------------------------- helpers -----------------------------

func headersToMap(hdrs []kafka.Header) map[string]string {
	if len(hdrs) == 0 {
		return map[string]string{}
	}
	m := make(map[string]string, len(hdrs))
	for _, h := range hdrs {
		m[h.Key] = string(h.Value)
	}
	return m
}
