# Mirador NRT Aggregator

**Mirador NRT Aggregator** is an **OpenTelemetry-compliant, near real-time (NRT) aggregator** that ingests telemetry from multiple sources (traces, metrics, logs), performs streaming summarization, anomaly detection, vectorization, and exports structured objects to **Weaviate** for long-term storage and semantic search.

It is inspired by the [OpenTelemetry Collector](https://opentelemetry.io/docs/collector/) and integrates seamlessly into OTel-based environments.

---

## ✨ Features

- **Receivers**  
  - **OTLP/gRPC** (`:4317`) — spec-compliant, traces/metrics/logs, gzip, TLS/mTLS  
  - **OTLP/HTTP** (`:4318`) — `/v1/{traces,metrics,logs}`, gzip, TLS/mTLS  
  - **Prometheus Remote Write** (`:19291`) — snappy/gzip  
  - **JSON logs** — HTTP (`:19292`), Kafka, Pulsar  
  - **Kafka** — ingest traces, metrics, PromRW, or JSON logs  
  - **Pulsar** — same as Kafka, with NDJSON splitting

- **Processors**  
  - **Filter** — drop/keep signals by conditions (`expr`)  
  - **SpanMetrics** — RED metrics from traces + `errors_total` via status/events  
  - **OTLP Logs → JSON** — flattens LogRecords into JSON for uniform processing  
  - **LogSum** — tumbling-window aggregations (top-K, error counts, quantiles)  
  - **Summarizer** — windowed statistics with t-digest quantiles  
  - **iForest** — anomaly detection & scoring (Isolation Forest)  
  - **Vectorizer** — embeddings via Ollama (CPU/GPU) or hash-based fallback

- **Exporters**  
  - **Weaviate** — `/v1/objects` upsert, vector + metadata storage  

- **Observability**  
  - Self-metrics endpoint (`:8888/metrics`)  
  - Health probes (`/healthz`)  
  - Configurable via YAML, just like OTel Collector  

---

## 🚀 Quick Start

### Build & Run locally
```bash
go build -o mirador-nrt-aggregator ./cmd/mirador-nrt-aggregator
./mirador-nrt-aggregator --config=config.example.yaml
```

### Example config
See [`config.example.yaml`](./config.example.yaml) for a full reference.  
It wires all receivers, processors, and the Weaviate exporter.

---

## 📦 Helm Chart

This repo provides a Helm chart similar to [opentelemetry-collector](https://github.com/open-telemetry/opentelemetry-helm-charts/tree/main/charts/opentelemetry-collector).

### Install
```bash
helm repo add mirador https://github.com/platformbuilds/mirador-nrt-aggregator
helm upgrade --install mirador mirador/mirador-nrt-aggregator \
  --set image.repository=yourorg/mirador-nrt-aggregator \
  --set image.tag=0.1.0 \
  --set-file weaviate.apiKeySecret.value=./weaviate_api_key.txt
```

### Values highlights
- `mode`: `Deployment` | `DaemonSet` | `StatefulSet`
- `service.ports`: Expose OTLP, PromRW, JSON logs, Prometheus metrics
- `config`: Paste full pipeline config (defaults included)
- `serviceMonitor` / `podMonitor`: Enable scraping with Prometheus Operator
- `weaviate.apiKeySecret`: Create or reference a Secret for Weaviate API key

---

## 🏗 Architecture

```
           +-------------+         +------------------+
 Traces -->|  Receivers  |-------> |                  |
 Metrics ->| (OTLP, Prom)|         |   Processors     |--+
 Logs ---->| Kafka, JSON)|         | (spanmetrics,    |  |
           +-------------+         |  summarizer,     |  v
                                    |  iforest, vector)| Exporters
                                    +------------------+  |
                                                           |
                                              +------------v----------+
                                              |      Weaviate         |
                                              |  (/v1/objects upsert) |
                                              +-----------------------+
```

- **Receivers**: Ingest OTLP, PromRW, JSON logs (HTTP/Kafka/Pulsar)  
- **Processors**: Filtering, summarization, anomaly scoring, vectorization  
- **Exporters**: Store enriched, vectorized objects in Weaviate and optionally mirror to stdout for debugging  

---

## 🔍 Example Pipelines

### Traces
```yaml
service:
  pipelines:
    traces:
      receivers: [otlpgrpc, otlphttp, kafka/traces, pulsar/traces]
      processors: [spanmetrics, summarizer, iforest, vectorizer]
      exporters: [weaviate, stdout]
```

### Metrics
```yaml
    metrics:
      receivers: [otlpgrpc, otlphttp, promrw, kafka/metrics, kafka/promrw]
      processors: [summarizer, iforest, vectorizer]
      exporters: [weaviate, stdout]
```

### Logs
```yaml
    logs:
      receivers: [otlpgrpc, otlphttp, jsonlogs/http, kafka/jsonlogs, pulsar/jsonlogs]
      processors: [otlplogs, logsum, iforest, vectorizer]
      exporters: [weaviate, stdout]
```

---

## ⚙️ Development

- Written in **Go**
- Internal packages:
  - `internal/receivers`: otlpgrpc, otlphttp, promrw, kafka, pulsar, jsonlogs
  - `internal/processors`: filter, spanmetrics, otlplogs, logsum, summarizer, iforest, vectorizer
  - `internal/exporters`: weaviate, stdout
  - `internal/model`: Envelope definitions
  - `internal/pipeline`: pipeline wiring

Run unit tests:
```bash
go test ./...
```

---

## 📖 References

- [OpenTelemetry Collector](https://opentelemetry.io/docs/collector/)
- [OpenTelemetry Helm charts](https://github.com/open-telemetry/opentelemetry-helm-charts)
- [Weaviate Vector DB](https://weaviate.io/)
- [Apache Pulsar](https://pulsar.apache.org/)
- [Apache Kafka](https://kafka.apache.org/)

---

## 📝 License

Apache License 2.0 — see [LICENSE](./LICENSE)
