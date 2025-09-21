package otlpgrpc

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/platformbuilds/mirador-nrt-aggregator/internal/config"
	"github.com/platformbuilds/mirador-nrt-aggregator/internal/model"

	colllog "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	collmet "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	colltr "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	met "go.opentelemetry.io/proto/otlp/metrics/v1"
)

func TestMetricsServiceExportForwardsEnvelope(t *testing.T) {
	ch := make(chan model.Envelope, 1)
	svc := &metricsSvc{out: ch}
	req := &collmet.ExportMetricsServiceRequest{ResourceMetrics: []*met.ResourceMetrics{{}}}
	if _, err := svc.Export(context.Background(), req); err != nil {
		t.Fatalf("Export returned error: %v", err)
	}

	env := <-ch
	if env.Kind != model.KindMetrics {
		t.Fatalf("unexpected kind %s", env.Kind)
	}
	if len(env.Bytes) == 0 {
		t.Fatalf("expected marshalled bytes")
	}
}

func TestTracesServiceExportForwardsEnvelope(t *testing.T) {
	ch := make(chan model.Envelope, 1)
	svc := &tracesSvc{out: ch}
	req := &colltr.ExportTraceServiceRequest{}
	if _, err := svc.Export(context.Background(), req); err != nil {
		t.Fatalf("Export returned error: %v", err)
	}
	env := <-ch
	if env.Kind != model.KindTraces {
		t.Fatalf("unexpected kind %s", env.Kind)
	}
}

func TestLogsServiceExportForwardsEnvelope(t *testing.T) {
	ch := make(chan model.Envelope, 1)
	svc := &logsSvc{out: ch}
	req := &colllog.ExportLogsServiceRequest{}
	if _, err := svc.Export(context.Background(), req); err != nil {
		t.Fatalf("Export returned error: %v", err)
	}
	env := <-ch
	if env.Kind != model.KindJSONLogs {
		t.Fatalf("unexpected kind %s", env.Kind)
	}
}

func TestReceiverStartTLSConfigError(t *testing.T) {
	r := New(config.ReceiverCfg{Endpoint: "127.0.0.1:0", Extra: map[string]any{
		"tls": map[string]any{"enabled": true},
	}})
	r.listen = func(network, addr string) (net.Listener, error) {
		return nopListener{}, nil
	}
	err := r.Start(context.Background(), make(chan model.Envelope))
	if err == nil || !strings.Contains(err.Error(), "cert_file") {
		t.Fatalf("expected tls config error, got %v", err)
	}
}

func TestReceiverStartFailsWhenPortInUse(t *testing.T) {
	r := New(config.ReceiverCfg{Endpoint: "127.0.0.1:4317"})
	r.listen = func(network, addr string) (net.Listener, error) {
		return nil, errors.New("in use")
	}
	if err := r.Start(context.Background(), make(chan model.Envelope)); err == nil {
		t.Fatal("expected error when port already in use")
	}
}

type nopListener struct{}

func (nopListener) Accept() (net.Conn, error) { return nil, errors.New("not implemented") }
func (nopListener) Close() error              { return nil }
func (nopListener) Addr() net.Addr            { return &net.TCPAddr{} }

func TestMetricsExportBackpressureRespectsContext(t *testing.T) {
	ch := make(chan model.Envelope)
	svc := &metricsSvc{out: ch}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = svc.Export(ctx, &collmet.ExportMetricsServiceRequest{})
	}()

	select {
	case <-done:
		t.Fatal("export should block before cancel")
	case <-time.After(50 * time.Millisecond):
	}

	cancel()
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("export did not unblock after cancel")
	}

	select {
	case <-ch:
		t.Fatal("no envelope expected when context canceled")
	default:
	}
}
