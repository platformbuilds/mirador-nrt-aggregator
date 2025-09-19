package otlpgrpc

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"time"

	"github.com/yourorg/mirador-nrt-aggregator/internal/config"
	"github.com/yourorg/mirador-nrt-aggregator/internal/model"

	colllog "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	collmet "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	colltr "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/proto"

	// Enable gzip-encoded requests per OTLP spec (gRPC-level content-coding)
	_ "google.golang.org/grpc/encoding/gzip"
)

// Receiver implements an OTLP gRPC server (traces + metrics + logs) and
// forwards raw Export*ServiceRequest protobuf bytes as model.Envelope into the pipeline.
type Receiver struct {
	endpoint         string
	maxRecvMsgBytes  int
	maxSendMsgBytes  int
	maxConcurrent    uint32
	enableReflection bool

	// TLS
	tlsEnabled        bool
	tlsCertFile       string
	tlsKeyFile        string
	tlsClientCAFile   string
	requireClientCert bool

	// Keepalive
	kap keepalive.ServerParameters
	kep keepalive.EnforcementPolicy
}

// New builds a new OTLP gRPC receiver.
//
// Supported extras in rc.Extra (collector-style):
//
//	# Core
//	- max_recv_msg_bytes: int (default 16 MiB)
//	- max_send_msg_bytes: int (default 16 MiB)
//	- max_concurrent_streams: int (default 0 => unlimited)
//
//	# Reflection
//	- reflection: bool (default true)
//
//	# TLS / mTLS
//	- tls.enabled: bool (default false)
//	- tls.cert_file: string
//	- tls.key_file: string
//	- tls.client_ca_file: string (enables mTLS if provided)
//	- tls.require_client_cert: bool (default false; useful when client_ca_file is set)
//
//	# Keepalive (sane collector defaults)
//	- keepalive.max_connection_idle_ms: int (default 0 = disabled)
//	- keepalive.max_connection_age_ms: int (default 0 = disabled)
//	- keepalive.max_connection_age_grace_ms: int (default 0 = disabled)
//	- keepalive.time_ms: int (ping interval; default 2m)
//	- keepalive.timeout_ms: int (ping ack timeout; default 20s)
//	- keepalive.permit_without_stream: bool (default true)
func New(rc config.ReceiverCfg) *Receiver {
	// defaults
	maxRecv := 16 * 1024 * 1024
	maxSend := 16 * 1024 * 1024
	maxConc := uint32(0)
	refl := true

	if v, ok := rc.Extra["max_recv_msg_bytes"].(int); ok && v > 0 {
		maxRecv = v
	}
	if v, ok := rc.Extra["max_send_msg_bytes"].(int); ok && v > 0 {
		maxSend = v
	}
	if v, ok := rc.Extra["max_concurrent_streams"].(int); ok && v >= 0 {
		maxConc = uint32(v)
	}
	if v, ok := rc.Extra["reflection"].(bool); ok {
		refl = v
	}

	// TLS
	tlsEnabled := false
	if v, ok := nestedBool(rc.Extra, "tls", "enabled"); ok {
		tlsEnabled = v
	}
	tlsCertFile := nestedString(rc.Extra, "tls", "cert_file")
	tlsKeyFile := nestedString(rc.Extra, "tls", "key_file")
	tlsClientCAFile := nestedString(rc.Extra, "tls", "client_ca_file")
	requireClientCert := false
	if v, ok := nestedBool(rc.Extra, "tls", "require_client_cert"); ok {
		requireClientCert = v
	}

	// Keepalive defaults similar to Collector
	kap := keepalive.ServerParameters{
		Time:    120 * time.Second, // ping clients every 2m if idle
		Timeout: 20 * time.Second,  // wait 20s for ack
		// MaxConnectionIdle/Age/Grace default zero unless provided
	}
	kep := keepalive.EnforcementPolicy{
		MinTime:             60 * time.Second,
		PermitWithoutStream: true,
	}
	if v, ok := nestedInt(rc.Extra, "keepalive", "max_connection_idle_ms"); ok && v > 0 {
		kap.MaxConnectionIdle = time.Duration(v) * time.Millisecond
	}
	if v, ok := nestedInt(rc.Extra, "keepalive", "max_connection_age_ms"); ok && v > 0 {
		kap.MaxConnectionAge = time.Duration(v) * time.Millisecond
	}
	if v, ok := nestedInt(rc.Extra, "keepalive", "max_connection_age_grace_ms"); ok && v > 0 {
		kap.MaxConnectionAgeGrace = time.Duration(v) * time.Millisecond
	}
	if v, ok := nestedInt(rc.Extra, "keepalive", "time_ms"); ok && v > 0 {
		kap.Time = time.Duration(v) * time.Millisecond
	}
	if v, ok := nestedInt(rc.Extra, "keepalive", "timeout_ms"); ok && v > 0 {
		kap.Timeout = time.Duration(v) * time.Millisecond
	}
	if v, ok := nestedBool(rc.Extra, "keepalive", "permit_without_stream"); ok {
		kep.PermitWithoutStream = v
	}

	return &Receiver{
		endpoint:          rc.Endpoint, // e.g. ":4317"
		maxRecvMsgBytes:   maxRecv,
		maxSendMsgBytes:   maxSend,
		maxConcurrent:     maxConc,
		enableReflection:  refl,
		tlsEnabled:        tlsEnabled,
		tlsCertFile:       tlsCertFile,
		tlsKeyFile:        tlsKeyFile,
		tlsClientCAFile:   tlsClientCAFile,
		requireClientCert: requireClientCert,
		kap:               kap,
		kep:               kep,
	}
}

func (r *Receiver) Start(ctx context.Context, out chan<- model.Envelope) error {
	addr := r.endpoint
	if addr == "" {
		addr = ":4317"
	}

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	log.Printf("[otlpgrpc] listening on %s (tls=%v)", addr, r.tlsEnabled)

	// Server options
	opts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(r.maxRecvMsgBytes),
		grpc.MaxSendMsgSize(r.maxSendMsgBytes),
		grpc.KeepaliveParams(r.kap),
		grpc.KeepaliveEnforcementPolicy(r.kep),
	}
	if r.maxConcurrent > 0 {
		opts = append(opts, grpc.MaxConcurrentStreams(r.maxConcurrent))
	}

	// TLS creds if enabled
	if r.tlsEnabled {
		creds, err := r.buildTLS()
		if err != nil {
			return fmt.Errorf("otlpgrpc: tls: %w", err)
		}
		opts = append(opts, grpc.Creds(creds))
	}

	srv := grpc.NewServer(opts...)

	// Register services (v1)
	ms := &metricsSvc{out: out}
	ts := &tracesSvc{out: out}
	ls := &logsSvc{out: out}
	collmet.RegisterMetricsServiceServer(srv, ms)
	colltr.RegisterTraceServiceServer(srv, ts)
	colllog.RegisterLogsServiceServer(srv, ls)

	if r.enableReflection {
		reflection.Register(srv)
	}

	errCh := make(chan error, 1)
	go func() {
		if serveErr := srv.Serve(lis); serveErr != nil {
			errCh <- serveErr
		}
	}()

	// Wait for context cancel or server error
	select {
	case <-ctx.Done():
		// graceful stop
		done := make(chan struct{})
		go func() {
			srv.GracefulStop()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			srv.Stop()
		}
		return nil
	case e := <-errCh:
		return e
	}
}

func (r *Receiver) buildTLS() (credentials.TransportCredentials, error) {
	if r.tlsCertFile == "" || r.tlsKeyFile == "" {
		return nil, errors.New("cert_file and key_file are required when tls.enabled=true")
	}
	cert, err := tls.LoadX509KeyPair(r.tlsCertFile, r.tlsKeyFile)
	if err != nil {
		return nil, err
	}
	tlsCfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}
	// mTLS?
	if r.tlsClientCAFile != "" {
		ca, err := os.ReadFile(r.tlsClientCAFile)
		if err != nil {
			return nil, err
		}
		cp := x509.NewCertPool()
		if !cp.AppendCertsFromPEM(ca) {
			return nil, errors.New("failed to append client CA")
		}
		tlsCfg.ClientCAs = cp
		if r.requireClientCert {
			tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert
		} else {
			tlsCfg.ClientAuth = tls.VerifyClientCertIfGiven
		}
	}
	return credentials.NewTLS(tlsCfg), nil
}

// ----------------- gRPC service impls -----------------

type metricsSvc struct {
	collmet.UnimplementedMetricsServiceServer
	out chan<- model.Envelope
}

func (s *metricsSvc) Export(ctx context.Context, req *collmet.ExportMetricsServiceRequest) (*collmet.ExportMetricsServiceResponse, error) {
	b, err := proto.Marshal(req)
	if err != nil {
		log.Printf("[otlpgrpc/metrics] marshal error: %v", err)
		return &collmet.ExportMetricsServiceResponse{}, nil
	}
	select {
	case s.out <- model.Envelope{
		Kind:   model.KindMetrics,
		Bytes:  b,
		Attrs:  map[string]string{},
		TSUnix: time.Now().Unix(),
	}:
	case <-ctx.Done():
	}
	return &collmet.ExportMetricsServiceResponse{}, nil
}

type tracesSvc struct {
	colltr.UnimplementedTraceServiceServer
	out chan<- model.Envelope
}

func (s *tracesSvc) Export(ctx context.Context, req *colltr.ExportTracesServiceRequest) (*colltr.ExportTracesServiceResponse, error) {
	b, err := proto.Marshal(req)
	if err != nil {
		log.Printf("[otlpgrpc/traces] marshal error: %v", err)
		return &colltr.ExportTracesServiceResponse{}, nil
	}
	select {
	case s.out <- model.Envelope{
		Kind:   model.KindTraces,
		Bytes:  b,
		Attrs:  map[string]string{},
		TSUnix: time.Now().Unix(),
	}:
	case <-ctx.Done():
	}
	return &colltr.ExportTracesServiceResponse{}, nil
}

type logsSvc struct {
	colllog.UnimplementedLogsServiceServer
	out chan<- model.Envelope
}

func (s *logsSvc) Export(ctx context.Context, req *colllog.ExportLogsServiceRequest) (*colllog.ExportLogsServiceResponse, error) {
	b, err := proto.Marshal(req)
	if err != nil {
		log.Printf("[otlpgrpc/logs] marshal error: %v", err)
		return &colllog.ExportLogsServiceResponse{}, nil
	}
	select {
	case s.out <- model.Envelope{
		Kind:   model.KindJSONLogs, // We treat OTLP logs as JSON-like events downstream.
		Bytes:  b,                  // NOTE: This is an OTLP logs request; downstream decoders must handle it.
		Attrs:  map[string]string{},
		TSUnix: time.Now().Unix(),
	}:
	case <-ctx.Done():
	}
	return &colllog.ExportLogsServiceResponse{}, nil
}

// ----------------- tiny helpers -----------------

func nestedString(m map[string]any, k1, k2 string) string {
	if m == nil {
		return ""
	}
	if n1, ok := m[k1].(map[string]any); ok {
		if s, ok := n1[k2].(string); ok {
			return s
		}
	}
	return ""
}
func nestedBool(m map[string]any, k1, k2 string) (bool, bool) {
	if m == nil {
		return false, false
	}
	if n1, ok := m[k1].(map[string]any); ok {
		if b, ok := n1[k2].(bool); ok {
			return b, true
		}
	}
	return false, false
}
func nestedInt(m map[string]any, k1, k2 string) (int, bool) {
	if m == nil {
		return 0, false
	}
	if n1, ok := m[k1].(map[string]any); ok {
		switch t := n1[k2].(type) {
		case int:
			return t, true
		case int64:
			return int(t), true
		case float64:
			return int(t), true
		}
	}
	return 0, false
}
