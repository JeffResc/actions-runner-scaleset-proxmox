package observability

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	sdkresource "go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
)

// TracerName is the canonical instrumentation-scope name. Use this when
// fetching a tracer (e.g. otel.Tracer(observability.TracerName)) so all
// spans land under one consistent name in the backend.
const TracerName = "github.com/jeffresc/github-actions-proxmox-scaleset"

// TracingOptions configure the OTLP/HTTP exporter. An empty Endpoint
// disables tracing (the global tracer becomes the no-op tracer, so
// instrumented code paths are essentially free).
type TracingOptions struct {
	Endpoint    string
	Insecure    bool
	SampleRatio float64

	// Resource attributes — exposed in every span (service.name, etc.).
	ServiceName    string
	ServiceVersion string
}

// TracerShutdown is returned by InitTracer; calling it flushes pending
// spans and shuts the exporter down. Always call it (typically via
// defer) — pending spans are otherwise lost on process exit.
type TracerShutdown func(ctx context.Context) error

// InitTracer wires the OTLP/HTTP exporter to the global tracer provider.
// When opts.Endpoint is empty, the global tracer is left as the no-op
// implementation and the returned shutdown is a no-op.
func InitTracer(ctx context.Context, opts TracingOptions, log *slog.Logger) (TracerShutdown, error) {
	if log == nil {
		log = slog.Default()
	}
	if opts.Endpoint == "" {
		log.Info("tracing disabled (no endpoint configured)")
		return func(context.Context) error { return nil }, nil
	}

	exporterOpts := []otlptracehttp.Option{
		otlptracehttp.WithEndpoint(opts.Endpoint),
	}
	if opts.Insecure {
		exporterOpts = append(exporterOpts, otlptracehttp.WithInsecure())
	}
	exporter, err := otlptracehttp.New(ctx, exporterOpts...)
	if err != nil {
		return nil, fmt.Errorf("tracing: build otlp/http exporter: %w", err)
	}

	res, err := sdkresource.Merge(
		sdkresource.Default(),
		sdkresource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName(opts.ServiceName),
			semconv.ServiceVersion(opts.ServiceVersion),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("tracing: build resource: %w", err)
	}

	sampler := sdktrace.AlwaysSample()
	if r := opts.SampleRatio; r > 0 && r < 1 {
		sampler = sdktrace.ParentBased(sdktrace.TraceIDRatioBased(r))
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sampler),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	log.Info("tracing enabled", "endpoint", opts.Endpoint, "insecure", opts.Insecure, "sample_ratio", opts.SampleRatio)

	shutdown := func(shutdownCtx context.Context) error {
		shutdownCtx, cancel := context.WithTimeout(shutdownCtx, 5*time.Second)
		defer cancel()
		if err := tp.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("tracing: shutdown tracer provider: %w", err)
		}
		return nil
	}
	return shutdown, nil
}
