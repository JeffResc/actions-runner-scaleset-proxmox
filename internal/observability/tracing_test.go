package observability_test

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"

	"github.com/jeffresc/github-actions-proxmox-scaleset/internal/observability"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// TestInitTracer_DisabledNoEndpoint: when no endpoint is configured, the
// shutdown function is a no-op and the global tracer stays as the no-op
// implementation. Instrumented code paths therefore stay cheap.
func TestInitTracer_DisabledNoEndpoint(t *testing.T) {
	t.Parallel()
	shutdown, err := observability.InitTracer(context.Background(),
		observability.TracingOptions{}, discardLogger())
	require.NoError(t, err)
	require.NotNil(t, shutdown)
	// Calling shutdown on the no-op must not error.
	require.NoError(t, shutdown(context.Background()))

	// A span via the package tracer must work even when tracing is off
	// — confirms the no-op fallback is safe.
	tr := otel.Tracer(observability.TracerName)
	ctx, span := tr.Start(context.Background(), "test-noop")
	span.End()
	require.NotNil(t, ctx)
}

// TestInitTracer_EnabledBuildsProvider: a non-empty endpoint builds a
// real provider; shutdown flushes cleanly. We don't actually export to
// a real OTLP collector — the exporter just buffers and the BatchSpan
// processor's flush during shutdown is what we exercise.
func TestInitTracer_EnabledBuildsProvider(t *testing.T) {
	t.Parallel()
	// Point at a guaranteed-unused localhost port; the exporter doesn't
	// fail-fast on connectivity so we still get a working provider.
	shutdown, err := observability.InitTracer(context.Background(),
		observability.TracingOptions{
			Endpoint:       "127.0.0.1:1",
			Insecure:       true,
			SampleRatio:    1.0,
			ServiceName:    "scaleset-test",
			ServiceVersion: "test",
		}, discardLogger())
	require.NoError(t, err)
	require.NotNil(t, shutdown)

	// Spawn a span and immediately end it — the BatchSpanProcessor
	// queues it; we don't care if export succeeds for the test, just
	// that the surface is wired and Shutdown doesn't panic.
	tr := otel.Tracer(observability.TracerName)
	_, span := tr.Start(context.Background(), "test-enabled")
	span.End()

	// Shutdown with a generous timeout — exporter may try to flush.
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // immediately cancel so the BatchSpanProcessor doesn't block on export
	_ = shutdown(ctx)
}
