package observability_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// TestSpanAttributes_VMIDAndNodeRoundTrip pins the SDK plumbing
// every hot-path span emitter in this orchestrator relies on:
// when a span is started with vm.id / vm.node attributes (or has
// them set via SetAttributes later), the recorded span carries
// both, keyed exactly as the production code keys them.
//
// Operators' Grafana / Tempo dashboards correlate spans to the
// pool's row state via these two attribute keys. A regression that
// renamed them to e.g. "vmid" / "node" would silently break every
// dashboard. This test locks in the contract.
//
// Lives in observability_test (not pool_test) so it documents the
// observability layer's promise; the per-call-site assertion that
// pool.Acquire actually sets these attributes is left to the pool
// tests, which exercise the real call path.
func TestSpanAttributes_VMIDAndNodeRoundTrip(t *testing.T) {
	t.Parallel()
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exp),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	defer func() { _ = tp.Shutdown(t.Context()) }()

	const (
		wantVMID = 10042
		wantNode = "pve1"
	)
	tr := tp.Tracer("test")
	_, span := tr.Start(t.Context(), "pool.Acquire",
		trace.WithAttributes(
			attribute.Int("vm.id", wantVMID),
			attribute.String("vm.node", wantNode),
		),
	)
	span.End()

	spans := exp.GetSpans()
	require.Len(t, spans, 1, "expected exactly one recorded span")
	got := attrsByKey(spans[0].Attributes)
	require.Equal(t, int64(wantVMID), got["vm.id"].AsInt64(),
		"vm.id attribute must round-trip; a renamed key here would silently break operator dashboards")
	require.Equal(t, wantNode, got["vm.node"].AsString(),
		"vm.node attribute must round-trip; a renamed key here would silently break operator dashboards")
}

// TestSpanAttributes_SetAttributesAfterStart pins that
// SetAttributes (the pattern pool.Acquire uses to add vm.id /
// vm.node AFTER it has resolved the row) actually propagates to
// the recorded span — not just the WithAttributes start-time
// path tested above.
func TestSpanAttributes_SetAttributesAfterStart(t *testing.T) {
	t.Parallel()
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exp),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	defer func() { _ = tp.Shutdown(t.Context()) }()

	tr := tp.Tracer("test")
	_, span := tr.Start(t.Context(), "pool.Acquire")
	span.SetAttributes(
		attribute.Int("vm.id", 20042),
		attribute.String("vm.node", "pve3"),
	)
	span.End()

	spans := exp.GetSpans()
	require.Len(t, spans, 1)
	got := attrsByKey(spans[0].Attributes)
	require.Equal(t, int64(20042), got["vm.id"].AsInt64())
	require.Equal(t, "pve3", got["vm.node"].AsString())
}

func attrsByKey(in []attribute.KeyValue) map[string]attribute.Value {
	out := make(map[string]attribute.Value, len(in))
	for _, kv := range in {
		out[string(kv.Key)] = kv.Value
	}
	return out
}
