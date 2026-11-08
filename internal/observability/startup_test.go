package observability

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestStartupObservationOnceAndFailure(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)
	m.TrackStartup("windows", "win", "vm-1", time.Now().Add(-time.Second))
	m.RunnerObservedOnline("linux", "vm-1") // sibling sets cannot consume timings
	m.RunnerObservedOnline("windows", "vm-1")
	m.StartupDelivered("windows", "vm-1")
	m.RunnerObservedOnline("windows", "vm-1")
	m.TrackStartup("windows", "win", "vm-2", time.Now())
	m.ForgetStartup("windows", "vm-2")
	m.RunnerObservedOnline("windows", "vm-2")
	families, err := reg.Gather()
	require.NoError(t, err)
	found := false
	for _, f := range families {
		if f.GetName() == "scaleset_startup_stage_duration_seconds" {
			found = true
			require.Len(t, f.Metric, 2)
			for _, metric := range f.Metric {
				require.EqualValues(t, 1, metric.Histogram.GetSampleCount())
			}
		}
	}
	require.True(t, found)
	require.Empty(t, m.startups)
}
