package schedule_test

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/stretchr/testify/require"

	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/schedule"
)

// fakeClock is a deterministic test clock. Tests advance time
// via Advance and the Runner unblocks one wait at a time. The
// implementation is intentionally simple: only one outstanding
// After call is permitted at a time, which matches the Runner's
// design (single-goroutine event loop).
type fakeClock struct {
	mu       sync.Mutex
	now      time.Time
	pending  chan time.Time
	pendingD time.Duration
	calls    chan struct{} // one item per After call (buffered)
}

func newFakeClock(start time.Time) *fakeClock {
	return &fakeClock{now: start, calls: make(chan struct{}, 16)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	ch := make(chan time.Time, 1)
	c.pending = ch
	c.pendingD = d
	c.mu.Unlock()
	select {
	case c.calls <- struct{}{}:
	default:
	}
	return ch
}

// waitForAfterCall blocks until the Runner calls After at
// least once since the last drain. Consumes one signal so
// successive waits track successive After invocations 1:1.
func (c *fakeClock) waitForAfterCall(t *testing.T) {
	t.Helper()
	select {
	case <-c.calls:
	case <-time.After(2 * time.Second):
		t.Fatal("Runner did not call clock.After within 2s")
	}
}

// Advance moves the clock forward by d and fires the pending
// After channel iff its deadline is now <= the new clock.
func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	if c.pending != nil {
		if c.pendingD <= d {
			ch := c.pending
			now := c.now
			c.pending = nil
			c.mu.Unlock()
			ch <- now
			return
		}
		c.pendingD -= d
	}
	c.mu.Unlock()
}

type applyCall struct {
	profile string
	hot     int
	warm    int
}

type recorder struct {
	mu    sync.Mutex
	calls []applyCall
	done  chan struct{}
}

func newRecorder() *recorder { return &recorder{done: make(chan struct{}, 16)} }

func (r *recorder) apply(profile string, hot, warm int) {
	r.mu.Lock()
	r.calls = append(r.calls, applyCall{profile, hot, warm})
	r.mu.Unlock()
	select {
	case r.done <- struct{}{}:
	default:
	}
}

func (r *recorder) snapshot() []applyCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]applyCall, len(r.calls))
	copy(out, r.calls)
	return out
}

func (r *recorder) waitForCalls(t *testing.T, n int) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		r.mu.Lock()
		have := len(r.calls)
		r.mu.Unlock()
		if have >= n {
			return
		}
		select {
		case <-r.done:
		case <-deadline:
			t.Fatalf("waited 2s for %d apply calls, only got %d (%+v)", n, have, r.snapshot())
		}
	}
}

func mustParse(t *testing.T, spec string) cron.Schedule {
	t.Helper()
	p := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	s, err := p.Parse(spec)
	require.NoError(t, err)
	return s
}

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
}

func TestRunner_FireAppliesOverridesThenWindowExpires(t *testing.T) {
	t.Parallel()
	// Start one minute before an "every hour at minute 30" fire
	// so the first cron.Next is exactly 1m out.
	start := time.Date(2026, 5, 23, 12, 29, 0, 0, time.UTC)
	clock := newFakeClock(start)
	rec := newRecorder()

	entry := schedule.Entry{
		Name:         "burst",
		Profile:      "cpu",
		Spec:         "30 * * * *",
		Cron:         mustParse(t, "30 * * * *"),
		Duration:     5 * time.Minute,
		Location:     time.UTC,
		HotSize:      10,
		WarmSize:     20,
		BaselineHot:  2,
		BaselineWarm: 3,
	}
	r, err := schedule.NewRunner([]schedule.Entry{entry}, rec.apply, clock, quietLogger(), nil)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runDone := make(chan struct{})
	go func() { defer close(runDone); _ = r.Run(ctx) }()

	// First clock.After call is for the 1m wait to first fire.
	clock.waitForAfterCall(t)
	clock.Advance(1 * time.Minute)
	rec.waitForCalls(t, 1)
	require.Equal(t, []applyCall{{"cpu", 10, 20}}, rec.snapshot())

	// Runner then waits to either the next fire (1h away) or the
	// window expiry (5m away). 5m wins.
	clock.waitForAfterCall(t)
	clock.Advance(5 * time.Minute)
	rec.waitForCalls(t, 2)
	require.Equal(t, []applyCall{
		{"cpu", 10, 20},
		{"cpu", 2, 3}, // revert to baseline
	}, rec.snapshot())

	cancel()
	<-runDone
}

func TestRunner_StartupReplayActivatesMidWindow(t *testing.T) {
	t.Parallel()
	// The 12:00 fire is 90 minutes in the past; duration = 4h so
	// we're firmly inside the window. Runner must apply the
	// override immediately at startup.
	start := time.Date(2026, 5, 23, 13, 30, 0, 0, time.UTC)
	clock := newFakeClock(start)
	rec := newRecorder()

	entry := schedule.Entry{
		Name:         "morning",
		Profile:      "cpu",
		Spec:         "0 12 * * *",
		Cron:         mustParse(t, "0 12 * * *"),
		Duration:     4 * time.Hour,
		Location:     time.UTC,
		HotSize:      8,
		WarmSize:     12,
		BaselineHot:  1,
		BaselineWarm: 1,
	}
	r, err := schedule.NewRunner([]schedule.Entry{entry}, rec.apply, clock, quietLogger(), nil)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runDone := make(chan struct{})
	go func() { defer close(runDone); _ = r.Run(ctx) }()

	rec.waitForCalls(t, 1)
	require.Equal(t, []applyCall{{"cpu", 8, 12}}, rec.snapshot(), "startup replay must apply the in-progress window")

	cancel()
	<-runDone
}

func TestRunner_OverlappingSchedulesLastFiredWins(t *testing.T) {
	t.Parallel()
	// Two entries for the same profile:
	//   "morning" fires at 12:00, hot=10, duration=10h
	//   "afternoon" fires at 14:00, hot=20, duration=10h
	// Startup at 14:30 — both windows are open, afternoon's
	// firedAt (14:00) is more recent so it wins.
	start := time.Date(2026, 5, 23, 14, 30, 0, 0, time.UTC)
	clock := newFakeClock(start)
	rec := newRecorder()

	morning := schedule.Entry{
		Name: "morning", Profile: "cpu", Spec: "0 12 * * *",
		Cron: mustParse(t, "0 12 * * *"), Duration: 10 * time.Hour, Location: time.UTC,
		HotSize: 10, WarmSize: 10, BaselineHot: 1, BaselineWarm: 1,
	}
	afternoon := schedule.Entry{
		Name: "afternoon", Profile: "cpu", Spec: "0 14 * * *",
		Cron: mustParse(t, "0 14 * * *"), Duration: 10 * time.Hour, Location: time.UTC,
		HotSize: 20, WarmSize: 20, BaselineHot: 1, BaselineWarm: 1,
	}

	r, err := schedule.NewRunner([]schedule.Entry{morning, afternoon}, rec.apply, clock, quietLogger(), nil)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runDone := make(chan struct{})
	go func() { defer close(runDone); _ = r.Run(ctx) }()

	rec.waitForCalls(t, 1)
	require.Equal(t, []applyCall{{"cpu", 20, 20}}, rec.snapshot(), "last-fired (afternoon) wins on startup replay")

	cancel()
	<-runDone
}

func TestRunner_NoEntriesBlocksUntilCancel(t *testing.T) {
	t.Parallel()
	clock := newFakeClock(time.Now())
	rec := newRecorder()
	r, err := schedule.NewRunner(nil, rec.apply, clock, quietLogger(), nil)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() { defer close(runDone); _ = r.Run(ctx) }()

	// Give Run time to start.
	time.Sleep(20 * time.Millisecond)
	require.Empty(t, rec.snapshot())
	cancel()
	select {
	case <-runDone:
	case <-time.After(time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

func TestRunner_RejectsBadEntries(t *testing.T) {
	t.Parallel()
	clock := newFakeClock(time.Now())
	rec := newRecorder()

	_, err := schedule.NewRunner([]schedule.Entry{{
		Name: "no-profile", Cron: mustParse(t, "* * * * *"), Duration: time.Minute,
	}}, rec.apply, clock, quietLogger(), nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "profile")

	_, err = schedule.NewRunner([]schedule.Entry{{
		Name: "no-cron", Profile: "cpu", Duration: time.Minute,
	}}, rec.apply, clock, quietLogger(), nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cron")

	_, err = schedule.NewRunner([]schedule.Entry{{
		Name: "zero-duration", Profile: "cpu", Cron: mustParse(t, "* * * * *"),
	}}, rec.apply, clock, quietLogger(), nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "duration")
}

func TestRunner_NilApplyFuncRejected(t *testing.T) {
	t.Parallel()
	_, err := schedule.NewRunner(nil, nil, nil, nil, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "apply")
}
