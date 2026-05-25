// Package schedule drives cron-keyed overrides of pool hot/warm
// sizes (issue #9). Each Entry is one operator-declared window
// ("8am weekdays for 10h, hot=10 warm=20"); at every cron fire
// the Runner calls Apply with the entry's HotSize / WarmSize for
// its target Profile. When the window closes (fire + Duration)
// the Runner reverts to the profile's baseline — unless another
// entry's window is still open with a later fire time, in which
// case last-fired wins.
//
// On startup the Runner replays each entry's most recent past
// fire so a restart at 02:00 inside a "midnight + 8h" window
// re-applies the night override instead of briefly snapping
// back to defaults.
//
// Time is injectable via jonboulle/clockwork.Clock so tests drive
// deterministic fires with a fake clock; production wires
// clockwork.NewRealClock().
package schedule

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/robfig/cron/v3"
)

// Entry is one resolved schedule. Cron is the *parsed* cron
// schedule (callers parse via config's resolveSchedules so any
// expression error surfaces at load).
type Entry struct {
	Name     string
	Profile  string
	Spec     string
	Cron     cron.Schedule
	Duration time.Duration
	Location *time.Location
	HotSize  int
	WarmSize int

	// Baseline is the profile's configured hot/warm size — the
	// values the Runner reverts to when no schedule window is
	// open for this profile. Filled in by the caller from
	// ProfileConfig.{HotSize,WarmSize}.
	BaselineHot  int
	BaselineWarm int
}

// ApplyFunc is invoked on each effective-state change: either a
// schedule fires (applying its overrides) or a window closes
// without a more-recent window still open (reverting to the
// profile baseline). Implementations typically call
// pool.Manager.SetTargetSizes.
type ApplyFunc func(profile string, hotSize, warmSize int)

// Metrics is the subset of observability.Metrics the Runner
// consumes. Defined as an interface so the package stays free
// of the observability import (and so tests can stub).
type Metrics interface {
	// IncFire is invoked once per schedule fire, before Apply.
	IncFire(profile, schedule string)
	// SetActive is invoked when the effective override for a
	// profile changes. schedule is the name of the now-active
	// entry, or "" when reverting to baseline.
	SetActive(profile, schedule string)
}

// noopMetrics swallows callbacks. Returned by the Runner when
// the caller doesn't pass a Metrics implementation, so the
// hot path doesn't need a nil check.
type noopMetrics struct{}

func (noopMetrics) IncFire(string, string)   {}
func (noopMetrics) SetActive(string, string) {}

// Runner owns one goroutine that processes the schedule event
// stream in monotonic order: fire events for the soonest cron
// next-time across all entries, plus expire events for each
// open window. Single goroutine keeps the active-window
// bookkeeping serial — no locks needed across entries.
type Runner struct {
	entries []*Entry
	apply   ApplyFunc
	clock   clockwork.Clock
	log     *slog.Logger
	metrics Metrics
}

// NewRunner constructs a Runner. Entries with empty Profile are
// rejected: every override must name a profile so the apply
// callback knows which pool to update. apply is required.
//
// Each entry whose Location resolves to UTC by default (the
// caller passed an empty timezone) is logged at Info on
// construction so the operator can confirm the wall-clock time
// the cron will fire against — a common footgun called out in
// #255.
func NewRunner(entries []Entry, apply ApplyFunc, clock clockwork.Clock, log *slog.Logger, metrics Metrics) (*Runner, error) {
	if apply == nil {
		return nil, errors.New("schedule: apply func is required")
	}
	if clock == nil {
		clock = clockwork.NewRealClock()
	}
	if log == nil {
		log = slog.Default()
	}
	if metrics == nil {
		metrics = noopMetrics{}
	}
	r := &Runner{
		entries: make([]*Entry, 0, len(entries)),
		apply:   apply,
		clock:   clock,
		log:     log,
		metrics: metrics,
	}
	for i := range entries {
		e := entries[i]
		if e.Profile == "" {
			return nil, fmt.Errorf("schedule: entries[%d] %q: profile is required", i, e.Name)
		}
		if e.Cron == nil {
			return nil, fmt.Errorf("schedule: entries[%d] %q: cron is required", i, e.Name)
		}
		if e.Duration <= 0 {
			return nil, fmt.Errorf("schedule: entries[%d] %q: duration must be > 0", i, e.Name)
		}
		defaultedToUTC := false
		if e.Location == nil {
			e.Location = time.UTC
			defaultedToUTC = true
		}
		// Log the resolved timezone so an operator who omitted
		// `timezone:` in their schedule config can see at startup
		// what wall-clock zone the cron will fire against. Cron
		// expressions like "0 8 * * 1-5" mean very different
		// things in UTC vs. America/New_York; surfacing the
		// resolved value at Info catches the most common config
		// mistake without forcing a breaking validation change.
		r.log.Info("schedule: entry timezone resolved",
			"name", e.Name, "profile", e.Profile,
			"cron", e.Spec, "timezone", e.Location.String(),
			"defaulted_to_utc", defaultedToUTC)
		r.entries = append(r.entries, &e)
	}
	return r, nil
}

// activation tracks the currently-open window per profile.
// activeUntil is the time after which this entry's override
// expires (fire + Duration). When two entries' windows overlap,
// the one with the most recent firedAt wins — bookkeeping
// updates activation only when a newer fire arrives.
type activation struct {
	entry       *Entry
	firedAt     time.Time
	activeUntil time.Time
}

// Run blocks until ctx is cancelled, processing the schedule
// event stream and invoking apply on every effective-state
// change. Returns nil on graceful ctx cancel, an error
// otherwise.
//
// Startup replay: for each entry, computes the most recent fire
// time in the past (call it lastFire). If now-lastFire <
// Duration, that entry is treated as if it just fired — the
// apply is invoked once for the entry whose lastFire is most
// recent per profile, so a restart inside the night-mode
// window doesn't briefly snap back to baseline.
func (r *Runner) Run(ctx context.Context) error {
	if len(r.entries) == 0 {
		<-ctx.Done()
		return nil
	}
	active := r.replayActiveAtStartup(r.clock.Now())
	for prof, a := range active {
		r.log.Info("schedule: window active at startup",
			"profile", prof, "schedule", a.entry.Name,
			"fired_at", a.firedAt, "active_until", a.activeUntil,
			"hot_size", a.entry.HotSize, "warm_size", a.entry.WarmSize)
		r.metrics.SetActive(prof, a.entry.Name)
		r.apply(prof, a.entry.HotSize, a.entry.WarmSize)
	}
	for {
		now := r.clock.Now()
		nextFire, fireEntry := r.nextFireAfter(now)
		nextExpire, expireProfile := r.nextExpiry(active)

		var (
			wait    time.Duration
			isFire  bool
			fireE   *Entry
			expProf string
		)
		switch {
		case fireEntry == nil && expireProfile == "":
			// No more fires (impossible with cron, but defensive)
			// and nothing active — block forever.
			<-ctx.Done()
			return nil
		case fireEntry != nil && (expireProfile == "" || !nextFire.After(nextExpire)):
			wait = nextFire.Sub(now)
			isFire = true
			fireE = fireEntry
		default:
			wait = nextExpire.Sub(now)
			isFire = false
			expProf = expireProfile
		}
		if wait < 0 {
			wait = 0
		}

		select {
		case <-ctx.Done():
			return nil
		case <-r.clock.After(wait):
		}

		now = r.clock.Now()
		if isFire {
			r.handleFire(fireE, now, active)
		} else {
			r.handleExpire(expProf, now, active)
		}
	}
}

// replayActiveAtStartup walks every entry's recent past fires
// (back one Duration) and returns the active window per
// profile, picking the most-recent firedAt when entries
// overlap.
func (r *Runner) replayActiveAtStartup(now time.Time) map[string]*activation {
	active := make(map[string]*activation)
	for _, e := range r.entries {
		// Find the most recent fire time at or before now by
		// stepping cron.Next() from (now - 2*Duration) until
		// it passes now. Two durations is a comfortable margin
		// for cron expressions whose period < Duration (e.g.
		// "every 5 minutes" with duration=10m).
		start := now.Add(-2 * e.Duration).In(e.Location)
		var lastFire time.Time
		for t := e.Cron.Next(start); !t.After(now); t = e.Cron.Next(t) {
			lastFire = t
		}
		if lastFire.IsZero() || now.Sub(lastFire) >= e.Duration {
			continue
		}
		a := &activation{entry: e, firedAt: lastFire, activeUntil: lastFire.Add(e.Duration)}
		if cur, ok := active[e.Profile]; !ok || lastFire.After(cur.firedAt) {
			active[e.Profile] = a
		}
	}
	return active
}

// nextFireAfter scans every entry's next cron time and returns
// the soonest one strictly after now.
func (r *Runner) nextFireAfter(now time.Time) (time.Time, *Entry) {
	var soonest time.Time
	var which *Entry
	for _, e := range r.entries {
		t := e.Cron.Next(now.In(e.Location))
		if which == nil || t.Before(soonest) {
			soonest = t
			which = e
		}
	}
	return soonest, which
}

// nextExpiry returns the soonest activeUntil across all active
// profiles. Empty profile string means no active windows.
func (r *Runner) nextExpiry(active map[string]*activation) (time.Time, string) {
	// Iterate in profile-name order so behaviour is
	// deterministic on simultaneous expiries (e.g. two profiles
	// whose windows happen to close at exactly the same instant
	// in a fake-clock test).
	profs := make([]string, 0, len(active))
	for p := range active {
		profs = append(profs, p)
	}
	sort.Strings(profs)
	var soonest time.Time
	var which string
	for _, p := range profs {
		a := active[p]
		if which == "" || a.activeUntil.Before(soonest) {
			soonest = a.activeUntil
			which = p
		}
	}
	return soonest, which
}

// handleFire is invoked when an entry's cron Next time arrives.
// Records the fire metric, updates active-window state, and
// calls apply iff the effective override actually changed.
func (r *Runner) handleFire(e *Entry, now time.Time, active map[string]*activation) {
	r.metrics.IncFire(e.Profile, e.Name)
	r.log.Info("schedule: fire", "profile", e.Profile, "schedule", e.Name,
		"hot_size", e.HotSize, "warm_size", e.WarmSize, "duration", e.Duration)
	new := &activation{entry: e, firedAt: now, activeUntil: now.Add(e.Duration)}
	cur, had := active[e.Profile]
	active[e.Profile] = new
	// Only re-apply if the resulting (hot, warm) actually
	// changed. Two consecutive fires of the same entry with the
	// same sizes shouldn't churn pool.SetTargetSizes.
	if !had || cur.entry.HotSize != e.HotSize || cur.entry.WarmSize != e.WarmSize {
		r.metrics.SetActive(e.Profile, e.Name)
		r.apply(e.Profile, e.HotSize, e.WarmSize)
	}
}

// handleExpire is invoked when the active window for a profile
// closes. Reverts to baseline unless another entry's window is
// still open for the same profile (in which case the more-recent
// window keeps applying).
func (r *Runner) handleExpire(profile string, now time.Time, active map[string]*activation) {
	cur, ok := active[profile]
	if !ok {
		return
	}
	if now.Before(cur.activeUntil) {
		// Spurious wake (shouldn't happen with monotonic
		// clock, but defensive).
		return
	}
	delete(active, profile)
	r.log.Info("schedule: window expired", "profile", profile, "schedule", cur.entry.Name)
	// Was there a still-open overlap with an earlier firedAt?
	// We didn't track losers in `active`, so just revert to
	// baseline. Operators expressing "rolling overlap" should
	// use one long-duration schedule rather than two staggered
	// short ones.
	r.metrics.SetActive(profile, "")
	r.apply(profile, cur.entry.BaselineHot, cur.entry.BaselineWarm)
}
