package observability

import "time"

type startupTiming struct {
	scaleset, profile   string
	requested, injected time.Time
	successful          bool
	onlineAt            time.Time
}

// ObserveStartupStage uses a fixed stage vocabulary at call sites, never VM labels.
func (m *Metrics) ObserveStartupStage(scaleset, profile, stage string, elapsed time.Duration) {
	if m != nil && m.StartupStage != nil {
		m.StartupStage.WithLabelValues(scaleset, profile, stage).Observe(elapsed.Seconds())
	}
}

// TrackStartup retains only bounded, in-process timing context, never JIT data.
// Entries expire after 30 minutes; missing observations do not become successes.
func (m *Metrics) TrackStartup(scaleset, profile, name string, requested time.Time) {
	if m == nil {
		return
	}
	now := time.Now()
	if requested.IsZero() {
		requested = now
	}
	m.startupMu.Lock()
	defer m.startupMu.Unlock()
	if m.startups == nil {
		m.startups = make(map[string]startupTiming)
	}
	for k, v := range m.startups {
		if now.Sub(v.injected) > 30*time.Minute {
			delete(m.startups, k)
		}
	}
	// A hard bound protects controllers whose guests never come online.
	if len(m.startups) >= 10000 {
		return
	}
	m.startups[scaleset+"/"+name] = startupTiming{scaleset: scaleset, profile: profile, requested: requested, injected: now}
}

// ForgetStartup removes failed handoffs without recording successful startup.
func (m *Metrics) ForgetStartup(scaleset, name string) {
	if m == nil {
		return
	}
	m.startupMu.Lock()
	defer m.startupMu.Unlock()
	delete(m.startups, scaleset+"/"+name)
}

// StartupDelivered confirms injection before publishing a successful online sample.
func (m *Metrics) StartupDelivered(scaleset, name string) {
	if m == nil {
		return
	}
	key := scaleset + "/" + name
	m.startupMu.Lock()
	timing, ok := m.startups[key]
	if ok {
		timing.successful = true
		m.startups[key] = timing
	}
	online := !timing.onlineAt.IsZero()
	m.startupMu.Unlock()
	if ok && online {
		m.RunnerObservedOnline(scaleset, name)
	}
}

// RunnerObservedOnline records once when the reconciler or JobStarted observes
// a runner, after successful injection. Timings include API polling delay.
func (m *Metrics) RunnerObservedOnline(scaleset, name string) {
	if m == nil {
		return
	}
	key := scaleset + "/" + name
	m.startupMu.Lock()
	timing, ok := m.startups[key]
	if ok && timing.onlineAt.IsZero() {
		timing.onlineAt = time.Now()
	}
	if ok && !timing.successful {
		m.startups[key] = timing
		m.startupMu.Unlock()
		return
	}
	delete(m.startups, key)
	m.startupMu.Unlock()
	if !ok || time.Since(timing.injected) > 30*time.Minute {
		return
	}
	m.ObserveStartupStage(timing.scaleset, timing.profile, "jit_to_online", timing.onlineAt.Sub(timing.injected))
	m.ObserveStartupStage(timing.scaleset, timing.profile, "demand_to_online", timing.onlineAt.Sub(timing.requested))
}
