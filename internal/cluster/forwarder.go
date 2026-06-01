package cluster

import (
	"context"
	"crypto/tls"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
)

// Forwarder is an http.Handler that reverse-proxies inbound requests to
// the current leader's endpoint, looked up dynamically via a
// [Coordinator]. Before delegating, it strips any inbound
// X-Forwarded-For / X-Real-IP / True-Client-IP headers so an attacker
// hitting a standby cannot spoof the source IP the leader sees — the
// stdlib [httputil.ReverseProxy] then sets a fresh X-Forwarded-For
// containing only the standby's connection peer (a trusted in-cluster
// proxy from the leader's perspective).
//
// LeaderEndpoint outcomes are kept distinct so a real config bug isn't
// masked as a transient election:
//   - endpoint == "" with no error (no leader observed yet) → 503 Service
//     Unavailable with Retry-After: 2 so a hook script's retry loop
//     converges once a leader is elected.
//   - a non-nil error (e.g. "leader raft addr has no matching HTTP peer
//     entry" — a peer-map misconfiguration, not a transient) → 502 Bad
//     Gateway with the error text, and the error is logged. Otherwise an
//     operator debugging a broken peer map gets a misleading "retry"
//     signal with nothing logged.
type Forwarder struct {
	coord  Coordinator
	proxy  *httputil.ReverseProxy
	scheme string // "http" or "https" — chosen at construction time
	log    *slog.Logger
}

// ForwarderOption customises a Forwarder at construction.
type ForwarderOption func(*Forwarder)

// WithForwarderLogger sets the logger used to surface leader-lookup
// errors. A nil logger is ignored (the default slog logger is kept).
func WithForwarderLogger(log *slog.Logger) ForwarderOption {
	return func(f *Forwarder) {
		if log != nil {
			f.log = log
		}
	}
}

// NewForwarder builds a Forwarder around the given Coordinator. When
// tlsClient is non-nil the Forwarder dials the leader over https with
// the supplied TLS config (typical use: a private CA + client cert for
// mTLS). Nil leaves inter-replica traffic plain — only safe on a
// cluster-internal subnet. The returned handler is safe for concurrent
// use.
func NewForwarder(coord Coordinator, tlsClient *tls.Config, opts ...ForwarderOption) *Forwarder {
	f := &Forwarder{coord: coord, scheme: "http", log: slog.Default()}
	transport := http.DefaultTransport
	if tlsClient != nil {
		f.scheme = "https"
		transport = &http.Transport{TLSClientConfig: tlsClient.Clone()}
	}
	for _, opt := range opts {
		opt(f)
	}
	f.proxy = &httputil.ReverseProxy{
		Director:     f.director,
		ErrorHandler: f.errorHandler,
		Transport:    transport,
	}
	return f
}

// director rewrites the outgoing request URL to point at the leader.
// Returns nil silently when no leader endpoint is available; the
// transport step then fails and errorHandler emits the 503. We use a
// request-context key to surface the no-leader case to errorHandler so
// the response code is distinguishable from a real upstream failure.
func (f *Forwarder) director(r *http.Request) {
	// Drop any client-supplied source-IP headers BEFORE delegating to
	// ReverseProxy. ReverseProxy.ServeHTTP appends r.RemoteAddr to
	// X-Forwarded-For; with the inbound values removed, the leader sees
	// only the standby's connection peer and downstream rate-limiters
	// can't be tricked into keying on a spoofed IP.
	r.Header.Del("X-Forwarded-For")
	r.Header.Del("X-Real-IP")
	r.Header.Del("True-Client-IP")

	endpoint, err := f.coord.LeaderEndpoint(r.Context())
	switch {
	case err != nil:
		// A genuine lookup error — e.g. a peer-map misconfiguration —
		// is NOT a transient election. Log it and mark the request so
		// errorHandler emits a distinct 502 with the error text, instead
		// of masquerading as a "retry, no leader yet" 503.
		f.log.Warn("forwarder: leader endpoint lookup failed", "err", err)
		*r = *r.WithContext(context.WithValue(r.Context(), lookupErrKey{}, err))
		r.URL = &url.URL{Scheme: f.scheme, Host: "127.0.0.1:0"}
		return
	case endpoint == "":
		// No leader observed yet (in-flight election). Transient → 503.
		*r = *r.WithContext(context.WithValue(r.Context(), noLeaderKey{}, true))
		r.URL = &url.URL{Scheme: f.scheme, Host: "127.0.0.1:0"}
		return
	}
	r.URL.Scheme = f.scheme
	r.URL.Host = endpoint
	r.Host = endpoint
	r.RequestURI = ""
}

type noLeaderKey struct{}

type lookupErrKey struct{}

func (f *Forwarder) errorHandler(w http.ResponseWriter, r *http.Request, _ error) {
	if err, ok := r.Context().Value(lookupErrKey{}).(error); ok && err != nil {
		http.Error(w, "leader lookup error: "+err.Error(), http.StatusBadGateway)
		return
	}
	if v, _ := r.Context().Value(noLeaderKey{}).(bool); v {
		w.Header().Set("Retry-After", "2")
		http.Error(w, "no leader available", http.StatusServiceUnavailable)
		return
	}
	http.Error(w, "leader unreachable", http.StatusBadGateway)
}

// ServeHTTP forwards the request to the current leader.
func (f *Forwarder) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.proxy.ServeHTTP(w, r)
}
