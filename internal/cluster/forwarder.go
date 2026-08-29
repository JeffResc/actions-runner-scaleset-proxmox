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
// hitting a standby cannot spoof the source IP the leader sees — we then
// call [httputil.ProxyRequest.SetXForwarded] so the leader sees a fresh
// X-Forwarded-For containing only the standby's connection peer (a
// trusted in-cluster proxy from the leader's perspective).
//
// LeaderEndpoint outcomes are kept distinct so a real config bug isn't
// masked as a transient election:
//   - endpoint == "" with no error (no leader observed yet) → 503 Service
//     Unavailable with Retry-After: 2 so a hook script's retry loop
//     converges once a leader is elected.
//   - a non-nil error (e.g. "leader raft addr has no matching HTTP peer
//     entry" — a peer-map misconfiguration, not a transient) → 502 Bad
//     Gateway with a generic body, and the error is logged server-side.
//     Otherwise an operator debugging a broken peer map gets a misleading
//     "retry" signal with nothing logged. The detail stays in the log,
//     not the response body, so internal topology isn't leaked to clients
//     that can reach a standby's admin port (#361).
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
		Rewrite:      f.rewrite,
		ErrorHandler: f.errorHandler,
		Transport:    transport,
	}
	return f
}

// rewrite retargets the outgoing request at the current leader. It runs
// in the ReverseProxy.Rewrite phase — the successor to the deprecated
// Director — which receives both the inbound (pr.In) and outbound
// (pr.Out) requests.
//
// It drops any client-supplied X-Forwarded-For / X-Real-IP /
// True-Client-IP on pr.Out, then calls SetXForwarded so the leader sees a
// fresh X-Forwarded-For carrying only the standby's connection peer
// (pr.In.RemoteAddr) — the same result ReverseProxy produced
// automatically under Director. We delete X-Forwarded-For ourselves
// rather than trust the proxy's own pre-Rewrite scrub, so the spoof strip
// holds regardless of stdlib internals.
//
// When no leader endpoint is available it points pr.Out at a dead host so
// the transport step fails and errorHandler emits the response; the
// no-leader (503) and lookup-error (502) cases are surfaced via
// request-context keys read back in errorHandler.
func (f *Forwarder) rewrite(pr *httputil.ProxyRequest) {
	pr.Out.Header.Del("X-Forwarded-For")
	pr.Out.Header.Del("X-Real-IP")
	pr.Out.Header.Del("True-Client-IP")
	pr.SetXForwarded()

	endpoint, err := f.coord.LeaderEndpoint(pr.Out.Context())
	switch {
	case err != nil:
		// A genuine lookup error — e.g. a peer-map misconfiguration —
		// is NOT a transient election. Log it and mark the request so
		// errorHandler emits a distinct 502 with the error text, instead
		// of masquerading as a "retry, no leader yet" 503.
		f.log.Warn("forwarder: leader endpoint lookup failed", "err", err)
		pr.Out = pr.Out.WithContext(context.WithValue(pr.Out.Context(), lookupErrKey{}, err))
		pr.Out.URL = &url.URL{Scheme: f.scheme, Host: "127.0.0.1:0"}
		return
	case endpoint == "":
		// No leader observed yet (in-flight election). Transient → 503.
		pr.Out = pr.Out.WithContext(context.WithValue(pr.Out.Context(), noLeaderKey{}, true))
		pr.Out.URL = &url.URL{Scheme: f.scheme, Host: "127.0.0.1:0"}
		return
	}
	pr.Out.URL.Scheme = f.scheme
	pr.Out.URL.Host = endpoint
	pr.Out.Host = endpoint
}

type noLeaderKey struct{}

type lookupErrKey struct{}

func (f *Forwarder) errorHandler(w http.ResponseWriter, r *http.Request, _ error) {
	if err, ok := r.Context().Value(lookupErrKey{}).(error); ok && err != nil {
		// The error originates from the internal raft peer map and was
		// already logged in rewrite. Return a generic 502 rather than
		// reflecting the internal topology/addressing to any client that
		// can reach the standby's admin port (#361).
		http.Error(w, "leader lookup error", http.StatusBadGateway)
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
