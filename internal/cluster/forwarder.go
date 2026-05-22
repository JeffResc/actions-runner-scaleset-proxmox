package cluster

import (
	"context"
	"crypto/tls"
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
// When the Coordinator's [Coordinator.LeaderEndpoint] returns the empty
// string (no leader observed yet), the Forwarder writes 503 Service
// Unavailable with Retry-After: 2 so a hook script's retry loop can
// converge once a leader is elected.
type Forwarder struct {
	coord  Coordinator
	proxy  *httputil.ReverseProxy
	scheme string // "http" or "https" — chosen at construction time
}

// NewForwarder builds a Forwarder around the given Coordinator. When
// tlsClient is non-nil the Forwarder dials the leader over https with
// the supplied TLS config (typical use: a private CA + client cert for
// mTLS). Nil leaves inter-replica traffic plain — only safe on a
// cluster-internal subnet. The returned handler is safe for concurrent
// use.
func NewForwarder(coord Coordinator, tlsClient *tls.Config) *Forwarder {
	f := &Forwarder{coord: coord, scheme: "http"}
	transport := http.DefaultTransport
	if tlsClient != nil {
		f.scheme = "https"
		transport = &http.Transport{TLSClientConfig: tlsClient.Clone()}
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
	if err != nil || endpoint == "" {
		// Mark the request so errorHandler can emit 503.
		*r = *r.WithContext(context.WithValue(r.Context(), noLeaderKey{}, true))
		// Leave URL pointing nowhere; the transport will fail and
		// errorHandler runs.
		r.URL = &url.URL{Scheme: f.scheme, Host: "127.0.0.1:0"}
		return
	}
	r.URL.Scheme = f.scheme
	r.URL.Host = endpoint
	r.Host = endpoint
	r.RequestURI = ""
}

type noLeaderKey struct{}

func (f *Forwarder) errorHandler(w http.ResponseWriter, r *http.Request, _ error) {
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
