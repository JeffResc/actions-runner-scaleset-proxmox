package cluster

import (
	"context"
	"errors"
	"net/http"
	"net/http/httputil"
	"net/url"
)

// Forwarder is an http.Handler that reverse-proxies inbound requests to
// the current leader's endpoint, looked up dynamically via a
// [Coordinator]. The standard library's [httputil.ReverseProxy] already
// appends RemoteAddr to X-Forwarded-For; this wrapper leaves all
// upstream proxy headers intact so the leader can recover the original
// client IP for logging or per-IP rate-limiting.
//
// When the Coordinator's [Coordinator.LeaderEndpoint] returns the empty
// string (no leader observed yet), the Forwarder writes 503 Service
// Unavailable with Retry-After: 2 so a hook script's retry loop can
// converge once a leader is elected.
type Forwarder struct {
	coord Coordinator
	proxy *httputil.ReverseProxy
}

// NewForwarder builds a Forwarder around the given Coordinator. The
// returned handler is safe for concurrent use.
func NewForwarder(coord Coordinator) *Forwarder {
	f := &Forwarder{coord: coord}
	f.proxy = &httputil.ReverseProxy{
		Director:     f.director,
		ErrorHandler: f.errorHandler,
	}
	return f
}

// ErrNoLeader is sent through Forwarder.ErrorHandler when LeaderEndpoint
// returns the empty string. The error handler maps it to 503; callers
// can detect it via errors.Is if they wrap the Forwarder for testing.
var ErrNoLeader = errors.New("cluster: no leader endpoint available")

// director rewrites the outgoing request URL to point at the leader.
// Returns nil silently when no leader endpoint is available; the
// transport step then fails and errorHandler emits the 503. We use a
// request-context key to surface the no-leader case to errorHandler so
// the response code is distinguishable from a real upstream failure.
func (f *Forwarder) director(r *http.Request) {
	endpoint, err := f.coord.LeaderEndpoint(r.Context())
	if err != nil || endpoint == "" {
		// Mark the request so errorHandler can emit 503.
		*r = *r.WithContext(context.WithValue(r.Context(), noLeaderKey{}, true))
		// Leave URL pointing nowhere; the transport will fail and
		// errorHandler runs.
		r.URL = &url.URL{Scheme: "http", Host: "127.0.0.1:0"}
		return
	}
	r.URL.Scheme = "http"
	r.URL.Host = endpoint
	r.Host = endpoint
	r.RequestURI = ""
	// httputil.ReverseProxy.ServeHTTP appends r.RemoteAddr to
	// X-Forwarded-For for us; nothing else to do.
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
