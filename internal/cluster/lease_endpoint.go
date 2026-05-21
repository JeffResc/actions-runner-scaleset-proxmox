package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
)

// publishLeaderEndpoint patches the named Lease object to set
// `metadata.annotations[annotation] = endpoint`. Idempotent — patching
// the same key with the same value is a no-op on the API server side.
//
// JSON-merge-patch is used so we touch only the targeted annotation;
// other annotations (e.g., ones added by GitOps tooling) are left
// untouched.
func publishLeaderEndpoint(ctx context.Context, client kubernetes.Interface, namespace, name, annotation, endpoint string) error {
	patch := map[string]any{
		"metadata": map[string]any{
			"annotations": map[string]string{
				annotation: endpoint,
			},
		},
	}
	body, err := json.Marshal(patch)
	if err != nil {
		return fmt.Errorf("marshal patch: %w", err)
	}
	_, err = client.CoordinationV1().
		Leases(namespace).
		Patch(ctx, name, types.MergePatchType, body, metav1.PatchOptions{})
	if err != nil {
		return fmt.Errorf("patch lease %s/%s: %w", namespace, name, err)
	}
	return nil
}

// leaderEndpointCache reads `metadata.annotations[annotation]` from a
// Lease and caches the result with a TTL. Standbys consult this on
// every admin request; without the cache the K8s API server would see
// one GET per request.
type leaderEndpointCache struct {
	client     kubernetes.Interface
	namespace  string
	name       string
	annotation string
	ttl        time.Duration

	mu       sync.Mutex
	value    string
	fetched  time.Time
	lastErr  error
	inflight chan struct{} // closed when an in-flight refresh completes
}

// newLeaderEndpointCache constructs a cache. TTL defaults to 4× the
// election RetryPeriod (~8s) — short enough that a stale entry from
// the previous leader heals within one leader-election round trip,
// long enough that admin call throughput isn't K8s-API-bound.
func newLeaderEndpointCache(client kubernetes.Interface, namespace, name, annotation string) leaderEndpointCache {
	return leaderEndpointCache{
		client:     client,
		namespace:  namespace,
		name:       name,
		annotation: annotation,
		ttl:        8 * time.Second,
	}
}

// get returns the cached endpoint when fresh; otherwise refreshes from
// the K8s API. Concurrent callers during a refresh share the in-flight
// request via a sync channel so we never issue more than one API call
// at a time.
func (c *leaderEndpointCache) get(ctx context.Context) (string, error) {
	c.mu.Lock()
	if !c.fetched.IsZero() && time.Since(c.fetched) < c.ttl {
		v, e := c.value, c.lastErr
		c.mu.Unlock()
		return v, e
	}
	// Coalesce with any in-flight refresh.
	if c.inflight != nil {
		ch := c.inflight
		c.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return "", ctx.Err()
		}
		c.mu.Lock()
		v, e := c.value, c.lastErr
		c.mu.Unlock()
		return v, e
	}
	ch := make(chan struct{})
	c.inflight = ch
	c.mu.Unlock()

	value, err := c.fetch(ctx)

	c.mu.Lock()
	c.value = value
	c.lastErr = err
	c.fetched = time.Now()
	c.inflight = nil
	close(ch)
	c.mu.Unlock()
	return value, err
}

func (c *leaderEndpointCache) fetch(ctx context.Context) (string, error) {
	lease, err := c.client.CoordinationV1().
		Leases(c.namespace).
		Get(ctx, c.name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			// Lease not yet created — election in flight. Surface
			// empty + nil so callers return 503 with Retry-After,
			// rather than a hard error.
			return "", nil
		}
		return "", fmt.Errorf("get lease %s/%s: %w", c.namespace, c.name, err)
	}
	if lease.Annotations == nil {
		return "", nil
	}
	return lease.Annotations[c.annotation], nil
}
