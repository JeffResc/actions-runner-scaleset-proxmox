package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jellydator/ttlcache/v3"
	"golang.org/x/sync/singleflight"
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

	// cache is a single-key TTL-bounded store of the most recent fetch
	// result. ttlcache owns the expiry accounting (WithDisableTouchOnHit
	// so a read doesn't extend the TTL — we want the entry to expire
	// relative to the fetch time, not the last read).
	cache *ttlcache.Cache[string, endpointResult]

	// sf collapses concurrent cache-miss refreshes into a single
	// kube-apiserver round-trip. Same pattern used in
	// internal/nodeselector for Proxmox /cluster/resources polling.
	// ttlcache's SuppressedLoader covers the same shape but its Loader
	// interface has no context, so we keep singleflight here to let
	// late callers bail via their own ctx.Done.
	sf singleflight.Group
}

// endpointResult is the (value, error) pair the cache stores. Caching
// the error lets the lookup keep a sticky error for the full TTL
// window — without this, a K8s API blip would re-hit the apiserver on
// every admin request until it recovers.
type endpointResult struct {
	value string
	err   error
}

// endpointCacheKey is the single key used in cache. The cache only
// ever holds one entry; we use ttlcache rather than a one-shot struct
// to delegate TTL accounting to the library.
const endpointCacheKey = "endpoint"

// newLeaderEndpointCache constructs a cache. TTL defaults to 4× the
// election RetryPeriod (~8s) — short enough that a stale entry from
// the previous leader heals within one leader-election round trip,
// long enough that admin call throughput isn't K8s-API-bound.
func newLeaderEndpointCache(client kubernetes.Interface, namespace, name, annotation string) *leaderEndpointCache {
	return &leaderEndpointCache{
		client:     client,
		namespace:  namespace,
		name:       name,
		annotation: annotation,
		cache: ttlcache.New[string, endpointResult](
			ttlcache.WithTTL[string, endpointResult](8*time.Second),
			ttlcache.WithDisableTouchOnHit[string, endpointResult](),
		),
	}
}

// get returns the cached endpoint when fresh; otherwise refreshes from
// the K8s API. Concurrent callers during a refresh share one fetch via
// singleflight so we never issue more than one API call at a time.
//
// DoChan (not Do) is used so a late caller can still observe ctx.Done
// and bail without being pinned to the first caller's fetch duration.
func (c *leaderEndpointCache) get(ctx context.Context) (string, error) {
	if item := c.cache.Get(endpointCacheKey); item != nil {
		r := item.Value()
		return r.value, r.err
	}

	ch := c.sf.DoChan(endpointCacheKey, func() (any, error) {
		value, err := c.fetch(ctx)
		res := endpointResult{value: value, err: err}
		c.cache.Set(endpointCacheKey, res, ttlcache.DefaultTTL)
		return res, nil
	})
	select {
	case r := <-ch:
		res := r.Val.(endpointResult)
		return res.value, res.err
	case <-ctx.Done():
		return "", ctx.Err()
	}
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
