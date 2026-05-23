// Package config defines the orchestrator's runtime configuration: YAML
// schema, env-var expansion for secrets, defaults, and validation.
//
// Secrets are NEVER stored in the YAML file. Instead, fields ending in
// `*Env` (e.g. TokenSecretEnv) name an environment variable from which the
// secret is read at startup by [Config.Resolve]. The resolved value is
// kept in a sibling field tagged `yaml:"-"` so it is never serialized
// back to disk.
package config

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/netip"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"time"

	"github.com/go-playground/validator/v10"
	"gopkg.in/yaml.v3"

	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/fileperm"
)

// Config is the full orchestrator configuration parsed from YAML.
//
// Profiles is an optional list of runner-profile overrides (per-label
// hardware shapes). When empty, ApplyDefaults synthesises a single
// "default" profile from the global Pool + Proxmox blocks so the
// orchestrator's profile-aware internals don't have to special-case the
// single-profile config shape.
//
// Quotas and Priority are optional multi-tenancy controls. Both
// default to "disabled" — the orchestrator records per-job
// metadata but does not throttle or re-order based on it. See the
// quotas / priority subpackages for the routing semantics.
type Config struct {
	GitHub        GitHubConfig        `yaml:"github" validate:"required"`
	ScaleSet      ScaleSetConfig      `yaml:"scaleset" validate:"required"`
	Proxmox       ProxmoxConfig       `yaml:"proxmox" validate:"required"`
	Nodes         NodesConfig         `yaml:"nodes" validate:"required"`
	Pool          PoolConfig          `yaml:"pool" validate:"required"`
	Profiles      []ProfileConfig     `yaml:"profiles"`
	Quotas        QuotasConfig        `yaml:"quotas"`
	Priority      PriorityConfig      `yaml:"priority"`
	Observability ObservabilityConfig `yaml:"observability"`
	AdminAPI      AdminAPIConfig      `yaml:"admin_api"`
	Cluster       ClusterConfig       `yaml:"cluster"`
}

// QuotasConfig caps per-org / per-repo concurrent VMs. The actual
// resolution lives in internal/quotas; this block is just the YAML
// surface.
type QuotasConfig struct {
	// DefaultPerRepo applies to jobs whose repo has no override.
	// 0 disables the default. The cap is per-repo: a fleet with
	// four busy repos can have 4 × DefaultPerRepo VMs in flight
	// (still capped by the global MaxConcurrentRunners).
	DefaultPerRepo int `yaml:"default_per_repo" validate:"gte=0"`

	// DefaultPerOrg is the same idea, applied when the job has
	// only an org (the listener payload carries both for repo-
	// scoped jobs).
	DefaultPerOrg int `yaml:"default_per_org" validate:"gte=0"`

	// Overrides are exact matches. Exactly one of Match.Org or
	// Match.Repo must be set per entry; this is validated by
	// internal/quotas at startup.
	Overrides []QuotaOverride `yaml:"overrides"`
}

// QuotaOverride scopes a cap to one org or one owner/repo.
type QuotaOverride struct {
	Match         QuotaMatch `yaml:"match"`
	MaxConcurrent int        `yaml:"max_concurrent" validate:"gte=0"`
}

// QuotaMatch is the override selector. Exactly one of Org or Repo
// must be set.
type QuotaMatch struct {
	Org  string `yaml:"org,omitempty"`
	Repo string `yaml:"repo,omitempty"`
}

// PriorityConfig declares the priority classes the scaler uses to
// classify incoming jobs. See internal/priority for the matching
// semantics. The block is optional — when absent every job falls
// into the synthetic "default" class with weight 0 and
// preempt=false.
type PriorityConfig struct {
	Classes []PriorityClassConfig `yaml:"classes"`
}

// PriorityClassConfig is one operator-declared class.
type PriorityClassConfig struct {
	Name    string              `yaml:"name" validate:"required"`
	Match   PriorityMatchConfig `yaml:"match"`
	Weight  int                 `yaml:"weight"`
	Preempt bool                `yaml:"preempt"`
}

// PriorityMatchConfig is the class's selector. All non-empty
// fields must equal their corresponding job field for the class
// to match. An empty selector means "match everything" — useful
// for declaring a default class.
type PriorityMatchConfig struct {
	WorkflowLabel string `yaml:"workflow_label,omitempty"`
	Repo          string `yaml:"repo,omitempty"`
	Org           string `yaml:"org,omitempty"`
}

// GitHubConfig configures GitHub authentication, scope, and the
// REST-API-driven reconciler that closes the gap when the scaleset
// listener drops or delivers garbled lifecycle messages.
type GitHubConfig struct {
	// AuthMode selects between GitHub App ("app") and personal access token
	// ("pat") authentication.
	AuthMode string           `yaml:"auth_mode" validate:"required,oneof=app pat"`
	App      *GitHubAppConfig `yaml:"app,omitempty" validate:"required_if=AuthMode app"`
	PAT      *GitHubPATConfig `yaml:"pat,omitempty" validate:"required_if=AuthMode pat"`
	Scope    GitHubScope      `yaml:"scope" validate:"required"`

	// PollInterval is how often the reconciler queries the runners API.
	// 15s is a good default — well under the rate limit and fast enough
	// to catch missed JobCompleted within one job's worth of wall clock.
	PollInterval string `yaml:"poll_interval"`

	// AssignedGrace is how long a VM may sit in `assigned` (JIT
	// injected, waiting for the runner to pick up work) before the
	// reconciler declares it dead and destroys it.
	AssignedGrace string `yaml:"assigned_grace"`

	// RunningIdleGrace is how long a VM may sit in `running` with its
	// runner observed online + idle on GitHub before the reconciler
	// destroys it (catches missed JobCompleted callbacks).
	RunningIdleGrace string `yaml:"running_idle_grace"`

	// AssignedOfflineGrace is the same as AssignedGrace but for the
	// runner-went-offline subcase; shorter because offline is a
	// stronger failure signal.
	AssignedOfflineGrace string `yaml:"assigned_offline_grace"`

	// Resolved durations (populated by Resolve).
	PollIntervalD         time.Duration `yaml:"-"`
	AssignedGraceD        time.Duration `yaml:"-"`
	RunningIdleGraceD     time.Duration `yaml:"-"`
	AssignedOfflineGraceD time.Duration `yaml:"-"`
}

// GitHubAppConfig configures GitHub App authentication. Exactly one of
// ClientID (newer Apps; format "Iv23...") or AppID (legacy numeric ID)
// must be set; this is validated in Config.Resolve.
type GitHubAppConfig struct {
	ClientID       string `yaml:"client_id,omitempty"`
	AppID          int64  `yaml:"app_id,omitempty"`
	InstallationID int64  `yaml:"installation_id" validate:"required,gt=0"`
	PrivateKeyPath string `yaml:"private_key_path" validate:"required,file"`
}

// Issuer returns the JWT issuer string for the App — preferring ClientID
// when set, otherwise stringifying AppID.
func (g GitHubAppConfig) Issuer() string {
	if g.ClientID != "" {
		return g.ClientID
	}
	if g.AppID > 0 {
		return strconv.FormatInt(g.AppID, 10)
	}
	return ""
}

// GitHubPATConfig configures personal access token authentication. The
// token itself is read from the env var named by TokenEnv at Resolve time.
type GitHubPATConfig struct {
	TokenEnv string `yaml:"token_env" validate:"required"`
	Token    string `yaml:"-"`
}

// GitHubScope selects the registration target. Exactly one of Org or Repo
// must be set; this is checked in [Config.Resolve].
type GitHubScope struct {
	Org  string `yaml:"org,omitempty"`
	Repo string `yaml:"repo,omitempty"`
}

// ScaleSetConfig configures the scale set's identity.
type ScaleSetConfig struct {
	Name                 string   `yaml:"name" validate:"required"`
	Labels               []string `yaml:"labels"`
	RunnerGroup          string   `yaml:"runner_group"`
	MaxConcurrentRunners int      `yaml:"max_concurrent_runners" validate:"required,gt=0"`
}

// ProxmoxConfig configures the Proxmox VE connection and VM template/network.
type ProxmoxConfig struct {
	Endpoint           string         `yaml:"endpoint" validate:"required,url"`
	InsecureSkipVerify bool           `yaml:"insecure_skip_verify"`
	Auth               ProxmoxAuth    `yaml:"auth" validate:"required"`
	TemplateVMID       int            `yaml:"template_vmid" validate:"required,gt=0"`
	VMIDRange          VMIDRange      `yaml:"vmid_range" validate:"required"`
	Storage            ProxmoxStorage `yaml:"storage" validate:"required"`
	Network            ProxmoxNetwork `yaml:"network" validate:"required"`
	Clone              CloneConfig    `yaml:"clone"`
}

// ProxmoxAuth holds API token credentials. The secret is read from the env
// var named by TokenSecretEnv at Resolve time.
type ProxmoxAuth struct {
	TokenID        string `yaml:"token_id" validate:"required"`
	TokenSecretEnv string `yaml:"token_secret_env" validate:"required"`
	TokenSecret    string `yaml:"-"`
}

// VMIDRange is the inclusive range of VMIDs the orchestrator may allocate.
type VMIDRange struct {
	Min int `yaml:"min" validate:"required,gt=0"`
	Max int `yaml:"max" validate:"required,gtfield=Min"`
}

// ProxmoxStorage names the Proxmox storage pools to use.
type ProxmoxStorage struct {
	Disk     string `yaml:"disk" validate:"required"`
	Snippets string `yaml:"snippets" validate:"required"`
}

// ProxmoxNetwork configures the VM NIC. VLANTag=0 means untagged.
type ProxmoxNetwork struct {
	Bridge  string `yaml:"bridge" validate:"required"`
	VLANTag int    `yaml:"vlan_tag" validate:"gte=0,lte=4094"`
}

// CloneConfig configures clone behaviour.
type CloneConfig struct {
	// Linked controls linked-clone vs full-clone behaviour. Linked is
	// much faster but pins clones to the template's storage. *bool so
	// "unset" (apply default) is distinguishable from "explicitly false".
	Linked *bool `yaml:"linked,omitempty"`
}

// LinkedOrDefault returns true unless the user explicitly set
// `linked: false`.
func (c CloneConfig) LinkedOrDefault() bool {
	if c.Linked == nil {
		return true
	}
	return *c.Linked
}

// NodesConfig selects the cluster placement strategy.
type NodesConfig struct {
	Strategy   string   `yaml:"strategy" validate:"required,oneof=single round_robin least_loaded"`
	Members    []string `yaml:"members"`
	SingleNode string   `yaml:"single_node"`
}

// PoolConfig configures pool sizes and timing.
//
// Pool-level HotSize / WarmSize / BootMaxAttempts / VMMaxAge are
// fall-back defaults for profiles that omit those fields. GlobalMax,
// when set, caps the sum of per-profile MaxConcurrentRunners so an
// operator can put a fleet-wide ceiling above the per-profile arithmetic.
type PoolConfig struct {
	HotSize           int    `yaml:"hot_size" validate:"gte=0"`
	WarmSize          int    `yaml:"warm_size" validate:"gte=0"`
	GlobalMax         int    `yaml:"global_max" validate:"gte=0"`
	ReconcileInterval string `yaml:"reconcile_interval"`
	VMMaxAge          string `yaml:"vm_max_age"`
	DrainTimeout      string `yaml:"drain_timeout"`
	BootMaxAttempts   int    `yaml:"boot_max_attempts" validate:"gte=1"`

	// PowerPollInterval is how often the manager polls Proxmox for the
	// power state of Assigned/Running VMs. When a row's VM is observed
	// "stopped" (the in-VM gh-runner.service powers off on job
	// completion), the row is queued for destruction. This replaces
	// the previous in-VM runner-hook callback channel — Proxmox is
	// the single source of truth for "is the job over".
	// Default "3s"; must be > 0.
	PowerPollInterval string `yaml:"power_poll_interval"`

	// VMIDReuseCooldown is the minimum time after a destroy completes
	// before the allocator may reissue the same VMID. Protects against
	// PVE-side qmdestroy task settle / lock-file contention when a fresh
	// clone targets a VMID that Proxmox is still tearing down. Default
	// "30s"; must be > 0.
	VMIDReuseCooldown string `yaml:"vmid_reuse_cooldown"`

	// OrphanGrace is how long a Proxmox VM may exist without a matching
	// store row before the GitHub reconciler's sweepProxmoxOrphans
	// destroys it. Must exceed the typical Clone → guest-agent-ready →
	// JIT-inject worst case; otherwise the reconciler will destroy
	// VMs the pool worker is still booting. Default "60s"; must be > 0.
	OrphanGrace string `yaml:"orphan_grace"`

	// CloneInflightGrace is the TTL safety net for the Provisioner's
	// in-flight clone tracker. Entries older than this are pruned in
	// case Clone hangs and never returns to clear them. Should be
	// comfortably longer than the worst-case Clone latency; default
	// "5m"; must be > 0.
	CloneInflightGrace string `yaml:"clone_inflight_grace"`

	// Resolved durations (populated by Resolve).
	ReconcileIntervalD  time.Duration `yaml:"-"`
	VMMaxAgeD           time.Duration `yaml:"-"`
	DrainTimeoutD       time.Duration `yaml:"-"`
	PowerPollIntervalD  time.Duration `yaml:"-"`
	VMIDReuseCooldownD  time.Duration `yaml:"-"`
	OrphanGraceD        time.Duration `yaml:"-"`
	CloneInflightGraceD time.Duration `yaml:"-"`
}

// ProfileConfig defines a runner profile — a named bundle of hardware
// shape and per-pool sizing that the scaler routes labelled jobs to.
//
// Fields left at their zero value inherit from the global Pool /
// Proxmox blocks at Resolve time, so the simplest single-profile config
// only needs `name` + `labels`.
type ProfileConfig struct {
	// Name is the profile identifier used in metrics, tags, and the
	// scaler's routing decisions. Must be unique across Profiles and
	// match the sanitised-tag character set [a-z0-9-].
	Name string `yaml:"name" validate:"required"`

	// Labels are the GitHub Actions labels this profile satisfies.
	// The router (issue #7) picks a profile whose Labels are a
	// superset of the job's requested labels.
	Labels []string `yaml:"labels"`

	// TemplateVMID overrides the global proxmox.template_vmid for
	// this profile. Zero inherits the global value.
	TemplateVMID int `yaml:"template_vmid"`

	// CPUCores / MemoryMB / DiskGB are per-clone hardware overrides
	// applied after the clone returns. Zero leaves the template's
	// default in place.
	CPUCores int `yaml:"cpu"`
	MemoryMB int `yaml:"memory_mb"`
	DiskGB   int `yaml:"disk_gb"`

	// Storage overrides the target storage pool for full clones.
	// Empty inherits the global proxmox.storage.disk.
	Storage string `yaml:"storage"`

	// HotSize / WarmSize / MaxConcurrentRunners are per-profile pool
	// sizes. *int so the unmarshaller can distinguish "field omitted"
	// (nil — inherit from global pool / scaleset) from "explicit 0"
	// (e.g. a gpu profile that wants no hot pool, only warm). The
	// effective values are resolved into the matching *_effective
	// helpers below at ApplyDefaults time.
	HotSize              *int `yaml:"hot_size,omitempty" validate:"omitempty,gte=0"`
	WarmSize             *int `yaml:"warm_size,omitempty" validate:"omitempty,gte=0"`
	MaxConcurrentRunners *int `yaml:"max_concurrent_runners,omitempty" validate:"omitempty,gte=0"`

	// BootMaxAttempts / VMMaxAge are per-profile recycle/poison knobs.
	// Nil/empty inherits the global pool defaults. BootMaxAttempts must
	// be >= 1; accepting 0 would poison every VM in this profile on its
	// first failed boot. VMMaxAge must be a positive Go duration;
	// zero/negative is rejected at config-load (Resolve) to avoid
	// silently disabling age-based recycling. To inherit the fleet
	// default, omit the field entirely.
	BootMaxAttempts *int   `yaml:"boot_max_attempts,omitempty" validate:"omitempty,gte=1"`
	VMMaxAge        string `yaml:"vm_max_age"`

	// VMMaxAgeD is the resolved duration (populated by Resolve).
	VMMaxAgeD time.Duration `yaml:"-"`
}

// HotSizeOrDefault returns the effective hot pool size for the
// profile, dereferencing the *int and falling back to the supplied
// default when nil.
func (p ProfileConfig) HotSizeOrDefault(d int) int { return derefIntDefault(p.HotSize, d) }

// WarmSizeOrDefault returns the effective warm pool size.
func (p ProfileConfig) WarmSizeOrDefault(d int) int { return derefIntDefault(p.WarmSize, d) }

// MaxConcurrentRunnersOrDefault returns the effective concurrency cap.
func (p ProfileConfig) MaxConcurrentRunnersOrDefault(d int) int {
	return derefIntDefault(p.MaxConcurrentRunners, d)
}

// BootMaxAttemptsOrDefault returns the effective boot-attempts ceiling.
func (p ProfileConfig) BootMaxAttemptsOrDefault(d int) int {
	return derefIntDefault(p.BootMaxAttempts, d)
}

func derefIntDefault(p *int, d int) int {
	if p == nil {
		return d
	}
	return *p
}

func intp(v int) *int { return &v }

// ObservabilityConfig configures logging, metrics, and tracing endpoints.
type ObservabilityConfig struct {
	HTTPAddr  string        `yaml:"http_addr"`
	LogLevel  string        `yaml:"log_level" validate:"oneof=debug info warn error"`
	LogFormat string        `yaml:"log_format" validate:"oneof=json text"`
	Tracing   TracingConfig `yaml:"tracing"`
}

// TracingConfig enables OTLP/HTTP trace export. Disabled when Endpoint
// is empty — the orchestrator then uses a no-op tracer that's free to
// call but produces no output.
type TracingConfig struct {
	// Endpoint is the OTLP/HTTP receiver, e.g. "otel-collector:4318".
	// Host:port only — the OTLP path ("/v1/traces") is appended by the
	// exporter.
	Endpoint string `yaml:"endpoint"`

	// Insecure, when true, uses plain HTTP. Defaults to false (HTTPS).
	Insecure bool `yaml:"insecure"`

	// SampleRatio in [0.0, 1.0] selects what fraction of root spans to
	// record. Defaults to 1.0 (record everything) — appropriate for an
	// orchestrator with low span volume.
	SampleRatio float64 `yaml:"sample_ratio"`
}

// AdminAPIConfig configures the admin HTTP API. Optional; disabled when
// HTTPAddr is empty.
type AdminAPIConfig struct {
	HTTPAddr        string `yaml:"http_addr"`
	SharedSecretEnv string `yaml:"shared_secret_env"`
	SharedSecret    string `yaml:"-"`

	// TrustedProxies is the list of CIDR ranges from which the admin
	// server will honor X-Forwarded-For / X-Real-IP headers when
	// deriving the source IP for per-IP rate limiting. Requests from
	// peers outside this list have their forwarded-for headers ignored
	// — the immediate TCP peer's IP is used instead, so an attacker
	// cannot spoof the source IP via a header.
	//
	// Defaults to loopback only ("127.0.0.0/8", "::1/128") at
	// ApplyDefaults time, which is correct for the standalone case and
	// for raft deployments where the Forwarder is always on the same
	// pod. Operators terminating TLS via an in-cluster proxy should
	// add that proxy's CIDR explicitly.
	TrustedProxies []string `yaml:"trusted_proxies"`

	// TLS turns on https for the admin HTTP server and the standby
	// Forwarder dial. Nil = plain HTTP (back-compat, only safe on a
	// cluster-internal ClusterIP/NodePort). Set CAFile to enable mTLS
	// — standbys then present a client cert and the leader verifies
	// it; without CAFile the connection is one-way TLS.
	TLS *TLSConfig `yaml:"tls"`
}

// TLSConfig is a reusable PEM-on-disk TLS bundle. Both server and
// client sides of an inter-replica transport reuse it: the server
// presents (CertFile, KeyFile) and (when CAFile is set) verifies
// presented client certs against it; the client presents the same
// (CertFile, KeyFile) as its client cert.
type TLSConfig struct {
	// CertFile is the PEM-encoded server (and client, for mTLS) cert
	// chain. Required when TLSConfig is non-nil.
	CertFile string `yaml:"cert_file" validate:"required"`

	// KeyFile is the PEM-encoded private key matching CertFile.
	// Required when TLSConfig is non-nil.
	KeyFile string `yaml:"key_file" validate:"required"`

	// CAFile is the PEM-encoded CA bundle used to verify the peer's
	// cert. Empty = one-way TLS (no client-cert verification on
	// either side). Set on both replicas for full mTLS.
	CAFile string `yaml:"ca_file,omitempty"`

	// InsecureSkipVerify disables peer-cert verification. Intended for
	// lab use; refuse to set in production unless paired with a
	// well-understood lab environment.
	InsecureSkipVerify bool `yaml:"insecure_skip_verify,omitempty"`
}

// ClusterConfig selects the leader-election backend. In "standalone"
// mode (the default) the process is always leader; multi-replica
// deployment is unsupported. In "raft" mode the process participates
// in an embedded hashicorp/raft cluster across the peers declared in
// Raft.Peers and only runs the control plane while it holds raft
// leadership.
type ClusterConfig struct {
	// Mode is "standalone" (default) or "raft".
	Mode string `yaml:"mode" validate:"oneof=standalone raft"`

	// Raft is only consulted when Mode == "raft".
	Raft ClusterRaftConfig `yaml:"raft"`
}

// ClusterRaftConfig configures the embedded raft coordinator. The
// orchestrator runs raft purely as a leader-election primitive — the
// FSM is a no-op and no state is replicated. Operators declare the
// full peer list once; dynamic membership changes are not supported.
type ClusterRaftConfig struct {
	// NodeID uniquely identifies this replica in the raft
	// configuration. Must match exactly one entry in Peers.NodeID.
	// Defaults to $HOSTNAME, then os.Hostname.
	NodeID string `yaml:"node_id"`

	// BindAddr is the listen address for the raft TCP transport,
	// e.g. "0.0.0.0:7000". Required.
	BindAddr string `yaml:"bind_addr"`

	// AdvertiseAddr is what other peers use to dial this replica's
	// raft port, e.g. "10.0.0.1:7000". When empty, falls back to
	// BindAddr — only safe when BindAddr is a routable (non-wildcard)
	// address.
	AdvertiseAddr string `yaml:"advertise_addr"`

	// DataDir is the on-disk directory where raft persists its log,
	// stable, and snapshot stores. Raft's election-safety invariants
	// (currentTerm, votedFor) require these to survive restart: with
	// in-memory stores, a partition + restart in the wrong window
	// could let two replicas each believe they are leader for the
	// same term. Required when cluster.mode=raft.
	DataDir string `yaml:"data_dir"`

	// Peers is the full static peer list, including this replica.
	// All replicas must agree on this list at startup.
	Peers []ClusterRaftPeer `yaml:"peers"`

	// Bootstrap, when true, makes this replica call
	// raft.BootstrapCluster on first start. Should be true on exactly
	// one replica during initial cluster setup. Safe to leave true on
	// subsequent restarts — with persistent stores (data_dir set) raft
	// detects existing state and skips re-bootstrap.
	Bootstrap bool `yaml:"bootstrap"`

	HeartbeatTimeout string `yaml:"heartbeat_timeout"` // default 1s
	ElectionTimeout  string `yaml:"election_timeout"`  // default 1s
	CommitTimeout    string `yaml:"commit_timeout"`    // default 50ms

	// TLS, when set, enables a TLS stream layer on the raft TCP
	// transport so peer-to-peer raft RPCs (AppendEntries, etc.) are
	// encrypted. Set CAFile to enable mTLS — each peer presents its
	// own cert and verifies the other against the bundle. Without
	// TLS the raft transport is plain TCP — only safe on a private
	// network operators trust end-to-end.
	TLS *TLSConfig `yaml:"tls"`

	// Resolved durations (populated by Resolve).
	HeartbeatTimeoutD time.Duration `yaml:"-"`
	ElectionTimeoutD  time.Duration `yaml:"-"`
	CommitTimeoutD    time.Duration `yaml:"-"`
}

// ClusterRaftPeer describes one replica in the raft cluster. NodeID +
// RaftAddr uniquely identify the peer to raft; HTTPAddr is what
// standbys reverse-proxy admin requests to once that peer becomes
// leader.
type ClusterRaftPeer struct {
	NodeID   string `yaml:"node_id" validate:"required"`
	RaftAddr string `yaml:"raft_addr" validate:"required"`
	HTTPAddr string `yaml:"http_addr"`
}

// Load reads, parses, defaults, env-resolves, and validates a YAML config
// file. The returned Config is ready to use by the rest of the
// orchestrator; on error the partially-parsed Config is discarded.
func Load(path string) (*Config, error) {
	// The config file holds the Proxmox API token, GitHub webhook
	// secret, and admin bearer — the same class of credential the App
	// PEM already protects. Refuse to read when the file is world- or
	// group-readable (mode > 0o600) or owned by a UID other than the
	// process's effective UID. Surfacing the misconfiguration at
	// startup is better than after the secret has been exfiltrated.
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("config: stat %s: %w", path, err)
	}
	if err := fileperm.CheckMode(info, path, 0o600); err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	if err := fileperm.CheckOwnership(info, path); err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	data, err := os.ReadFile(path) // #nosec G304 -- path is the operator-supplied config file; perm- and owner-checked above.
	if err != nil {
		return nil, fmt.Errorf("config: read %s: %w", path, err)
	}
	return Parse(data)
}

// Parse is the in-memory equivalent of [Load]. Useful for tests.
func Parse(data []byte) (*Config, error) {
	var cfg Config
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true) // catch typos in YAML keys early
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("config: parse yaml: %w", err)
	}
	cfg.ApplyDefaults()
	if err := cfg.Resolve(); err != nil {
		return nil, fmt.Errorf("config: resolve: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("config: validate: %w", err)
	}
	return &cfg, nil
}

// defaultProfileName is the synthetic profile name used when no
// `profiles:` block is declared. Kept aligned with tags.DefaultProfile;
// this package does not import tags to avoid a circular reference.
const defaultProfileName = "default"

// ApplyDefaults fills in sane defaults for optional fields.
func (c *Config) ApplyDefaults() {
	// Pool
	if c.Pool.ReconcileInterval == "" {
		c.Pool.ReconcileInterval = "10s"
	}
	if c.Pool.VMMaxAge == "" {
		c.Pool.VMMaxAge = "24h"
	}
	if c.Pool.DrainTimeout == "" {
		c.Pool.DrainTimeout = "30m"
	}
	if c.Pool.BootMaxAttempts == 0 {
		c.Pool.BootMaxAttempts = 3
	}
	if c.Pool.PowerPollInterval == "" {
		c.Pool.PowerPollInterval = "3s"
	}
	if c.Pool.VMIDReuseCooldown == "" {
		c.Pool.VMIDReuseCooldown = "30s"
	}
	if c.Pool.OrphanGrace == "" {
		c.Pool.OrphanGrace = "60s"
	}
	if c.Pool.CloneInflightGrace == "" {
		c.Pool.CloneInflightGrace = "5m"
	}
	// Observability
	if c.Observability.HTTPAddr == "" {
		c.Observability.HTTPAddr = ":9100"
	}
	if c.Observability.LogLevel == "" {
		c.Observability.LogLevel = "info"
	}
	if c.Observability.LogFormat == "" {
		c.Observability.LogFormat = "json"
	}
	// GitHub reconciler defaults — tuned around the production failure
	// mode (over-provisioned runners that GitHub never assigns).
	if c.GitHub.PollInterval == "" {
		c.GitHub.PollInterval = "15s"
	}
	if c.GitHub.AssignedGrace == "" {
		c.GitHub.AssignedGrace = "5m"
	}
	if c.GitHub.RunningIdleGrace == "" {
		c.GitHub.RunningIdleGrace = "30s"
	}
	if c.GitHub.AssignedOfflineGrace == "" {
		c.GitHub.AssignedOfflineGrace = "2m"
	}
	// Proxmox.Clone default is captured in CloneConfig.LinkedOrDefault.
	// Cluster
	if c.Cluster.Mode == "" {
		c.Cluster.Mode = "standalone"
	}
	if c.Cluster.Raft.HeartbeatTimeout == "" {
		c.Cluster.Raft.HeartbeatTimeout = "1s"
	}
	if c.Cluster.Raft.ElectionTimeout == "" {
		c.Cluster.Raft.ElectionTimeout = "1s"
	}
	if c.Cluster.Raft.CommitTimeout == "" {
		c.Cluster.Raft.CommitTimeout = "50ms"
	}
	// Admin API: default trusted-proxy list to loopback. Operators
	// terminating TLS via an in-cluster proxy can override; raft
	// deployments where the Forwarder lives on the same pod are
	// covered by the default.
	if c.AdminAPI.TrustedProxies == nil {
		c.AdminAPI.TrustedProxies = []string{"127.0.0.0/8", "::1/128"}
	}
	// Profiles: synthesise a single default profile from the global
	// pool / proxmox / scaleset fields when no profiles block is
	// declared. The rest of the orchestrator then runs the profile
	// path uniformly — old single-profile configs still work, but
	// internally there is always at least one profile.
	if len(c.Profiles) == 0 {
		c.Profiles = []ProfileConfig{{
			Name:                 defaultProfileName,
			Labels:               append([]string(nil), c.ScaleSet.Labels...),
			HotSize:              intp(c.Pool.HotSize),
			WarmSize:             intp(c.Pool.WarmSize),
			MaxConcurrentRunners: intp(c.ScaleSet.MaxConcurrentRunners),
			BootMaxAttempts:      intp(c.Pool.BootMaxAttempts),
			VMMaxAge:             c.Pool.VMMaxAge,
		}}
	} else {
		// Each profile inherits unset sizing knobs from the global
		// pool / scaleset blocks. We assign back into the slice
		// element because range copies the value. Nil-vs-explicit-zero
		// distinction matters here: a profile that explicitly sets
		// hot_size: 0 keeps it; a profile that omits the field
		// inherits the global default.
		for i := range c.Profiles {
			p := &c.Profiles[i]
			if p.HotSize == nil {
				p.HotSize = intp(c.Pool.HotSize)
			}
			if p.WarmSize == nil {
				p.WarmSize = intp(c.Pool.WarmSize)
			}
			if p.MaxConcurrentRunners == nil {
				p.MaxConcurrentRunners = intp(c.ScaleSet.MaxConcurrentRunners)
			}
			if p.BootMaxAttempts == nil {
				p.BootMaxAttempts = intp(c.Pool.BootMaxAttempts)
			}
			if p.VMMaxAge == "" {
				p.VMMaxAge = c.Pool.VMMaxAge
			}
		}
	}
}

// Resolve performs env-var expansion of secret-bearing fields and parses
// duration strings into [time.Duration]. Call before [Validate].
func (c *Config) Resolve() error {
	// GitHub PAT token.
	if c.GitHub.PAT != nil {
		v, err := mustEnv(c.GitHub.PAT.TokenEnv)
		if err != nil {
			return fmt.Errorf("github.pat.token_env: %w", err)
		}
		c.GitHub.PAT.Token = v
	}
	// GitHub App: exactly one of client_id / app_id.
	if c.GitHub.App != nil {
		hasClient := c.GitHub.App.ClientID != ""
		hasAppID := c.GitHub.App.AppID > 0
		switch {
		case hasClient && hasAppID:
			return errors.New("github.app: set exactly one of client_id or app_id (both are present)")
		case !hasClient && !hasAppID:
			return errors.New("github.app: set exactly one of client_id or app_id (both are empty)")
		}
	}
	// GitHub scope: exactly one of org/repo.
	switch {
	case c.GitHub.Scope.Org == "" && c.GitHub.Scope.Repo == "":
		return errors.New("github.scope: exactly one of org or repo must be set (both are empty)")
	case c.GitHub.Scope.Org != "" && c.GitHub.Scope.Repo != "":
		return errors.New("github.scope: exactly one of org or repo must be set (both are present)")
	}
	// Proxmox API token secret.
	if c.Proxmox.Auth.TokenSecretEnv != "" {
		v, err := mustEnv(c.Proxmox.Auth.TokenSecretEnv)
		if err != nil {
			return fmt.Errorf("proxmox.auth.token_secret_env: %w", err)
		}
		c.Proxmox.Auth.TokenSecret = v
	}
	// Admin API shared secret (optional).
	if c.AdminAPI.SharedSecretEnv != "" {
		v, err := mustEnv(c.AdminAPI.SharedSecretEnv)
		if err != nil {
			return fmt.Errorf("admin_api.shared_secret_env: %w", err)
		}
		c.AdminAPI.SharedSecret = v
	}
	// Node selector consistency.
	switch c.Nodes.Strategy {
	case "single":
		if c.Nodes.SingleNode == "" {
			return errors.New("nodes.single_node is required when strategy=single")
		}
	case "round_robin", "least_loaded":
		if len(c.Nodes.Members) == 0 {
			return fmt.Errorf("nodes.members is required when strategy=%s", c.Nodes.Strategy)
		}
	}
	// Durations.
	var err error
	c.Pool.ReconcileIntervalD, err = time.ParseDuration(c.Pool.ReconcileInterval)
	if err != nil {
		return fmt.Errorf("pool.reconcile_interval: %w", err)
	}
	c.Pool.VMMaxAgeD, err = time.ParseDuration(c.Pool.VMMaxAge)
	if err != nil {
		return fmt.Errorf("pool.vm_max_age: %w", err)
	}
	c.Pool.DrainTimeoutD, err = time.ParseDuration(c.Pool.DrainTimeout)
	if err != nil {
		return fmt.Errorf("pool.drain_timeout: %w", err)
	}
	c.Pool.PowerPollIntervalD, err = time.ParseDuration(c.Pool.PowerPollInterval)
	if err != nil {
		return fmt.Errorf("pool.power_poll_interval: %w", err)
	}
	if c.Pool.PowerPollIntervalD <= 0 {
		return errors.New("pool.power_poll_interval must be positive")
	}
	c.Pool.VMIDReuseCooldownD, err = time.ParseDuration(c.Pool.VMIDReuseCooldown)
	if err != nil {
		return fmt.Errorf("pool.vmid_reuse_cooldown: %w", err)
	}
	if c.Pool.VMIDReuseCooldownD <= 0 {
		return errors.New("pool.vmid_reuse_cooldown must be positive")
	}
	c.Pool.OrphanGraceD, err = time.ParseDuration(c.Pool.OrphanGrace)
	if err != nil {
		return fmt.Errorf("pool.orphan_grace: %w", err)
	}
	if c.Pool.OrphanGraceD <= 0 {
		return errors.New("pool.orphan_grace must be positive")
	}
	c.Pool.CloneInflightGraceD, err = time.ParseDuration(c.Pool.CloneInflightGrace)
	if err != nil {
		return fmt.Errorf("pool.clone_inflight_grace: %w", err)
	}
	if c.Pool.CloneInflightGraceD <= 0 {
		return errors.New("pool.clone_inflight_grace must be positive")
	}
	c.GitHub.PollIntervalD, err = time.ParseDuration(c.GitHub.PollInterval)
	if err != nil {
		return fmt.Errorf("github.poll_interval: %w", err)
	}
	c.GitHub.AssignedGraceD, err = time.ParseDuration(c.GitHub.AssignedGrace)
	if err != nil {
		return fmt.Errorf("github.assigned_grace: %w", err)
	}
	c.GitHub.RunningIdleGraceD, err = time.ParseDuration(c.GitHub.RunningIdleGrace)
	if err != nil {
		return fmt.Errorf("github.running_idle_grace: %w", err)
	}
	c.GitHub.AssignedOfflineGraceD, err = time.ParseDuration(c.GitHub.AssignedOfflineGrace)
	if err != nil {
		return fmt.Errorf("github.assigned_offline_grace: %w", err)
	}
	// Profiles: parse per-profile vm_max_age now so callers don't have
	// to re-parse on the hot path. Empty inherits from pool (already
	// applied in ApplyDefaults, so this normally just round-trips).
	for i := range c.Profiles {
		p := &c.Profiles[i]
		if p.VMMaxAge == "" {
			continue
		}
		d, err := time.ParseDuration(p.VMMaxAge)
		if err != nil {
			return fmt.Errorf("profiles[%d].vm_max_age: %w", i, err)
		}
		if d <= 0 {
			return fmt.Errorf("profiles[%q].vm_max_age must be positive (got %q)", p.Name, p.VMMaxAge)
		}
		p.VMMaxAgeD = d
	}
	// Cluster.
	if err := c.resolveCluster(); err != nil {
		return err
	}
	return nil
}

// resolveCluster fills in NodeID from $HOSTNAME/os.Hostname when
// empty, parses raft timing durations, and validates that "raft"
// mode has a self-consistent peer list.
func (c *Config) resolveCluster() error {
	r := &c.Cluster.Raft
	if r.NodeID == "" {
		if id := os.Getenv("HOSTNAME"); id != "" {
			r.NodeID = id
		} else if host, err := os.Hostname(); err == nil {
			r.NodeID = host
		}
	}
	var err error
	r.HeartbeatTimeoutD, err = time.ParseDuration(r.HeartbeatTimeout)
	if err != nil {
		return fmt.Errorf("cluster.raft.heartbeat_timeout: %w", err)
	}
	r.ElectionTimeoutD, err = time.ParseDuration(r.ElectionTimeout)
	if err != nil {
		return fmt.Errorf("cluster.raft.election_timeout: %w", err)
	}
	r.CommitTimeoutD, err = time.ParseDuration(r.CommitTimeout)
	if err != nil {
		return fmt.Errorf("cluster.raft.commit_timeout: %w", err)
	}
	if c.Cluster.Mode != "raft" {
		return nil
	}
	switch {
	case r.NodeID == "":
		return errors.New("cluster.raft.node_id is required (set the field, $HOSTNAME, or have a usable hostname) when cluster.mode=raft")
	case r.BindAddr == "":
		return errors.New("cluster.raft.bind_addr is required when cluster.mode=raft")
	case len(r.Peers) == 0:
		return errors.New("cluster.raft.peers must list every replica (including this one) when cluster.mode=raft")
	}
	selfFound := false
	seen := make(map[string]struct{}, len(r.Peers))
	for i, p := range r.Peers {
		if p.NodeID == "" {
			return fmt.Errorf("cluster.raft.peers[%d].node_id is required", i)
		}
		if _, dup := seen[p.NodeID]; dup {
			return fmt.Errorf("cluster.raft.peers: duplicate node_id %q", p.NodeID)
		}
		seen[p.NodeID] = struct{}{}
		if p.RaftAddr == "" {
			return fmt.Errorf("cluster.raft.peers[%d].raft_addr is required", i)
		}
		if p.NodeID == r.NodeID {
			selfFound = true
		}
	}
	if !selfFound {
		return fmt.Errorf("cluster.raft.peers does not include this replica's node_id %q", r.NodeID)
	}
	if r.DataDir == "" {
		return errors.New("cluster.raft.data_dir is required when cluster.mode=raft (raft consensus state must survive restart)")
	}
	return nil
}

// Validate runs the struct-tag validator and any cross-field checks not
// expressible in tags.
func (c *Config) Validate() error {
	v := validator.New(validator.WithRequiredStructEnabled())
	if err := v.Struct(c); err != nil {
		return err
	}
	if c.Pool.HotSize+c.Pool.WarmSize > c.ScaleSet.MaxConcurrentRunners {
		return fmt.Errorf("pool.hot_size+pool.warm_size (%d) must not exceed scaleset.max_concurrent_runners (%d)",
			c.Pool.HotSize+c.Pool.WarmSize, c.ScaleSet.MaxConcurrentRunners)
	}
	if c.Proxmox.TemplateVMID >= c.Proxmox.VMIDRange.Min && c.Proxmox.TemplateVMID <= c.Proxmox.VMIDRange.Max {
		return fmt.Errorf("proxmox.template_vmid (%d) must be outside vmid_range [%d, %d]",
			c.Proxmox.TemplateVMID, c.Proxmox.VMIDRange.Min, c.Proxmox.VMIDRange.Max)
	}
	// The Proxmox API token is sent in a header on every request; require
	// https:// so the credential never traverses the wire in cleartext.
	u, err := url.Parse(c.Proxmox.Endpoint)
	if err != nil || u.Scheme != "https" {
		return errors.New("proxmox.endpoint must use https:// (the API token is sent on every request and would leak in cleartext over http)")
	}
	for _, cidr := range c.AdminAPI.TrustedProxies {
		if _, err := netip.ParsePrefix(cidr); err != nil {
			return fmt.Errorf("admin_api.trusted_proxies: invalid CIDR %q: %w", cidr, err)
		}
	}
	if c.AdminAPI.TLS != nil {
		if err := c.AdminAPI.TLS.validate("admin_api.tls"); err != nil {
			return err
		}
	}
	if c.Cluster.Raft.TLS != nil {
		if err := c.Cluster.Raft.TLS.validate("cluster.raft.tls"); err != nil {
			return err
		}
	}
	if err := c.validateProfiles(); err != nil {
		return err
	}
	if err := c.validateQuotas(); err != nil {
		return err
	}
	if err := c.validatePriority(); err != nil {
		return err
	}
	return nil
}

// validateQuotas enforces the exactly-one-of-org-or-repo rule on
// overrides. Detailed semantics live in internal/quotas; the check
// is duplicated here so a malformed config fails at Load time with
// a stable error message instead of later at app startup.
func (c *Config) validateQuotas() error {
	for i, o := range c.Quotas.Overrides {
		hasOrg, hasRepo := o.Match.Org != "", o.Match.Repo != ""
		if hasOrg == hasRepo {
			return fmt.Errorf("quotas.overrides[%d].match: set exactly one of org or repo", i)
		}
	}
	return nil
}

// validatePriority enforces non-empty, unique class names. The
// upstream priority.Matcher enforces the same rules at New() time;
// catching them here is operator-experience polish.
func (c *Config) validatePriority() error {
	seen := make(map[string]int, len(c.Priority.Classes))
	for i, cl := range c.Priority.Classes {
		if cl.Name == "" {
			return fmt.Errorf("priority.classes[%d].name is required", i)
		}
		if prev, dup := seen[cl.Name]; dup {
			return fmt.Errorf("priority.classes: duplicate class name %q at indexes %d and %d", cl.Name, prev, i)
		}
		seen[cl.Name] = i
	}
	return nil
}

// profileNameRE constrains profile names to the same character set
// Proxmox tags accept after sanitisation, so a name can round-trip
// through tags.ProfileTag without surprises.
var profileNameRE = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,62}$`)

// validateProfiles enforces non-empty, unique, well-formed profile
// names and the GlobalMax ceiling. Per-profile inheritance from global
// pool fields already happened in ApplyDefaults.
func (c *Config) validateProfiles() error {
	if len(c.Profiles) == 0 {
		// ApplyDefaults guarantees ≥ 1 profile; defending against a
		// caller that bypassed it (e.g. constructing a Config in a
		// test) keeps later code from segfaulting on an empty slice.
		return errors.New("profiles: at least one profile is required (ApplyDefaults synthesises a default)")
	}
	seen := make(map[string]int, len(c.Profiles))
	sumMax := 0
	for i, p := range c.Profiles {
		if !profileNameRE.MatchString(p.Name) {
			return fmt.Errorf("profiles[%d].name %q must match %s", i, p.Name, profileNameRE.String())
		}
		if prev, dup := seen[p.Name]; dup {
			return fmt.Errorf("profiles: duplicate name %q at indexes %d and %d", p.Name, prev, i)
		}
		seen[p.Name] = i
		maxConc := p.MaxConcurrentRunnersOrDefault(c.ScaleSet.MaxConcurrentRunners)
		if maxConc <= 0 {
			return fmt.Errorf("profiles[%d] %q: max_concurrent_runners must be > 0 (inherited from scaleset when omitted)", i, p.Name)
		}
		hot := p.HotSizeOrDefault(c.Pool.HotSize)
		warm := p.WarmSizeOrDefault(c.Pool.WarmSize)
		if hot+warm > maxConc {
			return fmt.Errorf("profiles[%d] %q: hot_size+warm_size (%d) must not exceed max_concurrent_runners (%d)",
				i, p.Name, hot+warm, maxConc)
		}
		// BootMaxAttempts must be >= 1 — accepting 0 would poison every
		// VM in this profile on its first failed boot. ApplyDefaults
		// inherits the global value when the profile omits the field, so
		// any non-nil pointer here is operator intent.
		if p.BootMaxAttempts != nil && *p.BootMaxAttempts < 1 {
			return fmt.Errorf("profiles[%d] %q: boot_max_attempts (%d) must be >= 1",
				i, p.Name, *p.BootMaxAttempts)
		}
		sumMax += maxConc
	}
	if c.Pool.GlobalMax > 0 && sumMax > c.Pool.GlobalMax {
		return fmt.Errorf("pool.global_max (%d) is below the sum of profile.max_concurrent_runners (%d)", c.Pool.GlobalMax, sumMax)
	}
	// Label-routing safety: every label the scaleset advertises must
	// be present on at least one profile, otherwise a job arriving
	// with that label has nowhere to go. The router (issue #7) will
	// reject such jobs at runtime; catching the misconfiguration at
	// load time is a much better operator experience.
	if gaps := uncoveredScaleSetLabels(c.ScaleSet.Labels, c.Profiles); len(gaps) > 0 {
		return fmt.Errorf("profiles: no profile covers scaleset labels %v — add the labels to a profile or remove them from scaleset.labels", gaps)
	}
	return nil
}

// uncoveredScaleSetLabels returns the labels in scaleSetLabels that
// no profile's label set contains. Used by Validate to enforce the
// label-routing coverage invariant.
func uncoveredScaleSetLabels(scaleSetLabels []string, profiles []ProfileConfig) []string {
	union := make(map[string]struct{})
	for _, p := range profiles {
		for _, l := range p.Labels {
			union[l] = struct{}{}
		}
	}
	var gaps []string
	for _, l := range scaleSetLabels {
		if _, ok := union[l]; !ok {
			gaps = append(gaps, l)
		}
	}
	return gaps
}

// BuildServerTLS returns a *tls.Config suitable for an
// http.Server.TLSConfig. When CAFile is set, ClientAuth is set to
// RequireAndVerifyClientCert so mTLS is enforced — pair with a
// matching client-side cert on every peer.
func (t *TLSConfig) BuildServerTLS() (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(t.CertFile, t.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("load tls keypair %q/%q: %w", t.CertFile, t.KeyFile, err)
	}
	cfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}
	if t.CAFile != "" {
		pool, err := loadCAPool(t.CAFile)
		if err != nil {
			return nil, err
		}
		cfg.ClientCAs = pool
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return cfg, nil
}

// BuildClientTLS returns a *tls.Config suitable for an
// http.Transport.TLSClientConfig. CAFile pins the server cert against
// a private CA when set; otherwise the system roots are used (subject
// to InsecureSkipVerify).
func (t *TLSConfig) BuildClientTLS() (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(t.CertFile, t.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("load tls keypair %q/%q: %w", t.CertFile, t.KeyFile, err)
	}
	cfg := &tls.Config{
		Certificates:       []tls.Certificate{cert},
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: t.InsecureSkipVerify, //nolint:gosec // user-opt-in for lab/test
	}
	if t.CAFile != "" {
		pool, err := loadCAPool(t.CAFile)
		if err != nil {
			return nil, err
		}
		cfg.RootCAs = pool
	}
	return cfg, nil
}

func loadCAPool(path string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(path) //nolint:gosec // path comes from a trusted config file
	if err != nil {
		return nil, fmt.Errorf("read ca file %q: %w", path, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("ca file %q contained no usable PEM blocks", path)
	}
	return pool, nil
}

// validate checks that referenced files exist. Loading and parsing the
// PEM content happens lazily in BuildServerTLS / BuildClientTLS so a
// missing cert at config-load time is a hard failure, but a bad PEM
// surfaces with file context when the server actually starts.
func (t *TLSConfig) validate(prefix string) error {
	if t.CertFile == "" {
		return fmt.Errorf("%s.cert_file is required when tls is configured", prefix)
	}
	if t.KeyFile == "" {
		return fmt.Errorf("%s.key_file is required when tls is configured", prefix)
	}
	if _, err := os.Stat(t.CertFile); err != nil {
		return fmt.Errorf("%s.cert_file %q: %w", prefix, t.CertFile, err)
	}
	if _, err := os.Stat(t.KeyFile); err != nil {
		return fmt.Errorf("%s.key_file %q: %w", prefix, t.KeyFile, err)
	}
	if t.CAFile != "" {
		if _, err := os.Stat(t.CAFile); err != nil {
			return fmt.Errorf("%s.ca_file %q: %w", prefix, t.CAFile, err)
		}
	}
	return nil
}

// mustEnv reads a required env var, returning an error if it's empty.
func mustEnv(name string) (string, error) {
	v, ok := os.LookupEnv(name)
	if !ok || v == "" {
		return "", fmt.Errorf("env var %q is not set or empty", name)
	}
	return v, nil
}
