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
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/go-playground/validator/v10"
	"gopkg.in/yaml.v3"
)

// Config is the full orchestrator configuration parsed from YAML.
type Config struct {
	GitHub        GitHubConfig        `yaml:"github" validate:"required"`
	ScaleSet      ScaleSetConfig      `yaml:"scaleset" validate:"required"`
	Proxmox       ProxmoxConfig       `yaml:"proxmox" validate:"required"`
	Nodes         NodesConfig         `yaml:"nodes" validate:"required"`
	Pool          PoolConfig          `yaml:"pool" validate:"required"`
	Observability ObservabilityConfig `yaml:"observability"`
	AdminAPI      AdminAPIConfig      `yaml:"admin_api"`
	Cluster       ClusterConfig       `yaml:"cluster"`
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
	CloudInit          CloudInit      `yaml:"cloud_init"`
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

// CloudInit configures the cloud-init userdata baked into cold-path clones.
type CloudInit struct {
	User              string   `yaml:"user"`
	SSHAuthorizedKeys []string `yaml:"ssh_authorized_keys"`
	DNSServers        []string `yaml:"dns_servers"`
}

// NodesConfig selects the cluster placement strategy.
type NodesConfig struct {
	Strategy   string   `yaml:"strategy" validate:"required,oneof=single round_robin least_loaded"`
	Members    []string `yaml:"members"`
	SingleNode string   `yaml:"single_node"`
}

// PoolConfig configures pool sizes and timing.
type PoolConfig struct {
	HotSize           int    `yaml:"hot_size" validate:"gte=0"`
	WarmSize          int    `yaml:"warm_size" validate:"gte=0"`
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
}

// ClusterConfig selects the leader-election backend. In "standalone"
// mode (the default) the process is always leader; multi-replica
// deployment is unsupported. In "kubernetes" mode the process
// participates in a coordination.k8s.io/v1 Lease via
// k8s.io/client-go/tools/leaderelection and only runs the control
// plane while it holds the Lease.
type ClusterConfig struct {
	// Mode is "standalone" (default) or "kubernetes".
	Mode string `yaml:"mode" validate:"oneof=standalone kubernetes"`

	// Kubernetes is only consulted when Mode == "kubernetes".
	Kubernetes ClusterKubernetesConfig `yaml:"kubernetes"`
}

// ClusterKubernetesConfig configures the Lease-backed leader election.
// All fields default to sensible values resolved from the Downward API
// env vars POD_NAME / POD_NAMESPACE / POD_IP when present.
type ClusterKubernetesConfig struct {
	// LeaseName is the metadata.name of the Lease object. Defaults to
	// "scaleset-<scaleset.name>" when empty.
	LeaseName string `yaml:"lease_name"`

	// LeaseNamespace is the metadata.namespace of the Lease. Defaults
	// to $POD_NAMESPACE when empty.
	LeaseNamespace string `yaml:"lease_namespace"`

	// Identity uniquely identifies this replica in election logs and
	// as the Lease holderIdentity. Defaults to $POD_NAME, then
	// os.Hostname.
	Identity string `yaml:"identity"`

	// PodIP is this pod's IP, published alongside the admin port in
	// the Lease annotation so standbys can reverse-proxy admin traffic
	// to the leader. Defaults to $POD_IP.
	PodIP string `yaml:"pod_ip"`

	LeaseDuration string `yaml:"lease_duration"` // default 15s
	RenewDeadline string `yaml:"renew_deadline"` // default 10s
	RetryPeriod   string `yaml:"retry_period"`   // default  2s

	// LeaderEndpointAnnotation is the Lease metadata.annotations key
	// used to publish the leader's endpoint. Defaults to
	// "scaleset.jeffresc.dev/leader-endpoint".
	LeaderEndpointAnnotation string `yaml:"leader_endpoint_annotation"`

	// Resolved durations (populated by Resolve).
	LeaseDurationD time.Duration `yaml:"-"`
	RenewDeadlineD time.Duration `yaml:"-"`
	RetryPeriodD   time.Duration `yaml:"-"`
}

// Load reads, parses, defaults, env-resolves, and validates a YAML config
// file. The returned Config is ready to use by the rest of the
// orchestrator; on error the partially-parsed Config is discarded.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
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
	if c.Cluster.Kubernetes.LeaseDuration == "" {
		c.Cluster.Kubernetes.LeaseDuration = "15s"
	}
	if c.Cluster.Kubernetes.RenewDeadline == "" {
		c.Cluster.Kubernetes.RenewDeadline = "10s"
	}
	if c.Cluster.Kubernetes.RetryPeriod == "" {
		c.Cluster.Kubernetes.RetryPeriod = "2s"
	}
	if c.Cluster.Kubernetes.LeaderEndpointAnnotation == "" {
		c.Cluster.Kubernetes.LeaderEndpointAnnotation = "scaleset.jeffresc.dev/leader-endpoint"
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
	// Cluster.
	if err := c.resolveCluster(); err != nil {
		return err
	}
	return nil
}

// resolveCluster fills in defaults from the Downward API env vars when
// the corresponding YAML fields are empty, parses durations, and
// validates that "kubernetes" mode has the inputs it needs.
func (c *Config) resolveCluster() error {
	k := &c.Cluster.Kubernetes
	if k.LeaseName == "" {
		k.LeaseName = "scaleset-" + c.ScaleSet.Name
	}
	if k.LeaseNamespace == "" {
		k.LeaseNamespace = os.Getenv("POD_NAMESPACE")
	}
	if k.Identity == "" {
		if id := os.Getenv("POD_NAME"); id != "" {
			k.Identity = id
		} else if host, err := os.Hostname(); err == nil {
			k.Identity = host
		}
	}
	if k.PodIP == "" {
		k.PodIP = os.Getenv("POD_IP")
	}
	var err error
	k.LeaseDurationD, err = time.ParseDuration(k.LeaseDuration)
	if err != nil {
		return fmt.Errorf("cluster.kubernetes.lease_duration: %w", err)
	}
	k.RenewDeadlineD, err = time.ParseDuration(k.RenewDeadline)
	if err != nil {
		return fmt.Errorf("cluster.kubernetes.renew_deadline: %w", err)
	}
	k.RetryPeriodD, err = time.ParseDuration(k.RetryPeriod)
	if err != nil {
		return fmt.Errorf("cluster.kubernetes.retry_period: %w", err)
	}
	if c.Cluster.Mode == "kubernetes" {
		switch {
		case k.LeaseNamespace == "":
			return errors.New("cluster.kubernetes.lease_namespace is required (set the field or $POD_NAMESPACE) when cluster.mode=kubernetes")
		case k.Identity == "":
			return errors.New("cluster.kubernetes.identity is required (set the field, $POD_NAME, or have a usable hostname) when cluster.mode=kubernetes")
		case k.PodIP == "":
			return errors.New("cluster.kubernetes.pod_ip is required (set the field or $POD_IP) when cluster.mode=kubernetes")
		}
		if k.RenewDeadlineD >= k.LeaseDurationD {
			return fmt.Errorf("cluster.kubernetes.renew_deadline (%s) must be < lease_duration (%s)",
				k.RenewDeadlineD, k.LeaseDurationD)
		}
		if k.RetryPeriodD >= k.RenewDeadlineD {
			return fmt.Errorf("cluster.kubernetes.retry_period (%s) must be < renew_deadline (%s)",
				k.RetryPeriodD, k.RenewDeadlineD)
		}
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
