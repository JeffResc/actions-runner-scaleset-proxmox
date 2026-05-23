# scaleset Helm chart

Deploys [actions-runner-scaleset-proxmox](https://github.com/jeffresc/actions-runner-scaleset-proxmox) as N replicas in Kubernetes with `coordination.k8s.io/v1` Lease-based leader election. One replica drives the GitHub WebSocket session, the Proxmox VM pool, and the REST reconciler at any time; the rest are warm standbys that take over on Lease expiry, rebuilding their in-memory state from Proxmox via the same `pool.Manager.Recover` path used for crash recovery.

The chart is **GitOps-safe**: the only Kubernetes object the controller writes is the Lease it creates for itself, and the in-process forwarder routes admin traffic to the leader without label, endpoint, or pod mutations. Flux and Argo CD won't fight you.

## Quick start

```sh
helm install scaleset deploy/chart/ \
  --namespace gh-runners \
  --create-namespace \
  --set secrets.github.patValue=ghp_xxx \
  --set secrets.proxmox.tokenSecretValue=pve-secret \
  --set secrets.adminApi.sharedSecretValue=hex-string \
  --set scalesetConfig.scaleset.name=proxmox-ubuntu-x64 \
  --set scalesetConfig.github.scope.org=my-org \
  --set scalesetConfig.proxmox.endpoint=https://pve.example.com:8006/api2/json \
  --set scalesetConfig.proxmox.auth.token_id=scaleset@pve!automation
```

For anything beyond a dev cluster, use a `values.yaml` file. See [values.yaml](values.yaml) for the schema and inline documentation.

## Production secrets

Set `secrets.existingSecret` to a Secret name you provision separately (SealedSecrets, external-secrets, etc.). The chart will then skip its convenience Secret. Required keys: `SCALESET_GITHUB_PAT_TOKEN`, `SCALESET_PROXMOX_AUTH_TOKEN_SECRET`, and (when `admin_api` is enabled) `SCALESET_ADMIN_API_SHARED_SECRET`. These are the canonical koanf env-override names — the orchestrator picks them up automatically from the matching `SCALESET_<yaml.path.uppercased>` env var, no yaml change needed.

## How leader election interacts with rollouts

The default `RollingUpdate` strategy lets a new pod become ready before the old one is torn down. Only one replica holds the Lease at a time — Helm's rollout brings up the new pod as a standby, and the old leader hands off cleanly when its `terminationGracePeriodSeconds` elapses (must exceed `scalesetConfig.pool.drain_timeout`, which the default values keep aligned at 30 minutes).

## Image tag

`image.tag` defaults to the chart's `appVersion`. Override per release when you want to track main, a feature branch, or a digest pin.

## ServiceMonitor

Set `serviceMonitor.enabled: true` if you run prometheus-operator. The chart's Service exposes `metrics` (9100) and `admin` (9101); the ServiceMonitor scrapes the metrics port on every replica.

## Smoke tests via `helm test`

Two `helm.sh/hook: test` pods ship with the chart, run via:

```sh
helm test scaleset --namespace gh-runners
```

- **test-connection** curls `/healthz` and waits for `/readyz` to flip green (up to 60s). This catches missing config, bad Proxmox credentials, and other startup failures without needing a real GitHub workflow run.
- **test-admin-forward** hits `/admin/state` repeatedly through the Service. Because the Service round-robins, most replica counts will route at least one request through a standby — proving the admin reverse-proxy hands off to the leader correctly. Skipped if the admin API or its shared secret is disabled.

Both pods auto-delete on success (`hook-delete-policy: hook-succeeded`). On failure they're left behind so you can `kubectl logs <pod>` to debug.

## What the chart does NOT do

- It does not deploy Proxmox or the runner template VM. See [`packer/`](../../packer/) for the template-build story.
- It does not configure NetworkPolicies. Add your own according to your cluster's policy posture.
- It does not provision the GitHub App or PAT. Create those out of band and inject as secrets.
