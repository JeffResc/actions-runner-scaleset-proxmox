# scaleset Helm chart

Deploys [actions-runner-scaleset-proxmox](https://github.com/jeffresc/actions-runner-scaleset-proxmox) in Kubernetes as a single-replica Deployment running `cluster.mode: standalone`. One process drives the GitHub scaleset listener, the Proxmox VM pool, and the REST reconciler.

> **Keep `replicaCount: 1`.** Under `standalone` every process believes it is leader, so a second replica would drive the pool independently. The orchestrator's HA mode is `cluster.mode: raft`, which needs stable network identities and a persistent per-replica `data_dir` — this Deployment-based chart does not provide either yet. See [docs/deployment.md](../../docs/deployment.md#high-availability-with-raft).

The chart is **GitOps-safe**: the controller mutates no Kubernetes objects at all — it only talks to Proxmox and GitHub. Flux and Argo CD stay in charge of every resource here.

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

## Config file permissions

The orchestrator refuses to read a `config.yaml` that grants any access to "other", and it must be able to reach the file as the owner or through one of its groups. Kubernetes cannot set the owner UID of a projected volume, so the chart mounts the ConfigMap with `defaultMode: 0440` and relies on `podSecurityContext.fsGroup` to make the pod's group the file's group owner.

Those two settings only work as a pair. Dropping `fsGroup`, or overriding `defaultMode` back to Kubernetes' `0644` default, makes the pod fail at startup with a `config: fileperm:` error before it contacts Proxmox or GitHub.

## Rollouts and draining

The default `RollingUpdate` strategy (`maxSurge: 1`, `maxUnavailable: 0`) brings the new pod up and waits for it to become ready before tearing the old one down. The old pod receives SIGTERM and drains — no new jobs accepted, running jobs allowed to finish, idle pool VMs destroyed. `terminationGracePeriodSeconds` must exceed `scalesetConfig.pool.drain_timeout`; the default values keep both aligned at 30 minutes.

Note that `maxSurge: 1` means both pods are briefly running at once, and under `standalone` each considers itself leader. The new pod adopts the existing owner-tagged VMs rather than destroying them, so in-flight jobs are not killed — but two control planes reconciling the same pool is not a supported configuration. Set `strategy.type: Recreate` if you would rather take the downtime than the overlap.

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
