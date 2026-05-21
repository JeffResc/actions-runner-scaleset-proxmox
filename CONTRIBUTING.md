# Contributing

Thanks for your interest in contributing to actions-runner-scaleset-proxmox!

## Dev Setup

**Requirements:** Go 1.26+, Docker (for building container images),
[golangci-lint](https://golangci-lint.run/) v2.12+, and Helm 3.14+ if you
touch the chart.

```bash
# Build
go build ./cmd/scaleset

# Run unit tests
go test -race ./...

# Lint
golangci-lint run --timeout=5m ./...

# Build the container image
docker build -f deploy/docker/Dockerfile -t scaleset:dev .

# Lint the Helm chart
helm lint deploy/chart
```

## Branch & PR Conventions

- Branch from `main`
- Keep PRs focused — one logical change per PR
- Include a brief description of the change and link the related issue

## Commit messages

This project uses [Conventional Commits](https://www.conventionalcommits.org/).
The commit type drives the next version when releases are cut:

- `feat:` → minor bump (`0.1.0` → `0.2.0`)
- `fix:` → patch bump (`0.1.0` → `0.1.1`)
- `feat!:` or a `BREAKING CHANGE:` footer → major bump
- `chore:`, `docs:`, `refactor:`, `test:`, `ci:` → no version bump, still
  shown in the changelog

Examples:

```
feat(pool): track reserved VMs in observability metrics
fix(provisioner): retry on transient Proxmox 5xx responses
chore(deps): bump go-proxmox to v0.2.0
```

If you squash-merge PRs, write the squash-merge commit in this format —
that's what release automation reads.

## Architecture Overview

```
cmd/scaleset/       # Binary entry point (Cobra CLI)
internal/
├── adminapi/       # HTTP admin/debug API (go-chi router)
├── cluster/        # Kubernetes Lease-based leader election
├── config/         # YAML config loading and validation
├── gh/             # GitHub Actions runner WebSocket session
├── githubauth/     # GitHub App / PAT auth wiring
├── nodeselector/   # Picks a Proxmox node for a new VM
├── observability/  # Prometheus metrics + OpenTelemetry tracing
├── pool/           # VM pool state machine (go-memdb)
├── provisioner/    # Proxmox VM clone/start/delete operations
├── scaler/         # Demand-driven reconciliation loop
├── store/          # In-memory state (go-memdb)
└── tags/           # Runner-label/tag matching
```

- **cluster:** Multiple replicas run with Kubernetes Lease-based leader
  election. The leader holds the GitHub WebSocket session and drives the
  pool; standbys are warm spares that take over on Lease expiry.
- **pool / store:** Pool state is held in-process via
  [hashicorp/go-memdb](https://github.com/hashicorp/go-memdb) — there is
  no on-disk database.
- **provisioner:** Talks to Proxmox via
  [go-proxmox](https://github.com/luthermonson/go-proxmox) to clone, start,
  and delete ephemeral runner VMs.
- **gh:** Connects to GitHub Actions using the Scaleset SDK.

## Testing

- Unit tests live alongside the code they test (`*_test.go` in the same
  package), not in a separate `tests/` directory
- Run with `-race` to catch data races
- New code should ship with tests
