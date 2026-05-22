# Contributing

Thanks for your interest in contributing to actions-runner-scaleset-proxmox!

## Dev Setup

**Requirements:** Go 1.26+, Docker (for building container images),
[golangci-lint](https://golangci-lint.run/) v2.12+, [Task](https://taskfile.dev)
(`brew install go-task/tap/go-task`), and Helm 3.14+ if you touch the chart.
[Packer](https://www.packer.io/) is optional and only needed for
`task e2e-packer`.

Common workflows ship as Taskfile targets (`task --list` to discover them):

```bash
task test         # go test -race ./... — unit tests, no build tags
task e2e          # in-process e2e suite against fake Proxmox + fake GitHub
task e2e-packer   # packer validate -syntax-only on the HCL (skips if packer missing)
task build        # compile bin/scaleset
task lint         # golangci-lint over the module
```

For raw equivalents:

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

## End-to-end tests

`task e2e` drives the real orchestrator binary in-process against fake
Proxmox and fake GitHub HTTP servers — no external dependencies needed.
The fakes implement the subsets of the Proxmox VE API and GitHub /
scaleset library protocols the orchestrator actually depends on; tests
boot the full binary, drive scenarios via the admin API, and assert on
live `/metrics` output. Run-time is ~30s.

Scenarios live under [test/e2e/](test/e2e/) and are gated by the
`e2e` build tag so `task test` skips them. CI runs both suites
([.github/workflows/ci.yaml](.github/workflows/ci.yaml)).

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
- Run with `-race` to catch data races (`task test` does this by default)
- New code should ship with tests
- Cross-cutting behaviour that needs the full binary (boot sequence,
  cluster failover, admin forwarding, etc.) goes under
  [test/e2e/](test/e2e/) with the `e2e` build tag — see the section above
