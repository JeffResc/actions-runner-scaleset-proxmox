# syntax=docker/dockerfile:1.26

# ---------- builder ----------
# Digest-pinned for reproducible, tamper-evident builds (tags are
# mutable). Tag kept inline for readability; bump both together.
FROM golang:1.26.5-alpine@sha256:0178a641fbb4858c5f1b48e34bdaabe0350a330a1b1149aabd498d0699ff5fb2 AS builder
WORKDIR /src
RUN apk add --no-cache ca-certificates

# VERSION is stamped into the binary via -ldflags. Production CI passes
# the release tag; local builds should pass
# `--build-arg VERSION=$(git describe --tags --always)` for a meaningful
# `scaleset version` output. Defaults to "dev" so a plain `docker build`
# still produces a runnable image.
ARG VERSION=dev

# Cache go modules.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
# CGO disabled — the orchestrator has no native dependencies. State is
# held in-process via hashicorp/go-memdb (pure Go); no on-disk DB driver
# is involved. Static binary keeps the distroless runtime image minimal.
ENV CGO_ENABLED=0 GOOS=linux
RUN go build -trimpath -ldflags="-s -w -X main.version=${VERSION}" \
    -o /out/scaleset ./cmd/scaleset

# ---------- runtime ----------
# Digest-pinned (see builder note). distroless, non-root, static, CGO off.
FROM gcr.io/distroless/static-debian12:nonroot@sha256:f5b485ea962d9bd1186b2f6b3a061191539b905b82ec395de78cbfae51f20e35
COPY --from=builder /out/scaleset /usr/local/bin/scaleset
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

ENTRYPOINT ["/usr/local/bin/scaleset"]
CMD ["run", "--config=/etc/scaleset/config.yaml"]
