# syntax=docker/dockerfile:1.26

# ---------- builder ----------
# Digest-pinned for reproducible, tamper-evident builds (tags are
# mutable). Tag kept inline for readability; bump both together.
FROM golang:1.27.0-alpine@sha256:4c9fe60190a2a3350ddc51de80d0224b8a6698d12bdfc999fee45ea9d6c46dbc AS builder
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
FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab
COPY --from=builder /out/scaleset /usr/local/bin/scaleset
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

ENTRYPOINT ["/usr/local/bin/scaleset"]
CMD ["run", "--config=/etc/scaleset/config.yaml"]
