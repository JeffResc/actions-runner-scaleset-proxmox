# syntax=docker/dockerfile:1.7

# ---------- builder ----------
FROM golang:1.26.3-alpine AS builder
WORKDIR /src
RUN apk add --no-cache git ca-certificates

# Cache go modules.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
# CGO disabled — the orchestrator has no native dependencies. State is
# held in-process via hashicorp/go-memdb (pure Go); no on-disk DB driver
# is involved. Static binary keeps the distroless runtime image minimal.
ENV CGO_ENABLED=0 GOOS=linux
RUN go build -trimpath -ldflags="-s -w -X main.version=$(git describe --tags --always 2>/dev/null || echo dev)" \
    -o /out/scaleset ./cmd/scaleset

# ---------- runtime ----------
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /out/scaleset /usr/local/bin/scaleset
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

ENTRYPOINT ["/usr/local/bin/scaleset"]
CMD ["run", "--config=/etc/scaleset/config.yaml"]
