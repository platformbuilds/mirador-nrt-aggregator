# syntax=docker/dockerfile:1

## ----------------------------
## Builder stage
## ----------------------------
FROM golang:1.22 AS builder

WORKDIR /src

# First copy go mod/sum to leverage layer caching for dependencies
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

# Copy the rest of the source tree
COPY . .

ARG VERSION="dev"
ARG GIT_COMMIT="none"
ARG BUILD_DATE="unknown"

RUN --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH:-amd64} \
    go build -trimpath \
      -ldflags "-s -w -X main.version=${VERSION} -X main.commit=${GIT_COMMIT} -X main.date=${BUILD_DATE}" \
      -o /out/mirador-nrt-aggregator \
      ./cmd/mirador-nrt-aggregator

## ----------------------------
## Runtime stage
## ----------------------------
FROM gcr.io/distroless/base-debian12 AS runtime

WORKDIR /app

COPY --from=builder /out/mirador-nrt-aggregator /usr/local/bin/mirador-nrt-aggregator
COPY config.example.yaml /etc/mirador/config.yaml

USER nonroot:nonroot

ENTRYPOINT ["/usr/local/bin/mirador-nrt-aggregator"]
CMD ["-config", "/etc/mirador/config.yaml"]
