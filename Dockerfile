# syntax=docker/dockerfile:1
ARG GO_IMAGE=golang:1.27.3-alpine@sha256:d200d2dd76b03a99b52651fd1325aff7dcd35422c35c01bac56571b0507f3deb
FROM --platform=$BUILDPLATFORM ${GO_IMAGE} AS build
WORKDIR /src
RUN apk add --no-cache ca-certificates
COPY go.mod go.sum ./
RUN go mod download
COPY cmd/ cmd/
COPY internal/ internal/
ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
RUN --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath \
    -ldflags="-s -w -X main.version=${VERSION}" -o /cnld-agent ./cmd/cnld-agent

FROM scratch
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /cnld-agent /cnld-agent
USER 65532:65532
ARG VERSION=dev
LABEL org.opencontainers.image.source="https://github.com/hitszosa/Campus-Network-Login-Discovery" \
      org.opencontainers.image.version="${VERSION}"
ENTRYPOINT ["/cnld-agent"]
CMD ["-config", "/config.yaml"]
