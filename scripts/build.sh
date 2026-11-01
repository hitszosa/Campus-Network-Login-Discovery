#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
version=${VERSION:-dev}
mkdir -p bin
for arch in amd64 arm64; do
  CGO_ENABLED=0 GOOS=linux GOARCH="$arch" go build -trimpath \
    -ldflags "-s -w -X main.version=$version" \
    -o "bin/cnld-agent-linux-$arch" ./cmd/cnld-agent
  CGO_ENABLED=0 GOOS=linux GOARCH="$arch" go build -trimpath \
    -ldflags "-s -w" -o "bin/cnld-watch-linux-$arch" ./cmd/cnld-watch
done
cd bin
sha256sum cnld-agent-linux-amd64 cnld-agent-linux-arm64 \
  cnld-watch-linux-amd64 cnld-watch-linux-arm64 > SHA256SUMS
