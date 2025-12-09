#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

IMAGE_TAG=${1:-yourdockerhubuser/mcp-data-sorter:latest}

echo "Building local binaries..."
mkdir -p bin
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -o bin/generator ./cmd/generator
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -o bin/sorter ./cmd/sorter
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -o bin/verifier ./cmd/verifier

echo "Building Docker image..."
docker build -t "${IMAGE_TAG}" -f docker/Dockerfile .

echo "Build done. To push: docker push ${IMAGE_TAG}"