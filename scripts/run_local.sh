#!/usr/bin/env bash
IMAGE=${1:-yourdockerhubuser/mcp-data-sorter:latest}
docker run --rm -it \
  --cpus=4 --memory=2g --memory-swap=2g \
  -e GEN_COUNT=10000 -e MEMBUF=20000 \
  -v /tmp/mcp_data:/data \
  --name mcp-run \
  ${IMAGE}