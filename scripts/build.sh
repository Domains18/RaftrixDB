#!/usr/bin/env bash
# Build RaftrixDB binaries.
set -euo pipefail

BINARY_NAME="raftrixdb"
OUTPUT_DIR="./bin"

echo "→ Building ${BINARY_NAME}..."
mkdir -p "${OUTPUT_DIR}"

go build \
  -ldflags="-s -w -X main.version=$(git describe --tags --always --dirty 2>/dev/null || echo dev)" \
  -o "${OUTPUT_DIR}/${BINARY_NAME}" \
  ./cmd/keystore

echo "✓ Built ${OUTPUT_DIR}/${BINARY_NAME}"
