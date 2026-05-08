#!/usr/bin/env bash
# Generate Go gRPC stubs from the proto definitions.
# Requires: protoc, protoc-gen-go, protoc-gen-go-grpc
#
# Install the plugins:
#   go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
#   go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
#
# Install protoc (Ubuntu/Debian):
#   sudo apt-get install -y protobuf-compiler
set -euo pipefail

PROTO_DIR="./proto"
OUT_DIR="."

echo "→ Generating gRPC stubs..."

protoc \
  --proto_path="${PROTO_DIR}" \
  --go_out="${OUT_DIR}" \
  --go_opt=paths=source_relative \
  --go-grpc_out="${OUT_DIR}" \
  --go-grpc_opt=paths=source_relative \
  "${PROTO_DIR}/kvstore/v1/kvstore.proto"

echo "✓ Done. Generated files in ${OUT_DIR}/kvstore/v1/"
