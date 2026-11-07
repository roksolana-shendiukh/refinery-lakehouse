#!/usr/bin/env bash
# Regenerates the producer's record schema from the landing table in Unity Catalog,
# then compiles it into a Python class. Run after every change to the landing table.
# Needs PRODUCER_CLIENT_ID and PRODUCER_CLIENT_SECRET in the environment (never in files).
set -euo pipefail

WORKSPACE_URL="https://adb-7405604651938730.10.azuredatabricks.net"
TABLE="dbr_dev_ua5816bd.refinery_bronze.zerobus_ais_messages"

: "${PRODUCER_CLIENT_ID:?set PRODUCER_CLIENT_ID}"
: "${PRODUCER_CLIENT_SECRET:?set PRODUCER_CLIENT_SECRET}"

mkdir -p proto src/ais_producer

uv run python -m zerobus.tools.generate_proto \
  --uc-endpoint "$WORKSPACE_URL" \
  --client-id "$PRODUCER_CLIENT_ID" \
  --client-secret "$PRODUCER_CLIENT_SECRET" \
  --table "$TABLE" \
  --proto-msg AisMessage \
  --output proto/ais_message.proto

uv run python -m grpc_tools.protoc \
  --proto_path=proto \
  --python_out=src/ais_producer \
  --pyi_out=src/ais_producer \
  proto/ais_message.proto

echo "Generated proto/ais_message.proto and src/ais_producer/ais_message_pb2.py"
