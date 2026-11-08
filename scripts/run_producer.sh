#!/usr/bin/env bash
# Starts the AIS producer (AISstream -> Zerobus) on this machine.
# Settings come from config/producer.env. The two secrets are taken from the shell if already
# exported, otherwise asked for without echo; they stay in this process and are never written to a file.
# Stop with Ctrl+C.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONFIG_FILE="${PRODUCER_CONFIG_FILE:-$REPO_ROOT/config/producer.env}"

if [[ ! -f "$CONFIG_FILE" ]]; then
  echo "Config file not found: $CONFIG_FILE" >&2
  exit 1
fi

set -a
# shellcheck source=../config/producer.env
source "$CONFIG_FILE"
set +a

ask_secret() {
  local name="$1" prompt="$2"
  if [[ -z "${!name:-}" ]]; then
    read -r -s -p "$prompt: " "${name?}"
    echo
  fi
  if [[ -z "${!name:-}" ]]; then
    echo "$name is empty" >&2
    exit 1
  fi
  export "${name?}"
}

ask_secret PRODUCER_CLIENT_SECRET "Producer client secret"
ask_secret AISSTREAM_API_KEY "AISstream API key"

cd "$REPO_ROOT"
PYTHONPATH=src exec uv run python -m ais_producer.main
