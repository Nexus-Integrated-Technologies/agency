#!/bin/sh
set -eu

NANOCLAW_HOME="${NANOCLAW_HOME:-/nanoclaw}"
CODEX_HOME="${CODEX_HOME:-${NANOCLAW_HOME}/.codex}"
CODEX_AUTH_JSON="${PAPERCLIP_CODEX_AUTH_JSON:-${NANOCLAW_CODEX_AUTH_JSON:-}}"
CODEX_AUTH_JSON_B64="${PAPERCLIP_CODEX_AUTH_JSON_B64:-${NANOCLAW_CODEX_AUTH_JSON_B64:-}}"

mkdir -p \
  "$NANOCLAW_HOME" \
  "$CODEX_HOME" \
  "$NANOCLAW_HOME/data" \
  "$NANOCLAW_HOME/groups" \
  "$NANOCLAW_HOME/store"

if [ -n "$CODEX_AUTH_JSON" ]; then
  printf '%s' "$CODEX_AUTH_JSON" > "$CODEX_HOME/auth.json"
  chmod 600 "$CODEX_HOME/auth.json"
elif [ -n "$CODEX_AUTH_JSON_B64" ]; then
  printf '%s' "$CODEX_AUTH_JSON_B64" | base64 -d > "$CODEX_HOME/auth.json"
  chmod 600 "$CODEX_HOME/auth.json"
fi

if [ "${NANOCLAW_WORKER_BACKEND:-codex}" = "codex" ] && [ ! -s "$CODEX_HOME/auth.json" ]; then
  echo "warning: NANOCLAW_WORKER_BACKEND=codex but CODEX_HOME/auth.json is missing" >&2
fi

cd "$NANOCLAW_HOME"
exec /usr/local/bin/nanoclaw gateway serve
