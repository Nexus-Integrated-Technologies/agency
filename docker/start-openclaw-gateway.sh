#!/bin/sh
set -eu

NANOCLAW_HOME="${NANOCLAW_HOME:-/nanoclaw}"
CODEX_HOME="${CODEX_HOME:-${NANOCLAW_HOME}/.codex}"
CODEX_AUTH_JSON="${PAPERCLIP_CODEX_AUTH_JSON:-${NANOCLAW_CODEX_AUTH_JSON:-}}"
CODEX_AUTH_JSON_B64="${PAPERCLIP_CODEX_AUTH_JSON_B64:-${NANOCLAW_CODEX_AUTH_JSON_B64:-}}"

export NANOCLAW_OPENCLAW_GATEWAY_EXECUTION_LANE="${NANOCLAW_OPENCLAW_GATEWAY_EXECUTION_LANE:-omx}"
export NANOCLAW_EXECUTION_LANE="${NANOCLAW_EXECUTION_LANE:-omx}"
if [ -n "${PORT:-}" ] && [ "${NANOCLAW_OPENCLAW_GATEWAY_PORT:-18789}" = "18789" ]; then
  export NANOCLAW_OPENCLAW_GATEWAY_PORT="$PORT"
else
  export NANOCLAW_OPENCLAW_GATEWAY_PORT="${NANOCLAW_OPENCLAW_GATEWAY_PORT:-18789}"
fi
RENDER_PUBLIC_ORIGIN=""
if [ -n "${RENDER_EXTERNAL_URL:-}" ]; then
  RENDER_PUBLIC_ORIGIN="${RENDER_EXTERNAL_URL%/}"
elif [ -n "${RENDER_EXTERNAL_HOSTNAME:-}" ]; then
  RENDER_PUBLIC_ORIGIN="https://${RENDER_EXTERNAL_HOSTNAME}"
elif [ -n "${RENDER_SERVICE_NAME:-}" ]; then
  RENDER_PUBLIC_ORIGIN="https://${RENDER_SERVICE_NAME}.onrender.com"
fi
if [ -n "$RENDER_PUBLIC_ORIGIN" ]; then
  RENDER_GATEWAY_URL="${RENDER_PUBLIC_ORIGIN}/openclaw"
  RENDER_GATEWAY_WS_URL="$(printf '%s' "$RENDER_GATEWAY_URL" | sed 's#^https:#wss:#; s#^http:#ws:#')"
  export NANOCLAW_OPENCLAW_GATEWAY_PUBLIC_WS_URL="${NANOCLAW_OPENCLAW_GATEWAY_PUBLIC_WS_URL:-$RENDER_GATEWAY_WS_URL}"
  export NANOCLAW_OPENCLAW_GATEWAY_PUBLIC_HEALTH_URL="${NANOCLAW_OPENCLAW_GATEWAY_PUBLIC_HEALTH_URL:-${RENDER_GATEWAY_URL}/health}"
fi
export NANOCLAW_OPENCLAW_GATEWAY_PUBLIC_WS_URL="wss://nexus-openclaw-gateway.onrender.com/openclaw"
export NANOCLAW_OPENCLAW_GATEWAY_PUBLIC_HEALTH_URL="https://nexus-openclaw-gateway.onrender.com/openclaw/health"
export NANOCLAW_CLAUDE_BIN="${NANOCLAW_CLAUDE_BIN:-claude}"
export NANOCLAW_WORKER_BACKEND="${NANOCLAW_FORCE_WORKER_BACKEND:-${NANOCLAW_PRIMARY_WORKER_BACKEND:-azure-openai}}"
export NANOCLAW_AZURE_OPENAI_FALLBACK_BACKEND="${NANOCLAW_AZURE_OPENAI_FALLBACK_BACKEND:-codex}"
export NANOCLAW_CODEX_USAGE_FALLBACK_BACKEND="${NANOCLAW_CODEX_USAGE_FALLBACK_BACKEND:-azure-openai}"
export AZURE_OPENAI_ENDPOINT="${AZURE_OPENAI_ENDPOINT:-https://nis-openai-c5e29800.openai.azure.com/}"
export AZURE_OPENAI_DEPLOYMENT="${AZURE_OPENAI_DEPLOYMENT:-nanoclaw-gpt-4-1-mini}"
export AZURE_OPENAI_API_VERSION="${AZURE_OPENAI_API_VERSION:-2024-10-21}"
export NANOCLAW_ZAI_ANTHROPIC_BASE_URL="${NANOCLAW_ZAI_ANTHROPIC_BASE_URL:-https://api.z.ai/api/anthropic}"
export NANOCLAW_ZAI_MODEL="${NANOCLAW_ZAI_MODEL:-glm-4.7}"
export NANOCLAW_OMX_RUNNER_LOCATION="${NANOCLAW_OMX_RUNNER_LOCATION:-local}"
export NANOCLAW_OMX_RUNNER_MODE="${NANOCLAW_OMX_RUNNER_MODE:-local}"
export NANOCLAW_OMX_RUNNER_PATH="${NANOCLAW_OMX_RUNNER_PATH:-/usr/local/bin/omx-paperclip-runner}"
export NANOCLAW_OMX_STATE_ROOT="${NANOCLAW_OMX_STATE_ROOT:-${NANOCLAW_HOME}/.nanoclaw-omx}"
export NANOCLAW_OMX_DEFAULT_MODE="${NANOCLAW_OMX_DEFAULT_MODE:-${PAPERCLIP_OMX_MODE:-exec}}"
export NANOCLAW_OMX_DEFAULT_MAX_WORKERS="${NANOCLAW_OMX_DEFAULT_MAX_WORKERS:-${PAPERCLIP_OMX_MAX_WORKERS:-1}}"
export NANOCLAW_OMX_EXEC_MONITOR_TIMEOUT_MS="${NANOCLAW_OMX_EXEC_MONITOR_TIMEOUT_MS:-300000}"

mkdir -p \
  "$NANOCLAW_HOME" \
  "$CODEX_HOME" \
  "$NANOCLAW_OMX_STATE_ROOT" \
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

if { [ "${NANOCLAW_WORKER_BACKEND:-}" = "codex" ] || [ "${NANOCLAW_AZURE_OPENAI_FALLBACK_BACKEND:-}" = "codex" ]; } && [ ! -s "$CODEX_HOME/auth.json" ]; then
  echo "warning: Codex is selected as primary or backup but CODEX_HOME/auth.json is missing" >&2
fi

if [ "${NANOCLAW_CODEX_USAGE_FALLBACK_BACKEND}" = "zai" ] \
  && [ -z "${ZAI_ANTHROPIC_AUTH_TOKEN:-${NANOCLAW_ZAI_ANTHROPIC_AUTH_TOKEN:-${ZAI_API_KEY:-${NANOCLAW_ZAI_API_KEY:-}}}}" ]; then
  echo "warning: Codex usage-limit fallback is set to ZAI but no ZAI token is configured" >&2
fi

AZURE_FALLBACK_BACKEND="$(printf '%s' "${NANOCLAW_CODEX_USAGE_FALLBACK_BACKEND:-}" | tr '[:upper:]' '[:lower:]')"
AZURE_WORKER_BACKEND="$(printf '%s' "${NANOCLAW_WORKER_BACKEND:-}" | tr '[:upper:]' '[:lower:]')"
case "$AZURE_FALLBACK_BACKEND:$AZURE_WORKER_BACKEND" in
  *azure*openai*|*azure-ai*|*azure_ai*|*azure-foundry*|*azure_foundry*|azure:*|*:azure)
    if [ -z "${AZURE_OPENAI_API_KEY:-${NANOCLAW_AZURE_OPENAI_API_KEY:-${AZURE_AI_API_KEY:-${NANOCLAW_AZURE_AI_API_KEY:-}}}}" ]; then
      echo "warning: Azure OpenAI backend is selected but no Azure API key is configured" >&2
    fi
    if [ -z "${AZURE_OPENAI_ENDPOINT:-${NANOCLAW_AZURE_OPENAI_ENDPOINT:-${AZURE_OPENAI_BASE_URL:-${NANOCLAW_AZURE_OPENAI_BASE_URL:-}}}}" ]; then
      echo "warning: Azure OpenAI backend is selected but no Azure endpoint is configured" >&2
    fi
    if [ -z "${AZURE_OPENAI_DEPLOYMENT:-${NANOCLAW_AZURE_OPENAI_DEPLOYMENT:-${AZURE_OPENAI_MODEL:-${NANOCLAW_AZURE_OPENAI_MODEL:-${AZURE_OPENAI_DEPLOYMENT_NAME:-${NANOCLAW_AZURE_OPENAI_DEPLOYMENT_NAME:-}}}}}}" ]; then
      echo "warning: Azure OpenAI backend is selected but no deployment/model is configured" >&2
    fi
    ;;
esac

cd "$NANOCLAW_HOME"
exec /usr/local/bin/nanoclaw gateway serve
