# Provider Routing Roadmap

Audience: operator/admin.

This is the current Nexus Control Plane provider posture for OpenClaw/NanoClaw
execution. Provider calls must continue to flow through the existing
gateway/worker/OMX contracts so execution evidence, run state, and writeback
remain inspectable.

## Current Live Posture

- Primary gateway backend: `azure-openai`
- Azure deployment: `nanoclaw-gpt-4-1-mini`
- Azure fallback backend: `codex`
- Codex usage-limit fallback: `zai`, configurable with
  `NANOCLAW_CODEX_USAGE_FALLBACK_BACKEND`
- Workers AI: not a default execution fallback
- ZAI: available as an explicit backend or fallback when configured, but not a
  side-channel around OMX/OpenClaw

The live paid smoke from the Render gateway container returned:

```json
{
  "error": null,
  "text": "azure-paid-smoke-ok",
  "backend": "azure-openai",
  "provider": "azure_openai",
  "biller": "azure",
  "billing_type": "azure_credits",
  "model": "nanoclaw-gpt-4-1-mini",
  "usage": {
    "input_tokens": 364,
    "cached_input_tokens": 0,
    "output_tokens": 6
  }
}
```

## Routing Controls

Container defaults still set the normal production posture:

```text
NANOCLAW_WORKER_BACKEND=azure-openai
NANOCLAW_AZURE_OPENAI_FALLBACK_BACKEND=codex
NANOCLAW_CODEX_USAGE_FALLBACK_BACKEND=zai
```

Per-run gateway hints can override provider routing without changing the
container:

```json
{
  "paperclip": {
    "gateway": {
      "workerBackend": "azure-openai",
      "model": "nanoclaw-gpt-4-1-mini",
      "azureFallbackBackend": "codex",
      "codexUsageFallbackBackend": "zai"
    }
  }
}
```

Supported `workerBackend` values include:

- `azure-openai`
- `codex`
- `zai`
- `github-copilot`
- `workers-ai`

Supported Azure fallback values now include:

- `codex`
- `zai`
- `workers-ai`
- `disabled`

Use `workers-ai` only when the Workers AI policy and spend brakes allow it.

## Roadmap

1. Keep Azure as the primary paid inference path for gateway execution.
2. Keep Codex as the backup execution path when Azure is unavailable.
3. Use ZAI as an explicit fallback or role-specific backend through gateway
   hints, not as an ad hoc provider call.
4. Keep Workers AI out of automatic execution until budget controls are proven
   under live load.
5. Promote provider outcomes into normalized execution evidence so the control
   plane can distinguish actual execution, advisory text, blocked work, and
   fallback behavior.
6. Add operator dashboards/reports from existing run metadata:
   `backend`, `provider`, `biller`, `billing_type`, `model`, `usage`, and
   `fallback_reason`.

## Smoke Command

From a container or host that already has Azure env vars configured:

```bash
python3 scripts/openclaw_azure_paid_smoke.py
```

The command invokes `nanoclaw exec-worker-stdio` with
`backend_override=AzureOpenAI` and prints only sanitized metadata. It performs a
real paid provider call.
