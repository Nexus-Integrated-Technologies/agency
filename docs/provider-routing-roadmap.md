# Provider Routing Roadmap

Audience: operator/admin.

This is the current Nexus Control Plane provider posture for OpenClaw/NanoClaw
execution. Provider calls must continue to flow through the existing
gateway/worker/OMX contracts so execution evidence, run state, and writeback
remain inspectable.

## Current Live Posture

- Primary gateway backend: `azure-openai`
- Azure deployment: currently `nanoclaw-gpt-4-1-mini`; Foundry deployments such
  as `DeepSeek-V3.1`, `DeepSeek-R1-0528`, Mistral, Grok, Llama, or Model Router
  can be selected by changing the deployment/model env without bypassing the
  gateway.
- Azure fallback backend: `codex`
- Codex usage-limit fallback: `azure-openai`, configurable with
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
NANOCLAW_FORCE_WORKER_BACKEND=azure-openai
NANOCLAW_AZURE_OPENAI_FALLBACK_BACKEND=codex
NANOCLAW_CODEX_USAGE_FALLBACK_BACKEND=azure-openai
```

Foundry `AIServices` resources are supported through either endpoint shape:

```text
# Deployment-scoped OpenAI-compatible route, including non-OpenAI Foundry deployments
NANOCLAW_AZURE_OPENAI_ENDPOINT=https://<resource>.services.ai.azure.com
NANOCLAW_AZURE_OPENAI_DEPLOYMENT=<deployment-name>
NANOCLAW_AZURE_OPENAI_API_VERSION=2024-10-21

# Foundry project endpoint copied from the Foundry portal
NANOCLAW_AZURE_AI_FOUNDRY_ENDPOINT=https://<resource>.services.ai.azure.com/api/projects/<project-name>
NANOCLAW_AZURE_AI_FOUNDRY_MODEL=<deployment-name>

# Direct Foundry Models route
NANOCLAW_AZURE_AI_FOUNDRY_ENDPOINT=https://<resource>.services.ai.azure.com/models
NANOCLAW_AZURE_AI_FOUNDRY_MODEL=<deployment-or-model-name>
NANOCLAW_AZURE_AI_FOUNDRY_API_VERSION=2024-05-01-preview
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
      "codexUsageFallbackBackend": "azure-openai"
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

## Cost-Aware Model Tiers

Use Azure Foundry as the primary biller, but do not use one model for every
task:

- Deterministic platform events: no model call.
- Lightweight advisory, routine summaries, and low-risk planning:
  `gpt-4.1-nano`, Phi-4 mini, Mistral Small, or a small DeepSeek distill
  deployment.
- Normal planning and operator analysis: `DeepSeek-V3.1`, `gpt-4.1-mini`, or
  Model Router when its routing/cost behavior is acceptable.
- Hard reasoning, remediation design, or multi-step debugging:
  `DeepSeek-R1-0528`, `MAI-DS-R1`, Grok reasoning, or a current GPT reasoning
  deployment.
- Highest-risk code execution and contract-sensitive remediation:
  reserve GPT Codex/frontier deployments for escalation or fallback, not as the
  default heartbeat lane.

For non-default Foundry deployments, set a rate card so Paperclip can estimate
cost at the control-plane level:

```json
NANOCLAW_AZURE_AI_FOUNDRY_RATE_CARD_JSON={
  "DeepSeek-V3.1": {
    "input_usd_per_1m": 0.27,
    "cached_input_usd_per_1m": 0.07,
    "output_usd_per_1m": 1.10
  }
}
```

Use the actual Azure pricing shown for the deployed model/region. The gateway
uses this only for estimates; Azure Cost Management remains the financial
source of truth.

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
