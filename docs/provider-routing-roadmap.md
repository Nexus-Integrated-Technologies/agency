# Provider Routing Roadmap

Audience: operator/admin.

This is the current Nexus Control Plane provider posture for OpenClaw/NanoClaw
execution. Provider calls must continue to flow through the existing
gateway/worker/OMX contracts so execution evidence, run state, and writeback
remain inspectable.

## Current Live Posture

- Primary gateway backend: `azure-openai`
- Azure deployment: currently `nanoclaw-gpt-4-1-mini`; Foundry deployments such
  as `service01-foundry-tiny`, `service01-foundry-planner`,
  `service01-foundry-reasoner`, and related slots can be selected by changing
  the deployment/model env without bypassing the gateway.
- Azure AI Foundry MaaS backend: `foundry-maas`, used for marketplace/provider
  models such as DeepSeek, Kimi, Mistral, Grok, Llama, or Model Router when
  those are exposed through the Azure AI Model Inference/OpenAI-compatible
  Foundry endpoint rather than as Azure OpenAI deployments.
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

Azure OpenAI-compatible deployments are supported through the `azure-openai`
backend:

```text
# Deployment-scoped Azure OpenAI route
NANOCLAW_AZURE_OPENAI_ENDPOINT=https://<resource>.services.ai.azure.com
NANOCLAW_AZURE_OPENAI_DEPLOYMENT=<deployment-name>
NANOCLAW_AZURE_OPENAI_API_VERSION=2024-10-21
```

Foundry MaaS/provider models are supported through the dedicated `foundry-maas`
backend:

```text
# Foundry project endpoint copied from the Foundry portal
NANOCLAW_AZURE_AI_FOUNDRY_ENDPOINT=https://<resource>.services.ai.azure.com/api/projects/<project-name>
NANOCLAW_AZURE_AI_FOUNDRY_MODEL=<deployment-name>

# Direct Foundry Models route
NANOCLAW_AZURE_AI_FOUNDRY_ENDPOINT=https://<resource>.services.ai.azure.com/models
NANOCLAW_AZURE_AI_FOUNDRY_MODEL=<deployment-or-model-name>
NANOCLAW_AZURE_AI_FOUNDRY_API_VERSION=2024-05-01-preview

# Equivalent explicit model-inference aliases
NANOCLAW_AZURE_MODEL_INFERENCE_ENDPOINT=https://<resource>.services.ai.azure.com/models
NANOCLAW_AZURE_MODEL_INFERENCE_MODEL=<deployment-or-model-name>
NANOCLAW_AZURE_MODEL_INFERENCE_API_VERSION=2024-05-01-preview
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
- `foundry-maas`
- `codex`
- `zai`
- `github-copilot`
- `workers-ai`

Supported Azure fallback values now include:

- `codex`
- `foundry-maas`
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

For `foundry-maas`, per-model rate cards are preferred because marketplace
model pricing varies materially by provider:

```text
NANOCLAW_MODEL_ROUTE_PLANNER_BACKEND=foundry-maas
NANOCLAW_MODEL_ROUTE_PLANNER_MODEL=DeepSeek-V3.2
NANOCLAW_AZURE_AI_FOUNDRY_ENDPOINT=https://<resource>.services.ai.azure.com/models
NANOCLAW_AZURE_AI_FOUNDRY_API_VERSION=2024-05-01-preview
NANOCLAW_AZURE_AI_FOUNDRY_RATE_CARD_JSON={"DeepSeek-V3.2":{"input_usd_per_1m":0.27,"cached_input_usd_per_1m":0.07,"output_usd_per_1m":1.10}}
```

## Service01 Model Matrix

This matrix is the intended service01 routing posture. It keeps inference
cheap by default, escalates only from evidence, and keeps every provider call
inside the OpenClaw/OMX/Nexus run contract.

Matrix rules:

- Prefer deterministic logic over inference when a rule, fingerprint, cache
  hit, budget gate, queue state, or run-state transition is enough.
- Every paid call must emit run id, agent, task kind, backend, provider,
  biller, model/deployment, usage, estimated cost, and fallback reason when
  present.
- Azure Foundry is the primary biller for inference while credits are active.
- Codex remains the backup for code-capable execution and local tool use.
- ZAI may be used as an explicit role fallback through gateway hints, not as a
  side channel around OpenClaw or OMX.
- Workers AI is not part of automatic execution unless its own policy is
  explicitly re-enabled.

### Deployment Slots

Create deployment names as stable slots. The deployment can point to a newer
model later without changing Paperclip issue/routine policy.

| Slot | Default candidate | Purpose | Cost posture |
| --- | --- | --- | --- |
| `service01-zero-call` | no model | Rules, dedupe, budget checks, cache hits, known-remediation matching | free |
| `service01-foundry-tiny` | Phi-4 mini, Mistral Small, GPT-4.1 nano, or small DeepSeek distill | short summaries, labels, routing explanations, low-risk advisory | cheapest paid lane |
| `service01-foundry-planner` | DeepSeek-V3.1 or DeepSeek-V3.2 after price smoke | normal planning, operator analysis, issue shaping, medium synthesis | default paid lane |
| `service01-foundry-reasoner` | DeepSeek-R1-0528 or MAI-DS-R1 | hard reasoning, root-cause design, ambiguous remediation strategy | capped escalation |
| `service01-foundry-coder` | current Codex/frontier coding deployment if available in Azure, otherwise Codex backend | contract-sensitive code reasoning before tool execution | escalation only |
| `service01-foundry-reviewer` | planner or reasoner slot depending on risk | PR/rubric review, evidence checking, security-sensitive review | bounded by review cap |
| `service01-foundry-vision` | GPT-4o-mini vision, Llama vision, or equivalent deployed vision model | screenshots, UI diffs, visual QA | explicit use only |
| `service01-foundry-safety` | deterministic policy first, Llama Guard or Azure safety model only when needed | safety/classification guardrail | blocked from code completion |

Do not point routine heartbeats, retry wakes, incident dedupe, or known
remediation matching at a paid model. Those use `service01-zero-call`.

### Agent Role Matrix

| Role | Default lane | Primary model slot | Escalation slot | Execution policy |
| --- | --- | --- | --- | --- |
| CEO/default executive | OMX/OpenClaw, advisory unless issue requires action | `service01-foundry-planner` | `service01-foundry-reasoner` | Does not mutate repos by default; can assign, prioritize, and ask for evidence. |
| CTO | OMX/OpenClaw code-capable | `service01-foundry-planner` | `service01-foundry-coder` then Codex backend | May execute code only through gateway/OMX and must return execution evidence. |
| COO | OMX/OpenClaw operations | `service01-foundry-tiny` or planner | `service01-foundry-reasoner` | Backlog, queue, scheduling, and run cleanup; no repo mutation unless assigned. |
| CFO | deterministic ledger first | `service01-foundry-tiny` | `service01-foundry-reasoner` | Cost reports, anomaly triage, budget gates; model calls require spend context. |
| CMO/content | local content-engine path first | `service01-foundry-tiny` | planner | Copy/content strategy only; content-engine mechanics remain deterministic/local. |
| QA/verifier | tests and artifacts first | `service01-foundry-tiny` | reviewer | Cannot mark code work complete from prose; needs structured verification. |
| Security/compliance | scanner/policy first | reviewer | reasoner or frontier coding slot | Human/operator gate for high-impact remediation or secret-sensitive action. |
| Researcher | browser/source evidence first | planner | reasoner | Must cite source/evidence; use model for synthesis, not unsupported facts. |
| Unknown/new agent | deterministic no-op or tiny | `service01-foundry-tiny` | CTO handoff | No code mutation until role policy is explicit. |

### Task Matrix

| Task kind | First path | Paid model slot | Escalation condition | Completion evidence |
| --- | --- | --- | --- | --- |
| `heartbeat_timer` with no issue/context | deterministic no-op/advisory | none | never | run status only |
| retry wake with no new context | deterministic retry guard | none | repeated failure threshold creates/updates issue | blocker or dedupe record |
| platform incident with known fingerprint | remediation registry | none | repeat threshold or critical severity wakes CTO | incident issue/comment |
| platform incident unknown | deterministic issue creation | tiny only for operator summary when enabled | critical or repeated unknown fingerprint | incident issue plus raw record |
| budget/cost anomaly | ledger query and caps | tiny | unexplained anomaly or threshold breach | ledger rows and cap decision |
| auth/secret failure | deterministic env/health checks | none initially | missing route after verified env path | blocker with missing key name redacted |
| code bug/remediation | OpenClaw/OMX workspace execution | planner for plan, coder/Codex for implementation | tests fail, contract unclear, or high-risk code | changed files, commits, tests, artifacts |
| PR review | diff/tests first | reviewer | security/high-risk or ambiguous evidence | findings and test refs |
| docs/operator docs | repo context first | tiny or planner | architectural ambiguity | changed docs and source refs |
| buyer/client copy | audience policy first | tiny or planner | high-value brand/positioning work | final copy only, no operator mechanics |
| research/current facts | source retrieval first | planner | conflicting sources or high-stakes conclusion | cited sources |
| content-engine render/intake | local manifest/QA first | none for mechanics | caption/summary generation only | manifest, sidecars, render QA |
| UI visual QA | browser/screenshot first | vision slot | visual ambiguity after deterministic checks | screenshot/diff artifact |

### Scale Matrix

| Scale class | Examples | Allowed slot | Notes |
| --- | --- | --- | --- |
| Logic | dedupe, caps, known incident, run state, route health | `service01-zero-call` | default whenever deterministic state is enough |
| Tiny | short summaries, label normalization, operator note rewrite | `service01-foundry-tiny` | cache aggressively, low max tokens |
| Standard | planning, medium synthesis, issue decomposition | `service01-foundry-planner` | default paid model for useful work |
| Heavy | hard debugging, multi-file remediation strategy, security reasoning | `service01-foundry-reasoner` | requires issue context and budget headroom |
| Execution | shell/code mutation, tests, commits, PR work | OpenClaw/OMX plus coder/Codex fallback | model output alone cannot complete the issue |

### Fallback Matrix

| Failure or limit | Next step | Hard stop |
| --- | --- | --- |
| deterministic route succeeds | do not call a model | any model call is a bug |
| tiny slot unavailable | planner slot if task is still worth paid inference | otherwise create blocker |
| planner slot unavailable | reasoner only for critical/repeated/high-value work | otherwise Codex/OpenClaw remains idle |
| reasoner slot unavailable | Codex or ZAI through gateway hints for execution-compatible tasks | no provider side channel |
| Codex usage limit | `NANOCLAW_CODEX_USAGE_FALLBACK_BACKEND`, preferably Azure while credits are active | missing fallback credentials fail explicitly |
| Azure key/quota failure | Codex backend for code-capable work, ZAI if configured for that role | Workers AI is not implicit fallback |
| repeated provider timeout | create/update platform incident and pause duplicate starts | do not fan out new swarms |
| budget cap reached | block paid inference and write cost blocker | no override without operator action |

### Initial Config Shape

The current gateway already supports per-run `paperclip.gateway` hints. The
next config-backed step is to store this matrix as operator policy and compile
it into those hints.

```json
{
  "defaultBiller": "azure",
  "defaultBackend": "azure-openai",
  "codexUsageFallbackBackend": "azure-openai",
  "workersAiAutomaticFallback": false,
  "slots": {
    "zero": { "mode": "deterministic" },
    "tiny": { "backend": "azure-openai", "deployment": "service01-foundry-tiny" },
    "planner": { "backend": "azure-openai", "deployment": "service01-foundry-planner" },
    "reasoner": { "backend": "azure-openai", "deployment": "service01-foundry-reasoner" },
    "coder": { "backend": "codex", "azureDeployment": "service01-foundry-coder" },
    "reviewer": { "backend": "azure-openai", "deployment": "service01-foundry-reviewer" },
    "vision": { "backend": "azure-openai", "deployment": "service01-foundry-vision" },
    "safety": { "backend": "azure-openai", "deployment": "service01-foundry-safety" }
  },
  "roles": {
    "CEO": { "defaultSlot": "planner", "maxSlot": "reasoner", "canMutateCode": false },
    "CTO": { "defaultSlot": "planner", "maxSlot": "coder", "canMutateCode": true },
    "COO": { "defaultSlot": "tiny", "maxSlot": "reasoner", "canMutateCode": false },
    "CFO": { "defaultSlot": "tiny", "maxSlot": "reasoner", "canMutateCode": false },
    "CMO": { "defaultSlot": "tiny", "maxSlot": "planner", "canMutateCode": false },
    "QA": { "defaultSlot": "tiny", "maxSlot": "reviewer", "canMutateCode": false },
    "Security": { "defaultSlot": "reviewer", "maxSlot": "reasoner", "canMutateCode": false },
    "Researcher": { "defaultSlot": "planner", "maxSlot": "reasoner", "canMutateCode": false }
  }
}
```

Runtime enforcement now derives a route for each gateway run when Paperclip did
not send a more specific gateway hint. Explicit per-run gateway hints still win,
except zero-call contextless wakes still resolve to the summary backend to avoid
accidental paid inference:

- `heartbeat_timer`, `interval_elapsed`, and contextless `retry_failed_run`
  resolve to the zero-call summary backend even if a paid backend is configured
  as the default.
- CTO repo/code work resolves to the coder slot, currently `codex` by default,
  with `NANOCLAW_CODEX_USAGE_FALLBACK_BACKEND=azure-openai` so quota/usage-limit
  fallback stays inside the gateway contract.
- CFO/cost work resolves to the tiny Azure slot.
- Standard non-code planning resolves to the planner Azure slot.
- Heavy reasoning resolves to the reasoner Azure slot.
- Visual work resolves to the vision Azure slot.
- Safety/classification work resolves to the safety Azure slot.

Slot-specific deployments are optional and override the generic Azure
deployment only when configured:

```text
NANOCLAW_MODEL_ROUTE_TINY_DEPLOYMENT=service01-foundry-tiny
NANOCLAW_MODEL_ROUTE_PLANNER_DEPLOYMENT=service01-foundry-planner
NANOCLAW_MODEL_ROUTE_REASONER_DEPLOYMENT=service01-foundry-reasoner
NANOCLAW_MODEL_ROUTE_CODER_BACKEND=codex
NANOCLAW_MODEL_ROUTE_CODER_DEPLOYMENT=service01-foundry-coder
NANOCLAW_MODEL_ROUTE_REVIEWER_DEPLOYMENT=service01-foundry-reviewer
NANOCLAW_MODEL_ROUTE_VISION_DEPLOYMENT=service01-foundry-vision
NANOCLAW_MODEL_ROUTE_SAFETY_DEPLOYMENT=service01-foundry-safety
```

If the slot deployment is unset, the gateway keeps the existing generic Azure
deployment/model env. That prevents a failed rollout while Foundry deployments
are being created.

Each routed run receives operator-visible env metadata:

```text
NANOCLAW_MODEL_ROUTE_SLOT
NANOCLAW_MODEL_ROUTE_ROLE
NANOCLAW_MODEL_ROUTE_TASK_KIND
NANOCLAW_MODEL_ROUTE_SCALE_CLASS
NANOCLAW_MODEL_ROUTE_REASON
```

### Budget Defaults

- Company daily paid-inference cap: `$5` until the model mix is proven.
- Agent daily cap: `$2` for non-CTO roles; CTO can use the company cap when a
  critical remediation is active.
- Contextless timers and retries: `$0`.
- Unknown incidents: first occurrence creates/updates an issue without paid
  inference unless severity is critical.
- Any run estimated above the configured per-run cap must either downshift the
  slot, request approval, or stop with a cost blocker.

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

For a Foundry MaaS/provider-model smoke, configure
`NANOCLAW_AZURE_AI_FOUNDRY_ENDPOINT`, `NANOCLAW_AZURE_AI_FOUNDRY_MODEL`, and the
matching key, then run:

```bash
python3 scripts/openclaw_foundry_maas_paid_smoke.py
```

That command invokes `nanoclaw exec-worker-stdio` with
`backend_override=FoundryMaaS` and prints only sanitized metadata. It also
performs a real paid provider call, so do not run it from timers or automated
heartbeats.
