# NanoClaw Rust Runtime Collapse Audit

Date: 2026-05-20
Branch baseline: `origin/buddha/openclaw-gateway-runtime-auth-ci` after PR 18

This audit starts the collapse from the `agency` workspace toward the Rust
NanoClaw runtime. The intent is not to throw Agency away. Agency is the native
Rust harness. NanoClaw is the claw-shaped third-party reference model. The
target is to adopt the useful Agency harness capabilities into a Rust NanoClaw
runtime through clean-room distillation, not to preserve every old module shape
or delete valuable ideas just because they are currently outside the compile
path.

Use four states while collapsing:

- `active`: compiled, tested, and part of the Rust NanoClaw runtime path.
- `assimilation candidate`: Agency capability that may become a smaller,
  cleaner foundation or NanoClaw runtime primitive.
- `parked`: still useful reference material, but not on the runtime compile
  path.
- `graveyard`: material intentionally decoupled from runtime execution because
  it is governance-only, holonic-only, obsolete, or unsafe to keep as active
  code.

The practical rule: classify first, clean-room useful capability second, and
only then park or graveyard what is not worth adopting.

## Current Runtime Boundary

The active Rust runtime boundary is narrow and concrete:

- `src/lib.rs` exposes only `foundation` and `nanoclaw`.
- `src/bin/nanoclaw.rs` is the explicit binary target.
- old root-level Rust files such as `src/main.rs`, `src/desktop.rs`, and
  `src/server.rs` are parked under `graveyard/agency-harness/src-root/`.
- `src/foundation/` contains the reusable domain layer.
- `src/nanoclaw/` contains the control-plane runtime: DB, queue, scheduler,
  local and Slack runtimes, OpenClaw gateway, OMX/provider routing, group
  runtime config, session sidecars, observability, remote control, and project
  environment handling.
- `Dockerfile.openclaw-gateway` already builds only `--bin nanoclaw`.

The first collapse cut in this pass makes Cargo stop auto-discovering legacy
targets:

- `autobins = false`
- `autotests = false`
- `autoexamples = false`
- `autobenches = false`
- explicit `[[bin]]` target for `nanoclaw`

This keeps the Rust NanoClaw runtime buildable while the legacy source tree is
still being sorted.

The next cut parks inactive bins and tests under `graveyard/agency-harness/`.
They remain available as clean-room reference material, but no longer sit in
Cargo's active discovery paths.

This pass also removed one active-runtime flake found by the stricter gate:
worker daemon sockets now include a hash of the session root instead of using
only the short session ID, so parallel sessions with names like `session-1` do
not collide in the host temp directory.

## Evidence From This Audit

`cargo check --all-targets` failed before the explicit target graph because
Cargo still discovered legacy files under `src/bin/` and `tests/`. The failures
were not in the NanoClaw runtime path. They came from old targets importing
disabled modules and removed dependencies:

- disabled crate modules: `agent`, `orchestrator`, `memory`, `tools`, `fpf`,
  `services`
- removed or intentionally absent deps: `tokio`, `candle_core`, `candle_onnx`,
  `tracing`, `tracing_subscriber`, `pdf_extract`, `futures`, `async_trait`

The repository still contains substantial parked or candidate code:

- active Rust NanoClaw source: `src/nanoclaw/`, about 33k lines
- active foundation source: `src/foundation/`, about 3k lines
- Agency source still on disk under `src/agent`, `src/fpf`, `src/memory`,
  `src/models`, `src/orchestrator`, `src/runtime`, `src/safety`,
  `src/services`, `src/tools`, and `src/utils`, about 25k lines
- inactive bins and integration tests parked under `graveyard/agency-harness/`
- desktop shell under `src-tauri/`
- old service scripts and Docker files parked under `graveyard/agency-harness/`

## What Is Left To Do

### 1. Lock The Build Boundary

Status: started.

Remaining work:

- Keep `cargo check --all-targets` passing with only the explicit NanoClaw
  binary and library targets.
- Keep old root-level Rust files parked under
  `graveyard/agency-harness/src-root/` unless a smaller replacement is
  clean-roomed into the active runtime.
- Keep CI aligned with the same active-target gates in
  `.github/workflows/rust-nanoclaw.yml`:
  - `cargo check --all-targets`
  - `cargo test --all-targets`
  - `cargo run --quiet --bin nanoclaw -- show-config`

Exit criterion:

- A fresh clone can run all active-target checks without compiling legacy
  Agency bins/tests.

### 2. Classify Agency Capabilities For Adoption

Status: started in `docs/agency-capability-adoption-ledger.md`.

Remaining work:

- Keep the capability ledger updated before moving or deleting old source
  directories.
- Decide whether each capability should be:
  - clean-roomed into `src/foundation/` as a domain primitive,
  - clean-roomed into `src/nanoclaw/` as runtime behavior,
  - parked as reference material outside the active compile path,
  - moved to `graveyard/holonic/`,
  - or deleted after a replacement is proven.
- Treat holonic and FPF material as candidates for distilled primitives before
  classifying them as graveyard material. Useful concepts may become typed
  lineage, gates, assurance, provenance, planning, or evidence contracts.
- Start with the largest and least active-target-critical areas:
  - `src/fpf/`
  - `src/orchestrator/`
  - `src/agent/`
  - `src/tools/`
  - `tests/` that target those modules

Exit criterion:

- No old Agency directory remains at top-level `src/` unless it has a documented
  adoption decision and a descendant role in the Rust NanoClaw architecture.

### 3. Replace Legacy Scripts With NanoClaw Entrypoints

Status: started.

Remaining work:

- Keep the root `start_agency.sh` as a guard only; it must not launch old
  `memory_server`, `speaker_server`, `listener_server`, or `nexus_server`
  binaries.
- Keep the original service launchers and old speaker compose stack parked under
  `graveyard/agency-harness/`.
- Replace old service/docker docs with NanoClaw-specific commands where those
  docs still describe the legacy suite as active.
- Keep `docker/start-openclaw-gateway.sh` because it is already part of the
  NanoClaw OpenClaw gateway runtime.
- Keep `Dockerfile.openclaw-gateway` but continue reducing its build context as
  legacy directories are moved out of the active path.

Exit criterion:

- Operator startup instructions mention only NanoClaw/Nexus runtime commands,
  not old Agency microservices.

### 4. Finish Runtime Channel Ownership

Status: active, with a unified CLI facade and deterministic health report in
place.

Already active:

- local inbound/outbound channel
- Slack runtime
- OpenClaw gateway
- scheduled tasks
- gateway/OMX/provider routing
- session sidecars
- runtime PID files under the active `data/runtime/` directory
- `runtime stop` and `runtime reload` operator controls for profiled runtime
  processes
- `runtime health` for deterministic checks over directories, PID file state,
  gateway/webhook auth posture, scheduled-task backlog, and recent execution
  evidence
- `runtime health --notify-local <chat>` for local operator alerts when health
  is degraded or unhealthy, with `--notify-always` available for explicit
  heartbeat reporting
- `runtime cleanup` for report-only stale/invalid PID-file cleanup, with
  mutation gated behind `--apply`

Remaining work:

- Keep refining channel ownership docs as runtime profiles mature.
- Confirm webhook server, Slack runtime, local runtime, and OpenClaw gateway can
  run from the same NanoClaw configuration model without hidden script state.

Exit criterion:

- The Rust runtime can be started, inspected, and stopped through NanoClaw
  commands only.

Current unified entrypoint:

- `nanoclaw runtime status` reports local, Slack, webhook, PM automation, and
  OpenClaw gateway readiness from one configuration model.
- `nanoclaw runtime inspect` reports runtime counts, task distribution, recent
  tasks, and recent execution provenance for lifecycle inspection.
- `nanoclaw runtime health --limit <n> [--strict] [--notify-local <chat>]`
  reports operator health checks without invoking model inference or legacy
  supervisor code; strict mode exits nonzero when the report is unhealthy, and
  local notifications write to the NanoClaw local outbox only when attention is
  needed unless `--notify-always` is set.
- `nanoclaw runtime cleanup [--apply]` reports stale or invalid runtime PID
  files and removes only those files when explicitly applied.
- `nanoclaw runtime poll` runs one local control-plane pump.
- `nanoclaw runtime serve --profile full|gateway|webhook|pm|slack` starts the
  selected runtime profile without legacy Agency startup scripts.
- `nanoclaw runtime stop --profile <profile>` terminates a profiled runtime
  from its NanoClaw PID file.
- `nanoclaw runtime reload --profile <profile>` sends a reload signal to the
  profiled runtime from the same control surface.

### 5. Normalize Execution Evidence Across Lanes

Status: first active Rust envelope landed.

Already active:

- `ExecutionResponse` can carry a typed `ExecutionEvidence` envelope.
- Host/script worker responses emit adapter type, mode, workspace, git state,
  artifacts, verification, blockers, and provenance IDs.
- OMX responses map team artifacts, terminal status, and failure summaries into
  the same envelope.
- OpenClaw GitHub Codespaces handoffs emit gateway-mode evidence while
  preserving the raw log artifact.
- Local and Slack runtimes persist the envelope into `execution_evidence` and
  also emit operator artifacts.
- `nanoclaw runtime inspect` reports recent durable execution evidence beside
  recent provenance.
- Swarm task results carry execution evidence in task metadata instead of
  relying only on summary prose.
- `ExecutionLaneRouter` validates evidence before returning a successful
  response; code, shell, and gateway modes must include artifacts.
- Scheduled task completion now calls a runtime closure gate: an executed task
  can only transition to completed from successful validated execution
  evidence.
- Direct swarm lanes (`repo_mirror` and `symphony`) now emit shell-mode
  execution evidence with command verification, body-backed log artifacts, and
  blockers on command failure instead of completing from remote stdout alone.
- Swarm task completion now runs an evidence contract gate across built-in
  lanes, so a successful agent, Codex, host, repo mirror, or Symphony task must
  carry valid structured execution evidence before it can be marked completed.
- Runtime task errors use a failed-run path, so missing/invalid execution
  evidence no longer reaches completed status through the generic error path.

Remaining work:

- Thread the same closure gate into every issue/writeback surface outside the
  local scheduled-task path.
- Add explicit verification command records instead of the current adapter
  status sentinel.
- Extend failure paths so nonzero shell, timeout, and cancelled runs return
  structured blockers even when the adapter exits with an error.
- Keep Azure, ZAI, Codex, and OpenClaw provider usage inside the existing
  adapter/gateway contracts.

Exit criterion:

- A run can be evaluated from structured evidence without reading prose logs.

### 6. Collapse Storage And State

Status: partially done.

Already active:

- central SQLite state
- per-session sidecar `inbound.db` and `outbound.db`
- group runtime config
- destination projection records
- `nanoclaw runtime state --limit <n>` reports the active central DB, runtime
  roots, local inbox/outbox/processed directories, linked session sidecars,
  orphan session directories, group roots, and queued task counts without
  deleting or migrating anything.

Remaining work:

- Decide which old stores under `store/`, `data/`, `.fastembed_cache/`, and
  legacy memory paths should be ignored, migrated, or deleted.
- Add a migration/cleanup command for stale local state that does not touch
  active production controller data without an explicit operator action.

Exit criterion:

- Operators can tell which state belongs to the active Rust runtime and which
  state is parked legacy residue.

### 7. Rewrite Public Repo Documentation

Status: started.

Remaining work:

- Keep the root README focused on the current Nexus/Rust NanoClaw runtime only.
- Keep the old Agency README body parked as
  `graveyard/agency-harness/LEGACY_README.md`.
- Keep the old `src/bin/README.md` parked with the legacy bins under
  `graveyard/agency-harness/src-bin/`.
- Keep `.github/copilot-instructions.md`, `.github/CONTRIBUTING.md`, the PR
  template, and `Makefile` aligned to the active NanoClaw gates.
- Update docs that still describe old FPF/governance/SOTA microservice posture
  as active runtime behavior.

Exit criterion:

- A reader can clone the repo and understand that this is the Rust NanoClaw
  runtime, not the old Agency microservice suite.

## Recommended Order

1. Merge the explicit Cargo target graph and collapse audit.
2. Park inactive bins/tests so `src/bin/` and `tests/` stop advertising broken
   old targets while preserving useful examples for capability extraction.
3. Rewrite README and startup docs around the NanoClaw runtime; keep legacy
   startup artifacts parked or guarded.
4. Refine the Agency capability adoption ledger, starting with `src/fpf/`,
   `src/orchestrator/`, `src/agent/`, and `src/tools/`.
5. Clean-room useful Agency concepts into `foundation` or `nanoclaw`; move only
   non-adopted holonic/governance material into `graveyard/holonic/`.
6. Expand the unified `nanoclaw runtime` command from status/inspect/poll/serve
   into stop/reload operations and richer health-loop reporting.
7. Persist structured execution evidence and make it the closure gate for every
   lane.
8. Add state-inspection and safe cleanup commands.

## Non-Goals During Collapse

- Do not bypass OpenClaw/OMX/Nexus provider routing to call Azure, ZAI, Codex,
  or any model directly.
- Do not make local-private operator routes public for convenience.
- Do not delete active runtime state or group memory during source cleanup.
- Do not revive old Agency dependencies just to make old surfaces pass. Re-home
  useful behavior through smaller Rust NanoClaw contracts instead.
