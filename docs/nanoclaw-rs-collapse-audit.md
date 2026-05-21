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

Status: active and guarded.

Already active:

- `Cargo.toml` disables Cargo auto-discovery for bins, tests, examples, and
  benches.
- `src/bin/nanoclaw.rs` is the only active binary target.
- Old root-level Rust files remain parked under
  `graveyard/agency-harness/src-root/`.
- `.github/workflows/rust-nanoclaw.yml` and `make verify` run the active target
  gates.
- `scripts/check-legacy-source-gates.sh` fails if legacy modules, root
  binaries, root tests, or missing parked source paths violate this boundary.

Exit criterion:

- A fresh clone can run all active-target checks without compiling legacy
  Agency bins/tests.

### 2. Classify Agency Capabilities For Adoption

Status: active and reportable.

Already active:

- `docs/agency-capability-adoption-ledger.md` records a decision and descendant
  role for each legacy source area.
- `nanoclaw runtime source-disposition --limit <n>` fingerprints the same
  source areas and reports their active Rust NanoClaw descendant roles without
  moving or deleting source.
- `make verify` and CI run the source-disposition report so drift is visible.

Remaining work:

- Keep the capability ledger updated before moving or deleting old source
  directories.
- Treat holonic and FPF material as candidates for distilled primitives before
  classifying them as graveyard material. Useful concepts may become typed
  lineage, gates, assurance, provenance, planning, or evidence contracts.

Exit criterion:

- No old Agency directory remains at top-level `src/` unless it has a documented
  adoption decision and a descendant role in the Rust NanoClaw architecture.

### 3. Replace Legacy Scripts With NanoClaw Entrypoints

Status: active and guarded.

Already active:

- Keep the root `start_agency.sh` as a guard only; it must not launch old
  `memory_server`, `speaker_server`, `listener_server`, or `nexus_server`
  binaries.
- Keep the original service launchers and old speaker compose stack parked under
  `graveyard/agency-harness/`.
- Park the original ONNX helper under
  `graveyard/agency-harness/scripts/bundle_onnx.sh`.
- Keep `scripts/bundle_onnx.sh` as a guard only; it must not send operators to
  the inactive `proof_of_life` bootstrap path.
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
  startup/preflight failures, gateway/webhook auth posture, scheduled-task
  backlog, and recent execution evidence
- `runtime health --notify-local <chat>` for local operator alerts when health
  is degraded or unhealthy, with `--notify-always` available for explicit
  heartbeat reporting. Startup/preflight failures are included in health, with
  repeated failures promoted from warning to failing status and known
  missing-config failures mapped to deterministic recovery suggestions.
- `runtime repair --plan|--apply` for converting health recovery evidence into
  guarded repair items. Apply mode refuses secret writes, operator config
  choices, legacy-lane enablement, and long-running profile restarts unless a
  deterministic auto-apply handler is registered.
- `runtime cleanup` for report-only stale/invalid PID-file cleanup, with
  mutation gated behind `--apply`
- a typed `runtimeChannels` registry that declares local, scheduler, Slack,
  webhook, OpenClaw gateway, and discontinued legacy PM automation ownership
  from the same configuration model used by status, inspect, and health reports

Remaining work:

- Confirm webhook server, Slack runtime, local runtime, and OpenClaw gateway can
  run from the same NanoClaw configuration model without hidden script state in
  long-lived production profiles.

Exit criterion:

- The Rust runtime can be started, inspected, and stopped through NanoClaw
  commands only.

Current unified entrypoint:

- `nanoclaw runtime status` reports local, Slack, webhook, PM automation, and
  OpenClaw gateway readiness from one configuration model, with a typed
  `runtimeChannels` aggregate for enabled state, auth posture, serve profiles,
  operator visibility, and legacy/discontinued status.
- `nanoclaw runtime inspect` reports runtime counts, task distribution, recent
  tasks, recent execution provenance, and the same runtime-channel registry for
  lifecycle inspection.
- `nanoclaw runtime health --limit <n> [--strict] [--notify-local <chat>]`
  reports operator health checks without invoking model inference or legacy
  supervisor code; it now fails enabled channels with missing auth/config,
  warns on degraded channel posture, and local notifications write to the
  NanoClaw local outbox only when attention is needed unless
  `--notify-always` is set.
- `nanoclaw runtime repair --plan|--apply --limit <n>` converts health
  recovery suggestions into deterministic repair items classified as
  `auto_applyable`, `operator_required`, or `blocked`; unknown startup failures
  become blocked diagnostic items instead of guessed fixes.
- `nanoclaw runtime cleanup [--apply]` reports stale or invalid runtime PID
  files and removes only those files when explicitly applied.
- `nanoclaw runtime poll` runs one local control-plane pump.
- `nanoclaw runtime serve --profile full|gateway|webhook|pm|slack` starts the
  selected runtime profile without legacy Agency startup scripts. Startup now
  preflights the same `runtimeChannels` registry used by status and health, so
  misconfigured channels fail before PID-file creation with
  `runtime_channel_misconfigured` and the exact missing config/auth fields.
  Preflight failures, startup attempts, running transitions, and startup
  failures append to `data/runtime/startup-events.jsonl` and are reported by
  `runtime state` under `runtime.startupEvents`; `runtime health` reads the
  same ledger so local operator notifications include startup failure evidence
  and the first deterministic recovery actions.
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
- Local and Slack scheduled-task paths persist execution provenance, logs, and
  evidence before applying the completion gate, so failed structured evidence is
  not discarded before the task is marked failed.
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
- Unsupported worker backends and unsupported custom execution lanes now return
  failed execution evidence with blockers instead of only returning opaque
  `Err` values.
- Container worker process failures now return `worker_process` shell-mode
  failed evidence with stdout/stderr in the log artifact and a
  `worker_process_error` blocker before completion is rejected.
- Remote-worker project sync, workspace sync, command failure, and invalid
  worker response paths now return `remote_worker_process` shell-mode failed
  evidence with the remote boundary preserved.
- Host worker daemon startup, socket write/shutdown, request timeout, empty
  outcome, and cancellation-before-evidence paths now return structured
  `worker_transport` evidence with `failed`, `timed_out`, or `cancelled`
  status before the run reaches closure validation.
- Successful host/script, backend response, OMX gateway, in-process advisory,
  blocked-execution, and worker-blocked paths now emit explicit verification
  records. The old generic `adapter_status` verification sentinel has been
  removed from the evidence builder, so adapters must provide their own
  verification evidence.
- The `nanoclaw task complete` operator command now requires
  `--manual-override` and records a manual completion override, while
  execution-driven completion remains gated through structured execution
  evidence.
- App-level task status updates now reject raw `Completed` transitions through
  `set_task_status`, so active writeback surfaces that use the app API must use
  execution evidence or an explicit manual completion override.
- The lower-level DB `set_task_status` helper also rejects raw `Completed`
  transitions; durable completion remains available only through run-completion
  update paths that the app gates with execution evidence or manual override.
- `make verify` now runs a task-completion source guard that rejects new
  `set_task_status` callsites outside the narrow app/DB/CLI/port boundary, so
  future writeback surfaces cannot quietly bypass the completion gate.
- Linear is no longer an active issue/writeback surface for this instance.
  Legacy Linear CLI and webhook paths are disabled by default and require
  `NANOCLAW_LINEAR_LEGACY_ENABLED=true` only for controlled migration/reference
  use.
- Future tool/adapter re-entry now has a native Rust contract:
  `ToolAdapterContract` requires request-plane capability coherence, explicit
  host OS control approval, operator-visible evidence, structured artifacts for
  completion-capable adapters, structured verification for every adapter, and
  blockers on failure. Old Agency tool material remains source material until a
  runtime adapter satisfies this contract.
- Active built-in runtime hands are now registered against that contract in
  `src/nanoclaw/tool_registry.rs`: Codex-local, OpenClaw gateway, OMX gateway,
  host shell, HTTP request, Workers AI advisory, and host OS control. The
  registry keeps Workers AI advisory-only and makes host OS control
  explicit-approval-only.
- External tool adapter manifests now load through the same registry path and
  fail closed when a contract is invalid, two external adapters claim the same
  id, or an external adapter tries to reuse a built-in runtime-hand id.
  `tool-adapters.example.json` is a checked-in compatibility fixture for
  plugin directories that will produce `NANOCLAW_TOOL_ADAPTERS_PATH`
  manifests.
- `runtime status`, `runtime inspect`, and `runtime health` expose the tool
  adapter registry and external manifest validation state. The external
  manifest path defaults to `tool-adapters.json` under the project root and can
  be overridden with `NANOCLAW_TOOL_ADAPTERS_PATH`.
- Scheduled script execution now passes through a deterministic command-safety
  gate before `/bin/sh -lc`. The first active slice blocks high-risk destructive
  commands such as broad `rm -rf`, raw device writes, filesystem formatting,
  destructive `diskutil`, host power-control, and recursive permission changes
  against broad targets. Blocked scripts return failed shell evidence with a
  `command_safety_policy` blocker, while scoped cleanup commands remain
  allowed.
- Direct swarm shell lanes now reuse the same command-safety gate before repo
  mirror or Symphony remote commands can run. A blocked remote command emits
  failed shell evidence with a `command_safety_policy` blocker and is marked
  non-retryable instead of creating remote side effects or relying on summary
  prose.
- Execution evidence and runtime artifact bodies now pass through deterministic
  output-safety redaction before persistence. Secret-shaped assignments,
  bearer credentials, provider secret keys, and GitHub tokens are redacted in
  durable evidence/artifact JSON, and matching records receive an
  `output_safety_report` artifact. Raw local log files remain available for
  operator debugging; the redaction boundary is the durable, queryable evidence
  stream.

Remaining work:

- If a new active Nexus/Paperclip/GitHub writeback surface needs to transition
  work states, route it through the app completion APIs instead of adding raw
  status mutation callsites.
- Wire concrete plugin directories to produce manifests compatible with
  `tool-adapters.example.json` when external plugins are introduced.
- Keep Azure, ZAI, Codex, and OpenClaw provider usage inside the existing
  adapter/gateway contracts.
- Extend the same command-safety classifier to any future active shell-capable
  adapter before it is allowed to satisfy completion evidence.

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
  orphan session directories, group roots, recent startup/preflight events, and
  queued task counts without deleting or migrating anything.
- Session state compaction now clean-room adopts the useful part of legacy
  episodic memory: when a compact runtime metadata snapshot is needed, the
  active `SessionState` preserves the original objective, inserts a
  deterministic omitted-turn summary, and keeps recent turns without calling an
  LLM or reintroducing the old memory module dependency graph.
- `nanoclaw runtime state` now reports `stateResidue`, a deterministic
  active-vs-legacy inventory that separates active runtime roots from legacy
  memory/vector/cache candidates such as `.fastembed_cache`, legacy
  `memory.json` stores, old `agency_history.jsonl`, and legacy memory source
  references.
- `nanoclaw runtime cleanup --state-residue` can include the same residue
  inventory beside stale-PID cleanup output, but the inventory is report-only:
  cleanup still deletes only stale or invalid PID files, never runtime state or
  legacy stores.
- The residue inventory now includes explicit `operatorActions` for each legacy
  candidate, classifying the safe default as leave-in-place and separating
  migration candidates, purge candidates, and source references without
  mutating any state.
- `nanoclaw runtime state-action --plan|--apply` now turns that residue
  inventory into guarded apply actions. Apply requires a concrete
  `--confirm <action-id>` from a prior plan, mutates exactly one target, archives
  eligible legacy memory/vector/history residue under
  `data/runtime/state-archive/`, and writes a rollback receipt under
  `data/runtime/state-actions/`.
- The state-action apply registry is intentionally non-destructive in this
  slice: it does not delete caches, mutate source references, or migrate legacy
  stores into active session sidecars without a separate runtime contract.
- `nanoclaw runtime source-disposition --limit <n>` now fingerprints and
  classifies the remaining legacy Agency source directories against their
  active Rust NanoClaw descendant roles. This keeps `src/fpf`,
  `src/orchestrator`, `src/agent`, `src/memory`, `src/tools`, and related
  source material in a deterministic report-only adoption ledger before any
  source move or deletion is considered.

Remaining work:

- Add explicit migration handlers only after a specific legacy store has an
  active runtime contract and replay/rollback path.
- Add guarded source-disposition actions only after a specific parked source
  path has a clean-room adoption contract or graveyard decision.

Exit criterion:

- Operators can tell which state and source material belongs to the active Rust
  runtime and which material is parked legacy residue.

### 7. Rewrite Public Repo Documentation

Status: active and guarded.

Already active:

- Root README is focused on the current Nexus/Rust NanoClaw runtime and points
  old Agency material to `graveyard/agency-harness/`.
- Legacy governance docs under `docs/governance/` now identify themselves as
  parked reference material, not active runtime authority for FPF, World Chain,
  constitutional appeals, or old governance services.
- `Makefile` and the Rust NanoClaw GitHub Actions gate now run
  `nanoclaw runtime state --limit 5` in addition to show-config, status, and
  inspect so state-boundary inventory remains part of active verification.
- `.github/copilot-instructions.md` now points contributors and AI agents at
  NanoClaw runtime status/state/inspect/health/cleanup commands instead of old
  gateway/local entrypoint examples.
- `scripts/check-legacy-source-gates.sh` is now part of `make verify` and the
  Rust NanoClaw GitHub Actions gate. It fails if Cargo auto-discovery is
  re-enabled, legacy Agency modules are exported from `src/lib.rs`, old
  `src/main.rs`/`src/server.rs`/`src/desktop.rs` or root `tests/` reappear, or
  required parked source paths are missing.
- The legacy ONNX bundling helper is parked under
  `graveyard/agency-harness/scripts/bundle_onnx.sh`. The root
  `scripts/bundle_onnx.sh` path is now a guard that refuses the old
  `proof_of_life` bootstrap path and points future model-lane work back through
  typed runtime provider contracts.

Remaining work:

- Continue scanning for newly added docs that describe old FPF/SOTA
  microservice posture as active runtime behavior.

Exit criterion:

- A reader can clone the repo and understand that this is the Rust NanoClaw
  runtime, not the old Agency microservice suite.

## Completed Collapse Milestones

1. Explicit Cargo target graph and collapse audit are merged.
2. Inactive bins, tests, root Rust files, service scripts, Docker artifacts, and
   legacy ONNX helper paths are parked or guarded.
3. README, contributor, and operator docs describe the Rust NanoClaw runtime
   instead of the old Agency microservice suite.
4. The Agency capability adoption ledger and runtime source-disposition report
   classify parked source material before any movement or deletion.
5. Useful Agency concepts now re-enter through active Rust contracts:
   foundation primitives, runtime channels, provider/gateway lanes, execution
   evidence, command safety, output safety, tool adapter contracts, session
   sidecars, state residue reporting, guarded repair, and guarded state actions.
6. The unified `nanoclaw runtime` command owns status, state, source
   disposition, inspect, health, repair, cleanup, poll, serve, stop, reload, and
   state-action flows.
7. Structured execution evidence is the closure gate for active lanes.
8. `make verify` and GitHub Actions now enforce active target, completion,
   source-disposition, and legacy-source gates.

## Remaining Collapse Work

1. Add new migration handlers only when a specific legacy state store has an
   active runtime contract and replay/rollback path.
2. Add source-disposition apply actions only when a specific parked source path
   has a clean-room adoption contract or graveyard decision.
3. Extend command safety to any future active shell-capable adapter before it is
   allowed to satisfy completion evidence.
4. Keep provider use for Azure, ZAI, Codex, OpenClaw, and future providers
   inside the adapter/gateway/runtime contracts.
5. Continue reducing Docker/build context only as parked source directories are
   actually moved out of the active path.

## Non-Goals During Collapse

- Do not bypass OpenClaw/OMX/Nexus provider routing to call Azure, ZAI, Codex,
  or any model directly.
- Do not make local-private operator routes public for convenience.
- Do not delete active runtime state or group memory during source cleanup.
- Do not revive old Agency dependencies just to make old surfaces pass. Re-home
  useful behavior through smaller Rust NanoClaw contracts instead.
