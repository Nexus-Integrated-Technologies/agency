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
- `src/main.rs` is no longer needed as a Cargo-discovered binary once the target
  graph is explicit.
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
- Decide whether `src/main.rs` should be deleted or kept only as a thin parked
  reference. The runtime should not depend on it once `nanoclaw` is the explicit
  binary.
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

Status: partially done.

Already active:

- local inbound/outbound channel
- Slack runtime
- OpenClaw gateway
- scheduled tasks
- gateway/OMX/provider routing
- session sidecars

Remaining work:

- Make channel ownership explicit in docs and CLI help.
- Add a single runtime command that can run the selected production channel set
  without requiring old scripts.
- Confirm webhook server, Slack runtime, local runtime, and OpenClaw gateway can
  run from the same NanoClaw configuration model without hidden script state.

Exit criterion:

- The Rust runtime can be started, inspected, and stopped through NanoClaw
  commands only.

### 5. Normalize Execution Evidence Across Lanes

Status: partially done in surrounding control-plane work, not complete here.

Remaining work:

- Ensure host, container, remote-worker, OMX, and gateway runs all produce the
  same structured evidence envelope.
- Promote provider outcome fields from routing docs into concrete runtime
  records:
  - backend
  - provider
  - model
  - billing route
  - usage
  - artifacts
  - verification
  - blockers
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

Remaining work:

- Decide which old stores under `store/`, `data/`, `.fastembed_cache/`, and
  legacy memory paths should be ignored, migrated, or deleted.
- Add a state-inspection command that reports the active central DB, session
  sidecars, group roots, outbox, and queued tasks.
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
6. Add a unified `nanoclaw runtime` command for local, Slack, webhook, and
   OpenClaw operation.
7. Add structured execution evidence as the closure gate for every lane.
8. Add state-inspection and safe cleanup commands.

## Non-Goals During Collapse

- Do not bypass OpenClaw/OMX/Nexus provider routing to call Azure, ZAI, Codex,
  or any model directly.
- Do not make local-private operator routes public for convenience.
- Do not delete active runtime state or group memory during source cleanup.
- Do not revive old Agency dependencies just to make old surfaces pass. Re-home
  useful behavior through smaller Rust NanoClaw contracts instead.
