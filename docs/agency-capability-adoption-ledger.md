# Agency Capability Adoption Ledger

Date: 2026-05-20

Agency is the Rust harness substrate. NanoClaw is the claw-behavior reference
model. This ledger prevents accidental deletion by making every old Agency
capability pass through an adoption decision before it is parked, graveyarded,
or clean-roomed into the active runtime.

## Decision States

- `active`: already compiled into `src/foundation/` or `src/nanoclaw/`.
- `assimilate-foundation`: extract a smaller domain primitive into
  `src/foundation/`.
- `assimilate-runtime`: extract runtime behavior into `src/nanoclaw/`.
- `park-reference`: keep as reference outside the active compile path.
- `graveyard-holonic`: preserve non-runtime holonic/governance material under
  `graveyard/holonic/`.
- `delete-after-replacement`: remove only after the replacement is active and
  validated.

## Directory Decisions

| Source | Decision | Assimilation target | Why |
| --- | --- | --- | --- |
| `src/fpf/assurance.rs`, `src/fpf/gate.rs`, `src/fpf/evidence.rs`, `src/fpf/evidence_graph.rs`, `src/fpf/provenance.rs` | `assimilate-foundation` | `src/foundation/{assurance,gate,provenance}.rs` and execution evidence records | These already match the Rust NanoClaw need for closure gates, proof, and run evaluation. |
| `src/fpf/task_signature.rs`, `src/fpf/scope.rs`, `src/fpf/capability.rs`, `src/fpf/service.rs` | `assimilate-foundation` | task signatures, request planes, capability manifests, service clauses | These are durable typing primitives, not old UI/runtime machinery. |
| `src/fpf/plan.rs`, `src/fpf/role.rs`, `src/fpf/role_algebra.rs`, `src/fpf/role_state.rs` | `assimilate-foundation` | plan/role/session domain types | Keep the typed planning value but avoid the old module topology. |
| `src/fpf/drr.rs`, `src/fpf/commitment.rs`, `src/fpf/transition.rs` | `assimilate-runtime` | run decision records, audit events, issue/run lifecycle transitions | Useful as operator-visible evidence if distilled into existing DB/run records. |
| `src/fpf/holon.rs`, `src/fpf/mereology.rs`, `src/fpf/aggregation.rs`, `src/fpf/kernel.rs` | `park-reference` then maybe `graveyard-holonic` | none until a concrete runtime contract needs them | Valuable conceptual source, but too broad for the immediate runtime. |
| `src/fpf/cg_*`, `src/fpf/*_cal.rs`, `src/fpf/*_chr.rs`, `src/fpf/mvpk.rs`, `src/fpf/sota_pack.rs`, `src/fpf/tga.rs`, `src/fpf/uts.rs` | `park-reference` | none yet | Research/governance-rich material should not re-enter active code without a narrow runtime use. |
| `src/orchestrator/planner.rs`, `src/orchestrator/router.rs`, `src/orchestrator/objective.rs`, `src/orchestrator/scheduler.rs`, `src/orchestrator/session.rs` | `assimilate-runtime` | `src/nanoclaw/{router,scheduler,runtime}.rs` plus `src/foundation/{planning,session}.rs` | These map directly to active NanoClaw operations. |
| `src/orchestrator/supervisor.rs`, `src/orchestrator/healing.rs`, `src/orchestrator/homeostasis.rs`, `src/orchestrator/sensory.rs` | `assimilate-runtime` | `nanoclaw runtime status|inspect|health|cleanup|poll|serve|stop|reload` plus local health notifications | Keep the harness idea of a supervisor, not the old supervisor type. |
| `src/orchestrator/budget.rs`, `src/orchestrator/optimal_info.rs`, `src/orchestrator/drr.rs`, `src/orchestrator/event_bus.rs` | `assimilate-runtime` | compute budgets, decision logs, event/audit stream | These support trustworthy autonomous execution and cost discipline. |
| `src/orchestrator/a2a.rs`, `src/orchestrator/arti_a2a.rs`, `src/orchestrator/uap_grpc.rs`, `src/orchestrator/sns.rs` | `park-reference` | future inter-agent protocol lane | Useful later, but not on the critical path to Rust NanoClaw production. |
| `src/orchestrator/metabolism.rs`, `src/orchestrator/sovereignty.rs`, `src/orchestrator/vault.rs` | `park-reference` | finance/identity/vault integrations only after explicit product need | Too broad to pull into runtime during collapse. |
| `src/agent/provider.rs`, `src/agent/cache.rs`, `src/agent/types.rs` | `assimilate-runtime` | provider routing, cache policy, execution request metadata | Must flow through the current executor/model-router contract. |
| `src/agent/react.rs`, `src/agent/reflection.rs`, `src/agent/autonomous.rs` | `assimilate-runtime` | execution loop patterns, blocker handling, self-review hooks | Extract behavior into run orchestration and evidence gates; do not revive the old agent object graph. |
| `src/agent/ctm.rs`, `src/agent/background.rs`, `src/agent/nqd.rs` | `park-reference` | future planning/evaluation heuristics | Research value, but not needed for current production collapse. |
| `src/agent/rl.rs`, `src/agent/training.rs` | `park-reference` | none now | Training is outside the runtime control-plane goal. |
| `src/tools/mod.rs`, `src/tools/code_exec.rs`, `src/tools/codebase.rs`, `src/tools/artifact.rs`, `src/tools/system.rs`, `src/tools/task_spawner.rs`, `src/tools/watchdog.rs` | `assimilate-runtime` | typed tool/action contract under `src/nanoclaw/` | These are harness hands; they should become audited runtime adapters. |
| `src/tools/dynamic.rs`, `src/tools/mcp.rs`, `src/tools/skills.rs` | `assimilate-runtime` with approval gates | plugin/tool registry and managed context loading | Useful, but only with operator-visible provenance and approval gates. |
| `src/tools/web_search.rs`, `src/tools/science.rs`, `src/tools/vision.rs`, `src/tools/wallet.rs`, `src/tools/speaker_rs.rs` | `park-reference` | specialist adapters when needed | Not core runtime collapse work. |
| `src/memory/episodic.rs`, `src/memory/history.rs`, `src/memory/compactor.rs` | `assimilate-runtime` | session context, turn history, compaction | Directly supports long-running agent sessions. |
| `src/memory/entry.rs`, `src/memory/indexer.rs` | `assimilate-foundation` or `assimilate-runtime` | context documents, codebase indexing | Useful if converted into deterministic context ingestion. |
| `src/memory/vector.rs`, `src/memory/manager.rs` | `park-reference` | future retrieval lane | Avoid pulling heavy vector dependencies into the core runtime until needed. |
| `src/safety/command.rs`, `src/safety/content_filter.rs`, `src/safety/hardening.rs`, `src/safety/rate_limiter.rs`, `src/safety/assurance.rs` | `assimilate-runtime` | host OS control policy, request-plane gates, execution provenance | These should harden the current runtime paths. |
| `src/utils/truncate.rs`, `src/utils/hardening.rs`, `src/utils/otel.rs` | `assimilate-runtime` | context shaping, process hardening, observability | Narrow utilities can be adopted without keeping the old utility module. |
| `src/utils/toon.rs`, `src/utils/sandbox.rs` | `park-reference` | none yet | Only adopt if a runtime evidence or sandbox contract needs them. |
| `src/runtime/wasm.rs` | `park-reference` | future sandbox/tool lane | Not required for the current host/container/remote-worker lanes. |
| `src/main.rs`, `src/desktop.rs`, `src/server.rs` | `park-reference` | `graveyard/agency-harness/src-root/` | These root files are outside the explicit Cargo target graph. The active CLI lives at `src/bin/nanoclaw.rs`; service/desktop concepts need smaller runtime contracts before re-entry. |
| `src/models/*`, `src/services/{speaker,listener}.rs`, `docker/Dockerfile.speaker`, old compose stack | `park-reference` | future media/audio lane if required | Model/audio services are not part of the NanoClaw control-plane collapse. The old Docker artifacts are parked under `graveyard/agency-harness/docker/`. |
| `src/services/memory.rs`, `src/services/responses.rs` | `park-reference` | future service wrappers | Runtime should stay CLI/local-first until a service wrapper is intentionally reintroduced. |
| `src-tauri/` | `park-reference` | future desktop operator shell | Keep out of the control-plane runtime path for now. |
| inactive `src/bin/*`, `tests/*`, and old service launch scripts | `park-reference` | `graveyard/agency-harness/` | Preserved as source material but removed from active Cargo discovery and root operator paths. |

## First Assimilation Slices

1. Evidence and closure: fold FPF evidence, gate, assurance, provenance, and
   task signature concepts into the active execution evidence contract. The
   first Rust envelope now exists on `ExecutionResponse` and persists to
   `execution_evidence`; the execution router validates the envelope before
   success returns, scheduled-task completion now requires successful validated
   evidence, and runtime errors record failed runs instead of completing through
   the error path. Direct swarm remote lanes now emit command verification,
   artifacts, and blockers, and built-in swarm lanes gate completion on valid
   execution evidence. Unsupported worker backends and custom execution lanes
   now produce failed evidence with blockers, and scheduled-task runtime loops
   persist evidence before rejecting failed completion. Container worker
   process failures now emit `worker_process` evidence with stdout/stderr
   artifacts instead of opaque process errors, and remote-worker sync/command
   failures now emit `remote_worker_process` evidence with remote boundaries.
   Host worker daemon startup, socket write/shutdown, timeout, empty outcome,
   and cancellation-before-evidence paths now emit `worker_transport`
   evidence with the correct failed/timed-out/cancelled status. Manual CLI task
   completion now requires an explicit `--manual-override`, keeping
   execution-driven closure on the structured evidence path. App-level raw
   task status updates now reject `Completed`, forcing writeback callers that
   use the app API through execution evidence or a manual override. The DB
   status helper also rejects raw `Completed` transitions, leaving durable
   completion to run-completion update paths. `make verify` now includes a
   source guard that rejects new `set_task_status` callsites outside the narrow
   app/DB/CLI/port boundary. Linear is parked as a discontinued legacy
   integration and is disabled by default. Remaining work is to extend the same
   contract to future external adapter plugins as they are registered.
2. Runtime supervisor:
   `nanoclaw runtime status|state|inspect|health|cleanup|poll|serve|stop|reload`
   now owns the basic lifecycle surface with NanoClaw PID files, deterministic
   state inventory, health checks, local health notifications, and explicit
   stale-PID cleanup. `runtime state` and `runtime cleanup --state-residue`
   now expose a report-only active-vs-legacy state residue inventory so
   `.fastembed_cache`, legacy memory stores, and old history files can be
   reviewed without deleting active production controller data. The inventory
   now includes explicit `operatorActions` that classify legacy candidates as
   migration, purge-candidate, preserve-reference, or review while keeping the
   safe default as leave-in-place. Remaining work is to connect health alerts to
   remote/operator channels when those channels are configured and add apply
   commands only for migration or purge actions with a runtime contract. A
   typed `runtimeChannels` registry now makes local, scheduler, Slack, webhook,
   OpenClaw gateway, and legacy PM ownership visible in status, inspect, and
   health output instead of leaving those responsibilities as scattered CLI
   branches. `runtime serve` now preflights that registry before startup, so
   channel misconfiguration fails deterministically before PID-file creation.
3. Tool contract: the first native foundation contract now exists as
   `ToolAdapterContract` in `src/foundation/tool_contract.rs`. Useful old
   Agency tools can only re-enter as typed runtime adapters after declaring
   request-plane policy, capability requirements, approval policy, required
   artifacts, required verification, failure blockers, workspace expectations,
   and operator visibility. Host OS control adapters require explicit
   approval, request-plane tools must declare matching capabilities, and
   completion-capable adapters must declare artifacts and verification before
   they can be treated as active runtime hands. The built-in runtime hands are
   now registered in `src/nanoclaw/tool_registry.rs` for Codex-local,
   OpenClaw gateway, OMX gateway, host shell, HTTP request, Workers AI advisory,
   and host OS control lanes, with unit coverage proving the registry satisfies
   the contract. External tool adapter manifests now load through the same
   contract validation path and are rejected if any adapter is invalid, if two
   external adapters claim the same id, or if an external adapter tries to
   reuse a built-in runtime-hand id. A checked-in
   `tool-adapters.example.json` fixture documents and tests the manifest shape.
   `runtime status`, `runtime inspect`, and `runtime health` now report the
   built-in registry and the external `NANOCLAW_TOOL_ADAPTERS_PATH` manifest
   state, so adapter contract drift is visible before a tool can be treated as
   an active runtime hand.
4. Session memory: adopt episodic/history/compaction into session sidecars or
   the central DB without reintroducing heavyweight vector dependencies. The
   first clean-room slice is active: `SessionState::compact_with_summary`
   deterministically preserves the first objective turn, records omitted-turn
   counts and role distribution, and keeps recent turns for compact execution
   metadata. This replaces the useful shape of legacy episodic compaction
   without model calls or the old memory dependency graph.
5. Safety hardening: map command/content/process/rate-limit checks into
   existing host OS control, executor, and request-plane gates. The first
   command-safety slice is active: `src/nanoclaw/command_safety.rs`
   deterministically blocks high-risk destructive shell commands before
   scheduled scripts reach `/bin/sh -lc`, while preserving scoped cleanup such
   as `rm -rf target node_modules`. Blocked scripts emit failed shell evidence
   with a `command_safety_policy` blocker instead of running and then relying on
   prose logs. Direct swarm shell lanes now reuse the same gate for repo mirror
   and Symphony remote commands before remote side effects or completion
   evidence. The first output-safety slice is also active:
   `src/nanoclaw/output_safety.rs` deterministically detects secret-shaped
   stdout/stderr/log content before it enters durable execution evidence or
   runtime artifact bodies. It redacts those persisted bodies and emits an
   `output_safety_report` artifact without blocking the execution or applying
   brittle prompt/content filters.

## Parking Rules

- Parking is reversible and source-preserving.
- Parked code must not be part of Cargo auto-discovery.
- Parked code must not be used as runtime proof.
- Any re-entry from parked code needs a smaller Rust NanoClaw contract,
  focused tests, and evidence in this ledger.
