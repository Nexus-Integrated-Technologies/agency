# NanoClaw Rust Migration

This repository is adopting the existing `agency` harness into a pure-Rust
NanoClaw-shaped runtime. NanoClaw is the claw-behavior reference model; Agency
is the Rust implementation substrate. The migration should clean-room useful
Agency capabilities into the foundation and NanoClaw runtime instead of treating
all old Agency/holonic material as disposable residue.

## Target Shape

NanoClaw parity means the Rust version should converge on these core surfaces:

| NanoClaw TypeScript surface | Rust target |
| --- | --- |
| `src/index.ts` orchestrator | `src/bin/nanoclaw.rs` + `src/nanoclaw/app.rs` |
| `src/db.ts` SQLite state | `src/nanoclaw/db.rs` |
| `src/group-queue.ts` per-group queue | `src/nanoclaw/queue.rs` |
| `src/config.ts` env-driven runtime | `src/nanoclaw/config.rs` |
| `groups/*/CLAUDE.md` memory roots | `groups/` scaffold created by bootstrap |
| container/runtime abstraction | future `src/nanoclaw/runtime/` work |
| channels + schedulers | future `src/nanoclaw/channels/` and scheduler work |

## Phase 1 Delivered Here

- A standalone `nanoclaw` Rust bootstrap binary that does not depend on the
  current `agency` runtime.
- The default runtime entrypoints (`cargo run`, `src/main.rs`) now route to the
  NanoClaw CLI surface instead of the legacy Agency runtime.
- A canonical domain base now lives in `src/foundation/`, with `nanoclaw` as
  the first runtime descendant rather than a separate ontology.
- The DigitalOcean VM dev environment is now modeled as a first-class
  foundation object, and the NanoClaw CLI can describe, sync, and exec against
  it.
- A NanoClaw-shaped SQLite schema with the core tables needed for messages,
  tasks, router state, sessions, and registered groups.
- A Rust queue model that mirrors NanoClaw's per-group pending-message and
  pending-task semantics closely enough to guide subsequent runtime work.
- A Rust scheduled-task surface with persisted task CRUD, due-task selection,
  next-run computation for `once`, `interval`, and `cron`, plus CLI commands for
  `task add`, `task list`, `task due`, `task pause`, `task resume`, `task delete`,
  and `task complete`.
- A first real Rust harness slice for local operation:
  `local send` writes inbound envelopes, `local run` processes them through the
  Rust queue/router/executor path, and `local outbox` reads outbound envelopes.
- Due scheduled tasks now execute through that same runtime path, emit outbound
  messages, persist run logs/results, and can be driven explicitly with
  `task run-due`.
- The runtime now owns a real Rust execution boundary: per-group sessions are
  persisted in SQLite, execution requests flow through Unix-socket IPC, and a
  long-lived Rust worker daemon is reused per group session instead of
  respawning on every turn.
- Scheduled task scripts and local message runs now share that same warmed
  worker session, so the Rust harness can keep per-group execution history and
  workspace state inside the session boundary.
- Execution is now lane-aware: the runtime can route work through the warmed
  host daemon, a container lane, or the DigitalOcean remote-worker lane, with
  CLI overrides for `local run --lane ...` and `task run-due --lane ...`.
- A `graveyard/holonic/` policy and initial relocation of governance-only docs.

## Clean-Room Upstream Baseline

The current clean-room upstream reference is official NanoClaw `v2.0.64`
(`0683c6e`). Keep this as a source-of-truth checkpoint for parity work: inspect
the upstream behavior, port the smallest equivalent Rust contract, and validate
through Rust tests rather than copying TypeScript implementation details.

The first `v2.0.64` slice ported into Rust is destination-aware message routing:

- inbound messages can carry a deterministic source destination when their chat
  maps to a registered group;
- system prompts now describe the available destinations and the required
  `<message to="...">...</message>` response shape;
- local and Slack runtimes split outbound model text into target-specific
  deliveries;
- plain text output still falls back to the current group so existing local
  harness behavior remains compatible.

The second `v2.0.64` slice ported into Rust is DB-backed per-group runtime
configuration:

- registered groups persist a JSON runtime config in SQLite, matching the
  upstream direction of group-specific provider/model behavior without copying
  the TypeScript implementation;
- `nanoclaw group-runtime show|set` gives operators a narrow CLI for provider,
  backend, model, effort, assistant-name, and prompt-window overrides;
- local and Slack execution paths resolve these overrides before building
  prompts, execution env, and backend overrides;
- Azure, ZAI, Codex, Claude, Workers AI, GitHub Copilot, and custom backend
  selection still flow through the existing executor/provider contract.

The remaining `v2.0.64` clean-room parity slices now have Rust equivalents:

- per-session sidecar storage creates upstream-shaped `inbound.db` and
  `outbound.db` files under each execution session while the central SQLite DB
  remains the source of truth for control-plane state;
- session sidecars store `on_wake` rows for fresh sessions and explicit
  operator wake events so missing wake handlers degrade into durable records
  instead of startup failures;
- per-group runtime config now carries container image and CLI-scope overrides
  alongside provider/model/effort, and the container lane resolves the image
  from that config before falling back to the instance default;
- destination projections are refreshed into the session sidecar on each
  runtime session use and invalidated centrally after group registration or
  approval resolution.

## Remaining Upstream Parity Ledger

- Preserve provider routing through the existing OpenClaw/OMX/Nexus contracts;
  Azure can be the active inference lane, but provider calls should not bypass
  the runtime evidence and adapter boundaries.

## Prune Rules

- Classify Agency subsystems as adoption candidates before pruning them. Useful
  harness concepts should be distilled into smaller `foundation` or `nanoclaw`
  contracts.
- If a subsystem is holonic, FPF-specific, or governance-specific and no longer
  needed for NanoClaw parity, move it into `graveyard/holonic/`.
- If a subsystem is merely out of scope but not holonic, evaluate it
  separately; do not assume it belongs in the graveyard.
- Prefer adding the Rust NanoClaw replacement before deleting or detaching the
  legacy path it supersedes.

## Near-Term Next Cuts

- The current collapse audit and ordered backlog live in
  `docs/nanoclaw-rs-collapse-audit.md`.
- Build an Agency capability adoption ledger before moving large directories.
- Strip `src/fpf/` and governance-dependent orchestration modules after their
  useful primitives are clean-roomed or their remaining call sites are removed
  or replaced.
- Collapse the workspace toward the small, operator-oriented NanoClaw surface:
  orchestrator, queue, DB, runtime, scheduler, channels, and group memory.
- Extend the subprocess executor into stronger isolation modes, especially
  warmed containerized and remote-worker execution, instead of relying on
  one-shot transport wrappers around the worker stdio path.
- Add non-local channels and outbound adapters so the Rust runtime can take
  over from the TypeScript harness rather than just mirroring its shape.
