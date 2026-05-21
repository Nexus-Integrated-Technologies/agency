# Nexus Rust NanoClaw Runtime

This repository is the Rust runtime substrate for the Nexus Control Plane. It
is being collapsed from the older Agency harness into a smaller NanoClaw-shaped
runtime while preserving useful Agency ideas through clean-room adoption.

The current goal is not to make Agency disappear. Agency is the native Rust
harness lineage. NanoClaw is the claw-behavior reference model. The active
runtime should keep the useful harness capabilities, but only after they are
distilled into clear Rust NanoClaw contracts.

## Active Boundary

- `src/foundation/` contains reusable domain primitives.
- `src/nanoclaw/` contains the active runtime implementation.
- `src/bin/nanoclaw.rs` is the only active binary target.
- `Cargo.toml` disables Cargo auto-discovery for legacy bins, tests, examples,
  and benches.
- `make verify` runs `scripts/check-legacy-source-gates.sh`, which fails if old
  Agency modules, tests, or parked root binaries leak back into the active Cargo
  surface.
- `graveyard/agency-harness/` preserves old Agency binaries and integration
  tests as reference material outside the active compile path.
- Legacy root helpers such as `start_agency.sh` and `scripts/bundle_onnx.sh`
  are compatibility guards only; they do not launch old Agency services or
  inactive binaries.

The old distributed Agency microservice suite is not the current operator
entrypoint. It remains source material for adoption decisions, not proof of the
runtime.

## Runtime Responsibilities

The active Rust runtime is responsible for:

- local control-plane operation,
- scheduled task execution,
- Slack and webhook runtime channels,
- OpenClaw gateway execution,
- OMX/provider routing,
- group runtime configuration,
- session sidecars and run evidence,
- destination projections,
- and operator-visible runtime state.

Provider/model access must pass through the existing adapter, gateway, and
runtime contracts. Do not bypass those paths for Azure, ZAI, Codex, OpenClaw,
or any other provider except for an explicit one-off diagnostic.

## Build And Check

```bash
cargo check --all-targets
cargo test --all-targets
cargo run --quiet --bin nanoclaw -- show-config
```

Expected active Cargo targets:

- library: `rust_agency`
- binary: `nanoclaw`
- build script: `build-script-build`

## Running The CLI

Show the resolved runtime configuration:

```bash
cargo run --quiet --bin nanoclaw -- show-config
```

Inspect the active runtime channel set:

```bash
cargo run --quiet --bin nanoclaw -- runtime status
```

The status report includes a typed `runtimeChannels` registry covering local,
scheduler, Slack, webhook, OpenClaw gateway, and discontinued legacy PM
automation ownership. It also includes runtime PID files under `data/runtime/`,
so local production profiles can be inspected and controlled without the old
Agency service scripts. Startup and preflight events are appended to
`data/runtime/startup-events.jsonl` for durable operator evidence.

Inspect runtime state, recent tasks, and recent execution provenance:

```bash
cargo run --quiet --bin nanoclaw -- runtime inspect --limit 5
```

Inventory the active runtime state roots without deleting or migrating anything:

```bash
cargo run --quiet --bin nanoclaw -- runtime state --limit 5
```

That report includes `stateResidue`, which separates active runtime roots from
legacy memory/vector/cache candidates and an `operatorActions` plan for each
legacy candidate. To include the same report beside stale-PID cleanup output
without deleting state:

```bash
cargo run --quiet --bin nanoclaw -- runtime cleanup --state-residue
```

Plan or apply guarded state-residue actions:

```bash
cargo run --quiet --bin nanoclaw -- runtime state-action --plan --limit 10
cargo run --quiet --bin nanoclaw -- runtime state-action --apply \
  --confirm state-residue:agency_history_jsonl:archive_legacy_state
```

`runtime state-action --apply` mutates exactly one confirmed action id from a
prior plan. The current apply registry is non-destructive: it can archive
legacy memory/vector/history residue into `data/runtime/state-archive/` and
writes a receipt under `data/runtime/state-actions/`. It does not delete source
references, purge caches destructively, or migrate legacy state without a
separate runtime contract.

Classify remaining legacy Agency source material against its active Rust
NanoClaw descendant surface:

```bash
cargo run --quiet --bin nanoclaw -- runtime source-disposition --limit 10
```

This report is deterministic and report-only. It fingerprints parked source
directories such as `src/fpf`, `src/orchestrator`, `src/agent`, `src/memory`,
and `src/tools`, then records whether each path should be distilled further,
left parked, or held for a future runtime contract.

Get a deterministic health report over runtime directories, PID files,
startup/preflight failures, runtime-channel ownership, gateway/webhook auth
posture, task backlog, recent execution evidence, and known recovery
suggestions for missing runtime config:

```bash
cargo run --quiet --bin nanoclaw -- runtime health --limit 5
cargo run --quiet --bin nanoclaw -- runtime health --strict
cargo run --quiet --bin nanoclaw -- runtime health --notify-local ops
```

Convert health recovery evidence into a guarded repair plan, or attempt only
safe deterministic repairs:

```bash
cargo run --quiet --bin nanoclaw -- runtime repair --plan --limit 5
cargo run --quiet --bin nanoclaw -- runtime repair --apply --limit 5
```

`runtime repair --apply` will not write secrets, choose ports, enable legacy
lanes, or restart long-running profiles automatically. Those remain explicit
operator actions.

Report stale or invalid runtime PID files, then remove them only when explicitly
applied:

```bash
cargo run --quiet --bin nanoclaw -- runtime cleanup
cargo run --quiet --bin nanoclaw -- runtime cleanup --apply
```

Execution lanes now return and persist a structured `ExecutionEvidence`
envelope alongside their operator-facing text. The envelope records adapter
type, execution mode, workspace, git state, artifacts, verification, blockers,
and provenance IDs so run evaluation can use typed evidence instead of summary
prose.

Poll the local control-plane runtime once:

```bash
cargo run --quiet --bin nanoclaw -- runtime poll
```

Serve a selected production profile through the NanoClaw entrypoint:

```bash
cargo run --quiet --bin nanoclaw -- runtime serve --profile full
cargo run --quiet --bin nanoclaw -- runtime serve --profile gateway
```

Startup uses the same `runtimeChannels` registry reported by status and health.
Misconfigured profile channels fail before PID-file creation with
`runtime_channel_misconfigured` and the missing config/auth fields. Those
preflight failures, startup attempts, running transitions, and startup failures
are recorded in `runtime state` under `runtime.startupEvents`. `runtime health`
derives deterministic recovery suggestions from the same ledger, keeping
secret-bearing fixes operator-gated instead of auto-applying them.

Stop or signal a running profile through the NanoClaw entrypoint:

```bash
cargo run --quiet --bin nanoclaw -- runtime stop --profile full
cargo run --quiet --bin nanoclaw -- runtime reload --profile full
```

List available commands:

```bash
cargo run --quiet --bin nanoclaw -- --help
```

Run a local control-plane command only after confirming the required operator
configuration and authentication boundaries for the target environment.

## Collapse Workflow

The collapse is source-preserving and evidence-driven:

1. Keep the active target graph narrow.
2. Classify old Agency capabilities before moving or deleting them.
3. Clean-room useful concepts into `src/foundation/` or `src/nanoclaw/`.
4. Park reference material outside Cargo discovery paths.
5. Delete only after an active replacement exists and passes focused checks.

Current tracking documents:

- `docs/nanoclaw-rs-migration.md`
- `docs/nanoclaw-rs-collapse-audit.md`
- `docs/agency-capability-adoption-ledger.md`

## Parked Agency Harness

The legacy README and old service/test targets are preserved under:

```text
graveyard/agency-harness/
```

Treat that directory as clean-room reference material. It is not part of the
active runtime proof, CI target set, or operator startup path.

## Change Discipline

Prefer small changes with clear validation evidence. When adopting old Agency
logic, extract the invariant or runtime contract first; do not revive broad old
module graphs just to make legacy surfaces compile.
