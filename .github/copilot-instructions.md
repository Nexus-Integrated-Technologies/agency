# GitHub Copilot instructions for contributors and AI agents

## Current Runtime Boundary

This repository is being collapsed from the older Agency harness into the Nexus
Rust NanoClaw runtime.

- Agency is the native Rust harness lineage.
- NanoClaw is the claw-behavior reference model.
- The active runtime lives in `src/foundation/`, `src/nanoclaw/`, and
  `src/bin/nanoclaw.rs`.
- Parked reference material lives under `graveyard/agency-harness/`.

Do not revive old Agency microservice targets just to satisfy stale docs or
tests. Useful ideas from the parked harness must be clean-roomed into smaller
foundation or runtime contracts with focused validation.

## Build, Test, And Run

Use the active NanoClaw gates:

```bash
cargo check --all-targets
cargo test --all-targets
cargo run --quiet --bin nanoclaw -- show-config
git diff --check
```

The equivalent Make target is:

```bash
make verify
```

The active Cargo target graph should expose only:

- library: `rust_agency`
- binary: `nanoclaw`
- build script: `build-script-build`

## Operator Entrypoints

Current commands:

```bash
cargo run --quiet --bin nanoclaw -- show-config
cargo run --quiet --bin nanoclaw -- runtime status
cargo run --quiet --bin nanoclaw -- runtime state --limit 5
cargo run --quiet --bin nanoclaw -- runtime inspect --limit 5
cargo run --quiet --bin nanoclaw -- runtime health --limit 5
cargo run --quiet --bin nanoclaw -- runtime cleanup --state-residue
```

`start_agency.sh` is a compatibility guard only. It must not launch the old
`memory_server`, `speaker_server`, `listener_server`, or `nexus_server`
binaries.

## Adoption Discipline

Before moving, deleting, or reintroducing old Agency code, update:

- `docs/agency-capability-adoption-ledger.md`
- `docs/nanoclaw-rs-collapse-audit.md`

Classify old code as one of:

- active,
- assimilate-foundation,
- assimilate-runtime,
- park-reference,
- graveyard-holonic,
- delete-after-replacement.

## Provider And Execution Boundaries

Provider/model calls must flow through the existing adapter, gateway, OMX, and
runtime contracts. Do not bypass those paths for Azure, ZAI, Codex, OpenClaw,
or other providers unless the task is explicitly a one-off diagnostic.

Execution changes should preserve operator-visible evidence: artifacts,
verification, blockers, workspace references, and provider/backend metadata.
