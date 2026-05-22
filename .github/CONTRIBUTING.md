# Contributing to the Nexus Rust NanoClaw Runtime

Thank you for contributing. This repository is in an active collapse from the
older Agency harness toward the Nexus Rust NanoClaw runtime.

## Legal Requirement

All contributors must sign the Contributor License Agreement. When you open a
pull request, the CLA check may require you to post:

```text
I have read and agree to the CLA
```

## Development Setup

Install the stable Rust toolchain, then use the active runtime gates:

```bash
cargo check --all-targets
cargo test --all-targets
cargo run --quiet --bin nanoclaw -- show-config
git diff --check
```

Or run:

```bash
make verify
```

## Runtime Scope

The active runtime is:

- `src/foundation/`
- `src/nanoclaw/`
- `src/bin/nanoclaw.rs`

Parked Agency harness code under `graveyard/agency-harness/` is reference
material only. Do not make the old Agency binaries, integration tests, or
microservice launcher active again unless the useful behavior has been
clean-roomed into a smaller NanoClaw contract.

## Pull Request Process

1. Create a branch.
2. Keep changes narrow and traceable.
3. Update the capability adoption ledger when moving or reintroducing old
   Agency concepts.
4. Run `make verify`.
5. Include validation evidence in the PR description.

## Reporting Bugs

Use the bug report template and include concrete reproduction steps, logs, the
branch/commit, and the validation command that failed.
