# Parked Agency Harness Reference

This folder preserves inactive Agency harness entrypoints, tests, and legacy
documentation while the runtime collapses toward Rust NanoClaw.

These files are not active runtime targets. They were moved out of Cargo's
auto-discovery paths so active validation can focus on:

- `src/foundation/`
- `src/nanoclaw/`
- `src/bin/nanoclaw.rs`

Use this material as clean-room reference only. If a concept needs to re-enter
the runtime, first record the adoption decision in
`docs/agency-capability-adoption-ledger.md`, then reimplement the smallest
equivalent primitive in `src/foundation/` or `src/nanoclaw/` with focused tests.

`LEGACY_README.md` is the pre-collapse repository README. It describes the old
Agency microservice suite and should not be treated as current operator
documentation.
