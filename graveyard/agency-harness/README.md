# Parked Agency Harness Reference

This folder preserves inactive Agency harness entrypoints, service scripts,
top-level Rust leftovers, tests, Docker files, and legacy documentation while
the runtime collapses toward Rust NanoClaw.

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

The original `start_agency.sh`, Matrix Conduit launcher, ONNX bundling helper,
speaker Dockerfile, and old compose stack are parked under `scripts/` and
`docker/`. The root `start_agency.sh` and `scripts/bundle_onnx.sh` paths are now
guards that point operators away from inactive Agency targets and toward active
NanoClaw contracts.

`src-root/` contains old root-level Rust files that are outside the explicit
Cargo target graph. They are kept only as adoption reference material.
