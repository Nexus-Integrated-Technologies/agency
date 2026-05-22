#!/bin/sh
set -eu

cat >&2 <<'MSG'
start_agency.sh is a legacy Agency harness entrypoint.

The active runtime is Nexus Rust NanoClaw. Use:

  cargo run --quiet --bin nanoclaw -- show-config
  cargo run --quiet --bin nanoclaw -- gateway serve
  cargo run --quiet --bin nanoclaw -- local run

The old script is preserved at:

  graveyard/agency-harness/scripts/start_agency.sh
MSG

exit 2
