#!/bin/sh
set -eu

cat >&2 <<'MSG'
scripts/bundle_onnx.sh is a legacy Agency model-artifact helper.

The active Nexus Rust NanoClaw runtime does not use the old proof_of_life
binary or ONNX runtime bootstrap path. The original helper is parked at:

  graveyard/agency-harness/scripts/bundle_onnx.sh

If a future model lane needs this behavior, clean-room it into a typed runtime
provider contract and keep validation under `make verify`.
MSG

exit 2
