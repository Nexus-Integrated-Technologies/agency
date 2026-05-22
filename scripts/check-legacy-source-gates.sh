#!/usr/bin/env bash
set -euo pipefail

python3 - <<'PY'
import pathlib
import re
import sys

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - local Python should be 3.11+
    print("python tomllib is required for legacy source gates", file=sys.stderr)
    sys.exit(2)

root = pathlib.Path.cwd()

with (root / "Cargo.toml").open("rb") as cargo_file:
    cargo = tomllib.load(cargo_file)

package = cargo.get("package", {})
for key in ("autobins", "autotests", "autoexamples", "autobenches"):
    if package.get(key) is not False:
        raise SystemExit(f"Cargo.toml package.{key} must remain false")

bins = cargo.get("bin", [])
expected_bins = [{"name": "nanoclaw", "path": "src/bin/nanoclaw.rs"}]
if bins != expected_bins:
    raise SystemExit(f"unexpected active bin targets: {bins!r}")

for target_key in ("test", "example", "bench"):
    if cargo.get(target_key):
        raise SystemExit(f"unexpected active {target_key} targets: {cargo[target_key]!r}")

lib_rs = (root / "src/lib.rs").read_text(encoding="utf-8")
for module in (
    "agent",
    "fpf",
    "memory",
    "models",
    "orchestrator",
    "runtime",
    "safety",
    "services",
    "tools",
):
    pattern = rf"(?m)^\s*pub\s+mod\s+{re.escape(module)}\s*;"
    if re.search(pattern, lib_rs):
        raise SystemExit(f"legacy module src/{module} must not be exported from src/lib.rs")

active_bin_files = sorted(path.name for path in (root / "src/bin").glob("*.rs"))
if active_bin_files != ["nanoclaw.rs"]:
    raise SystemExit(f"src/bin may only expose nanoclaw.rs; found {active_bin_files!r}")

for forbidden in ("src/main.rs", "src/server.rs", "src/desktop.rs", "tests"):
    if (root / forbidden).exists():
        raise SystemExit(f"{forbidden} must remain parked outside the active Cargo surface")

required_parked_paths = (
    "graveyard/agency-harness/LEGACY_README.md",
    "graveyard/agency-harness/scripts/bundle_onnx.sh",
    "graveyard/agency-harness/src-bin/README.md",
    "graveyard/agency-harness/src-root/main.rs",
    "graveyard/agency-harness/src-root/server.rs",
    "graveyard/agency-harness/src-root/desktop.rs",
    "graveyard/agency-harness/tests",
)
missing = [path for path in required_parked_paths if not (root / path).exists()]
if missing:
    raise SystemExit(f"missing parked legacy source paths: {missing!r}")

active_bundle_helper = (root / "scripts/bundle_onnx.sh").read_text(encoding="utf-8")
if "cargo run --bin proof_of_life" in active_bundle_helper:
    raise SystemExit("scripts/bundle_onnx.sh must remain a guard, not a legacy proof_of_life launcher")

print("legacy source gates ok")
PY
