#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

if ! command -v rg >/dev/null 2>&1; then
  echo "check-task-completion-gates: ripgrep is required" >&2
  exit 1
fi

allowed_files=$(
  cat <<'EOF'
src/foundation/ports.rs
src/nanoclaw/app.rs
src/nanoclaw/cli.rs
src/nanoclaw/db.rs
EOF
)

unexpected=$(
  rg -l 'set_task_status\(' src/foundation src/nanoclaw \
    | sort \
    | while IFS= read -r file; do
      if ! grep -Fxq "$file" <<<"$allowed_files"; then
        echo "$file"
      fi
    done
)

if [[ -n "$unexpected" ]]; then
  echo "Unexpected set_task_status callsites detected:" >&2
  echo "$unexpected" >&2
  echo "Task completion must flow through NanoclawApp completion gates." >&2
  exit 1
fi

echo "task completion gate source check passed"
