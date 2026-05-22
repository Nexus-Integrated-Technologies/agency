#!/usr/bin/env python3
"""Run a paid Azure OpenAI smoke through NanoClaw's worker contract.

This intentionally prints only sanitized provider metadata. It does not print
environment variables, provider keys, or request headers.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import shutil
import subprocess
import tempfile
import uuid


def resolve_nanoclaw_bin() -> str:
    explicit = os.environ.get("NANOCLAW_BIN", "").strip()
    if explicit:
        return explicit
    return shutil.which("nanoclaw") or "/usr/local/bin/nanoclaw"


def build_request() -> dict:
    now = dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")
    session_id = "azure-paid-smoke-" + uuid.uuid4().hex[:8]
    root = os.path.join(tempfile.gettempdir(), session_id)
    message = os.environ.get("NANOCLAW_AZURE_SMOKE_PROMPT", "").strip()
    if not message:
        message = "Output token azure-paid-smoke-ok only."

    return {
        "invocation_id": "exec-" + session_id,
        "requested_at": now,
        "group": {
            "jid": "paperclip:smoke:azure",
            "name": "Azure Paid Smoke",
            "folder": "paperclip_agent_azure_paid_smoke",
            "trigger": "@nanoclaw",
            "added_at": now,
            "requires_trigger": False,
            "is_main": False,
        },
        "prompt": message,
        "paperclip_overlay_context": None,
        "messages": [
            {
                "id": "msg-" + session_id,
                "chat_jid": "paperclip:smoke:azure",
                "sender": "paperclip",
                "sender_name": "Paperclip Smoke",
                "content": message,
                "timestamp": now,
                "is_from_me": False,
                "is_bot_message": True,
            }
        ],
        "task_id": None,
        "script": None,
        "omx": None,
        "assistant_name": "NanoClaw",
        "request_plane": "Web",
        "env": {},
        "session": {
            "id": session_id,
            "group_folder": "paperclip_agent_azure_paid_smoke",
            "workspace_root": root,
            "session_root": os.path.join(root, "session"),
            "ipc_root": os.path.join(root, "session", "ipc"),
            "state_root": os.path.join(root, "session", "state"),
            "logs_root": os.path.join(root, "session", "logs"),
        },
        "backend_override": "AzureOpenAI",
        "task_signature": None,
        "routing_decision": None,
        "objective": None,
        "plan": None,
        "boundary_claims": [],
        "gate_evaluation": None,
    }


def main() -> int:
    request = build_request()
    timeout = int(os.environ.get("NANOCLAW_AZURE_SMOKE_TIMEOUT_SECONDS", "180"))
    proc = subprocess.run(
        [resolve_nanoclaw_bin(), "exec-worker-stdio"],
        input=json.dumps(request),
        text=True,
        capture_output=True,
        timeout=timeout,
        check=False,
    )

    if proc.returncode != 0:
        print(
            json.dumps(
                {
                    "ok": False,
                    "error": "nanoclaw exec-worker-stdio failed",
                    "status": proc.returncode,
                    "stderr": proc.stderr.strip()[:800],
                },
                indent=2,
            )
        )
        return proc.returncode

    output = json.loads(proc.stdout)
    response = output.get("response") or {}
    metadata = response.get("metadata") or {}
    print(
        json.dumps(
            {
                "ok": output.get("error") is None,
                "error": output.get("error"),
                "text": response.get("text"),
                "backend": metadata.get("backend"),
                "provider": metadata.get("provider"),
                "biller": metadata.get("biller"),
                "billing_type": metadata.get("billing_type"),
                "model": metadata.get("model"),
                "usage": metadata.get("usage"),
            },
            indent=2,
        )
    )
    return 0 if output.get("error") is None else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, indent=2))
        raise SystemExit(1)
