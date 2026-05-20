# External Tool Adapter Manifests

NanoClaw can load extra runtime-hand contracts from a JSON manifest. This is
the clean-room re-entry path for legacy Agency tool material and future plugin
directories: a tool is not an active runtime hand until it declares its
capabilities, request plane, approval policy, required evidence, and failure
blockers.

## Runtime Path

The manifest path defaults to:

```text
tool-adapters.json
```

Override it with:

```text
NANOCLAW_TOOL_ADAPTERS_PATH=/absolute/path/to/tool-adapters.json
```

`nanoclaw runtime status`, `nanoclaw runtime inspect`, and
`nanoclaw runtime health` report the external manifest state beside the
built-in registry.

## Contract Rules

- `id` must be unique inside the external manifest.
- `id` must not reuse a built-in runtime-hand id such as `codex_local`,
  `openclaw_gateway`, `omx_gateway`, `host_shell`, `http_request`,
  `workers_ai_advisory`, or `host_os_control`.
- `operator_visible` must be `true`.
- `blockers_required_on_failure` must be `true`.
- every adapter must declare `verification_kinds_required`.
- `request_plane` should use lowercase `web`, `email`, or `none`; legacy Rust
  enum casing is accepted for compatibility.
- completion-capable modes must declare `artifact_kinds_required`.
- `shell` adapters require `host_command`.
- `http` adapters require `web_request`.
- `host_os_control` adapters require `os_control` and explicit approval.
- Workers AI-style advisory adapters must remain advisory; they cannot satisfy
  code completion evidence.

Use `tool-adapters.example.json` as the checked-in compatibility fixture. The
test suite loads that file through the same manifest loader used by runtime
status, so schema drift breaks locally before a plugin becomes active.
