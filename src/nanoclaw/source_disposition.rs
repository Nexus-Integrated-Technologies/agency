use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use super::NanoclawConfig;

#[derive(Debug, Clone, Copy)]
struct SourceDispositionSeed {
    key: &'static str,
    path: &'static str,
    source_kind: &'static str,
    decision: &'static str,
    descendant_role: &'static str,
    recommended_action: &'static str,
    rationale: &'static str,
}

fn source_disposition_seeds() -> Vec<SourceDispositionSeed> {
    vec![
        SourceDispositionSeed {
            key: "fpf",
            path: "src/fpf",
            source_kind: "holonic_governance_reference",
            decision: "partially_adopted_reference",
            descendant_role: "src/foundation plus src/nanoclaw/fpf_bridge",
            recommended_action: "distill_remaining_governance_or_graveyard_holonic",
            rationale: "FPF concepts are useful, but only narrow assurance, boundary, provenance, role, and gate concepts should enter active runtime contracts.",
        },
        SourceDispositionSeed {
            key: "orchestrator",
            path: "src/orchestrator",
            source_kind: "legacy_orchestration_reference",
            decision: "parked_reference",
            descendant_role: "src/foundation planning/routing/queue plus src/nanoclaw runtime commands",
            recommended_action: "distill_runtime_orchestration_only",
            rationale: "The active runtime already owns scheduling, queues, gateway lanes, and operator commands; old orchestrator code remains source material only.",
        },
        SourceDispositionSeed {
            key: "agent",
            path: "src/agent",
            source_kind: "legacy_agent_loop_reference",
            decision: "parked_reference",
            descendant_role: "src/nanoclaw/executor, src/nanoclaw/model_router, src/nanoclaw/openclaw_gateway, and src/nanoclaw/omx",
            recommended_action: "distill_provider_loop_only",
            rationale: "Agent loop ideas must re-enter through adapter/gateway/runtime contracts rather than reviving the legacy agent module graph.",
        },
        SourceDispositionSeed {
            key: "memory",
            path: "src/memory",
            source_kind: "legacy_memory_reference",
            decision: "partially_adopted_reference",
            descendant_role: "src/foundation/session plus src/nanoclaw/session_storage",
            recommended_action: "distill_replayable_session_contracts_only",
            rationale: "Useful episodic compaction has been clean-roomed into active session state; remaining memory code needs explicit replay and rollback contracts before adoption.",
        },
        SourceDispositionSeed {
            key: "tools",
            path: "src/tools",
            source_kind: "legacy_tool_reference",
            decision: "partially_adopted_reference",
            descendant_role: "src/foundation/tool_contract plus src/nanoclaw/tool_registry",
            recommended_action: "distill_typed_adapter_contracts_only",
            rationale: "Tools can re-enter only as typed adapters with request-plane policy, artifacts, verification, blockers, and operator visibility.",
        },
        SourceDispositionSeed {
            key: "models",
            path: "src/models",
            source_kind: "legacy_model_reference",
            decision: "parked_reference",
            descendant_role: "src/nanoclaw/model_router through provider contracts",
            recommended_action: "leave_parked_until_model_lane_needed",
            rationale: "Model implementations are not part of the active collapse path and should not be revived without a concrete provider lane contract.",
        },
        SourceDispositionSeed {
            key: "services",
            path: "src/services",
            source_kind: "legacy_service_reference",
            decision: "parked_reference",
            descendant_role: "src/nanoclaw webhook, gateway, Slack, and local channel services",
            recommended_action: "leave_parked_until_service_contract_needed",
            rationale: "The active runtime uses smaller service boundaries; old service modules remain examples until a service contract needs them.",
        },
        SourceDispositionSeed {
            key: "safety",
            path: "src/safety",
            source_kind: "legacy_safety_reference",
            decision: "distill_candidate",
            descendant_role: "src/nanoclaw/command_safety, src/nanoclaw/ingress_policy, and src/nanoclaw/output_safety",
            recommended_action: "distill_runtime_safety_guards",
            rationale: "Safety utilities are likely useful, but should be adopted as narrow command, ingress, and output guards instead of as the legacy module.",
        },
    ]
}

fn to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn collect_files(root: &Path) -> Result<Vec<PathBuf>> {
    if !root.exists() {
        return Ok(Vec::new());
    }

    let mut pending = vec![root.to_path_buf()];
    let mut files = Vec::<PathBuf>::new();

    while let Some(path) = pending.pop() {
        let metadata = fs::metadata(&path)
            .with_context(|| format!("failed to stat source path {}", path.display()))?;
        if metadata.is_dir() {
            for entry in fs::read_dir(&path)
                .with_context(|| format!("failed to read source directory {}", path.display()))?
            {
                pending.push(entry?.path());
            }
        } else if metadata.is_file() {
            files.push(path);
        }
    }

    files.sort();
    Ok(files)
}

fn source_metrics(project_root: &Path, relative_path: &str) -> Result<Value> {
    let root = project_root.join(relative_path);
    let files = collect_files(&root)?;
    let mut total_bytes = 0_u64;
    let mut total_lines = 0_u64;
    let mut hasher = Sha256::new();

    for file in &files {
        let relative_file = file.strip_prefix(project_root).unwrap_or(file);
        let bytes = fs::read(file)
            .with_context(|| format!("failed to read source file {}", file.display()))?;
        total_bytes += bytes.len() as u64;
        total_lines += bytes.iter().filter(|byte| **byte == b'\n').count() as u64;
        hasher.update(relative_file.to_string_lossy().as_bytes());
        hasher.update(b"\0");
        hasher.update(&bytes);
        hasher.update(b"\0");
    }

    Ok(json!({
        "exists": root.exists(),
        "path": root.display().to_string(),
        "relativePath": relative_path,
        "fileCount": files.len(),
        "totalBytes": total_bytes,
        "totalLines": total_lines,
        "fingerprintSha256": if files.is_empty() { Value::Null } else { Value::String(to_hex(&hasher.finalize())) },
    }))
}

fn seed_to_item(config: &NanoclawConfig, seed: SourceDispositionSeed) -> Result<Value> {
    let metrics = source_metrics(&config.project_root, seed.path)?;
    let present = metrics["exists"].as_bool().unwrap_or(false);
    Ok(json!({
        "key": seed.key,
        "sourceKind": seed.source_kind,
        "status": if present { "present" } else { "missing" },
        "decision": seed.decision,
        "recommendedAction": seed.recommended_action,
        "descendantRole": seed.descendant_role,
        "mutationPolicy": {
            "default": "report_only",
            "moveSource": false,
            "deleteSource": false,
            "requiresCleanRoomContract": true,
        },
        "cargoSurface": {
            "activeByDefault": false,
            "reason": "Cargo auto-discovery is disabled and legacy Agency modules are not exported from src/lib.rs.",
        },
        "rationale": seed.rationale,
        "metrics": metrics,
    }))
}

pub fn source_disposition_report(config: &NanoclawConfig, limit: usize) -> Result<Value> {
    let seeds = source_disposition_seeds();
    let mut items = Vec::<Value>::new();
    for seed in seeds.iter().take(limit.max(1)) {
        items.push(seed_to_item(config, *seed)?);
    }

    let present_count = items
        .iter()
        .filter(|item| item["status"].as_str() == Some("present"))
        .count();
    let distill_candidates = items
        .iter()
        .filter(|item| {
            item["recommendedAction"]
                .as_str()
                .unwrap_or_default()
                .starts_with("distill")
        })
        .count();

    Ok(json!({
        "schemaVersion": "2026-05-21",
        "status": "ok",
        "kind": "source_disposition",
        "policy": {
            "mutationDefault": "off",
            "sourceMoves": false,
            "sourceDeletes": false,
            "cleanRoomRequired": true,
        },
        "projectRoot": config.project_root.display().to_string(),
        "summary": {
            "trackedSources": source_disposition_seeds().len(),
            "reportedSources": items.len(),
            "presentSources": present_count,
            "distillCandidates": distill_candidates,
        },
        "items": items,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config(project_root: PathBuf) -> NanoclawConfig {
        let mut config = NanoclawConfig::from_env();
        config.project_root = project_root.clone();
        config.data_dir = project_root.join("data");
        config.groups_dir = project_root.join("groups");
        config.store_dir = project_root.join("store");
        config.db_path = config.store_dir.join("messages.db");
        config
    }

    #[test]
    fn source_disposition_reports_legacy_source_without_mutation() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let source_dir = temp.path().join("src/fpf");
        fs::create_dir_all(&source_dir)?;
        fs::write(source_dir.join("holon.rs"), "pub struct Holon;\n")?;

        let report = source_disposition_report(&test_config(temp.path().to_path_buf()), 1)?;
        assert_eq!(report["kind"], "source_disposition");
        assert_eq!(report["policy"]["sourceDeletes"], false);
        assert_eq!(report["items"][0]["key"], "fpf");
        assert_eq!(report["items"][0]["status"], "present");
        assert_eq!(report["items"][0]["cargoSurface"]["activeByDefault"], false);
        assert_eq!(report["items"][0]["metrics"]["fileCount"], 1);
        assert!(report["items"][0]["metrics"]["fingerprintSha256"]
            .as_str()
            .is_some_and(|value| value.len() == 64));
        assert!(source_dir.join("holon.rs").exists());
        Ok(())
    }
}
