use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
};

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};

use crate::foundation::{
    validate_tool_adapter_contract, CapabilityManifest, RequestPlane, ToolAdapterApprovalPolicy,
    ToolAdapterContract, ToolAdapterContractViolation, ToolAdapterMode,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolAdapterContractValidationReport {
    pub id: String,
    pub violations: Vec<ToolAdapterContractViolation>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct ExternalToolAdapterManifest {
    #[serde(default)]
    pub contracts: Vec<ToolAdapterContract>,
}

pub fn built_in_tool_adapter_contracts() -> Vec<ToolAdapterContract> {
    vec![
        codex_local_contract(),
        openclaw_gateway_contract(),
        omx_gateway_contract(),
        host_shell_contract(),
        http_request_contract(),
        workers_ai_advisory_contract(),
        host_os_control_contract(),
    ]
}

pub fn built_in_tool_adapter_contract(id: &str) -> Option<ToolAdapterContract> {
    built_in_tool_adapter_contracts()
        .into_iter()
        .find(|contract| contract.id == id)
}

pub fn validate_built_in_tool_adapter_contracts() -> Vec<ToolAdapterContractValidationReport> {
    validate_tool_adapter_contracts(&built_in_tool_adapter_contracts())
}

pub fn load_external_tool_adapter_contracts(
    path: impl AsRef<Path>,
) -> Result<Vec<ToolAdapterContract>> {
    let path = path.as_ref();
    if !path.exists() {
        return Ok(Vec::new());
    }

    let raw =
        fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))?;
    if raw.trim().is_empty() {
        return Ok(Vec::new());
    }

    let contracts = parse_external_tool_adapter_contracts(&raw, path)?;
    let reports = validate_external_tool_adapter_contracts(&contracts);
    if !reports.is_empty() {
        bail!(
            "external tool adapter manifest {} failed validation: {}",
            path.display(),
            summarize_validation_reports(&reports)
        );
    }

    Ok(contracts)
}

pub fn validate_tool_adapter_contracts(
    contracts: &[ToolAdapterContract],
) -> Vec<ToolAdapterContractValidationReport> {
    let mut reports_by_id = BTreeMap::<String, Vec<ToolAdapterContractViolation>>::new();

    for contract in contracts {
        let violations = validate_tool_adapter_contract(contract);
        if !violations.is_empty() {
            reports_by_id
                .entry(contract.id.clone())
                .or_default()
                .extend(violations);
        }
    }

    let mut seen = BTreeMap::<&str, usize>::new();
    for contract in contracts {
        *seen.entry(contract.id.as_str()).or_default() += 1;
    }

    for (id, count) in seen {
        if count > 1 {
            reports_by_id.entry(id.to_string()).or_default().push(
                ToolAdapterContractViolation::new("id", "tool adapter ids must be unique"),
            );
        }
    }

    reports_by_id
        .into_iter()
        .map(|(id, violations)| ToolAdapterContractValidationReport { id, violations })
        .collect()
}

pub fn validate_external_tool_adapter_contracts(
    contracts: &[ToolAdapterContract],
) -> Vec<ToolAdapterContractValidationReport> {
    let mut reports_by_id = validate_tool_adapter_contracts(contracts)
        .into_iter()
        .map(|report| (report.id, report.violations))
        .collect::<BTreeMap<_, _>>();
    let built_in_ids = built_in_tool_adapter_contracts()
        .into_iter()
        .map(|contract| contract.id)
        .collect::<BTreeSet<_>>();

    for contract in contracts {
        if built_in_ids.contains(&contract.id) {
            reports_by_id.entry(contract.id.clone()).or_default().push(
                ToolAdapterContractViolation::new(
                    "id",
                    "external tool adapter id is reserved for a built-in adapter",
                ),
            );
        }
    }

    reports_by_id
        .into_iter()
        .map(|(id, violations)| ToolAdapterContractValidationReport { id, violations })
        .collect()
}

fn parse_external_tool_adapter_contracts(
    raw: &str,
    path: &Path,
) -> Result<Vec<ToolAdapterContract>> {
    let value = serde_json::from_str::<serde_json::Value>(raw)
        .with_context(|| format!("failed to parse {}", path.display()))?;

    if value.is_array() {
        serde_json::from_value::<Vec<ToolAdapterContract>>(value)
            .with_context(|| format!("failed to decode contract array from {}", path.display()))
    } else {
        let manifest = serde_json::from_value::<ExternalToolAdapterManifest>(value)
            .with_context(|| format!("failed to decode manifest from {}", path.display()))?;
        Ok(manifest.contracts)
    }
}

fn summarize_validation_reports(reports: &[ToolAdapterContractValidationReport]) -> String {
    reports
        .iter()
        .map(|report| {
            let details = report
                .violations
                .iter()
                .map(|violation| format!("{}={}", violation.field, violation.message))
                .collect::<Vec<_>>()
                .join(", ");
            format!("{}: {}", report.id, details)
        })
        .collect::<Vec<_>>()
        .join("; ")
}

fn codex_local_contract() -> ToolAdapterContract {
    ToolAdapterContract {
        id: "codex_local".to_string(),
        runtime_name: "codex-local".to_string(),
        mode: ToolAdapterMode::Code,
        request_plane: RequestPlane::None,
        capabilities: CapabilityManifest {
            repo_sync: true,
            host_command: true,
            secret_broker: true,
            ..CapabilityManifest::default()
        },
        approval_policy: ToolAdapterApprovalPolicy::NotRequired,
        artifact_kinds_required: vec!["diff".to_string(), "execution_log".to_string()],
        verification_kinds_required: vec![
            "execution_evidence".to_string(),
            "verification_command".to_string(),
        ],
        blockers_required_on_failure: true,
        workspace_required: true,
        operator_visible: true,
        source_material: Some("active:nanoclaw/executor".to_string()),
    }
}

fn openclaw_gateway_contract() -> ToolAdapterContract {
    ToolAdapterContract {
        id: "openclaw_gateway".to_string(),
        runtime_name: "openclaw-gateway".to_string(),
        mode: ToolAdapterMode::Gateway,
        request_plane: RequestPlane::Web,
        capabilities: CapabilityManifest {
            web_request: true,
            repo_sync: true,
            secret_broker: true,
            ..CapabilityManifest::default()
        },
        approval_policy: ToolAdapterApprovalPolicy::NotRequired,
        artifact_kinds_required: vec!["gateway_result".to_string(), "execution_log".to_string()],
        verification_kinds_required: vec![
            "gateway_status".to_string(),
            "execution_evidence".to_string(),
        ],
        blockers_required_on_failure: true,
        workspace_required: true,
        operator_visible: true,
        source_material: Some("active:nanoclaw/openclaw_gateway".to_string()),
    }
}

fn omx_gateway_contract() -> ToolAdapterContract {
    ToolAdapterContract {
        id: "omx_gateway".to_string(),
        runtime_name: "omx-gateway".to_string(),
        mode: ToolAdapterMode::Gateway,
        request_plane: RequestPlane::None,
        capabilities: CapabilityManifest {
            repo_sync: true,
            host_command: true,
            secret_broker: true,
            ..CapabilityManifest::default()
        },
        approval_policy: ToolAdapterApprovalPolicy::NotRequired,
        artifact_kinds_required: vec!["omx_session_log".to_string(), "team_status".to_string()],
        verification_kinds_required: vec![
            "team_status".to_string(),
            "execution_evidence".to_string(),
        ],
        blockers_required_on_failure: true,
        workspace_required: true,
        operator_visible: true,
        source_material: Some("active:nanoclaw/omx".to_string()),
    }
}

fn host_shell_contract() -> ToolAdapterContract {
    ToolAdapterContract {
        id: "host_shell".to_string(),
        runtime_name: "host-shell".to_string(),
        mode: ToolAdapterMode::Shell,
        request_plane: RequestPlane::None,
        capabilities: CapabilityManifest {
            host_command: true,
            ..CapabilityManifest::default()
        },
        approval_policy: ToolAdapterApprovalPolicy::NotRequired,
        artifact_kinds_required: vec!["stdout".to_string(), "stderr".to_string()],
        verification_kinds_required: vec![
            "exit_status".to_string(),
            "execution_evidence".to_string(),
        ],
        blockers_required_on_failure: true,
        workspace_required: false,
        operator_visible: true,
        source_material: Some("active:nanoclaw/executor".to_string()),
    }
}

fn http_request_contract() -> ToolAdapterContract {
    ToolAdapterContract {
        id: "http_request".to_string(),
        runtime_name: "http-request".to_string(),
        mode: ToolAdapterMode::Http,
        request_plane: RequestPlane::Web,
        capabilities: CapabilityManifest {
            web_request: true,
            ..CapabilityManifest::default()
        },
        approval_policy: ToolAdapterApprovalPolicy::NotRequired,
        artifact_kinds_required: Vec::new(),
        verification_kinds_required: vec!["http_status".to_string(), "response_shape".to_string()],
        blockers_required_on_failure: true,
        workspace_required: false,
        operator_visible: true,
        source_material: Some("active:nanoclaw/request_plane".to_string()),
    }
}

fn workers_ai_advisory_contract() -> ToolAdapterContract {
    ToolAdapterContract {
        id: "workers_ai_advisory".to_string(),
        runtime_name: "workers-ai-advisory".to_string(),
        mode: ToolAdapterMode::Advisory,
        request_plane: RequestPlane::Web,
        capabilities: CapabilityManifest {
            web_request: true,
            secret_broker: true,
            ..CapabilityManifest::default()
        },
        approval_policy: ToolAdapterApprovalPolicy::NotRequired,
        artifact_kinds_required: Vec::new(),
        verification_kinds_required: vec![
            "policy_gate".to_string(),
            "advisory_response".to_string(),
        ],
        blockers_required_on_failure: true,
        workspace_required: false,
        operator_visible: true,
        source_material: Some("active:nanoclaw/model_router".to_string()),
    }
}

fn host_os_control_contract() -> ToolAdapterContract {
    ToolAdapterContract {
        id: "host_os_control".to_string(),
        runtime_name: "host-os-control".to_string(),
        mode: ToolAdapterMode::HostOsControl,
        request_plane: RequestPlane::None,
        capabilities: CapabilityManifest {
            os_control: true,
            ..CapabilityManifest::default()
        },
        approval_policy: ToolAdapterApprovalPolicy::ExplicitApproval,
        artifact_kinds_required: vec!["approval_record".to_string(), "host_action_log".to_string()],
        verification_kinds_required: vec![
            "approval_decision".to_string(),
            "host_action_result".to_string(),
        ],
        blockers_required_on_failure: true,
        workspace_required: false,
        operator_visible: true,
        source_material: Some("active:nanoclaw/host_os_control".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, path::PathBuf};

    use super::*;
    use tempfile::tempdir;

    fn example_external_code_contract() -> ToolAdapterContract {
        ToolAdapterContract {
            id: "example_repo_patch".to_string(),
            runtime_name: "example-repo-patch".to_string(),
            mode: ToolAdapterMode::Code,
            request_plane: RequestPlane::None,
            capabilities: CapabilityManifest {
                repo_sync: true,
                host_command: true,
                secret_broker: true,
                ..CapabilityManifest::default()
            },
            approval_policy: ToolAdapterApprovalPolicy::NotRequired,
            artifact_kinds_required: vec!["diff".to_string(), "execution_log".to_string()],
            verification_kinds_required: vec![
                "execution_evidence".to_string(),
                "verification_command".to_string(),
            ],
            blockers_required_on_failure: true,
            workspace_required: true,
            operator_visible: true,
            source_material: Some("plugins/example-repo-patch/manifest.json".to_string()),
        }
    }

    fn example_external_http_contract() -> ToolAdapterContract {
        ToolAdapterContract {
            id: "example_web_check".to_string(),
            runtime_name: "example-web-check".to_string(),
            mode: ToolAdapterMode::Http,
            request_plane: RequestPlane::Web,
            capabilities: CapabilityManifest {
                web_request: true,
                ..CapabilityManifest::default()
            },
            approval_policy: ToolAdapterApprovalPolicy::NotRequired,
            artifact_kinds_required: Vec::new(),
            verification_kinds_required: vec!["http_status".to_string()],
            blockers_required_on_failure: true,
            workspace_required: false,
            operator_visible: true,
            source_material: Some("plugins/example-web-check/manifest.json".to_string()),
        }
    }

    #[test]
    fn built_in_tool_adapter_contracts_are_valid() {
        let reports = validate_built_in_tool_adapter_contracts();

        assert_eq!(reports, Vec::new());
    }

    #[test]
    fn built_in_tool_adapter_contract_ids_are_unique() {
        let contracts = built_in_tool_adapter_contracts();
        let ids = contracts
            .iter()
            .map(|contract| contract.id.as_str())
            .collect::<BTreeSet<_>>();

        assert_eq!(ids.len(), contracts.len());
    }

    #[test]
    fn external_tool_adapter_manifest_loads_valid_contracts() -> Result<()> {
        let dir = tempdir()?;
        let manifest_path = dir.path().join("tool-adapters.json");
        let manifest = ExternalToolAdapterManifest {
            contracts: vec![example_external_code_contract()],
        };
        fs::write(&manifest_path, serde_json::to_string_pretty(&manifest)?)?;

        let loaded = load_external_tool_adapter_contracts(&manifest_path)?;

        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].id, "example_repo_patch");
        Ok(())
    }

    #[test]
    fn external_tool_adapter_manifest_loads_valid_contract_array() -> Result<()> {
        let dir = tempdir()?;
        let manifest_path = dir.path().join("tool-adapters.json");
        let contract = example_external_http_contract();
        fs::write(
            &manifest_path,
            serde_json::to_string_pretty(&vec![contract])?,
        )?;

        let loaded = load_external_tool_adapter_contracts(&manifest_path)?;

        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].id, "example_web_check");
        Ok(())
    }

    #[test]
    fn checked_in_external_tool_adapter_example_loads() -> Result<()> {
        let manifest_path =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tool-adapters.example.json");

        let loaded = load_external_tool_adapter_contracts(&manifest_path)?;
        let ids = loaded
            .iter()
            .map(|contract| contract.id.as_str())
            .collect::<Vec<_>>();

        assert_eq!(ids, vec!["example_repo_patch", "example_web_check"]);
        Ok(())
    }

    #[test]
    fn external_tool_adapter_manifest_missing_file_is_empty() -> Result<()> {
        let missing_path = PathBuf::from("/tmp/nanoclaw-missing-tool-adapters.json");

        assert!(load_external_tool_adapter_contracts(missing_path)?.is_empty());
        Ok(())
    }

    #[test]
    fn external_tool_adapter_manifest_rejects_invalid_contract() -> Result<()> {
        let dir = tempdir()?;
        let manifest_path = dir.path().join("tool-adapters.json");
        let mut contract = example_external_code_contract();
        contract.verification_kinds_required.clear();
        fs::write(
            &manifest_path,
            serde_json::to_string_pretty(&vec![contract])?,
        )?;

        let error = load_external_tool_adapter_contracts(&manifest_path).unwrap_err();

        assert!(error
            .to_string()
            .contains("failed validation: example_repo_patch"));
        Ok(())
    }

    #[test]
    fn external_tool_adapter_manifest_rejects_duplicate_contract_ids() -> Result<()> {
        let dir = tempdir()?;
        let manifest_path = dir.path().join("tool-adapters.json");
        let first = example_external_code_contract();
        let second = example_external_code_contract();
        fs::write(
            &manifest_path,
            serde_json::to_string_pretty(&vec![first, second])?,
        )?;

        let error = load_external_tool_adapter_contracts(&manifest_path).unwrap_err();

        assert!(error
            .to_string()
            .contains("tool adapter ids must be unique"));
        Ok(())
    }

    #[test]
    fn external_tool_adapter_manifest_rejects_builtin_id_collisions() -> Result<()> {
        let dir = tempdir()?;
        let manifest_path = dir.path().join("tool-adapters.json");
        let contract = built_in_tool_adapter_contract("host_shell").unwrap();
        fs::write(
            &manifest_path,
            serde_json::to_string_pretty(&vec![contract])?,
        )?;

        let error = load_external_tool_adapter_contracts(&manifest_path).unwrap_err();

        assert!(error
            .to_string()
            .contains("reserved for a built-in adapter"));
        Ok(())
    }

    #[test]
    fn built_in_host_os_control_requires_explicit_approval() {
        let contract = built_in_tool_adapter_contract("host_os_control").unwrap();

        assert_eq!(
            contract.approval_policy,
            ToolAdapterApprovalPolicy::ExplicitApproval
        );
        assert_eq!(contract.mode, ToolAdapterMode::HostOsControl);
        assert!(contract.capabilities.os_control);
    }

    #[test]
    fn built_in_workers_ai_is_advisory_only() {
        let contract = built_in_tool_adapter_contract("workers_ai_advisory").unwrap();

        assert_eq!(contract.mode, ToolAdapterMode::Advisory);
        assert!(!contract.mode.is_completion_capable());
        assert!(contract.artifact_kinds_required.is_empty());
        assert!(!contract.verification_kinds_required.is_empty());
    }

    #[test]
    fn built_in_code_and_gateway_contracts_require_artifacts() {
        for id in [
            "codex_local",
            "openclaw_gateway",
            "omx_gateway",
            "host_shell",
        ] {
            let contract = built_in_tool_adapter_contract(id).unwrap();

            assert!(contract.mode.is_completion_capable());
            assert!(!contract.artifact_kinds_required.is_empty());
        }
    }
}
