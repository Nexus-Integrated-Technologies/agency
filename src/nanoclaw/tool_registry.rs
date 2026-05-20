use crate::foundation::{
    validate_tool_adapter_contract, CapabilityManifest, RequestPlane, ToolAdapterApprovalPolicy,
    ToolAdapterContract, ToolAdapterContractViolation, ToolAdapterMode,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolAdapterContractValidationReport {
    pub id: String,
    pub violations: Vec<ToolAdapterContractViolation>,
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
    built_in_tool_adapter_contracts()
        .into_iter()
        .filter_map(|contract| {
            let violations = validate_tool_adapter_contract(&contract);
            (!violations.is_empty()).then_some(ToolAdapterContractValidationReport {
                id: contract.id,
                violations,
            })
        })
        .collect()
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
    use std::collections::BTreeSet;

    use super::*;

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
