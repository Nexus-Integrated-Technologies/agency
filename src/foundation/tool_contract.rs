use serde::{Deserialize, Serialize};

use super::{CapabilityManifest, RequestPlane};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ToolAdapterMode {
    Code,
    Shell,
    Gateway,
    Http,
    Advisory,
    HostOsControl,
    Custom(String),
}

impl ToolAdapterMode {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Code => "code",
            Self::Shell => "shell",
            Self::Gateway => "gateway",
            Self::Http => "http",
            Self::Advisory => "advisory",
            Self::HostOsControl => "host_os_control",
            Self::Custom(value) => value.as_str(),
        }
    }

    pub fn is_completion_capable(&self) -> bool {
        matches!(
            self,
            Self::Code | Self::Shell | Self::Gateway | Self::HostOsControl
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ToolAdapterApprovalPolicy {
    NotRequired,
    ExplicitApproval,
    Denied,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ToolAdapterContract {
    pub id: String,
    pub runtime_name: String,
    pub mode: ToolAdapterMode,
    pub request_plane: RequestPlane,
    pub capabilities: CapabilityManifest,
    pub approval_policy: ToolAdapterApprovalPolicy,
    pub artifact_kinds_required: Vec<String>,
    pub verification_kinds_required: Vec<String>,
    pub blockers_required_on_failure: bool,
    pub workspace_required: bool,
    pub operator_visible: bool,
    pub source_material: Option<String>,
}

impl ToolAdapterContract {
    pub fn validation_violations(&self) -> Vec<ToolAdapterContractViolation> {
        validate_tool_adapter_contract(self)
    }

    pub fn is_valid(&self) -> bool {
        self.validation_violations().is_empty()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ToolAdapterContractViolation {
    pub field: String,
    pub message: String,
}

impl ToolAdapterContractViolation {
    pub fn new(field: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            message: message.into(),
        }
    }
}

pub fn validate_tool_adapter_contract(
    contract: &ToolAdapterContract,
) -> Vec<ToolAdapterContractViolation> {
    let mut violations = Vec::new();

    if contract.id.trim().is_empty() {
        violations.push(ToolAdapterContractViolation::new(
            "id",
            "tool adapter id is required",
        ));
    }

    if contract.runtime_name.trim().is_empty() {
        violations.push(ToolAdapterContractViolation::new(
            "runtime_name",
            "runtime name is required",
        ));
    }

    if !contract.operator_visible {
        violations.push(ToolAdapterContractViolation::new(
            "operator_visible",
            "runtime tools must leave operator-visible evidence",
        ));
    }

    if !contract.blockers_required_on_failure {
        violations.push(ToolAdapterContractViolation::new(
            "blockers_required_on_failure",
            "failed tool runs must emit structured blockers",
        ));
    }

    if contract.verification_kinds_required.is_empty() {
        violations.push(ToolAdapterContractViolation::new(
            "verification_kinds_required",
            "tool adapters must declare verification evidence",
        ));
    }

    if contract.mode.is_completion_capable() && contract.artifact_kinds_required.is_empty() {
        violations.push(ToolAdapterContractViolation::new(
            "artifact_kinds_required",
            "completion-capable tool adapters must declare artifacts",
        ));
    }

    if has_blank_entry(&contract.artifact_kinds_required) {
        violations.push(ToolAdapterContractViolation::new(
            "artifact_kinds_required",
            "artifact requirement entries must not be blank",
        ));
    }

    if has_blank_entry(&contract.verification_kinds_required) {
        violations.push(ToolAdapterContractViolation::new(
            "verification_kinds_required",
            "verification requirement entries must not be blank",
        ));
    }

    if let Some(source_material) = &contract.source_material {
        if source_material.trim().is_empty() {
            violations.push(ToolAdapterContractViolation::new(
                "source_material",
                "source material must be meaningful when present",
            ));
        }
    }

    match &contract.request_plane {
        RequestPlane::Web => {
            if !contract.capabilities.web_request && !contract.capabilities.browser {
                violations.push(ToolAdapterContractViolation::new(
                    "capabilities",
                    "web request-plane tools require web_request or browser capability",
                ));
            }
        }
        RequestPlane::Email => {
            if !contract.capabilities.email_request {
                violations.push(ToolAdapterContractViolation::new(
                    "capabilities",
                    "email request-plane tools require email_request capability",
                ));
            }
        }
        RequestPlane::None | RequestPlane::Custom(_) => {}
    }

    match &contract.mode {
        ToolAdapterMode::Code => {
            if !contract.capabilities.repo_sync && !contract.capabilities.host_command {
                violations.push(ToolAdapterContractViolation::new(
                    "capabilities",
                    "code adapters require repo_sync or host_command capability",
                ));
            }
        }
        ToolAdapterMode::Shell => {
            if !contract.capabilities.host_command {
                violations.push(ToolAdapterContractViolation::new(
                    "capabilities",
                    "shell adapters require host_command capability",
                ));
            }
        }
        ToolAdapterMode::Http => {
            if !contract.capabilities.web_request {
                violations.push(ToolAdapterContractViolation::new(
                    "capabilities",
                    "http adapters require web_request capability",
                ));
            }
        }
        ToolAdapterMode::HostOsControl => {
            if !contract.capabilities.os_control {
                violations.push(ToolAdapterContractViolation::new(
                    "capabilities",
                    "host OS control adapters require os_control capability",
                ));
            }

            if contract.approval_policy != ToolAdapterApprovalPolicy::ExplicitApproval {
                violations.push(ToolAdapterContractViolation::new(
                    "approval_policy",
                    "host OS control adapters require explicit approval",
                ));
            }
        }
        ToolAdapterMode::Gateway | ToolAdapterMode::Advisory | ToolAdapterMode::Custom(_) => {}
    }

    violations
}

fn has_blank_entry(values: &[String]) -> bool {
    values.iter().any(|value| value.trim().is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_contract() -> ToolAdapterContract {
        ToolAdapterContract {
            id: "repo_patch".to_string(),
            runtime_name: "repo-patch".to_string(),
            mode: ToolAdapterMode::Code,
            request_plane: RequestPlane::None,
            capabilities: CapabilityManifest {
                repo_sync: true,
                host_command: true,
                ..CapabilityManifest::default()
            },
            approval_policy: ToolAdapterApprovalPolicy::NotRequired,
            artifact_kinds_required: vec!["diff".to_string(), "execution_log".to_string()],
            verification_kinds_required: vec!["test_command".to_string()],
            blockers_required_on_failure: true,
            workspace_required: true,
            operator_visible: true,
            source_material: Some("graveyard/agency-harness/src/tools/code_exec.rs".to_string()),
        }
    }

    #[test]
    fn tool_adapter_contract_accepts_audited_code_adapter() {
        let contract = base_contract();

        assert!(contract.is_valid());
        assert!(validate_tool_adapter_contract(&contract).is_empty());
    }

    #[test]
    fn tool_adapter_contract_rejects_completion_capable_adapter_without_artifacts() {
        let mut contract = base_contract();
        contract.artifact_kinds_required.clear();

        let violations = validate_tool_adapter_contract(&contract);

        assert!(violations
            .iter()
            .any(|violation| violation.field == "artifact_kinds_required"));
    }

    #[test]
    fn tool_adapter_contract_rejects_host_os_control_without_explicit_approval() {
        let mut contract = base_contract();
        contract.id = "host_click".to_string();
        contract.runtime_name = "host-click".to_string();
        contract.mode = ToolAdapterMode::HostOsControl;
        contract.capabilities = CapabilityManifest {
            os_control: true,
            ..CapabilityManifest::default()
        };
        contract.approval_policy = ToolAdapterApprovalPolicy::NotRequired;

        let violations = validate_tool_adapter_contract(&contract);

        assert!(violations
            .iter()
            .any(|violation| violation.field == "approval_policy"));
    }

    #[test]
    fn tool_adapter_contract_rejects_request_plane_capability_mismatch() {
        let mut contract = base_contract();
        contract.request_plane = RequestPlane::Web;
        contract.capabilities.web_request = false;
        contract.capabilities.browser = false;

        let violations = validate_tool_adapter_contract(&contract);

        assert!(violations
            .iter()
            .any(|violation| violation.message.contains("web request-plane")));
    }

    #[test]
    fn tool_adapter_contract_allows_advisory_without_artifacts_but_not_verification() {
        let mut contract = base_contract();
        contract.mode = ToolAdapterMode::Advisory;
        contract.artifact_kinds_required.clear();

        assert!(validate_tool_adapter_contract(&contract).is_empty());

        contract.verification_kinds_required.clear();
        let violations = validate_tool_adapter_contract(&contract);

        assert!(violations
            .iter()
            .any(|violation| violation.field == "verification_kinds_required"));
    }
}
