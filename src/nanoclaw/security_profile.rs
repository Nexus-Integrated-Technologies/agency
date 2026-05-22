use std::collections::BTreeMap;

use crate::foundation::{
    AssuranceTuple, BoundaryClaim, CapabilityManifest, CapabilityManifestPolicy, ExecutionLocation,
    ExecutionProvenanceRecord, ExecutionRunKind, ExecutionStatus, ExecutionSyncScope,
    ExecutionTrustLevel, GateEvaluation, ProvenanceEdge, RequestPlane, SymbolCarrier,
    TaskSignature,
};

pub const CAPABILITY_MANIFEST_ENV_KEY: &str = "NANOCLAW_CAPABILITY_MANIFEST";

#[derive(Debug, Clone, Copy, Default)]
pub struct DeriveCapabilityManifestInput {
    pub allow_repo_sync: bool,
    pub allow_ssh: bool,
    pub allow_host_command: bool,
    pub allow_secret_broker: bool,
    pub allow_browser: bool,
    pub allow_os_control: bool,
}

#[derive(Debug, Clone)]
pub struct BuildExecutionProvenanceInput {
    pub id: String,
    pub run_kind: ExecutionRunKind,
    pub group_folder: String,
    pub chat_jid: Option<String>,
    pub execution_location: ExecutionLocation,
    pub request_plane: RequestPlane,
    pub effective_capabilities: CapabilityManifest,
    pub project_environment_id: Option<String>,
    pub mount_summary: Vec<crate::foundation::ExecutionMountSummaryEntry>,
    pub secret_handles_used: Vec<String>,
    pub fallback_reason: Option<String>,
    pub sync_scope: Option<ExecutionSyncScope>,
    pub task_signature: Option<TaskSignature>,
    pub boundary_claims: Vec<BoundaryClaim>,
    pub gate_evaluation: Option<GateEvaluation>,
    pub assurance: Option<AssuranceTuple>,
    pub symbol_carriers: Vec<SymbolCarrier>,
    pub provenance_edges: Vec<ProvenanceEdge>,
    pub status: ExecutionStatus,
    pub created_at: String,
    pub updated_at: String,
    pub completed_at: Option<String>,
}

pub fn empty_capability_manifest() -> CapabilityManifest {
    CapabilityManifest::default()
}

pub fn derive_capability_manifest(
    request_plane: &RequestPlane,
    input: DeriveCapabilityManifestInput,
) -> CapabilityManifest {
    CapabilityManifest {
        web_request: matches!(request_plane, RequestPlane::Web),
        email_request: matches!(request_plane, RequestPlane::Email),
        browser: matches!(request_plane, RequestPlane::Web) && input.allow_browser,
        repo_sync: input.allow_repo_sync,
        ssh: input.allow_ssh,
        host_command: input.allow_host_command,
        secret_broker: input.allow_secret_broker,
        os_control: input.allow_os_control,
    }
}

pub fn apply_capability_manifest_policy(
    base: &CapabilityManifest,
    policy: Option<&CapabilityManifestPolicy>,
) -> CapabilityManifest {
    let Some(policy) = policy else {
        return base.clone();
    };

    CapabilityManifest {
        web_request: apply_manifest_flag(base.web_request, policy.web_request),
        email_request: apply_manifest_flag(base.email_request, policy.email_request),
        browser: apply_manifest_flag(base.browser, policy.browser),
        repo_sync: apply_manifest_flag(base.repo_sync, policy.repo_sync),
        ssh: apply_manifest_flag(base.ssh, policy.ssh),
        host_command: apply_manifest_flag(base.host_command, policy.host_command),
        secret_broker: apply_manifest_flag(base.secret_broker, policy.secret_broker),
        os_control: apply_manifest_flag(base.os_control, policy.os_control),
    }
}

pub fn get_capability_manifest_env(manifest: &CapabilityManifest) -> BTreeMap<String, String> {
    let mut env = BTreeMap::new();
    env.insert(
        CAPABILITY_MANIFEST_ENV_KEY.to_string(),
        serde_json::to_string(manifest).unwrap_or_else(|_| "{}".to_string()),
    );
    env
}

pub fn resolve_execution_trust_level(location: &ExecutionLocation) -> ExecutionTrustLevel {
    match location {
        ExecutionLocation::Host => ExecutionTrustLevel::HostTrusted,
        ExecutionLocation::RemoteWorker | ExecutionLocation::Omx => {
            ExecutionTrustLevel::SandboxedRemote
        }
        ExecutionLocation::LocalContainer | ExecutionLocation::Custom(_) => {
            ExecutionTrustLevel::SandboxedLocal
        }
    }
}

pub fn build_execution_provenance_record(
    input: BuildExecutionProvenanceInput,
) -> ExecutionProvenanceRecord {
    ExecutionProvenanceRecord {
        id: input.id,
        run_kind: input.run_kind,
        group_folder: input.group_folder,
        chat_jid: input.chat_jid,
        execution_location: input.execution_location.clone(),
        trust_level: resolve_execution_trust_level(&input.execution_location),
        request_plane: input.request_plane,
        effective_capabilities: input.effective_capabilities,
        project_environment_id: input.project_environment_id,
        mount_summary: input.mount_summary,
        secret_handles_used: dedupe_sort(input.secret_handles_used),
        fallback_reason: input.fallback_reason,
        sync_scope: input.sync_scope,
        gate_decision: input
            .gate_evaluation
            .as_ref()
            .map(|evaluation| evaluation.decision),
        task_signature: input.task_signature,
        boundary_claims: input.boundary_claims,
        gate_evaluation: input.gate_evaluation,
        assurance: input.assurance,
        symbol_carriers: input.symbol_carriers,
        provenance_edges: input.provenance_edges,
        status: input.status,
        created_at: input.created_at,
        updated_at: input.updated_at,
        completed_at: input.completed_at,
    }
}

fn apply_manifest_flag(base: bool, policy: Option<bool>) -> bool {
    match policy {
        Some(false) => false,
        Some(true) => base,
        None => base,
    }
}

fn dedupe_sort(values: Vec<String>) -> Vec<String> {
    let mut values = values
        .into_iter()
        .filter(|value| !value.trim().is_empty())
        .collect::<Vec<_>>();
    values.sort();
    values.dedup();
    values
}

#[cfg(test)]
mod tests {
    use crate::foundation::{
        CapabilityManifestPolicy, ExecutionLocation, ExecutionMountKind,
        ExecutionMountSummaryEntry, ExecutionRunKind, ExecutionStatus, RequestPlane,
    };

    use super::{
        apply_capability_manifest_policy, build_execution_provenance_record,
        derive_capability_manifest, BuildExecutionProvenanceInput, DeriveCapabilityManifestInput,
    };

    #[test]
    fn derives_manifest_from_request_plane() {
        let manifest = derive_capability_manifest(
            &RequestPlane::Web,
            DeriveCapabilityManifestInput {
                allow_browser: true,
                allow_host_command: true,
                ..Default::default()
            },
        );

        assert!(manifest.web_request);
        assert!(manifest.browser);
        assert!(manifest.host_command);
        assert!(!manifest.email_request);
    }

    #[test]
    fn policy_can_only_reduce_manifest() {
        let base = derive_capability_manifest(
            &RequestPlane::None,
            DeriveCapabilityManifestInput {
                allow_host_command: true,
                allow_secret_broker: true,
                ..Default::default()
            },
        );
        let policy = CapabilityManifestPolicy {
            host_command: Some(false),
            secret_broker: Some(true),
            ..Default::default()
        };

        let manifest = apply_capability_manifest_policy(&base, Some(&policy));
        assert!(!manifest.host_command);
        assert!(manifest.secret_broker);
    }

    #[test]
    fn provenance_builder_sets_trust_level() {
        let record = build_execution_provenance_record(BuildExecutionProvenanceInput {
            id: "prov-1".to_string(),
            run_kind: ExecutionRunKind::Codex,
            group_folder: "main".to_string(),
            chat_jid: Some("main".to_string()),
            execution_location: ExecutionLocation::Host,
            request_plane: RequestPlane::Web,
            effective_capabilities: derive_capability_manifest(
                &RequestPlane::Web,
                DeriveCapabilityManifestInput {
                    allow_host_command: true,
                    ..Default::default()
                },
            ),
            project_environment_id: None,
            mount_summary: vec![ExecutionMountSummaryEntry {
                host_path: Some("/tmp/project".to_string()),
                container_path: None,
                readonly: false,
                kind: ExecutionMountKind::Project,
            }],
            secret_handles_used: vec!["env-key:OPENAI_API_KEY".to_string()],
            fallback_reason: None,
            sync_scope: None,
            task_signature: None,
            boundary_claims: Vec::new(),
            gate_evaluation: None,
            assurance: None,
            symbol_carriers: Vec::new(),
            provenance_edges: Vec::new(),
            status: ExecutionStatus::Started,
            created_at: "2026-04-06T00:00:00Z".to_string(),
            updated_at: "2026-04-06T00:00:00Z".to_string(),
            completed_at: None,
        });

        assert_eq!(record.trust_level.as_str(), "host_trusted");
    }
}
