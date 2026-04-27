use std::path::Path;
use std::process::{Command, Stdio};

use anyhow::Result;
use serde::{Deserialize, Serialize};

use super::project_environments::{
    build_project_environment_runtime_state, default_project_environments_path,
    list_project_environment_unset_keys, resolve_project_environment,
    should_inject_project_environment_secrets, ProjectEnvironmentResolutionInput,
    ProjectEnvironmentRuntimeState, ProjectRuntime, ResolvedProjectEnvironment,
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum WorkerBackend {
    Summary,
    Codex,
    Claude,
    WorkersAI,
    Custom(String),
}

impl WorkerBackend {
    pub fn parse(input: &str) -> Self {
        match input.trim().to_ascii_lowercase().as_str() {
            "" | "summary" => Self::Summary,
            "codex" => Self::Codex,
            "claude" => Self::Claude,
            "workers-ai" | "workers_ai" | "workersai" => Self::WorkersAI,
            other => Self::Custom(other.to_string()),
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            Self::Summary => "summary",
            Self::Codex => "codex",
            Self::Claude => "claude",
            Self::WorkersAI => "workers-ai",
            Self::Custom(value) => value.as_str(),
        }
    }

    pub fn project_runtime(&self) -> Option<ProjectRuntime> {
        match self {
            Self::Codex => Some(ProjectRuntime::Codex),
            Self::Claude => Some(ProjectRuntime::Claude),
            Self::WorkersAI | Self::Summary | Self::Custom(_) => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ResolvedWorkerBackend {
    pub backend: WorkerBackend,
    pub project_environment: Option<ResolvedProjectEnvironment>,
    pub runtime_state: Option<ProjectEnvironmentRuntimeState>,
    pub unset_keys: Vec<String>,
}

pub fn resolve_worker_backend(
    workspace_root: &Path,
    group_folder: &str,
) -> Result<ResolvedWorkerBackend> {
    let project_environment = resolve_project_environment(
        &default_project_environments_path(),
        ProjectEnvironmentResolutionInput {
            explicit_id: std::env::var("NANOCLAW_PROJECT_ENVIRONMENT_ID")
                .ok()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty()),
            requested_paths: vec![workspace_root.display().to_string()],
            resolved_paths: vec![workspace_root.display().to_string()],
            group_folder: Some(group_folder.to_string()),
        },
    )?;
    let backend = resolve_backend_choice(project_environment.as_ref());
    let (runtime_state, unset_keys) = if let Some(runtime) = backend.project_runtime() {
        let runtime_state = project_environment
            .as_ref()
            .map(|resolved| {
                build_project_environment_runtime_state(
                    &resolved.project,
                    runtime,
                    should_inject_project_environment_secrets(&resolved.project, runtime),
                )
            })
            .transpose()?;
        let unset_keys = project_environment
            .as_ref()
            .map(|resolved| list_project_environment_unset_keys(&resolved.project, runtime))
            .unwrap_or_default();
        (runtime_state, unset_keys)
    } else {
        (None, Vec::new())
    };

    Ok(ResolvedWorkerBackend {
        backend,
        project_environment,
        runtime_state,
        unset_keys,
    })
}

fn resolve_backend_choice(
    project_environment: Option<&ResolvedProjectEnvironment>,
) -> WorkerBackend {
    if let Some(explicit) = explicit_backend_from_env() {
        return explicit;
    }

    if let Some(project_environment) = project_environment {
        if project_environment.project.codex.is_some() {
            return WorkerBackend::Codex;
        }
        if project_environment.project.claude.is_some() {
            return WorkerBackend::Claude;
        }
    }

    WorkerBackend::Summary
}

fn explicit_backend_from_env() -> Option<WorkerBackend> {
    std::env::var("NANOCLAW_WORKER_BACKEND")
        .or_else(|_| std::env::var("NANOCLAW_MODEL_BACKEND"))
        .ok()
        .map(|value| WorkerBackend::parse(&value))
}

pub fn is_command_available(program: &str) -> bool {
    Command::new(program)
        .arg("--help")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok()
}

#[cfg(test)]
mod tests {
    use super::{is_command_available, WorkerBackend};

    #[test]
    fn parses_worker_backends() {
        assert_eq!(WorkerBackend::parse("codex"), WorkerBackend::Codex);
        assert_eq!(WorkerBackend::parse("claude"), WorkerBackend::Claude);
        assert_eq!(WorkerBackend::parse("workers-ai"), WorkerBackend::WorkersAI);
        assert_eq!(WorkerBackend::parse(""), WorkerBackend::Summary);
    }

    #[test]
    fn command_probe_does_not_crash() {
        assert!(!is_command_available("__nanoclaw_missing_binary__"));
    }
}
