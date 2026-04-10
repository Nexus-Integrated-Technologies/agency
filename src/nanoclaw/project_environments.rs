use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::path::{Component, Path, PathBuf};

use anyhow::{bail, Context, Result};
use serde::Deserialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectRuntime {
    Codex,
    Claude,
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(default, rename_all = "camelCase")]
pub struct ProjectEnvironmentRuntimeConfig {
    pub env: BTreeMap<String, String>,
    pub env_files: Vec<String>,
    pub secret_env: BTreeMap<String, String>,
    pub secret_env_files: Vec<String>,
    pub secret_handles: Vec<String>,
    pub allow_secret_env_injection: bool,
    pub unset: Vec<String>,
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(default, rename_all = "camelCase")]
pub struct ProjectEnvironmentCodexConfig {
    #[serde(flatten)]
    pub runtime: ProjectEnvironmentRuntimeConfig,
    pub cwd: Option<String>,
    pub add_dirs: Vec<String>,
    pub sandbox: Option<String>,
    pub model: Option<String>,
    pub profile: Option<String>,
    pub search: Option<bool>,
    pub timeout_ms: Option<u64>,
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(default, rename_all = "camelCase")]
pub struct ProjectEnvironmentMatch {
    pub paths: Vec<String>,
    pub requested_paths: Vec<String>,
    pub group_folders: Vec<String>,
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(default, rename_all = "camelCase")]
pub struct ProjectEnvironmentDefinition {
    pub id: String,
    pub description: Option<String>,
    #[serde(flatten)]
    pub runtime: ProjectEnvironmentRuntimeConfig,
    #[serde(rename = "match")]
    pub match_rules: ProjectEnvironmentMatch,
    pub codex: Option<ProjectEnvironmentCodexConfig>,
    pub claude: Option<ProjectEnvironmentRuntimeConfig>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
struct ProjectEnvironmentRegistryFile {
    projects: Vec<ProjectEnvironmentDefinition>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectEnvironmentResolutionInput {
    pub explicit_id: Option<String>,
    pub requested_paths: Vec<String>,
    pub resolved_paths: Vec<String>,
    pub group_folder: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedProjectEnvironment {
    pub project: ProjectEnvironmentDefinition,
    pub matched_by: ProjectEnvironmentMatchKind,
    pub matched_value: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProjectEnvironmentMatchKind {
    Id,
    ResolvedPath,
    RequestedPath,
    GroupFolder,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectEnvironmentRuntimeState {
    pub env: BTreeMap<String, String>,
    pub secret_handles: Vec<String>,
    pub applied_env_keys: Vec<String>,
    pub blocked_secret_env_keys: Vec<String>,
}

pub fn default_project_environments_path() -> PathBuf {
    env::var("NANOCLAW_PROJECT_ENVIRONMENTS_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            env::var("HOME")
                .map(PathBuf::from)
                .unwrap_or_else(|_| PathBuf::from("."))
                .join(".config")
                .join("nanoclaw")
                .join("project-environments.json")
        })
}

pub fn load_project_environments(path: &Path) -> Vec<ProjectEnvironmentDefinition> {
    let Ok(raw) = fs::read_to_string(path) else {
        return Vec::new();
    };
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Vec::new();
    }

    if let Ok(entries) = serde_json::from_str::<Vec<ProjectEnvironmentDefinition>>(trimmed) {
        return entries
            .into_iter()
            .filter(|entry| !entry.id.trim().is_empty())
            .collect();
    }
    if let Ok(file) = serde_json::from_str::<ProjectEnvironmentRegistryFile>(trimmed) {
        return file
            .projects
            .into_iter()
            .filter(|entry| !entry.id.trim().is_empty())
            .collect();
    }

    Vec::new()
}

pub fn resolve_project_environment(
    path: &Path,
    input: ProjectEnvironmentResolutionInput,
) -> Result<Option<ResolvedProjectEnvironment>> {
    let projects = load_project_environments(path);
    if let Some(explicit_id) = input
        .explicit_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        let Some(project) = projects.into_iter().find(|entry| entry.id == explicit_id) else {
            bail!(
                "no project environment is configured with id '{}' at {}",
                explicit_id,
                path.display()
            );
        };
        return Ok(Some(ResolvedProjectEnvironment {
            project,
            matched_by: ProjectEnvironmentMatchKind::Id,
            matched_value: explicit_id.to_string(),
        }));
    }

    let normalized_requested = input
        .requested_paths
        .iter()
        .map(|value| normalize_requested_path(value))
        .filter(|value| !value.is_empty())
        .collect::<BTreeSet<_>>();
    let normalized_resolved = input
        .resolved_paths
        .iter()
        .map(|value| normalize_absolute_path(value))
        .collect::<BTreeSet<_>>();
    let group_folder = input.group_folder.as_deref().map(str::trim).unwrap_or("");

    let mut best: Option<(
        ProjectEnvironmentDefinition,
        i64,
        ProjectEnvironmentMatchKind,
        String,
    )> = None;

    for project in projects {
        if let Some(candidate) = score_project_environment_match(
            &project,
            group_folder,
            &normalized_requested,
            &normalized_resolved,
        ) {
            if best
                .as_ref()
                .map(|(_, score, _, _)| candidate.1 > *score)
                .unwrap_or(true)
            {
                best = Some((project, candidate.1, candidate.0, candidate.2));
            }
        }
    }

    Ok(best.map(
        |(project, _, matched_by, matched_value)| ResolvedProjectEnvironment {
            project,
            matched_by,
            matched_value,
        },
    ))
}

pub fn build_project_environment_runtime_state(
    project: &ProjectEnvironmentDefinition,
    runtime: ProjectRuntime,
    include_secret_env: bool,
) -> Result<ProjectEnvironmentRuntimeState> {
    let runtime_config = runtime_config(project, runtime);
    let mut merged = BTreeMap::new();
    let mut blocked_secret_env_keys = BTreeSet::new();
    let mut secret_handles = BTreeSet::new();

    for env_file in &project.runtime.env_files {
        apply_env_file(
            env_file,
            false,
            include_secret_env,
            &mut merged,
            &mut blocked_secret_env_keys,
            &mut secret_handles,
        )?;
    }
    apply_env_record(
        &project.runtime.env,
        false,
        include_secret_env,
        &mut merged,
        &mut blocked_secret_env_keys,
        &mut secret_handles,
    );
    apply_env_record(
        &project.runtime.secret_env,
        true,
        include_secret_env,
        &mut merged,
        &mut blocked_secret_env_keys,
        &mut secret_handles,
    );
    for env_file in &project.runtime.secret_env_files {
        apply_env_file(
            env_file,
            true,
            include_secret_env,
            &mut merged,
            &mut blocked_secret_env_keys,
            &mut secret_handles,
        )?;
    }

    if let Some(config) = runtime_config {
        for env_file in runtime_env_files(config) {
            apply_env_file(
                env_file,
                false,
                include_secret_env,
                &mut merged,
                &mut blocked_secret_env_keys,
                &mut secret_handles,
            )?;
        }
        apply_env_record(
            runtime_env(config),
            false,
            include_secret_env,
            &mut merged,
            &mut blocked_secret_env_keys,
            &mut secret_handles,
        );
        apply_env_record(
            runtime_secret_env(config),
            true,
            include_secret_env,
            &mut merged,
            &mut blocked_secret_env_keys,
            &mut secret_handles,
        );
        for env_file in runtime_secret_env_files(config) {
            apply_env_file(
                env_file,
                true,
                include_secret_env,
                &mut merged,
                &mut blocked_secret_env_keys,
                &mut secret_handles,
            )?;
        }
    }

    for handle in &project.runtime.secret_handles {
        let handle = handle.trim();
        if !handle.is_empty() {
            secret_handles.insert(handle.to_string());
        }
    }
    if let Some(config) = runtime_config {
        for handle in runtime_secret_handles(config) {
            let handle = handle.trim();
            if !handle.is_empty() {
                secret_handles.insert(handle.to_string());
            }
        }
    }

    for key in &project.runtime.unset {
        merged.remove(key);
        blocked_secret_env_keys.remove(key);
    }
    if let Some(config) = runtime_config {
        for key in runtime_unset(config) {
            merged.remove(key);
            blocked_secret_env_keys.remove(key);
        }
    }

    Ok(ProjectEnvironmentRuntimeState {
        env: merged.clone(),
        secret_handles: secret_handles.into_iter().collect(),
        applied_env_keys: merged.keys().cloned().collect(),
        blocked_secret_env_keys: blocked_secret_env_keys.into_iter().collect(),
    })
}

fn apply_env_file(
    env_file: &str,
    secret_hint: bool,
    include_secret_env: bool,
    merged: &mut BTreeMap<String, String>,
    blocked_secret_env_keys: &mut BTreeSet<String>,
    secret_handles: &mut BTreeSet<String>,
) -> Result<()> {
    let normalized_path = normalize_absolute_path(env_file);
    let is_secret = secret_hint || is_likely_secret_env_file(&normalized_path);
    if is_secret {
        secret_handles.insert(format!("env-file:{}", normalized_path.display()));
    }
    let file_env = read_env_file_at_path(&normalized_path)?;
    apply_env_record(
        &file_env,
        is_secret,
        include_secret_env,
        merged,
        blocked_secret_env_keys,
        secret_handles,
    );
    Ok(())
}

fn apply_env_record(
    env_record: &BTreeMap<String, String>,
    secret: bool,
    include_secret_env: bool,
    merged: &mut BTreeMap<String, String>,
    blocked_secret_env_keys: &mut BTreeSet<String>,
    secret_handles: &mut BTreeSet<String>,
) {
    for (key, value) in env_record {
        let is_secret = secret || is_likely_secret_env_key(key);
        if is_secret {
            blocked_secret_env_keys.insert(key.clone());
            secret_handles.insert(format!("env-key:{key}"));
            if !include_secret_env {
                merged.remove(key);
                continue;
            }
        }
        merged.insert(key.clone(), value.clone());
    }
}

pub fn list_project_environment_unset_keys(
    project: &ProjectEnvironmentDefinition,
    runtime: ProjectRuntime,
) -> Vec<String> {
    let mut unset = BTreeSet::new();
    for key in &project.runtime.unset {
        unset.insert(key.clone());
    }
    if let Some(config) = runtime_config(project, runtime) {
        for key in runtime_unset(config) {
            unset.insert(key.clone());
        }
    }
    unset.into_iter().collect()
}

pub fn should_inject_project_environment_secrets(
    project: &ProjectEnvironmentDefinition,
    runtime: ProjectRuntime,
) -> bool {
    project.runtime.allow_secret_env_injection
        || runtime_config(project, runtime)
            .map(runtime_allow_secret_env_injection)
            .unwrap_or(false)
}

fn runtime_config<'a>(
    project: &'a ProjectEnvironmentDefinition,
    runtime: ProjectRuntime,
) -> Option<RuntimeConfigRef<'a>> {
    match runtime {
        ProjectRuntime::Codex => project.codex.as_ref().map(RuntimeConfigRef::Codex),
        ProjectRuntime::Claude => project.claude.as_ref().map(RuntimeConfigRef::Claude),
    }
}

fn runtime_env(config: RuntimeConfigRef<'_>) -> &BTreeMap<String, String> {
    match config {
        RuntimeConfigRef::Codex(config) => &config.runtime.env,
        RuntimeConfigRef::Claude(config) => &config.env,
    }
}

fn runtime_env_files(config: RuntimeConfigRef<'_>) -> &[String] {
    match config {
        RuntimeConfigRef::Codex(config) => &config.runtime.env_files,
        RuntimeConfigRef::Claude(config) => &config.env_files,
    }
}

fn runtime_secret_env(config: RuntimeConfigRef<'_>) -> &BTreeMap<String, String> {
    match config {
        RuntimeConfigRef::Codex(config) => &config.runtime.secret_env,
        RuntimeConfigRef::Claude(config) => &config.secret_env,
    }
}

fn runtime_secret_env_files(config: RuntimeConfigRef<'_>) -> &[String] {
    match config {
        RuntimeConfigRef::Codex(config) => &config.runtime.secret_env_files,
        RuntimeConfigRef::Claude(config) => &config.secret_env_files,
    }
}

fn runtime_secret_handles(config: RuntimeConfigRef<'_>) -> &[String] {
    match config {
        RuntimeConfigRef::Codex(config) => &config.runtime.secret_handles,
        RuntimeConfigRef::Claude(config) => &config.secret_handles,
    }
}

fn runtime_unset(config: RuntimeConfigRef<'_>) -> &[String] {
    match config {
        RuntimeConfigRef::Codex(config) => &config.runtime.unset,
        RuntimeConfigRef::Claude(config) => &config.unset,
    }
}

fn runtime_allow_secret_env_injection(config: RuntimeConfigRef<'_>) -> bool {
    match config {
        RuntimeConfigRef::Codex(config) => config.runtime.allow_secret_env_injection,
        RuntimeConfigRef::Claude(config) => config.allow_secret_env_injection,
    }
}

#[derive(Clone, Copy)]
pub enum RuntimeConfigRef<'a> {
    Codex(&'a ProjectEnvironmentCodexConfig),
    Claude(&'a ProjectEnvironmentRuntimeConfig),
}

fn score_project_environment_match(
    project: &ProjectEnvironmentDefinition,
    group_folder: &str,
    requested_paths: &BTreeSet<String>,
    resolved_paths: &BTreeSet<PathBuf>,
) -> Option<(ProjectEnvironmentMatchKind, i64, String)> {
    let mut best: Option<(ProjectEnvironmentMatchKind, i64, String)> = None;

    for candidate in &project.match_rules.group_folders {
        let normalized = candidate.trim();
        if !normalized.is_empty() && !group_folder.is_empty() && normalized == group_folder {
            let score = 20_000 + normalized.len() as i64;
            best = pick_best(
                best,
                (
                    ProjectEnvironmentMatchKind::GroupFolder,
                    score,
                    normalized.to_string(),
                ),
            );
        }
    }

    for pattern in &project.match_rules.requested_paths {
        let normalized_pattern = normalize_requested_path(pattern);
        if normalized_pattern.is_empty() {
            continue;
        }
        for candidate in requested_paths {
            if requested_path_within(&normalized_pattern, candidate) {
                let score = 40_000 + normalized_pattern.len() as i64;
                best = pick_best(
                    best,
                    (
                        ProjectEnvironmentMatchKind::RequestedPath,
                        score,
                        normalized_pattern.clone(),
                    ),
                );
            }
        }
    }

    for pattern in &project.match_rules.paths {
        let normalized_pattern = normalize_absolute_path(pattern);
        for candidate in resolved_paths {
            if path_within(&normalized_pattern, candidate) {
                let score = 50_000 + normalized_pattern.display().to_string().len() as i64;
                best = pick_best(
                    best,
                    (
                        ProjectEnvironmentMatchKind::ResolvedPath,
                        score,
                        normalized_pattern.display().to_string(),
                    ),
                );
            } else if path_within(candidate, &normalized_pattern) {
                let score = 30_000 + normalized_pattern.display().to_string().len() as i64;
                best = pick_best(
                    best,
                    (
                        ProjectEnvironmentMatchKind::ResolvedPath,
                        score,
                        normalized_pattern.display().to_string(),
                    ),
                );
            }
        }
    }

    best
}

fn pick_best(
    current: Option<(ProjectEnvironmentMatchKind, i64, String)>,
    next: (ProjectEnvironmentMatchKind, i64, String),
) -> Option<(ProjectEnvironmentMatchKind, i64, String)> {
    match current {
        Some(current) if current.1 >= next.1 => Some(current),
        _ => Some(next),
    }
}

fn normalize_absolute_path(value: &str) -> PathBuf {
    let expanded = expand_home(value.trim());
    let resolved = if expanded.is_absolute() {
        expanded
    } else {
        env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join(expanded)
    };
    fs::canonicalize(&resolved).unwrap_or(resolved)
}

fn normalize_requested_path(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return String::new();
    }
    if let Some(host_path) = trimmed.strip_prefix("host:") {
        return format!("host:{}", normalize_absolute_path(host_path).display());
    }
    normalize_posix_like(trimmed)
}

fn normalize_posix_like(value: &str) -> String {
    let mut parts = Vec::new();
    let absolute = value.starts_with('/');
    for component in Path::new(value).components() {
        match component {
            Component::Normal(part) => parts.push(part.to_string_lossy().to_string()),
            Component::ParentDir => {
                parts.pop();
            }
            Component::RootDir => {}
            Component::CurDir => {}
            _ => {}
        }
    }
    let normalized = parts.join("/");
    if absolute {
        format!("/{}", normalized)
    } else if normalized.is_empty() {
        ".".to_string()
    } else {
        normalized
    }
}

fn parse_env_content(content: &str) -> BTreeMap<String, String> {
    let mut parsed = BTreeMap::new();
    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let candidate = trimmed.strip_prefix("export ").unwrap_or(trimmed).trim();
        let Some((key, value)) = candidate.split_once('=') else {
            continue;
        };
        let value = value.trim();
        let unquoted = if value.len() >= 2
            && ((value.starts_with('"') && value.ends_with('"'))
                || (value.starts_with('\'') && value.ends_with('\'')))
        {
            &value[1..value.len() - 1]
        } else {
            value
        };
        parsed.insert(key.trim().to_string(), unquoted.to_string());
    }
    parsed
}

fn read_env_file_at_path(path: &Path) -> Result<BTreeMap<String, String>> {
    let content = fs::read_to_string(path)
        .with_context(|| format!("failed to read project environment file {}", path.display()))?;
    Ok(parse_env_content(&content))
}

fn is_likely_secret_env_file(path: &Path) -> bool {
    let basename = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or_default()
        .to_ascii_lowercase();
    basename == ".env"
        || basename.starts_with(".env.")
        || basename == ".dev.vars"
        || basename.contains("secret")
        || basename.contains("credential")
}

fn is_likely_secret_env_key(key: &str) -> bool {
    let normalized = key.to_ascii_lowercase();
    [
        "token",
        "secret",
        "password",
        "passwd",
        "api_key",
        "auth",
        "cookie",
        "private",
        "client_secret",
        "signing_key",
    ]
    .iter()
    .any(|needle| normalized.contains(needle))
}

fn path_within(base_dir: &Path, candidate_path: &Path) -> bool {
    let relative = candidate_path.strip_prefix(base_dir);
    relative.is_ok() || base_dir == candidate_path
}

fn requested_path_within(pattern: &str, candidate: &str) -> bool {
    candidate == pattern || candidate.starts_with(&format!("{pattern}/"))
}

fn expand_home(value: &str) -> PathBuf {
    if value == "~" {
        return env::var("HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from(value));
    }
    if let Some(stripped) = value.strip_prefix("~/") {
        return env::var("HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("~"))
            .join(stripped);
    }
    PathBuf::from(value)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;

    use super::{
        build_project_environment_runtime_state, resolve_project_environment,
        should_inject_project_environment_secrets, ProjectEnvironmentMatchKind,
        ProjectEnvironmentResolutionInput, ProjectRuntime,
    };

    #[test]
    fn resolves_project_environment_by_group_folder() {
        let temp = tempdir().unwrap();
        let registry = temp.path().join("project-environments.json");
        fs::write(
            &registry,
            r#"{
              "projects": [
                {
                  "id": "pm",
                  "match": { "groupFolders": ["slack_linear-issues"] },
                  "codex": { "cwd": "/srv/code-mirror/project" }
                }
              ]
            }"#,
        )
        .unwrap();

        let resolved = resolve_project_environment(
            &registry,
            ProjectEnvironmentResolutionInput {
                explicit_id: None,
                requested_paths: Vec::new(),
                resolved_paths: Vec::new(),
                group_folder: Some("slack_linear-issues".to_string()),
            },
        )
        .unwrap()
        .unwrap();

        assert_eq!(resolved.project.id, "pm");
        assert_eq!(
            resolved.matched_by,
            ProjectEnvironmentMatchKind::GroupFolder
        );
    }

    #[test]
    fn blocks_secret_env_by_default() {
        let temp = tempdir().unwrap();
        let env_file = temp.path().join("service.env");
        fs::write(&env_file, "VISIBLE=value\n").unwrap();
        let registry = temp.path().join("project-environments.json");
        fs::write(
            &registry,
            format!(
                r#"{{
                  "projects": [
                    {{
                      "id": "pm",
                      "match": {{"groupFolders": ["slack_linear-issues"]}},
                      "codex": {{
                        "envFiles": ["{}"],
                        "secretEnv": {{"API_KEY": "secret"}}
                      }}
                    }}
                  ]
                }}"#,
                env_file.display()
            ),
        )
        .unwrap();
        let resolved = resolve_project_environment(
            &registry,
            ProjectEnvironmentResolutionInput {
                explicit_id: Some("pm".to_string()),
                requested_paths: Vec::new(),
                resolved_paths: Vec::new(),
                group_folder: None,
            },
        )
        .unwrap()
        .unwrap();
        let state = build_project_environment_runtime_state(
            &resolved.project,
            ProjectRuntime::Codex,
            false,
        )
        .unwrap();

        assert!(!state.env.contains_key("API_KEY"));
        assert_eq!(state.env.get("VISIBLE").map(String::as_str), Some("value"));
        assert!(!should_inject_project_environment_secrets(
            &resolved.project,
            ProjectRuntime::Codex
        ));
    }
}
