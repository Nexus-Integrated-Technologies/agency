use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use anyhow::{bail, Context, Result};

use crate::foundation::DevelopmentEnvironmentProvider;
use crate::foundation::{DevelopmentEnvironment, SshEndpoint};

use super::config::NanoclawConfig;

const COMMON_SSH_PATHS: &[&str] = &["/usr/bin/ssh", "/opt/homebrew/bin/ssh"];
const COMMON_RSYNC_PATHS: &[&str] = &[
    "/opt/homebrew/bin/rsync",
    "/usr/local/bin/rsync",
    "/usr/bin/rsync",
];
const SSH_CONTROL_PATH: &str = "/tmp/nanoclaw-ssh-%C";
const PREPARE_DEV_ENVIRONMENT_SCRIPT: &str = r#"
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y build-essential pkg-config libssl-dev git rsync curl ca-certificates
if ! command -v cargo >/dev/null 2>&1; then
  curl https://sh.rustup.rs -sSf | sh -s -- -y --profile minimal
fi
. "$HOME/.cargo/env"
rustup default stable >/dev/null 2>&1 || true
printf "cargo=%s\n" "$(cargo --version)"
printf "rustc=%s\n" "$(rustc --version)"
printf "git=%s\n" "$(git --version)"
printf "rsync=%s\n" "$(rsync --version | head -n1)"
"#;
const COMMON_REMOTE_SYNC_EXCLUDES: &[&str] = &[
    ".git",
    "node_modules",
    "target",
    "dist",
    "build",
    "coverage",
    ".next",
    ".nuxt",
    ".svelte-kit",
    ".turbo",
    ".cache",
    ".parcel-cache",
    ".wrangler",
    ".venv",
    "venv",
    "__pycache__",
    "*.pyc",
    "*.pyo",
    "*.log",
];
const PROJECT_SYNC_EXCLUDES: &[&str] = &[
    ".env",
    "data",
    "groups",
    "logs",
    "store",
    "repo-tokens",
    ".DS_Store",
    ".tmp-apple-container",
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandPlan {
    pub command: String,
    pub args: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncResult {
    pub local_source: String,
    pub remote_target: String,
    pub command: String,
    pub args: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecResult {
    pub remote_target: String,
    pub remote_repo_path: String,
    pub command: String,
    pub args: Vec<String>,
    pub stdout: String,
}

#[derive(Debug, Clone)]
pub struct DigitalOceanDevEnvironment {
    environment: DevelopmentEnvironment,
    project_root: PathBuf,
}

impl DigitalOceanDevEnvironment {
    pub fn from_config(config: &NanoclawConfig) -> Self {
        Self {
            environment: config.development_environment.clone(),
            project_root: config.project_root.clone(),
        }
    }

    pub fn remote_repo_path(&self) -> Result<String> {
        let repo_root = self
            .environment
            .repo_root
            .as_deref()
            .context("missing droplet repo root")?;
        let project_name = resolve_project_slug(&self.project_root)?;
        Ok(format!("{repo_root}/{project_name}"))
    }

    pub fn environment(&self) -> &DevelopmentEnvironment {
        &self.environment
    }

    pub fn sync_project(&self) -> Result<SyncResult> {
        let remote_repo_path = self.remote_repo_path()?;
        self.sync_git_metadata(&remote_repo_path)?;
        let mut excludes =
            Vec::with_capacity(PROJECT_SYNC_EXCLUDES.len() + COMMON_REMOTE_SYNC_EXCLUDES.len());
        for exclude in PROJECT_SYNC_EXCLUDES {
            excludes.push(*exclude);
        }
        for exclude in COMMON_REMOTE_SYNC_EXCLUDES {
            excludes.push(*exclude);
        }

        self.sync_directory(&self.project_root, &remote_repo_path, true, &excludes)
    }

    pub fn seed_cargo_cache(&self) -> Result<Vec<SyncResult>> {
        let cargo_home = resolve_local_cargo_home();
        let ssh = self.ssh_endpoint()?;
        let remote_cargo_home = resolve_remote_cargo_home(&ssh);
        let mut results = Vec::new();

        for subdirectory in ["registry", "git"] {
            let local_source = cargo_home.join(subdirectory);
            if !local_source.exists() {
                continue;
            }

            results.push(self.sync_directory(
                &local_source,
                &format!("{remote_cargo_home}/{subdirectory}"),
                false,
                &[],
            )?);
        }

        if results.is_empty() {
            bail!(
                "local Cargo cache was not found at {}",
                cargo_home.display()
            );
        }

        Ok(results)
    }

    pub fn exec(&self, command: &str) -> Result<ExecResult> {
        self.exec_with_stdin(command, &[])
    }

    pub fn exec_with_stdin(&self, command: &str, stdin: &[u8]) -> Result<ExecResult> {
        let ssh = self.ssh_endpoint()?;
        let remote_repo_path = self.remote_repo_path()?;
        let remote_command = format!(
            "cd {} && {}",
            shell_quote(&remote_repo_path),
            command.trim()
        );
        let plan = build_ssh_plan(&ssh, &remote_command);
        let mut process = Command::new(&plan.command)
            .args(&plan.args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .with_context(|| format!("failed to start {}", plan.command))?;
        if !stdin.is_empty() {
            process
                .stdin
                .as_mut()
                .context("DigitalOcean VM command stdin was not available")?
                .write_all(stdin)
                .with_context(|| format!("failed to write stdin to {}", plan.command))?;
        }
        let output = process
            .wait_with_output()
            .with_context(|| format!("failed to wait for {}", plan.command))?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
            bail!(
                "DigitalOcean VM command failed with status {}{}",
                output.status,
                if stderr.is_empty() {
                    String::new()
                } else {
                    format!(": {stderr}")
                }
            );
        }

        Ok(ExecResult {
            remote_target: build_ssh_target(&ssh),
            remote_repo_path,
            command: plan.command,
            args: plan.args,
            stdout: String::from_utf8_lossy(&output.stdout).to_string(),
        })
    }

    pub fn sync_group_workspace(
        &self,
        local_group_root: &Path,
        group_folder: &str,
    ) -> Result<SyncResult> {
        let remote_group_root = self.remote_group_workspace_path(group_folder)?;
        self.sync_directory(local_group_root, &remote_group_root, true, &[])
    }

    pub fn remote_group_workspace_path(&self, group_folder: &str) -> Result<String> {
        let remote_worker_root = self
            .environment
            .remote_worker_root
            .as_deref()
            .context("missing remote worker root")?;
        Ok(format!("{remote_worker_root}/groups/{group_folder}"))
    }

    pub fn prepare_dev_environment(&self) -> Result<ExecResult> {
        let ssh = self.ssh_endpoint()?;
        let remote_repo_path = self.remote_repo_path()?;
        ensure_remote_directory(&ssh, &remote_repo_path)?;
        self.exec(PREPARE_DEV_ENVIRONMENT_SCRIPT)
    }

    fn ssh_endpoint(&self) -> Result<SshEndpoint> {
        self.environment
            .ssh
            .clone()
            .context("DigitalOcean VM SSH configuration is missing")
    }

    fn sync_git_metadata(&self, remote_repo_path: &str) -> Result<Option<SyncResult>> {
        let local_git_dir = self.project_root.join(".git");
        if !local_git_dir.is_dir() {
            return Ok(None);
        }

        self.sync_directory(
            &local_git_dir,
            &format!("{remote_repo_path}/.git"),
            true,
            &[],
        )
        .map(Some)
    }

    fn sync_directory(
        &self,
        local_source: &Path,
        remote_path: &str,
        delete: bool,
        excludes: &[&str],
    ) -> Result<SyncResult> {
        let ssh = self.ssh_endpoint()?;
        ensure_remote_directory(&ssh, remote_path)?;

        let rsync_binary = resolve_binary("RSYNC_BIN", "rsync", COMMON_RSYNC_PATHS);
        let ssh_transport = build_ssh_transport_command(&ssh);
        let remote_target = format!("{}:{}/", build_ssh_target(&ssh), remote_path);
        let mut args = vec![
            "-az".to_string(),
            "--no-owner".to_string(),
            "--no-group".to_string(),
        ];
        if delete {
            args.push("--delete".to_string());
        }
        args.push("-e".to_string());
        args.push(ssh_transport);
        for exclude in excludes {
            args.push("--exclude".to_string());
            args.push((*exclude).to_string());
        }
        args.push(format!("{}/", local_source.display()));
        args.push(remote_target.clone());

        let status = Command::new(&rsync_binary)
            .args(&args)
            .status()
            .with_context(|| format!("failed to start {}", rsync_binary))?;
        if !status.success() {
            bail!("rsync to DigitalOcean VM failed with status {status}");
        }

        normalize_remote_ownership(&ssh, remote_path)?;

        Ok(SyncResult {
            local_source: local_source.display().to_string(),
            remote_target,
            command: rsync_binary,
            args,
        })
    }
}

impl DevelopmentEnvironmentProvider for DigitalOceanDevEnvironment {
    fn development_environment(&self) -> &DevelopmentEnvironment {
        &self.environment
    }
}

fn resolve_binary(env_name: &str, fallback: &str, candidates: &[&str]) -> String {
    if let Some(value) = std::env::var_os(env_name).filter(|value| !value.is_empty()) {
        return value.to_string_lossy().to_string();
    }

    for candidate in candidates {
        if Path::new(candidate).exists() {
            return (*candidate).to_string();
        }
    }

    fallback.to_string()
}

fn resolve_local_cargo_home() -> PathBuf {
    std::env::var_os("CARGO_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|value| PathBuf::from(value).join(".cargo")))
        .unwrap_or_else(|| PathBuf::from(".cargo"))
}

fn build_ssh_target(ssh: &SshEndpoint) -> String {
    if let Some(user) = ssh.user.as_deref().filter(|value| !value.trim().is_empty()) {
        format!("{}@{}", user.trim(), ssh.host)
    } else {
        ssh.host.clone()
    }
}

fn resolve_remote_cargo_home(ssh: &SshEndpoint) -> String {
    match ssh.user.as_deref().map(str::trim) {
        Some("root") => "/root/.cargo".to_string(),
        Some(user) if !user.is_empty() => format!("/home/{user}/.cargo"),
        _ => "/root/.cargo".to_string(),
    }
}

fn build_ssh_plan(ssh: &SshEndpoint, command: &str) -> CommandPlan {
    let ssh_binary = resolve_binary("SSH_BIN", "ssh", COMMON_SSH_PATHS);
    let mut args = vec![
        "-o".to_string(),
        "BatchMode=yes".to_string(),
        "-o".to_string(),
        "ConnectTimeout=15".to_string(),
        "-p".to_string(),
        ssh.port.to_string(),
    ];
    if ssh_multiplexing_enabled() {
        args.push("-o".to_string());
        args.push("ControlMaster=auto".to_string());
        args.push("-o".to_string());
        args.push("ControlPersist=60".to_string());
        args.push("-o".to_string());
        args.push(format!("ControlPath={SSH_CONTROL_PATH}"));
    }
    if let Some(user) = ssh.user.as_deref().filter(|value| !value.trim().is_empty()) {
        args.push("-l".to_string());
        args.push(user.trim().to_string());
    }
    args.push(ssh.host.clone());
    args.push(wrap_remote_script(command));

    CommandPlan {
        command: ssh_binary,
        args,
    }
}

fn ssh_multiplexing_enabled() -> bool {
    std::env::var("NANOCLAW_SSH_MULTIPLEXING")
        .ok()
        .map(|value| {
            matches!(
                value.trim(),
                "1" | "true" | "TRUE" | "yes" | "YES" | "on" | "ON"
            )
        })
        .unwrap_or(false)
}

fn build_ssh_transport_command(ssh: &SshEndpoint) -> String {
    let plan = build_ssh_plan(ssh, "true");
    let transport_args = &plan.args[..plan.args.len().saturating_sub(2)];
    let mut parts = Vec::with_capacity(transport_args.len() + 1);
    parts.push(shell_quote(&plan.command));
    for arg in transport_args {
        parts.push(shell_quote(arg));
    }
    parts.join(" ")
}

fn ensure_remote_directory(ssh: &SshEndpoint, remote_repo_path: &str) -> Result<()> {
    let remote_command = format!("mkdir -p {}", shell_quote(remote_repo_path));
    let plan = build_ssh_plan(ssh, &remote_command);
    let status = Command::new(&plan.command)
        .args(&plan.args)
        .status()
        .with_context(|| format!("failed to start {}", plan.command))?;
    if !status.success() {
        bail!("failed to prepare remote repository directory on DigitalOcean VM");
    }
    Ok(())
}

fn normalize_remote_ownership(ssh: &SshEndpoint, remote_path: &str) -> Result<()> {
    if !matches!(ssh.user.as_deref().map(str::trim), Some("root")) {
        return Ok(());
    }

    let remote_command = format!(
        "if [ -e {path} ]; then chown -R root:root {path}; fi",
        path = shell_quote(remote_path)
    );
    let plan = build_ssh_plan(ssh, &remote_command);
    let status = Command::new(&plan.command)
        .args(&plan.args)
        .status()
        .with_context(|| format!("failed to start {}", plan.command))?;
    if !status.success() {
        bail!("failed to normalize ownership on DigitalOcean VM");
    }

    Ok(())
}

fn resolve_project_slug(project_root: &Path) -> Result<String> {
    if let Some(slug) = resolve_git_remote_slug(project_root)? {
        return Ok(slug);
    }

    let name = project_root
        .file_name()
        .and_then(|value| value.to_str())
        .filter(|value| !value.trim().is_empty())
        .context("failed to resolve project name for remote repo path")?;
    Ok(format!("local/{}", name.trim()))
}

fn resolve_git_remote_slug(project_root: &Path) -> Result<Option<String>> {
    let output = Command::new("git")
        .arg("config")
        .arg("--get")
        .arg("remote.origin.url")
        .current_dir(project_root)
        .output();

    let Ok(output) = output else {
        return Ok(None);
    };
    if !output.status.success() {
        return Ok(None);
    }

    let remote = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if remote.is_empty() {
        return Ok(None);
    }

    if let Some(path) = remote
        .strip_prefix("https://github.com/")
        .or_else(|| remote.strip_prefix("http://github.com/"))
        .or_else(|| remote.strip_prefix("git@github.com:"))
        .or_else(|| remote.strip_prefix("ssh://git@github.com/"))
    {
        return Ok(Some(path.trim_end_matches(".git").to_string()));
    }

    Ok(None)
}

fn wrap_remote_script(script: &str) -> String {
    format!("/bin/bash -lc {}", shell_quote(script))
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

#[cfg(test)]
mod tests {
    use super::{build_ssh_plan, resolve_git_remote_slug};
    use crate::foundation::SshEndpoint;
    use anyhow::Result;
    use std::process::Command;
    use tempfile::tempdir;

    #[test]
    fn builds_non_interactive_ssh_plan() {
        let plan = build_ssh_plan(
            &SshEndpoint {
                host: "example.com".to_string(),
                user: Some("root".to_string()),
                port: 22,
            },
            "pwd",
        );

        assert_eq!(plan.command, "/usr/bin/ssh");
        assert!(plan.args.iter().any(|arg| arg == "BatchMode=yes"));
        assert!(!plan.args.iter().any(|arg| arg == "ControlMaster=auto"));
        assert!(plan.args.iter().any(|arg| arg == "example.com"));
    }

    #[test]
    fn resolves_github_remote_slug() -> Result<()> {
        let temp = tempdir()?;
        Command::new("git")
            .arg("init")
            .current_dir(temp.path())
            .status()?;
        Command::new("git")
            .args([
                "remote",
                "add",
                "origin",
                "git@github.com:nexus-integrated-technologies/agency.git",
            ])
            .current_dir(temp.path())
            .status()?;

        assert_eq!(
            resolve_git_remote_slug(temp.path())?,
            Some("nexus-integrated-technologies/agency".to_string())
        );

        Ok(())
    }
}
