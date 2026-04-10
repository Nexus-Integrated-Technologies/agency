use std::process::Command;

use anyhow::{bail, Context, Result};
use serde::Serialize;

use crate::foundation::{ExecutionLocation, SshEndpoint};

use super::config::NanoclawConfig;
use super::host_os_control::{build_default_context, HostOsControlContext};

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct RemoteControlStatus {
    pub configured: bool,
    pub reachable: bool,
    pub target: Option<String>,
    pub workspace_root: Option<String>,
    pub error: Option<String>,
}

pub fn remote_control_target(config: &NanoclawConfig) -> Option<SshEndpoint> {
    let host = config
        .remote_control_ssh_host
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())?;
    Some(SshEndpoint {
        host: host.to_string(),
        user: config
            .remote_control_ssh_user
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string),
        port: config.remote_control_ssh_port,
    })
}

pub fn build_remote_control_context(
    source_group: impl Into<String>,
    config: &NanoclawConfig,
) -> Result<HostOsControlContext> {
    let ssh_target = remote_control_target(config)
        .context("remote control target is not configured; set NANOCLAW_REMOTE_CONTROL_SSH_HOST")?;
    let mut context = build_default_context(source_group, &config.host_os_control_policy_path);
    context.chat_jid = config.host_os_approval_chat_jid.clone();
    context.execution_location = ExecutionLocation::Custom("remote_control".to_string());
    context.ssh_target = Some(ssh_target);
    Ok(context)
}

pub fn describe_remote_control(config: &NanoclawConfig) -> RemoteControlStatus {
    let Some(ssh) = remote_control_target(config) else {
        return RemoteControlStatus {
            configured: false,
            reachable: false,
            target: None,
            workspace_root: config.remote_control_workspace_root.clone(),
            error: Some(
                "remote control target is not configured; set NANOCLAW_REMOTE_CONTROL_SSH_HOST"
                    .to_string(),
            ),
        };
    };

    match probe_remote_control_target(&ssh) {
        Ok(_) => RemoteControlStatus {
            configured: true,
            reachable: true,
            target: Some(ssh_target_display(&ssh)),
            workspace_root: config.remote_control_workspace_root.clone(),
            error: None,
        },
        Err(error) => RemoteControlStatus {
            configured: true,
            reachable: false,
            target: Some(ssh_target_display(&ssh)),
            workspace_root: config.remote_control_workspace_root.clone(),
            error: Some(error.to_string()),
        },
    }
}

fn probe_remote_control_target(ssh: &SshEndpoint) -> Result<()> {
    run_remote_command(
        ssh,
        "printf 'host=%s\\nuser=%s\\nos=%s\\n' \"$(hostname)\" \"$(id -un)\" \"$(uname -s)\"",
    )
    .map(|_| ())
}

pub(crate) fn run_remote_command(ssh: &SshEndpoint, command: &str) -> Result<String> {
    let mut args = vec![
        "-o".to_string(),
        "BatchMode=yes".to_string(),
        "-o".to_string(),
        "ConnectTimeout=15".to_string(),
        "-p".to_string(),
        ssh.port.to_string(),
    ];
    if let Some(user) = ssh.user.as_deref().filter(|value| !value.trim().is_empty()) {
        args.push("-l".to_string());
        args.push(user.trim().to_string());
    }
    args.push(ssh.host.clone());
    args.push(format!("/bin/bash -lc {}", shell_quote(command)));

    let output = Command::new("ssh")
        .args(&args)
        .output()
        .with_context(|| format!("failed to execute ssh {}", ssh_target_display(ssh)))?;
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    if !output.status.success() {
        bail!(
            "remote control command failed on {} with status {}{}",
            ssh_target_display(ssh),
            output.status,
            if stderr.is_empty() {
                String::new()
            } else {
                format!(": {stderr}")
            }
        );
    }
    Ok(if stdout.is_empty() {
        "ok".to_string()
    } else {
        stdout
    })
}

pub(crate) fn ssh_target_display(ssh: &SshEndpoint) -> String {
    let base = if let Some(user) = ssh.user.as_deref().filter(|value| !value.trim().is_empty()) {
        format!("{}@{}", user.trim(), ssh.host)
    } else {
        ssh.host.clone()
    };
    format!("{}:{}", base, ssh.port)
}

pub(crate) fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::foundation::{
        DevelopmentEnvironment, DevelopmentEnvironmentKind, ExecutionLane, RemoteWorkerMode,
        SshEndpoint,
    };

    use super::{describe_remote_control, remote_control_target};
    use crate::nanoclaw::config::NanoclawConfig;

    fn test_config() -> NanoclawConfig {
        let project_root = PathBuf::from("/tmp/nanoclaw");
        NanoclawConfig {
            project_root: project_root.clone(),
            data_dir: project_root.join("data"),
            groups_dir: project_root.join("groups"),
            store_dir: project_root.join("store"),
            db_path: project_root.join("store/messages.db"),
            assistant_name: "Andy".to_string(),
            default_trigger: "@Andy".to_string(),
            timezone: "UTC".to_string(),
            max_concurrent_groups: 2,
            execution_lane: ExecutionLane::Auto,
            container_image: "rust:1.75-slim".to_string(),
            container_runtime: None,
            container_groups: Vec::new(),
            droplet_ssh_host: "127.0.0.1".to_string(),
            droplet_ssh_user: "root".to_string(),
            droplet_ssh_port: 22,
            droplet_repo_root: "/srv/code-mirror".to_string(),
            remote_worker_mode: RemoteWorkerMode::Off,
            remote_worker_root: "/root/.nanoclaw-worker".to_string(),
            remote_worker_binary: "/usr/local/bin/nanoclaw-rs".to_string(),
            remote_worker_bootstrap_timeout_ms: 30_000,
            remote_worker_sync_interval_ms: 500,
            remote_worker_tunnel_port_base: 13_000,
            omx_runner_path: "/usr/local/bin/omx-paperclip-runner".to_string(),
            omx_state_root: "/root/.nanoclaw-omx".to_string(),
            omx_callback_url: "http://127.0.0.1:8788/webhook/omx".to_string(),
            omx_callback_token: String::new(),
            omx_default_mode: "team".to_string(),
            omx_default_max_workers: 3,
            omx_poll_interval_ms: 5_000,
            openclaw_gateway_bind_host: "127.0.0.1".to_string(),
            openclaw_gateway_public_host: "127.0.0.1".to_string(),
            openclaw_gateway_port: 0,
            openclaw_gateway_token: String::new(),
            openclaw_gateway_execution_lane: ExecutionLane::Host,
            slack_env_file: None,
            slack_poll_interval_ms: 500,
            linear_webhook_port: 0,
            linear_webhook_secret: String::new(),
            github_webhook_secret: String::new(),
            linear_chat_jid: String::new(),
            linear_api_key: String::new(),
            linear_write_api_key: String::new(),
            linear_pm_team_keys: Vec::new(),
            linear_pm_policy_interval_minutes: 60,
            linear_pm_digest_interval_hours: 6,
            linear_pm_guardrail_max_automations: 10,
            observability_chat_jid: String::new(),
            observability_group_folder: "observability".to_string(),
            observability_webhook_token: String::new(),
            observability_auto_blue_team: true,
            observability_adapters_path: project_root.join("observability-adapters.json"),
            development_environment: DevelopmentEnvironment {
                id: "digitalocean-droplet".to_string(),
                name: "DigitalOcean VM".to_string(),
                kind: DevelopmentEnvironmentKind::DigitalOceanDroplet,
                workspace_root: Some(project_root.display().to_string()),
                repo_root: Some("/srv/code-mirror".to_string()),
                ssh: Some(SshEndpoint {
                    host: "127.0.0.1".to_string(),
                    user: Some("root".to_string()),
                    port: 22,
                }),
                remote_worker_mode: RemoteWorkerMode::Off,
                remote_worker_root: Some("/root/.nanoclaw-worker".to_string()),
                bootstrap_timeout_ms: Some(30_000),
                sync_interval_ms: Some(500),
                tunnel_port_base: Some(13_000),
            },
            sender_allowlist_path: project_root.join("sender-allowlist.json"),
            project_environments_path: project_root.join("project-environments.json"),
            host_os_control_policy_path: project_root.join("host-os-policy.json"),
            host_os_approval_chat_jid: None,
            remote_control_ssh_host: None,
            remote_control_ssh_user: None,
            remote_control_ssh_port: 22,
            remote_control_workspace_root: None,
        }
    }

    #[test]
    fn reports_unconfigured_remote_control_target() {
        let status = describe_remote_control(&test_config());
        assert!(!status.configured);
        assert!(!status.reachable);
    }

    #[test]
    fn builds_remote_control_target_from_config() {
        let mut config = test_config();
        config.remote_control_ssh_host = Some("mac.example.test".to_string());
        config.remote_control_ssh_user = Some("javoerokour".to_string());
        config.remote_control_ssh_port = 2222;
        let target = remote_control_target(&config).unwrap();
        assert_eq!(target.host, "mac.example.test");
        assert_eq!(target.user.as_deref(), Some("javoerokour"));
        assert_eq!(target.port, 2222);
    }
}
