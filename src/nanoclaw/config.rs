use std::env;
use std::path::PathBuf;

use crate::foundation::{
    DevelopmentEnvironment, DevelopmentEnvironmentKind, ExecutionLane, RemoteWorkerMode,
    SshEndpoint,
};

#[derive(Debug, Clone)]
pub struct NanoclawConfig {
    pub project_root: PathBuf,
    pub data_dir: PathBuf,
    pub groups_dir: PathBuf,
    pub store_dir: PathBuf,
    pub db_path: PathBuf,
    pub assistant_name: String,
    pub default_trigger: String,
    pub timezone: String,
    pub max_concurrent_groups: usize,
    pub execution_lane: ExecutionLane,
    pub container_image: String,
    pub container_runtime: Option<String>,
    pub container_groups: Vec<String>,
    pub droplet_ssh_host: String,
    pub droplet_ssh_user: String,
    pub droplet_ssh_port: u16,
    pub droplet_repo_root: String,
    pub remote_worker_mode: RemoteWorkerMode,
    pub remote_worker_root: String,
    pub remote_worker_binary: String,
    pub remote_worker_bootstrap_timeout_ms: u64,
    pub remote_worker_sync_interval_ms: u64,
    pub remote_worker_tunnel_port_base: u16,
    pub omx_runner_path: String,
    pub omx_state_root: String,
    pub omx_callback_url: String,
    pub omx_callback_token: String,
    pub omx_default_mode: String,
    pub omx_default_max_workers: usize,
    pub omx_poll_interval_ms: u64,
    pub openclaw_gateway_bind_host: String,
    pub openclaw_gateway_public_host: String,
    pub openclaw_gateway_port: u16,
    pub openclaw_gateway_token: String,
    pub openclaw_gateway_execution_lane: ExecutionLane,
    pub slack_env_file: Option<PathBuf>,
    pub slack_poll_interval_ms: u64,
    pub linear_webhook_port: u16,
    pub linear_webhook_secret: String,
    pub github_webhook_secret: String,
    pub linear_chat_jid: String,
    pub linear_api_key: String,
    pub linear_write_api_key: String,
    pub linear_pm_team_keys: Vec<String>,
    pub linear_pm_policy_interval_minutes: u64,
    pub linear_pm_digest_interval_hours: u64,
    pub linear_pm_guardrail_max_automations: usize,
    pub observability_chat_jid: String,
    pub observability_group_folder: String,
    pub observability_webhook_token: String,
    pub observability_auto_blue_team: bool,
    pub observability_adapters_path: PathBuf,
    pub development_environment: DevelopmentEnvironment,
    pub sender_allowlist_path: PathBuf,
    pub project_environments_path: PathBuf,
    pub host_os_control_policy_path: PathBuf,
    pub host_os_approval_chat_jid: Option<String>,
    pub remote_control_ssh_host: Option<String>,
    pub remote_control_ssh_user: Option<String>,
    pub remote_control_ssh_port: u16,
    pub remote_control_workspace_root: Option<String>,
}

impl NanoclawConfig {
    pub fn from_env() -> Self {
        let project_root = env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
        let home_dir = env::var("HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|_| project_root.clone());
        let assistant_name = env::var("ASSISTANT_NAME").unwrap_or_else(|_| "Andy".to_string());
        let data_dir = project_root.join("data");
        let groups_dir = project_root.join("groups");
        let store_dir = project_root.join("store");
        let db_path = store_dir.join("messages.db");
        let timezone = env::var("TZ").unwrap_or_else(|_| "UTC".to_string());
        let max_concurrent_groups = env::var("MAX_CONCURRENT_CONTAINERS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(5);
        let execution_lane = ExecutionLane::parse(
            &env::var("NANOCLAW_EXECUTION_LANE").unwrap_or_else(|_| "auto".to_string()),
        );
        let container_image =
            env::var("NANOCLAW_CONTAINER_IMAGE").unwrap_or_else(|_| "rust:1.75-slim".to_string());
        let container_runtime = env::var("NANOCLAW_CONTAINER_RUNTIME")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        let container_groups = env::var("NANOCLAW_CONTAINER_GROUPS")
            .or_else(|_| env::var("CONTAINER_GROUPS"))
            .unwrap_or_default()
            .split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
            .collect::<Vec<_>>();
        let droplet_ssh_host = env::var("DROPLET_SSH_HOST")
            .or_else(|_| env::var("DIGITALOCEAN_HOST"))
            .unwrap_or_else(|_| "167.99.103.123".to_string());
        let droplet_ssh_user = env::var("DROPLET_SSH_USER")
            .or_else(|_| env::var("DIGITALOCEAN_SSH_USER"))
            .unwrap_or_else(|_| "root".to_string());
        let droplet_ssh_port = env::var("DROPLET_SSH_PORT")
            .or_else(|_| env::var("DIGITALOCEAN_SSH_PORT"))
            .ok()
            .and_then(|value| value.parse::<u16>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(22);
        let droplet_repo_root = env::var("DROPLET_REPO_ROOT")
            .or_else(|_| env::var("DIGITALOCEAN_REPO_ROOT"))
            .unwrap_or_else(|_| "/srv/code-mirror".to_string());
        let remote_worker_mode = RemoteWorkerMode::parse(
            &env::var("REMOTE_WORKER_MODE").unwrap_or_else(|_| "off".to_string()),
        );
        let remote_worker_root =
            env::var("REMOTE_WORKER_ROOT").unwrap_or_else(|_| "/root/.nanoclaw-worker".to_string());
        let remote_worker_binary = env::var("REMOTE_WORKER_BINARY")
            .unwrap_or_else(|_| "/usr/local/bin/nanoclaw-rs".to_string());
        let remote_worker_bootstrap_timeout_ms = env::var("REMOTE_WORKER_BOOTSTRAP_TIMEOUT_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| *value >= 1000)
            .unwrap_or(30_000);
        let remote_worker_sync_interval_ms = env::var("REMOTE_WORKER_SYNC_INTERVAL_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| *value >= 250)
            .unwrap_or(500);
        let remote_worker_tunnel_port_base = env::var("REMOTE_WORKER_TUNNEL_PORT_BASE")
            .ok()
            .and_then(|value| value.parse::<u16>().ok())
            .filter(|value| *value >= 1024)
            .unwrap_or(13_000);
        let omx_runner_path = env::var("NANOCLAW_OMX_RUNNER_PATH")
            .unwrap_or_else(|_| "/usr/local/bin/omx-paperclip-runner".to_string());
        let omx_state_root = env::var("NANOCLAW_OMX_STATE_ROOT")
            .unwrap_or_else(|_| "/root/.nanoclaw-omx".to_string());
        let omx_callback_token = env::var("NANOCLAW_OMX_CALLBACK_TOKEN").unwrap_or_default();
        let omx_default_mode =
            env::var("NANOCLAW_OMX_DEFAULT_MODE").unwrap_or_else(|_| "team".to_string());
        let omx_default_max_workers = env::var("NANOCLAW_OMX_DEFAULT_MAX_WORKERS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(3);
        let omx_poll_interval_ms = env::var("NANOCLAW_OMX_POLL_INTERVAL_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| *value >= 250)
            .unwrap_or(5_000);
        let openclaw_gateway_bind_host = env::var("NANOCLAW_OPENCLAW_GATEWAY_BIND_HOST")
            .unwrap_or_else(|_| "0.0.0.0".to_string());
        let openclaw_gateway_public_host = env::var("NANOCLAW_OPENCLAW_GATEWAY_PUBLIC_HOST")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| droplet_ssh_host.clone());
        let openclaw_gateway_port = env::var("NANOCLAW_OPENCLAW_GATEWAY_PORT")
            .ok()
            .and_then(|value| value.parse::<u16>().ok())
            .unwrap_or(0);
        let openclaw_gateway_token =
            env::var("NANOCLAW_OPENCLAW_GATEWAY_TOKEN").unwrap_or_default();
        let openclaw_gateway_execution_lane = ExecutionLane::parse(
            &env::var("NANOCLAW_OPENCLAW_GATEWAY_EXECUTION_LANE")
                .unwrap_or_else(|_| "host".to_string()),
        );
        let slack_env_file = env::var("NANOCLAW_SLACK_ENV_FILE")
            .ok()
            .map(PathBuf::from)
            .or_else(|| {
                let candidate = project_root.join(".env");
                candidate.exists().then_some(candidate)
            })
            .or_else(|| {
                project_root
                    .parent()
                    .map(|parent| parent.join("nanoclaw").join(".env"))
                    .filter(|candidate| candidate.exists())
            });
        let slack_poll_interval_ms = env::var("NANOCLAW_SLACK_POLL_INTERVAL_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| *value >= 100)
            .unwrap_or(500);
        let linear_webhook_port = env::var("LINEAR_WEBHOOK_PORT")
            .ok()
            .and_then(|value| value.parse::<u16>().ok())
            .unwrap_or(0);
        let omx_callback_url = env::var("NANOCLAW_OMX_CALLBACK_URL").unwrap_or_else(|_| {
            format!(
                "http://127.0.0.1:{}/webhook/omx",
                linear_webhook_port.max(1)
            )
        });
        let linear_webhook_secret = env::var("LINEAR_WEBHOOK_SECRET").unwrap_or_default();
        let github_webhook_secret = env::var("GITHUB_WEBHOOK_SECRET").unwrap_or_default();
        let linear_chat_jid = env::var("LINEAR_CHAT_JID").unwrap_or_default();
        let linear_api_key = env::var("LINEAR_API_KEY").unwrap_or_default();
        let linear_write_api_key = env::var("LINEAR_WRITE_API_KEY").unwrap_or_default();
        let linear_pm_team_keys = env::var("LINEAR_PM_TEAM_KEYS")
            .unwrap_or_default()
            .split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
            .collect::<Vec<_>>();
        let linear_pm_policy_interval_minutes = env::var("LINEAR_PM_POLICY_INTERVAL_MINUTES")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(60);
        let linear_pm_digest_interval_hours = env::var("LINEAR_PM_DIGEST_INTERVAL_HOURS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(6);
        let linear_pm_guardrail_max_automations = env::var("LINEAR_PM_GUARDRAIL_MAX_AUTOMATIONS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(10);
        let observability_chat_jid = env::var("OBSERVABILITY_CHAT_JID").unwrap_or_default();
        let observability_group_folder = env::var("OBSERVABILITY_GROUP_FOLDER")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| "observability".to_string());
        let observability_webhook_token =
            env::var("OBSERVABILITY_WEBHOOK_TOKEN").unwrap_or_default();
        let observability_auto_blue_team = env::var("OBSERVABILITY_AUTO_BLUE_TEAM")
            .ok()
            .map(|value| {
                !matches!(
                    value.trim().to_ascii_lowercase().as_str(),
                    "false" | "0" | "no"
                )
            })
            .unwrap_or(true);
        let observability_adapters_path = env::var("OBSERVABILITY_ADAPTERS_PATH")
            .map(PathBuf::from)
            .unwrap_or_else(|_| {
                home_dir
                    .join(".config")
                    .join("nanoclaw")
                    .join("observability-adapters.json")
            });
        let sender_allowlist_path = env::var("NANOCLAW_SENDER_ALLOWLIST_PATH")
            .map(PathBuf::from)
            .unwrap_or_else(|_| {
                home_dir
                    .join(".config")
                    .join("nanoclaw")
                    .join("sender-allowlist.json")
            });
        let project_environments_path = env::var("NANOCLAW_PROJECT_ENVIRONMENTS_PATH")
            .map(PathBuf::from)
            .unwrap_or_else(|_| {
                home_dir
                    .join(".config")
                    .join("nanoclaw")
                    .join("project-environments.json")
            });
        let host_os_control_policy_path = env::var("NANOCLAW_HOST_OS_CONTROL_POLICY_PATH")
            .map(PathBuf::from)
            .unwrap_or_else(|_| {
                home_dir
                    .join(".config")
                    .join("nanoclaw")
                    .join("host-os-control-policy.json")
            });
        let host_os_approval_chat_jid = env::var("NANOCLAW_HOST_OS_APPROVAL_CHAT_JID")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        let remote_control_ssh_host = env::var("NANOCLAW_REMOTE_CONTROL_SSH_HOST")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        let remote_control_ssh_user = env::var("NANOCLAW_REMOTE_CONTROL_SSH_USER")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        let remote_control_ssh_port = env::var("NANOCLAW_REMOTE_CONTROL_SSH_PORT")
            .ok()
            .and_then(|value| value.parse::<u16>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(22);
        let remote_control_workspace_root = env::var("NANOCLAW_REMOTE_CONTROL_WORKSPACE_ROOT")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        let development_environment = DevelopmentEnvironment {
            id: "digitalocean-droplet".to_string(),
            name: "DigitalOcean VM".to_string(),
            kind: DevelopmentEnvironmentKind::DigitalOceanDroplet,
            workspace_root: Some(project_root.display().to_string()),
            repo_root: Some(droplet_repo_root.clone()),
            ssh: Some(SshEndpoint {
                host: droplet_ssh_host.clone(),
                user: Some(droplet_ssh_user.clone()),
                port: droplet_ssh_port,
            }),
            remote_worker_mode: remote_worker_mode.clone(),
            remote_worker_root: Some(remote_worker_root.clone()),
            bootstrap_timeout_ms: Some(remote_worker_bootstrap_timeout_ms),
            sync_interval_ms: Some(remote_worker_sync_interval_ms),
            tunnel_port_base: Some(remote_worker_tunnel_port_base),
        };

        Self {
            project_root,
            data_dir,
            groups_dir,
            store_dir,
            db_path,
            assistant_name: assistant_name.clone(),
            default_trigger: format!("@{}", assistant_name),
            timezone,
            max_concurrent_groups,
            execution_lane,
            container_image,
            container_runtime,
            container_groups,
            droplet_ssh_host,
            droplet_ssh_user,
            droplet_ssh_port,
            droplet_repo_root,
            remote_worker_mode,
            remote_worker_root,
            remote_worker_binary,
            remote_worker_bootstrap_timeout_ms,
            remote_worker_sync_interval_ms,
            remote_worker_tunnel_port_base,
            omx_runner_path,
            omx_state_root,
            omx_callback_url,
            omx_callback_token,
            omx_default_mode,
            omx_default_max_workers,
            omx_poll_interval_ms,
            openclaw_gateway_bind_host,
            openclaw_gateway_public_host,
            openclaw_gateway_port,
            openclaw_gateway_token,
            openclaw_gateway_execution_lane,
            slack_env_file,
            slack_poll_interval_ms,
            linear_webhook_port,
            linear_webhook_secret,
            github_webhook_secret,
            linear_chat_jid,
            linear_api_key,
            linear_write_api_key,
            linear_pm_team_keys,
            linear_pm_policy_interval_minutes,
            linear_pm_digest_interval_hours,
            linear_pm_guardrail_max_automations,
            observability_chat_jid,
            observability_group_folder,
            observability_webhook_token,
            observability_auto_blue_team,
            observability_adapters_path,
            development_environment,
            sender_allowlist_path,
            project_environments_path,
            host_os_control_policy_path,
            host_os_approval_chat_jid,
            remote_control_ssh_host,
            remote_control_ssh_user,
            remote_control_ssh_port,
            remote_control_workspace_root,
        }
    }

    pub fn openclaw_gateway_public_ws_url(&self) -> Option<String> {
        if self.openclaw_gateway_port == 0 || self.openclaw_gateway_public_host.trim().is_empty() {
            return None;
        }
        Some(format!(
            "ws://{}:{}",
            self.openclaw_gateway_public_host, self.openclaw_gateway_port
        ))
    }

    pub fn openclaw_gateway_public_health_url(&self) -> Option<String> {
        if self.openclaw_gateway_port == 0 || self.openclaw_gateway_public_host.trim().is_empty() {
            return None;
        }
        Some(format!(
            "http://{}:{}/health",
            self.openclaw_gateway_public_host, self.openclaw_gateway_port
        ))
    }
}
