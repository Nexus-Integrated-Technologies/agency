use std::fs;
use std::path::Path;

use serde::Serialize;

use super::NanoclawConfig;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeChannelKind {
    Local,
    Scheduler,
    Slack,
    Webhook,
    OpenClawGateway,
    PmAutomation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeChannelStatus {
    Ready,
    Disabled,
    Degraded,
    Misconfigured,
    LegacyDisabled,
}

impl RuntimeChannelStatus {
    pub fn message(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::Disabled => "disabled",
            Self::Degraded => "degraded",
            Self::Misconfigured => "misconfigured",
            Self::LegacyDisabled => "legacy_disabled",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeChannelAuthPosture {
    pub required: bool,
    pub configured: bool,
    pub mechanisms: Vec<String>,
}

impl RuntimeChannelAuthPosture {
    fn not_required(mechanism: impl Into<String>) -> Self {
        Self {
            required: false,
            configured: true,
            mechanisms: vec![mechanism.into()],
        }
    }

    fn required(configured: bool, mechanisms: Vec<String>) -> Self {
        Self {
            required: true,
            configured,
            mechanisms,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeChannelDescriptor {
    pub id: String,
    pub kind: RuntimeChannelKind,
    pub enabled: bool,
    pub status: RuntimeChannelStatus,
    pub status_message: String,
    pub config_source: String,
    pub auth: RuntimeChannelAuthPosture,
    pub required_config: Vec<String>,
    pub missing_config: Vec<String>,
    pub serve_profiles: Vec<String>,
    pub operator_visible: bool,
    pub legacy: bool,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeChannelRegistrySummary {
    pub total: usize,
    pub enabled: usize,
    pub ready: usize,
    pub disabled: usize,
    pub degraded: usize,
    pub misconfigured: usize,
    pub legacy: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeChannelRegistry {
    pub ok: bool,
    pub summary: RuntimeChannelRegistrySummary,
    pub channels: Vec<RuntimeChannelDescriptor>,
}

pub fn runtime_channel_registry(config: &NanoclawConfig) -> RuntimeChannelRegistry {
    let channels = vec![
        local_channel(config),
        scheduler_channel(),
        slack_channel(config),
        webhook_channel(config),
        openclaw_gateway_channel(config),
        pm_automation_channel(config),
    ];
    let summary = summarize(&channels);

    RuntimeChannelRegistry {
        ok: summary.misconfigured == 0,
        summary,
        channels,
    }
}

fn summarize(channels: &[RuntimeChannelDescriptor]) -> RuntimeChannelRegistrySummary {
    RuntimeChannelRegistrySummary {
        total: channels.len(),
        enabled: channels.iter().filter(|channel| channel.enabled).count(),
        ready: channels
            .iter()
            .filter(|channel| channel.status == RuntimeChannelStatus::Ready)
            .count(),
        disabled: channels
            .iter()
            .filter(|channel| {
                matches!(
                    channel.status,
                    RuntimeChannelStatus::Disabled | RuntimeChannelStatus::LegacyDisabled
                )
            })
            .count(),
        degraded: channels
            .iter()
            .filter(|channel| channel.status == RuntimeChannelStatus::Degraded)
            .count(),
        misconfigured: channels
            .iter()
            .filter(|channel| channel.status == RuntimeChannelStatus::Misconfigured)
            .count(),
        legacy: channels.iter().filter(|channel| channel.legacy).count(),
    }
}

fn descriptor(
    id: &str,
    kind: RuntimeChannelKind,
    enabled: bool,
    status: RuntimeChannelStatus,
    config_source: impl Into<String>,
    auth: RuntimeChannelAuthPosture,
    required_config: Vec<&str>,
    missing_config: Vec<String>,
    serve_profiles: Vec<&str>,
    operator_visible: bool,
    legacy: bool,
    notes: Vec<&str>,
) -> RuntimeChannelDescriptor {
    RuntimeChannelDescriptor {
        id: id.to_string(),
        kind,
        enabled,
        status,
        status_message: status.message().to_string(),
        config_source: config_source.into(),
        auth,
        required_config: required_config.into_iter().map(str::to_string).collect(),
        missing_config,
        serve_profiles: serve_profiles.into_iter().map(str::to_string).collect(),
        operator_visible,
        legacy,
        notes: notes.into_iter().map(str::to_string).collect(),
    }
}

fn local_channel(config: &NanoclawConfig) -> RuntimeChannelDescriptor {
    descriptor(
        "local",
        RuntimeChannelKind::Local,
        true,
        RuntimeChannelStatus::Ready,
        config
            .data_dir
            .join("channels")
            .join("local")
            .display()
            .to_string(),
        RuntimeChannelAuthPosture::not_required("local_filesystem"),
        vec![],
        vec![],
        vec!["poll", "full"],
        true,
        false,
        vec!["durable local inbox/outbox channel"],
    )
}

fn scheduler_channel() -> RuntimeChannelDescriptor {
    descriptor(
        "scheduler",
        RuntimeChannelKind::Scheduler,
        true,
        RuntimeChannelStatus::Ready,
        "runtime database scheduled_tasks table",
        RuntimeChannelAuthPosture::not_required("local_control_plane"),
        vec![],
        vec![],
        vec!["poll", "full"],
        true,
        false,
        vec!["scheduled tasks are advanced by runtime poll or the full runtime profile"],
    )
}

fn slack_channel(config: &NanoclawConfig) -> RuntimeChannelDescriptor {
    let env_file = config
        .slack_env_file
        .as_ref()
        .map(|path| path.display().to_string());
    let missing_config = slack_missing_config(config.slack_env_file.as_deref());
    let auth_configured = config.slack_env_file.is_some() && missing_config.is_empty();
    let status = match (&env_file, missing_config.is_empty()) {
        (Some(_), true) => RuntimeChannelStatus::Ready,
        (Some(_), false) => RuntimeChannelStatus::Misconfigured,
        (None, _) => RuntimeChannelStatus::Disabled,
    };

    descriptor(
        "slack",
        RuntimeChannelKind::Slack,
        env_file.is_some(),
        status,
        env_file.unwrap_or_else(|| "NANOCLAW_SLACK_ENV_FILE or project .env".to_string()),
        RuntimeChannelAuthPosture::required(
            auth_configured,
            vec![
                "slack_bot_token_from_env_file".to_string(),
                "slack_app_token_from_env_file".to_string(),
            ],
        ),
        vec![
            "NANOCLAW_SLACK_ENV_FILE or project .env",
            "SLACK_BOT_TOKEN",
            "SLACK_APP_TOKEN",
        ],
        missing_config,
        vec!["full", "slack"],
        true,
        false,
        vec!["Socket Mode runtime channel; secrets are not read by status reporting"],
    )
}

fn slack_missing_config(env_file: Option<&Path>) -> Vec<String> {
    let Some(env_file) = env_file else {
        return vec!["NANOCLAW_SLACK_ENV_FILE or project .env".to_string()];
    };
    if !env_file.exists() {
        return vec![format!("readable Slack env file: {}", env_file.display())];
    }

    let content = match fs::read_to_string(env_file) {
        Ok(content) => content,
        Err(error) => {
            return vec![format!(
                "readable Slack env file: {} ({error})",
                env_file.display()
            )]
        }
    };
    ["SLACK_BOT_TOKEN", "SLACK_APP_TOKEN"]
        .iter()
        .filter(|key| !env_file_defines_key(&content, key))
        .map(|key| (*key).to_string())
        .collect()
}

fn env_file_defines_key(content: &str, key: &str) -> bool {
    content.lines().any(|line| {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            return false;
        }
        let Some((line_key, value)) = trimmed.split_once('=') else {
            return false;
        };
        line_key.trim() == key && !value.trim().trim_matches('"').trim_matches('\'').is_empty()
    })
}

fn webhook_channel(config: &NanoclawConfig) -> RuntimeChannelDescriptor {
    let enabled = config.linear_webhook_port > 0;
    let has_linear_auth =
        config.linear_legacy_enabled && !config.linear_webhook_secret.trim().is_empty();
    let has_github_auth = !config.github_webhook_secret.trim().is_empty();
    let has_observability_auth = !config.observability_webhook_token.trim().is_empty();
    let auth_configured = has_linear_auth || has_github_auth || has_observability_auth;
    let status = if !enabled {
        RuntimeChannelStatus::Disabled
    } else if auth_configured {
        RuntimeChannelStatus::Ready
    } else {
        RuntimeChannelStatus::Misconfigured
    };
    let mut mechanisms = Vec::new();
    if has_linear_auth {
        mechanisms.push("linear_hmac".to_string());
    }
    if has_github_auth {
        mechanisms.push("github_hmac".to_string());
    }
    if has_observability_auth {
        mechanisms.push("observability_token".to_string());
    }
    if mechanisms.is_empty() {
        mechanisms.push("not_configured".to_string());
    }
    let mut missing_config = Vec::new();
    if enabled && !auth_configured {
        missing_config.push("GITHUB_WEBHOOK_SECRET or OBSERVABILITY_WEBHOOK_TOKEN".to_string());
        if config.linear_legacy_enabled {
            missing_config.push("LINEAR_WEBHOOK_SECRET".to_string());
        }
    }

    descriptor(
        "webhook",
        RuntimeChannelKind::Webhook,
        enabled,
        status,
        format!(
            "LINEAR_WEBHOOK_PORT={}, GITHUB_WEBHOOK_SECRET, OBSERVABILITY_WEBHOOK_TOKEN",
            config.linear_webhook_port
        ),
        RuntimeChannelAuthPosture::required(auth_configured, mechanisms),
        vec![
            "LINEAR_WEBHOOK_PORT",
            "GITHUB_WEBHOOK_SECRET or OBSERVABILITY_WEBHOOK_TOKEN",
        ],
        missing_config,
        vec!["full", "webhook"],
        true,
        false,
        vec!["handles GitHub, observability, OMX, and legacy Linear webhook routes"],
    )
}

fn openclaw_gateway_channel(config: &NanoclawConfig) -> RuntimeChannelDescriptor {
    let port_configured = config.openclaw_gateway_port > 0;
    let token_configured = !config.openclaw_gateway_token.trim().is_empty();
    let mut missing_config = Vec::new();
    if !port_configured {
        missing_config.push("NANOCLAW_OPENCLAW_GATEWAY_PORT".to_string());
    }
    if !token_configured {
        missing_config.push("NANOCLAW_OPENCLAW_GATEWAY_TOKEN".to_string());
    }
    let status = match (port_configured, token_configured) {
        (true, true) => RuntimeChannelStatus::Ready,
        (true, false) => RuntimeChannelStatus::Misconfigured,
        (false, true) => RuntimeChannelStatus::Degraded,
        (false, false) => RuntimeChannelStatus::Disabled,
    };

    descriptor(
        "openclaw_gateway",
        RuntimeChannelKind::OpenClawGateway,
        port_configured,
        status,
        format!(
            "NANOCLAW_OPENCLAW_GATEWAY_PORT={}, NANOCLAW_OPENCLAW_GATEWAY_TOKEN",
            config.openclaw_gateway_port
        ),
        RuntimeChannelAuthPosture::required(token_configured, vec!["x-openclaw-token".to_string()]),
        vec![
            "NANOCLAW_OPENCLAW_GATEWAY_PORT",
            "NANOCLAW_OPENCLAW_GATEWAY_TOKEN",
        ],
        missing_config,
        vec!["full", "gateway"],
        true,
        false,
        vec!["Cloud adapter ingress that executes through the configured gateway lane"],
    )
}

fn pm_automation_channel(config: &NanoclawConfig) -> RuntimeChannelDescriptor {
    let chat_configured = !config.linear_chat_jid.trim().is_empty();
    let enabled = config.linear_legacy_enabled && chat_configured;
    let auth_configured =
        !config.linear_api_key.trim().is_empty() || !config.linear_write_api_key.trim().is_empty();
    let mut missing_config = Vec::new();
    if !config.linear_legacy_enabled {
        missing_config.push("NANOCLAW_LINEAR_LEGACY_ENABLED=true".to_string());
    }
    if !chat_configured {
        missing_config.push("LINEAR_CHAT_JID".to_string());
    }
    if config.linear_legacy_enabled && !auth_configured {
        missing_config.push("LINEAR_API_KEY or LINEAR_WRITE_API_KEY".to_string());
    }
    let status = if enabled {
        if auth_configured {
            RuntimeChannelStatus::Ready
        } else {
            RuntimeChannelStatus::Misconfigured
        }
    } else if config.linear_legacy_enabled {
        RuntimeChannelStatus::Misconfigured
    } else {
        RuntimeChannelStatus::LegacyDisabled
    };

    descriptor(
        "pm_automation",
        RuntimeChannelKind::PmAutomation,
        enabled,
        status,
        "NANOCLAW_LINEAR_LEGACY_ENABLED, LINEAR_CHAT_JID, LINEAR_API_KEY",
        RuntimeChannelAuthPosture::required(
            auth_configured,
            vec![
                "linear_api_key".to_string(),
                "linear_write_api_key".to_string(),
            ],
        ),
        vec![
            "NANOCLAW_LINEAR_LEGACY_ENABLED=true",
            "LINEAR_CHAT_JID",
            "LINEAR_API_KEY or LINEAR_WRITE_API_KEY",
        ],
        missing_config,
        vec!["pm"],
        true,
        true,
        vec!["discontinued Linear PM automation lane; disabled by default"],
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn registry_reports_default_channel_ownership() -> anyhow::Result<()> {
        let temp = tempdir()?;
        let mut config = NanoclawConfig::from_env();
        config.project_root = temp.path().to_path_buf();
        config.data_dir = temp.path().join("data");
        config.slack_env_file = None;
        config.linear_webhook_port = 0;
        config.openclaw_gateway_port = 0;
        config.openclaw_gateway_token.clear();
        config.linear_legacy_enabled = false;

        let registry = runtime_channel_registry(&config);

        assert!(registry.ok);
        assert_eq!(registry.summary.total, 6);
        assert_channel(&registry, "local", RuntimeChannelStatus::Ready, true);
        assert_channel(&registry, "scheduler", RuntimeChannelStatus::Ready, true);
        assert_channel(&registry, "slack", RuntimeChannelStatus::Disabled, false);
        assert_channel(&registry, "webhook", RuntimeChannelStatus::Disabled, false);
        assert_channel(
            &registry,
            "openclaw_gateway",
            RuntimeChannelStatus::Disabled,
            false,
        );
        assert_channel(
            &registry,
            "pm_automation",
            RuntimeChannelStatus::LegacyDisabled,
            false,
        );
        Ok(())
    }

    #[test]
    fn registry_reports_configured_gateway_and_webhook_auth() -> anyhow::Result<()> {
        let temp = tempdir()?;
        let mut config = NanoclawConfig::from_env();
        config.project_root = temp.path().to_path_buf();
        config.data_dir = temp.path().join("data");
        config.slack_env_file = None;
        config.openclaw_gateway_port = 8788;
        config.openclaw_gateway_token = "gateway-token".to_string();
        config.linear_webhook_port = 8789;
        config.github_webhook_secret = "github-secret".to_string();

        let registry = runtime_channel_registry(&config);

        assert!(registry.ok);
        let gateway = find_channel(&registry, "openclaw_gateway");
        assert_eq!(gateway.status, RuntimeChannelStatus::Ready);
        assert!(gateway.auth.required);
        assert!(gateway.auth.configured);
        assert_eq!(gateway.serve_profiles, vec!["full", "gateway"]);
        let webhook = find_channel(&registry, "webhook");
        assert_eq!(webhook.status, RuntimeChannelStatus::Ready);
        assert!(webhook
            .auth
            .mechanisms
            .iter()
            .any(|mechanism| mechanism == "github_hmac"));
        Ok(())
    }

    #[test]
    fn registry_marks_enabled_channels_without_auth_as_misconfigured() -> anyhow::Result<()> {
        let temp = tempdir()?;
        let mut config = NanoclawConfig::from_env();
        config.project_root = temp.path().to_path_buf();
        config.data_dir = temp.path().join("data");
        config.slack_env_file = None;
        config.openclaw_gateway_port = 8788;
        config.openclaw_gateway_token.clear();
        config.linear_webhook_port = 8789;
        config.github_webhook_secret.clear();
        config.observability_webhook_token.clear();
        config.linear_legacy_enabled = false;

        let registry = runtime_channel_registry(&config);

        assert!(!registry.ok);
        assert_channel(
            &registry,
            "openclaw_gateway",
            RuntimeChannelStatus::Misconfigured,
            true,
        );
        assert_channel(
            &registry,
            "webhook",
            RuntimeChannelStatus::Misconfigured,
            true,
        );
        assert_eq!(registry.summary.misconfigured, 2);
        Ok(())
    }

    #[test]
    fn registry_reports_slack_env_file_readiness() -> anyhow::Result<()> {
        let temp = tempdir()?;
        let env_file = temp.path().join(".env");
        fs::write(&env_file, "SLACK_BOT_TOKEN=x\nSLACK_APP_TOKEN=xapp\n")?;
        let mut config = NanoclawConfig::from_env();
        config.project_root = temp.path().to_path_buf();
        config.data_dir = temp.path().join("data");
        config.slack_env_file = Some(env_file.clone());

        let registry = runtime_channel_registry(&config);
        let slack = find_channel(&registry, "slack");

        assert_eq!(slack.status, RuntimeChannelStatus::Ready);
        assert!(slack.enabled);
        assert_eq!(slack.config_source, env_file.display().to_string());
        Ok(())
    }

    fn find_channel<'a>(
        registry: &'a RuntimeChannelRegistry,
        id: &str,
    ) -> &'a RuntimeChannelDescriptor {
        registry
            .channels
            .iter()
            .find(|channel| channel.id == id)
            .unwrap_or_else(|| panic!("missing channel {id}"))
    }

    fn assert_channel(
        registry: &RuntimeChannelRegistry,
        id: &str,
        status: RuntimeChannelStatus,
        enabled: bool,
    ) {
        let channel = find_channel(registry, id);
        assert_eq!(channel.status, status);
        assert_eq!(channel.enabled, enabled);
    }
}
