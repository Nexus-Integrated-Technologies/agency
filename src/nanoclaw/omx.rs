use std::collections::BTreeMap;
use std::convert::Infallible;
use std::env;
use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};

use anyhow::{anyhow, bail, Context, Result};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::foundation::{
    ArtifactKind, ArtifactRecord, ArtifactStore, DevelopmentEnvironmentKind, EventLog,
    ExecutionBoundary, ExecutionBoundaryKind, ExecutionContext, FoundationEvent, MessageRecord,
};

use super::app::NanoclawApp;
use super::config::NanoclawConfig;
use super::dev_environment::DigitalOceanDevEnvironment;
use super::service_slack::{ensure_registered_group, send_recorded_slack_message};
use super::slack::SlackChannel;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(into = "String", try_from = "String")]
pub enum OmxMode {
    Exec,
    Team,
    Ralph,
    DeepInterview,
    Ralplan,
    Custom(String),
}

impl OmxMode {
    pub fn parse(input: &str) -> Self {
        match input.trim().to_ascii_lowercase().as_str() {
            "" | "exec" => Self::Exec,
            "team" => Self::Team,
            "ralph" => Self::Ralph,
            "deep-interview" | "deep_interview" | "deepinterview" => Self::DeepInterview,
            "ralplan" => Self::Ralplan,
            other => Self::Custom(other.to_string()),
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            Self::Exec => "exec",
            Self::Team => "team",
            Self::Ralph => "ralph",
            Self::DeepInterview => "deep-interview",
            Self::Ralplan => "ralplan",
            Self::Custom(value) => value.as_str(),
        }
    }
}

impl From<OmxMode> for String {
    fn from(value: OmxMode) -> Self {
        value.as_str().to_string()
    }
}

impl TryFrom<String> for OmxMode {
    type Error = Infallible;

    fn try_from(value: String) -> std::result::Result<Self, Self::Error> {
        Ok(Self::parse(&value))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(into = "String", try_from = "String")]
pub enum OmxSessionStatus {
    Pending,
    Running,
    Idle,
    WaitingInput,
    Completed,
    Failed,
    Stopped,
    Custom(String),
}

impl OmxSessionStatus {
    pub fn parse(input: &str) -> Self {
        match input.trim().to_ascii_lowercase().as_str() {
            "" | "pending" => Self::Pending,
            "running" => Self::Running,
            "idle" => Self::Idle,
            "waiting_input" | "waiting-input" | "waitinginput" => Self::WaitingInput,
            "completed" | "success" => Self::Completed,
            "failed" | "error" => Self::Failed,
            "stopped" | "cancelled" | "canceled" => Self::Stopped,
            other => Self::Custom(other.to_string()),
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Idle => "idle",
            Self::WaitingInput => "waiting_input",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Stopped => "stopped",
            Self::Custom(value) => value.as_str(),
        }
    }

    pub fn is_active(&self) -> bool {
        matches!(
            self,
            Self::Pending | Self::Running | Self::Idle | Self::WaitingInput
        )
    }

    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Stopped)
    }
}

impl From<OmxSessionStatus> for String {
    fn from(value: OmxSessionStatus) -> Self {
        value.as_str().to_string()
    }
}

impl TryFrom<String> for OmxSessionStatus {
    type Error = Infallible;

    fn try_from(value: String) -> std::result::Result<Self, Self::Error> {
        Ok(Self::parse(&value))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(into = "String", try_from = "String")]
pub enum OmxEventKind {
    SessionStart,
    SessionIdle,
    AskUserQuestion,
    SessionStop,
    SessionEnd,
    ReplyInjected,
    Status,
    Custom(String),
}

impl From<OmxEventKind> for String {
    fn from(value: OmxEventKind) -> Self {
        value.as_str().to_string()
    }
}

impl TryFrom<String> for OmxEventKind {
    type Error = Infallible;

    fn try_from(value: String) -> std::result::Result<Self, Self::Error> {
        Ok(Self::parse(&value))
    }
}

impl OmxEventKind {
    pub fn parse(input: &str) -> Self {
        match input.trim().to_ascii_lowercase().as_str() {
            "session-start" | "session_start" => Self::SessionStart,
            "session-idle" | "session_idle" => Self::SessionIdle,
            "ask-user-question" | "ask_user_question" => Self::AskUserQuestion,
            "session-stop" | "session_stop" => Self::SessionStop,
            "session-end" | "session_end" => Self::SessionEnd,
            "reply-injected" | "reply_injected" => Self::ReplyInjected,
            "" | "status" => Self::Status,
            other => Self::Custom(other.to_string()),
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            Self::SessionStart => "session-start",
            Self::SessionIdle => "session-idle",
            Self::AskUserQuestion => "ask-user-question",
            Self::SessionStop => "session-stop",
            Self::SessionEnd => "session-end",
            Self::ReplyInjected => "reply-injected",
            Self::Status => "status",
            Self::Custom(value) => value.as_str(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct OmxArtifactRef {
    pub kind: String,
    pub title: String,
    pub location: Option<String>,
    pub body: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OmxExecutionOptions {
    pub mode: OmxMode,
    pub max_workers: Option<usize>,
    pub external_run_id: Option<String>,
}

impl Default for OmxExecutionOptions {
    fn default() -> Self {
        Self {
            mode: OmxMode::Team,
            max_workers: None,
            external_run_id: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OmxSessionRecord {
    pub session_id: String,
    pub group_folder: String,
    pub chat_jid: Option<String>,
    pub task_id: Option<String>,
    pub external_run_id: Option<String>,
    pub mode: OmxMode,
    pub status: OmxSessionStatus,
    pub tmux_session: Option<String>,
    pub team_name: Option<String>,
    pub workspace_root: String,
    pub last_summary: Option<String>,
    pub pending_question: Option<String>,
    pub last_event_kind: Option<OmxEventKind>,
    pub last_activity_at: String,
    pub created_at: String,
    pub updated_at: String,
    pub completed_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OmxEventRecord {
    pub id: String,
    pub session_id: String,
    pub event_kind: OmxEventKind,
    pub group_folder: String,
    pub chat_jid: Option<String>,
    pub summary: Option<String>,
    pub question: Option<String>,
    pub payload_json: Option<Value>,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum OmxRunnerCommand {
    Invoke,
    Status,
    Stop,
    Inject,
}

impl OmxRunnerCommand {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Invoke => "invoke",
            Self::Status => "status",
            Self::Stop => "stop",
            Self::Inject => "inject",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OmxRunnerRequest {
    pub command: OmxRunnerCommand,
    pub session_id: Option<String>,
    pub group_folder: String,
    pub chat_jid: Option<String>,
    pub task_id: Option<String>,
    pub external_run_id: Option<String>,
    pub mode: OmxMode,
    pub max_workers: Option<usize>,
    pub prompt: Option<String>,
    pub reply: Option<String>,
    pub cwd: String,
    pub callback_url: Option<String>,
    pub callback_token: Option<String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    pub metadata: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OmxRunnerResponse {
    pub ok: bool,
    pub session_id: String,
    pub status: OmxSessionStatus,
    pub tmux_session: Option<String>,
    pub team_name: Option<String>,
    pub summary: String,
    pub question: Option<String>,
    pub log_path: Option<String>,
    pub log_body: Option<String>,
    pub artifacts: Vec<OmxArtifactRef>,
    pub external_run_id: Option<String>,
    pub mode: OmxMode,
    pub workspace_root: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OmxWebhookPayload {
    pub token: Option<String>,
    pub event: OmxEventKind,
    pub session_id: String,
    pub group_folder: String,
    pub chat_jid: Option<String>,
    pub task_id: Option<String>,
    pub external_run_id: Option<String>,
    pub mode: OmxMode,
    pub status: OmxSessionStatus,
    pub tmux_session: Option<String>,
    pub team_name: Option<String>,
    pub summary: Option<String>,
    pub question: Option<String>,
    pub workspace_root: String,
    pub artifacts: Vec<OmxArtifactRef>,
    pub created_at: String,
}

#[derive(Debug, Clone)]
pub struct OmxRunnerClient {
    dev_environment: DigitalOceanDevEnvironment,
    runner_path: String,
    repo_runner_path: String,
    runner_location: OmxRunnerLocation,
    callback_url: String,
    callback_token: String,
    default_mode: OmxMode,
    default_max_workers: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OmxRunnerLocation {
    Local,
    RemoteSsh,
}

impl OmxRunnerLocation {
    fn from_config(config: &NanoclawConfig) -> Self {
        if let Ok(value) = env::var("NANOCLAW_OMX_RUNNER_LOCATION")
            .or_else(|_| env::var("NANOCLAW_OMX_RUNNER_MODE"))
        {
            match value.trim().to_ascii_lowercase().as_str() {
                "local" | "host" | "container" | "cloudflare" => return Self::Local,
                "remote" | "remote_ssh" | "remote-ssh" | "ssh" | "droplet" | "digitalocean" => {
                    return Self::RemoteSsh;
                }
                _ => {}
            }
        }

        if matches!(
            config.development_environment.kind,
            DevelopmentEnvironmentKind::Local
        ) {
            Self::Local
        } else {
            Self::RemoteSsh
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::RemoteSsh => "remote_ssh",
        }
    }
}

impl OmxRunnerClient {
    pub fn from_config(config: &NanoclawConfig) -> Self {
        Self {
            dev_environment: DigitalOceanDevEnvironment::from_config(config),
            runner_path: config.omx_runner_path.clone(),
            repo_runner_path: "deploy/omx-paperclip/omx-paperclip-runner.mjs".to_string(),
            runner_location: OmxRunnerLocation::from_config(config),
            callback_url: config.omx_callback_url.clone(),
            callback_token: config.omx_callback_token.clone(),
            default_mode: OmxMode::parse(&config.omx_default_mode),
            default_max_workers: config.omx_default_max_workers,
        }
    }

    pub fn invoke(
        &self,
        group_folder: &str,
        chat_jid: Option<&str>,
        task_id: Option<&str>,
        prompt: &str,
        cwd: &Path,
        options: Option<&OmxExecutionOptions>,
        env: BTreeMap<String, String>,
    ) -> Result<OmxRunnerResponse> {
        let runner_cwd = self.prepare_group_workspace(group_folder, cwd, true)?;
        let request = OmxRunnerRequest {
            command: OmxRunnerCommand::Invoke,
            session_id: None,
            group_folder: group_folder.to_string(),
            chat_jid: chat_jid.map(str::to_string),
            task_id: task_id.map(str::to_string),
            external_run_id: options.and_then(|value| value.external_run_id.clone()),
            mode: options
                .map(|value| value.mode.clone())
                .unwrap_or_else(|| self.default_mode.clone()),
            max_workers: options
                .and_then(|value| value.max_workers)
                .or(Some(self.default_max_workers)),
            prompt: Some(prompt.to_string()),
            reply: None,
            cwd: runner_cwd,
            callback_url: non_empty_string(Some(self.callback_url.clone())),
            callback_token: non_empty_string(Some(self.callback_token.clone())),
            env,
            metadata: None,
        };
        self.run(request)
    }

    pub fn status(&self, session_id: &str, cwd: &Path) -> Result<OmxRunnerResponse> {
        self.run(OmxRunnerRequest {
            command: OmxRunnerCommand::Status,
            session_id: Some(session_id.to_string()),
            group_folder: String::new(),
            chat_jid: None,
            task_id: None,
            external_run_id: None,
            mode: self.default_mode.clone(),
            max_workers: None,
            prompt: None,
            reply: None,
            cwd: cwd.display().to_string(),
            callback_url: non_empty_string(Some(self.callback_url.clone())),
            callback_token: non_empty_string(Some(self.callback_token.clone())),
            env: BTreeMap::new(),
            metadata: None,
        })
    }

    pub fn stop(&self, session_id: &str, cwd: &Path) -> Result<OmxRunnerResponse> {
        self.run(OmxRunnerRequest {
            command: OmxRunnerCommand::Stop,
            session_id: Some(session_id.to_string()),
            group_folder: String::new(),
            chat_jid: None,
            task_id: None,
            external_run_id: None,
            mode: self.default_mode.clone(),
            max_workers: None,
            prompt: None,
            reply: None,
            cwd: cwd.display().to_string(),
            callback_url: non_empty_string(Some(self.callback_url.clone())),
            callback_token: non_empty_string(Some(self.callback_token.clone())),
            env: BTreeMap::new(),
            metadata: None,
        })
    }

    pub fn inject(
        &self,
        session_id: &str,
        group_folder: &str,
        chat_jid: Option<&str>,
        cwd: &Path,
        reply: &str,
    ) -> Result<OmxRunnerResponse> {
        let runner_cwd = self.prepare_group_workspace(group_folder, cwd, false)?;
        self.run(OmxRunnerRequest {
            command: OmxRunnerCommand::Inject,
            session_id: Some(session_id.to_string()),
            group_folder: group_folder.to_string(),
            chat_jid: chat_jid.map(str::to_string),
            task_id: None,
            external_run_id: None,
            mode: self.default_mode.clone(),
            max_workers: None,
            prompt: None,
            reply: Some(reply.to_string()),
            cwd: runner_cwd,
            callback_url: non_empty_string(Some(self.callback_url.clone())),
            callback_token: non_empty_string(Some(self.callback_token.clone())),
            env: BTreeMap::new(),
            metadata: None,
        })
    }

    fn prepare_group_workspace(
        &self,
        group_folder: &str,
        cwd: &Path,
        sync_project: bool,
    ) -> Result<String> {
        match self.runner_location {
            OmxRunnerLocation::Local => Ok(cwd.display().to_string()),
            OmxRunnerLocation::RemoteSsh => {
                if sync_project {
                    self.dev_environment.sync_project()?;
                }
                self.sync_remote_group_workspace(group_folder, cwd)
            }
        }
    }

    fn sync_remote_group_workspace(&self, group_folder: &str, cwd: &Path) -> Result<String> {
        self.dev_environment
            .sync_group_workspace(cwd, group_folder)?;
        self.dev_environment
            .remote_group_workspace_path(group_folder)
    }

    fn run(&self, request: OmxRunnerRequest) -> Result<OmxRunnerResponse> {
        let command = format!(
            "if [ -x {runner} ]; then {runner} {subcommand} --json; elif [ -f {repo_runner} ]; then node {repo_runner} {subcommand} --json; else echo 'omx runner not found' >&2; exit 1; fi",
            runner = shell_quote(&self.runner_path),
            repo_runner = shell_quote(&self.repo_runner_path),
            subcommand = request.command.as_str(),
        );
        let stdin = serde_json::to_vec(&request).context("failed to encode OMX runner request")?;
        let stdout = match self.runner_location {
            OmxRunnerLocation::Local => self.exec_local_runner(&command, &stdin)?,
            OmxRunnerLocation::RemoteSsh => {
                self.dev_environment
                    .exec_with_stdin(&command, &stdin)?
                    .stdout
            }
        };
        let response = serde_json::from_str::<OmxRunnerResponse>(&stdout)
            .context("failed to decode OMX runner response")?;
        if !response.ok {
            bail!("OMX runner reported failure: {}", response.summary);
        }
        Ok(response)
    }

    fn exec_local_runner(&self, command: &str, stdin: &[u8]) -> Result<String> {
        let mut process = Command::new("/bin/sh")
            .arg("-lc")
            .arg(command)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .context("failed to start local OMX runner command")?;
        process
            .stdin
            .as_mut()
            .context("local OMX runner stdin was not available")?
            .write_all(stdin)
            .context("failed to write local OMX runner stdin")?;
        let output = process
            .wait_with_output()
            .context("failed to wait for local OMX runner command")?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
            let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
            bail!(
                "local OMX runner command failed with status {}{}{}",
                output.status,
                if stderr.is_empty() {
                    String::new()
                } else {
                    format!(": {stderr}")
                },
                if stdout.is_empty() {
                    String::new()
                } else {
                    format!("; stdout: {}", truncate_runner_output(&stdout))
                }
            );
        }
        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    }

    #[cfg(test)]
    fn local_for_test(runner_path: String) -> Self {
        let config = NanoclawConfig::from_env();
        Self {
            dev_environment: DigitalOceanDevEnvironment::from_config(&config),
            runner_path,
            repo_runner_path: "deploy/omx-paperclip/omx-paperclip-runner.mjs".to_string(),
            runner_location: OmxRunnerLocation::Local,
            callback_url: String::new(),
            callback_token: String::new(),
            default_mode: OmxMode::Exec,
            default_max_workers: 1,
        }
    }
}

pub fn describe_omx_readiness(config: &NanoclawConfig) -> Value {
    json!({
        "ok": true,
        "endpoint": "omx-webhook",
        "status": if config.omx_callback_token.trim().is_empty() { "degraded" } else { "ready" },
        "method": "POST",
        "tokenRequired": !config.omx_callback_token.trim().is_empty(),
        "runnerPath": config.omx_runner_path,
        "runnerLocation": OmxRunnerLocation::from_config(config).as_str(),
        "callbackUrl": config.omx_callback_url,
    })
}

pub fn is_valid_omx_token(config: &NanoclawConfig, token: Option<&str>) -> bool {
    let configured = config.omx_callback_token.trim();
    if configured.is_empty() {
        return true;
    }
    token.map(str::trim) == Some(configured)
}

pub fn apply_omx_webhook_payload(
    app: &mut NanoclawApp,
    channel: &mut Option<SlackChannel>,
    payload: OmxWebhookPayload,
) -> Result<Value> {
    let now = payload.created_at.clone();
    let group = if let Some(chat_jid) = payload.chat_jid.as_deref() {
        Some(ensure_registered_group(
            app,
            chat_jid,
            Some(&payload.group_folder),
        )?)
    } else {
        app.groups()?
            .into_iter()
            .find(|group| group.folder == payload.group_folder)
    };
    let chat_jid = payload
        .chat_jid
        .clone()
        .or_else(|| group.as_ref().map(|value| value.jid.clone()));
    let session = OmxSessionRecord {
        session_id: payload.session_id.clone(),
        group_folder: payload.group_folder.clone(),
        chat_jid: chat_jid.clone(),
        task_id: payload.task_id.clone(),
        external_run_id: payload.external_run_id.clone(),
        mode: payload.mode.clone(),
        status: payload.status.clone(),
        tmux_session: payload.tmux_session.clone(),
        team_name: payload.team_name.clone(),
        workspace_root: payload.workspace_root.clone(),
        last_summary: payload.summary.clone(),
        pending_question: payload.question.clone(),
        last_event_kind: Some(payload.event.clone()),
        last_activity_at: now.clone(),
        created_at: existing_omx_created_at(app, &payload.session_id)?
            .unwrap_or_else(|| now.clone()),
        updated_at: now.clone(),
        completed_at: payload.status.is_terminal().then_some(now.clone()),
    };
    app.db.upsert_omx_session(&session)?;
    let event = OmxEventRecord {
        id: format!("omx-event:{}:{}", payload.session_id, now),
        session_id: payload.session_id.clone(),
        event_kind: payload.event.clone(),
        group_folder: payload.group_folder.clone(),
        chat_jid: chat_jid.clone(),
        summary: payload.summary.clone(),
        question: payload.question.clone(),
        payload_json: Some(serde_json::to_value(&payload).unwrap_or(Value::Null)),
        created_at: now.clone(),
    };
    app.db.insert_omx_event(&event)?;
    app.record_event(FoundationEvent::RouterStateUpdated {
        key: format!("omx:last-event:{}", payload.session_id),
        value: payload.event.as_str().to_string(),
    });

    if let Some(group) = group.as_ref() {
        if let Some(summary) = payload
            .summary
            .as_deref()
            .filter(|value| !value.trim().is_empty())
        {
            let artifact = ArtifactRecord {
                id: format!("artifact:omx:{}:{}", payload.session_id, now),
                group_id: group.jid.clone(),
                task_id: payload.task_id.clone(),
                kind: ArtifactKind::ExecutionLog,
                title: format!("OMX {} update", payload.event.as_str()),
                body: summary.to_string(),
                location: payload.tmux_session.clone(),
                created_at: now.clone(),
            };
            app.record_artifact(artifact.clone());
            app.record_event(FoundationEvent::ArtifactEmitted {
                artifact,
                context: Some(ExecutionContext {
                    group_id: group.jid.clone(),
                    chat_id: chat_jid.clone(),
                    task_id: payload.task_id.clone(),
                    boundary: ExecutionBoundary {
                        kind: ExecutionBoundaryKind::Omx,
                        root: Some(payload.workspace_root.clone()),
                        isolated: true,
                    },
                    workspace_root: Some(payload.workspace_root.clone()),
                }),
            });
        }
    }

    if let (Some(channel), Some(chat_jid), Some(group)) =
        (channel.as_mut(), chat_jid.as_deref(), group.as_ref())
    {
        if let Some(text) = format_slack_message(&payload) {
            let _ = send_recorded_slack_message(
                app,
                channel,
                chat_jid,
                Some(&group.name),
                &text,
                "omx",
                Some("OMX"),
                true,
                true,
            );
        }
    }

    Ok(json!({
        "ok": true,
        "sessionId": payload.session_id,
        "event": payload.event.as_str(),
        "status": payload.status.as_str(),
    }))
}

pub fn maybe_inject_slack_reply(app: &mut NanoclawApp, message: &MessageRecord) -> Result<bool> {
    if message.is_from_me || message.is_bot_message {
        return Ok(false);
    }
    let Some(session) = app.db.find_active_omx_session_for_chat(&message.chat_jid)? else {
        return Ok(false);
    };
    let client = OmxRunnerClient::from_config(&app.config);
    let cwd = Path::new(&app.config.groups_dir).join(&session.group_folder);
    let response = client.inject(
        &session.session_id,
        &session.group_folder,
        session.chat_jid.as_deref(),
        &cwd,
        &message.content,
    )?;
    let updated = OmxSessionRecord {
        session_id: session.session_id.clone(),
        group_folder: session.group_folder.clone(),
        chat_jid: session.chat_jid.clone(),
        task_id: session.task_id.clone(),
        external_run_id: response
            .external_run_id
            .clone()
            .or(session.external_run_id.clone()),
        mode: response.mode.clone(),
        status: response.status.clone(),
        tmux_session: response.tmux_session.clone(),
        team_name: response.team_name.clone(),
        workspace_root: response.workspace_root.clone(),
        last_summary: Some(response.summary.clone()),
        pending_question: response.question.clone(),
        last_event_kind: Some(OmxEventKind::ReplyInjected),
        last_activity_at: Utc::now().to_rfc3339(),
        created_at: session.created_at.clone(),
        updated_at: Utc::now().to_rfc3339(),
        completed_at: response
            .status
            .is_terminal()
            .then_some(Utc::now().to_rfc3339()),
    };
    app.db.upsert_omx_session(&updated)?;
    app.db.insert_omx_event(&OmxEventRecord {
        id: format!("omx-event:{}:reply", session.session_id),
        session_id: session.session_id,
        event_kind: OmxEventKind::ReplyInjected,
        group_folder: updated.group_folder,
        chat_jid: updated.chat_jid,
        summary: Some(response.summary),
        question: response.question,
        payload_json: None,
        created_at: Utc::now().to_rfc3339(),
    })?;
    Ok(true)
}

fn existing_omx_created_at(app: &NanoclawApp, session_id: &str) -> Result<Option<String>> {
    Ok(app
        .db
        .get_omx_session(session_id)?
        .map(|session| session.created_at))
}

fn format_slack_message(payload: &OmxWebhookPayload) -> Option<String> {
    let summary = payload.summary.as_deref().unwrap_or("").trim();
    match payload.event {
        OmxEventKind::SessionStart => Some(format!(
            "OMX session started.\nMode: {}\nSession: {}\nTmux: {}\n{}",
            payload.mode.as_str(),
            payload.session_id,
            payload.tmux_session.as_deref().unwrap_or("-"),
            if summary.is_empty() {
                "Runner is live.".to_string()
            } else {
                summary.to_string()
            }
        )),
        OmxEventKind::SessionIdle => Some(format!(
            "OMX session is idle.\nSession: {}\n{}",
            payload.session_id,
            if summary.is_empty() {
                "Reply in this thread to inject more input.".to_string()
            } else {
                summary.to_string()
            }
        )),
        OmxEventKind::AskUserQuestion => Some(format!(
            "OMX needs input.\nSession: {}\nQuestion: {}\nReply in this thread to inject your answer.",
            payload.session_id,
            payload.question.as_deref().unwrap_or(summary)
        )),
        OmxEventKind::SessionStop | OmxEventKind::SessionEnd => Some(format!(
            "OMX session {}.\nSession: {}\nStatus: {}\n{}",
            if matches!(payload.event, OmxEventKind::SessionStop) {
                "stopped"
            } else {
                "ended"
            },
            payload.session_id,
            payload.status.as_str(),
            if summary.is_empty() {
                "No summary provided.".to_string()
            } else {
                summary.to_string()
            }
        )),
        OmxEventKind::ReplyInjected | OmxEventKind::Status | OmxEventKind::Custom(_) => None,
    }
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

fn non_empty_string(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn truncate_runner_output(value: &str) -> String {
    const MAX_CHARS: usize = 2_000;
    let mut chars = value.chars();
    let truncated: String = chars.by_ref().take(MAX_CHARS).collect();
    if chars.next().is_some() {
        format!("{truncated}...[truncated]")
    } else {
        truncated
    }
}

pub fn parse_omx_webhook_payload(body: &[u8]) -> Result<OmxWebhookPayload> {
    let payload = serde_json::from_slice::<OmxWebhookPayload>(body)
        .context("failed to decode OMX webhook payload")?;
    if payload.session_id.trim().is_empty() {
        return Err(anyhow!("missing OMX session_id"));
    }
    if payload.group_folder.trim().is_empty() {
        return Err(anyhow!("missing OMX group_folder"));
    }
    Ok(payload)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_runner_executes_without_remote_ssh_environment() {
        let temp = tempfile::tempdir().expect("temp dir");
        let runner = temp.path().join("omx-paperclip-runner");
        std::fs::write(
            &runner,
            r#"#!/bin/sh
cat >/dev/null
printf '%s\n' '{"ok":true,"session_id":"local-test","status":"completed","tmux_session":null,"team_name":null,"summary":"local runner ok","question":null,"log_path":null,"log_body":null,"artifacts":[],"external_run_id":null,"mode":"exec","workspace_root":"/workspace"}'
"#,
        )
        .expect("write runner");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&runner, std::fs::Permissions::from_mode(0o755))
                .expect("chmod runner");
        }

        let client = OmxRunnerClient::local_for_test(runner.display().to_string());
        let response = client
            .invoke(
                "group-1",
                None,
                None,
                "run locally",
                temp.path(),
                None,
                BTreeMap::new(),
            )
            .expect("local runner should execute");

        assert_eq!(response.session_id, "local-test");
        assert_eq!(response.status, OmxSessionStatus::Completed);
        assert_eq!(response.summary, "local runner ok");
    }

    #[test]
    fn invoke_request_carries_per_run_env_to_local_runner() {
        let temp = tempfile::tempdir().expect("temp dir");
        let runner = temp.path().join("omx-paperclip-runner");
        std::fs::write(
            &runner,
            r#"#!/bin/sh
payload="$(cat)"
case "$payload" in
  *'"PAPERCLIP_AGENT_ID":"agent-1"'*'"PAPERCLIP_API_KEY":"run-key"'*)
    printf '%s\n' '{"ok":true,"session_id":"local-test","status":"completed","tmux_session":null,"team_name":null,"summary":"env carried","question":null,"log_path":null,"log_body":null,"artifacts":[],"external_run_id":null,"mode":"exec","workspace_root":"/workspace"}'
    ;;
  *)
    printf '%s\n' "$payload" >&2
    exit 1
    ;;
esac
"#,
        )
        .expect("write runner");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&runner, std::fs::Permissions::from_mode(0o755))
                .expect("chmod runner");
        }

        let client = OmxRunnerClient::local_for_test(runner.display().to_string());
        let response = client
            .invoke(
                "group-1",
                None,
                None,
                "run locally",
                temp.path(),
                None,
                BTreeMap::from([
                    ("PAPERCLIP_AGENT_ID".to_string(), "agent-1".to_string()),
                    ("PAPERCLIP_API_KEY".to_string(), "run-key".to_string()),
                ]),
            )
            .expect("local runner should receive env");

        assert_eq!(response.summary, "env carried");
    }

    #[test]
    fn local_runner_nonzero_error_includes_stdout_diagnostics() {
        let temp = tempfile::tempdir().expect("temp dir");
        let runner = temp.path().join("omx-paperclip-runner");
        std::fs::write(
            &runner,
            r#"#!/bin/sh
cat >/dev/null
printf '%s\n' '{"ok":false,"session_id":"","status":"failed","summary":"codex auth failed","question":null,"tmux_session":null,"team_name":null,"log_path":null,"log_body":"401 Unauthorized","artifacts":[],"external_run_id":null,"mode":"exec","workspace_root":""}'
exit 1
"#,
        )
        .expect("write runner");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&runner, std::fs::Permissions::from_mode(0o755))
                .expect("chmod runner");
        }

        let client = OmxRunnerClient::local_for_test(runner.display().to_string());
        let error = client
            .invoke(
                "group-1",
                None,
                None,
                "run locally",
                temp.path(),
                None,
                BTreeMap::new(),
            )
            .expect_err("local runner should fail");
        let message = format!("{error:#}");

        assert!(message.contains("local OMX runner command failed"));
        assert!(message.contains("codex auth failed"));
        assert!(message.contains("401 Unauthorized"));
    }

    #[test]
    fn omx_enums_round_trip_as_strings() {
        let payload = serde_json::json!({
            "token": "token-1",
            "event": "ask-user-question",
            "session_id": "session-1",
            "group_folder": "main",
            "chat_jid": "slack:C123",
            "task_id": null,
            "external_run_id": "run-1",
            "mode": "deep-interview",
            "status": "waiting_input",
            "tmux_session": "omx-session-1",
            "team_name": "team-alpha",
            "summary": "Need input",
            "question": "What should I do next?",
            "workspace_root": "/workspace",
            "artifacts": [{
                "kind": "omx-log",
                "title": "Runner log",
                "location": "/tmp/log.txt",
                "body": "hello"
            }],
            "created_at": "2026-04-08T00:00:00Z"
        });

        let parsed: OmxWebhookPayload =
            serde_json::from_value(payload.clone()).expect("payload should deserialize");
        assert_eq!(parsed.event, OmxEventKind::AskUserQuestion);
        assert_eq!(parsed.mode, OmxMode::DeepInterview);
        assert_eq!(parsed.status, OmxSessionStatus::WaitingInput);

        let encoded = serde_json::to_value(parsed).expect("payload should serialize");
        assert_eq!(encoded, payload);
    }
}
