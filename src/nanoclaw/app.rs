use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use chrono::Utc;

use crate::foundation::{
    ArtifactKind, ArtifactRecord, ArtifactStore, EventLog, ExecutionBoundary,
    ExecutionBoundaryKind, ExecutionContext, FoundationEvent, FoundationStore, Group,
    ScheduledTask, TaskStatus,
};

use super::config::NanoclawConfig;
use super::db::{NanoclawDb, NanoclawDbCounts};
use super::dev_environment::DigitalOceanDevEnvironment;
use super::queue::GroupQueue;
use super::scheduler::{build_run_log, build_scheduled_task, compute_next_run, TaskScheduleInput};

const MAIN_TEMPLATE: &str =
    "# {assistant}\n\nYou are {assistant}. This is the private NanoClaw admin channel.\n";
const GLOBAL_TEMPLATE: &str = "# {assistant}\n\nYou are {assistant}. Use this as the default template for newly created groups.\n";

pub struct NanoclawApp {
    pub config: NanoclawConfig,
    pub db: NanoclawDb,
    pub queue: GroupQueue,
    artifacts: Vec<ArtifactRecord>,
    events: Vec<FoundationEvent>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootstrapSummary {
    pub db_path: String,
    pub groups_dir: String,
    pub default_trigger: String,
    pub timezone: String,
    pub counts: NanoclawDbCounts,
    pub artifacts_emitted: usize,
    pub foundation_events: usize,
    pub development_environment: String,
}

impl NanoclawApp {
    pub fn open(config: NanoclawConfig) -> Result<Self> {
        let max_active_groups = config.max_concurrent_groups;
        ensure_runtime_layout(&config)?;
        let db = NanoclawDb::open(&config.db_path)?;
        let added_at = Utc::now().to_rfc3339();
        let main_group = Group::main(&config.assistant_name, &added_at);
        db.upsert_group(&main_group)?;
        db.set_router_state("last_timestamp", "")?;

        Ok(Self {
            config,
            db,
            queue: GroupQueue::new(max_active_groups),
            artifacts: Vec::new(),
            events: Vec::new(),
        })
    }

    pub fn bootstrap(config: NanoclawConfig) -> Result<(Self, BootstrapSummary)> {
        let mut app = Self::open(config)?;
        let config = app.config.clone();
        let added_at = Utc::now().to_rfc3339();
        let main_group = Group::main(&config.assistant_name, &added_at);
        let dev_environment = DigitalOceanDevEnvironment::from_config(&config);

        let bootstrap_artifact = ArtifactRecord {
            id: format!("bootstrap:{}", added_at),
            group_id: main_group.jid.clone(),
            task_id: None,
            kind: ArtifactKind::BootstrapSummary,
            title: "NanoClaw foundation bootstrap".to_string(),
            body: format!(
                "project_root={}\ngroups_dir={}\ndb_path={}\ndefault_trigger={}",
                config.project_root.display(),
                config.groups_dir.display(),
                config.db_path.display(),
                config.default_trigger
            ),
            location: Some(config.project_root.display().to_string()),
            created_at: added_at.clone(),
        };
        let dev_environment_artifact = ArtifactRecord {
            id: format!("dev-environment:{}", added_at),
            group_id: main_group.jid.clone(),
            task_id: None,
            kind: ArtifactKind::DevelopmentEnvironmentSnapshot,
            title: "DigitalOcean development environment".to_string(),
            body: format!(
                "kind={:?}\nhost={}\nrepo_root={}\nremote_worker_mode={}",
                config.development_environment.kind,
                config.droplet_ssh_host,
                config.droplet_repo_root,
                config.remote_worker_mode.as_str()
            ),
            location: config.development_environment.repo_root.clone(),
            created_at: added_at.clone(),
        };
        let bootstrap_context = ExecutionContext {
            group_id: main_group.jid.clone(),
            chat_id: Some(main_group.jid.clone()),
            task_id: None,
            boundary: ExecutionBoundary {
                kind: ExecutionBoundaryKind::InProcess,
                root: Some(config.project_root.display().to_string()),
                isolated: false,
            },
            workspace_root: Some(config.project_root.display().to_string()),
        };

        app.record_event(FoundationEvent::GroupRegistered {
            group: main_group.clone(),
        });
        app.record_event(FoundationEvent::RouterStateUpdated {
            key: "last_timestamp".to_string(),
            value: String::new(),
        });
        app.record_event(FoundationEvent::DevelopmentEnvironmentResolved {
            environment: dev_environment.environment().clone(),
        });
        app.record_artifact(bootstrap_artifact.clone());
        app.record_artifact(dev_environment_artifact.clone());
        app.record_event(FoundationEvent::ArtifactEmitted {
            artifact: bootstrap_artifact,
            context: Some(bootstrap_context),
        });
        app.record_event(FoundationEvent::ArtifactEmitted {
            artifact: dev_environment_artifact,
            context: None,
        });

        let summary = BootstrapSummary {
            db_path: app.db.path().display().to_string(),
            groups_dir: app.config.groups_dir.display().to_string(),
            default_trigger: app.config.default_trigger.clone(),
            timezone: app.config.timezone.clone(),
            counts: app.db.counts()?,
            artifacts_emitted: app.artifacts.len(),
            foundation_events: app.events.len(),
            development_environment: app.config.development_environment.name.clone(),
        };

        Ok((app, summary))
    }

    pub fn schedule_task(&mut self, input: TaskScheduleInput) -> Result<ScheduledTask> {
        let task = build_scheduled_task(input, &self.config.timezone, Utc::now())?;
        self.db.create_task(&task)?;
        self.record_event(FoundationEvent::TaskScheduled { task: task.clone() });
        Ok(task)
    }

    pub fn list_tasks(&self) -> Result<Vec<ScheduledTask>> {
        self.db.list_tasks()
    }

    pub fn due_tasks(&self) -> Result<Vec<ScheduledTask>> {
        self.db.list_due_tasks(&Utc::now().to_rfc3339())
    }

    pub fn set_task_status(&self, task_id: &str, status: TaskStatus) -> Result<()> {
        self.db.set_task_status(task_id, status)
    }

    pub fn delete_task(&self, task_id: &str) -> Result<()> {
        self.db.delete_task(task_id)
    }

    pub fn complete_task_run(
        &mut self,
        task_id: &str,
        duration_ms: i64,
        result: Option<String>,
        error: Option<String>,
    ) -> Result<Option<ScheduledTask>> {
        let Some(task) = self.db.get_task_by_id(task_id)? else {
            return Ok(None);
        };
        let now = Utc::now();
        let next_run = compute_next_run(&task, &self.config.timezone, now)?;
        let summary = match (&error, &result) {
            (Some(error), _) => format!("Error: {error}"),
            (None, Some(result)) if !result.trim().is_empty() => result.clone(),
            _ => "Completed".to_string(),
        };
        let log = build_run_log(task_id, duration_ms, result, error, now);
        self.db.log_task_run(&log)?;
        self.db
            .update_task_after_run(task_id, next_run.as_deref(), &summary)?;
        self.db.get_task_by_id(task_id)
    }

    pub fn task(&self, task_id: &str) -> Result<Option<ScheduledTask>> {
        self.db.get_task_by_id(task_id)
    }

    pub fn groups(&self) -> Result<Vec<Group>> {
        self.db.list_groups()
    }

    pub fn register_group(&mut self, group: Group) -> Result<Group> {
        self.db.upsert_group(&group)?;
        ensure_group_folder(&self.config, &group.folder, &self.config.assistant_name)?;
        self.record_event(FoundationEvent::GroupRegistered {
            group: group.clone(),
        });
        Ok(group)
    }

    pub fn ensure_group_for_chat(
        &mut self,
        chat_jid: &str,
        suggested_name: Option<&str>,
    ) -> Result<Group> {
        if let Some(group) = self
            .db
            .list_groups()?
            .into_iter()
            .find(|group| group.jid == chat_jid)
        {
            return Ok(group);
        }

        let group = Group {
            jid: chat_jid.to_string(),
            name: suggested_name.unwrap_or(chat_jid).to_string(),
            folder: normalize_group_folder(chat_jid),
            trigger: self.config.default_trigger.clone(),
            added_at: Utc::now().to_rfc3339(),
            requires_trigger: false,
            is_main: false,
        };
        self.register_group(group)
    }

    pub fn import_registered_groups(
        &mut self,
        source_db_path: &Path,
        source_groups_dir: Option<&Path>,
    ) -> Result<usize> {
        let groups = NanoclawDb::read_registered_groups_from_path(source_db_path)?;
        let mut imported = 0usize;
        for group in groups
            .into_iter()
            .filter(|group| group.jid.starts_with("slack:"))
        {
            self.register_group(group.clone())?;
            if let Some(source_groups_dir) = source_groups_dir {
                copy_group_template(source_groups_dir, &group.folder, &self.config)?;
            }
            imported += 1;
        }
        Ok(imported)
    }

    pub fn artifacts(&self) -> &[ArtifactRecord] {
        &self.artifacts
    }

    pub fn events(&self) -> &[FoundationEvent] {
        &self.events
    }
}

fn ensure_runtime_layout(config: &NanoclawConfig) -> Result<()> {
    for dir in [&config.data_dir, &config.groups_dir, &config.store_dir] {
        fs::create_dir_all(dir).with_context(|| format!("failed to create {}", dir.display()))?;
    }

    let main_dir = config.groups_dir.join("main");
    let global_dir = config.groups_dir.join("global");
    fs::create_dir_all(&main_dir)
        .with_context(|| format!("failed to create {}", main_dir.display()))?;
    fs::create_dir_all(&global_dir)
        .with_context(|| format!("failed to create {}", global_dir.display()))?;

    write_template_if_missing(
        &main_dir.join("CLAUDE.md"),
        &config.assistant_name,
        MAIN_TEMPLATE,
    )?;
    write_template_if_missing(
        &global_dir.join("CLAUDE.md"),
        &config.assistant_name,
        GLOBAL_TEMPLATE,
    )?;
    Ok(())
}

fn ensure_group_folder(config: &NanoclawConfig, folder: &str, assistant_name: &str) -> Result<()> {
    let group_dir = config.groups_dir.join(folder);
    fs::create_dir_all(&group_dir)
        .with_context(|| format!("failed to create {}", group_dir.display()))?;
    write_template_if_missing(
        &group_dir.join("CLAUDE.md"),
        assistant_name,
        GLOBAL_TEMPLATE,
    )?;
    Ok(())
}

fn copy_group_template(
    source_groups_dir: &Path,
    folder: &str,
    config: &NanoclawConfig,
) -> Result<()> {
    let source = source_groups_dir.join(folder).join("CLAUDE.md");
    if !source.exists() {
        return Ok(());
    }
    let destination_dir = config.groups_dir.join(folder);
    fs::create_dir_all(&destination_dir)
        .with_context(|| format!("failed to create {}", destination_dir.display()))?;
    let destination = destination_dir.join("CLAUDE.md");
    fs::copy(&source, &destination).with_context(|| {
        format!(
            "failed to copy group template from {} to {}",
            source.display(),
            destination.display()
        )
    })?;
    Ok(())
}

fn normalize_group_folder(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut previous_was_separator = false;
    for ch in input.chars() {
        let normalized = if ch.is_ascii_alphanumeric() {
            previous_was_separator = false;
            ch.to_ascii_lowercase()
        } else {
            if previous_was_separator {
                continue;
            }
            previous_was_separator = true;
            '_'
        };
        out.push(normalized);
    }
    let trimmed = out.trim_matches('_');
    if trimmed.is_empty() {
        "group".to_string()
    } else {
        trimmed.to_string()
    }
}

impl ArtifactStore for NanoclawApp {
    fn record_artifact(&mut self, artifact: ArtifactRecord) {
        self.artifacts.push(artifact);
    }

    fn artifacts(&self) -> &[ArtifactRecord] {
        self.artifacts.as_slice()
    }
}

impl EventLog for NanoclawApp {
    fn record_event(&mut self, event: FoundationEvent) {
        self.events.push(event);
    }

    fn events(&self) -> &[FoundationEvent] {
        self.events.as_slice()
    }
}

fn write_template_if_missing(
    path: &std::path::Path,
    assistant_name: &str,
    template: &str,
) -> Result<()> {
    if path.exists() {
        return Ok(());
    }

    let body = template.replace("{assistant}", assistant_name);
    fs::write(path, body).with_context(|| format!("failed to write {}", path.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use tempfile::tempdir;

    use super::NanoclawApp;
    use crate::foundation::{
        ArtifactKind, FoundationEvent, TaskContextMode, TaskScheduleType, TaskStatus,
    };
    use crate::nanoclaw::config::NanoclawConfig;
    use crate::nanoclaw::scheduler::TaskScheduleInput;

    #[test]
    fn bootstrap_creates_foundation_lineage() -> Result<()> {
        let temp = tempdir()?;
        let project_root = temp.path().to_path_buf();
        let config = NanoclawConfig {
            project_root: project_root.clone(),
            data_dir: project_root.join("data"),
            groups_dir: project_root.join("groups"),
            store_dir: project_root.join("store"),
            db_path: project_root.join("store/messages.db"),
            assistant_name: "Andy".to_string(),
            default_trigger: "@Andy".to_string(),
            timezone: "UTC".to_string(),
            max_concurrent_groups: 2,
            execution_lane: crate::foundation::ExecutionLane::Auto,
            container_image: "rust:1.75-slim".to_string(),
            container_runtime: None,
            container_groups: Vec::new(),
            droplet_ssh_host: "127.0.0.1".to_string(),
            droplet_ssh_user: "root".to_string(),
            droplet_ssh_port: 22,
            droplet_repo_root: "/srv/code-mirror".to_string(),
            remote_worker_mode: crate::foundation::RemoteWorkerMode::Off,
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
            openclaw_gateway_execution_lane: crate::foundation::ExecutionLane::Host,
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
            sender_allowlist_path: project_root.join("sender-allowlist.json"),
            project_environments_path: project_root.join("project-environments.json"),
            host_os_control_policy_path: project_root.join("host-os-control-policy.json"),
            host_os_approval_chat_jid: None,
            remote_control_ssh_host: None,
            remote_control_ssh_user: None,
            remote_control_ssh_port: 22,
            remote_control_workspace_root: None,
            development_environment: crate::foundation::DevelopmentEnvironment {
                id: "digitalocean-droplet".to_string(),
                name: "DigitalOcean VM".to_string(),
                kind: crate::foundation::DevelopmentEnvironmentKind::DigitalOceanDroplet,
                workspace_root: Some(project_root.display().to_string()),
                repo_root: Some("/srv/code-mirror".to_string()),
                ssh: Some(crate::foundation::SshEndpoint {
                    host: "127.0.0.1".to_string(),
                    user: Some("root".to_string()),
                    port: 22,
                }),
                remote_worker_mode: crate::foundation::RemoteWorkerMode::Off,
                remote_worker_root: Some("/root/.nanoclaw-worker".to_string()),
                bootstrap_timeout_ms: Some(30_000),
                sync_interval_ms: Some(500),
                tunnel_port_base: Some(13_000),
            },
        };

        let (app, summary) = NanoclawApp::bootstrap(config)?;

        assert_eq!(summary.artifacts_emitted, 2);
        assert_eq!(summary.foundation_events, 5);
        assert_eq!(summary.development_environment, "DigitalOcean VM");
        assert_eq!(app.artifacts().len(), 2);
        assert!(matches!(
            app.artifacts()[0].kind,
            ArtifactKind::BootstrapSummary
        ));
        assert!(matches!(
            app.artifacts()[1].kind,
            ArtifactKind::DevelopmentEnvironmentSnapshot
        ));
        assert!(app
            .events()
            .iter()
            .any(|event| matches!(event, FoundationEvent::GroupRegistered { .. })));
        assert!(app.events().iter().any(|event| matches!(
            event,
            FoundationEvent::DevelopmentEnvironmentResolved { .. }
        )));

        Ok(())
    }

    #[test]
    fn schedules_once_task_through_app() -> Result<()> {
        let temp = tempdir()?;
        let project_root = temp.path().to_path_buf();
        let mut app = NanoclawApp::open(NanoclawConfig {
            project_root: project_root.clone(),
            data_dir: project_root.join("data"),
            groups_dir: project_root.join("groups"),
            store_dir: project_root.join("store"),
            db_path: project_root.join("store/messages.db"),
            assistant_name: "Andy".to_string(),
            default_trigger: "@Andy".to_string(),
            timezone: "UTC".to_string(),
            max_concurrent_groups: 2,
            execution_lane: crate::foundation::ExecutionLane::Auto,
            container_image: "rust:1.75-slim".to_string(),
            container_runtime: None,
            container_groups: Vec::new(),
            droplet_ssh_host: "127.0.0.1".to_string(),
            droplet_ssh_user: "root".to_string(),
            droplet_ssh_port: 22,
            droplet_repo_root: "/srv/code-mirror".to_string(),
            remote_worker_mode: crate::foundation::RemoteWorkerMode::Off,
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
            openclaw_gateway_execution_lane: crate::foundation::ExecutionLane::Host,
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
            sender_allowlist_path: project_root.join("sender-allowlist.json"),
            project_environments_path: project_root.join("project-environments.json"),
            host_os_control_policy_path: project_root.join("host-os-control-policy.json"),
            host_os_approval_chat_jid: None,
            remote_control_ssh_host: None,
            remote_control_ssh_user: None,
            remote_control_ssh_port: 22,
            remote_control_workspace_root: None,
            development_environment: crate::foundation::DevelopmentEnvironment {
                id: "digitalocean-droplet".to_string(),
                name: "DigitalOcean VM".to_string(),
                kind: crate::foundation::DevelopmentEnvironmentKind::DigitalOceanDroplet,
                workspace_root: Some(project_root.display().to_string()),
                repo_root: Some("/srv/code-mirror".to_string()),
                ssh: Some(crate::foundation::SshEndpoint {
                    host: "127.0.0.1".to_string(),
                    user: Some("root".to_string()),
                    port: 22,
                }),
                remote_worker_mode: crate::foundation::RemoteWorkerMode::Off,
                remote_worker_root: Some("/root/.nanoclaw-worker".to_string()),
                bootstrap_timeout_ms: Some(30_000),
                sync_interval_ms: Some(500),
                tunnel_port_base: Some(13_000),
            },
        })?;

        let task = app.schedule_task(TaskScheduleInput {
            group_folder: "main".to_string(),
            chat_jid: "main".to_string(),
            prompt: "ping".to_string(),
            script: None,
            request_plane: None,
            schedule_type: TaskScheduleType::Once,
            schedule_value: "2026-04-05T15:00:00Z".to_string(),
            context_mode: TaskContextMode::Isolated,
        })?;

        assert_eq!(app.list_tasks()?.len(), 1);
        assert_eq!(task.status, TaskStatus::Active);
        assert!(app
            .events()
            .iter()
            .any(|event| matches!(event, FoundationEvent::TaskScheduled { .. })));
        Ok(())
    }

    #[test]
    fn ensures_group_for_new_chat() -> Result<()> {
        let temp = tempdir()?;
        let project_root = temp.path().to_path_buf();
        let mut app = NanoclawApp::open(NanoclawConfig {
            project_root: project_root.clone(),
            data_dir: project_root.join("data"),
            groups_dir: project_root.join("groups"),
            store_dir: project_root.join("store"),
            db_path: project_root.join("store/messages.db"),
            assistant_name: "Andy".to_string(),
            default_trigger: "@Andy".to_string(),
            timezone: "UTC".to_string(),
            max_concurrent_groups: 2,
            execution_lane: crate::foundation::ExecutionLane::Auto,
            container_image: "rust:1.75-slim".to_string(),
            container_runtime: None,
            container_groups: Vec::new(),
            droplet_ssh_host: "127.0.0.1".to_string(),
            droplet_ssh_user: "root".to_string(),
            droplet_ssh_port: 22,
            droplet_repo_root: "/srv/code-mirror".to_string(),
            remote_worker_mode: crate::foundation::RemoteWorkerMode::Off,
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
            openclaw_gateway_execution_lane: crate::foundation::ExecutionLane::Host,
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
            sender_allowlist_path: project_root.join("sender-allowlist.json"),
            project_environments_path: project_root.join("project-environments.json"),
            host_os_control_policy_path: project_root.join("host-os-control-policy.json"),
            host_os_approval_chat_jid: None,
            remote_control_ssh_host: None,
            remote_control_ssh_user: None,
            remote_control_ssh_port: 22,
            remote_control_workspace_root: None,
            development_environment: crate::foundation::DevelopmentEnvironment {
                id: "digitalocean-droplet".to_string(),
                name: "DigitalOcean VM".to_string(),
                kind: crate::foundation::DevelopmentEnvironmentKind::DigitalOceanDroplet,
                workspace_root: Some(project_root.display().to_string()),
                repo_root: Some("/srv/code-mirror".to_string()),
                ssh: Some(crate::foundation::SshEndpoint {
                    host: "127.0.0.1".to_string(),
                    user: Some("root".to_string()),
                    port: 22,
                }),
                remote_worker_mode: crate::foundation::RemoteWorkerMode::Off,
                remote_worker_root: Some("/root/.nanoclaw-worker".to_string()),
                bootstrap_timeout_ms: Some(30_000),
                sync_interval_ms: Some(500),
                tunnel_port_base: Some(13_000),
            },
        })?;

        let group = app.ensure_group_for_chat("local:test-room", Some("Test Room"))?;
        assert_eq!(group.folder, "local_test_room");
        assert!(project_root
            .join("groups")
            .join("local_test_room")
            .join("CLAUDE.md")
            .exists());
        Ok(())
    }
}
