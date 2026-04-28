use std::collections::BTreeSet;
use std::fs;
use std::time::Instant;

use anyhow::Result;
use chrono::Utc;
use uuid::Uuid;

use crate::foundation::{
    ArtifactKind, ArtifactRecord, ArtifactStore, EventLog, ExecutionContext, FoundationEvent,
    Group, MessageRecord, ScheduledTask, TaskContextMode,
};

use super::app::NanoclawApp;
use super::executor::{
    build_execution_session, ExecutionRequest, ExecutionSession, ExecutorBoundary,
};
use super::fpf_bridge::{build_boundary_claims, derive_task_signature};
use super::ingress_policy::{has_authorized_trigger, should_drop_inbound_message};
use super::local_channel::{LocalChannel, LocalInboundEnvelope, LocalOutboundEnvelope};
use super::request_plane::default_request_plane;
use super::router::{format_messages, format_outbound, format_task_request};
use super::sender_allowlist::load_sender_allowlist;
use super::swarm::pump_swarm_once;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimePumpSummary {
    pub inbound_messages: usize,
    pub processed_groups: usize,
    pub scheduled_tasks_run: usize,
    pub scheduled_task_errors: usize,
    pub swarm_tasks_run: usize,
    pub swarm_task_errors: usize,
    pub outbound_messages: usize,
}

pub struct LocalRuntime<E> {
    pub app: NanoclawApp,
    pub channel: LocalChannel,
    pub executor: E,
}

impl<E: ExecutorBoundary> LocalRuntime<E> {
    pub fn new(app: NanoclawApp, executor: E) -> Result<Self> {
        let channel = LocalChannel::new(&app.config.data_dir)?;
        Ok(Self {
            app,
            channel,
            executor,
        })
    }

    pub fn enqueue_local_message(
        &self,
        envelope: LocalInboundEnvelope,
    ) -> Result<std::path::PathBuf> {
        self.channel.enqueue_inbound(envelope)
    }

    pub fn poll_once(&mut self) -> Result<RuntimePumpSummary> {
        let inbound = self.channel.poll_inbound()?;
        let inbound_count = inbound.len();
        let mut chats = BTreeSet::new();

        for message in inbound {
            let allowlist = load_sender_allowlist(&self.app.config.sender_allowlist_path);
            if should_drop_inbound_message(&message, &allowlist) {
                continue;
            }
            let group = self
                .app
                .ensure_group_for_chat(&message.chat_jid, message.sender_name.as_deref())?;
            self.app.db.store_chat_metadata(
                &message.chat_jid,
                &message.timestamp,
                Some(&group.name),
                Some("local"),
                Some(false),
            )?;
            self.app.db.store_message(&message)?;
            self.app.record_event(FoundationEvent::MessageRecorded {
                message: message.clone(),
            });
            let _ = self.app.queue.enqueue_message_check(&message.chat_jid);
            chats.insert(message.chat_jid);
        }

        let mut outbound_count = 0;
        for chat_jid in &chats {
            if self.process_group_messages(chat_jid)? {
                outbound_count += 1;
            }
            self.app.queue.finish_group(chat_jid);
        }

        let (scheduled_tasks_run, scheduled_task_errors, task_outbound_messages) =
            self.run_due_tasks()?;
        outbound_count += task_outbound_messages;
        let swarm_summary = pump_swarm_once(&mut self.app, &self.executor)?;

        Ok(RuntimePumpSummary {
            inbound_messages: inbound_count,
            processed_groups: chats.len(),
            scheduled_tasks_run,
            scheduled_task_errors,
            swarm_tasks_run: swarm_summary.tasks_completed,
            swarm_task_errors: swarm_summary.tasks_failed,
            outbound_messages: outbound_count,
        })
    }

    pub fn read_outbox(&self) -> Result<Vec<LocalOutboundEnvelope>> {
        self.channel.read_outbox()
    }

    fn process_group_messages(&mut self, chat_jid: &str) -> Result<bool> {
        let cursor_key = format!("last_agent_timestamp:{}", chat_jid);
        let since = self.app.db.router_state(&cursor_key)?.unwrap_or_default();
        let messages = self.app.db.messages_since(chat_jid, &since, 200, false)?;
        if messages.is_empty() {
            return Ok(false);
        }

        let group = self
            .app
            .groups()?
            .into_iter()
            .find(|group| group.jid == chat_jid)
            .expect("group should exist before processing messages");
        let allowlist = load_sender_allowlist(&self.app.config.sender_allowlist_path);
        if !has_authorized_trigger(&group, &messages, &allowlist)? {
            return Ok(false);
        }
        let session = self.ensure_execution_session(&group)?;
        let prompt = format_messages(&messages, &self.app.config.timezone)?;
        let request_plane = default_request_plane();
        let created_at = Utc::now().to_rfc3339();
        let task_signature =
            derive_task_signature(&prompt, &messages, None, &request_plane, false, &created_at);
        let boundary_claims =
            build_boundary_claims(&group.folder, None, &prompt, &request_plane, &created_at);
        let execution = self.executor.execute(ExecutionRequest {
            group: group.clone(),
            prompt,
            paperclip_overlay_context: None,
            messages: messages.clone(),
            task_id: None,
            script: None,
            omx: None,
            assistant_name: self.app.config.assistant_name.clone(),
            request_plane,
            env: Default::default(),
            session: session.clone(),
            backend_override: None,
            task_signature: Some(task_signature),
            routing_decision: None,
            objective: None,
            plan: None,
            boundary_claims,
            gate_evaluation: None,
        })?;
        self.record_execution_provenance(&execution)?;
        self.record_execution_log_artifact(&group, None, &session, &execution)?;
        let sent = self.deliver_response(
            &group,
            None,
            &execution.text,
            ArtifactKind::Transcript,
            format!("Local runtime response for {}", group.name),
            execution.boundary,
        )?;
        self.app.db.upsert_router_state(
            &cursor_key,
            &messages
                .last()
                .map(|message| message.timestamp.clone())
                .unwrap_or_else(|| Utc::now().to_rfc3339()),
        )?;
        self.app.record_event(FoundationEvent::RouterStateUpdated {
            key: cursor_key,
            value: messages
                .last()
                .map(|message| message.timestamp.clone())
                .unwrap_or_else(|| Utc::now().to_rfc3339()),
        });
        Ok(sent)
    }

    fn run_due_tasks(&mut self) -> Result<(usize, usize, usize)> {
        let due_tasks = self.app.due_tasks()?;
        let mut executed = 0;
        let mut errors = 0;
        let mut outbound_messages = 0;

        for task in due_tasks {
            let _ = self.app.queue.enqueue_task(&task.chat_jid, &task.id);
            let started = Instant::now();
            let result = self.execute_due_task(&task);
            let duration_ms = started.elapsed().as_millis() as i64;
            match result {
                Ok(outbound) => {
                    executed += 1;
                    if outbound {
                        outbound_messages += 1;
                    }
                }
                Err(error) => {
                    errors += 1;
                    self.app.complete_task_run(
                        &task.id,
                        duration_ms,
                        None,
                        Some(error.to_string()),
                    )?;
                }
            }
            self.app.queue.finish_group(&task.chat_jid);
        }

        Ok((executed, errors, outbound_messages))
    }

    fn execute_due_task(&mut self, task: &ScheduledTask) -> Result<bool> {
        let started = Instant::now();
        let group = self
            .app
            .ensure_group_for_chat(&task.chat_jid, Some(&task.group_folder))?;
        let session = self.ensure_execution_session(&group)?;
        let context_messages = match task.context_mode {
            TaskContextMode::Group => self.app.db.messages_since(&task.chat_jid, "", 50, true)?,
            _ => Vec::new(),
        };
        let prompt = format_task_request(task, &context_messages, &self.app.config.timezone)?;
        let request_plane = task
            .request_plane
            .clone()
            .unwrap_or_else(default_request_plane);
        let created_at = Utc::now().to_rfc3339();
        let task_signature = derive_task_signature(
            &prompt,
            &context_messages,
            task.script.as_deref(),
            &request_plane,
            false,
            &created_at,
        );
        let boundary_claims = build_boundary_claims(
            &group.folder,
            Some(&task.id),
            &prompt,
            &request_plane,
            &created_at,
        );
        let execution = self.executor.execute(ExecutionRequest {
            group: group.clone(),
            prompt,
            paperclip_overlay_context: None,
            messages: context_messages,
            task_id: Some(task.id.clone()),
            script: task.script.clone(),
            omx: None,
            assistant_name: self.app.config.assistant_name.clone(),
            request_plane,
            env: Default::default(),
            session: session.clone(),
            backend_override: None,
            task_signature: Some(task_signature),
            routing_decision: None,
            objective: None,
            plan: None,
            boundary_claims,
            gate_evaluation: None,
        })?;
        self.record_execution_provenance(&execution)?;
        self.record_execution_log_artifact(&group, Some(&task.id), &session, &execution)?;
        let sent = self.deliver_response(
            &group,
            Some(&task.id),
            &execution.text,
            ArtifactKind::TaskResult,
            format!("Scheduled task result for {}", group.name),
            execution.boundary,
        )?;
        let summary = if sent {
            Some(execution.text)
        } else {
            Some("Task completed without outbound text.".to_string())
        };
        self.app.complete_task_run(
            &task.id,
            started.elapsed().as_millis() as i64,
            summary,
            None,
        )?;
        Ok(sent)
    }

    fn ensure_execution_session(&self, group: &Group) -> Result<ExecutionSession> {
        let session_id = self
            .app
            .db
            .session_for_group(&group.folder)?
            .unwrap_or_else(|| format!("session-{}", Uuid::new_v4()));
        self.app.db.upsert_session(&group.folder, &session_id)?;
        let session = build_execution_session(
            &self.app.config.data_dir,
            &group.folder,
            &session_id,
            &self.app.config.groups_dir.join(&group.folder),
        );
        session.ensure_layout()?;
        Ok(session)
    }

    fn record_execution_log_artifact(
        &mut self,
        group: &Group,
        task_id: Option<&str>,
        session: &ExecutionSession,
        execution: &super::executor::ExecutionResponse,
    ) -> Result<()> {
        let Some(log_path) = execution.log_path.as_deref() else {
            return Ok(());
        };
        let body = if let Some(log_body) = execution.log_body.as_deref() {
            log_body.to_string()
        } else {
            fs::read_to_string(log_path)?
        };
        let created_at = Utc::now().to_rfc3339();
        let artifact = ArtifactRecord {
            id: format!("artifact:exec-log:{}:{}", session.id, created_at),
            group_id: group.jid.clone(),
            task_id: task_id.map(str::to_string),
            kind: ArtifactKind::ExecutionLog,
            title: format!("Execution log for {}", group.name),
            body,
            location: Some(log_path.to_string()),
            created_at: created_at.clone(),
        };
        self.app.record_artifact(artifact.clone());
        self.app.record_event(FoundationEvent::ArtifactEmitted {
            artifact,
            context: Some(ExecutionContext {
                group_id: group.jid.clone(),
                chat_id: Some(group.jid.clone()),
                task_id: task_id.map(str::to_string),
                boundary: execution.boundary.clone(),
                workspace_root: Some(session.workspace_root.clone()),
            }),
        });
        Ok(())
    }

    fn record_execution_provenance(
        &mut self,
        execution: &super::executor::ExecutionResponse,
    ) -> Result<()> {
        let Some(provenance) = execution.provenance.as_ref() else {
            return Ok(());
        };
        self.app.db.create_execution_provenance(provenance)?;
        Ok(())
    }

    fn deliver_response(
        &mut self,
        group: &Group,
        task_id: Option<&str>,
        raw_text: &str,
        kind: ArtifactKind,
        title: String,
        boundary: crate::foundation::ExecutionBoundary,
    ) -> Result<bool> {
        let text = format_outbound(raw_text);
        if text.is_empty() {
            return Ok(false);
        }

        let outbound = self.channel.send_message(&group.jid, &text)?;
        let bot_message = MessageRecord {
            id: outbound.id.clone(),
            chat_jid: outbound.chat_jid.clone(),
            sender: self.app.config.assistant_name.clone(),
            sender_name: Some(self.app.config.assistant_name.clone()),
            content: text.clone(),
            timestamp: outbound.timestamp.clone(),
            is_from_me: true,
            is_bot_message: true,
        };
        self.app.db.store_chat_metadata(
            &group.jid,
            &outbound.timestamp,
            Some(&group.name),
            Some("local"),
            Some(false),
        )?;
        self.app.db.store_message(&bot_message)?;
        self.app.record_event(FoundationEvent::MessageRecorded {
            message: bot_message.clone(),
        });
        let artifact = ArtifactRecord {
            id: format!("artifact:{}", outbound.id),
            group_id: group.jid.clone(),
            task_id: task_id.map(str::to_string),
            kind,
            title,
            body: text,
            location: Some(self.channel.outbox_path().display().to_string()),
            created_at: outbound.timestamp.clone(),
        };
        self.app.record_artifact(artifact.clone());
        self.app.record_event(FoundationEvent::ArtifactEmitted {
            artifact,
            context: Some(ExecutionContext {
                group_id: group.jid.clone(),
                chat_id: Some(group.jid.clone()),
                task_id: task_id.map(str::to_string),
                boundary,
                workspace_root: Some(
                    self.app
                        .config
                        .groups_dir
                        .join(&group.folder)
                        .display()
                        .to_string(),
                ),
            }),
        });
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use crate::foundation::{
        DevelopmentEnvironment, DevelopmentEnvironmentKind, RemoteWorkerMode, SshEndpoint,
    };
    use crate::nanoclaw::config::NanoclawConfig;
    use crate::nanoclaw::executor::InProcessEchoExecutor;

    use super::{LocalChannel, LocalRuntime};

    #[test]
    fn runtime_processes_local_inbox_to_outbox() {
        let temp = tempdir().unwrap();
        let project_root = temp.path().to_path_buf();
        let app = crate::nanoclaw::app::NanoclawApp::open(NanoclawConfig {
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
            openclaw_gateway_public_ws_url: None,
            openclaw_gateway_public_health_url: None,
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
        })
        .unwrap();
        let mut runtime = LocalRuntime::new(app, InProcessEchoExecutor::new("Andy")).unwrap();
        let channel = LocalChannel::new(&project_root.join("data")).unwrap();
        channel
            .enqueue_inbound(crate::nanoclaw::local_channel::LocalInboundEnvelope {
                id: Some("message-1".to_string()),
                chat_jid: "main".to_string(),
                sender: "user".to_string(),
                sender_name: Some("User".to_string()),
                content: "hello".to_string(),
                timestamp: Some("2026-04-05T12:00:00Z".to_string()),
            })
            .unwrap();

        let summary = runtime.poll_once().unwrap();
        assert_eq!(summary.inbound_messages, 1);
        assert_eq!(summary.scheduled_tasks_run, 0);
        assert_eq!(summary.outbound_messages, 1);
        let outbox = runtime.read_outbox().unwrap();
        assert_eq!(outbox.len(), 1);
        assert!(outbox[0].text.contains("Andy received 1 message"));
    }

    #[test]
    fn runtime_executes_due_tasks_to_outbox() {
        let temp = tempdir().unwrap();
        let project_root = temp.path().to_path_buf();
        let mut app = crate::nanoclaw::app::NanoclawApp::open(NanoclawConfig {
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
            openclaw_gateway_public_ws_url: None,
            openclaw_gateway_public_health_url: None,
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
        })
        .unwrap();
        let task = app
            .schedule_task(crate::nanoclaw::scheduler::TaskScheduleInput {
                group_folder: "main".to_string(),
                chat_jid: "main".to_string(),
                prompt: "daily check".to_string(),
                script: None,
                request_plane: None,
                schedule_type: crate::foundation::TaskScheduleType::Once,
                schedule_value: "2026-04-05T00:00:00Z".to_string(),
                context_mode: crate::foundation::TaskContextMode::Isolated,
            })
            .unwrap();

        let mut runtime = LocalRuntime::new(app, InProcessEchoExecutor::new("Andy")).unwrap();
        let summary = runtime.poll_once().unwrap();
        assert_eq!(summary.inbound_messages, 0);
        assert_eq!(summary.scheduled_tasks_run, 1);
        assert_eq!(summary.scheduled_task_errors, 0);
        assert_eq!(summary.outbound_messages, 1);

        let stored_task = runtime.app.task(&task.id).unwrap().unwrap();
        assert_eq!(stored_task.status, crate::foundation::TaskStatus::Completed);
        let outbox = runtime.read_outbox().unwrap();
        assert_eq!(outbox.len(), 1);
        assert!(outbox[0].text.contains("Andy received 0 message"));
    }
}
