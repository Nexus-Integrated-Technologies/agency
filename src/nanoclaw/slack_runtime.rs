use std::collections::BTreeMap;
use std::thread;
use std::time::{Duration, Instant};

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
use super::omx::{maybe_inject_slack_reply, OmxExecutionOptions, OmxMode};
use super::request_plane::default_request_plane;
use super::router::{format_messages, format_outbound, format_task_request};
use super::sender_allowlist::{is_trigger_allowed, load_sender_allowlist};
use super::slack::{SlackChannel, SlackInboundEvent};
use super::slack_threading::get_slack_base_jid;
use super::swarm::pump_swarm_once;

#[derive(Debug, Clone, PartialEq, Eq)]
struct SlackOmxTrigger {
    prompt: String,
    options: OmxExecutionOptions,
}

pub struct SlackRuntime<E> {
    pub app: NanoclawApp,
    pub channel: SlackChannel,
    pub executor: E,
    tick_interval: Duration,
}

impl<E: ExecutorBoundary> SlackRuntime<E> {
    pub fn new(app: NanoclawApp, channel: SlackChannel, executor: E) -> Self {
        let tick_interval = Duration::from_millis(app.config.slack_poll_interval_ms);
        Self {
            app,
            channel,
            executor,
            tick_interval,
        }
    }

    pub fn sync_channel_metadata(&mut self) -> Result<usize> {
        let now = Utc::now().to_rfc3339();
        let channels = self.channel.sync_channel_metadata()?;
        let count = channels.len();
        for (jid, name) in channels {
            self.app
                .db
                .store_chat_metadata(&jid, &now, Some(&name), Some("slack"), Some(true))?;
        }
        Ok(count)
    }

    pub fn run_forever(&mut self) -> Result<()> {
        let _ = self.sync_channel_metadata();
        loop {
            match self.run_socket_session() {
                Ok(()) => {}
                Err(error) => {
                    eprintln!("slack runtime reconnecting after error: {error:#}");
                    thread::sleep(Duration::from_secs(2));
                }
            }
        }
    }

    fn run_socket_session(&mut self) -> Result<()> {
        let mut socket = self.channel.connect_socket()?;
        let mut last_tick = Instant::now();
        loop {
            let registered_groups = self.registered_groups()?;
            let event = self.channel.read_event(&mut socket, &registered_groups)?;
            let had_event = event.is_some();
            if let Some(event) = event {
                self.ingest_slack_event(event)?;
            }

            if last_tick.elapsed() >= self.tick_interval {
                let _ = self.run_due_tasks()?;
                let _ = pump_swarm_once(&mut self.app, &self.executor)?;
                last_tick = Instant::now();
            }

            if !had_event {
                thread::sleep(Duration::from_millis(50));
            }
        }
    }

    fn registered_groups(&self) -> Result<BTreeMap<String, Group>> {
        Ok(self
            .app
            .groups()?
            .into_iter()
            .map(|group| (group.jid.clone(), group))
            .collect())
    }

    fn ingest_slack_event(&mut self, event: SlackInboundEvent) -> Result<()> {
        if let Some(group) = event.derived_group.clone() {
            self.app.register_group(group)?;
        }
        let allowlist = load_sender_allowlist(&self.app.config.sender_allowlist_path);
        if should_drop_inbound_message(&event.message, &allowlist) {
            return Ok(());
        }
        self.app.db.store_chat_metadata(
            &event.base_jid,
            &event.message.timestamp,
            event.base_name.as_deref(),
            Some("slack"),
            Some(event.is_group),
        )?;
        if event.message.chat_jid != event.base_jid {
            self.app.db.store_chat_metadata(
                &event.message.chat_jid,
                &event.message.timestamp,
                event.effective_name.as_deref(),
                Some("slack"),
                Some(false),
            )?;
        }
        self.app.db.store_message(&event.message)?;
        self.app.record_event(FoundationEvent::MessageRecorded {
            message: event.message.clone(),
        });
        if maybe_inject_slack_reply(&mut self.app, &event.message)? {
            return Ok(());
        }
        let _ = self
            .app
            .queue
            .enqueue_message_check(&event.message.chat_jid);
        let _ = self.process_group_messages(&event.message.chat_jid)?;
        self.app.queue.finish_group(&event.message.chat_jid);
        Ok(())
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
        let omx_trigger = messages
            .last()
            .and_then(|message| parse_slack_omx_trigger(&message.content, &group.trigger));
        let omx_trigger_allowed = omx_trigger.is_some()
            && messages
                .last()
                .map(|message| {
                    message.is_from_me
                        || is_trigger_allowed(&group.jid, &message.sender, &allowlist)
                })
                .unwrap_or(false);
        if !has_authorized_trigger(&group, &messages, &allowlist)? && !omx_trigger_allowed {
            return Ok(false);
        }
        let session = self.ensure_execution_session(&group)?;
        let mut execution_messages = messages.clone();
        let omx_options = omx_trigger.as_ref().map(|trigger| trigger.options.clone());
        if let Some(trigger) = omx_trigger.as_ref() {
            if let Some(last_message) = execution_messages.last_mut() {
                last_message.content = trigger.prompt.clone();
            }
        }
        let prompt = format_messages(&execution_messages, &self.app.config.timezone)?;
        let request_plane = default_request_plane();
        let created_at = Utc::now().to_rfc3339();
        let task_signature = derive_task_signature(
            &prompt,
            &execution_messages,
            None,
            &request_plane,
            omx_options.is_some(),
            &created_at,
        );
        let boundary_claims =
            build_boundary_claims(&group.folder, None, &prompt, &request_plane, &created_at);
        let execution = self.executor.execute(ExecutionRequest {
            group: group.clone(),
            prompt,
            messages: execution_messages,
            task_id: None,
            script: None,
            omx: omx_options,
            assistant_name: self.app.config.assistant_name.clone(),
            request_plane,
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
            format!("Slack runtime response for {}", group.name),
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
            .groups()?
            .into_iter()
            .find(|group| group.jid == task.chat_jid)
            .unwrap_or_else(|| Group {
                jid: task.chat_jid.clone(),
                name: task.group_folder.clone(),
                folder: task.group_folder.clone(),
                trigger: self.app.config.default_trigger.clone(),
                added_at: Utc::now().to_rfc3339(),
                requires_trigger: false,
                is_main: false,
            });
        let group = self.app.register_group(group)?;
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
            messages: context_messages,
            task_id: Some(task.id.clone()),
            script: task.script.clone(),
            omx: None,
            assistant_name: self.app.config.assistant_name.clone(),
            request_plane,
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
            std::fs::read_to_string(log_path)?
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

        let Some(outbound) = self.channel.send_message(&group.jid, &text)? else {
            return Ok(false);
        };
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
        let base_jid = get_slack_base_jid(&group.jid).unwrap_or_else(|| group.jid.clone());
        let is_group = !base_jid.trim_start_matches("slack:").starts_with('D');
        self.app.db.store_chat_metadata(
            &base_jid,
            &outbound.timestamp,
            Some(&group.name),
            Some("slack"),
            Some(is_group),
        )?;
        if group.jid != base_jid {
            self.app.db.store_chat_metadata(
                &group.jid,
                &outbound.timestamp,
                Some(&group.name),
                Some("slack"),
                Some(false),
            )?;
        }
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
            location: Some(format!("slack://{}", group.jid)),
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

fn parse_slack_omx_trigger(input: &str, trigger: &str) -> Option<SlackOmxTrigger> {
    let trimmed = strip_optional_trigger_prefix(input, trigger);
    let (allows_mode, body) = if let Some(rest) = strip_ascii_prefix(trimmed, "omx:") {
        (false, rest)
    } else if let Some(rest) = strip_ascii_prefix(trimmed, "!omx:") {
        (false, rest)
    } else if let Some(rest) = strip_command_body(trimmed, "omx") {
        (true, rest)
    } else if let Some(rest) = strip_command_body(trimmed, "!omx") {
        (true, rest)
    } else {
        return None;
    };
    let body = body.trim();
    if body.is_empty() {
        return None;
    }

    let (mode, prompt) = if allows_mode {
        if let Some((mode, prompt)) = parse_omx_mode_prefix(body) {
            (mode, prompt)
        } else {
            (OmxMode::Team, body)
        }
    } else {
        (OmxMode::Team, body)
    };
    let prompt = prompt.trim();
    if prompt.is_empty() {
        return None;
    }

    Some(SlackOmxTrigger {
        prompt: prompt.to_string(),
        options: OmxExecutionOptions {
            mode,
            max_workers: None,
            external_run_id: None,
        },
    })
}

fn strip_optional_trigger_prefix<'a>(input: &'a str, trigger: &str) -> &'a str {
    let trimmed = input.trim();
    let Some(rest) = strip_ascii_prefix(trimmed, trigger.trim()) else {
        return trimmed;
    };
    rest.trim_start_matches(|ch: char| ch.is_whitespace() || matches!(ch, ':' | ',' | '-'))
        .trim_start()
}

fn strip_command_body<'a>(input: &'a str, command: &str) -> Option<&'a str> {
    let rest = strip_ascii_prefix(input, command)?;
    if rest.is_empty() {
        return None;
    }
    let mut chars = rest.chars();
    let separator = chars.next()?;
    if !separator.is_whitespace() {
        return None;
    }
    Some(rest.trim_start())
}

fn strip_ascii_prefix<'a>(input: &'a str, prefix: &str) -> Option<&'a str> {
    if input.len() < prefix.len() {
        return None;
    }
    let (head, tail) = input.split_at(prefix.len());
    head.eq_ignore_ascii_case(prefix).then_some(tail)
}

fn parse_omx_mode_prefix(input: &str) -> Option<(OmxMode, &str)> {
    let token = input.split_whitespace().next()?;
    let mode = parse_named_omx_mode(token.trim_end_matches(':'))?;
    let rest = input[token.len()..].trim();
    Some((mode, rest))
}

fn parse_named_omx_mode(token: &str) -> Option<OmxMode> {
    match token.trim().to_ascii_lowercase().as_str() {
        "exec" => Some(OmxMode::Exec),
        "team" => Some(OmxMode::Team),
        "ralph" => Some(OmxMode::Ralph),
        "deep-interview" | "deep_interview" | "deepinterview" => Some(OmxMode::DeepInterview),
        "ralplan" => Some(OmxMode::Ralplan),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{parse_slack_omx_trigger, OmxMode};

    #[test]
    fn parses_simple_omx_trigger() {
        let trigger = parse_slack_omx_trigger("omx: refactor the parser", "@Andy").unwrap();
        assert_eq!(trigger.options.mode, OmxMode::Team);
        assert_eq!(trigger.prompt, "refactor the parser");
    }

    #[test]
    fn parses_trigger_after_assistant_mention() {
        let trigger = parse_slack_omx_trigger("@Andy omx team build a plan", "@Andy").unwrap();
        assert_eq!(trigger.options.mode, OmxMode::Team);
        assert_eq!(trigger.prompt, "build a plan");
    }

    #[test]
    fn parses_named_omx_mode() {
        let trigger =
            parse_slack_omx_trigger("!omx ralplan: decompose this feature", "@Andy").unwrap();
        assert_eq!(trigger.options.mode, OmxMode::Ralplan);
        assert_eq!(trigger.prompt, "decompose this feature");
    }

    #[test]
    fn ignores_non_omx_messages() {
        assert!(parse_slack_omx_trigger("hello there", "@Andy").is_none());
    }
}
