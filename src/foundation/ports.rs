use anyhow::Result;

use super::domain::{
    ArtifactRecord, DevelopmentEnvironment, Group, MessageRecord, QueueOutcome, QueueSnapshot,
    ScheduledTask, TaskRunLog, TaskStatus,
};
use super::events::FoundationEvent;

pub trait FoundationStore {
    fn upsert_group(&self, group: &Group) -> Result<()>;
    fn list_groups(&self) -> Result<Vec<Group>>;
    fn set_router_state(&self, key: &str, value: &str) -> Result<()>;
    fn get_router_state(&self, key: &str) -> Result<Option<String>>;
    fn store_chat_metadata(
        &self,
        chat_jid: &str,
        timestamp: &str,
        name: Option<&str>,
        channel: Option<&str>,
        is_group: Option<bool>,
    ) -> Result<()>;
    fn store_message(&self, message: &MessageRecord) -> Result<()>;
    fn messages_since(
        &self,
        chat_jid: &str,
        since_timestamp: &str,
        limit: usize,
        include_bot_messages: bool,
    ) -> Result<Vec<MessageRecord>>;
    fn create_task(&self, task: &ScheduledTask) -> Result<()>;
    fn get_task_by_id(&self, task_id: &str) -> Result<Option<ScheduledTask>>;
    fn list_tasks(&self) -> Result<Vec<ScheduledTask>>;
    fn list_tasks_for_group(&self, group_folder: &str) -> Result<Vec<ScheduledTask>>;
    fn list_due_tasks(&self, now_iso: &str) -> Result<Vec<ScheduledTask>>;
    fn update_task_after_run(
        &self,
        task_id: &str,
        next_run: Option<&str>,
        last_result: &str,
    ) -> Result<()>;
    fn set_task_status(&self, task_id: &str, status: TaskStatus) -> Result<()>;
    fn delete_task(&self, task_id: &str) -> Result<()>;
    fn log_task_run(&self, log: &TaskRunLog) -> Result<()>;
}

pub trait WorkQueue {
    fn enqueue_group_messages(&mut self, group_id: &str) -> QueueOutcome;
    fn enqueue_group_task(&mut self, group_id: &str, task_id: &str) -> QueueOutcome;
    fn finish_group(&mut self, group_id: &str);
    fn snapshots(&self) -> Vec<QueueSnapshot>;
}

pub trait ArtifactStore {
    fn record_artifact(&mut self, artifact: ArtifactRecord);
    fn artifacts(&self) -> &[ArtifactRecord];
}

pub trait DevelopmentEnvironmentProvider {
    fn development_environment(&self) -> &DevelopmentEnvironment;
}

pub trait EventLog {
    fn record_event(&mut self, event: FoundationEvent);
    fn events(&self) -> &[FoundationEvent];
}
