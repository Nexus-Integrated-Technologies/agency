use anyhow::Result;
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::{Path, PathBuf};
use uuid::Uuid;

use super::TaskKind;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DurableTaskStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Retrying,
}

impl DurableTaskStatus {
    pub fn parse(input: &str) -> Self {
        match input.trim().to_ascii_lowercase().as_str() {
            "" | "pending" => Self::Pending,
            "running" => Self::Running,
            "completed" => Self::Completed,
            "failed" => Self::Failed,
            "retrying" => Self::Retrying,
            _ => Self::Pending,
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Retrying => "retrying",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DurableTaskRecord {
    pub id: String,
    pub task_kind: TaskKind,
    pub payload: Value,
    pub status: DurableTaskStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub attempts: i32,
    pub last_error: Option<String>,
}

pub trait DurableTaskQueue: Send + Sync {
    fn enqueue(&self, task_kind: &TaskKind, payload: &Value) -> Result<String>;
    fn dequeue(&self) -> Result<Option<DurableTaskRecord>>;
    fn complete(&self, task_id: &str) -> Result<()>;
    fn fail(&self, task_id: &str, error: &str, should_retry: bool) -> Result<()>;
    fn get_status(&self, task_id: &str) -> Result<Option<DurableTaskStatus>>;
    fn count(&self, status: DurableTaskStatus) -> Result<i64>;
}

#[derive(Debug, Clone)]
pub struct SqliteDurableTaskQueue {
    db_path: PathBuf,
}

impl SqliteDurableTaskQueue {
    pub fn new(db_path: impl AsRef<Path>) -> Result<Self> {
        let queue = Self {
            db_path: db_path.as_ref().to_path_buf(),
        };
        queue.ensure_schema()?;
        Ok(queue)
    }

    fn ensure_schema(&self) -> Result<()> {
        let conn = Connection::open(&self.db_path)?;
        conn.execute(
            r#"
            CREATE TABLE IF NOT EXISTS durable_tasks (
                id TEXT PRIMARY KEY,
                task_kind TEXT NOT NULL,
                payload TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                attempts INTEGER DEFAULT 0,
                last_error TEXT
            )
            "#,
            [],
        )?;
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_durable_tasks_status ON durable_tasks(status)",
            [],
        )?;
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_durable_tasks_created_at ON durable_tasks(created_at)",
            [],
        )?;
        Ok(())
    }

    fn open(&self) -> Result<Connection> {
        Ok(Connection::open(&self.db_path)?)
    }
}

impl DurableTaskQueue for SqliteDurableTaskQueue {
    fn enqueue(&self, task_kind: &TaskKind, payload: &Value) -> Result<String> {
        let conn = self.open()?;
        let id = Uuid::new_v4().to_string();
        let now = Utc::now().to_rfc3339();
        conn.execute(
            "INSERT INTO durable_tasks (id, task_kind, payload, status, created_at, updated_at, attempts) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 0)",
            params![
                &id,
                task_kind.as_str(),
                serde_json::to_string(payload)?,
                DurableTaskStatus::Pending.as_str(),
                &now,
                &now
            ],
        )?;
        Ok(id)
    }

    fn dequeue(&self) -> Result<Option<DurableTaskRecord>> {
        let mut conn = self.open()?;
        let transaction = conn.transaction()?;
        let row = transaction
            .query_row(
                r#"
                SELECT id, task_kind, payload, status, created_at, updated_at, attempts, last_error
                FROM durable_tasks
                WHERE status IN ('pending', 'retrying')
                ORDER BY created_at ASC
                LIMIT 1
                "#,
                [],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, String>(5)?,
                        row.get::<_, i32>(6)?,
                        row.get::<_, Option<String>>(7)?,
                    ))
                },
            )
            .optional()?;

        let Some((id, task_kind, payload, status, created_at, updated_at, attempts, last_error)) =
            row
        else {
            return Ok(None);
        };

        transaction.execute(
            "UPDATE durable_tasks SET status = ?1, updated_at = ?2 WHERE id = ?3",
            params![
                DurableTaskStatus::Running.as_str(),
                Utc::now().to_rfc3339(),
                &id
            ],
        )?;
        transaction.commit()?;

        Ok(Some(DurableTaskRecord {
            id,
            task_kind: TaskKind::parse(&task_kind),
            payload: serde_json::from_str(&payload)?,
            status: DurableTaskStatus::parse(&status),
            created_at: DateTime::parse_from_rfc3339(&created_at)?.with_timezone(&Utc),
            updated_at: DateTime::parse_from_rfc3339(&updated_at)?.with_timezone(&Utc),
            attempts,
            last_error,
        }))
    }

    fn complete(&self, task_id: &str) -> Result<()> {
        let conn = self.open()?;
        conn.execute(
            "UPDATE durable_tasks SET status = ?1, updated_at = ?2 WHERE id = ?3",
            params![
                DurableTaskStatus::Completed.as_str(),
                Utc::now().to_rfc3339(),
                task_id
            ],
        )?;
        Ok(())
    }

    fn fail(&self, task_id: &str, error: &str, should_retry: bool) -> Result<()> {
        let conn = self.open()?;
        let status = if should_retry {
            DurableTaskStatus::Retrying
        } else {
            DurableTaskStatus::Failed
        };
        conn.execute(
            "UPDATE durable_tasks SET status = ?1, updated_at = ?2, attempts = attempts + 1, last_error = ?3 WHERE id = ?4",
            params![status.as_str(), Utc::now().to_rfc3339(), error, task_id],
        )?;
        Ok(())
    }

    fn get_status(&self, task_id: &str) -> Result<Option<DurableTaskStatus>> {
        let conn = self.open()?;
        let status = conn
            .query_row(
                "SELECT status FROM durable_tasks WHERE id = ?1",
                params![task_id],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        Ok(status.map(|value| DurableTaskStatus::parse(&value)))
    }

    fn count(&self, status: DurableTaskStatus) -> Result<i64> {
        let conn = self.open()?;
        let count = conn.query_row(
            "SELECT COUNT(*) FROM durable_tasks WHERE status = ?1",
            params![status.as_str()],
            |row| row.get(0),
        )?;
        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::{DurableTaskQueue, DurableTaskStatus, SqliteDurableTaskQueue};
    use crate::foundation::TaskKind;
    use serde_json::json;
    use tempfile::NamedTempFile;

    #[test]
    fn sqlite_durable_queue_round_trips_task() {
        let file = NamedTempFile::new().unwrap();
        let queue = SqliteDurableTaskQueue::new(file.path()).unwrap();
        let task_id = queue
            .enqueue(&TaskKind::Planning, &json!({"prompt":"ship it"}))
            .unwrap();

        let dequeued = queue.dequeue().unwrap().unwrap();
        assert_eq!(dequeued.id, task_id);
        assert_eq!(dequeued.task_kind, TaskKind::Planning);

        queue.complete(&task_id).unwrap();
        assert_eq!(
            queue.get_status(&task_id).unwrap(),
            Some(DurableTaskStatus::Completed)
        );
    }
}
