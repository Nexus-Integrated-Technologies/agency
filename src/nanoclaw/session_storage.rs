use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use chrono::Utc;
use rusqlite::{params, Connection};
use uuid::Uuid;

use crate::foundation::{Group, MessageRecord, ScheduledTask};

use super::executor::ExecutionSession;
use super::router::DestinationEntry;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionSidecarPaths {
    pub session_root: PathBuf,
    pub inbound_db: PathBuf,
    pub outbound_db: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboundSidecarMessage {
    pub id: String,
    pub in_reply_to: Option<String>,
    pub timestamp: String,
    pub kind: String,
    pub chat_jid: String,
    pub content: String,
}

pub fn session_sidecar_paths(session: &ExecutionSession) -> SessionSidecarPaths {
    let session_root = PathBuf::from(&session.session_root);
    SessionSidecarPaths {
        inbound_db: session_root.join("inbound.db"),
        outbound_db: session_root.join("outbound.db"),
        session_root,
    }
}

pub fn ensure_session_sidecars(session: &ExecutionSession) -> Result<SessionSidecarPaths> {
    session.ensure_layout()?;
    let paths = session_sidecar_paths(session);
    ensure_inbound_db(&paths.inbound_db)?;
    ensure_outbound_db(&paths.outbound_db)?;
    Ok(paths)
}

pub fn record_inbound_message(
    paths: &SessionSidecarPaths,
    message: &MessageRecord,
    source_session_id: Option<&str>,
    on_wake: bool,
) -> Result<()> {
    let conn = open_sidecar_writer(&paths.inbound_db)?;
    let seq = next_sequence(&conn, "messages_in")?;
    conn.execute(
        r#"
        INSERT OR IGNORE INTO messages_in (
          id, seq, kind, timestamp, status, chat_jid, content,
          source_session_id, trigger, on_wake
        )
        VALUES (?1, ?2, ?3, ?4, 'pending', ?5, ?6, ?7, 1, ?8)
        "#,
        params![
            message.id,
            seq,
            if on_wake { "wake" } else { "message" },
            message.timestamp,
            message.chat_jid,
            message.content,
            source_session_id,
            if on_wake { 1 } else { 0 },
        ],
    )
    .with_context(|| {
        format!(
            "failed to record inbound message {} in {}",
            message.id,
            paths.inbound_db.display()
        )
    })?;
    Ok(())
}

pub fn record_task_request(
    paths: &SessionSidecarPaths,
    task: &ScheduledTask,
    content: &str,
    timestamp: &str,
) -> Result<String> {
    let id = format!("task-in-{}-{}", task.id, Uuid::new_v4());
    let conn = open_sidecar_writer(&paths.inbound_db)?;
    let seq = next_sequence(&conn, "messages_in")?;
    conn.execute(
        r#"
        INSERT INTO messages_in (
          id, seq, kind, timestamp, status, chat_jid, content,
          source_session_id, trigger, on_wake
        )
        VALUES (?1, ?2, 'task', ?3, 'pending', ?4, ?5, NULL, 1, 0)
        "#,
        params![id, seq, timestamp, task.chat_jid, content],
    )
    .with_context(|| {
        format!(
            "failed to record task request {} in {}",
            task.id,
            paths.inbound_db.display()
        )
    })?;
    Ok(id)
}

pub fn record_on_wake_message(
    paths: &SessionSidecarPaths,
    group: &Group,
    content: &str,
) -> Result<String> {
    let id = format!("wake-{}", Uuid::new_v4());
    let message = MessageRecord {
        id: id.clone(),
        chat_jid: group.jid.clone(),
        sender: "system".to_string(),
        sender_name: Some("NanoClaw".to_string()),
        content: content.to_string(),
        timestamp: Utc::now().to_rfc3339(),
        is_from_me: false,
        is_bot_message: false,
    };
    record_inbound_message(paths, &message, None, true)?;
    Ok(id)
}

pub fn record_outbound_message(
    paths: &SessionSidecarPaths,
    message: &OutboundSidecarMessage,
) -> Result<()> {
    let conn = open_sidecar_writer(&paths.outbound_db)?;
    let seq = next_sequence(&conn, "messages_out")?;
    conn.execute(
        r#"
        INSERT OR IGNORE INTO messages_out (
          id, seq, in_reply_to, timestamp, kind, chat_jid, content
        )
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
        "#,
        params![
            message.id,
            seq,
            message.in_reply_to,
            message.timestamp,
            message.kind,
            message.chat_jid,
            message.content,
        ],
    )
    .with_context(|| {
        format!(
            "failed to record outbound message {} in {}",
            message.id,
            paths.outbound_db.display()
        )
    })?;
    Ok(())
}

pub fn refresh_destinations(
    paths: &SessionSidecarPaths,
    current_group: &Group,
    destinations: &[DestinationEntry],
) -> Result<()> {
    let conn = open_sidecar_writer(&paths.inbound_db)?;
    conn.execute("DELETE FROM destinations", [])
        .with_context(|| format!("failed to clear {}", paths.inbound_db.display()))?;
    let updated_at = Utc::now().to_rfc3339();
    for destination in destinations {
        conn.execute(
            r#"
            INSERT INTO destinations (
              name, display_name, chat_jid, group_folder, updated_at
            )
            VALUES (?1, ?2, ?3, ?4, ?5)
            ON CONFLICT(name) DO UPDATE SET
              display_name = excluded.display_name,
              chat_jid = excluded.chat_jid,
              group_folder = excluded.group_folder,
              updated_at = excluded.updated_at
            "#,
            params![
                destination.name,
                destination.display_name,
                destination.chat_jid,
                destination.group_folder,
                updated_at,
            ],
        )
        .with_context(|| {
            format!(
                "failed to write destination {} to {}",
                destination.name,
                paths.inbound_db.display()
            )
        })?;
    }
    conn.execute(
        r#"
        INSERT INTO session_routing (
          id, group_folder, chat_jid, updated_at
        )
        VALUES (1, ?1, ?2, ?3)
        ON CONFLICT(id) DO UPDATE SET
          group_folder = excluded.group_folder,
          chat_jid = excluded.chat_jid,
          updated_at = excluded.updated_at
        "#,
        params![current_group.folder, current_group.jid, updated_at],
    )
    .with_context(|| {
        format!(
            "failed to refresh session routing in {}",
            paths.inbound_db.display()
        )
    })?;
    Ok(())
}

pub fn inbound_count(paths: &SessionSidecarPaths) -> Result<i64> {
    count_rows(&paths.inbound_db, "messages_in")
}

pub fn outbound_count(paths: &SessionSidecarPaths) -> Result<i64> {
    count_rows(&paths.outbound_db, "messages_out")
}

fn ensure_inbound_db(path: &Path) -> Result<()> {
    let conn = open_sidecar_writer(path)?;
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS messages_in (
          id TEXT PRIMARY KEY,
          seq INTEGER NOT NULL UNIQUE,
          kind TEXT NOT NULL,
          timestamp TEXT NOT NULL,
          status TEXT NOT NULL DEFAULT 'pending',
          process_after TEXT,
          recurrence TEXT,
          series_id TEXT,
          tries INTEGER NOT NULL DEFAULT 0,
          trigger INTEGER NOT NULL DEFAULT 1,
          chat_jid TEXT,
          content TEXT NOT NULL,
          source_session_id TEXT,
          on_wake INTEGER NOT NULL DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_messages_in_status_seq
          ON messages_in(status, seq);
        CREATE INDEX IF NOT EXISTS idx_messages_in_on_wake
          ON messages_in(on_wake, seq);

        CREATE TABLE IF NOT EXISTS delivered (
          message_id TEXT PRIMARY KEY,
          delivered_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS destinations (
          name TEXT PRIMARY KEY,
          display_name TEXT NOT NULL,
          chat_jid TEXT NOT NULL,
          group_folder TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS session_routing (
          id INTEGER PRIMARY KEY CHECK (id = 1),
          group_folder TEXT NOT NULL,
          chat_jid TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );
        "#,
    )
    .with_context(|| format!("failed to initialize inbound sidecar {}", path.display()))?;
    Ok(())
}

fn ensure_outbound_db(path: &Path) -> Result<()> {
    let conn = open_sidecar_writer(path)?;
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS messages_out (
          id TEXT PRIMARY KEY,
          seq INTEGER NOT NULL UNIQUE,
          in_reply_to TEXT,
          timestamp TEXT NOT NULL,
          kind TEXT NOT NULL,
          chat_jid TEXT,
          content TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_messages_out_seq
          ON messages_out(seq);

        CREATE TABLE IF NOT EXISTS processing_ack (
          message_id TEXT PRIMARY KEY,
          acknowledged_at TEXT NOT NULL
        );
        "#,
    )
    .with_context(|| format!("failed to initialize outbound sidecar {}", path.display()))?;
    Ok(())
}

fn open_sidecar_writer(path: &Path) -> Result<Connection> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let conn = Connection::open(path)
        .with_context(|| format!("failed to open session sidecar {}", path.display()))?;
    conn.execute_batch("PRAGMA journal_mode=DELETE; PRAGMA foreign_keys=ON;")
        .with_context(|| format!("failed to configure session sidecar {}", path.display()))?;
    Ok(conn)
}

fn next_sequence(conn: &Connection, table: &str) -> Result<i64> {
    let sql = format!("SELECT COALESCE(MAX(seq), 0) + 1 FROM {table}");
    conn.query_row(&sql, [], |row| row.get::<_, i64>(0))
        .with_context(|| format!("failed to allocate next sequence for {table}"))
}

fn count_rows(path: &Path, table: &str) -> Result<i64> {
    let conn = open_sidecar_writer(path)?;
    let sql = format!("SELECT COUNT(*) FROM {table}");
    conn.query_row(&sql, [], |row| row.get::<_, i64>(0))
        .with_context(|| format!("failed to count rows in {table}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    use crate::nanoclaw::executor::build_execution_session;
    use crate::nanoclaw::router::destinations_from_groups;

    #[test]
    fn initializes_split_session_databases() -> Result<()> {
        let temp = tempdir()?;
        let session = build_execution_session(
            temp.path(),
            "main",
            "session-test",
            &temp.path().join("groups/main"),
        );
        let paths = ensure_session_sidecars(&session)?;

        assert!(paths.inbound_db.exists());
        assert!(paths.outbound_db.exists());

        let conn = Connection::open(&paths.inbound_db)?;
        let on_wake_exists: i64 = conn.query_row(
            "SELECT COUNT(*) FROM pragma_table_info('messages_in') WHERE name = 'on_wake'",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(on_wake_exists, 1);
        Ok(())
    }

    #[test]
    fn records_messages_tasks_wake_and_outbound() -> Result<()> {
        let temp = tempdir()?;
        let session = build_execution_session(
            temp.path(),
            "main",
            "session-test",
            &temp.path().join("groups/main"),
        );
        let paths = ensure_session_sidecars(&session)?;
        let group = Group::main("Andy", "2026-04-04T00:00:00Z");

        record_inbound_message(
            &paths,
            &MessageRecord {
                id: "m1".to_string(),
                chat_jid: "main".to_string(),
                sender: "user".to_string(),
                sender_name: None,
                content: "hello".to_string(),
                timestamp: "2026-04-05T12:00:00Z".to_string(),
                is_from_me: false,
                is_bot_message: false,
            },
            Some("upstream-session"),
            false,
        )?;
        record_on_wake_message(&paths, &group, "wake")?;
        record_outbound_message(
            &paths,
            &OutboundSidecarMessage {
                id: "out-1".to_string(),
                in_reply_to: Some("m1".to_string()),
                timestamp: "2026-04-05T12:01:00Z".to_string(),
                kind: "message".to_string(),
                chat_jid: "main".to_string(),
                content: "reply".to_string(),
            },
        )?;

        assert_eq!(inbound_count(&paths)?, 2);
        assert_eq!(outbound_count(&paths)?, 1);
        let conn = Connection::open(&paths.inbound_db)?;
        let wake_count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM messages_in WHERE on_wake = 1",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(wake_count, 1);
        Ok(())
    }

    #[test]
    fn refreshes_destination_projection() -> Result<()> {
        let temp = tempdir()?;
        let session = build_execution_session(
            temp.path(),
            "main",
            "session-test",
            &temp.path().join("groups/main"),
        );
        let paths = ensure_session_sidecars(&session)?;
        let group = Group::main("Andy", "2026-04-04T00:00:00Z");
        let destinations = destinations_from_groups(std::slice::from_ref(&group));

        refresh_destinations(&paths, &group, &destinations)?;

        let conn = Connection::open(&paths.inbound_db)?;
        let destination_count: i64 =
            conn.query_row("SELECT COUNT(*) FROM destinations", [], |row| row.get(0))?;
        assert_eq!(destination_count, 1);
        let routed_group: String = conn.query_row(
            "SELECT group_folder FROM session_routing WHERE id = 1",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(routed_group, "main");
        Ok(())
    }
}
