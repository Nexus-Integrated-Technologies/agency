use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use rusqlite::{params, Connection, OpenFlags};
use serde_json::Value;

use crate::foundation::{
    ExecutionLocation, ExecutionProvenanceRecord, ExecutionRunKind, ExecutionStatus,
    ExecutionTrustLevel, FoundationStore, GateDecision, Group, HostOsControlApprovalDecision,
    HostOsControlApprovalRequestRecord, HostOsControlApprovalStatus, MessageRecord, RequestPlane,
    ScheduledTask, SwarmRequestedLane, SwarmResolvedLane, SwarmRun, SwarmRunStatus, SwarmTask,
    SwarmTaskDependency, SwarmTaskStatus, TaskContextMode, TaskRunLog, TaskScheduleType,
    TaskStatus,
};
use crate::nanoclaw::observability::{
    ObservabilityEvent, ObservabilityEventStatus, ObservabilitySchemaAdapterDefinition,
    ObservabilitySeverity, UpsertObservabilityEventInput,
};
use crate::nanoclaw::omx::{
    OmxEventKind, OmxEventRecord, OmxMode, OmxSessionRecord, OmxSessionStatus,
};
use crate::nanoclaw::pm::{
    LinearIssueThread, PmAuditEvent, PmIssueMemory, PmIssueTrackedComment, StoredPmAuditEvent,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NanoclawDbCounts {
    pub chats: i64,
    pub messages: i64,
    pub scheduled_tasks: i64,
    pub registered_groups: i64,
}

pub struct NanoclawDb {
    path: PathBuf,
    conn: Connection,
}

impl NanoclawDb {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).with_context(|| {
                format!(
                    "failed to create database parent directory {}",
                    parent.display()
                )
            })?;
        }

        let conn = Connection::open(&path)
            .with_context(|| format!("failed to open sqlite database {}", path.display()))?;
        let db = Self { path, conn };
        db.init_schema()?;
        db.apply_migrations()?;
        Ok(db)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn read_registered_groups_from_path(path: impl AsRef<Path>) -> Result<Vec<Group>> {
        let path = path.as_ref();
        let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
            .with_context(|| format!("failed to open sqlite database {}", path.display()))?;
        let mut stmt = conn
            .prepare(
                r#"
                SELECT jid, name, folder, trigger_pattern, added_at, requires_trigger, is_main
                FROM registered_groups
                ORDER BY folder
                "#,
            )
            .with_context(|| {
                format!(
                    "failed to prepare registered_groups query for {}",
                    path.display()
                )
            })?;
        let rows = stmt
            .query_map([], |row| {
                Ok(Group {
                    jid: row.get(0)?,
                    name: row.get(1)?,
                    folder: row.get(2)?,
                    trigger: row.get(3)?,
                    added_at: row.get(4)?,
                    requires_trigger: row.get::<_, i64>(5)? != 0,
                    is_main: row.get::<_, i64>(6)? != 0,
                })
            })
            .with_context(|| {
                format!("failed to query registered groups from {}", path.display())
            })?;

        let mut groups = Vec::new();
        for row in rows {
            groups.push(row.context("failed to decode registered group row")?);
        }
        Ok(groups)
    }

    pub fn init_schema(&self) -> Result<()> {
        self.conn
            .execute_batch(
                r#"
                PRAGMA foreign_keys = ON;

                CREATE TABLE IF NOT EXISTS chats (
                  jid TEXT PRIMARY KEY,
                  name TEXT,
                  last_message_time TEXT,
                  channel TEXT,
                  is_group INTEGER DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS messages (
                  id TEXT,
                  chat_jid TEXT,
                  sender TEXT,
                  sender_name TEXT,
                  content TEXT,
                  timestamp TEXT,
                  is_from_me INTEGER,
                  is_bot_message INTEGER DEFAULT 0,
                  PRIMARY KEY (id, chat_jid),
                  FOREIGN KEY (chat_jid) REFERENCES chats(jid)
                );
                CREATE INDEX IF NOT EXISTS idx_messages_timestamp ON messages(timestamp);

                CREATE TABLE IF NOT EXISTS scheduled_tasks (
                  id TEXT PRIMARY KEY,
                  group_folder TEXT NOT NULL,
                  chat_jid TEXT NOT NULL,
                  prompt TEXT NOT NULL,
                  script TEXT,
                  request_plane TEXT,
                  schedule_type TEXT NOT NULL,
                  schedule_value TEXT NOT NULL,
                  context_mode TEXT DEFAULT 'isolated',
                  next_run TEXT,
                  last_run TEXT,
                  last_result TEXT,
                  status TEXT DEFAULT 'active',
                  created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_scheduled_tasks_next_run
                  ON scheduled_tasks(next_run);
                CREATE INDEX IF NOT EXISTS idx_scheduled_tasks_status
                  ON scheduled_tasks(status);

                CREATE TABLE IF NOT EXISTS task_run_logs (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  task_id TEXT NOT NULL,
                  run_at TEXT NOT NULL,
                  duration_ms INTEGER NOT NULL,
                  status TEXT NOT NULL,
                  result TEXT,
                  error TEXT,
                  FOREIGN KEY (task_id) REFERENCES scheduled_tasks(id)
                );

                CREATE TABLE IF NOT EXISTS pm_audit_events (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  issue_key TEXT,
                  issue_identifier TEXT,
                  thread_jid TEXT,
                  phase TEXT NOT NULL,
                  status TEXT NOT NULL,
                  tool TEXT,
                  error_code TEXT,
                  blocking INTEGER DEFAULT 0,
                  metadata_json TEXT,
                  created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_pm_audit_issue_key
                  ON pm_audit_events(issue_key, created_at);
                CREATE INDEX IF NOT EXISTS idx_pm_audit_issue_identifier
                  ON pm_audit_events(issue_identifier, created_at);
                CREATE INDEX IF NOT EXISTS idx_pm_audit_thread_jid
                  ON pm_audit_events(thread_jid, created_at);

                CREATE TABLE IF NOT EXISTS pm_issue_memory (
                  issue_key TEXT PRIMARY KEY,
                  issue_identifier TEXT NOT NULL,
                  summary TEXT,
                  next_action TEXT,
                  blockers_json TEXT,
                  current_state TEXT,
                  repo_hint TEXT,
                  last_source TEXT,
                  memory_json TEXT,
                  updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_pm_issue_memory_identifier
                  ON pm_issue_memory(issue_identifier, updated_at);

                CREATE TABLE IF NOT EXISTS pm_issue_comments (
                  issue_key TEXT NOT NULL,
                  issue_identifier TEXT,
                  comment_kind TEXT NOT NULL,
                  comment_id TEXT NOT NULL,
                  body_hash TEXT,
                  body TEXT,
                  updated_at TEXT NOT NULL,
                  PRIMARY KEY (issue_key, comment_kind)
                );
                CREATE INDEX IF NOT EXISTS idx_pm_issue_comments_identifier
                  ON pm_issue_comments(issue_identifier, comment_kind);

                CREATE TABLE IF NOT EXISTS observability_events (
                  id TEXT PRIMARY KEY,
                  fingerprint TEXT NOT NULL UNIQUE,
                  source TEXT NOT NULL,
                  environment TEXT,
                  service TEXT,
                  category TEXT,
                  severity TEXT NOT NULL,
                  title TEXT NOT NULL,
                  message TEXT,
                  status TEXT NOT NULL DEFAULT 'open',
                  chat_jid TEXT,
                  thread_ts TEXT,
                  swarm_run_id TEXT,
                  deployment_url TEXT,
                  healthcheck_url TEXT,
                  repo TEXT,
                  repo_path TEXT,
                  metadata_json TEXT,
                  raw_payload_json TEXT,
                  first_seen_at TEXT NOT NULL,
                  last_seen_at TEXT NOT NULL,
                  occurrence_count INTEGER NOT NULL DEFAULT 1
                );
                CREATE INDEX IF NOT EXISTS idx_observability_events_last_seen
                  ON observability_events(last_seen_at DESC);
                CREATE INDEX IF NOT EXISTS idx_observability_events_severity
                  ON observability_events(severity, status, last_seen_at DESC);
                CREATE INDEX IF NOT EXISTS idx_observability_events_source
                  ON observability_events(source, environment, service, last_seen_at DESC);

                CREATE TABLE IF NOT EXISTS observability_schema_adapters (
                  id TEXT PRIMARY KEY,
                  source_key TEXT NOT NULL UNIQUE,
                  version TEXT,
                  enabled INTEGER NOT NULL DEFAULT 1,
                  match_json TEXT,
                  field_map_json TEXT,
                  defaults_json TEXT,
                  metadata_paths_json TEXT,
                  validation_json TEXT,
                  updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_observability_schema_adapters_enabled
                  ON observability_schema_adapters(enabled, source_key);

                CREATE TABLE IF NOT EXISTS router_state (
                  key TEXT PRIMARY KEY,
                  value TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS sessions (
                  group_folder TEXT PRIMARY KEY,
                  session_id TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS execution_provenance (
                  id TEXT PRIMARY KEY,
                  run_kind TEXT NOT NULL,
                  group_folder TEXT NOT NULL,
                  chat_jid TEXT,
                  execution_location TEXT NOT NULL,
                  trust_level TEXT NOT NULL,
                  request_plane TEXT NOT NULL,
                  effective_capabilities_json TEXT NOT NULL,
                  project_environment_id TEXT,
                  mount_summary_json TEXT NOT NULL,
                  secret_handles_used_json TEXT NOT NULL,
                  fallback_reason TEXT,
                  sync_scope_json TEXT,
                  task_signature_json TEXT,
                  boundary_claims_json TEXT NOT NULL DEFAULT '[]',
                  gate_decision TEXT,
                  gate_evaluation_json TEXT,
                  assurance_json TEXT,
                  symbol_carriers_json TEXT NOT NULL DEFAULT '[]',
                  provenance_edges_json TEXT NOT NULL DEFAULT '[]',
                  status TEXT NOT NULL,
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL,
                  completed_at TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_execution_provenance_group
                  ON execution_provenance(group_folder, created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_execution_provenance_status
                  ON execution_provenance(status, updated_at DESC);

                CREATE TABLE IF NOT EXISTS omx_sessions (
                  session_id TEXT PRIMARY KEY,
                  group_folder TEXT NOT NULL,
                  chat_jid TEXT,
                  task_id TEXT,
                  external_run_id TEXT,
                  mode TEXT NOT NULL,
                  status TEXT NOT NULL,
                  tmux_session TEXT,
                  team_name TEXT,
                  workspace_root TEXT NOT NULL,
                  last_summary TEXT,
                  pending_question TEXT,
                  last_event_kind TEXT,
                  last_activity_at TEXT NOT NULL,
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL,
                  completed_at TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_omx_sessions_chat
                  ON omx_sessions(chat_jid, status, updated_at DESC);
                CREATE INDEX IF NOT EXISTS idx_omx_sessions_group
                  ON omx_sessions(group_folder, updated_at DESC);
                CREATE INDEX IF NOT EXISTS idx_omx_sessions_external_run
                  ON omx_sessions(external_run_id, updated_at DESC);

                CREATE TABLE IF NOT EXISTS omx_events (
                  id TEXT PRIMARY KEY,
                  session_id TEXT NOT NULL,
                  event_kind TEXT NOT NULL,
                  group_folder TEXT NOT NULL,
                  chat_jid TEXT,
                  summary TEXT,
                  question TEXT,
                  payload_json TEXT,
                  created_at TEXT NOT NULL,
                  FOREIGN KEY (session_id) REFERENCES omx_sessions(session_id)
                );
                CREATE INDEX IF NOT EXISTS idx_omx_events_session
                  ON omx_events(session_id, created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_omx_events_chat
                  ON omx_events(chat_jid, created_at DESC);

                CREATE TABLE IF NOT EXISTS host_os_control_approvals (
                  id TEXT PRIMARY KEY,
                  source_group TEXT NOT NULL,
                  chat_jid TEXT,
                  action_kind TEXT NOT NULL,
                  action_scope TEXT NOT NULL,
                  action_fingerprint TEXT NOT NULL,
                  action_summary TEXT NOT NULL,
                  action_payload_json TEXT,
                  allowed_decisions_json TEXT NOT NULL,
                  boundary_claim_json TEXT,
                  gate_evaluation_json TEXT,
                  status TEXT NOT NULL,
                  resolved_decision TEXT,
                  created_at TEXT NOT NULL,
                  resolved_at TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_host_os_control_approvals_status
                  ON host_os_control_approvals(status, created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_host_os_control_approvals_group
                  ON host_os_control_approvals(source_group, status, created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_host_os_control_approvals_fingerprint
                  ON host_os_control_approvals(source_group, action_fingerprint, status, created_at DESC);

                CREATE TABLE IF NOT EXISTS swarm_runs (
                  id TEXT PRIMARY KEY,
                  group_folder TEXT NOT NULL,
                  chat_jid TEXT NOT NULL,
                  created_by TEXT NOT NULL,
                  objective TEXT NOT NULL,
                  requested_lane TEXT NOT NULL,
                  objective_signature_json TEXT,
                  objective_gate_json TEXT,
                  status TEXT NOT NULL,
                  max_concurrency INTEGER NOT NULL DEFAULT 2,
                  summary TEXT,
                  result_json TEXT,
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL,
                  completed_at TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_swarm_runs_status
                  ON swarm_runs(status, updated_at DESC);
                CREATE INDEX IF NOT EXISTS idx_swarm_runs_group
                  ON swarm_runs(group_folder, updated_at DESC);

                CREATE TABLE IF NOT EXISTS swarm_tasks (
                  id TEXT PRIMARY KEY,
                  run_id TEXT NOT NULL,
                  task_key TEXT NOT NULL,
                  title TEXT NOT NULL,
                  prompt TEXT,
                  command TEXT,
                  requested_lane TEXT NOT NULL,
                  resolved_lane TEXT,
                  task_signature_json TEXT,
                  boundary_quadrant TEXT,
                  gate_decision TEXT,
                  gate_evaluation_json TEXT,
                  required_roles_json TEXT NOT NULL DEFAULT '[]',
                  status TEXT NOT NULL,
                  priority INTEGER NOT NULL DEFAULT 50,
                  target_group_folder TEXT NOT NULL,
                  target_chat_jid TEXT NOT NULL,
                  cwd TEXT,
                  repo TEXT,
                  repo_path TEXT,
                  sync INTEGER NOT NULL DEFAULT 0,
                  host TEXT,
                  user TEXT,
                  port INTEGER,
                  timeout_ms INTEGER,
                  metadata_json TEXT,
                  result_json TEXT,
                  error TEXT,
                  lease_owner TEXT,
                  lease_expires_at TEXT,
                  attempts INTEGER NOT NULL DEFAULT 0,
                  max_attempts INTEGER NOT NULL DEFAULT 3,
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL,
                  started_at TEXT,
                  completed_at TEXT,
                  FOREIGN KEY (run_id) REFERENCES swarm_runs(id)
                );
                CREATE INDEX IF NOT EXISTS idx_swarm_tasks_run
                  ON swarm_tasks(run_id, priority DESC, created_at);
                CREATE INDEX IF NOT EXISTS idx_swarm_tasks_status
                  ON swarm_tasks(status, lease_expires_at, priority DESC, created_at);

                CREATE TABLE IF NOT EXISTS swarm_task_dependencies (
                  task_id TEXT NOT NULL,
                  depends_on_task_id TEXT NOT NULL,
                  PRIMARY KEY (task_id, depends_on_task_id),
                  FOREIGN KEY (task_id) REFERENCES swarm_tasks(id),
                  FOREIGN KEY (depends_on_task_id) REFERENCES swarm_tasks(id)
                );
                CREATE INDEX IF NOT EXISTS idx_swarm_task_dependencies_task
                  ON swarm_task_dependencies(task_id);
                CREATE INDEX IF NOT EXISTS idx_swarm_task_dependencies_dep
                  ON swarm_task_dependencies(depends_on_task_id);

                CREATE TABLE IF NOT EXISTS registered_groups (
                  jid TEXT PRIMARY KEY,
                  name TEXT NOT NULL,
                  folder TEXT NOT NULL UNIQUE,
                  trigger_pattern TEXT NOT NULL,
                  added_at TEXT NOT NULL,
                  container_config TEXT,
                  requires_trigger INTEGER DEFAULT 1,
                  is_main INTEGER DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS linear_issue_threads (
                  issue_key TEXT PRIMARY KEY,
                  chat_jid TEXT NOT NULL,
                  thread_ts TEXT NOT NULL,
                  issue_identifier TEXT,
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL,
                  closed_at TEXT
                );
                "#,
            )
            .context("failed to initialize NanoClaw schema")?;
        Ok(())
    }

    fn apply_migrations(&self) -> Result<()> {
        self.ensure_column("scheduled_tasks", "script", "TEXT")?;
        self.ensure_column("scheduled_tasks", "request_plane", "TEXT")?;
        self.ensure_column("scheduled_tasks", "context_mode", "TEXT DEFAULT 'isolated'")?;
        self.ensure_column("linear_issue_threads", "closed_at", "TEXT")?;
        self.ensure_column("linear_issue_threads", "issue_identifier", "TEXT")?;
        self.ensure_column("execution_provenance", "task_signature_json", "TEXT")?;
        self.ensure_column(
            "execution_provenance",
            "boundary_claims_json",
            "TEXT NOT NULL DEFAULT '[]'",
        )?;
        self.ensure_column("execution_provenance", "gate_decision", "TEXT")?;
        self.ensure_column("execution_provenance", "gate_evaluation_json", "TEXT")?;
        self.ensure_column("execution_provenance", "assurance_json", "TEXT")?;
        self.ensure_column(
            "execution_provenance",
            "symbol_carriers_json",
            "TEXT NOT NULL DEFAULT '[]'",
        )?;
        self.ensure_column(
            "execution_provenance",
            "provenance_edges_json",
            "TEXT NOT NULL DEFAULT '[]'",
        )?;
        self.ensure_column("host_os_control_approvals", "boundary_claim_json", "TEXT")?;
        self.ensure_column("host_os_control_approvals", "gate_evaluation_json", "TEXT")?;
        self.ensure_column("swarm_runs", "objective_signature_json", "TEXT")?;
        self.ensure_column("swarm_runs", "objective_gate_json", "TEXT")?;
        self.ensure_column("swarm_tasks", "task_signature_json", "TEXT")?;
        self.ensure_column("swarm_tasks", "boundary_quadrant", "TEXT")?;
        self.ensure_column("swarm_tasks", "gate_decision", "TEXT")?;
        self.ensure_column("swarm_tasks", "gate_evaluation_json", "TEXT")?;
        self.ensure_column(
            "swarm_tasks",
            "required_roles_json",
            "TEXT NOT NULL DEFAULT '[]'",
        )?;
        Ok(())
    }

    pub fn upsert_router_state(&self, key: &str, value: &str) -> Result<()> {
        self.conn
            .execute(
                r#"
                INSERT INTO router_state (key, value)
                VALUES (?1, ?2)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                "#,
                params![key, value],
            )
            .with_context(|| format!("failed to upsert router_state for key {key}"))?;
        Ok(())
    }

    pub fn router_state(&self, key: &str) -> Result<Option<String>> {
        let mut stmt = self
            .conn
            .prepare("SELECT value FROM router_state WHERE key = ?1")
            .with_context(|| format!("failed to prepare router_state query for key {key}"))?;
        let value = stmt.query_row(params![key], |row| row.get::<_, String>(0));
        match value {
            Ok(value) => Ok(Some(value)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(err) => {
                Err(err).with_context(|| format!("failed to read router_state for key {key}"))
            }
        }
    }

    pub fn upsert_session(&self, group_folder: &str, session_id: &str) -> Result<()> {
        self.conn
            .execute(
                r#"
                INSERT INTO sessions (group_folder, session_id)
                VALUES (?1, ?2)
                ON CONFLICT(group_folder) DO UPDATE SET session_id = excluded.session_id
                "#,
                params![group_folder, session_id],
            )
            .with_context(|| format!("failed to upsert session for group {}", group_folder))?;
        Ok(())
    }

    pub fn session_for_group(&self, group_folder: &str) -> Result<Option<String>> {
        let mut stmt = self
            .conn
            .prepare("SELECT session_id FROM sessions WHERE group_folder = ?1")
            .with_context(|| format!("failed to prepare session lookup for {}", group_folder))?;
        let row = stmt.query_row(params![group_folder], |row| row.get::<_, String>(0));
        match row {
            Ok(session_id) => Ok(Some(session_id)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(err) => {
                Err(err).with_context(|| format!("failed to read session for {}", group_folder))
            }
        }
    }

    pub fn create_execution_provenance(&self, record: &ExecutionProvenanceRecord) -> Result<()> {
        self.conn
            .execute(
                r#"
                INSERT INTO execution_provenance (
                  id,
                  run_kind,
                  group_folder,
                  chat_jid,
                  execution_location,
                  trust_level,
                  request_plane,
                  effective_capabilities_json,
                  project_environment_id,
                  mount_summary_json,
                  secret_handles_used_json,
                  fallback_reason,
                  sync_scope_json,
                  task_signature_json,
                  boundary_claims_json,
                  gate_decision,
                  gate_evaluation_json,
                  assurance_json,
                  symbol_carriers_json,
                  provenance_edges_json,
                  status,
                  created_at,
                  updated_at,
                  completed_at
                )
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24)
                "#,
                params![
                    record.id,
                    record.run_kind.as_str(),
                    record.group_folder,
                    record.chat_jid,
                    record.execution_location.as_str(),
                    record.trust_level.as_str(),
                    record.request_plane.as_str(),
                    serde_json::to_string(&record.effective_capabilities)?,
                    record.project_environment_id,
                    serde_json::to_string(&record.mount_summary)?,
                    serde_json::to_string(&record.secret_handles_used)?,
                    record.fallback_reason,
                    record
                        .sync_scope
                        .as_ref()
                        .map(serde_json::to_string)
                        .transpose()?,
                    record
                        .task_signature
                        .as_ref()
                        .map(serde_json::to_string)
                        .transpose()?,
                    serde_json::to_string(&record.boundary_claims)?,
                    record.gate_decision.as_ref().map(GateDecision::as_str),
                    record
                        .gate_evaluation
                        .as_ref()
                        .map(serde_json::to_string)
                        .transpose()?,
                    record
                        .assurance
                        .as_ref()
                        .map(serde_json::to_string)
                        .transpose()?,
                    serde_json::to_string(&record.symbol_carriers)?,
                    serde_json::to_string(&record.provenance_edges)?,
                    record.status.as_str(),
                    record.created_at,
                    record.updated_at,
                    record.completed_at,
                ],
            )
            .with_context(|| format!("failed to insert execution provenance {}", record.id))?;
        Ok(())
    }

    pub fn update_execution_provenance(
        &self,
        id: &str,
        status: Option<ExecutionStatus>,
        fallback_reason: Option<Option<String>>,
        completed_at: Option<Option<String>>,
    ) -> Result<()> {
        let mut fields = Vec::new();
        let mut values = Vec::new();

        if let Some(status) = status {
            fields.push("status = ?".to_string());
            values.push(rusqlite::types::Value::Text(status.as_str().to_string()));
        }
        if let Some(fallback_reason) = fallback_reason {
            fields.push("fallback_reason = ?".to_string());
            values.push(match fallback_reason {
                Some(value) => rusqlite::types::Value::Text(value),
                None => rusqlite::types::Value::Null,
            });
        }
        if let Some(completed_at) = completed_at {
            fields.push("completed_at = ?".to_string());
            values.push(match completed_at {
                Some(value) => rusqlite::types::Value::Text(value),
                None => rusqlite::types::Value::Null,
            });
        }

        fields.push("updated_at = ?".to_string());
        values.push(rusqlite::types::Value::Text(
            chrono::Utc::now().to_rfc3339(),
        ));

        let sql = format!(
            "UPDATE execution_provenance SET {} WHERE id = ?",
            fields.join(", ")
        );
        values.push(rusqlite::types::Value::Text(id.to_string()));
        self.conn
            .execute(&sql, rusqlite::params_from_iter(values))
            .with_context(|| format!("failed to update execution provenance {}", id))?;
        Ok(())
    }

    pub fn get_execution_provenance(&self, id: &str) -> Result<Option<ExecutionProvenanceRecord>> {
        let mut stmt = self
            .conn
            .prepare(
                r#"
                SELECT
                  id,
                  run_kind,
                  group_folder,
                  chat_jid,
                  execution_location,
                  trust_level,
                  request_plane,
                  effective_capabilities_json,
                  project_environment_id,
                  mount_summary_json,
                  secret_handles_used_json,
                  fallback_reason,
                  sync_scope_json,
                  task_signature_json,
                  boundary_claims_json,
                  gate_decision,
                  gate_evaluation_json,
                  assurance_json,
                  symbol_carriers_json,
                  provenance_edges_json,
                  status,
                  created_at,
                  updated_at,
                  completed_at
                FROM execution_provenance
                WHERE id = ?1
                "#,
            )
            .with_context(|| format!("failed to prepare execution provenance lookup for {}", id))?;
        let row = stmt.query_row(params![id], map_execution_provenance_row);
        match row {
            Ok(record) => Ok(Some(record)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(err) => {
                Err(err).with_context(|| format!("failed to read execution provenance {}", id))
            }
        }
    }

    pub fn list_execution_provenance(
        &self,
        group_folder: Option<&str>,
        limit: usize,
    ) -> Result<Vec<ExecutionProvenanceRecord>> {
        let limit = limit.max(1) as i64;
        let (sql, params): (&str, Vec<rusqlite::types::Value>) =
            if let Some(group_folder) = group_folder {
                (
                    r#"
                SELECT
                  id,
                  run_kind,
                  group_folder,
                  chat_jid,
                  execution_location,
                  trust_level,
                  request_plane,
                  effective_capabilities_json,
                  project_environment_id,
                  mount_summary_json,
                  secret_handles_used_json,
                  fallback_reason,
                  sync_scope_json,
                  task_signature_json,
                  boundary_claims_json,
                  gate_decision,
                  gate_evaluation_json,
                  assurance_json,
                  symbol_carriers_json,
                  provenance_edges_json,
                  status,
                  created_at,
                  updated_at,
                  completed_at
                FROM execution_provenance
                WHERE group_folder = ?1
                ORDER BY created_at DESC
                LIMIT ?2
                "#,
                    vec![
                        rusqlite::types::Value::Text(group_folder.to_string()),
                        rusqlite::types::Value::Integer(limit),
                    ],
                )
            } else {
                (
                    r#"
                SELECT
                  id,
                  run_kind,
                  group_folder,
                  chat_jid,
                  execution_location,
                  trust_level,
                  request_plane,
                  effective_capabilities_json,
                  project_environment_id,
                  mount_summary_json,
                  secret_handles_used_json,
                  fallback_reason,
                  sync_scope_json,
                  status,
                  created_at,
                  updated_at,
                  completed_at
                FROM execution_provenance
                ORDER BY created_at DESC
                LIMIT ?1
                "#,
                    vec![rusqlite::types::Value::Integer(limit)],
                )
            };
        let mut stmt = self
            .conn
            .prepare(sql)
            .context("failed to prepare execution provenance query")?;
        let rows = stmt
            .query_map(
                rusqlite::params_from_iter(params),
                map_execution_provenance_row,
            )
            .context("failed to execute execution provenance query")?;
        let mut records = Vec::new();
        for row in rows {
            records.push(row.context("failed to decode execution provenance row")?);
        }
        Ok(records)
    }

    pub fn upsert_omx_session(&self, record: &OmxSessionRecord) -> Result<()> {
        self.conn
            .execute(
                r#"
                INSERT INTO omx_sessions (
                  session_id,
                  group_folder,
                  chat_jid,
                  task_id,
                  external_run_id,
                  mode,
                  status,
                  tmux_session,
                  team_name,
                  workspace_root,
                  last_summary,
                  pending_question,
                  last_event_kind,
                  last_activity_at,
                  created_at,
                  updated_at,
                  completed_at
                )
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17)
                ON CONFLICT(session_id) DO UPDATE SET
                  group_folder = excluded.group_folder,
                  chat_jid = excluded.chat_jid,
                  task_id = excluded.task_id,
                  external_run_id = excluded.external_run_id,
                  mode = excluded.mode,
                  status = excluded.status,
                  tmux_session = excluded.tmux_session,
                  team_name = excluded.team_name,
                  workspace_root = excluded.workspace_root,
                  last_summary = excluded.last_summary,
                  pending_question = excluded.pending_question,
                  last_event_kind = excluded.last_event_kind,
                  last_activity_at = excluded.last_activity_at,
                  updated_at = excluded.updated_at,
                  completed_at = excluded.completed_at
                "#,
                params![
                    record.session_id,
                    record.group_folder,
                    record.chat_jid,
                    record.task_id,
                    record.external_run_id,
                    record.mode.as_str(),
                    record.status.as_str(),
                    record.tmux_session,
                    record.team_name,
                    record.workspace_root,
                    record.last_summary,
                    record.pending_question,
                    record.last_event_kind.as_ref().map(OmxEventKind::as_str),
                    record.last_activity_at,
                    record.created_at,
                    record.updated_at,
                    record.completed_at,
                ],
            )
            .with_context(|| format!("failed to upsert OMX session {}", record.session_id))?;
        Ok(())
    }

    pub fn get_omx_session(&self, session_id: &str) -> Result<Option<OmxSessionRecord>> {
        let mut stmt = self
            .conn
            .prepare(
                r#"
                SELECT
                  session_id,
                  group_folder,
                  chat_jid,
                  task_id,
                  external_run_id,
                  mode,
                  status,
                  tmux_session,
                  team_name,
                  workspace_root,
                  last_summary,
                  pending_question,
                  last_event_kind,
                  last_activity_at,
                  created_at,
                  updated_at,
                  completed_at
                FROM omx_sessions
                WHERE session_id = ?1
                "#,
            )
            .with_context(|| format!("failed to prepare OMX session lookup for {}", session_id))?;
        let row = stmt.query_row(params![session_id], map_omx_session_row);
        match row {
            Ok(record) => Ok(Some(record)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(err) => {
                Err(err).with_context(|| format!("failed to read OMX session {}", session_id))
            }
        }
    }

    pub fn find_active_omx_session_for_chat(
        &self,
        chat_jid: &str,
    ) -> Result<Option<OmxSessionRecord>> {
        let mut stmt = self
            .conn
            .prepare(
                r#"
                SELECT
                  session_id,
                  group_folder,
                  chat_jid,
                  task_id,
                  external_run_id,
                  mode,
                  status,
                  tmux_session,
                  team_name,
                  workspace_root,
                  last_summary,
                  pending_question,
                  last_event_kind,
                  last_activity_at,
                  created_at,
                  updated_at,
                  completed_at
                FROM omx_sessions
                WHERE chat_jid = ?1
                  AND status IN ('pending', 'running', 'idle', 'waiting_input')
                ORDER BY updated_at DESC
                LIMIT 1
                "#,
            )
            .with_context(|| {
                format!(
                    "failed to prepare active OMX session lookup for {}",
                    chat_jid
                )
            })?;
        let row = stmt.query_row(params![chat_jid], map_omx_session_row);
        match row {
            Ok(record) => Ok(Some(record)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(err) => Err(err)
                .with_context(|| format!("failed to read active OMX session for {}", chat_jid)),
        }
    }

    pub fn list_omx_sessions(&self, limit: usize) -> Result<Vec<OmxSessionRecord>> {
        let mut stmt = self
            .conn
            .prepare(
                r#"
                SELECT
                  session_id,
                  group_folder,
                  chat_jid,
                  task_id,
                  external_run_id,
                  mode,
                  status,
                  tmux_session,
                  team_name,
                  workspace_root,
                  last_summary,
                  pending_question,
                  last_event_kind,
                  last_activity_at,
                  created_at,
                  updated_at,
                  completed_at
                FROM omx_sessions
                ORDER BY updated_at DESC
                LIMIT ?1
                "#,
            )
            .context("failed to prepare OMX sessions query")?;
        let rows = stmt
            .query_map(params![limit.max(1) as i64], map_omx_session_row)
            .context("failed to execute OMX sessions query")?;
        let mut records = Vec::new();
        for row in rows {
            records.push(row.context("failed to decode OMX session row")?);
        }
        Ok(records)
    }

    pub fn insert_omx_event(&self, record: &OmxEventRecord) -> Result<()> {
        self.conn
            .execute(
                r#"
                INSERT INTO omx_events (
                  id,
                  session_id,
                  event_kind,
                  group_folder,
                  chat_jid,
                  summary,
                  question,
                  payload_json,
                  created_at
                )
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
                "#,
                params![
                    record.id,
                    record.session_id,
                    record.event_kind.as_str(),
                    record.group_folder,
                    record.chat_jid,
                    record.summary,
                    record.question,
                    record
                        .payload_json
                        .as_ref()
                        .map(serde_json::to_string)
                        .transpose()?,
                    record.created_at,
                ],
            )
            .with_context(|| format!("failed to insert OMX event {}", record.id))?;
        Ok(())
    }

    pub fn list_omx_events(&self, session_id: &str, limit: usize) -> Result<Vec<OmxEventRecord>> {
        let mut stmt = self
            .conn
            .prepare(
                r#"
                SELECT
                  id,
                  session_id,
                  event_kind,
                  group_folder,
                  chat_jid,
                  summary,
                  question,
                  payload_json,
                  created_at
                FROM omx_events
                WHERE session_id = ?1
                ORDER BY created_at DESC
                LIMIT ?2
                "#,
            )
            .with_context(|| format!("failed to prepare OMX events query for {}", session_id))?;
        let rows = stmt
            .query_map(params![session_id, limit.max(1) as i64], map_omx_event_row)
            .with_context(|| format!("failed to execute OMX events query for {}", session_id))?;
        let mut records = Vec::new();
        for row in rows {
            records.push(row.context("failed to decode OMX event row")?);
        }
        Ok(records)
    }

    pub fn create_host_os_control_approval_request(
        &self,
        record: &HostOsControlApprovalRequestRecord,
    ) -> Result<()> {
        self.conn
            .execute(
                r#"
                INSERT INTO host_os_control_approvals (
                  id,
                  source_group,
                  chat_jid,
                  action_kind,
                  action_scope,
                  action_fingerprint,
                  action_summary,
                  action_payload_json,
                  allowed_decisions_json,
                  boundary_claim_json,
                  gate_evaluation_json,
                  status,
                  resolved_decision,
                  created_at,
                  resolved_at
                )
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)
                "#,
                params![
                    record.id,
                    record.source_group,
                    record.chat_jid,
                    record.action_kind,
                    record.action_scope,
                    record.action_fingerprint,
                    record.action_summary,
                    record.action_payload.as_ref().map(Value::to_string),
                    serde_json::to_string(&record.allowed_decisions)?,
                    record
                        .boundary_claim
                        .as_ref()
                        .map(serde_json::to_string)
                        .transpose()?,
                    record
                        .gate_evaluation
                        .as_ref()
                        .map(serde_json::to_string)
                        .transpose()?,
                    record.status.as_str(),
                    record
                        .resolved_decision
                        .as_ref()
                        .map(|value| value.as_str()),
                    record.created_at,
                    record.resolved_at,
                ],
            )
            .with_context(|| {
                format!(
                    "failed to insert host OS control approval request {}",
                    record.id
                )
            })?;
        Ok(())
    }

    pub fn get_host_os_control_approval_request(
        &self,
        id: &str,
    ) -> Result<Option<HostOsControlApprovalRequestRecord>> {
        let mut stmt = self
            .conn
            .prepare(
                r#"
                SELECT
                  id,
                  source_group,
                  chat_jid,
                  action_kind,
                  action_scope,
                  action_fingerprint,
                  action_summary,
                  action_payload_json,
                  allowed_decisions_json,
                  boundary_claim_json,
                  gate_evaluation_json,
                  status,
                  resolved_decision,
                  created_at,
                  resolved_at
                FROM host_os_control_approvals
                WHERE id = ?1
                "#,
            )
            .with_context(|| {
                format!(
                    "failed to prepare host OS control approval lookup for {}",
                    id
                )
            })?;
        let row = stmt.query_row(params![id], map_host_os_control_approval_row);
        match row {
            Ok(record) => Ok(Some(record)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(err) => Err(err)
                .with_context(|| format!("failed to read host OS control approval request {}", id)),
        }
    }

    pub fn find_pending_host_os_control_approval_request(
        &self,
        source_group: &str,
        fingerprint: &str,
    ) -> Result<Option<HostOsControlApprovalRequestRecord>> {
        let mut stmt = self
            .conn
            .prepare(
                r#"
                SELECT
                  id,
                  source_group,
                  chat_jid,
                  action_kind,
                  action_scope,
                  action_fingerprint,
                  action_summary,
                  action_payload_json,
                  allowed_decisions_json,
                  boundary_claim_json,
                  gate_evaluation_json,
                  status,
                  resolved_decision,
                  created_at,
                  resolved_at
                FROM host_os_control_approvals
                WHERE source_group = ?1 AND action_fingerprint = ?2 AND status = 'pending'
                ORDER BY created_at DESC
                LIMIT 1
                "#,
            )
            .with_context(|| {
                format!(
                    "failed to prepare pending host OS control approval lookup for {} {}",
                    source_group, fingerprint
                )
            })?;
        let row = stmt.query_row(
            params![source_group, fingerprint],
            map_host_os_control_approval_row,
        );
        match row {
            Ok(record) => Ok(Some(record)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(err) => Err(err).with_context(|| {
                format!(
                    "failed to read pending host OS control approval for {} {}",
                    source_group, fingerprint
                )
            }),
        }
    }

    pub fn list_host_os_control_approval_requests(
        &self,
        status: Option<HostOsControlApprovalStatus>,
        source_group: Option<&str>,
        limit: usize,
    ) -> Result<Vec<HostOsControlApprovalRequestRecord>> {
        let mut sql = String::from(
            r#"
            SELECT
              id,
              source_group,
              chat_jid,
              action_kind,
              action_scope,
              action_fingerprint,
              action_summary,
              action_payload_json,
              allowed_decisions_json,
              boundary_claim_json,
              gate_evaluation_json,
              status,
              resolved_decision,
              created_at,
              resolved_at
            FROM host_os_control_approvals
            "#,
        );
        let mut clauses = Vec::new();
        let mut values = Vec::new();
        if let Some(status) = status {
            clauses.push("status = ?".to_string());
            values.push(rusqlite::types::Value::Text(status.as_str().to_string()));
        }
        if let Some(source_group) = source_group {
            clauses.push("source_group = ?".to_string());
            values.push(rusqlite::types::Value::Text(source_group.to_string()));
        }
        if !clauses.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&clauses.join(" AND "));
        }
        sql.push_str(" ORDER BY created_at DESC LIMIT ?");
        values.push(rusqlite::types::Value::Integer(limit.max(1) as i64));
        let mut stmt = self
            .conn
            .prepare(&sql)
            .context("failed to prepare host OS control approval query")?;
        let rows = stmt
            .query_map(
                rusqlite::params_from_iter(values),
                map_host_os_control_approval_row,
            )
            .context("failed to execute host OS control approval query")?;
        let mut records = Vec::new();
        for row in rows {
            records.push(row.context("failed to decode host OS control approval row")?);
        }
        Ok(records)
    }

    pub fn resolve_host_os_control_approval_request(
        &self,
        id: &str,
        status: HostOsControlApprovalStatus,
        resolved_decision: Option<HostOsControlApprovalDecision>,
        resolved_at: Option<&str>,
    ) -> Result<()> {
        let resolved_at = resolved_at
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| chrono::Utc::now().to_rfc3339());
        self.conn
            .execute(
                r#"
                UPDATE host_os_control_approvals
                SET status = ?1, resolved_decision = ?2, resolved_at = ?3
                WHERE id = ?4
                "#,
                params![
                    status.as_str(),
                    resolved_decision.as_ref().map(|value| value.as_str()),
                    resolved_at,
                    id,
                ],
            )
            .with_context(|| format!("failed to resolve host OS control approval {}", id))?;
        Ok(())
    }

    pub fn create_swarm_run(
        &self,
        run: &SwarmRun,
        tasks: &[SwarmTask],
        dependencies: &[SwarmTaskDependency],
    ) -> Result<()> {
        self.conn
            .execute_batch("BEGIN IMMEDIATE")
            .context("failed to begin swarm run transaction")?;
        let result = (|| -> Result<()> {
            self.conn
                .execute(
                    r#"
                    INSERT INTO swarm_runs (
                      id,
                      group_folder,
                      chat_jid,
                      created_by,
                      objective,
                      requested_lane,
                      objective_signature_json,
                      objective_gate_json,
                      status,
                      max_concurrency,
                      summary,
                      result_json,
                      created_at,
                      updated_at,
                      completed_at
                    )
                    VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)
                    "#,
                    params![
                        run.id,
                        run.group_folder,
                        run.chat_jid,
                        run.created_by,
                        run.objective,
                        run.requested_lane.as_str(),
                        run.objective_signature
                            .as_ref()
                            .map(serde_json::to_string)
                            .transpose()?,
                        run.objective_gate
                            .as_ref()
                            .map(serde_json::to_string)
                            .transpose()?,
                        run.status.as_str(),
                        run.max_concurrency,
                        run.summary,
                        run.result.as_ref().map(Value::to_string),
                        run.created_at,
                        run.updated_at,
                        run.completed_at,
                    ],
                )
                .with_context(|| format!("failed to insert swarm run {}", run.id))?;
            for task in tasks {
                self.conn
                    .execute(
                        r#"
                        INSERT INTO swarm_tasks (
                          id,
                          run_id,
                          task_key,
                          title,
                          prompt,
                          command,
                          requested_lane,
                          resolved_lane,
                          task_signature_json,
                          boundary_quadrant,
                          gate_decision,
                          gate_evaluation_json,
                          required_roles_json,
                          status,
                          priority,
                          target_group_folder,
                          target_chat_jid,
                          cwd,
                          repo,
                          repo_path,
                          sync,
                          host,
                          user,
                          port,
                          timeout_ms,
                          metadata_json,
                          result_json,
                          error,
                          lease_owner,
                          lease_expires_at,
                          attempts,
                          max_attempts,
                          created_at,
                          updated_at,
                          started_at,
                          completed_at
                        )
                        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26, ?27, ?28, ?29, ?30, ?31, ?32, ?33, ?34, ?35, ?36)
                        "#,
                        params![
                            task.id,
                            task.run_id,
                            task.task_key,
                            task.title,
                            task.prompt,
                            task.command,
                            task.requested_lane.as_str(),
                            task.resolved_lane.as_ref().map(SwarmResolvedLane::as_str),
                            task.task_signature
                                .as_ref()
                                .map(serde_json::to_string)
                                .transpose()?,
                            task.boundary_quadrant.as_ref().map(|value| value.as_str()),
                            task.gate_decision.as_ref().map(GateDecision::as_str),
                            task.gate_evaluation
                                .as_ref()
                                .map(serde_json::to_string)
                                .transpose()?,
                            serde_json::to_string(&task.required_roles)?,
                            task.status.as_str(),
                            task.priority,
                            task.target_group_folder,
                            task.target_chat_jid,
                            task.cwd,
                            task.repo,
                            task.repo_path,
                            if task.sync { 1 } else { 0 },
                            task.host,
                            task.user,
                            task.port.map(i64::from),
                            task.timeout_ms.map(|value| value as i64),
                            task.metadata.as_ref().map(Value::to_string),
                            task.result.as_ref().map(Value::to_string),
                            task.error,
                            task.lease_owner,
                            task.lease_expires_at,
                            task.attempts,
                            task.max_attempts,
                            task.created_at,
                            task.updated_at,
                            task.started_at,
                            task.completed_at,
                        ],
                    )
                    .with_context(|| format!("failed to insert swarm task {}", task.id))?;
            }
            for dependency in dependencies {
                self.conn
                    .execute(
                        r#"
                        INSERT INTO swarm_task_dependencies (task_id, depends_on_task_id)
                        VALUES (?1, ?2)
                        "#,
                        params![dependency.task_id, dependency.depends_on_task_id],
                    )
                    .with_context(|| {
                        format!(
                            "failed to insert swarm task dependency {} -> {}",
                            dependency.task_id, dependency.depends_on_task_id
                        )
                    })?;
            }
            self.conn
                .execute_batch("COMMIT")
                .context("failed to commit swarm run transaction")?;
            Ok(())
        })();
        if let Err(error) = result {
            let _ = self.conn.execute_batch("ROLLBACK");
            return Err(error);
        }
        Ok(())
    }

    pub fn get_swarm_run(&self, id: &str) -> Result<Option<SwarmRun>> {
        let mut stmt = self
            .conn
            .prepare(
                r#"
                SELECT
                  id,
                  group_folder,
                  chat_jid,
                  created_by,
                  objective,
                  requested_lane,
                  objective_signature_json,
                  objective_gate_json,
                  status,
                  max_concurrency,
                  summary,
                  result_json,
                  created_at,
                  updated_at,
                  completed_at
                FROM swarm_runs
                WHERE id = ?1
                "#,
            )
            .with_context(|| format!("failed to prepare swarm run lookup for {}", id))?;
        let row = stmt.query_row(params![id], map_swarm_run_row);
        match row {
            Ok(run) => Ok(Some(run)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(err) => Err(err).with_context(|| format!("failed to read swarm run {}", id)),
        }
    }

    pub fn list_swarm_runs(&self, limit: usize) -> Result<Vec<SwarmRun>> {
        let mut stmt = self
            .conn
            .prepare(
                r#"
                SELECT
                  id,
                  group_folder,
                  chat_jid,
                  created_by,
                  objective,
                  requested_lane,
                  objective_signature_json,
                  objective_gate_json,
                  status,
                  max_concurrency,
                  summary,
                  result_json,
                  created_at,
                  updated_at,
                  completed_at
                FROM swarm_runs
                ORDER BY updated_at DESC
                LIMIT ?1
                "#,
            )
            .context("failed to prepare swarm runs query")?;
        let rows = stmt
            .query_map(params![limit.max(1) as i64], map_swarm_run_row)
            .context("failed to query swarm runs")?;
        let mut runs = Vec::new();
        for row in rows {
            runs.push(row.context("failed to decode swarm run row")?);
        }
        Ok(runs)
    }

    pub fn list_active_swarm_runs(&self) -> Result<Vec<SwarmRun>> {
        let mut stmt = self
            .conn
            .prepare(
                r#"
                SELECT
                  id,
                  group_folder,
                  chat_jid,
                  created_by,
                  objective,
                  requested_lane,
                  objective_signature_json,
                  objective_gate_json,
                  status,
                  max_concurrency,
                  summary,
                  result_json,
                  created_at,
                  updated_at,
                  completed_at
                FROM swarm_runs
                WHERE status IN ('pending', 'active')
                ORDER BY created_at
                "#,
            )
            .context("failed to prepare active swarm runs query")?;
        let rows = stmt
            .query_map([], map_swarm_run_row)
            .context("failed to query active swarm runs")?;
        let mut runs = Vec::new();
        for row in rows {
            runs.push(row.context("failed to decode active swarm run row")?);
        }
        Ok(runs)
    }

    pub fn list_swarm_tasks(&self, run_id: &str) -> Result<Vec<SwarmTask>> {
        let mut stmt = self
            .conn
            .prepare(
                r#"
                SELECT
                  id,
                  run_id,
                  task_key,
                  title,
                  prompt,
                  command,
                  requested_lane,
                  resolved_lane,
                  task_signature_json,
                  boundary_quadrant,
                  gate_decision,
                  gate_evaluation_json,
                  required_roles_json,
                  status,
                  priority,
                  target_group_folder,
                  target_chat_jid,
                  cwd,
                  repo,
                  repo_path,
                  sync,
                  host,
                  user,
                  port,
                  timeout_ms,
                  metadata_json,
                  result_json,
                  error,
                  lease_owner,
                  lease_expires_at,
                  attempts,
                  max_attempts,
                  created_at,
                  updated_at,
                  started_at,
                  completed_at
                FROM swarm_tasks
                WHERE run_id = ?1
                ORDER BY priority DESC, created_at
                "#,
            )
            .with_context(|| format!("failed to prepare swarm task query for {}", run_id))?;
        let rows = stmt
            .query_map(params![run_id], map_swarm_task_row)
            .with_context(|| format!("failed to query swarm tasks for {}", run_id))?;
        let mut tasks = Vec::new();
        for row in rows {
            tasks.push(row.context("failed to decode swarm task row")?);
        }
        Ok(tasks)
    }

    pub fn get_swarm_task(&self, task_id: &str) -> Result<Option<SwarmTask>> {
        let mut stmt = self
            .conn
            .prepare(
                r#"
                SELECT
                  id,
                  run_id,
                  task_key,
                  title,
                  prompt,
                  command,
                  requested_lane,
                  resolved_lane,
                  task_signature_json,
                  boundary_quadrant,
                  gate_decision,
                  gate_evaluation_json,
                  required_roles_json,
                  status,
                  priority,
                  target_group_folder,
                  target_chat_jid,
                  cwd,
                  repo,
                  repo_path,
                  sync,
                  host,
                  user,
                  port,
                  timeout_ms,
                  metadata_json,
                  result_json,
                  error,
                  lease_owner,
                  lease_expires_at,
                  attempts,
                  max_attempts,
                  created_at,
                  updated_at,
                  started_at,
                  completed_at
                FROM swarm_tasks
                WHERE id = ?1
                "#,
            )
            .with_context(|| format!("failed to prepare swarm task lookup for {}", task_id))?;
        let row = stmt.query_row(params![task_id], map_swarm_task_row);
        match row {
            Ok(task) => Ok(Some(task)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(err) => Err(err).with_context(|| format!("failed to read swarm task {}", task_id)),
        }
    }

    pub fn list_swarm_task_dependencies(&self, run_id: &str) -> Result<Vec<SwarmTaskDependency>> {
        let mut stmt = self
            .conn
            .prepare(
                r#"
                SELECT d.task_id, d.depends_on_task_id
                FROM swarm_task_dependencies d
                INNER JOIN swarm_tasks t ON t.id = d.task_id
                WHERE t.run_id = ?1
                "#,
            )
            .with_context(|| {
                format!(
                    "failed to prepare swarm task dependency query for {}",
                    run_id
                )
            })?;
        let rows = stmt
            .query_map(params![run_id], |row| {
                Ok(SwarmTaskDependency {
                    task_id: row.get(0)?,
                    depends_on_task_id: row.get(1)?,
                })
            })
            .with_context(|| format!("failed to query swarm dependencies for {}", run_id))?;
        let mut dependencies = Vec::new();
        for row in rows {
            dependencies.push(row.context("failed to decode swarm dependency row")?);
        }
        Ok(dependencies)
    }

    pub fn update_swarm_run(&self, run: &SwarmRun) -> Result<()> {
        self.conn
            .execute(
                r#"
                UPDATE swarm_runs
                SET
                  group_folder = ?1,
                  chat_jid = ?2,
                  created_by = ?3,
                  objective = ?4,
                  requested_lane = ?5,
                  objective_signature_json = ?6,
                  objective_gate_json = ?7,
                  status = ?8,
                  max_concurrency = ?9,
                  summary = ?10,
                  result_json = ?11,
                  created_at = ?12,
                  updated_at = ?13,
                  completed_at = ?14
                WHERE id = ?15
                "#,
                params![
                    run.group_folder,
                    run.chat_jid,
                    run.created_by,
                    run.objective,
                    run.requested_lane.as_str(),
                    run.objective_signature
                        .as_ref()
                        .map(serde_json::to_string)
                        .transpose()?,
                    run.objective_gate
                        .as_ref()
                        .map(serde_json::to_string)
                        .transpose()?,
                    run.status.as_str(),
                    run.max_concurrency,
                    run.summary,
                    run.result.as_ref().map(Value::to_string),
                    run.created_at,
                    run.updated_at,
                    run.completed_at,
                    run.id,
                ],
            )
            .with_context(|| format!("failed to update swarm run {}", run.id))?;
        Ok(())
    }

    pub fn update_swarm_task(&self, task: &SwarmTask) -> Result<()> {
        self.conn
            .execute(
                r#"
                UPDATE swarm_tasks
                SET
                  run_id = ?1,
                  task_key = ?2,
                  title = ?3,
                  prompt = ?4,
                  command = ?5,
                  requested_lane = ?6,
                  resolved_lane = ?7,
                  task_signature_json = ?8,
                  boundary_quadrant = ?9,
                  gate_decision = ?10,
                  gate_evaluation_json = ?11,
                  required_roles_json = ?12,
                  status = ?13,
                  priority = ?14,
                  target_group_folder = ?15,
                  target_chat_jid = ?16,
                  cwd = ?17,
                  repo = ?18,
                  repo_path = ?19,
                  sync = ?20,
                  host = ?21,
                  user = ?22,
                  port = ?23,
                  timeout_ms = ?24,
                  metadata_json = ?25,
                  result_json = ?26,
                  error = ?27,
                  lease_owner = ?28,
                  lease_expires_at = ?29,
                  attempts = ?30,
                  max_attempts = ?31,
                  created_at = ?32,
                  updated_at = ?33,
                  started_at = ?34,
                  completed_at = ?35
                WHERE id = ?36
                "#,
                params![
                    task.run_id,
                    task.task_key,
                    task.title,
                    task.prompt,
                    task.command,
                    task.requested_lane.as_str(),
                    task.resolved_lane.as_ref().map(SwarmResolvedLane::as_str),
                    task.task_signature
                        .as_ref()
                        .map(serde_json::to_string)
                        .transpose()?,
                    task.boundary_quadrant.as_ref().map(|value| value.as_str()),
                    task.gate_decision.as_ref().map(GateDecision::as_str),
                    task.gate_evaluation
                        .as_ref()
                        .map(serde_json::to_string)
                        .transpose()?,
                    serde_json::to_string(&task.required_roles)?,
                    task.status.as_str(),
                    task.priority,
                    task.target_group_folder,
                    task.target_chat_jid,
                    task.cwd,
                    task.repo,
                    task.repo_path,
                    if task.sync { 1 } else { 0 },
                    task.host,
                    task.user,
                    task.port.map(i64::from),
                    task.timeout_ms.map(|value| value as i64),
                    task.metadata.as_ref().map(Value::to_string),
                    task.result.as_ref().map(Value::to_string),
                    task.error,
                    task.lease_owner,
                    task.lease_expires_at,
                    task.attempts,
                    task.max_attempts,
                    task.created_at,
                    task.updated_at,
                    task.started_at,
                    task.completed_at,
                    task.id,
                ],
            )
            .with_context(|| format!("failed to update swarm task {}", task.id))?;
        Ok(())
    }

    pub fn claim_swarm_task(
        &self,
        task_id: &str,
        lease_owner: &str,
        lease_expires_at: &str,
        resolved_lane: &SwarmResolvedLane,
        started_at: &str,
    ) -> Result<bool> {
        let info = self
            .conn
            .execute(
                r#"
                UPDATE swarm_tasks
                SET
                  status = 'running',
                  resolved_lane = ?1,
                  lease_owner = ?2,
                  lease_expires_at = ?3,
                  attempts = attempts + 1,
                  started_at = COALESCE(started_at, ?4),
                  updated_at = ?4
                WHERE id = ?5
                  AND status IN ('pending', 'ready')
                "#,
                params![
                    resolved_lane.as_str(),
                    lease_owner,
                    lease_expires_at,
                    started_at,
                    task_id,
                ],
            )
            .with_context(|| format!("failed to claim swarm task {}", task_id))?;
        Ok(info > 0)
    }

    pub fn release_expired_swarm_task_leases(&self, now_iso: &str) -> Result<usize> {
        let updated = self
            .conn
            .execute(
                r#"
                UPDATE swarm_tasks
                SET
                  status = CASE
                    WHEN attempts >= max_attempts THEN 'failed'
                    ELSE 'ready'
                  END,
                  error = CASE
                    WHEN attempts >= max_attempts THEN COALESCE(error, 'Swarm task lease expired too many times.')
                    ELSE error
                  END,
                  lease_owner = NULL,
                  lease_expires_at = NULL,
                  updated_at = ?1
                WHERE status IN ('running')
                  AND lease_expires_at IS NOT NULL
                  AND lease_expires_at <= ?1
                "#,
                params![now_iso],
            )
            .context("failed to release expired swarm task leases")?;
        Ok(updated)
    }

    pub fn cancel_swarm_run(&self, run_id: &str, now_iso: &str) -> Result<()> {
        self.conn
            .execute_batch("BEGIN IMMEDIATE")
            .context("failed to begin swarm cancel transaction")?;
        let result = (|| -> Result<()> {
            self.conn
                .execute(
                    r#"
                    UPDATE swarm_runs
                    SET status = 'canceled', completed_at = ?1, updated_at = ?1
                    WHERE id = ?2
                    "#,
                    params![now_iso, run_id],
                )
                .with_context(|| format!("failed to cancel swarm run {}", run_id))?;
            self.conn
                .execute(
                    r#"
                    UPDATE swarm_tasks
                    SET
                      status = 'canceled',
                      lease_owner = NULL,
                      lease_expires_at = NULL,
                      completed_at = ?1,
                      updated_at = ?1
                    WHERE run_id = ?2
                      AND status IN ('pending', 'ready', 'running', 'blocked')
                    "#,
                    params![now_iso, run_id],
                )
                .with_context(|| format!("failed to cancel swarm tasks for {}", run_id))?;
            self.conn
                .execute_batch("COMMIT")
                .context("failed to commit swarm cancel transaction")?;
            Ok(())
        })();
        if let Err(error) = result {
            let _ = self.conn.execute_batch("ROLLBACK");
            return Err(error);
        }
        Ok(())
    }

    pub fn upsert_registered_group(&self, group: &Group) -> Result<()> {
        self.conn
            .execute(
                r#"
                INSERT INTO registered_groups (
                  jid,
                  name,
                  folder,
                  trigger_pattern,
                  added_at,
                  container_config,
                  requires_trigger,
                  is_main
                )
                VALUES (?1, ?2, ?3, ?4, ?5, NULL, ?6, ?7)
                ON CONFLICT(jid) DO UPDATE SET
                  name = excluded.name,
                  folder = excluded.folder,
                  trigger_pattern = excluded.trigger_pattern,
                  added_at = excluded.added_at,
                  requires_trigger = excluded.requires_trigger,
                  is_main = excluded.is_main
                "#,
                params![
                    group.jid,
                    group.name,
                    group.folder,
                    group.trigger,
                    group.added_at,
                    if group.requires_trigger { 1 } else { 0 },
                    if group.is_main { 1 } else { 0 },
                ],
            )
            .with_context(|| format!("failed to upsert registered group {}", group.jid))?;
        Ok(())
    }

    pub fn list_registered_groups(&self) -> Result<Vec<Group>> {
        let mut stmt = self
            .conn
            .prepare(
                r#"
                SELECT jid, name, folder, trigger_pattern, added_at, requires_trigger, is_main
                FROM registered_groups
                ORDER BY folder
                "#,
            )
            .context("failed to prepare registered_groups query")?;
        let rows = stmt
            .query_map([], |row| {
                Ok(Group {
                    jid: row.get(0)?,
                    name: row.get(1)?,
                    folder: row.get(2)?,
                    trigger: row.get(3)?,
                    added_at: row.get(4)?,
                    requires_trigger: row.get::<_, i64>(5)? != 0,
                    is_main: row.get::<_, i64>(6)? != 0,
                })
            })
            .context("failed to query registered groups")?;

        let mut groups = Vec::new();
        for row in rows {
            groups.push(row.context("failed to decode registered group row")?);
        }
        Ok(groups)
    }

    pub fn store_chat_metadata(
        &self,
        chat_jid: &str,
        timestamp: &str,
        name: Option<&str>,
        channel: Option<&str>,
        is_group: Option<bool>,
    ) -> Result<()> {
        let channel = channel.map(str::to_string);
        let group = is_group.map(|value| if value { 1 } else { 0 });
        if let Some(name) = name {
            self.conn
                .execute(
                    r#"
                    INSERT INTO chats (jid, name, last_message_time, channel, is_group)
                    VALUES (?1, ?2, ?3, ?4, ?5)
                    ON CONFLICT(jid) DO UPDATE SET
                      name = excluded.name,
                      last_message_time = MAX(last_message_time, excluded.last_message_time),
                      channel = COALESCE(excluded.channel, channel),
                      is_group = COALESCE(excluded.is_group, is_group)
                    "#,
                    params![chat_jid, name, timestamp, channel, group],
                )
                .with_context(|| format!("failed to store chat metadata for {}", chat_jid))?;
        } else {
            self.conn
                .execute(
                    r#"
                    INSERT INTO chats (jid, name, last_message_time, channel, is_group)
                    VALUES (?1, ?2, ?3, ?4, ?5)
                    ON CONFLICT(jid) DO UPDATE SET
                      last_message_time = MAX(last_message_time, excluded.last_message_time),
                      channel = COALESCE(excluded.channel, channel),
                      is_group = COALESCE(excluded.is_group, is_group)
                    "#,
                    params![chat_jid, chat_jid, timestamp, channel, group],
                )
                .with_context(|| format!("failed to store chat metadata for {}", chat_jid))?;
        }
        Ok(())
    }

    pub fn store_message(&self, message: &MessageRecord) -> Result<()> {
        self.conn
            .execute(
                r#"
                INSERT OR REPLACE INTO messages (
                  id,
                  chat_jid,
                  sender,
                  sender_name,
                  content,
                  timestamp,
                  is_from_me,
                  is_bot_message
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                "#,
                params![
                    message.id,
                    message.chat_jid,
                    message.sender,
                    message.sender_name,
                    message.content,
                    message.timestamp,
                    if message.is_from_me { 1 } else { 0 },
                    if message.is_bot_message { 1 } else { 0 },
                ],
            )
            .with_context(|| format!("failed to store message {}", message.id))?;
        Ok(())
    }

    pub fn messages_since(
        &self,
        chat_jid: &str,
        since_timestamp: &str,
        limit: usize,
        include_bot_messages: bool,
    ) -> Result<Vec<MessageRecord>> {
        let sql = if include_bot_messages {
            r#"
            SELECT id, chat_jid, sender, sender_name, content, timestamp, is_from_me, is_bot_message
            FROM messages
            WHERE chat_jid = ?1 AND timestamp > ?2
              AND content != '' AND content IS NOT NULL
            ORDER BY timestamp
            LIMIT ?3
            "#
        } else {
            r#"
            SELECT id, chat_jid, sender, sender_name, content, timestamp, is_from_me, is_bot_message
            FROM messages
            WHERE chat_jid = ?1 AND timestamp > ?2
              AND is_bot_message = 0
              AND content != '' AND content IS NOT NULL
            ORDER BY timestamp
            LIMIT ?3
            "#
        };

        let mut stmt = self
            .conn
            .prepare(sql)
            .with_context(|| format!("failed to prepare message query for {}", chat_jid))?;
        let rows = stmt
            .query_map(params![chat_jid, since_timestamp, limit as i64], |row| {
                Ok(MessageRecord {
                    id: row.get(0)?,
                    chat_jid: row.get(1)?,
                    sender: row.get(2)?,
                    sender_name: row.get(3)?,
                    content: row.get(4)?,
                    timestamp: row.get(5)?,
                    is_from_me: row.get::<_, i64>(6)? != 0,
                    is_bot_message: row.get::<_, i64>(7)? != 0,
                })
            })
            .with_context(|| format!("failed to execute message query for {}", chat_jid))?;

        let mut messages = Vec::new();
        for row in rows {
            messages.push(row.context("failed to decode message row")?);
        }
        Ok(messages)
    }

    pub fn create_task(&self, task: &ScheduledTask) -> Result<()> {
        self.conn
            .execute(
                r#"
                INSERT INTO scheduled_tasks (
                  id,
                  group_folder,
                  chat_jid,
                  prompt,
                  script,
                  request_plane,
                  schedule_type,
                  schedule_value,
                  context_mode,
                  next_run,
                  last_run,
                  last_result,
                  status,
                  created_at
                )
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)
                "#,
                params![
                    task.id,
                    task.group_folder,
                    task.chat_jid,
                    task.prompt,
                    task.script,
                    task.request_plane.as_ref().map(RequestPlane::as_str),
                    task.schedule_type.as_str(),
                    task.schedule_value,
                    task.context_mode.as_str(),
                    task.next_run,
                    task.last_run,
                    task.last_result,
                    task.status.as_str(),
                    task.created_at,
                ],
            )
            .with_context(|| format!("failed to create scheduled task {}", task.id))?;
        Ok(())
    }

    pub fn get_task_by_id(&self, task_id: &str) -> Result<Option<ScheduledTask>> {
        let mut stmt = self
            .conn
            .prepare(
                r#"
                SELECT
                  id,
                  group_folder,
                  chat_jid,
                  prompt,
                  script,
                  request_plane,
                  schedule_type,
                  schedule_value,
                  context_mode,
                  next_run,
                  last_run,
                  last_result,
                  status,
                  created_at
                FROM scheduled_tasks
                WHERE id = ?1
                "#,
            )
            .with_context(|| format!("failed to prepare task lookup for {}", task_id))?;
        let row = stmt.query_row(params![task_id], map_task_row);
        match row {
            Ok(task) => Ok(Some(task)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(err) => Err(err).with_context(|| format!("failed to read task {}", task_id)),
        }
    }

    pub fn list_tasks(&self) -> Result<Vec<ScheduledTask>> {
        self.query_tasks(
            r#"
            SELECT
              id,
              group_folder,
              chat_jid,
              prompt,
              script,
              request_plane,
              schedule_type,
              schedule_value,
              context_mode,
              next_run,
              last_run,
              last_result,
              status,
              created_at
            FROM scheduled_tasks
            ORDER BY created_at DESC
            "#,
            [],
        )
    }

    pub fn list_tasks_for_group(&self, group_folder: &str) -> Result<Vec<ScheduledTask>> {
        self.query_tasks(
            r#"
            SELECT
              id,
              group_folder,
              chat_jid,
              prompt,
              script,
              request_plane,
              schedule_type,
              schedule_value,
              context_mode,
              next_run,
              last_run,
              last_result,
              status,
              created_at
            FROM scheduled_tasks
            WHERE group_folder = ?1
            ORDER BY created_at DESC
            "#,
            [group_folder],
        )
    }

    pub fn list_due_tasks(&self, now_iso: &str) -> Result<Vec<ScheduledTask>> {
        self.query_tasks(
            r#"
            SELECT
              id,
              group_folder,
              chat_jid,
              prompt,
              script,
              request_plane,
              schedule_type,
              schedule_value,
              context_mode,
              next_run,
              last_run,
              last_result,
              status,
              created_at
            FROM scheduled_tasks
            WHERE status = 'active' AND next_run IS NOT NULL AND next_run <= ?1
            ORDER BY next_run
            "#,
            [now_iso],
        )
    }

    pub fn update_task_after_run(
        &self,
        task_id: &str,
        next_run: Option<&str>,
        last_result: &str,
    ) -> Result<()> {
        let now = chrono::Utc::now().to_rfc3339();
        self.conn
            .execute(
                r#"
                UPDATE scheduled_tasks
                SET next_run = ?1,
                    last_run = ?2,
                    last_result = ?3,
                    status = CASE WHEN ?1 IS NULL THEN 'completed' ELSE status END
                WHERE id = ?4
                "#,
                params![next_run, now, last_result, task_id],
            )
            .with_context(|| format!("failed to update task after run {}", task_id))?;
        Ok(())
    }

    pub fn set_task_status(&self, task_id: &str, status: TaskStatus) -> Result<()> {
        self.conn
            .execute(
                "UPDATE scheduled_tasks SET status = ?1 WHERE id = ?2",
                params![status.as_str(), task_id],
            )
            .with_context(|| format!("failed to update task status for {}", task_id))?;
        Ok(())
    }

    pub fn delete_task(&self, task_id: &str) -> Result<()> {
        self.conn
            .execute(
                "DELETE FROM task_run_logs WHERE task_id = ?1",
                params![task_id],
            )
            .with_context(|| format!("failed to delete task logs for {}", task_id))?;
        self.conn
            .execute(
                "DELETE FROM scheduled_tasks WHERE id = ?1",
                params![task_id],
            )
            .with_context(|| format!("failed to delete task {}", task_id))?;
        Ok(())
    }

    pub fn log_task_run(&self, log: &TaskRunLog) -> Result<()> {
        self.conn
            .execute(
                r#"
                INSERT INTO task_run_logs (task_id, run_at, duration_ms, status, result, error)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                "#,
                params![
                    log.task_id,
                    log.run_at,
                    log.duration_ms,
                    log.status.as_str(),
                    log.result,
                    log.error,
                ],
            )
            .with_context(|| format!("failed to insert task run log for {}", log.task_id))?;
        Ok(())
    }

    pub fn record_pm_audit_event(&self, event: &PmAuditEvent) -> Result<i64> {
        self.conn
            .execute(
                r#"
                INSERT INTO pm_audit_events (
                  issue_key,
                  issue_identifier,
                  thread_jid,
                  phase,
                  status,
                  tool,
                  error_code,
                  blocking,
                  metadata_json,
                  created_at
                )
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
                "#,
                params![
                    event.issue_key,
                    event.issue_identifier,
                    event.thread_jid,
                    event.phase,
                    event.status,
                    event.tool,
                    event.error_code,
                    if event.blocking { 1 } else { 0 },
                    event.metadata.as_ref().map(Value::to_string),
                    event
                        .created_at
                        .clone()
                        .unwrap_or_else(|| chrono::Utc::now().to_rfc3339()),
                ],
            )
            .context("failed to insert pm audit event")?;
        Ok(self.conn.last_insert_rowid())
    }

    pub fn get_pm_audit_events(
        &self,
        issue_identifier: Option<&str>,
    ) -> Result<Vec<StoredPmAuditEvent>> {
        let mut sql = String::from(
            r#"
            SELECT
              id,
              issue_key,
              issue_identifier,
              thread_jid,
              phase,
              status,
              tool,
              error_code,
              blocking,
              metadata_json,
              created_at
            FROM pm_audit_events
            "#,
        );
        if issue_identifier.is_some() {
            sql.push_str(" WHERE issue_identifier = ?1");
        }
        sql.push_str(" ORDER BY id");
        let mut stmt = self
            .conn
            .prepare(&sql)
            .context("failed to prepare pm audit query")?;
        let rows = if let Some(issue_identifier) = issue_identifier {
            stmt.query_map(params![issue_identifier], map_pm_audit_row)?
        } else {
            stmt.query_map([], map_pm_audit_row)?
        };
        let mut events = Vec::new();
        for row in rows {
            events.push(row.context("failed to decode pm audit row")?);
        }
        Ok(events)
    }

    pub fn get_pm_issue_memory(&self, issue_key: &str) -> Result<Option<PmIssueMemory>> {
        let mut stmt = self
            .conn
            .prepare(
                r#"
                SELECT
                  issue_key,
                  issue_identifier,
                  summary,
                  next_action,
                  blockers_json,
                  current_state,
                  repo_hint,
                  last_source,
                  memory_json,
                  updated_at
                FROM pm_issue_memory
                WHERE issue_key = ?1
                "#,
            )
            .with_context(|| {
                format!("failed to prepare pm issue memory lookup for {}", issue_key)
            })?;
        let row = stmt.query_row(params![issue_key], map_pm_issue_memory_row);
        match row {
            Ok(memory) => Ok(Some(memory)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(err) => Err(err)
                .with_context(|| format!("failed to read pm issue memory for {}", issue_key)),
        }
    }

    pub fn get_pm_issue_memory_by_identifier(
        &self,
        issue_identifier: &str,
    ) -> Result<Option<PmIssueMemory>> {
        let mut stmt = self
            .conn
            .prepare(
                r#"
                SELECT
                  issue_key,
                  issue_identifier,
                  summary,
                  next_action,
                  blockers_json,
                  current_state,
                  repo_hint,
                  last_source,
                  memory_json,
                  updated_at
                FROM pm_issue_memory
                WHERE issue_identifier = ?1
                ORDER BY updated_at DESC
                LIMIT 1
                "#,
            )
            .with_context(|| {
                format!(
                    "failed to prepare pm issue memory lookup for identifier {}",
                    issue_identifier
                )
            })?;
        let row = stmt.query_row(params![issue_identifier], map_pm_issue_memory_row);
        match row {
            Ok(memory) => Ok(Some(memory)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(err) => Err(err).with_context(|| {
                format!(
                    "failed to read pm issue memory for identifier {}",
                    issue_identifier
                )
            }),
        }
    }

    pub fn set_pm_issue_memory(&self, memory: &PmIssueMemory) -> Result<()> {
        self.conn
            .execute(
                r#"
                INSERT INTO pm_issue_memory (
                  issue_key,
                  issue_identifier,
                  summary,
                  next_action,
                  blockers_json,
                  current_state,
                  repo_hint,
                  last_source,
                  memory_json,
                  updated_at
                )
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
                ON CONFLICT(issue_key) DO UPDATE SET
                  issue_identifier = excluded.issue_identifier,
                  summary = excluded.summary,
                  next_action = excluded.next_action,
                  blockers_json = excluded.blockers_json,
                  current_state = excluded.current_state,
                  repo_hint = excluded.repo_hint,
                  last_source = excluded.last_source,
                  memory_json = excluded.memory_json,
                  updated_at = excluded.updated_at
                "#,
                params![
                    memory.issue_key,
                    memory.issue_identifier,
                    memory.summary,
                    memory.next_action,
                    memory
                        .blockers
                        .as_ref()
                        .map(|value| serde_json::to_string(value).unwrap()),
                    memory.current_state,
                    memory.repo_hint,
                    memory.last_source,
                    memory.memory.as_ref().map(Value::to_string),
                    memory.updated_at,
                ],
            )
            .with_context(|| {
                format!("failed to upsert pm issue memory for {}", memory.issue_key)
            })?;
        Ok(())
    }

    pub fn get_pm_issue_tracked_comment(
        &self,
        issue_key: &str,
        comment_kind: &str,
    ) -> Result<Option<PmIssueTrackedComment>> {
        let mut stmt = self
            .conn
            .prepare(
                r#"
                SELECT
                  issue_key,
                  issue_identifier,
                  comment_kind,
                  comment_id,
                  body_hash,
                  body,
                  updated_at
                FROM pm_issue_comments
                WHERE issue_key = ?1 AND comment_kind = ?2
                "#,
            )
            .with_context(|| {
                format!(
                    "failed to prepare pm tracked comment lookup for {} {}",
                    issue_key, comment_kind
                )
            })?;
        let row = stmt.query_row(params![issue_key, comment_kind], |row| {
            Ok(PmIssueTrackedComment {
                issue_key: row.get(0)?,
                issue_identifier: row.get(1)?,
                comment_kind: row.get(2)?,
                comment_id: row.get(3)?,
                body_hash: row.get(4)?,
                body: row.get(5)?,
                updated_at: row.get(6)?,
            })
        });
        match row {
            Ok(comment) => Ok(Some(comment)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(err) => Err(err).with_context(|| {
                format!(
                    "failed to read pm tracked comment for {} {}",
                    issue_key, comment_kind
                )
            }),
        }
    }

    pub fn set_pm_issue_tracked_comment(&self, comment: &PmIssueTrackedComment) -> Result<()> {
        self.conn
            .execute(
                r#"
                INSERT INTO pm_issue_comments (
                  issue_key,
                  issue_identifier,
                  comment_kind,
                  comment_id,
                  body_hash,
                  body,
                  updated_at
                )
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                ON CONFLICT(issue_key, comment_kind) DO UPDATE SET
                  issue_identifier = excluded.issue_identifier,
                  comment_id = excluded.comment_id,
                  body_hash = excluded.body_hash,
                  body = excluded.body,
                  updated_at = excluded.updated_at
                "#,
                params![
                    comment.issue_key,
                    comment.issue_identifier,
                    comment.comment_kind,
                    comment.comment_id,
                    comment.body_hash,
                    comment.body,
                    comment.updated_at,
                ],
            )
            .with_context(|| {
                format!(
                    "failed to upsert pm tracked comment for {} {}",
                    comment.issue_key, comment.comment_kind
                )
            })?;
        Ok(())
    }

    pub fn upsert_observability_event(
        &self,
        input: &UpsertObservabilityEventInput,
    ) -> Result<(ObservabilityEvent, bool)> {
        let seen_at = input
            .seen_at
            .clone()
            .unwrap_or_else(|| chrono::Utc::now().to_rfc3339());
        if self
            .get_observability_event_by_fingerprint(&input.fingerprint)?
            .is_some()
        {
            self.conn
                .execute(
                    r#"
                    UPDATE observability_events
                    SET source = ?1,
                        environment = ?2,
                        service = ?3,
                        category = ?4,
                        severity = ?5,
                        title = ?6,
                        message = ?7,
                        status = ?8,
                        deployment_url = ?9,
                        healthcheck_url = ?10,
                        repo = ?11,
                        repo_path = ?12,
                        metadata_json = ?13,
                        raw_payload_json = ?14,
                        last_seen_at = ?15,
                        occurrence_count = occurrence_count + 1
                    WHERE fingerprint = ?16
                    "#,
                    params![
                        input.source,
                        input.environment,
                        input.service,
                        input.category,
                        input.severity.as_str(),
                        input.title,
                        input.message,
                        input
                            .status
                            .clone()
                            .unwrap_or(ObservabilityEventStatus::Open)
                            .as_str(),
                        input.deployment_url,
                        input.healthcheck_url,
                        input.repo,
                        input.repo_path,
                        input.metadata.as_ref().map(Value::to_string),
                        input.raw_payload.as_ref().map(Value::to_string),
                        seen_at,
                        input.fingerprint,
                    ],
                )
                .with_context(|| {
                    format!("failed to update observability event {}", input.fingerprint)
                })?;
            let stored = self
                .get_observability_event_by_fingerprint(&input.fingerprint)?
                .with_context(|| {
                    format!(
                        "observability event {} disappeared after update",
                        input.fingerprint
                    )
                })?;
            return Ok((stored, false));
        }

        self.conn
            .execute(
                r#"
                INSERT INTO observability_events (
                  id,
                  fingerprint,
                  source,
                  environment,
                  service,
                  category,
                  severity,
                  title,
                  message,
                  status,
                  deployment_url,
                  healthcheck_url,
                  repo,
                  repo_path,
                  metadata_json,
                  raw_payload_json,
                  first_seen_at,
                  last_seen_at,
                  occurrence_count
                )
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, 1)
                "#,
                params![
                    input.id,
                    input.fingerprint,
                    input.source,
                    input.environment,
                    input.service,
                    input.category,
                    input.severity.as_str(),
                    input.title,
                    input.message,
                    input
                        .status
                        .clone()
                        .unwrap_or(ObservabilityEventStatus::Open)
                        .as_str(),
                    input.deployment_url,
                    input.healthcheck_url,
                    input.repo,
                    input.repo_path,
                    input.metadata.as_ref().map(Value::to_string),
                    input.raw_payload.as_ref().map(Value::to_string),
                    seen_at,
                    seen_at,
                ],
            )
            .with_context(|| {
                format!(
                    "failed to insert observability event {}",
                    input.fingerprint
                )
            })?;
        let stored = self
            .get_observability_event_by_fingerprint(&input.fingerprint)?
            .with_context(|| {
                format!(
                    "observability event {} disappeared after insert",
                    input.fingerprint
                )
            })?;
        Ok((stored, true))
    }

    pub fn get_observability_event_by_fingerprint(
        &self,
        fingerprint: &str,
    ) -> Result<Option<ObservabilityEvent>> {
        let mut stmt = self
            .conn
            .prepare(
                r#"
                SELECT
                  id,
                  fingerprint,
                  source,
                  environment,
                  service,
                  category,
                  severity,
                  title,
                  message,
                  status,
                  chat_jid,
                  thread_ts,
                  swarm_run_id,
                  deployment_url,
                  healthcheck_url,
                  repo,
                  repo_path,
                  metadata_json,
                  raw_payload_json,
                  first_seen_at,
                  last_seen_at,
                  occurrence_count
                FROM observability_events
                WHERE fingerprint = ?1
                "#,
            )
            .with_context(|| {
                format!("failed to prepare observability lookup for {}", fingerprint)
            })?;
        let row = stmt.query_row(params![fingerprint], map_observability_event_row);
        match row {
            Ok(event) => Ok(Some(event)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(error) => Err(error)
                .with_context(|| format!("failed to read observability event {}", fingerprint)),
        }
    }

    pub fn get_observability_event_by_id(&self, id: &str) -> Result<Option<ObservabilityEvent>> {
        let mut stmt = self
            .conn
            .prepare(
                r#"
                SELECT
                  id,
                  fingerprint,
                  source,
                  environment,
                  service,
                  category,
                  severity,
                  title,
                  message,
                  status,
                  chat_jid,
                  thread_ts,
                  swarm_run_id,
                  deployment_url,
                  healthcheck_url,
                  repo,
                  repo_path,
                  metadata_json,
                  raw_payload_json,
                  first_seen_at,
                  last_seen_at,
                  occurrence_count
                FROM observability_events
                WHERE id = ?1
                "#,
            )
            .with_context(|| format!("failed to prepare observability lookup for {}", id))?;
        let row = stmt.query_row(params![id], map_observability_event_row);
        match row {
            Ok(event) => Ok(Some(event)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(error) => {
                Err(error).with_context(|| format!("failed to read observability event {}", id))
            }
        }
    }

    pub fn list_observability_events(
        &self,
        limit: usize,
        status: Option<&ObservabilityEventStatus>,
        severity: Option<&ObservabilitySeverity>,
    ) -> Result<Vec<ObservabilityEvent>> {
        let mut sql = String::from(
            r#"
            SELECT
              id,
              fingerprint,
              source,
              environment,
              service,
              category,
              severity,
              title,
              message,
              status,
              chat_jid,
              thread_ts,
              swarm_run_id,
              deployment_url,
              healthcheck_url,
              repo,
              repo_path,
              metadata_json,
              raw_payload_json,
              first_seen_at,
              last_seen_at,
              occurrence_count
            FROM observability_events
            "#,
        );
        let mut conditions = Vec::new();
        if status.is_some() {
            conditions.push("status = ?1");
        }
        if severity.is_some() {
            conditions.push(if status.is_some() {
                "severity = ?2"
            } else {
                "severity = ?1"
            });
        }
        if !conditions.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&conditions.join(" AND "));
        }
        sql.push_str(" ORDER BY last_seen_at DESC LIMIT ?");

        let mut stmt = self
            .conn
            .prepare(&sql)
            .context("failed to prepare observability list query")?;
        let rows = match (status, severity) {
            (Some(status), Some(severity)) => stmt.query_map(
                params![status.as_str(), severity.as_str(), limit.max(1) as i64],
                map_observability_event_row,
            )?,
            (Some(status), None) => stmt.query_map(
                params![status.as_str(), limit.max(1) as i64],
                map_observability_event_row,
            )?,
            (None, Some(severity)) => stmt.query_map(
                params![severity.as_str(), limit.max(1) as i64],
                map_observability_event_row,
            )?,
            (None, None) => {
                stmt.query_map(params![limit.max(1) as i64], map_observability_event_row)?
            }
        };
        let mut events = Vec::new();
        for row in rows {
            events.push(row.context("failed to decode observability event row")?);
        }
        Ok(events)
    }

    pub fn attach_observability_event_thread(
        &self,
        fingerprint: &str,
        chat_jid: &str,
        thread_ts: Option<&str>,
    ) -> Result<()> {
        self.conn
            .execute(
                r#"
                UPDATE observability_events
                SET chat_jid = ?1, thread_ts = ?2
                WHERE fingerprint = ?3
                "#,
                params![chat_jid, thread_ts, fingerprint],
            )
            .with_context(|| {
                format!(
                    "failed to attach thread info to observability event {}",
                    fingerprint
                )
            })?;
        Ok(())
    }

    pub fn attach_observability_event_swarm_run(
        &self,
        fingerprint: &str,
        swarm_run_id: &str,
    ) -> Result<()> {
        self.conn
            .execute(
                r#"
                UPDATE observability_events
                SET
                  swarm_run_id = ?1,
                  status = CASE WHEN status = 'open' THEN 'investigating' ELSE status END
                WHERE fingerprint = ?2
                "#,
                params![swarm_run_id, fingerprint],
            )
            .with_context(|| {
                format!(
                    "failed to attach swarm run to observability event {}",
                    fingerprint
                )
            })?;
        Ok(())
    }

    pub fn upsert_observability_schema_adapter(
        &self,
        adapter: &ObservabilitySchemaAdapterDefinition,
    ) -> Result<ObservabilitySchemaAdapterDefinition> {
        let updated_at = adapter
            .updated_at
            .clone()
            .unwrap_or_else(|| chrono::Utc::now().to_rfc3339());
        self.conn
            .execute(
                r#"
                INSERT INTO observability_schema_adapters (
                  id,
                  source_key,
                  version,
                  enabled,
                  match_json,
                  field_map_json,
                  defaults_json,
                  metadata_paths_json,
                  validation_json,
                  updated_at
                )
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
                ON CONFLICT(source_key) DO UPDATE SET
                  id = excluded.id,
                  version = excluded.version,
                  enabled = excluded.enabled,
                  match_json = excluded.match_json,
                  field_map_json = excluded.field_map_json,
                  defaults_json = excluded.defaults_json,
                  metadata_paths_json = excluded.metadata_paths_json,
                  validation_json = excluded.validation_json,
                  updated_at = excluded.updated_at
                "#,
                params![
                    adapter.id,
                    adapter.source_key,
                    adapter.version,
                    if adapter.enabled == Some(false) { 0 } else { 1 },
                    adapter
                        .match_rules
                        .as_ref()
                        .map(|value| serde_json::to_string(value).unwrap()),
                    adapter
                        .field_map
                        .as_ref()
                        .map(|value| serde_json::to_string(value).unwrap()),
                    adapter.defaults.as_ref().map(Value::to_string),
                    adapter
                        .metadata_paths
                        .as_ref()
                        .map(|value| serde_json::to_string(value).unwrap()),
                    adapter
                        .validation
                        .as_ref()
                        .map(|value| serde_json::to_string(value).unwrap()),
                    updated_at,
                ],
            )
            .with_context(|| {
                format!(
                    "failed to upsert observability schema adapter {}",
                    adapter.source_key
                )
            })?;
        self.get_observability_schema_adapter(&adapter.source_key)?
            .with_context(|| {
                format!(
                    "observability schema adapter {} disappeared after upsert",
                    adapter.source_key
                )
            })
    }

    pub fn get_observability_schema_adapter(
        &self,
        source_key: &str,
    ) -> Result<Option<ObservabilitySchemaAdapterDefinition>> {
        let mut stmt = self
            .conn
            .prepare(
                r#"
                SELECT
                  id,
                  source_key,
                  version,
                  enabled,
                  match_json,
                  field_map_json,
                  defaults_json,
                  metadata_paths_json,
                  validation_json,
                  updated_at
                FROM observability_schema_adapters
                WHERE source_key = ?1
                "#,
            )
            .with_context(|| {
                format!(
                    "failed to prepare observability schema adapter lookup for {}",
                    source_key
                )
            })?;
        let row = stmt.query_row(params![source_key], map_observability_schema_adapter_row);
        match row {
            Ok(adapter) => Ok(Some(adapter)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(error) => Err(error).with_context(|| {
                format!("failed to read observability schema adapter {}", source_key)
            }),
        }
    }

    pub fn list_observability_schema_adapters(
        &self,
        enabled_only: bool,
    ) -> Result<Vec<ObservabilitySchemaAdapterDefinition>> {
        let sql = if enabled_only {
            r#"
            SELECT
              id,
              source_key,
              version,
              enabled,
              match_json,
              field_map_json,
              defaults_json,
              metadata_paths_json,
              validation_json,
              updated_at
            FROM observability_schema_adapters
            WHERE enabled = 1
            ORDER BY source_key ASC
            "#
        } else {
            r#"
            SELECT
              id,
              source_key,
              version,
              enabled,
              match_json,
              field_map_json,
              defaults_json,
              metadata_paths_json,
              validation_json,
              updated_at
            FROM observability_schema_adapters
            ORDER BY source_key ASC
            "#
        };
        let mut stmt = self
            .conn
            .prepare(sql)
            .context("failed to prepare observability schema adapter list query")?;
        let rows = stmt.query_map([], map_observability_schema_adapter_row)?;
        let mut adapters = Vec::new();
        for row in rows {
            adapters.push(row.context("failed to decode observability schema adapter row")?);
        }
        Ok(adapters)
    }

    pub fn get_linear_issue_thread(&self, issue_key: &str) -> Result<Option<LinearIssueThread>> {
        let mut stmt = self
            .conn
            .prepare(
                r#"
                SELECT chat_jid, thread_ts, issue_identifier, closed_at
                FROM linear_issue_threads
                WHERE issue_key = ?1
                "#,
            )
            .with_context(|| {
                format!(
                    "failed to prepare linear issue thread lookup for {}",
                    issue_key
                )
            })?;
        let row = stmt.query_row(params![issue_key], |row| {
            Ok(LinearIssueThread {
                chat_jid: row.get(0)?,
                thread_ts: row.get(1)?,
                issue_identifier: row.get(2)?,
                closed_at: row.get(3)?,
            })
        });
        match row {
            Ok(thread) => Ok(Some(thread)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(err) => Err(err)
                .with_context(|| format!("failed to read linear issue thread for {}", issue_key)),
        }
    }

    pub fn get_linear_issue_thread_by_identifier(
        &self,
        issue_identifier: &str,
    ) -> Result<Option<LinearIssueThread>> {
        let mut stmt = self
            .conn
            .prepare(
                r#"
                SELECT chat_jid, thread_ts, issue_identifier, closed_at
                FROM linear_issue_threads
                WHERE issue_identifier = ?1
                   OR issue_key = ?2
                ORDER BY updated_at DESC
                LIMIT 1
                "#,
            )
            .with_context(|| {
                format!(
                    "failed to prepare linear issue thread lookup for identifier {}",
                    issue_identifier
                )
            })?;
        let row = stmt.query_row(
            params![issue_identifier, format!("identifier:{}", issue_identifier)],
            |row| {
                Ok(LinearIssueThread {
                    chat_jid: row.get(0)?,
                    thread_ts: row.get(1)?,
                    issue_identifier: row.get(2)?,
                    closed_at: row.get(3)?,
                })
            },
        );
        match row {
            Ok(thread) => Ok(Some(thread)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(err) => Err(err).with_context(|| {
                format!(
                    "failed to read linear issue thread for identifier {}",
                    issue_identifier
                )
            }),
        }
    }

    pub fn set_linear_issue_thread(
        &self,
        issue_key: &str,
        chat_jid: &str,
        thread_ts: &str,
        issue_identifier: Option<&str>,
    ) -> Result<()> {
        let now = chrono::Utc::now().to_rfc3339();
        self.conn
            .execute(
                r#"
                INSERT INTO linear_issue_threads (
                  issue_key,
                  chat_jid,
                  thread_ts,
                  issue_identifier,
                  created_at,
                  updated_at
                )
                VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                ON CONFLICT(issue_key) DO UPDATE SET
                  chat_jid = excluded.chat_jid,
                  thread_ts = excluded.thread_ts,
                  issue_identifier = COALESCE(excluded.issue_identifier, linear_issue_threads.issue_identifier),
                  updated_at = excluded.updated_at
                "#,
                params![issue_key, chat_jid, thread_ts, issue_identifier, now, now],
            )
            .with_context(|| format!("failed to upsert linear issue thread for {}", issue_key))?;
        Ok(())
    }

    pub fn mark_linear_issue_thread_closed(&self, issue_key: &str, closed_at: &str) -> Result<()> {
        self.conn
            .execute(
                r#"
                UPDATE linear_issue_threads
                SET closed_at = ?1, updated_at = ?2
                WHERE issue_key = ?3
                "#,
                params![closed_at, chrono::Utc::now().to_rfc3339(), issue_key],
            )
            .with_context(|| {
                format!(
                    "failed to mark linear issue thread closed for {}",
                    issue_key
                )
            })?;
        Ok(())
    }

    pub fn reopen_linear_issue_thread(&self, issue_key: &str) -> Result<()> {
        self.conn
            .execute(
                r#"
                UPDATE linear_issue_threads
                SET closed_at = NULL, updated_at = ?1
                WHERE issue_key = ?2
                "#,
                params![chrono::Utc::now().to_rfc3339(), issue_key],
            )
            .with_context(|| format!("failed to reopen linear issue thread for {}", issue_key))?;
        Ok(())
    }

    pub fn counts(&self) -> Result<NanoclawDbCounts> {
        Ok(NanoclawDbCounts {
            chats: self.table_count("chats")?,
            messages: self.table_count("messages")?,
            scheduled_tasks: self.table_count("scheduled_tasks")?,
            registered_groups: self.table_count("registered_groups")?,
        })
    }

    fn table_count(&self, table: &str) -> Result<i64> {
        let sql = format!("SELECT COUNT(*) FROM {table}");
        let mut stmt = self
            .conn
            .prepare(&sql)
            .with_context(|| format!("failed to prepare count query for {table}"))?;
        stmt.query_row([], |row| row.get(0))
            .with_context(|| format!("failed to count rows in {table}"))
    }

    fn ensure_column(&self, table: &str, column: &str, definition: &str) -> Result<()> {
        let pragma = format!("PRAGMA table_info({table})");
        let mut stmt = self
            .conn
            .prepare(&pragma)
            .with_context(|| format!("failed to inspect schema for {table}"))?;
        let columns = stmt
            .query_map([], |row| row.get::<_, String>(1))
            .with_context(|| format!("failed to read schema for {table}"))?;
        for existing in columns {
            if existing? == column {
                return Ok(());
            }
        }

        let alter = format!("ALTER TABLE {table} ADD COLUMN {column} {definition}");
        self.conn
            .execute_batch(&alter)
            .with_context(|| format!("failed to add {column} to {table}"))?;
        Ok(())
    }

    fn query_tasks<P>(&self, sql: &str, params: P) -> Result<Vec<ScheduledTask>>
    where
        P: rusqlite::Params,
    {
        let mut stmt = self
            .conn
            .prepare(sql)
            .context("failed to prepare task query")?;
        let rows = stmt
            .query_map(params, map_task_row)
            .context("failed to execute task query")?;

        let mut tasks = Vec::new();
        for row in rows {
            tasks.push(row.context("failed to decode task row")?);
        }
        Ok(tasks)
    }
}

impl FoundationStore for NanoclawDb {
    fn upsert_group(&self, group: &Group) -> Result<()> {
        self.upsert_registered_group(group)
    }

    fn list_groups(&self) -> Result<Vec<Group>> {
        self.list_registered_groups()
    }

    fn set_router_state(&self, key: &str, value: &str) -> Result<()> {
        self.upsert_router_state(key, value)
    }

    fn get_router_state(&self, key: &str) -> Result<Option<String>> {
        self.router_state(key)
    }

    fn store_chat_metadata(
        &self,
        chat_jid: &str,
        timestamp: &str,
        name: Option<&str>,
        channel: Option<&str>,
        is_group: Option<bool>,
    ) -> Result<()> {
        NanoclawDb::store_chat_metadata(self, chat_jid, timestamp, name, channel, is_group)
    }

    fn store_message(&self, message: &MessageRecord) -> Result<()> {
        NanoclawDb::store_message(self, message)
    }

    fn messages_since(
        &self,
        chat_jid: &str,
        since_timestamp: &str,
        limit: usize,
        include_bot_messages: bool,
    ) -> Result<Vec<MessageRecord>> {
        NanoclawDb::messages_since(self, chat_jid, since_timestamp, limit, include_bot_messages)
    }

    fn create_task(&self, task: &ScheduledTask) -> Result<()> {
        NanoclawDb::create_task(self, task)
    }

    fn get_task_by_id(&self, task_id: &str) -> Result<Option<ScheduledTask>> {
        NanoclawDb::get_task_by_id(self, task_id)
    }

    fn list_tasks(&self) -> Result<Vec<ScheduledTask>> {
        NanoclawDb::list_tasks(self)
    }

    fn list_tasks_for_group(&self, group_folder: &str) -> Result<Vec<ScheduledTask>> {
        NanoclawDb::list_tasks_for_group(self, group_folder)
    }

    fn list_due_tasks(&self, now_iso: &str) -> Result<Vec<ScheduledTask>> {
        NanoclawDb::list_due_tasks(self, now_iso)
    }

    fn update_task_after_run(
        &self,
        task_id: &str,
        next_run: Option<&str>,
        last_result: &str,
    ) -> Result<()> {
        NanoclawDb::update_task_after_run(self, task_id, next_run, last_result)
    }

    fn set_task_status(&self, task_id: &str, status: TaskStatus) -> Result<()> {
        NanoclawDb::set_task_status(self, task_id, status)
    }

    fn delete_task(&self, task_id: &str) -> Result<()> {
        NanoclawDb::delete_task(self, task_id)
    }

    fn log_task_run(&self, log: &TaskRunLog) -> Result<()> {
        NanoclawDb::log_task_run(self, log)
    }
}

fn map_task_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ScheduledTask> {
    let request_plane = row
        .get::<_, Option<String>>(5)?
        .map(|value| RequestPlane::parse(&value));

    Ok(ScheduledTask {
        id: row.get(0)?,
        group_folder: row.get(1)?,
        chat_jid: row.get(2)?,
        prompt: row.get(3)?,
        script: row.get(4)?,
        request_plane,
        schedule_type: TaskScheduleType::parse(&row.get::<_, String>(6)?),
        schedule_value: row.get(7)?,
        context_mode: TaskContextMode::parse(&row.get::<_, String>(8)?),
        next_run: row.get(9)?,
        last_run: row.get(10)?,
        last_result: row.get(11)?,
        status: TaskStatus::parse(&row.get::<_, String>(12)?),
        created_at: row.get(13)?,
    })
}

fn map_omx_session_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<OmxSessionRecord> {
    Ok(OmxSessionRecord {
        session_id: row.get(0)?,
        group_folder: row.get(1)?,
        chat_jid: row.get(2)?,
        task_id: row.get(3)?,
        external_run_id: row.get(4)?,
        mode: OmxMode::parse(&row.get::<_, String>(5)?),
        status: OmxSessionStatus::parse(&row.get::<_, String>(6)?),
        tmux_session: row.get(7)?,
        team_name: row.get(8)?,
        workspace_root: row.get(9)?,
        last_summary: row.get(10)?,
        pending_question: row.get(11)?,
        last_event_kind: row
            .get::<_, Option<String>>(12)?
            .map(|value| OmxEventKind::parse(&value)),
        last_activity_at: row.get(13)?,
        created_at: row.get(14)?,
        updated_at: row.get(15)?,
        completed_at: row.get(16)?,
    })
}

fn map_omx_event_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<OmxEventRecord> {
    Ok(OmxEventRecord {
        id: row.get(0)?,
        session_id: row.get(1)?,
        event_kind: OmxEventKind::parse(&row.get::<_, String>(2)?),
        group_folder: row.get(3)?,
        chat_jid: row.get(4)?,
        summary: row.get(5)?,
        question: row.get(6)?,
        payload_json: parse_json_value(row.get(7)?),
        created_at: row.get(8)?,
    })
}

fn map_pm_audit_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<StoredPmAuditEvent> {
    Ok(StoredPmAuditEvent {
        id: row.get(0)?,
        issue_key: row.get(1)?,
        issue_identifier: row.get(2)?,
        thread_jid: row.get(3)?,
        phase: row.get(4)?,
        status: row.get(5)?,
        tool: row.get(6)?,
        error_code: row.get(7)?,
        blocking: row.get::<_, i64>(8)? != 0,
        metadata: parse_json_value(row.get(9)?),
        created_at: row.get(10)?,
    })
}

fn map_observability_event_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ObservabilityEvent> {
    Ok(ObservabilityEvent {
        id: row.get(0)?,
        fingerprint: row.get(1)?,
        source: row.get(2)?,
        environment: row.get(3)?,
        service: row.get(4)?,
        category: row.get(5)?,
        severity: ObservabilitySeverity::parse(&row.get::<_, String>(6)?),
        title: row.get(7)?,
        message: row.get(8)?,
        status: ObservabilityEventStatus::parse(&row.get::<_, String>(9)?),
        chat_jid: row.get(10)?,
        thread_ts: row.get(11)?,
        swarm_run_id: row.get(12)?,
        deployment_url: row.get(13)?,
        healthcheck_url: row.get(14)?,
        repo: row.get(15)?,
        repo_path: row.get(16)?,
        metadata: parse_json_value(row.get(17)?),
        raw_payload: parse_json_value(row.get(18)?),
        first_seen_at: row.get(19)?,
        last_seen_at: row.get(20)?,
        occurrence_count: row.get(21)?,
    })
}

fn map_observability_schema_adapter_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<ObservabilitySchemaAdapterDefinition> {
    Ok(ObservabilitySchemaAdapterDefinition {
        id: row.get(0)?,
        source_key: row.get(1)?,
        version: row.get(2)?,
        enabled: Some(row.get::<_, i64>(3)? != 0),
        match_rules: parse_json_optional(row.get(4)?)?,
        field_map: parse_json_optional(row.get(5)?)?,
        defaults: parse_json_value(row.get(6)?),
        metadata_paths: parse_json_optional(row.get(7)?)?,
        validation: parse_json_optional(row.get(8)?)?,
        updated_at: Some(row.get(9)?),
    })
}

fn map_pm_issue_memory_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<PmIssueMemory> {
    Ok(PmIssueMemory {
        issue_key: row.get(0)?,
        issue_identifier: row.get(1)?,
        summary: row.get(2)?,
        next_action: row.get(3)?,
        blockers: parse_json_vec(row.get(4)?),
        current_state: row.get(5)?,
        repo_hint: row.get(6)?,
        last_source: row.get(7)?,
        memory: parse_json_value(row.get(8)?),
        updated_at: row.get(9)?,
    })
}

fn map_execution_provenance_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<ExecutionProvenanceRecord> {
    Ok(ExecutionProvenanceRecord {
        id: row.get(0)?,
        run_kind: ExecutionRunKind::parse(&row.get::<_, String>(1)?),
        group_folder: row.get(2)?,
        chat_jid: row.get(3)?,
        execution_location: ExecutionLocation::parse(&row.get::<_, String>(4)?),
        trust_level: ExecutionTrustLevel::parse(&row.get::<_, String>(5)?),
        request_plane: RequestPlane::parse(&row.get::<_, String>(6)?),
        effective_capabilities: parse_json_required(row.get(7)?)?,
        project_environment_id: row.get(8)?,
        mount_summary: parse_json_required(row.get(9)?)?,
        secret_handles_used: parse_json_required(row.get(10)?)?,
        fallback_reason: row.get(11)?,
        sync_scope: parse_json_optional(row.get(12)?)?,
        task_signature: parse_json_optional(row.get(13)?)?,
        boundary_claims: parse_json_optional(row.get(14)?)?.unwrap_or_default(),
        gate_decision: row
            .get::<_, Option<String>>(15)?
            .map(|value| GateDecision::parse(&value)),
        gate_evaluation: parse_json_optional(row.get(16)?)?,
        assurance: parse_json_optional(row.get(17)?)?,
        symbol_carriers: parse_json_optional(row.get(18)?)?.unwrap_or_default(),
        provenance_edges: parse_json_optional(row.get(19)?)?.unwrap_or_default(),
        status: ExecutionStatus::parse(&row.get::<_, String>(20)?),
        created_at: row.get(21)?,
        updated_at: row.get(22)?,
        completed_at: row.get(23)?,
    })
}

fn map_host_os_control_approval_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<HostOsControlApprovalRequestRecord> {
    Ok(HostOsControlApprovalRequestRecord {
        id: row.get(0)?,
        source_group: row.get(1)?,
        chat_jid: row.get(2)?,
        action_kind: row.get(3)?,
        action_scope: row.get(4)?,
        action_fingerprint: row.get(5)?,
        action_summary: row.get(6)?,
        action_payload: parse_json_value(row.get(7)?),
        allowed_decisions: parse_json_required(row.get(8)?)?,
        boundary_claim: parse_json_optional(row.get(9)?)?,
        gate_evaluation: parse_json_optional(row.get(10)?)?,
        status: HostOsControlApprovalStatus::parse(&row.get::<_, String>(11)?),
        resolved_decision: row
            .get::<_, Option<String>>(12)?
            .map(|value| HostOsControlApprovalDecision::parse(&value)),
        created_at: row.get(13)?,
        resolved_at: row.get(14)?,
    })
}

fn map_swarm_run_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<SwarmRun> {
    Ok(SwarmRun {
        id: row.get(0)?,
        group_folder: row.get(1)?,
        chat_jid: row.get(2)?,
        created_by: row.get(3)?,
        objective: row.get(4)?,
        requested_lane: SwarmRequestedLane::parse(&row.get::<_, String>(5)?),
        objective_signature: parse_json_optional(row.get(6)?)?,
        objective_gate: parse_json_optional(row.get(7)?)?,
        status: SwarmRunStatus::parse(&row.get::<_, String>(8)?),
        max_concurrency: row.get(9)?,
        summary: row.get(10)?,
        result: parse_json_value(row.get(11)?),
        created_at: row.get(12)?,
        updated_at: row.get(13)?,
        completed_at: row.get(14)?,
    })
}

fn map_swarm_task_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<SwarmTask> {
    Ok(SwarmTask {
        id: row.get(0)?,
        run_id: row.get(1)?,
        task_key: row.get(2)?,
        title: row.get(3)?,
        prompt: row.get(4)?,
        command: row.get(5)?,
        requested_lane: SwarmRequestedLane::parse(&row.get::<_, String>(6)?),
        resolved_lane: row
            .get::<_, Option<String>>(7)?
            .map(|value| SwarmResolvedLane::parse(&value)),
        task_signature: parse_json_optional(row.get(8)?)?,
        boundary_quadrant: row
            .get::<_, Option<String>>(9)?
            .map(|value| crate::foundation::BoundaryQuadrant::parse(&value)),
        gate_decision: row
            .get::<_, Option<String>>(10)?
            .map(|value| GateDecision::parse(&value)),
        gate_evaluation: parse_json_optional(row.get(11)?)?,
        required_roles: parse_json_optional(row.get(12)?)?.unwrap_or_default(),
        status: SwarmTaskStatus::parse(&row.get::<_, String>(13)?),
        priority: row.get(14)?,
        target_group_folder: row.get(15)?,
        target_chat_jid: row.get(16)?,
        cwd: row.get(17)?,
        repo: row.get(18)?,
        repo_path: row.get(19)?,
        sync: row.get::<_, i64>(20)? != 0,
        host: row.get(21)?,
        user: row.get(22)?,
        port: row.get::<_, Option<i64>>(23)?.map(|value| value as u16),
        timeout_ms: row.get::<_, Option<i64>>(24)?.map(|value| value as u64),
        metadata: parse_json_value(row.get(25)?),
        result: parse_json_value(row.get(26)?),
        error: row.get(27)?,
        lease_owner: row.get(28)?,
        lease_expires_at: row.get(29)?,
        attempts: row.get(30)?,
        max_attempts: row.get(31)?,
        created_at: row.get(32)?,
        updated_at: row.get(33)?,
        started_at: row.get(34)?,
        completed_at: row.get(35)?,
    })
}

fn parse_json_value(raw: Option<String>) -> Option<Value> {
    raw.and_then(|value| serde_json::from_str(&value).ok())
}

fn parse_json_vec(raw: Option<String>) -> Option<Vec<String>> {
    raw.and_then(|value| serde_json::from_str(&value).ok())
}

fn parse_json_optional<T>(raw: Option<String>) -> rusqlite::Result<Option<T>>
where
    T: serde::de::DeserializeOwned,
{
    raw.map(|value| {
        serde_json::from_str(&value).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                value.len(),
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })
    })
    .transpose()
}

fn parse_json_required<T>(raw: String) -> rusqlite::Result<T>
where
    T: serde::de::DeserializeOwned,
{
    serde_json::from_str(&raw).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            raw.len(),
            rusqlite::types::Type::Text,
            Box::new(error),
        )
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use anyhow::Result;
    use serde_json::json;
    use tempfile::tempdir;

    use super::NanoclawDb;
    use crate::foundation::{
        CapabilityManifest, ExecutionLocation, ExecutionMountKind, ExecutionMountSummaryEntry,
        ExecutionProvenanceRecord, ExecutionRunKind, ExecutionStatus, ExecutionTrustLevel, Group,
        HostOsControlApprovalDecision, HostOsControlApprovalRequestRecord,
        HostOsControlApprovalStatus, MessageRecord, RequestPlane, ScheduledTask,
        SwarmRequestedLane, SwarmResolvedLane, SwarmRun, SwarmRunStatus, SwarmTask,
        SwarmTaskDependency, SwarmTaskStatus, TaskContextMode, TaskScheduleType, TaskStatus,
    };
    use crate::nanoclaw::observability::{
        ObservabilityEventStatus, ObservabilitySchemaAdapterDefinition, ObservabilitySeverity,
        UpsertObservabilityEventInput,
    };
    use crate::nanoclaw::pm::{PmAuditEvent, PmIssueMemory, PmIssueTrackedComment};

    #[test]
    fn initializes_schema_and_state() -> Result<()> {
        let dir = tempdir()?;
        let db = NanoclawDb::open(dir.path().join("messages.db"))?;

        db.upsert_router_state("last_timestamp", "123")?;
        let group = Group::main("Andy", "2026-04-04T00:00:00Z");
        db.upsert_registered_group(&group)?;

        assert_eq!(db.router_state("last_timestamp")?, Some("123".to_string()));
        assert_eq!(db.list_registered_groups()?, vec![group]);
        assert_eq!(db.counts()?.registered_groups, 1);

        Ok(())
    }

    #[test]
    fn stores_and_reads_scheduled_tasks() -> Result<()> {
        let dir = tempdir()?;
        let db = NanoclawDb::open(dir.path().join("messages.db"))?;
        let task = ScheduledTask {
            id: "task-1".to_string(),
            group_folder: "main".to_string(),
            chat_jid: "main".to_string(),
            prompt: "ping".to_string(),
            script: None,
            request_plane: None,
            schedule_type: TaskScheduleType::Once,
            schedule_value: "2026-04-05T12:00:00Z".to_string(),
            context_mode: TaskContextMode::Isolated,
            next_run: Some("2026-04-05T12:00:00Z".to_string()),
            last_run: None,
            last_result: None,
            status: TaskStatus::Active,
            created_at: "2026-04-05T11:00:00Z".to_string(),
        };

        db.create_task(&task)?;

        assert_eq!(db.get_task_by_id("task-1")?, Some(task.clone()));
        assert_eq!(db.list_due_tasks("2026-04-05T12:00:00Z")?, vec![task]);
        Ok(())
    }

    #[test]
    fn stores_and_reads_messages_since_cursor() -> Result<()> {
        let dir = tempdir()?;
        let db = NanoclawDb::open(dir.path().join("messages.db"))?;
        db.store_chat_metadata(
            "main",
            "2026-04-05T12:00:00Z",
            Some("Main"),
            Some("local"),
            Some(false),
        )?;
        db.store_message(&MessageRecord {
            id: "m1".to_string(),
            chat_jid: "main".to_string(),
            sender: "user".to_string(),
            sender_name: Some("User".to_string()),
            content: "hello".to_string(),
            timestamp: "2026-04-05T12:00:00Z".to_string(),
            is_from_me: false,
            is_bot_message: false,
        })?;
        db.store_message(&MessageRecord {
            id: "m2".to_string(),
            chat_jid: "main".to_string(),
            sender: "andy".to_string(),
            sender_name: Some("Andy".to_string()),
            content: "reply".to_string(),
            timestamp: "2026-04-05T12:01:00Z".to_string(),
            is_from_me: true,
            is_bot_message: true,
        })?;

        let messages = db.messages_since("main", "2026-04-05T11:59:00Z", 20, false)?;
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].id, "m1");
        Ok(())
    }

    #[test]
    fn stores_and_reads_sessions() -> Result<()> {
        let dir = tempdir()?;
        let db = NanoclawDb::open(dir.path().join("messages.db"))?;
        db.upsert_session("main", "session-1")?;

        assert_eq!(db.session_for_group("main")?, Some("session-1".to_string()));

        db.upsert_session("main", "session-2")?;
        assert_eq!(db.session_for_group("main")?, Some("session-2".to_string()));
        Ok(())
    }

    #[test]
    fn stores_pm_memory_comments_and_threads() -> Result<()> {
        let dir = tempdir()?;
        let db = NanoclawDb::open(dir.path().join("messages.db"))?;
        db.set_pm_issue_memory(&PmIssueMemory {
            issue_key: "linear:issue-1".to_string(),
            issue_identifier: "BYB-23".to_string(),
            summary: Some("summary".to_string()),
            next_action: Some("next".to_string()),
            blockers: Some(vec!["blocked".to_string()]),
            current_state: Some("In Review".to_string()),
            repo_hint: Some("nanoclaw".to_string()),
            last_source: Some("test".to_string()),
            memory: Some(json!({ "score": 85 })),
            updated_at: "2026-04-06T07:00:00Z".to_string(),
        })?;
        let memory = db.get_pm_issue_memory("linear:issue-1")?.unwrap();
        assert_eq!(memory.issue_identifier, "BYB-23");
        assert_eq!(memory.blockers, Some(vec!["blocked".to_string()]));
        assert_eq!(memory.memory, Some(json!({ "score": 85 })));

        db.set_pm_issue_tracked_comment(&PmIssueTrackedComment {
            issue_key: "linear:issue-1".to_string(),
            issue_identifier: Some("BYB-23".to_string()),
            comment_kind: "pm-quality".to_string(),
            comment_id: "comment-1".to_string(),
            body_hash: Some("hash".to_string()),
            body: Some("body".to_string()),
            updated_at: "2026-04-06T07:00:00Z".to_string(),
        })?;
        let comment = db
            .get_pm_issue_tracked_comment("linear:issue-1", "pm-quality")?
            .unwrap();
        assert_eq!(comment.comment_id, "comment-1");

        db.set_linear_issue_thread(
            "linear:issue-1",
            "slack:C123::thread:1774956749.012119",
            "1774956749.012119",
            Some("BYB-23"),
        )?;
        let thread = db.get_linear_issue_thread_by_identifier("BYB-23")?.unwrap();
        assert_eq!(thread.chat_jid, "slack:C123::thread:1774956749.012119");

        db.mark_linear_issue_thread_closed("linear:issue-1", "2026-04-06T07:05:00Z")?;
        assert!(db
            .get_linear_issue_thread("linear:issue-1")?
            .unwrap()
            .closed_at
            .is_some());
        db.reopen_linear_issue_thread("linear:issue-1")?;
        assert!(db
            .get_linear_issue_thread("linear:issue-1")?
            .unwrap()
            .closed_at
            .is_none());
        Ok(())
    }

    #[test]
    fn stores_and_updates_observability_events() -> Result<()> {
        let dir = tempdir()?;
        let db = NanoclawDb::open(dir.path().join("messages.db"))?;
        let input = UpsertObservabilityEventInput {
            id: "obs-1".to_string(),
            fingerprint: "fingerprint-1".to_string(),
            source: "payments-api".to_string(),
            environment: Some("prod".to_string()),
            service: Some("checkout".to_string()),
            category: Some("http".to_string()),
            severity: ObservabilitySeverity::Error,
            title: "Checkout failures".to_string(),
            message: Some("timeouts".to_string()),
            status: Some(ObservabilityEventStatus::Open),
            deployment_url: Some("https://example.com".to_string()),
            healthcheck_url: None,
            repo: Some("nexus-integrated-technologies/agency".to_string()),
            repo_path: Some("services/checkout".to_string()),
            metadata: Some(json!({ "source": "test" })),
            raw_payload: Some(json!({ "source": "payments-api" })),
            seen_at: Some("2026-04-06T12:00:00Z".to_string()),
        };
        let (created, was_created) = db.upsert_observability_event(&input)?;
        assert!(was_created);
        assert_eq!(created.occurrence_count, 1);

        let (updated, was_created) =
            db.upsert_observability_event(&UpsertObservabilityEventInput {
                message: Some("still timing out".to_string()),
                seen_at: Some("2026-04-06T12:01:00Z".to_string()),
                ..input
            })?;
        assert!(!was_created);
        assert_eq!(updated.occurrence_count, 2);

        db.attach_observability_event_thread(
            "fingerprint-1",
            "slack:C123::thread:1.23",
            Some("1.23"),
        )?;
        db.attach_observability_event_swarm_run("fingerprint-1", "swarm-run-1")?;
        let stored = db
            .get_observability_event_by_fingerprint("fingerprint-1")?
            .unwrap();
        assert_eq!(stored.chat_jid.as_deref(), Some("slack:C123::thread:1.23"));
        assert_eq!(stored.swarm_run_id.as_deref(), Some("swarm-run-1"));
        assert_eq!(stored.status, ObservabilityEventStatus::Investigating);
        assert_eq!(db.list_observability_events(10, None, None)?.len(), 1);
        Ok(())
    }

    #[test]
    fn stores_observability_schema_adapters() -> Result<()> {
        let dir = tempdir()?;
        let db = NanoclawDb::open(dir.path().join("messages.db"))?;
        let adapter = ObservabilitySchemaAdapterDefinition {
            id: "vercel".to_string(),
            source_key: "vercel".to_string(),
            version: Some("1".to_string()),
            enabled: Some(true),
            match_rules: Some(
                crate::nanoclaw::observability::ObservabilitySchemaAdapterMatch {
                    source: Some(vec!["vercel".to_string()]),
                    top_level_keys_any: None,
                    top_level_keys_all: Some(vec!["severity".to_string(), "title".to_string()]),
                },
            ),
            field_map: Some(BTreeMap::from([
                (
                    crate::nanoclaw::observability::ObservabilityAdapterField::Severity,
                    crate::nanoclaw::observability::ObservabilityFieldSelector::One(
                        "severity".to_string(),
                    ),
                ),
                (
                    crate::nanoclaw::observability::ObservabilityAdapterField::Title,
                    crate::nanoclaw::observability::ObservabilityFieldSelector::One(
                        "title".to_string(),
                    ),
                ),
            ])),
            defaults: Some(json!({ "environment": "prod" })),
            metadata_paths: Some(vec!["context".to_string()]),
            validation: Some(
                crate::nanoclaw::observability::ObservabilitySchemaAdapterValidation {
                    required_fields: Some(vec![
                        crate::nanoclaw::observability::ObservabilityAdapterField::Severity,
                        crate::nanoclaw::observability::ObservabilityAdapterField::Title,
                    ]),
                    required_paths: Some(vec!["severity".to_string(), "title".to_string()]),
                },
            ),
            updated_at: Some("2026-04-06T12:00:00Z".to_string()),
        };
        let stored = db.upsert_observability_schema_adapter(&adapter)?;
        assert_eq!(stored.source_key, "vercel");
        assert_eq!(db.list_observability_schema_adapters(true)?.len(), 1);
        Ok(())
    }

    #[test]
    fn stores_pm_audit_events() -> Result<()> {
        let dir = tempdir()?;
        let db = NanoclawDb::open(dir.path().join("messages.db"))?;
        db.record_pm_audit_event(&PmAuditEvent {
            issue_key: Some("linear:issue-1".to_string()),
            issue_identifier: Some("BYB-23".to_string()),
            thread_jid: Some("slack:C123::thread:1774956749.012119".to_string()),
            phase: "github_sync_started".to_string(),
            status: "started".to_string(),
            tool: Some("github-webhook".to_string()),
            error_code: None,
            blocking: false,
            metadata: Some(json!({ "eventType": "pull_request" })),
            created_at: Some("2026-04-06T07:00:00Z".to_string()),
        })?;
        let events = db.get_pm_audit_events(Some("BYB-23"))?;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].phase, "github_sync_started");
        assert_eq!(
            events[0].metadata,
            Some(json!({ "eventType": "pull_request" }))
        );
        Ok(())
    }

    #[test]
    fn stores_execution_provenance_records() -> Result<()> {
        let dir = tempdir()?;
        let db = NanoclawDb::open(dir.path().join("messages.db"))?;
        db.create_execution_provenance(&ExecutionProvenanceRecord {
            id: "exec-1".to_string(),
            run_kind: ExecutionRunKind::Codex,
            group_folder: "main".to_string(),
            chat_jid: Some("main".to_string()),
            execution_location: ExecutionLocation::Host,
            trust_level: ExecutionTrustLevel::HostTrusted,
            request_plane: RequestPlane::Web,
            effective_capabilities: CapabilityManifest {
                web_request: true,
                ..Default::default()
            },
            project_environment_id: Some("proj-1".to_string()),
            mount_summary: vec![ExecutionMountSummaryEntry {
                host_path: Some("/tmp/main".to_string()),
                container_path: None,
                readonly: false,
                kind: ExecutionMountKind::Project,
            }],
            secret_handles_used: vec!["env-key:OPENAI_API_KEY".to_string()],
            fallback_reason: None,
            sync_scope: None,
            task_signature: None,
            boundary_claims: Vec::new(),
            gate_decision: None,
            gate_evaluation: None,
            assurance: None,
            symbol_carriers: Vec::new(),
            provenance_edges: Vec::new(),
            status: ExecutionStatus::Success,
            created_at: "2026-04-06T08:00:00Z".to_string(),
            updated_at: "2026-04-06T08:00:00Z".to_string(),
            completed_at: Some("2026-04-06T08:00:01Z".to_string()),
        })?;

        let stored = db.get_execution_provenance("exec-1")?.unwrap();
        assert_eq!(stored.run_kind, ExecutionRunKind::Codex);
        assert_eq!(stored.execution_location, ExecutionLocation::Host);
        assert_eq!(stored.mount_summary.len(), 1);
        assert_eq!(db.list_execution_provenance(Some("main"), 10)?.len(), 1);
        Ok(())
    }

    #[test]
    fn stores_host_os_control_approval_requests() -> Result<()> {
        let dir = tempdir()?;
        let db = NanoclawDb::open(dir.path().join("messages.db"))?;
        db.create_host_os_control_approval_request(&HostOsControlApprovalRequestRecord {
            id: "approval-1".to_string(),
            source_group: "main".to_string(),
            chat_jid: Some("main".to_string()),
            action_kind: "open_application".to_string(),
            action_scope: "Slack".to_string(),
            action_fingerprint: "fp-1".to_string(),
            action_summary: "Open Slack".to_string(),
            action_payload: Some(json!({ "kind": "open_application", "application": "Slack" })),
            allowed_decisions: vec![
                HostOsControlApprovalDecision::Once,
                HostOsControlApprovalDecision::Deny,
            ],
            boundary_claim: None,
            gate_evaluation: None,
            status: HostOsControlApprovalStatus::Pending,
            resolved_decision: None,
            created_at: "2026-04-06T08:00:00Z".to_string(),
            resolved_at: None,
        })?;

        let stored = db
            .find_pending_host_os_control_approval_request("main", "fp-1")?
            .unwrap();
        assert_eq!(stored.status, HostOsControlApprovalStatus::Pending);
        db.resolve_host_os_control_approval_request(
            "approval-1",
            HostOsControlApprovalStatus::Approved,
            Some(HostOsControlApprovalDecision::Once),
            Some("2026-04-06T08:01:00Z"),
        )?;
        let resolved = db
            .get_host_os_control_approval_request("approval-1")?
            .unwrap();
        assert_eq!(resolved.status, HostOsControlApprovalStatus::Approved);
        assert_eq!(
            db.list_host_os_control_approval_requests(
                Some(HostOsControlApprovalStatus::Approved),
                Some("main"),
                10,
            )?
            .len(),
            1
        );
        Ok(())
    }

    #[test]
    fn stores_and_updates_swarm_runs() -> Result<()> {
        let dir = tempdir()?;
        let db = NanoclawDb::open(dir.path().join("messages.db"))?;
        let run = SwarmRun {
            id: "swarm-run-1".to_string(),
            group_folder: "main".to_string(),
            chat_jid: "main".to_string(),
            created_by: "tester".to_string(),
            objective: "Ship the change".to_string(),
            requested_lane: SwarmRequestedLane::Auto,
            objective_signature: None,
            objective_gate: None,
            status: SwarmRunStatus::Pending,
            max_concurrency: 2,
            summary: None,
            result: None,
            created_at: "2026-04-06T09:00:00Z".to_string(),
            updated_at: "2026-04-06T09:00:00Z".to_string(),
            completed_at: None,
        };
        let seed_task = SwarmTask {
            id: "swarm-run-1:seed".to_string(),
            run_id: run.id.clone(),
            task_key: "seed".to_string(),
            title: "Seed".to_string(),
            prompt: Some("Seed objective".to_string()),
            command: None,
            requested_lane: SwarmRequestedLane::Agent,
            resolved_lane: None,
            task_signature: None,
            boundary_quadrant: None,
            gate_decision: None,
            gate_evaluation: None,
            required_roles: Vec::new(),
            status: SwarmTaskStatus::Completed,
            priority: 100,
            target_group_folder: "main".to_string(),
            target_chat_jid: "main".to_string(),
            cwd: None,
            repo: None,
            repo_path: None,
            sync: false,
            host: None,
            user: None,
            port: None,
            timeout_ms: None,
            metadata: Some(json!({ "requestPlane": "none" })),
            result: Some(json!({ "summary": "seeded" })),
            error: None,
            lease_owner: None,
            lease_expires_at: None,
            attempts: 1,
            max_attempts: 3,
            created_at: "2026-04-06T09:00:00Z".to_string(),
            updated_at: "2026-04-06T09:00:00Z".to_string(),
            started_at: Some("2026-04-06T09:00:00Z".to_string()),
            completed_at: Some("2026-04-06T09:00:00Z".to_string()),
        };
        let task = SwarmTask {
            id: "swarm-run-1:analysis".to_string(),
            run_id: run.id.clone(),
            task_key: "analysis".to_string(),
            title: "Analyze".to_string(),
            prompt: Some("Analyze objective".to_string()),
            command: None,
            requested_lane: SwarmRequestedLane::Agent,
            resolved_lane: None,
            task_signature: None,
            boundary_quadrant: None,
            gate_decision: None,
            gate_evaluation: None,
            required_roles: Vec::new(),
            status: SwarmTaskStatus::Pending,
            priority: 90,
            target_group_folder: "main".to_string(),
            target_chat_jid: "main".to_string(),
            cwd: None,
            repo: None,
            repo_path: None,
            sync: false,
            host: None,
            user: None,
            port: None,
            timeout_ms: None,
            metadata: Some(json!({ "requestPlane": "none" })),
            result: None,
            error: None,
            lease_owner: None,
            lease_expires_at: None,
            attempts: 0,
            max_attempts: 3,
            created_at: "2026-04-06T09:00:00Z".to_string(),
            updated_at: "2026-04-06T09:00:00Z".to_string(),
            started_at: None,
            completed_at: None,
        };
        db.create_swarm_run(
            &run,
            &[seed_task, task.clone()],
            &[SwarmTaskDependency {
                task_id: task.id.clone(),
                depends_on_task_id: "swarm-run-1:seed".to_string(),
            }],
        )?;

        let stored = db.get_swarm_run(&run.id)?.unwrap();
        assert_eq!(stored.status, SwarmRunStatus::Pending);
        assert_eq!(db.list_active_swarm_runs()?.len(), 1);
        assert_eq!(db.list_swarm_tasks(&run.id)?.len(), 2);
        assert_eq!(db.list_swarm_task_dependencies(&run.id)?.len(), 1);

        let mut updated = stored.clone();
        updated.status = SwarmRunStatus::Active;
        updated.summary = Some("running".to_string());
        updated.updated_at = "2026-04-06T09:01:00Z".to_string();
        db.update_swarm_run(&updated)?;
        assert_eq!(
            db.get_swarm_run(&run.id)?.unwrap().status,
            SwarmRunStatus::Active
        );
        Ok(())
    }

    #[test]
    fn claims_and_cancels_swarm_tasks() -> Result<()> {
        let dir = tempdir()?;
        let db = NanoclawDb::open(dir.path().join("messages.db"))?;
        let run = SwarmRun {
            id: "swarm-run-2".to_string(),
            group_folder: "main".to_string(),
            chat_jid: "main".to_string(),
            created_by: "tester".to_string(),
            objective: "Execute commands".to_string(),
            requested_lane: SwarmRequestedLane::Host,
            objective_signature: None,
            objective_gate: None,
            status: SwarmRunStatus::Pending,
            max_concurrency: 1,
            summary: None,
            result: None,
            created_at: "2026-04-06T09:00:00Z".to_string(),
            updated_at: "2026-04-06T09:00:00Z".to_string(),
            completed_at: None,
        };
        let task = SwarmTask {
            id: "swarm-run-2:host".to_string(),
            run_id: run.id.clone(),
            task_key: "host".to_string(),
            title: "Run host command".to_string(),
            prompt: None,
            command: Some("printf ok".to_string()),
            requested_lane: SwarmRequestedLane::Host,
            resolved_lane: None,
            task_signature: None,
            boundary_quadrant: None,
            gate_decision: None,
            gate_evaluation: None,
            required_roles: Vec::new(),
            status: SwarmTaskStatus::Ready,
            priority: 50,
            target_group_folder: "main".to_string(),
            target_chat_jid: "main".to_string(),
            cwd: None,
            repo: None,
            repo_path: None,
            sync: false,
            host: None,
            user: None,
            port: None,
            timeout_ms: None,
            metadata: None,
            result: None,
            error: None,
            lease_owner: None,
            lease_expires_at: None,
            attempts: 0,
            max_attempts: 2,
            created_at: "2026-04-06T09:00:00Z".to_string(),
            updated_at: "2026-04-06T09:00:00Z".to_string(),
            started_at: None,
            completed_at: None,
        };
        db.create_swarm_run(&run, &[task], &[])?;
        assert!(db.claim_swarm_task(
            "swarm-run-2:host",
            "lease-owner",
            "2026-04-06T09:05:00Z",
            &SwarmResolvedLane::Host,
            "2026-04-06T09:01:00Z",
        )?);
        let claimed = db.get_swarm_task("swarm-run-2:host")?.unwrap();
        assert_eq!(claimed.status, SwarmTaskStatus::Running);
        assert_eq!(claimed.attempts, 1);

        db.cancel_swarm_run("swarm-run-2", "2026-04-06T09:10:00Z")?;
        let canceled = db.get_swarm_task("swarm-run-2:host")?.unwrap();
        assert_eq!(canceled.status, SwarmTaskStatus::Canceled);
        assert_eq!(
            db.get_swarm_run("swarm-run-2")?.unwrap().status,
            SwarmRunStatus::Canceled
        );
        Ok(())
    }
}
