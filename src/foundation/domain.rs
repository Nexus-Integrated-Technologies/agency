use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{
    AssuranceTuple, BoundaryClaim, BoundaryQuadrant, GateDecision, GateEvaluation, ProvenanceEdge,
    SymbolCarrier, TaskSignature,
};

pub type GroupId = String;
pub type ChatId = String;
pub type TaskId = String;
pub type ArtifactId = String;
pub type RouterStateKey = String;
pub type DevelopmentEnvironmentId = String;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Group {
    pub jid: GroupId,
    pub name: String,
    pub folder: String,
    pub trigger: String,
    pub added_at: String,
    pub requires_trigger: bool,
    pub is_main: bool,
}

impl Group {
    pub fn main(assistant_name: &str, added_at: &str) -> Self {
        Self {
            jid: "main".to_string(),
            name: "Main".to_string(),
            folder: "main".to_string(),
            trigger: format!("@{}", assistant_name),
            added_at: added_at.to_string(),
            requires_trigger: true,
            is_main: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScheduledTask {
    pub id: TaskId,
    pub group_folder: String,
    pub chat_jid: ChatId,
    pub prompt: String,
    pub script: Option<String>,
    pub request_plane: Option<RequestPlane>,
    pub schedule_type: TaskScheduleType,
    pub schedule_value: String,
    pub context_mode: TaskContextMode,
    pub next_run: Option<String>,
    pub last_run: Option<String>,
    pub last_result: Option<String>,
    pub status: TaskStatus,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RequestPlane {
    Web,
    Email,
    None,
    Custom(String),
}

impl RequestPlane {
    pub fn parse(input: &str) -> Self {
        match input.trim().to_ascii_lowercase().as_str() {
            "web" => Self::Web,
            "email" => Self::Email,
            "" | "none" => Self::None,
            other => Self::Custom(other.to_string()),
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            Self::Web => "web",
            Self::Email => "email",
            Self::None => "none",
            Self::Custom(value) => value.as_str(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TaskScheduleType {
    Cron,
    Interval,
    Once,
    Custom(String),
}

impl TaskScheduleType {
    pub fn parse(input: &str) -> Self {
        match input.trim().to_ascii_lowercase().as_str() {
            "cron" => Self::Cron,
            "interval" => Self::Interval,
            "once" => Self::Once,
            other => Self::Custom(other.to_string()),
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            Self::Cron => "cron",
            Self::Interval => "interval",
            Self::Once => "once",
            Self::Custom(value) => value.as_str(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TaskContextMode {
    Group,
    Isolated,
    Custom(String),
}

impl TaskContextMode {
    pub fn parse(input: &str) -> Self {
        match input.trim().to_ascii_lowercase().as_str() {
            "group" => Self::Group,
            "" | "isolated" => Self::Isolated,
            other => Self::Custom(other.to_string()),
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            Self::Group => "group",
            Self::Isolated => "isolated",
            Self::Custom(value) => value.as_str(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TaskStatus {
    Active,
    Paused,
    Completed,
    Custom(String),
}

impl TaskStatus {
    pub fn parse(input: &str) -> Self {
        match input.trim().to_ascii_lowercase().as_str() {
            "" | "active" => Self::Active,
            "paused" => Self::Paused,
            "completed" => Self::Completed,
            other => Self::Custom(other.to_string()),
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            Self::Active => "active",
            Self::Paused => "paused",
            Self::Completed => "completed",
            Self::Custom(value) => value.as_str(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaskRunLog {
    pub task_id: TaskId,
    pub run_at: String,
    pub duration_ms: i64,
    pub status: TaskRunStatus,
    pub result: Option<String>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TaskRunStatus {
    Success,
    Error,
    Custom(String),
}

impl TaskRunStatus {
    pub fn parse(input: &str) -> Self {
        match input.trim().to_ascii_lowercase().as_str() {
            "" | "success" => Self::Success,
            "error" => Self::Error,
            other => Self::Custom(other.to_string()),
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            Self::Success => "success",
            Self::Error => "error",
            Self::Custom(value) => value.as_str(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MessageRecord {
    pub id: String,
    pub chat_jid: ChatId,
    pub sender: String,
    pub sender_name: Option<String>,
    pub content: String,
    pub timestamp: String,
    pub is_from_me: bool,
    pub is_bot_message: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ArtifactKind {
    BootstrapSummary,
    DevelopmentEnvironmentSnapshot,
    Transcript,
    TaskResult,
    ExecutionLog,
    MemorySnapshot,
    Custom(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArtifactRecord {
    pub id: ArtifactId,
    pub group_id: GroupId,
    pub task_id: Option<TaskId>,
    pub kind: ArtifactKind,
    pub title: String,
    pub body: String,
    pub location: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct CapabilityManifest {
    pub web_request: bool,
    pub email_request: bool,
    pub browser: bool,
    pub repo_sync: bool,
    pub ssh: bool,
    pub host_command: bool,
    pub secret_broker: bool,
    pub os_control: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct CapabilityManifestPolicy {
    pub web_request: Option<bool>,
    pub email_request: Option<bool>,
    pub browser: Option<bool>,
    pub repo_sync: Option<bool>,
    pub ssh: Option<bool>,
    pub host_command: Option<bool>,
    pub secret_broker: Option<bool>,
    pub os_control: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ExecutionLocation {
    LocalContainer,
    RemoteWorker,
    Omx,
    Host,
    Custom(String),
}

impl ExecutionLocation {
    pub fn parse(input: &str) -> Self {
        match input.trim().to_ascii_lowercase().as_str() {
            "local_container" | "local-container" | "container" => Self::LocalContainer,
            "remote_worker" | "remote-worker" | "remote" => Self::RemoteWorker,
            "omx" | "omx_droplet" | "omx-droplet" => Self::Omx,
            "" | "host" => Self::Host,
            other => Self::Custom(other.to_string()),
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            Self::LocalContainer => "local_container",
            Self::RemoteWorker => "remote_worker",
            Self::Omx => "omx",
            Self::Host => "host",
            Self::Custom(value) => value.as_str(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ExecutionTrustLevel {
    HostTrusted,
    SandboxedLocal,
    SandboxedRemote,
    Custom(String),
}

impl ExecutionTrustLevel {
    pub fn parse(input: &str) -> Self {
        match input.trim().to_ascii_lowercase().as_str() {
            "" | "host_trusted" | "host-trusted" => Self::HostTrusted,
            "sandboxed_local" | "sandboxed-local" => Self::SandboxedLocal,
            "sandboxed_remote" | "sandboxed-remote" => Self::SandboxedRemote,
            other => Self::Custom(other.to_string()),
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            Self::HostTrusted => "host_trusted",
            Self::SandboxedLocal => "sandboxed_local",
            Self::SandboxedRemote => "sandboxed_remote",
            Self::Custom(value) => value.as_str(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ExecutionMountKind {
    Project,
    Group,
    Global,
    Ipc,
    Claude,
    AgentRunner,
    Additional,
    Custom(String),
}

impl ExecutionMountKind {
    pub fn parse(input: &str) -> Self {
        match input.trim().to_ascii_lowercase().as_str() {
            "" | "project" => Self::Project,
            "group" => Self::Group,
            "global" => Self::Global,
            "ipc" => Self::Ipc,
            "claude" => Self::Claude,
            "agent_runner" | "agent-runner" => Self::AgentRunner,
            "additional" => Self::Additional,
            other => Self::Custom(other.to_string()),
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            Self::Project => "project",
            Self::Group => "group",
            Self::Global => "global",
            Self::Ipc => "ipc",
            Self::Claude => "claude",
            Self::AgentRunner => "agent_runner",
            Self::Additional => "additional",
            Self::Custom(value) => value.as_str(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExecutionMountSummaryEntry {
    pub host_path: Option<String>,
    pub container_path: Option<String>,
    pub readonly: bool,
    pub kind: ExecutionMountKind,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ExecutionSyncMode {
    None,
    Full,
    Discovery,
    Custom(String),
}

impl ExecutionSyncMode {
    pub fn parse(input: &str) -> Self {
        match input.trim().to_ascii_lowercase().as_str() {
            "" | "none" => Self::None,
            "full" => Self::Full,
            "discovery" => Self::Discovery,
            other => Self::Custom(other.to_string()),
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            Self::None => "none",
            Self::Full => "full",
            Self::Discovery => "discovery",
            Self::Custom(value) => value.as_str(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExecutionAdditionalMountSync {
    pub container_path: String,
    pub readonly: bool,
    pub sync_mode: ExecutionSyncMode,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExecutionSyncScope {
    pub project: ExecutionSyncMode,
    pub group: bool,
    pub claude: bool,
    pub ipc: bool,
    pub additional_mounts: Vec<ExecutionAdditionalMountSync>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ExecutionRunKind {
    Container,
    Codex,
    Claude,
    HostOsControl,
    Omx,
    Summary,
    Custom(String),
}

impl ExecutionRunKind {
    pub fn parse(input: &str) -> Self {
        match input.trim().to_ascii_lowercase().as_str() {
            "container" => Self::Container,
            "codex" => Self::Codex,
            "claude" => Self::Claude,
            "host_os_control" | "host-os-control" => Self::HostOsControl,
            "omx" => Self::Omx,
            "" | "summary" => Self::Summary,
            other => Self::Custom(other.to_string()),
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            Self::Container => "container",
            Self::Codex => "codex",
            Self::Claude => "claude",
            Self::HostOsControl => "host_os_control",
            Self::Omx => "omx",
            Self::Summary => "summary",
            Self::Custom(value) => value.as_str(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ExecutionStatus {
    Started,
    Success,
    Error,
    Custom(String),
}

impl ExecutionStatus {
    pub fn parse(input: &str) -> Self {
        match input.trim().to_ascii_lowercase().as_str() {
            "" | "started" => Self::Started,
            "success" => Self::Success,
            "error" => Self::Error,
            other => Self::Custom(other.to_string()),
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            Self::Started => "started",
            Self::Success => "success",
            Self::Error => "error",
            Self::Custom(value) => value.as_str(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExecutionProvenanceRecord {
    pub id: String,
    pub run_kind: ExecutionRunKind,
    pub group_folder: String,
    pub chat_jid: Option<String>,
    pub execution_location: ExecutionLocation,
    pub trust_level: ExecutionTrustLevel,
    pub request_plane: RequestPlane,
    pub effective_capabilities: CapabilityManifest,
    pub project_environment_id: Option<String>,
    pub mount_summary: Vec<ExecutionMountSummaryEntry>,
    pub secret_handles_used: Vec<String>,
    pub fallback_reason: Option<String>,
    pub sync_scope: Option<ExecutionSyncScope>,
    #[serde(default)]
    pub task_signature: Option<TaskSignature>,
    #[serde(default)]
    pub boundary_claims: Vec<BoundaryClaim>,
    #[serde(default)]
    pub gate_decision: Option<GateDecision>,
    #[serde(default)]
    pub gate_evaluation: Option<GateEvaluation>,
    #[serde(default)]
    pub assurance: Option<AssuranceTuple>,
    #[serde(default)]
    pub symbol_carriers: Vec<SymbolCarrier>,
    #[serde(default)]
    pub provenance_edges: Vec<ProvenanceEdge>,
    pub status: ExecutionStatus,
    pub created_at: String,
    pub updated_at: String,
    pub completed_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum SwarmRequestedLane {
    Auto,
    Agent,
    Codex,
    Host,
    RepoMirror,
    Symphony,
    Custom(String),
}

impl SwarmRequestedLane {
    pub fn parse(input: &str) -> Self {
        match input.trim().to_ascii_lowercase().as_str() {
            "" | "auto" => Self::Auto,
            "agent" => Self::Agent,
            "codex" => Self::Codex,
            "host" => Self::Host,
            "repo_mirror" | "repo-mirror" => Self::RepoMirror,
            "symphony" => Self::Symphony,
            other => Self::Custom(other.to_string()),
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            Self::Auto => "auto",
            Self::Agent => "agent",
            Self::Codex => "codex",
            Self::Host => "host",
            Self::RepoMirror => "repo_mirror",
            Self::Symphony => "symphony",
            Self::Custom(value) => value.as_str(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum SwarmResolvedLane {
    Agent,
    Codex,
    Host,
    RepoMirror,
    Symphony,
    Custom(String),
}

impl SwarmResolvedLane {
    pub fn parse(input: &str) -> Self {
        match input.trim().to_ascii_lowercase().as_str() {
            "" | "agent" => Self::Agent,
            "codex" => Self::Codex,
            "host" => Self::Host,
            "repo_mirror" | "repo-mirror" => Self::RepoMirror,
            "symphony" => Self::Symphony,
            other => Self::Custom(other.to_string()),
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            Self::Agent => "agent",
            Self::Codex => "codex",
            Self::Host => "host",
            Self::RepoMirror => "repo_mirror",
            Self::Symphony => "symphony",
            Self::Custom(value) => value.as_str(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum SwarmRunStatus {
    Pending,
    Active,
    Completed,
    Failed,
    Canceled,
    Custom(String),
}

impl SwarmRunStatus {
    pub fn parse(input: &str) -> Self {
        match input.trim().to_ascii_lowercase().as_str() {
            "" | "pending" => Self::Pending,
            "active" => Self::Active,
            "completed" => Self::Completed,
            "failed" => Self::Failed,
            "canceled" | "cancelled" => Self::Canceled,
            other => Self::Custom(other.to_string()),
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            Self::Pending => "pending",
            Self::Active => "active",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Canceled => "canceled",
            Self::Custom(value) => value.as_str(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum SwarmTaskStatus {
    Pending,
    Ready,
    Running,
    Completed,
    Failed,
    Blocked,
    Canceled,
    Custom(String),
}

impl SwarmTaskStatus {
    pub fn parse(input: &str) -> Self {
        match input.trim().to_ascii_lowercase().as_str() {
            "" | "pending" => Self::Pending,
            "ready" => Self::Ready,
            "running" => Self::Running,
            "completed" => Self::Completed,
            "failed" => Self::Failed,
            "blocked" => Self::Blocked,
            "canceled" | "cancelled" => Self::Canceled,
            other => Self::Custom(other.to_string()),
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            Self::Pending => "pending",
            Self::Ready => "ready",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Blocked => "blocked",
            Self::Canceled => "canceled",
            Self::Custom(value) => value.as_str(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SwarmRun {
    pub id: String,
    pub group_folder: String,
    pub chat_jid: String,
    pub created_by: String,
    pub objective: String,
    pub requested_lane: SwarmRequestedLane,
    #[serde(default)]
    pub objective_signature: Option<TaskSignature>,
    #[serde(default)]
    pub objective_gate: Option<GateEvaluation>,
    pub status: SwarmRunStatus,
    pub max_concurrency: i64,
    pub summary: Option<String>,
    pub result: Option<Value>,
    pub created_at: String,
    pub updated_at: String,
    pub completed_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SwarmTask {
    pub id: String,
    pub run_id: String,
    pub task_key: String,
    pub title: String,
    pub prompt: Option<String>,
    pub command: Option<String>,
    pub requested_lane: SwarmRequestedLane,
    pub resolved_lane: Option<SwarmResolvedLane>,
    #[serde(default)]
    pub task_signature: Option<TaskSignature>,
    #[serde(default)]
    pub boundary_quadrant: Option<BoundaryQuadrant>,
    #[serde(default)]
    pub gate_decision: Option<GateDecision>,
    #[serde(default)]
    pub gate_evaluation: Option<GateEvaluation>,
    #[serde(default)]
    pub required_roles: Vec<String>,
    pub status: SwarmTaskStatus,
    pub priority: i64,
    pub target_group_folder: String,
    pub target_chat_jid: String,
    pub cwd: Option<String>,
    pub repo: Option<String>,
    pub repo_path: Option<String>,
    pub sync: bool,
    pub host: Option<String>,
    pub user: Option<String>,
    pub port: Option<u16>,
    pub timeout_ms: Option<u64>,
    pub metadata: Option<Value>,
    pub result: Option<Value>,
    pub error: Option<String>,
    pub lease_owner: Option<String>,
    pub lease_expires_at: Option<String>,
    pub attempts: i64,
    pub max_attempts: i64,
    pub created_at: String,
    pub updated_at: String,
    pub started_at: Option<String>,
    pub completed_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SwarmTaskDependency {
    pub task_id: String,
    pub depends_on_task_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum HostOsControlApprovalDecision {
    Once,
    Session,
    Always,
    Deny,
    Custom(String),
}

impl HostOsControlApprovalDecision {
    pub fn parse(input: &str) -> Self {
        match input.trim().to_ascii_lowercase().as_str() {
            "once" => Self::Once,
            "session" => Self::Session,
            "always" => Self::Always,
            "" | "deny" => Self::Deny,
            other => Self::Custom(other.to_string()),
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            Self::Once => "once",
            Self::Session => "session",
            Self::Always => "always",
            Self::Deny => "deny",
            Self::Custom(value) => value.as_str(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum HostOsControlApprovalStatus {
    Pending,
    Approved,
    Consumed,
    Denied,
    Custom(String),
}

impl HostOsControlApprovalStatus {
    pub fn parse(input: &str) -> Self {
        match input.trim().to_ascii_lowercase().as_str() {
            "" | "pending" => Self::Pending,
            "approved" => Self::Approved,
            "consumed" => Self::Consumed,
            "denied" => Self::Denied,
            other => Self::Custom(other.to_string()),
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            Self::Pending => "pending",
            Self::Approved => "approved",
            Self::Consumed => "consumed",
            Self::Denied => "denied",
            Self::Custom(value) => value.as_str(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HostOsControlApprovalRequestRecord {
    pub id: String,
    pub source_group: String,
    pub chat_jid: Option<String>,
    pub action_kind: String,
    pub action_scope: String,
    pub action_fingerprint: String,
    pub action_summary: String,
    pub action_payload: Option<Value>,
    pub allowed_decisions: Vec<HostOsControlApprovalDecision>,
    #[serde(default)]
    pub boundary_claim: Option<BoundaryClaim>,
    #[serde(default)]
    pub gate_evaluation: Option<GateEvaluation>,
    pub status: HostOsControlApprovalStatus,
    pub resolved_decision: Option<HostOsControlApprovalDecision>,
    pub created_at: String,
    pub resolved_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SshEndpoint {
    pub host: String,
    pub user: Option<String>,
    pub port: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RemoteWorkerMode {
    Off,
    Main,
    All,
    Custom(String),
}

impl RemoteWorkerMode {
    pub fn parse(input: &str) -> Self {
        match input.trim().to_ascii_lowercase().as_str() {
            "" | "off" => Self::Off,
            "main" => Self::Main,
            "all" => Self::All,
            other => Self::Custom(other.to_string()),
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            Self::Off => "off",
            Self::Main => "main",
            Self::All => "all",
            Self::Custom(value) => value.as_str(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ExecutionLane {
    Auto,
    Host,
    Container,
    RemoteWorker,
    Omx,
    Custom(String),
}

impl ExecutionLane {
    pub fn parse(input: &str) -> Self {
        match input.trim().to_ascii_lowercase().as_str() {
            "" | "auto" => Self::Auto,
            "host" => Self::Host,
            "container" => Self::Container,
            "remote" | "remote-worker" | "remote_worker" => Self::RemoteWorker,
            "omx" => Self::Omx,
            other => Self::Custom(other.to_string()),
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            Self::Auto => "auto",
            Self::Host => "host",
            Self::Container => "container",
            Self::RemoteWorker => "remote-worker",
            Self::Omx => "omx",
            Self::Custom(value) => value.as_str(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum DevelopmentEnvironmentKind {
    Local,
    DigitalOceanDroplet,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DevelopmentEnvironment {
    pub id: DevelopmentEnvironmentId,
    pub name: String,
    pub kind: DevelopmentEnvironmentKind,
    pub workspace_root: Option<String>,
    pub repo_root: Option<String>,
    pub ssh: Option<SshEndpoint>,
    pub remote_worker_mode: RemoteWorkerMode,
    pub remote_worker_root: Option<String>,
    pub bootstrap_timeout_ms: Option<u64>,
    pub sync_interval_ms: Option<u64>,
    pub tunnel_port_base: Option<u16>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ExecutionBoundaryKind {
    Container,
    Host,
    RemoteWorker,
    Omx,
    InProcess,
    External(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExecutionBoundary {
    pub kind: ExecutionBoundaryKind,
    pub root: Option<String>,
    pub isolated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExecutionContext {
    pub group_id: GroupId,
    pub chat_id: Option<ChatId>,
    pub task_id: Option<TaskId>,
    pub boundary: ExecutionBoundary,
    pub workspace_root: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct QueueSnapshot {
    pub jid: GroupId,
    pub active: bool,
    pub pending_messages: bool,
    pub running_task_id: Option<TaskId>,
    pub pending_tasks: Vec<TaskId>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum QueueOutcome {
    Started,
    Queued,
    SkippedDuplicate,
}
