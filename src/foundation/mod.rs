//! Canonical foundation for the Rust Agency -> NanoClaw domain.
//!
//! Layering rule:
//! - The foundation owns first-class domain entities and execution lineage.
//! - Runtimes such as `nanoclaw` are descendants of this layer.
//! - Richer Agency subsystems may remain, but they should attach themselves to
//!   the same group/task/message/artifact graph instead of defining a parallel
//!   ontology.

pub mod domain;
pub mod events;
pub mod ports;

pub use domain::{
    ArtifactKind, ArtifactRecord, CapabilityManifest, CapabilityManifestPolicy, ChatId,
    DevelopmentEnvironment, DevelopmentEnvironmentId, DevelopmentEnvironmentKind,
    ExecutionAdditionalMountSync, ExecutionBoundary, ExecutionBoundaryKind, ExecutionContext,
    ExecutionLane, ExecutionLocation, ExecutionMountKind, ExecutionMountSummaryEntry,
    ExecutionProvenanceRecord, ExecutionRunKind, ExecutionStatus, ExecutionSyncMode,
    ExecutionSyncScope, ExecutionTrustLevel, Group, GroupId, HostOsControlApprovalDecision,
    HostOsControlApprovalRequestRecord, HostOsControlApprovalStatus, MessageRecord, QueueOutcome,
    QueueSnapshot, RemoteWorkerMode, RequestPlane, RouterStateKey, ScheduledTask, SshEndpoint,
    SwarmRequestedLane, SwarmResolvedLane, SwarmRun, SwarmRunStatus, SwarmTask,
    SwarmTaskDependency, SwarmTaskStatus, TaskContextMode, TaskId, TaskRunLog, TaskRunStatus,
    TaskScheduleType, TaskStatus,
};
pub use events::FoundationEvent;
pub use ports::{
    ArtifactStore, DevelopmentEnvironmentProvider, EventLog, FoundationStore, WorkQueue,
};
