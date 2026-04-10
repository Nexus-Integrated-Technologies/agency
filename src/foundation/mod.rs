//! Canonical foundation for the Rust Agency -> NanoClaw domain.
//!
//! Layering rule:
//! - The foundation owns first-class domain entities and execution lineage.
//! - Runtimes such as `nanoclaw` are descendants of this layer.
//! - Richer Agency subsystems may remain, but they should attach themselves to
//!   the same group/task/message/artifact graph instead of defining a parallel
//!   ontology.

pub mod assurance;
pub mod boundary;
pub mod context;
pub mod domain;
pub mod events;
pub mod gate;
pub mod objective;
pub mod planning;
pub mod ports;
pub mod provenance;
pub mod queue;
pub mod refresh;
pub mod roles;
pub mod routing;
pub mod scheduler;
pub mod scope;
pub mod service;
pub mod session;
pub mod signature;

pub use assurance::{AssuranceTuple, CongruenceLevel, Formality};
pub use boundary::{classify_boundary_text, BoundaryClaim, BoundaryClaimSource, BoundaryQuadrant};
pub use context::{ContextDocument, ContextLoader, ProjectContext};
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
pub use gate::{GateAspect, GateCheck, GateDecision, GateEvaluation, GateScope};
pub use objective::{Objective, ResourceBudget};
pub use planning::{Plan, PlanStep, PlanStepStatus};
pub use ports::{
    ArtifactStore, DevelopmentEnvironmentProvider, EventLog, FoundationStore, WorkQueue,
};
pub use provenance::{EvidenceGraph, ProvenanceEdge, ProvenanceEdgeKind, SymbolCarrier};
pub use queue::{DurableTaskQueue, DurableTaskRecord, DurableTaskStatus, SqliteDurableTaskQueue};
pub use refresh::{RefreshAction, RefreshPlan, RefreshReport, RefreshTrigger};
pub use roles::RoleAlgebra;
pub use routing::{
    HarnessRouter, RouteConfidence, RoutingDecision, RoutingInput, ScaleClass, ScaleElasticity,
    ScaleProfile,
};
pub use scheduler::{
    build_run_log, build_scheduled_task, compute_next_run, HabitDefinition, TaskScheduleInput,
};
pub use scope::{ScopeSet, ScopeSlice};
pub use service::{ServiceClause, ServiceStatus};
pub use session::{SessionRole, SessionState, SessionStore, SessionTurn};
pub use signature::{TaskBudget, TaskDataShape, TaskKind, TaskSignature};
