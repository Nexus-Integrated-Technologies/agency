//! Rust NanoClaw migration surface.
//!
//! The default compiled path is the NanoClaw-shaped runtime in `src/nanoclaw/`.
//! Legacy Agency subsystems remain on disk, but the old dependency surface is
//! intentionally disabled until those systems are re-homed as descendants of
//! the new foundation model.

pub mod foundation;
pub mod nanoclaw;

#[cfg(feature = "legacy-agency")]
compile_error!(
    "feature `legacy-agency` is temporarily disabled while the Cargo surface is being cut down to the NanoClaw foundation"
);

pub use foundation::{
    ArtifactKind, ArtifactRecord, AssuranceTuple, BoundaryClaim, BoundaryClaimSource,
    BoundaryQuadrant, CongruenceLevel, ContextDocument, ContextLoader, DevelopmentEnvironment,
    DevelopmentEnvironmentKind, DurableTaskQueue, DurableTaskRecord, DurableTaskStatus,
    EvidenceGraph, ExecutionBoundary, ExecutionBoundaryKind, ExecutionContext, ExecutionLane,
    Formality, FoundationEvent, GateAspect, GateCheck, GateDecision, GateEvaluation, GateScope,
    Group, HabitDefinition, HarnessRouter, Objective, Plan, PlanStep, PlanStepStatus,
    ProjectContext, ProvenanceEdge, ProvenanceEdgeKind, QueueOutcome, QueueSnapshot, RefreshAction,
    RefreshPlan, RefreshReport, RefreshTrigger, RemoteWorkerMode, RequestPlane, ResourceBudget,
    RoleAlgebra, RouteConfidence, RoutingDecision, RoutingInput, ScaleClass, ScaleElasticity,
    ScaleProfile, ScheduledTask, ScopeSet, ScopeSlice, ServiceClause, ServiceStatus, SessionRole,
    SessionState, SessionStore, SessionTurn, SqliteDurableTaskQueue, SshEndpoint, SymbolCarrier,
    TaskBudget, TaskContextMode, TaskDataShape, TaskKind, TaskRunLog, TaskRunStatus,
    TaskScheduleInput, TaskScheduleType, TaskSignature, TaskStatus,
};
pub use nanoclaw::{run_cli as run_nanoclaw_cli, BootstrapSummary, NanoclawApp, NanoclawConfig};
