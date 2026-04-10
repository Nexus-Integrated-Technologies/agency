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
    ArtifactKind, ArtifactRecord, DevelopmentEnvironment, DevelopmentEnvironmentKind,
    ExecutionBoundary, ExecutionBoundaryKind, ExecutionContext, ExecutionLane, FoundationEvent,
    Group, QueueOutcome, QueueSnapshot, RemoteWorkerMode, RequestPlane, ScheduledTask, SshEndpoint,
    TaskContextMode, TaskRunLog, TaskRunStatus, TaskScheduleType, TaskStatus,
};
pub use nanoclaw::{run_cli as run_nanoclaw_cli, BootstrapSummary, NanoclawApp, NanoclawConfig};
