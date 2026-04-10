use serde::{Deserialize, Serialize};

use super::domain::{
    ArtifactRecord, DevelopmentEnvironment, ExecutionContext, Group, MessageRecord, RouterStateKey,
    ScheduledTask,
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum FoundationEvent {
    GroupRegistered {
        group: Group,
    },
    MessageRecorded {
        message: MessageRecord,
    },
    TaskScheduled {
        task: ScheduledTask,
    },
    RouterStateUpdated {
        key: RouterStateKey,
        value: String,
    },
    DevelopmentEnvironmentResolved {
        environment: DevelopmentEnvironment,
    },
    ArtifactEmitted {
        artifact: ArtifactRecord,
        context: Option<ExecutionContext>,
    },
}
