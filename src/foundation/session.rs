use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;

use super::Plan;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SessionRole {
    User,
    Assistant,
    System,
    Tool,
    Custom(String),
}

impl SessionRole {
    pub fn as_str(&self) -> &str {
        match self {
            Self::User => "user",
            Self::Assistant => "assistant",
            Self::System => "system",
            Self::Tool => "tool",
            Self::Custom(value) => value.as_str(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionTurn {
    pub role: SessionRole,
    pub content: String,
    pub timestamp: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct SessionState {
    pub turns: Vec<SessionTurn>,
    pub last_plan: Option<Plan>,
    #[serde(default)]
    pub metadata: BTreeMap<String, String>,
}

impl SessionState {
    pub fn push_turn(&mut self, role: SessionRole, content: impl Into<String>) {
        self.turns.push(SessionTurn {
            role,
            content: content.into(),
            timestamp: None,
        });
    }
}

#[derive(Debug, Clone)]
pub struct SessionStore {
    path: PathBuf,
}

impl SessionStore {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn save(&self, state: &SessionState) -> Result<()> {
        let json =
            serde_json::to_string_pretty(state).context("failed to serialize session state")?;
        fs::write(&self.path, json)
            .with_context(|| format!("failed to write session file {}", self.path.display()))?;
        Ok(())
    }

    pub fn load(&self) -> Result<SessionState> {
        if !self.path.exists() {
            return Ok(SessionState::default());
        }

        let json = fs::read_to_string(&self.path)
            .with_context(|| format!("failed to read session file {}", self.path.display()))?;
        Ok(serde_json::from_str(&json).context("failed to deserialize session state")?)
    }

    pub fn clear(&self) -> Result<()> {
        if self.path.exists() {
            fs::remove_file(&self.path).with_context(|| {
                format!("failed to remove session file {}", self.path.display())
            })?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{SessionRole, SessionState, SessionStore};
    use crate::foundation::Plan;
    use tempfile::tempdir;

    #[test]
    fn session_store_round_trips_state() {
        let dir = tempdir().unwrap();
        let store = SessionStore::new(dir.path().join("session.json"));
        let mut state = SessionState::default();
        state.push_turn(SessionRole::User, "hello");
        state.last_plan = Some(Plan::new("ship release"));

        store.save(&state).unwrap();
        let loaded = store.load().unwrap();

        assert_eq!(loaded.turns.len(), 1);
        assert_eq!(loaded.last_plan.unwrap().goal, "ship release");
    }
}
