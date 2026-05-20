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

    pub fn compact_with_summary(&self, max_turns: usize, summary_char_limit: usize) -> Self {
        if max_turns == 0 {
            let mut compact = self.clone();
            compact.turns.clear();
            compact.metadata.insert(
                "compacted_from_turns".to_string(),
                self.turns.len().to_string(),
            );
            compact.metadata.insert(
                "compacted_omitted_turns".to_string(),
                self.turns.len().to_string(),
            );
            return compact;
        }

        if self.turns.len() <= max_turns {
            return self.clone();
        }

        let mut compact = self.clone();
        if max_turns <= 2 {
            compact.turns = self.turns[self.turns.len() - max_turns..].to_vec();
            compact.metadata.insert(
                "compacted_from_turns".to_string(),
                self.turns.len().to_string(),
            );
            compact.metadata.insert(
                "compacted_omitted_turns".to_string(),
                (self.turns.len() - compact.turns.len()).to_string(),
            );
            return compact;
        }

        let first_turn = self.turns[0].clone();
        let recent_count = max_turns - 2;
        let recent_start = self.turns.len() - recent_count;
        let omitted = &self.turns[1..recent_start];
        let mut role_counts = BTreeMap::<String, usize>::new();
        for turn in omitted {
            *role_counts
                .entry(turn.role.as_str().to_string())
                .or_default() += 1;
        }
        let role_summary = role_counts
            .into_iter()
            .map(|(role, count)| format!("{role}={count}"))
            .collect::<Vec<_>>()
            .join(", ");
        let last_omitted = omitted
            .last()
            .map(|turn| summarize_text_for_session(&turn.content, summary_char_limit))
            .unwrap_or_else(|| "none".to_string());

        compact.turns = Vec::with_capacity(max_turns);
        compact.turns.push(first_turn);
        compact.turns.push(SessionTurn {
            role: SessionRole::System,
            content: format!(
                "[CONTEXT COMPACTED]: {} older turns omitted. Roles: {}. Last omitted: {}",
                omitted.len(),
                if role_summary.is_empty() {
                    "none"
                } else {
                    role_summary.as_str()
                },
                last_omitted
            ),
            timestamp: None,
        });
        compact.turns.extend_from_slice(&self.turns[recent_start..]);
        compact.metadata.insert(
            "compacted_from_turns".to_string(),
            self.turns.len().to_string(),
        );
        compact.metadata.insert(
            "compacted_omitted_turns".to_string(),
            omitted.len().to_string(),
        );
        compact
    }
}

fn summarize_text_for_session(input: &str, max_chars: usize) -> String {
    let trimmed = input.trim();
    if max_chars == 0 {
        return String::new();
    }
    if trimmed.chars().count() <= max_chars {
        return trimmed.to_string();
    }
    let mut summary = trimmed.chars().take(max_chars).collect::<String>();
    summary.push_str("...");
    summary
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

    #[test]
    fn compacts_session_state_with_deterministic_summary() {
        let mut state = SessionState::default();
        state.push_turn(SessionRole::User, "original objective");
        state.push_turn(SessionRole::Assistant, "first answer");
        state.push_turn(SessionRole::User, "second request");
        state.push_turn(SessionRole::Assistant, "second answer with details");
        state.push_turn(SessionRole::User, "current request");

        let compact = state.compact_with_summary(4, 12);

        assert_eq!(compact.turns.len(), 4);
        assert_eq!(compact.turns[0].content, "original objective");
        assert_eq!(compact.turns[1].role, SessionRole::System);
        assert!(compact.turns[1]
            .content
            .contains("[CONTEXT COMPACTED]: 2 older turns omitted"));
        assert!(compact.turns[1].content.contains("assistant=1"));
        assert!(compact.turns[1].content.contains("user=1"));
        assert!(compact.turns[1].content.contains("second reque..."));
        assert_eq!(compact.turns[2].content, "second answer with details");
        assert_eq!(compact.turns[3].content, "current request");
        assert_eq!(
            compact
                .metadata
                .get("compacted_from_turns")
                .map(String::as_str),
            Some("5")
        );
        assert_eq!(
            compact
                .metadata
                .get("compacted_omitted_turns")
                .map(String::as_str),
            Some("2")
        );
    }
}
