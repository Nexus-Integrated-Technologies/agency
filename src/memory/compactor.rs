//! High-Fidelity Context Compaction
//! 
//! Provides logic to summarize and compress long conversation histories
//! while preserving the core objective and recent context.

use anyhow::Result;
use std::sync::Arc;
use tracing::{info, warn};

use crate::agent::{LLMProvider, SimpleAgent, AgentConfig, AgentType};
use crate::memory::episodic::{ConversationTurn, EpisodicMemory, Role};
use crate::orchestrator::profile::AgencyProfile;

const MIN_TURNS_FOR_COMPACTION: usize = 10;
const RECENT_TURNS_TO_KEEP: usize = 5;
const SUMMARY_MODEL: &str = "qwen2.5:3b-q4";

pub struct ContextCompactor;

impl ContextCompactor {
    /// Compacts the episodic memory if it exceeds the specified token limit.
    pub async fn compact_if_needed(
        memory: &mut EpisodicMemory,
        provider: Arc<dyn LLMProvider>,
        profile: &AgencyProfile,
        max_tokens: usize,
    ) -> Result<bool> {
        let current_tokens = memory.estimate_total_tokens();
        
        if current_tokens < max_tokens {
            return Ok(false);
        }

        info!("Triggering context compaction (current: {} tokens, limit: {})", current_tokens, max_tokens);

        let turns = memory.get_turns();
        if turns.len() < MIN_TURNS_FOR_COMPACTION {
            warn!("Memory is too small to compact effectively, skipping.");
            return Ok(false);
        }

        // 1. Identify parts to keep and summarize.
        let (first_message, middle_turns, recent_turns) =
            partition_turns_for_compaction(&turns, RECENT_TURNS_TO_KEEP)?;
        let middle_text = format_turns_for_summary(middle_turns);

        // 3. Perform summarization
        let mut config = AgentConfig::new(AgentType::GeneralChat, profile);
        config.model = SUMMARY_MODEL.to_string(); // Use a fast model for summary
        let summarizer = SimpleAgent::new_with_provider(provider, config);

        let prompt = format!(
            "Please provide a concise technical summary of the following conversation history. \nFocus on key decisions made, tools used, and the current progress toward the goal. \nKEEP IT UNDER 500 CHARACTERS.\n\n### History to Summarize:\n{}"
            , 
            middle_text
        );

        let summary_response = summarizer.execute_simple(&prompt, None).await?;
        let summary_content = summary_response.answer;

        // 4. Construct new memory state
        let mut new_turns = Vec::with_capacity(2 + recent_turns.len());
        new_turns.push(first_message.clone());
        
        new_turns.push(crate::memory::episodic::ConversationTurn {
            role: crate::memory::episodic::Role::System,
            content: format!("[CONTEXT COMPACTED]: Previous turns summarized here: {}", summary_content),
            timestamp: chrono::Utc::now(),
            agent: Some("SystemCompactor".to_string()),
        });
        
        new_turns.extend(recent_turns.iter().cloned());

        memory.replace_turns(new_turns);
        info!("Compaction complete. New token count: {}", memory.estimate_total_tokens());

        Ok(true)
    }
}

fn partition_turns_for_compaction(
    turns: &[ConversationTurn],
    recent_turns_to_keep: usize,
) -> Result<(&ConversationTurn, &[ConversationTurn], &[ConversationTurn])> {
    let (first_turn, remaining) = turns
        .split_first()
        .ok_or_else(|| anyhow::anyhow!("Cannot compact empty memory"))?;

    if remaining.len() <= recent_turns_to_keep {
        return Err(anyhow::anyhow!(
            "Insufficient turns for compaction partitioning"
        ));
    }

    let middle_len = remaining.len() - recent_turns_to_keep;
    let (middle_turns, recent_turns) = remaining.split_at(middle_len);
    Ok((first_turn, middle_turns, recent_turns))
}

fn role_for_summary(role: &Role) -> &'static str {
    match role {
        Role::User => "User",
        Role::Assistant => "Assistant",
        Role::System => "System",
        Role::Tool => "Tool",
    }
}

fn format_turns_for_summary(turns: &[ConversationTurn]) -> String {
    turns
        .iter()
        .map(|turn| format!("{}: {}", role_for_summary(&turn.role), turn.content))
        .collect::<Vec<_>>()
        .join("\n\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn turn(role: Role, content: &str) -> ConversationTurn {
        ConversationTurn {
            role,
            content: content.to_string(),
            timestamp: Utc::now(),
            agent: None,
        }
    }

    #[test]
    fn partition_turns_preserves_goal_middle_and_recent_segments() {
        let turns = vec![
            turn(Role::User, "goal"),
            turn(Role::Assistant, "a1"),
            turn(Role::User, "u2"),
            turn(Role::Assistant, "a3"),
            turn(Role::User, "u4"),
            turn(Role::Assistant, "a5"),
            turn(Role::User, "u6"),
        ];

        let (first, middle, recent) =
            partition_turns_for_compaction(&turns, 2).expect("partition should succeed");

        assert_eq!(first.content, "goal");
        assert_eq!(middle.len(), 4);
        assert_eq!(middle[0].content, "a1");
        assert_eq!(middle[3].content, "u4");
        assert_eq!(recent.len(), 2);
        assert_eq!(recent[0].content, "a5");
        assert_eq!(recent[1].content, "u6");
    }

    #[test]
    fn partition_turns_requires_middle_segment() {
        let turns = vec![turn(Role::User, "goal"), turn(Role::Assistant, "latest")];
        let err = partition_turns_for_compaction(&turns, 1).expect_err("partition should fail");
        assert!(
            err.to_string()
                .contains("Insufficient turns for compaction partitioning")
        );
    }

    #[test]
    fn format_turns_for_summary_includes_role_labels() {
        let turns = vec![
            turn(Role::User, "need plan"),
            turn(Role::Assistant, "implemented"),
            turn(Role::System, "checkpoint"),
            turn(Role::Tool, "shell output"),
        ];

        let summary = format_turns_for_summary(&turns);
        assert!(summary.contains("User: need plan"));
        assert!(summary.contains("Assistant: implemented"));
        assert!(summary.contains("System: checkpoint"));
        assert!(summary.contains("Tool: shell output"));
    }
}
