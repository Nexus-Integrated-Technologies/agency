// ReAct Agent Implementation
// 
// Implements the Reasoning + Acting framework for intelligent agent behavior.

use async_trait::async_trait;
use ollama_rs::Ollama;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::collections::{HashMap, HashSet};
use tracing::{debug, info, warn};
use futures_util::StreamExt;

use super::{Agent, AgentConfig, AgentType, is_action_query, LLMProvider, OllamaProvider, OpenAICompatibleProvider, AgentResult, AgentError};
use crate::memory::Memory;
use crate::tools::{ToolCall, ToolRegistry};
use pai_core::{HookManager, HookEvent, HookEventType, HookAction};
use pai_core::uap::{SovereignAgent, UapTask, UapStep, UapStepStatus, UapArtifact};

use pai_core::vcp::{ValueCommitment, CommitmentModality, EconomicImpact};
use pai_core::sap::{AlignmentEngine, AlignmentAudit, AuditStatus};

/// A single step in the ReAct loop
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReActStep {
    /// The agent's thought/reasoning
    pub thought: String,
    /// The actions to take (tool calls)
    pub actions: Vec<ToolCall>,
    /// The observations from the actions
    pub observations: Vec<String>,
    /// Whether this is the final answer
    pub is_final: bool,
    /// The final answer (if is_final is true)
    pub answer: Option<String>,
}

struct QualityScoringProfile {
    min_words_for_repetition_check: usize,
    severe_repetition_ratio: f32,
    severe_repetition_penalty: f32,
    moderate_repetition_ratio: f32,
    moderate_repetition_penalty: f32,
    prompt_leak_markers: &'static [&'static str],
    short_response_len: Option<usize>,
    short_response_penalty: f32,
    sparse_word_count: Option<usize>,
    sparse_response_len: Option<usize>,
    sparse_response_penalty: f32,
    non_ascii_ratio_threshold: Option<f32>,
    non_ascii_min_len: Option<usize>,
    non_ascii_penalty: f32,
}

const REACT_QUALITY_PROFILE: QualityScoringProfile = QualityScoringProfile {
    min_words_for_repetition_check: 10,
    severe_repetition_ratio: 0.2,
    severe_repetition_penalty: 0.1,
    moderate_repetition_ratio: 0.4,
    moderate_repetition_penalty: 0.4,
    prompt_leak_markers: &["## User Query", "## Instruction", "<|im_start|>"],
    short_response_len: None,
    short_response_penalty: 1.0,
    sparse_word_count: Some(3),
    sparse_response_len: Some(20),
    sparse_response_penalty: 0.1,
    non_ascii_ratio_threshold: Some(0.5),
    non_ascii_min_len: Some(20),
    non_ascii_penalty: 0.5,
};

const SIMPLE_QUALITY_PROFILE: QualityScoringProfile = QualityScoringProfile {
    min_words_for_repetition_check: 20,
    severe_repetition_ratio: 0.3,
    severe_repetition_penalty: 0.2,
    moderate_repetition_ratio: 0.5,
    moderate_repetition_penalty: 0.5,
    prompt_leak_markers: &["## User Query", "## Instruction"],
    short_response_len: Some(5),
    short_response_penalty: 0.3,
    sparse_word_count: None,
    sparse_response_len: None,
    sparse_response_penalty: 1.0,
    non_ascii_ratio_threshold: None,
    non_ascii_min_len: None,
    non_ascii_penalty: 1.0,
};

fn score_response_quality_with_profile(response: &str, profile: &QualityScoringProfile) -> f32 {
    let mut score = 1.0;
    let words: Vec<&str> = response.split_whitespace().collect();
    let total_words = words.len();

    if total_words > profile.min_words_for_repetition_check {
        let unique_words: HashSet<&str> = words.iter().copied().collect();
        let uniqueness_ratio = unique_words.len() as f32 / total_words as f32;
        if uniqueness_ratio < profile.severe_repetition_ratio {
            score *= profile.severe_repetition_penalty;
        } else if uniqueness_ratio < profile.moderate_repetition_ratio {
            score *= profile.moderate_repetition_penalty;
        }
    }

    if let (Some(word_limit), Some(min_len)) = (profile.sparse_word_count, profile.sparse_response_len) {
        if total_words < word_limit && response.len() > min_len {
            score *= profile.sparse_response_penalty;
        }
    }

    if let (Some(threshold), Some(min_len)) = (profile.non_ascii_ratio_threshold, profile.non_ascii_min_len) {
        if response.len() > min_len {
            let non_ascii_count = response.chars().filter(|c| !c.is_ascii()).count() as f32;
            let ratio = non_ascii_count / response.len() as f32;
            if ratio > threshold {
                score *= profile.non_ascii_penalty;
            }
        }
    }

    if profile.prompt_leak_markers.iter().any(|marker| response.contains(marker)) {
        score *= 0.1;
    }

    if let Some(min_len) = profile.short_response_len {
        if response.trim().len() < min_len {
            score *= profile.short_response_penalty;
        }
    }

    score
}

fn find_prompt_leak_point(response: &str) -> Option<usize> {
    for marker in ["## User Query", "## Instruction", "<|im_start|>user"] {
        if let Some(idx) = response.find(marker) {
            return Some(idx);
        }
    }
    None
}

impl ReActStep {
    pub fn thought(thought: impl Into<String>) -> Self {
        Self {
            thought: thought.into(),
            actions: Vec::new(),
            observations: Vec::new(),
            is_final: false,
            answer: None,
        }
    }

    pub fn with_action(mut self, action: ToolCall) -> Self {
        self.actions.push(action);
        self
    }

    pub fn with_actions(mut self, actions: Vec<ToolCall>) -> Self {
        self.actions = actions;
        self
    }

    pub fn final_answer(thought: impl Into<String>, answer: impl Into<String>) -> Self {
        Self {
            thought: thought.into(),
            actions: Vec::new(),
            observations: Vec::new(),
            is_final: true,
            answer: Some(answer.into()),
        }
    }
}

/// Response from an agent execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentResponse {
    /// The final answer (PlainView/Publication Surface)
    pub answer: String,
    /// The internal reasoning (TechView/Reasoning Surface)
    pub thought: Option<String>,
    /// All steps taken to reach the answer (Trace/Evidence Surface)
    pub steps: Vec<ReActStep>,
    /// The agent type that generated this response
    pub agent_type: AgentType,
    /// Whether the response was successful
    pub success: bool,
    /// Any error message
    pub error: Option<String>,
    /// FPF Reliability Score (R) - calculated via CG-Spec
    pub reliability: f32,
    /// Token usage for this response
    pub cost_tokens: u32,
    /// Pending approval for HITL
    pub pending_approval: Option<crate::safety::ApprovalRequest>,
}

impl AgentResponse {
    pub fn success(answer: impl Into<String>, steps: Vec<ReActStep>, agent_type: AgentType) -> Self {
        let answer_str = answer.into();
        Self {
            answer: answer_str,
            thought: None,
            steps,
            agent_type,
            success: true,
            error: None,
            reliability: 1.0,
            cost_tokens: 0,
            pending_approval: None,
        }
    }

    pub fn with_thought(mut self, thought: impl Into<String>) -> Self {
        self.thought = Some(thought.into());
        self
    }

    pub fn with_reliability(mut self, r: f32) -> Self {
        self.reliability = r;
        self
    }

    pub fn with_approval(mut self, approval: crate::safety::ApprovalRequest) -> Self {
        self.pending_approval = Some(approval);
        self
    }

    pub fn failure(error: impl Into<String>, steps: Vec<ReActStep>, agent_type: AgentType) -> Self {
        let error = error.into();
        Self {
            answer: format!("I encountered an error: {}", error),
            thought: None,
            steps,
            agent_type,
            success: false,
            error: Some(error),
            reliability: 0.0,
            cost_tokens: 0,
            pending_approval: None,
        }
    }
}

/// ReAct Agent with reasoning and tool use capabilities
#[derive(Clone)]
pub struct ReActAgent {
    provider: Arc<dyn LLMProvider>,
    config: AgentConfig,
    tools: Arc<ToolRegistry>,
    memory: Option<Arc<dyn Memory>>,
    safety: Option<Arc<tokio::sync::Mutex<crate::safety::SafetyGuard>>>,
    pub pai_hooks: Option<Arc<HookManager>>,
    pub pai_memory: Option<Arc<pai_core::memory::TieredMemoryManager>>,
    pub recovery: Option<Arc<pai_core::recovery::RecoveryJournal>>,
}

impl ReActAgent {
    pub fn new(
        ollama: Ollama,
        config: AgentConfig,
        tools: Arc<ToolRegistry>,
    ) -> Self {
        let provider = if let Some(ref url) = config.provider_url {
            Arc::new(OpenAICompatibleProvider::new(url.clone(), None)) as Arc<dyn LLMProvider>
        } else {
            Arc::new(OllamaProvider::new(ollama)) as Arc<dyn LLMProvider>
        };

        Self {
            provider,
            config,
            tools,
            memory: None,
            safety: None,
            pai_hooks: None,
            pai_memory: None,
            recovery: None,
        }
    }

    pub fn new_with_provider(
        provider: Arc<dyn LLMProvider>,
        config: AgentConfig,
        tools: Arc<ToolRegistry>,
    ) -> Self {
        Self {
            provider,
            config,
            tools,
            memory: None,
            safety: None,
            pai_hooks: None,
            pai_memory: None,
            recovery: None,
        }
    }

    pub fn with_recovery(mut self, recovery: Arc<pai_core::recovery::RecoveryJournal>) -> Self {
        self.recovery = Some(recovery);
        self
    }

    pub fn with_hooks(mut self, hooks: Arc<HookManager>) -> Self {
        self.pai_hooks = Some(hooks);
        self
    }

    pub fn with_memory_manager(mut self, memory: Arc<pai_core::memory::TieredMemoryManager>) -> Self {
        self.pai_memory = Some(memory);
        self
    }

    pub fn with_provider(mut self, provider: Arc<dyn LLMProvider>) -> Self {
        self.provider = provider;
        self
    }

    pub fn with_memory(mut self, memory: Arc<dyn Memory>) -> Self {
        self.memory = Some(memory);
        self
    }

    pub fn with_safety(mut self, safety: Arc<tokio::sync::Mutex<crate::safety::SafetyGuard>>) -> Self {
        self.safety = Some(safety);
        self
    }
}

#[async_trait]
impl SovereignAgent for ReActAgent {
    async fn create_task(&self, input: &str, commitment: Option<ValueCommitment>) -> anyhow::Result<UapTask> {
        let mut task = UapTask::new(input);
        
        // L5: Perform automatic alignment check during creation
        let audit = self.audit_alignment(input, false).await?;
        task.audit = Some(audit);
        
        task.commitment = commitment;
        Ok(task)
    }

    async fn propose_commitment(&self, input: &str) -> anyhow::Result<ValueCommitment> {
        // DSGM: Estimate economic impact before starting
        let mut impact = EconomicImpact::default();
        
        // SOTA Heuristics for Labor Decoupling
        let q = input.to_lowercase();
        if q.contains("refactor") || q.contains("architect") {
            impact.labor_decoupling_score = 8.0;
            impact.capital_generation_score = 0.9;
        } else if q.contains("fix") || q.contains("typo") {
            impact.labor_decoupling_score = 0.5;
            impact.capital_generation_score = 0.1;
        } else {
            impact.labor_decoupling_score = 2.0;
            impact.capital_generation_score = 0.4;
        }

        let mut commitment = ValueCommitment::new("pending", "agent-1", CommitmentModality::Aspirational);
        commitment.impact = impact;
        
        Ok(commitment)
    }

    async fn audit_alignment(&self, input: &str, override_lever: bool) -> anyhow::Result<AlignmentAudit> {
        let engine = AlignmentEngine::sovereign_defaults();
        let metadata = HashMap::new(); // In full impl, pass relevant context
        Ok(engine.audit(input, &metadata, override_lever))
    }

    async fn execute_step(&self, task_id: &str, input: Option<serde_json::Value>) -> anyhow::Result<UapStep> {
        let mut step = UapStep::new(task_id, "ReAct Step");
        let query = Self::query_from_step_input(input.as_ref());

        // L5 Check: Ensure we aren't blocked before executing
        let audit = self.audit_alignment(&query, false).await?;
        if audit.status == AuditStatus::Blocked {
            return Err(anyhow::anyhow!("BLOCKED: Action violates Sovereign Alignment Protocol. Pull the Sovereign Lever to override."));
        }

        step.status = UapStepStatus::Running;

        // Execute one iteration of the ReAct loop
        let res = self.execute(&query, None).await
            .map_err(|e| anyhow::anyhow!(e))?;

        step.output = Some(res.answer);
        step.status = UapStepStatus::Completed;
        step.is_last = true; // For now, we execute full turns as single UAP steps

        Ok(step)
    }

    async fn list_steps(&self, _task_id: &str) -> anyhow::Result<Vec<UapStep>> {
        Ok(vec![]) // Placeholder for persistence
    }

    async fn get_task(&self, _task_id: &str) -> anyhow::Result<UapTask> {
        Ok(UapTask::new("Not found")) // Placeholder for persistence
    }

    async fn list_artifacts(&self, _task_id: &str) -> anyhow::Result<Vec<UapArtifact>> {
        Ok(vec![])
    }
}

impl ReActAgent {
    fn query_from_step_input(input: Option<&serde_json::Value>) -> String {
        input
            .and_then(serde_json::Value::as_str)
            .unwrap_or("Continue with task")
            .to_string()
    }

    /// Build the ReAct prompt
    async fn build_react_prompt(&self, query: &str, steps: &[
ReActStep],
 context: Option<&str>) -> String {
        let mut prompt = String::new();

        if let Some(ctx) = context {
            prompt.push_str(&format!("## Context
{}

", ctx));
        }

        prompt.push_str("## Available Tools
");
        prompt.push_str("Standard Tools:\n");
        prompt.push_str(&self.tools.generate_filtered_tools_prompt(&self.config.allowed_tools).await);
        
        // SOTA: Laboratory Surface (FPF Principle)
        // Show dynamic tools that are currently in the 'laboratory'
        let lab_tools = self.tools.tool_names().await.into_iter()
            .filter(|n| !self.config.allowed_tools.contains(n) && n != "forge_tool")
            .collect::<Vec<_>>();
            
        if !lab_tools.is_empty() {
            prompt.push_str("\nLaboratory (Experimental) Tools:\n");
            prompt.push_str("NOTE: These tools are currently in the laboratory. Successful use will promote them to the standard set.\n");
            prompt.push_str(&self.tools.generate_filtered_tools_prompt(&lab_tools).await);
        }
        
        prompt.push_str("\n");

        if self.config.reasoning_enabled {
            prompt.push_str(r###"## Response Format (SNS-Core)

Respond using the EXACT symbolic format below. Use symbols to save tokens.

🧠 Identify strategy and next steps.
⚡ Break down logic or explain tool results.
→ {"name": "tool_name", "parameters": {"key": "value"}}
🎯 Your response to the user.

---
RULES:
1. Every turn MUST include 🧠 and ⚡.
2. Use → for tool calls (JSON format). Do NOT wrap JSON in code blocks.
3. Use 🎯 for final messages to the user.
4. NEVER output [OBSERVATION].

EXAMPLE OF TOOL CALL:
🧠 Check system status.
⚡ Evaluate resource availability.
→ {"name": "system_monitor", "parameters": {"action": "status"}}

"###);
        } else {
            prompt.push_str(r###"## Response Format (SNS-Core)
Respond directly. Use → for tool calls (JSON). Use 🎯 for final response.

RULES:
1. Use → for tool calls.
2. Use 🎯 for final response.
3. Keep it brief and direct.
"###);
        }

        prompt.push_str(&format!("## User Query
{}

", query));

        if !steps.is_empty() {
            prompt.push_str("## Trace
");
            for step in steps {
                if !step.thought.is_empty() {
                    prompt.push_str(&format!("⚡ {}\n", step.thought));
                }
                for action in &step.actions {
                    if let Ok(action_json) = serde_json::to_string(action) {
                        prompt.push_str(&format!("→ {}\n", action_json));
                    }
                }
                for obs in &step.observations {
                    prompt.push_str(&format!("👁️ {}\n", obs));
                }
                prompt.push_str("\n");
            }
        }

        prompt.push_str("Continue:\n");
        prompt
    }

    /// Parse the LLM response using strict Tags
    fn parse_response(&self, response: &str, _query: &str) -> AgentResult<ReActStep> {
        debug!("Raw LLM Response for parsing:\n{}", response);

        // HALLUCINATION GUARD: Truncate at [OBSERVATION] or 👁️
        let clean_response = if let Some(obs_idx) = response.find("👁️") {
            warn!("Hallucinated 👁️ detected. Truncating.");
            &response[..obs_idx]
        } else if let Some(obs_idx) = response.to_uppercase().find("[OBSERVATION]") {
            warn!("Hallucinated [OBSERVATION] detected. Truncating.");
            &response[..obs_idx]
        } else {
            response
        };

        // Try to extract REASONING or THOUGHT (Support ⚡ and 🧠)
        let thought = self.extract_tag(clean_response, "⚡")
            .or_else(|| self.extract_tag(clean_response, "🧠"))
            .or_else(|| self.extract_tag(clean_response, "[REASONING]"))
            .or_else(|| self.extract_tag(clean_response, "[THOUGHT]"))
            .unwrap_or_else(|| {
                "Executing task...".to_string()
            });

        // 1. Check for Actions (Support →)
        let actions = self.extract_all_tags(clean_response, "→");
        let legacy_actions = self.extract_all_tags(clean_response, "[ACTION]");
        
        let mut tool_calls = Vec::new();
        
        for action_str in actions.into_iter().chain(legacy_actions.into_iter()) {
            if let Some(call) = self.parse_json_tool_call(&action_str) {
                tool_calls.push(call);
            }
        }

        // 2. Fallback: Search for raw JSON objects
        if tool_calls.is_empty() {
            if let Some(call) = self.find_raw_json_tool_call(clean_response) {
                tool_calls.push(call);
            }
        }

        if !tool_calls.is_empty() {
            return Ok(ReActStep::thought(thought).with_actions(tool_calls));
        }

        // 3. Check for Answer (Support 🎯)
        if let Some(answer) = self.extract_tag(clean_response, "🎯").or_else(|| self.extract_tag(clean_response, "[ANSWER]")) {
            return Ok(ReActStep::final_answer(thought, answer));
        }

        // 4. Robustness Fallback: FPF Compliance
        if clean_response.trim().is_empty() && response.to_uppercase().contains("[OBSERVATION]") {
            warn!("Model started with hallucinated [OBSERVATION]. Retrying turn...");
            return Ok(ReActStep::thought("System detected a hallucinated observation. Please provide your reasoning and next action or final answer using the specified tags."));
        }

        // SOTA: Intelligent Fallback
        if !clean_response.trim().is_empty() {
            let reliability = self.score_response_quality(clean_response);
            if reliability < 0.2 {
                warn!("Low reliability (R={:.2}) detected in tagless response. Rejecting as gibberish.", reliability);
                return Ok(ReActStep::thought("The model provided an incoherent response. Retrying with stricter instructions..."));
            }

            info!("Model provided tagless response (R={:.2}). Treating as final answer for FPF compliance.", reliability);
            return Ok(ReActStep::final_answer("Executing task...", clean_response.trim()));
        }

        warn!("Model failed to provide tags or content. Wrapping raw response.");
        Ok(ReActStep::final_answer("Conversational response", clean_response))
    }

    /// Normalize steps to ensure every action has an observation and remove orphans.
    /// Derived from codex-rs/normalize.rs
    fn normalize_steps(&self, steps: &mut Vec<ReActStep>) {
        let mut i = 0;
        while i < steps.len() {
            let step = &mut steps[i];
            
            // 1. Ensure action steps have observations. If not, add 'aborted' observation.
            if !step.actions.is_empty() && step.observations.is_empty() && !step.is_final {
                warn!("Found step with actions but no observations. Adding synthetic 'aborted' observation.");
                step.observations.push("Task was aborted or interrupted before tool execution completed.".to_string());
            }
            
            // 2. Remove orphan observations (observations without preceding actions in same step)
            if step.actions.is_empty() && !step.observations.is_empty() {
                warn!("Removing orphan observations from step {}", i);
                step.observations.clear();
            }
            
            i += 1;
        }
    }

    /// FPF Quality Scoring: Detect hallucinations and repetitive patterns
    fn score_response_quality(&self, response: &str) -> f32 {
        score_response_quality_with_profile(response, &REACT_QUALITY_PROFILE)
    }

    fn find_raw_json_tool_call(&self, text: &str) -> Option<ToolCall> {
        // Look for common markers like "ACTION:" or just raw JSON
        let markers = ["ACTION:", "Action:", "Call tool:", "Execute:"];
        for marker in markers {
            if let Some(start_idx) = text.find(marker) {
                if let Some(call) = self.parse_json_tool_call(&text[start_idx..]) {
                    return Some(call);
                }
            }
        }
        
        // Final attempt: scan for any JSON-like object that looks like a ToolCall
        self.parse_json_tool_call(text)
    }

    fn parse_json_tool_call(&self, text: &str) -> Option<ToolCall> {
        if let Some(json_start) = text.find('{') {
            let json_text = &text[json_start..];
            // Find matching brace
            let mut depth = 0;
            let mut json_end = 0;
            for (i, c) in json_text.chars().enumerate() {
                match c {
                    '{' => depth += 1,
                    '}' => {
                        depth -= 1;
                        if depth == 0 {
                            json_end = i + 1;
                            break;
                        }
                    }
                    _ => {} // Ignore other characters
                }
            }
            if json_end > 0 {
                let action_json = &json_text[..json_end];
                if let Ok(call) = serde_json::from_str::<ToolCall>(action_json) {
                    // Basic validation that it's a real tool call
                    if !call.name.is_empty() {
                        return Some(call);
                    }
                }
            }
        }
        None
    }

    fn extract_tag(&self, text: &str, tag: &str) -> Option<String> {
        // Robust search for [TAG], [TAG]:, TAG:, **TAG**: (common in small models)
        let tag_name = tag.trim_matches(|c| c == '[' || c == ']');
        let patterns = [
            format!("[{}]", tag_name.to_uppercase()),
            format!("[{}]:", tag_name.to_uppercase()),
            format!("{}:", tag_name.to_uppercase()),
            format!("**{}**:", tag_name.to_uppercase()),
            format!("**{}**", tag_name.to_uppercase()),
            format!("### {}", tag_name.to_uppercase()),
            tag.to_string(), // Direct support for emojis like 🧠
        ];

        let text_upper = text.to_uppercase();
        
        for pattern in patterns {
            if let Some(start_idx) = text.find(&pattern) {
                let start = start_idx + pattern.len();
                
                // Look for the next possible tag to find the end
                let next_tags = [
                    "[PLANNING]", "[REASONING]", "[THOUGHT]", "[ACTION]", "[ANSWER]", "[OBSERVATION]", 
                    "PLANNING:", "REASONING:", "THOUGHT:", "ACTION:", "ANSWER:",
                    "**PLANNING**", "**REASONING**", "**THOUGHT**", "**ACTION**", "**ANSWER**",
                    "🧠", "⚡", "→", "🎯", "👁️"
                ];
                let mut end = text.len();
                
                for t in next_tags {
                    if let Some(next_idx) = text[start..].find(t) {
                        let abs_next_idx = start + next_idx;
                        if abs_next_idx < end {
                            end = abs_next_idx;
                        }
                    }
                }
                
                let result = text[start..end].trim().trim_start_matches(':').trim().to_string();
                if !result.is_empty() { return Some(result); }
            }
        }
        None
    }

    fn extract_all_tags(&self, text: &str, tag: &str) -> Vec<String> {
        let mut results = Vec::new();
        let tag_name = tag.trim_matches(|c| c == '[' || c == ']');
        let patterns = [
            format!("[{}]", tag_name.to_uppercase()),
            format!("{}:", tag_name.to_uppercase()),
            format!("**{}**:", tag_name.to_uppercase()),
            tag.to_string(),
        ];
        
        // We use the first pattern that matches to find all occurrences
        for pattern in patterns {
            let mut pos = 0;
            while let Some(start_idx) = text[pos..].find(&pattern) {
                let start = pos + start_idx + pattern.len();
                
                // Find end (next tag or end of string)
                let next_tags = [
                    "[PLANNING]", "[REASONING]", "[THOUGHT]", "[ACTION]", "[ANSWER]", "[OBSERVATION]",
                    "PLANNING:", "REASONING:", "THOUGHT:", "ACTION:", "ANSWER:",
                    "**PLANNING**", "**REASONING**", "**THOUGHT**", "**ACTION**", "**ANSWER**",
                    "🧠", "⚡", "→", "🎯", "👁️"
                ];
                let mut end = text.len();
                
                for t in next_tags {
                    if let Some(next_idx) = text[start..].find(t) {
                        let abs_next_idx = start + next_idx;
                        if abs_next_idx < end {
                            end = abs_next_idx;
                        }
                    }
                }
                
                let result = text[start..end].trim().trim_start_matches(':').trim().to_string();
                if !result.is_empty() {
                    results.push(result);
                }
                pos = end;
                if pos >= text.len() { break; }
            }
            if !results.is_empty() { break; }
        }
        results
    }

    /// Execute a single step of the ReAct loop with streaming
    pub async fn step_stream(&self, query: &str, steps: &[ReActStep], context: Option<&str>) -> AgentResult<ReActStep> {
        let prompt = self.build_react_prompt(query, steps, context).await;
        let system = Some(self.config.system_prompt.clone());
        
        debug!("ReAct prompt (streaming):\n{}", prompt);
        info!("   ⏳ Iteration starting (model: {})...", self.config.model);

        let mut stream = self.provider.generate_stream(&self.config.model, prompt, system).await
            .map_err(|e| AgentError::Provider(e.to_string()))?;
        let mut full_content = String::new();

        while let Some(chunk_res) = stream.next().await {
            let chunk = chunk_res.map_err(|e| AgentError::Provider(e.to_string()))?;
            full_content.push_str(&chunk);
            // SOTA: No token-by-token printing to stdout to avoid IO bottlenecks.
            // Tokens are streamed to the UI via the provider's internal tx channel.
        }

        debug!("Full streamed response:\n{}", full_content);

        self.parse_response(&full_content, query)
    }

    /// Execute a single step of the ReAct loop
    pub async fn step(&self, query: &str, steps: &[
ReActStep],
 context: Option<&str>) -> AgentResult<ReActStep> {
        let prompt = self.build_react_prompt(query, steps, context).await;
        let system = Some(self.config.system_prompt.clone());
        
        debug!("ReAct prompt:\n{}", prompt);

        let content = self.provider.generate(&self.config.model, prompt, system).await
            .map_err(|e| AgentError::Provider(e.to_string()))?;

        debug!("LLM response:\n{}", content);

        self.parse_response(&content, query)
    }
}

#[async_trait]
impl Agent for ReActAgent {
    fn agent_type(&self) -> AgentType {
        self.config.agent_type
    }

    fn name(&self) -> &str {
        match self.config.agent_type {
            AgentType::GeneralChat => "GeneralChat",
            AgentType::Reasoner => "Reasoner",
            AgentType::Coder => "Coder",
            AgentType::Researcher => "Researcher",
            AgentType::Planner => "Planner",
            AgentType::Reviewer => "Reviewer",
        }
    }

    fn system_prompt(&self) -> &str {
        &self.config.system_prompt
    }

    fn model(&self) -> &str {
        &self.config.model
    }

    async fn execute(&self, query: &str, context: Option<&str>) -> AgentResult<AgentResponse> {
        self.execute_with_steering(query, context, None).await
    }
}

impl ReActAgent {
    pub async fn execute_with_steering(
        &self, 
        query: &str, 
        context: Option<&str>,
        mut steering_rx: Option<tokio::sync::mpsc::Receiver<String>>
    ) -> AgentResult<AgentResponse> {
        info!("ReAct agent starting execution for query: {}", query);
        
        let mut steps = Vec::new();
        
        for iteration in 0..self.config.max_iterations {
            debug!("ReAct iteration {}", iteration + 1);
            
            // Check for steering messages BEFORE the turn
            if let Some(ref mut rx) = steering_rx {
                while let Ok(steer_msg) = rx.try_recv() {
                    info!("Agent steered: {}", steer_msg);
                    let _ = self.provider.notify(&format!("\n🔄 STEERING RECEIVED: {}\n", steer_msg)).await;
                    steps.push(ReActStep::thought(format!("[STEERED]: {}", steer_msg)));
                }
            }

            let _ = self.provider.notify("STATE:THOUGHT_START").await;
            let _ = self.provider.notify(&format!("STATE:MODEL:{}", self.config.model)).await;
            let _ = self.provider.notify(&format!("\n[ITERATION {}]\n", iteration + 1)).await;
            
            let mut step = match self.step_stream(query, &steps, context).await {
                Ok(s) => {
                    for action in &s.actions {
                        let msg = format!("🔧 Using Tool: {}...", action.name);
                        println!("      {}", msg);
                        let _ = self.provider.notify(&format!("\n{}\n", msg)).await;
                        crate::emit_event!(crate::orchestrator::AgencyEvent::ToolCallStarted { tool: action.name.clone() });
                    }
                    s
                },
                Err(e) => {
                    warn!("ReAct step parsing failed: {}", e);
                    let _ = self.provider.notify(&format!("\n❌ Parsing error: {}\n", e)).await;
                    steps.push(ReActStep::thought(format!("Parsing error: {}", e)));
                    return Ok(AgentResponse::failure(e.to_string(), steps, self.config.agent_type));
                }
            };

            // LAZINESS FILTER: Detect finishing without action for complex queries
            if step.is_final && steps.is_empty() && is_action_query(query) {
                warn!("Laziness detected: Agent tried to finish without any tool calls for an action query.");
                let hint = "SYSTEM HINT: Your query requires ACTION (creating, analyzing, searching). You MUST use tools first. Do NOT provide a final answer until you have observations from the required tools.";
                
                // Convert current final answer back to a thought and continue
                step.is_final = false;
                step.thought = format!("{} [REJECTED: No tool used. I must use tools.]", step.thought);
                
                let mut hint_step = step.clone();
                hint_step.observations.push(hint.to_string());
                steps.push(hint_step);
                continue;
            }
            
            if step.is_final {
                let answer = step.answer.clone().unwrap_or_else(|| step.thought.clone());
                steps.push(step);
                
                // SOTA: Trace Normalization (FPF Principle)
                self.normalize_steps(&mut steps);
                
                info!("ReAct agent completed in {} iterations", iteration + 1);
                return Ok(AgentResponse::success(answer, steps, self.config.agent_type));
            }

            if !step.actions.is_empty() {
                // SOTA: Human-in-the-Loop (HITL) Check (FPF Principle: Verifiable Autonomy)
                if let Some(ref safety_mutex) = self.safety {
                    let guard = safety_mutex.lock().await;
                    for action in &step.actions {
                        if let Some(request) = guard.needs_human_approval(&action.name, &action.parameters, self.tools.clone()).await {
                            info!("🚨 HITL triggered for tool: {}. Pausing execution for approval.", action.name);
                            let _ = self.provider.notify(&format!("\n🚨 HITL REQUIRED: {}\n", request.rationale)).await;
                            
                            steps.push(step);
                            self.normalize_steps(&mut steps);
                            
                            return Ok(AgentResponse::success("Awaiting human approval for sensitive operation.", steps, self.config.agent_type)
                                .with_approval(request));
                        }
                    }
                }

                // Loop Guard: Check for redundant tool calls
                if let Some(last_step) = steps.last() {
                    if last_step.actions == step.actions {
                        warn!("Redundant tool calls detected. Injecting loop guard hint.");
                        let mut loop_guard_step = step.clone();
                        loop_guard_step.observations = vec!["SYSTEM HINT: Redundant tool call detected. Try a different approach or provide a final answer.".to_string()];
                        steps.push(loop_guard_step);
                        continue;
                    }
                }

                // Context Compression: If we have too many steps, summarize old ones
                if steps.len() > 5 {
                    info!("Trace compression active ({} steps). Summarizing early history.", steps.len());
                    // Simple compression: Keep first step and last 3 steps, replace middle with summary
                    let first = steps[0].clone();
                    let last_three = steps[steps.len()-3..].to_vec();
                    let mut compressed = vec![first];
                    compressed.push(ReActStep::thought("[SYSTEM: Early history summarized to save context tokens]"));
                    compressed.extend(last_three);
                    steps = compressed;
                }

                debug!("Executing {} tools in parallel", step.actions.len());
                
                // PAI: Trigger PreToolUse Hooks
                if let Some(ref hm) = self.pai_hooks {
                    for action in &step.actions {
                        let mut event = HookEvent {
                            event_type: HookEventType::PreToolUse,
                            session_id: "agent-session".to_string(), // In a real system this would be passed down
                            payload: serde_json::json!({
                                "tool_name": action.name,
                                "tool_input": action.parameters,
                                "description": action.name // Temporary fallback for description matching
                            }),
                            timestamp: chrono::Utc::now(),
                        };
                        
                        pai_core::enrichment::EnrichmentEngine::enrich(&mut event);

                        match hm.trigger(&event).await {
                            Ok(HookAction::Block(reason)) => {
                                warn!("PAI Blocked tool {}: {}", action.name, reason);
                                let mut blocked_steps = steps.clone();
                                blocked_steps.push(ReActStep::thought(format!("SECURITY BLOCKED: {}", reason)));
                                
                                // Log the blocked event
                                if let Some(ref mem) = self.pai_memory {
                                    let mut blocked_event = event.clone();
                                    blocked_event.payload["security_action"] = serde_json::json!("block");
                                    blocked_event.payload["security_reason"] = serde_json::json!(reason);
                                    let _ = mem.log_event(&blocked_event);
                                }

                                return Ok(AgentResponse::failure(format!("Security Block: {}", reason), blocked_steps, self.config.agent_type));
                            },
                            Ok(_) => {
                                // Log the allowed event
                                if let Some(ref mem) = self.pai_memory {
                                    let _ = mem.log_event(&event);
                                }

                                // PAI: Trigger Recovery Snapshot for destructive tools
                                if action.name == "Edit" || action.name == "Write" {
                                    if let (Some(ref rec), Some(path)) = (&self.recovery, action.parameters["path"].as_str()) {
                                        let _ = rec.snapshot(std::path::Path::new(path));
                                    }
                                }
                            }
                            Err(e) => warn!("PAI Hook error: {}", e),
                        }
                    }
                }

                for action in &step.actions {
                    crate::emit_event!(crate::orchestrator::AgencyEvent::ToolCallStarted { 
                        tool: action.name.clone() 
                    });
                }

                let results = self.tools.execute_parallel(&step.actions).await;
                
                let mut observations = Vec::new();
                for (i, res) in results.into_iter().enumerate() {
                    let action = &step.actions[i];
                    let mut obs = match res {
                        Ok(output) => {
                            crate::emit_event!(crate::orchestrator::AgencyEvent::ToolCallFinished { 
                                tool: action.name.clone(), 
                                success: true 
                            });
                            let _ = self.provider.notify(&format!("\n👁️ Observation: {}\n", output.summary)).await;
                            
                            // SOTA: Tool Promotion (Laboratory graduation)
                            let _ = self.tools.promote_tool(&action.name).await;
                            
                            // SOTA: TOON Data Optimization (FPF Principle: Token Sovereignty)
                            // If the tool data is complex (not just summary), use TOON notation.
                            if output.data.is_object() || output.data.is_array() {
                                crate::utils::toon::ToonFormatter::format(&output.data)
                            } else {
                                output.summary
                            }
                        },
                        Err(e) => {
                            crate::emit_event!(crate::orchestrator::AgencyEvent::ToolCallFinished { 
                                tool: action.name.clone(), 
                                success: false 
                            });
                            let _ = self.provider.notify(&format!("\n❌ Tool failed: {}\n", e)).await;
                            format!("Tool execution failed: {}", e)
                        },
                    };
                    
                    // Context Compression: Truncate tool outputs if they are too long
                    use crate::utils::truncate::{truncate_text, TruncationPolicy};
                    obs = truncate_text(&obs, TruncationPolicy::Bytes(1500));
                    observations.push(obs);
                }

                let step_with_obs = ReActStep {
                    thought: step.thought.clone(),
                    actions: step.actions.clone(),
                    observations,
                    is_final: false,
                    answer: None,
                };
                steps.push(step_with_obs);
            } else {
                steps.push(step);
            }
        }

        // SOTA: Trace Normalization (FPF Principle)
        self.normalize_steps(&mut steps);

        Ok(AgentResponse::failure(
            format!("Reached maximum iterations ({})", self.config.max_iterations),
            steps,
            self.config.agent_type,
        ))
    }
}

/// Simple agent for direct conversation without ReAct loop
#[derive(Clone)]
pub struct SimpleAgent {
    provider: Arc<dyn LLMProvider>,
    config: AgentConfig,
}

impl SimpleAgent {
    pub fn new(ollama: Ollama, config: AgentConfig) -> Self {
        let provider = if let Some(ref url) = config.provider_url {
            Arc::new(OpenAICompatibleProvider::new(url.clone(), None)) as Arc<dyn LLMProvider>
        } else {
            Arc::new(OllamaProvider::new(ollama)) as Arc<dyn LLMProvider>
        };

        Self { provider, config }
    }

    pub fn new_with_provider(provider: Arc<dyn LLMProvider>, config: AgentConfig) -> Self {
        Self { provider, config }
    }

    pub fn with_provider(mut self, provider: Arc<dyn LLMProvider>) -> Self {
        self.provider = provider;
        self
    }

    pub async fn execute_simple(&self, query: &str, context: Option<&str>) -> AgentResult<AgentResponse> {
        let mut prompt = String::new();
        let system = Some(self.config.system_prompt.clone());
        
        // FPF-SOTA (January 2026): Multi-View Publication with SLL/BLP
        prompt.push_str("<|im_start|>system\nYou are a high-fidelity intelligence layer. \
            Follow the Standard Notation System (SNS): Use symbols to minimize tokens. \
            Follow the First Principles Framework (FPF): \
            1. BLP (Bitter-Lesson Preference): Prefer general, scale-amenable solutions. \
            2. SLL (Scaling-Law Lens): Identify scale variables (S) and χ elasticity. \
            Use ⚡ for internal thought and 🎯 for the final user response.<|im_end|>\n");

        if let Some(ctx) = context {
            prompt.push_str(ctx);
            prompt.push_str("\n");
        }
        prompt.push_str(&format!("<|im_start|>user\n{}<|im_end|>\n<|im_start|>assistant\n⚡\n", query));

        debug!("Simple conversational prompt:\n{}", prompt);

        let _ = self.provider.notify(&format!("STATE:MODEL:{}", self.config.model)).await;

        let content = self.provider.generate(&self.config.model, prompt, system).await
            .map_err(|e| AgentError::Provider(e.to_string()))?;
        
        // MVPK Projection: Extract Thought (TechView) and Answer (PlainView)
        let mut thought = "Processing...".to_string();
        let mut answer = content.clone();

        if let Some(t) = self.extract_tag(&content, "⚡")
            .or_else(|| self.extract_tag(&content, "[THOUGHT]"))
            .or_else(|| self.extract_tag(&content, "THOUGHT:")) {
            thought = t;
        }
        
        if let Some(a) = self.extract_tag(&content, "🎯")
            .or_else(|| self.extract_tag(&content, "[ANSWER]"))
            .or_else(|| self.extract_tag(&content, "ANSWER:")) {
            answer = a;
        }

        // FPF Consistency: If no tags found but substantial text exists, treat as raw answer
        if answer.len() < 2 && content.len() > 2 {
            answer = content;
        }

        // CG-Spec: Formality (F) and Reliability (R) calculation
        let reliability = self.score_response_quality(&answer);
        
        if reliability < 0.3 {
            warn!("Low reliability (R={:.2}) detected. Applying FPF Truncation Guard.", reliability);
            if let Some(truncate_idx) = self.find_repetition_point(&answer) {
                answer = answer[..truncate_idx].to_string();
            }
        }

        let step = ReActStep::final_answer(thought.clone(), &answer);
        Ok(AgentResponse::success(answer, vec![step], self.config.agent_type)
            .with_thought(thought)
            .with_reliability(reliability))
    }

    /// FPF Quality Scoring: Detect hallucinations and repetitive patterns
    fn score_response_quality(&self, response: &str) -> f32 {
        score_response_quality_with_profile(response, &SIMPLE_QUALITY_PROFILE)
    }
    
    /// Find the first point where content starts repeating
    fn find_repetition_point(&self, response: &str) -> Option<usize> {
        find_prompt_leak_point(response)
    }

    /// Streaming variant of execute_simple for real-time token output
    /// Calls the callback with each token chunk as it arrives
    pub async fn execute_simple_stream<F>(
        &self, 
        query: &str, 
        context: Option<&str>,
        mut on_token: F
    ) -> AgentResult<AgentResponse> 
    where
        F: FnMut(&str) + Send
    {
        let mut prompt = String::new();
        let system = Some(self.config.system_prompt.clone());
        
        // FPF multi-view publication header
        prompt.push_str("<|im_start|>system\nYou are a high-fidelity intelligence layer. \
            Follow the First Principles Framework (FPF): ALWAYS start with [THOUGHT] to process, then [ANSWER] for the user.<|im_end|>\n");

        if let Some(ctx) = context {
            // Inject context directly (assumed ChatML formatted)
            prompt.push_str(ctx);
            prompt.push_str("\n");
        }
        prompt.push_str(&format!("<|im_start|>user\n{}<|im_end|>\n<|im_start|>assistant\n[THOUGHT]\n", query));

        debug!("Simple conversational prompt (streaming):\n{}", prompt);

        let _ = self.provider.notify(&format!("STATE:MODEL:{}", self.config.model)).await;

        // Use streaming generation
        let mut stream = self.provider.generate_stream(&self.config.model, prompt, system).await
            .map_err(|e| AgentError::Provider(e.to_string()))?;
        let mut full_response = String::new();
        
        while let Some(chunk_result) = stream.next().await {
            if let Ok(chunk) = chunk_result {
                on_token(&chunk);
                full_response.push_str(&chunk);
            }
        }

        // Final FPF Scoring on the full trace
        let reliability = self.score_response_quality(&full_response);
        let mut thought = "Streaming...".to_string();
        let mut answer = full_response.clone();

        if let Some(t) = self.extract_tag(&full_response, "[THOUGHT]") { thought = t; }
        if let Some(a) = self.extract_tag(&full_response, "[ANSWER]") { answer = a; }

        let step = ReActStep::final_answer(thought.clone(), &answer);
        Ok(AgentResponse::success(answer, vec![step], self.config.agent_type)
            .with_thought(thought)
            .with_reliability(reliability))
    }

    fn extract_tag(&self, text: &str, tag: &str) -> Option<String> {
        let tag_name = tag.trim_matches(|c| c == '[' || c == ']');
        let patterns = [
            format!("[{}]", tag_name.to_uppercase()),
            format!("[{}]:", tag_name.to_uppercase()),
            format!("{}:", tag_name.to_uppercase()),
            format!("**{}**:", tag_name.to_uppercase()),
            format!("**{}**", tag_name.to_uppercase()),
            format!("### {}", tag_name.to_uppercase()),
        ];

        let text_upper = text.to_uppercase();
        for pattern in patterns {
            if let Some(start_idx) = text_upper.find(&pattern) {
                let start = start_idx + pattern.len();
                let next_tags = [
                    "[THOUGHT]", "[ANSWER]", "THOUGHT:", "ANSWER:", 
                    "**THOUGHT**", "**ANSWER**", "### THOUGHT", "### ANSWER"
                ];
                let mut end = text.len();
                
                for t in next_tags {
                    if let Some(next_idx) = text_upper[start..].find(t) {
                        let abs_next_idx = start + next_idx;
                        if abs_next_idx < end {
                            end = abs_next_idx;
                        }
                    }
                }
                
                let result = text[start..end].trim().trim_start_matches(':').trim().to_string();
                if !result.is_empty() { return Some(result); }
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::orchestrator::profile::AgencyProfile;
    use serde_json::json;

    #[test]
    fn test_extract_tag() {
        let profile = AgencyProfile::default();
        let config = AgentConfig::new(AgentType::GeneralChat, &profile);
        let agent = ReActAgent::new(Ollama::default(), config, Arc::new(ToolRegistry::default()));
        
        let response = "[THOUGHT]\nI should check the weather.\n[ACTION]\n{\"name\": \"get_weather\", \"parameters\": {\"location\": \"Seattle\"}}\n";
        
        let thought = agent.extract_tag(response, "[THOUGHT]");
        assert_eq!(thought.expect("Failed to extract thought"), "I should check the weather.");
        
        let action = agent.extract_tag(response, "[ACTION]");
        assert_eq!(action.expect("Failed to extract action"), "{\"name\": \"get_weather\", \"parameters\": {\"location\": \"Seattle\"}}");
    }

    #[test]
    fn test_query_from_step_input_uses_string_value() {
        let input = json!("Refactor auth module");
        let query = ReActAgent::query_from_step_input(Some(&input));
        assert_eq!(query, "Refactor auth module");
    }

    #[test]
    fn test_query_from_step_input_falls_back_for_non_string() {
        let input = json!({ "task": "Refactor auth module" });
        let query = ReActAgent::query_from_step_input(Some(&input));
        assert_eq!(query, "Continue with task");
    }

    #[test]
    fn test_query_from_step_input_falls_back_for_none() {
        let query = ReActAgent::query_from_step_input(None);
        assert_eq!(query, "Continue with task");
    }

    #[test]
    fn test_quality_profile_keeps_agent_specific_behavior() {
        let leaked = "## User Query\nrepeat repeat repeat repeat repeat repeat repeat repeat";
        let react_score = score_response_quality_with_profile(leaked, &REACT_QUALITY_PROFILE);
        let simple_score = score_response_quality_with_profile(leaked, &SIMPLE_QUALITY_PROFILE);
        assert!(react_score <= 0.1);
        assert!(simple_score <= 0.1);
    }

    #[test]
    fn test_find_prompt_leak_point_finds_chatml_marker() {
        let response = "clean prefix <|im_start|>user leaked prompt";
        let idx = find_prompt_leak_point(response);
        assert_eq!(idx, response.find("<|im_start|>user"));
    }
}
