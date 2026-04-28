//! Router - Query routing to appropriate agents
//! 
//! Determines which agent should handle a given query.

use anyhow::Result;
use ollama_rs::Ollama;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, OnceLock};
use tracing::info;

use crate::agent::{AgentType, LLMProvider, OllamaProvider, OpenAICompatibleProvider};
use crate::orchestrator::ScaleProfile;

const GREETINGS: &[&str] = &[
    "hi",
    "hello",
    "hey",
    "howdy",
    "greetings",
    "good morning",
    "good afternoon",
    "good evening",
];
const IDENTITY_KEYWORDS: &[&str] = &[
    "who are you",
    "what is your name",
    "what are you",
    "your identity",
    "your name",
];
const FILESYSTEM_KEYWORDS: &[&str] = &[
    "list",
    "folder",
    "directory",
    "file",
    "ls",
    "dir",
    "tree",
    "structure",
    "show files",
    "show folders",
    "what is in",
    "contents of",
    "read ",
];
const CODE_KEYWORDS: &[&str] = &[
    "code",
    "function",
    "program",
    "script",
    "bug",
    "error",
    "compile",
    "debug",
    "implement",
    "algorithm",
    "class",
    "method",
    "variable",
    "rust",
    "python",
    "javascript",
    "typescript",
    "java",
    "c++",
    "golang",
    "write a",
    "create a",
    "fix the",
    "refactor",
];
const PLANNING_KEYWORDS: &[&str] = &[
    "plan",
    "schedule",
    "steps",
    "how to",
    "break down",
    "organize",
    "roadmap",
    "workflow",
    "process",
    "strategy",
    "goal",
    "milestone",
];
const RESEARCH_KEYWORDS: &[&str] = &[
    "search",
    "find",
    "look up",
    "research",
    "latest",
    "current",
    "news",
    "information about",
    "tell me about",
];
const TOOL_VERBS: &[&str] = &["use ", "run ", "execute ", "invoke ", "call "];
const TOOL_NAMES: &[&str] = &["speaker", "search", "shell", "browser", "file", "terminal"];

/// Routing decision for a query
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutingDecision {
    /// FPF Integration: Multi-Candidate Portfolio (G.5)
    pub candidate_agents: Vec<AgentType>,
    /// Whether to search memory for context
    pub should_search_memory: bool,
    /// Whether strict reasoning/planning tags are required
    pub reasoning_required: bool,
    /// Confidence in the routing decision (0.0 - 1.0)
    pub confidence: f32,
    /// Reason for the routing decision
    pub reason: String,
    /// FPF Integration: Scaling-Law Lens (C.18.1)
    pub scale: ScaleProfile,
}

/// Router for directing queries to appropriate agents
#[derive(Clone)]
pub struct Router {
    provider: Arc<dyn LLMProvider>,
    model: String,
}

impl Router {
    fn normalize_keywords(keywords: &[&str]) -> Vec<String> {
        keywords
            .iter()
            .map(|keyword| Self::normalize_for_matching(keyword))
            .filter(|keyword| !keyword.is_empty())
            .collect()
    }

    fn json_value_ci<'a>(value: &'a serde_json::Value, key: &str) -> Option<&'a serde_json::Value> {
        let upper_key = key.to_ascii_uppercase();
        value.get(key).or_else(|| value.get(&upper_key))
    }

    fn json_field_ci<'a>(value: &'a serde_json::Value, key: &str) -> Option<&'a str> {
        Self::json_value_ci(value, key).and_then(|entry| entry.as_str())
    }

    fn json_bool_ci(value: &serde_json::Value, key: &str) -> Option<bool> {
        Self::json_value_ci(value, key).and_then(|entry| entry.as_bool())
    }

    fn llm_decision(agent: AgentType, should_search_memory: bool, reason: String) -> RoutingDecision {
        RoutingDecision {
            candidate_agents: vec![agent],
            should_search_memory,
            reasoning_required: true, // LLM-routed queries are usually complex
            confidence: 0.7,
            reason,
            scale: ScaleProfile::new(0.5, 8.0), // Placeholder, will be updated by caller
        }
    }

    fn parse_json_routing(response: &str) -> Option<RoutingDecision> {
        let start = response.find('{')?;
        let end = response.rfind('}')?;
        let json_str = &response[start..=end];
        let v = serde_json::from_str::<serde_json::Value>(json_str).ok()?;

        let agent_str = Self::json_field_ci(&v, "agent")
            .map(|s| s.to_lowercase())
            .unwrap_or_else(|| "reasoner".to_string());
        let agent = Self::parse_agent_label(agent_str.as_str());

        let should_search_memory = Self::json_bool_ci(&v, "memory").unwrap_or_else(|| {
            Self::json_field_ci(&v, "memory")
                .map(|s| matches!(s.to_ascii_lowercase().as_str(), "yes" | "true"))
                .unwrap_or(false)
        });

        let reason = Self::json_field_ci(&v, "reason")
            .unwrap_or("LLM routing decision")
            .to_string();

        Some(Self::llm_decision(agent, should_search_memory, reason))
    }

    fn parse_regex_routing(response: &str) -> RoutingDecision {
        // Fallback for non-JSON outputs.
        static AGENT_RE: OnceLock<Regex> = OnceLock::new();
        static MEMORY_RE: OnceLock<Regex> = OnceLock::new();
        static REASON_RE: OnceLock<Regex> = OnceLock::new();
        let agent_re = AGENT_RE.get_or_init(|| {
            Regex::new(r"(?i)AGENT:\s*(\w+)").expect("AGENT regex literal must be valid")
        });
        let memory_re = MEMORY_RE.get_or_init(|| {
            Regex::new(r"(?i)MEMORY:\s*(yes|no)").expect("MEMORY regex literal must be valid")
        });
        let reason_re = REASON_RE.get_or_init(|| {
            Regex::new(r"(?i)REASON:\s*(.+)").expect("REASON regex literal must be valid")
        });

        let agent_str = agent_re
            .captures(response)
            .and_then(|c| c.get(1))
            .map(|m| m.as_str().to_lowercase())
            .unwrap_or_else(|| "reasoner".to_string());
        let agent = Self::parse_agent_label(agent_str.as_str());

        let should_search_memory = memory_re
            .captures(response)
            .and_then(|c| c.get(1))
            .map(|m| m.as_str().eq_ignore_ascii_case("yes"))
            .unwrap_or(false);

        let reason = reason_re
            .captures(response)
            .and_then(|c| c.get(1))
            .map(|m| m.as_str().trim().to_string())
            .unwrap_or_else(|| "LLM routing decision".to_string());

        Self::llm_decision(agent, should_search_memory, reason)
    }

    fn normalize_for_matching(input: &str) -> String {
        let mut normalized = String::with_capacity(input.len());
        let mut last_was_space = true;

        for ch in input.chars().flat_map(char::to_lowercase) {
            if ch.is_alphanumeric() {
                normalized.push(ch);
                last_was_space = false;
            } else if !last_was_space {
                normalized.push(' ');
                last_was_space = true;
            }
        }

        if normalized.ends_with(' ') {
            normalized.pop();
        }

        normalized
    }

    fn contains_phrase(normalized_query: &str, normalized_phrase: &str) -> bool {
        if normalized_phrase.is_empty() {
            return false;
        }

        if normalized_query == normalized_phrase {
            return true;
        }

        for (idx, _) in normalized_query.match_indices(normalized_phrase) {
            let start_ok = idx == 0 || normalized_query[..idx].ends_with(' ');
            if !start_ok {
                continue;
            }

            let end = idx + normalized_phrase.len();
            let end_ok = end == normalized_query.len() || normalized_query[end..].starts_with(' ');
            if end_ok {
                return true;
            }
        }

        false
    }

    fn contains_any_cached(
        normalized_query: &str,
        keywords: &[&str],
        cache: &OnceLock<Vec<String>>,
    ) -> bool {
        let normalized_keywords = cache.get_or_init(|| Self::normalize_keywords(keywords));
        normalized_keywords
            .iter()
            .any(|keyword| Self::contains_phrase(normalized_query, keyword))
    }

    fn contains_any_normalized(normalized_query: &str, keywords: &[&str]) -> bool {
        keywords
            .iter()
            .any(|keyword| Self::contains_phrase(normalized_query, &Self::normalize_for_matching(keyword)))
    }

    fn parse_agent_label(agent: &str) -> AgentType {
        match agent {
            "general_chat" | "generalchat" | "chat" => AgentType::GeneralChat,
            "coder" | "programmer" | "developer" => AgentType::Coder,
            "researcher" | "research" => AgentType::Researcher,
            "planner" | "planning" => AgentType::Planner,
            _ => AgentType::Reasoner,
        }
    }

    fn quick_decision(
        agent: AgentType,
        should_search_memory: bool,
        reasoning_required: bool,
        confidence: f32,
        reason: &str,
        scale: ScaleProfile,
    ) -> RoutingDecision {
        RoutingDecision {
            candidate_agents: vec![agent],
            should_search_memory,
            reasoning_required,
            confidence,
            reason: reason.to_string(),
            scale,
        }
    }

    fn has_url_hint(q_lower: &str) -> bool {
        q_lower.contains("http://")
            || q_lower.contains("https://")
            || q_lower.contains(".com")
            || q_lower.contains(".org")
    }

    fn estimate_complexity(query: &str, q_lower: &str) -> f32 {
        if Self::has_url_hint(q_lower) {
            0.9 // URLs are high-complexity external unknowns
        } else if query.len() > 100
            || q_lower.contains("code")
            || q_lower.contains("analyze")
            || q_lower.contains("refactor")
        {
            0.8
        } else if query.len() > 30 || q_lower.contains("explain") {
            0.5
        } else {
            0.1
        }
    }

    fn fast_path_decision(
        &self,
        q_lower: &str,
        normalized_query: &str,
        tool_requested: bool,
        scale: &ScaleProfile,
    ) -> Option<RoutingDecision> {
        // FPF Integration: Tool-Use Detection (Pre-Route Fast Path)
        // When users explicitly request a tool, bypass GeneralChat and route to agent with tool access.
        if tool_requested {
            return Some(Self::quick_decision(
                AgentType::Coder, // Coder has tool access
                false,
                true,
                0.95,
                "Query explicitly mentions tool usage (FPF Tool Detection)",
                scale.clone(),
            ));
        }

        // Very short, greeting, or identity messages -> GeneralChat (1b for speed)
        // Expanded threshold to 60 chars to catch simple questions like "What is the capital of France?"
        // unless they look like code or research queries.
        let is_code_related = Self::is_code_related_normalized(normalized_query);
        let is_research_related = Self::is_research_related_normalized(normalized_query);
        let is_planning_related = Self::is_planning_related_normalized(normalized_query);

        let is_short_simple = q_lower.len() < 60
            && !is_code_related
            && !is_research_related
            && !is_planning_related;

        if is_short_simple
            || Self::is_greeting_normalized(normalized_query)
            || Self::is_identity_query_normalized(normalized_query)
        {
            return Some(Self::quick_decision(
                AgentType::GeneralChat,
                false,
                false, // Greetings never require strict reasoning tags
                0.9,
                "Simple greeting or short message",
                scale.clone(),
            ));
        }

        // Filesystem / Directory heuristics (Fast-Path)
        if Self::is_filesystem_related_normalized(normalized_query) {
            return Some(Self::quick_decision(
                AgentType::Coder,
                false,
                true,
                0.95,
                "Direct filesystem query (heuristics fast-path)",
                scale.clone(),
            ));
        }

        // Knowledge Graph / Relationship heuristics
        if q_lower.contains("graph") || q_lower.contains("relationship") || q_lower.contains("visualize")
        {
            return Some(Self::quick_decision(
                AgentType::Reasoner,
                true,
                true,
                0.9,
                "Knowledge graph or relationship query",
                scale.clone(),
            ));
        }

        // Code-related keywords -> Coder
        if is_code_related && !Self::is_complex_query_lower(q_lower) {
            return Some(Self::quick_decision(
                AgentType::Coder,
                false,
                true,
                0.85,
                "Query contains code-related keywords",
                scale.clone(),
            ));
        }

        // Planning keywords -> Planner
        if is_planning_related || Self::is_complex_query_lower(q_lower) {
            return Some(Self::quick_decision(
                AgentType::Planner,
                true,
                true,
                0.8,
                "Query involves planning or task decomposition",
                scale.clone(),
            ));
        }

        // Research/search keywords -> Researcher
        if is_research_related {
            return Some(Self::quick_decision(
                AgentType::Researcher,
                true,
                true,
                0.8,
                "Query requires information gathering",
                scale.clone(),
            ));
        }

        None
    }

    pub fn new(ollama: Ollama) -> Self {
        Self {
            provider: Arc::new(OllamaProvider::new(ollama)),
            model: "llama3.2:3b".to_string(),
        }
    }

    pub fn new_with_provider(provider: Arc<dyn LLMProvider>) -> Self {
        Self {
            provider,
            model: "llama3.2:3b".to_string(),
        }
    }

    pub fn with_provider(mut self, provider: Arc<dyn LLMProvider>) -> Self {
        self.provider = provider;
        self
    }

    #[allow(dead_code)]
    pub fn with_model(mut self, model: impl Into<String>) -> Self {
        self.model = model.into();
        self
    }

    #[allow(dead_code)]
    pub fn with_provider_url(mut self, url: Option<String>) -> Self {
        if let Some(url_str) = url {
            self.provider = Arc::new(OpenAICompatibleProvider::new(url_str, None));
        }
        self
    }

    /// Route a query to the appropriate agent
    pub async fn route(&self, query: &str, vram_available_gb: Option<f32>) -> Result<RoutingDecision> {
        // FPF Integration: Scaling-Law Lens (SLL) - The Scale Probe
        // 1. Calculate complexity (Scale Variables S)
        let q_lower = query.to_lowercase();
        let normalized_query = Self::normalize_for_matching(&q_lower);
        let complexity = Self::estimate_complexity(query, &q_lower);

        // 2. Evaluate Scale Probe against actual hardware state
        let vram = vram_available_gb.unwrap_or(8.0); // Fallback to 8GB if tool is missing
        let scale = ScaleProfile::new(complexity, vram);
        let tool_requested = Self::mentions_tool_normalized(&normalized_query);

        // FPF Integration: Reasoning Requirement Probe
        // Determine if the task is complex enough to merit strict reasoning tags
        let reasoning_required = complexity > 0.3 || tool_requested;

        // Quick heuristics for simple cases
        if let Some(decision) =
            self.fast_path_decision(&q_lower, &normalized_query, tool_requested, &scale)
        {
            return Ok(decision);
        }

        // Use LLM for complex routing decisions
        let mut decision = self.llm_route(query).await?;
        decision.scale = scale;
        decision.reasoning_required = reasoning_required;

        // FPF Integration: Portfolio Generation (G.5)
        // For high-complexity tasks, mandate at least 2 alternative candidates.
        if decision.scale.predicted_complexity > 0.7 && decision.candidate_agents.len() < 2 {
            info!("SLL-Audit: High complexity detected. Expanding to Multi-Candidate Portfolio.");
            match decision.candidate_agents[0] {
                AgentType::Coder => decision.candidate_agents.push(AgentType::Reasoner),
                AgentType::Researcher => decision.candidate_agents.push(AgentType::Reasoner),
                _ => decision.candidate_agents.push(AgentType::Researcher),
            }
        }

        Ok(decision)
    }

    fn is_greeting_normalized(normalized_query: &str) -> bool {
        GREETINGS.iter().any(|g| {
            normalized_query == *g
                || (normalized_query.starts_with(g)
                    && normalized_query
                        .as_bytes()
                        .get(g.len())
                        .is_some_and(|next| *next == b' '))
        })
    }

    fn is_identity_query_normalized(normalized_query: &str) -> bool {
        static IDENTITY_NORMALIZED: OnceLock<Vec<String>> = OnceLock::new();
        // Also handle very short identity queries
        Self::contains_any_cached(normalized_query, IDENTITY_KEYWORDS, &IDENTITY_NORMALIZED)
            || normalized_query == "what are you"
    }

    fn is_filesystem_related_normalized(normalized_query: &str) -> bool {
        static FILESYSTEM_NORMALIZED: OnceLock<Vec<String>> = OnceLock::new();
        Self::contains_any_cached(normalized_query, FILESYSTEM_KEYWORDS, &FILESYSTEM_NORMALIZED)
    }

    fn is_code_related_normalized(normalized_query: &str) -> bool {
        static CODE_NORMALIZED: OnceLock<Vec<String>> = OnceLock::new();
        Self::contains_any_cached(normalized_query, CODE_KEYWORDS, &CODE_NORMALIZED)
    }

    fn is_planning_related_normalized(normalized_query: &str) -> bool {
        static PLANNING_NORMALIZED: OnceLock<Vec<String>> = OnceLock::new();
        Self::contains_any_cached(normalized_query, PLANNING_KEYWORDS, &PLANNING_NORMALIZED)
    }

    fn is_research_related_normalized(normalized_query: &str) -> bool {
        static RESEARCH_NORMALIZED: OnceLock<Vec<String>> = OnceLock::new();
        Self::contains_any_cached(normalized_query, RESEARCH_KEYWORDS, &RESEARCH_NORMALIZED)
    }

    /// FPF Integration: Detect explicit tool usage requests
    /// Routes to agent with tool access when user asks to "use" something
    fn mentions_tool_normalized(normalized_query: &str) -> bool {
        static TOOL_VERBS_NORMALIZED: OnceLock<Vec<String>> = OnceLock::new();
        static TOOL_NAMES_NORMALIZED: OnceLock<Vec<String>> = OnceLock::new();
        // Check for verb + any word (e.g., "use speaker")
        let has_tool_verb = Self::contains_any_cached(normalized_query, TOOL_VERBS, &TOOL_VERBS_NORMALIZED);
        let mentions_tool_name =
            Self::contains_any_cached(normalized_query, TOOL_NAMES, &TOOL_NAMES_NORMALIZED);

        // Either "use X" pattern or explicit tool name mention
        (has_tool_verb && normalized_query.len() > 5)
            || (normalized_query.contains("tool") && mentions_tool_name)
    }

    fn is_complex_query_lower(q_lower: &str) -> bool {
        q_lower.contains(" and ")
            || q_lower.contains(" then ")
            || q_lower.contains(", then ")
            || q_lower.contains(" and finally ")
    }

    async fn llm_route(&self, query: &str) -> Result<RoutingDecision> {
        let prompt = format!(
            r#"q → classify(["general_chat", "reasoner", "coder", "researcher", "planner"]) → agent
q → needs_memory? → memory
→ {{agent, memory, reason: why?}}

q = "{}"
"#,
            query
        );

        let system = Some(super::sns::get_sns_system_prompt());
        let content = self.provider.generate(&self.model, prompt, system).await?;

        self.parse_routing_response(&content)
    }

    fn parse_routing_response(&self, response: &str) -> Result<RoutingDecision> {
        // Try parsing as JSON-like structure first (SNS output)
        if let Some(decision) = Self::parse_json_routing(response) {
            return Ok(decision);
        }

        // Fallback to previous Regex parsing
        Ok(Self::parse_regex_routing(response))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_greeting_detection() {
        let router = Router::new(Ollama::default());
        let res = router.route("hi", None).await.unwrap();
        assert_eq!(res.candidate_agents[0], AgentType::GeneralChat);
    }

    #[tokio::test]
    async fn test_code_detection() {
        let router = Router::new(Ollama::default());
        let res = router.route("write a python function", None).await.unwrap();
        assert_eq!(res.candidate_agents[0], AgentType::Coder);
    }

    #[test]
    fn test_estimate_complexity_prefers_url_signal() {
        let query = "can you summarize https://example.com for me";
        let q_lower = query.to_lowercase();
        assert_eq!(Router::estimate_complexity(query, &q_lower), 0.9);
    }

    #[tokio::test]
    async fn test_tool_request_routes_to_coder_fast_path() {
        let router = Router::new(Ollama::default());
        let res = router.route("use shell to list files", None).await.unwrap();
        assert_eq!(res.candidate_agents[0], AgentType::Coder);
        assert!(res.reason.contains("tool usage"));
    }

    #[test]
    fn test_keyword_matching_requires_word_boundaries() {
        let no_match = Router::normalize_for_matching("classical music theory");
        assert!(!Router::contains_any_normalized(&no_match, CODE_KEYWORDS));

        let match_query = Router::normalize_for_matching("design a class in rust");
        assert!(Router::contains_any_normalized(&match_query, CODE_KEYWORDS));
    }

    #[test]
    fn test_greeting_detection_avoids_prefix_false_positive() {
        let no_greeting = Router::normalize_for_matching("history of distributed systems");
        assert!(!Router::is_greeting_normalized(&no_greeting));

        let greeting = Router::normalize_for_matching("hi there");
        assert!(Router::is_greeting_normalized(&greeting));
    }

    #[test]
    fn test_parse_routing_response_accepts_boolean_memory() {
        let router = Router::new(Ollama::default());
        let response = r#"{"agent":"coder","memory":true,"reason":"Need context"}"#;

        let parsed = router.parse_routing_response(response).unwrap();

        assert_eq!(parsed.candidate_agents, vec![AgentType::Coder]);
        assert!(parsed.should_search_memory);
        assert_eq!(parsed.reason, "Need context");
    }

    #[test]
    fn test_parse_routing_response_accepts_uppercase_boolean_memory() {
        let router = Router::new(Ollama::default());
        let response = r#"{"AGENT":"researcher","MEMORY":true}"#;

        let parsed = router.parse_routing_response(response).unwrap();

        assert_eq!(parsed.candidate_agents, vec![AgentType::Researcher]);
        assert!(parsed.should_search_memory);
    }
}
