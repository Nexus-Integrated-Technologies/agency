//! Router - Query routing to appropriate agents
//! 
//! Determines which agent should handle a given query.

use anyhow::Result;
use ollama_rs::Ollama;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
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
    fn contains_any(query: &str, keywords: &[&str]) -> bool {
        keywords.iter().any(|k| query.contains(k))
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
        
        // URL Detection: Escalates complexity to Heavy (0.9) to mandate tool-use
        let has_url = q_lower.contains("http://") || q_lower.contains("https://") || q_lower.contains(".com") || q_lower.contains(".org");

        let complexity = if has_url {
            0.9 // URLs are high-complexity external unknowns
        } else if query.len() > 100 || q_lower.contains("code") || q_lower.contains("analyze") || q_lower.contains("refactor") {
            0.8
        } else if query.len() > 30 || q_lower.contains("explain") {
            0.5
        } else {
            0.1
        };

        // 2. Evaluate Scale Probe against actual hardware state
        let vram = vram_available_gb.unwrap_or(8.0); // Fallback to 8GB if tool is missing
        let scale = ScaleProfile::new(complexity, vram);
        
        // FPF Integration: Reasoning Requirement Probe
        // Determine if the task is complex enough to merit strict reasoning tags
        let reasoning_required = complexity > 0.3 || self.mentions_tool(&q_lower);

        // Quick heuristics for simple cases
        
        // FPF Integration: Tool-Use Detection (Pre-Route Fast Path)
        // When users explicitly request a tool, bypass GeneralChat and route to agent with tool access.
        if self.mentions_tool(&q_lower) {
            return Ok(Self::quick_decision(
                AgentType::Coder, // Coder has tool access
                false,
                true,
                0.95,
                "Query explicitly mentions tool usage (FPF Tool Detection)",
                scale,
            ));
        }
        
        // Very short, greeting, or identity messages -> GeneralChat (1b for speed)
        // Expanded threshold to 60 chars to catch simple questions like "What is the capital of France?"
        // unless they look like code or research queries.
        let is_short_simple = q_lower.len() < 60 
            && !self.is_code_related(&q_lower) 
            && !self.is_research_related(&q_lower)
            && !self.is_planning_related(&q_lower);

        if is_short_simple || self.is_greeting(&q_lower) || self.is_identity_query(&q_lower) {
            return Ok(Self::quick_decision(
                AgentType::GeneralChat,
                false,
                false, // Greetings never require strict reasoning tags
                0.9,
                "Simple greeting or short message",
                scale,
            ));
        }

        // Filesystem / Directory heuristics (Fast-Path)
        if self.is_filesystem_related(&q_lower) {
            return Ok(Self::quick_decision(
                AgentType::Coder,
                false,
                true,
                0.95,
                "Direct filesystem query (heuristics fast-path)",
                scale,
            ));
        }

        // Knowledge Graph / Relationship heuristics
        if q_lower.contains("graph") || q_lower.contains("relationship") || q_lower.contains("visualize") {
            return Ok(Self::quick_decision(
                AgentType::Reasoner,
                true,
                true,
                0.9,
                "Knowledge graph or relationship query",
                scale,
            ));
        }

        // Code-related keywords -> Coder
        if self.is_code_related(&q_lower) && !self.is_complex_query(&q_lower) {
            return Ok(Self::quick_decision(
                AgentType::Coder,
                false,
                true,
                0.85,
                "Query contains code-related keywords",
                scale,
            ));
        }

        // Planning keywords -> Planner
        if self.is_planning_related(&q_lower) || self.is_complex_query(&q_lower) {
            return Ok(Self::quick_decision(
                AgentType::Planner,
                true,
                true,
                0.8,
                "Query involves planning or task decomposition",
                scale,
            ));
        }

        // Research/search keywords -> Researcher
        if self.is_research_related(&q_lower) {
            return Ok(Self::quick_decision(
                AgentType::Researcher,
                true,
                true,
                0.8,
                "Query requires information gathering",
                scale,
            ));
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

    fn is_greeting(&self, query: &str) -> bool {
        GREETINGS
            .iter()
            .any(|g| query.starts_with(g) || query == *g)
    }

    fn is_identity_query(&self, query: &str) -> bool {
        // Also handle very short identity queries
        Self::contains_any(query, IDENTITY_KEYWORDS) || query.trim().to_lowercase() == "what are you"
    }

    fn is_filesystem_related(&self, query: &str) -> bool {
        Self::contains_any(query, FILESYSTEM_KEYWORDS)
    }

    fn is_code_related(&self, query: &str) -> bool {
        Self::contains_any(query, CODE_KEYWORDS)
    }

    fn is_planning_related(&self, query: &str) -> bool {
        Self::contains_any(query, PLANNING_KEYWORDS)
    }

    fn is_research_related(&self, query: &str) -> bool {
        Self::contains_any(query, RESEARCH_KEYWORDS)
    }

    /// FPF Integration: Detect explicit tool usage requests
    /// Routes to agent with tool access when user asks to "use" something
    fn mentions_tool(&self, query: &str) -> bool {
        // Check for verb + any word (e.g., "use speaker")
        let has_tool_verb = Self::contains_any(query, TOOL_VERBS);
        let mentions_tool_name = Self::contains_any(query, TOOL_NAMES);
        
        // Either "use X" pattern or explicit tool name mention
        (has_tool_verb && query.len() > 5) || (query.contains("tool") && mentions_tool_name)
    }

    fn is_complex_query(&self, query: &str) -> bool {
        let q = query.to_lowercase();
        q.contains(" and ") || q.contains(" then ") || q.contains(", then ") || q.contains(" and finally ")
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
        if let Some(start) = response.find('{') {
            if let Some(end) = response.rfind('}') {
                let json_str = &response[start..=end];
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(json_str) {
                    let agent_str = v["agent"].as_str()
                        .or_else(|| v["AGENT"].as_str())
                        .map(|s| s.to_lowercase())
                        .unwrap_or_else(|| "reasoner".to_string());

                    let agent_type = Self::parse_agent_label(agent_str.as_str());

                    let memory_val = v["memory"].as_str()
                        .or_else(|| v["MEMORY"].as_str())
                        .map(|s| s.to_lowercase())
                        .unwrap_or_else(|| "no".to_string());
                    
                    let should_search_memory = memory_val == "yes" || memory_val == "true";

                    let reason = v["reason"].as_str()
                        .or_else(|| v["REASON"].as_str())
                        .unwrap_or("LLM routing decision")
                        .to_string();

                    return Ok(RoutingDecision {
                        candidate_agents: vec![agent_type],
                        should_search_memory,
                        reasoning_required: true, // LLM-routed queries are usually complex
                        confidence: 0.7,
                        reason,
                        scale: ScaleProfile::new(0.5, 8.0), // Placeholder, will be updated by caller
                    });
                }
            }
        }

        // Fallback to previous Regex parsing
        let agent_re = Regex::new(r"(?i)AGENT:\s*(\w+)")?;
        let memory_re = Regex::new(r"(?i)MEMORY:\s*(yes|no)")?;
        let reason_re = Regex::new(r"(?i)REASON:\s*(.+)")?;

        let agent_str = agent_re
            .captures(response)
            .and_then(|c| c.get(1))
            .map(|m| m.as_str().to_lowercase())
            .unwrap_or_else(|| "reasoner".to_string());

        let agent_type = Self::parse_agent_label(agent_str.as_str());

        let should_search_memory = memory_re
            .captures(response)
            .and_then(|c| c.get(1))
            .map(|m| m.as_str().to_lowercase() == "yes")
            .unwrap_or(false);

        let reason = reason_re
            .captures(response)
            .and_then(|c| c.get(1))
            .map(|m| m.as_str().trim().to_string())
            .unwrap_or_else(|| "LLM routing decision".to_string());

        Ok(RoutingDecision {
            candidate_agents: vec![agent_type],
            should_search_memory,
            reasoning_required: true,
            confidence: 0.7, // LLM routing is less certain
            reason,
            scale: ScaleProfile::new(0.5, 8.0), // Placeholder
        })
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
}
