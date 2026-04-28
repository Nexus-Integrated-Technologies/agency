//! Tool System Module
//! 
//! Provides structured tool calling with JSON schema definitions
//! and result caching for high performance.

mod web_search;
mod code_exec;
mod memory_query;
mod artifact;
mod sandbox;
mod codebase;
mod system;
mod dynamic;
mod knowledge_graph;
mod agency_control;
mod visualization;
mod speaker_rs;
mod science;
mod models;
mod vision;
mod mcp;
mod skills;
mod a2a;
mod task_spawner;
mod watchdog;
mod notify;
mod swarm_bounty;
mod mutation;
mod wallet;
mod hands;
mod provider;
pub use provider::ProviderTool;
mod wasm_compiler;
mod wasm_executor;

pub use web_search::WebSearchTool;
pub use speaker_rs::SpeakerRsTool;
pub use code_exec::CodeExecTool;
pub use memory_query::MemoryQueryTool;
pub use artifact::ArtifactTool;
pub use sandbox::SandboxTool;
pub use codebase::CodebaseTool;
pub use system::SystemTool;
pub use knowledge_graph::KnowledgeGraphTool;
pub use agency_control::AgencyControlTool;
pub use visualization::VisualizationTool;
pub use science::ScienceTool;
pub use models::ModelManager;
pub use vision::VisionTool;
pub use dynamic::{DynamicTool, ForgeTool};
pub use a2a::{PeerAgentTool, RemoteAgencyTool, AnonymousAgencyTool};
pub use mcp::{McpServer, McpProxyTool};
pub use skills::{MarkdownSkill, SkillLoader};
pub use task_spawner::TaskSpawnerTool;
pub use watchdog::WatchdogTool;
pub use notify::NotifyTool;
pub use swarm_bounty::SwarmBountyTool;
pub use mutation::MutationTool;
pub use wallet::WalletTool;
pub use hands::HandsTool;
pub use wasm_compiler::WasmCompilerTool;
pub use wasm_executor::WasmExecutorTool;

use crate::agent::{AgentResult, LadeQuadrant};
use crate::orchestrator::AgencyEvent;
use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use std::path::{Path, PathBuf};
use tokio::sync::{Mutex, RwLock};

/// Output from a tool execution
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ToolOutput {
    /// Whether the tool execution was successful
    pub success: bool,
    /// The output data (can be string, JSON object, etc.)
    pub data: Value,
    /// Human-readable summary of the output
    pub summary: String,
    /// Optional error message if success is false
    pub error: Option<String>,
}

impl ToolOutput {
    /// Create a successful output
    pub fn success(data: impl Into<Value>, summary: impl Into<String>) -> Self {
        Self {
            success: true,
            data: data.into(),
            summary: summary.into(),
            error: None,
        }
    }

    /// Create a successful output with string data
    pub fn success_str(content: impl Into<String>) -> Self {
        let content = content.into();
        Self {
            success: true,
            summary: content.clone(),
            data: Value::String(content),
            error: None,
        }
    }

    /// Create a failed output
    pub fn failure(error: impl Into<String>) -> Self {
        let error = error.into();
        Self {
            success: false,
            data: Value::Null,
            summary: format!("Error: {}", error),
            error: Some(error),
        }
    }
}

/// A tool call request parsed from LLM output
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq, Hash)]
pub struct ToolCall {
    /// Name of the tool to call
    pub name: String,
    /// Parameters for the tool
    pub parameters: Value,
}

/// Trait for tools that can be executed by agents
#[async_trait]
pub trait Tool: Send + Sync {
    /// Get the unique name of the tool
    fn name(&self) -> String;
    
    /// Get a description of what the tool does
    fn description(&self) -> String;
    
    /// Get the JSON schema for the tool's parameters
    fn parameters(&self) -> Value;

    /// Get the U.WorkScope (operational constraints) for this tool (FPF Principle)
    /// This allows the agent to evaluate if the tool can handle the specific task.
    fn work_scope(&self) -> Value {
        // Default: Unconstrained
        json!({
            "status": "unconstrained",
            "notes": "No explicit hardware or data constraints declared."
        })
    }

    /// Perform a security check before execution (FPF SOTA Protection)
    async fn security_oracle(&self, _params: &Value) -> AgentResult<bool> {
        // Default: Passive (Assume safe or handled by validator)
        Ok(true)
    }
    
    /// Execute the tool with the given parameters
    async fn execute(&self, params: Value) -> AgentResult<ToolOutput>;

    /// Whether this tool requires explicit human confirmation
    fn requires_confirmation(&self) -> bool {
        false
    }
}

/// Registry for available tools with built-in caching
pub struct ToolRegistry {
    tools: RwLock<HashMap<String, Arc<dyn Tool>>>,
    cache: Arc<Mutex<HashMap<String, ToolOutput>>>,
    custom_tools_dir: PathBuf,
    standard_tools_dir: PathBuf,
}

impl ToolRegistry {
    async fn insert_tool_arc(&self, name: String, tool: Arc<dyn Tool>) {
        let mut tools = self.tools.write().await;
        tools.insert(name, tool);
    }

    /// Create a new empty registry
    pub fn new(custom_dir: impl Into<PathBuf>, standard_dir: impl Into<PathBuf>) -> Self {
        Self {
            tools: RwLock::new(HashMap::new()),
            cache: Arc::new(Mutex::new(HashMap::new())),
            custom_tools_dir: custom_dir.into(),
            standard_tools_dir: standard_dir.into(),
        }
    }

    /// Register a tool
    #[allow(dead_code)]
    pub async fn register<T: Tool + 'static + Default>(&self) {
        let tool = T::default();
        let name = tool.name();
        self.insert_tool_arc(name, Arc::new(tool)).await;
    }

    /// Register a tool instance
    pub async fn register_instance<T: Tool + 'static>(&self, tool: T) {
        let name = tool.name();
        self.insert_tool_arc(name, Arc::new(tool)).await;
    }

    /// Load all dynamic tools from a directory
    pub async fn load_dynamic_tools(&self, dir_path: impl AsRef<Path>) -> Result<usize> {
        let path = dir_path.as_ref();
        if !path.exists() {
            return Ok(0);
        }

        let mut loaded_tools: Vec<(String, Arc<dyn Tool>)> = Vec::new();
        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) == Some("json") {
                match DynamicTool::from_file(&path) {
                    Ok(tool) => {
                        let name = tool.name();
                        tracing::info!("Loaded dynamic tool: {}", name);
                        loaded_tools.push((name, Arc::new(tool)));
                    }
                    Err(e) => tracing::warn!("Failed to load dynamic tool at {:?}: {}", path, e),
                }
            }
        }

        if loaded_tools.is_empty() {
            return Ok(0);
        }

        let count = loaded_tools.len();
        let mut tools = self.tools.write().await;
        for (name, tool) in loaded_tools {
            tools.insert(name, tool);
        }
        Ok(count)
    }

    /// Register all tools from an MCP server
    pub async fn register_mcp_server(&self, server: Arc<McpServer>) -> Result<usize> {
        let tools = server.list_tools().await?;
        let mut proxies = Vec::with_capacity(tools.len());
        for tool_def in tools {
            let proxy = Arc::new(McpProxyTool::new(server.clone(), tool_def));
            proxies.push((proxy.name(), proxy as Arc<dyn Tool>));
        }

        if proxies.is_empty() {
            return Ok(0);
        }

        let count = proxies.len();
        let mut registered_tools = self.tools.write().await;
        for (name, proxy) in proxies {
            registered_tools.insert(name, proxy);
        }
        Ok(count)
    }

    /// Get all tool names
    pub async fn tool_names(&self) -> Vec<String> {
        let tools = self.tools.read().await;
        tools.keys().cloned().collect()
    }

    /// Generate a combined schema for all tools (for LLM prompt)
    #[allow(dead_code)]
    pub async fn generate_tools_prompt(&self) -> String {
        let names = self.tool_names().await;
        self.generate_filtered_tools_prompt(&names).await
    }

    /// Generate a schema for specific tools
    pub async fn generate_filtered_tools_prompt(&self, allowed_names: &[String]) -> String {
        if allowed_names.is_empty() {
            return "No tools available for this task.\n".to_string();
        }

        let mut prompt = String::from("Available Tools:\n\n");
        
        let tools = self.tools.read().await;
        let mut names: Vec<_> = allowed_names.iter().filter(|n| tools.contains_key(*n)).collect();
        names.sort();

        for name in names {
            let tool = &tools[name];
            // Use more compact formatting for tool definitions
            prompt.push_str(&format!("- {}: {} (params: {})\n", 
                name, 
                tool.description(),
                serde_json::to_string(&tool.parameters()).unwrap_or_default()
            ));

            // FPF Integration: Surface the Capability WorkScope
            let scope = tool.work_scope();
            if scope["status"] != "unconstrained" {
                prompt.push_str(&format!("  U.WorkScope (Constraints): {}\n", scope));
            }
        }
        
        prompt
    }

    /// Get a specific tool by name
    pub async fn get_tool(&self, name: &str) -> Option<Arc<dyn Tool>> {
        let tools = self.tools.read().await;
        tools.get(name).cloned()
    }

    /// Execute a tool call with caching
    pub async fn execute(&self, call: &ToolCall) -> AgentResult<ToolOutput> {
        let cache_key = format!("{}:{}", call.name, serde_json::to_string(&call.parameters)?);
        
        // Check cache
        {
            let cache = self.cache.lock().await;
            if let Some(output) = cache.get(&cache_key) {
                tracing::debug!("Cache Hit for tool: {}", call.name);
                return Ok(output.clone());
            }
        }

        let tool = {
            let tools = self.tools.read().await;
            tools.get(&call.name).cloned()
        };

        let result = match tool {
            Some(tool) => {
                // SOTA Security Check
                if !tool.security_oracle(&call.parameters).await? {
                    // Emit FPF Boundary Crossing (A.6.B - Quadrant A: Admissibility)
                    crate::emit_event!(AgencyEvent::BoundaryCrossing(crate::orchestrator::event_bus::FPFBoundClaim {
                        quadrant: LadeQuadrant::A,
                        claim_id: format!("GF-{}", call.name),
                        content: format!("Security Oracle blocked execution of tool '{}'", call.name),
                    }));
                    return Ok(ToolOutput::failure(format!("Security Oracle blocked execution of tool '{}'", call.name)));
                }
                tool.execute(call.parameters.clone()).await?
            },
            None => ToolOutput::failure(format!("Unknown tool: {}", call.name)),
        };

        // Update cache if successful or specific failure
        if result.success {
            let mut cache = self.cache.lock().await;
            cache.insert(cache_key, result.clone());
        }

        Ok(result)
    }

    /// Execute multiple tool calls in parallel
    pub async fn execute_parallel(&self, calls: &[ToolCall]) -> Vec<AgentResult<ToolOutput>> {
        let mut futures = Vec::new();
        for call in calls {
            futures.push(self.execute(call));
        }
        futures_util::future::join_all(futures).await
    }

    /// Clear the tool cache
    #[allow(dead_code)]
    pub async fn clear_cache(&self) {
        let mut cache = self.cache.lock().await;
        cache.clear();
    }

    /// Promote a custom tool to the standard set
    pub async fn promote_tool(&self, name: &str) -> Result<()> {
        let tools = self.tools.read().await;
        if let Some(_tool) = tools.get(name) {
            // Check if it's a dynamic tool in the custom directory
            let metadata_path = self.custom_tools_dir.join(format!("{}.json", name));
            if metadata_path.exists() {
                tracing::info!("🚀 Promoting tool '{}' to standard set.", name);
                
                // Ensure standard directory exists
                if !self.standard_tools_dir.exists() {
                    std::fs::create_dir_all(&self.standard_tools_dir)?;
                }

                // Move metadata
                let new_metadata_path = self.standard_tools_dir.join(format!("{}.json", name));
                std::fs::rename(&metadata_path, &new_metadata_path)?;

                // Read metadata to find script path
                let content = std::fs::read_to_string(&new_metadata_path)?;
                let metadata: serde_json::Value = serde_json::from_str(&content)?;
                
                if let Some(script_path) = metadata["script_path"].as_str() {
                    let old_script = self.custom_tools_dir.join(script_path);
                    let new_script = self.standard_tools_dir.join(script_path);
                    if old_script.exists() {
                        std::fs::rename(old_script, new_script)?;
                    }
                }

                return Ok(());
            }
            // If it's already in standard or built-in, do nothing
            return Ok(());
        }
        Err(anyhow::anyhow!("Tool not found for promotion"))
    }
}

impl Default for ToolRegistry {
    fn default() -> Self {
        Self::new("custom_tools", "standard_tools")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[derive(Default)]
    struct MockTool;

    #[async_trait]
    impl Tool for MockTool {
        fn name(&self) -> String { "mock_tool".to_string() }
        fn description(&self) -> String { "A mock tool for testing".to_string() }
        fn parameters(&self) -> Value { json!({"type": "object"}) }
        async fn execute(&self, params: Value) -> AgentResult<ToolOutput> {
            Ok(ToolOutput::success(params, "Mock execution successful"))
        }
    }

    #[tokio::test]
    async fn test_tool_registration() {
        let registry = ToolRegistry::default();
        registry.register::<MockTool>().await;
        
        let names = registry.tool_names().await;
        assert!(names.contains(&"mock_tool".to_string()));
    }

    #[tokio::test]
    async fn test_tool_execution_caching() {
        let registry = ToolRegistry::default();
        registry.register::<MockTool>().await;
        
        let call = ToolCall {
            name: "mock_tool".to_string(),
            parameters: json!({"input": "test"}),
        };
        
        // First execution (cache miss)
        let res1 = registry.execute(&call).await.unwrap();
        assert!(res1.success);
        
        // Second execution (cache hit)
        let res2 = registry.execute(&call).await.unwrap();
        assert_eq!(res1, res2);
    }

    #[tokio::test]
    async fn test_generate_tools_prompt() {
        let registry = ToolRegistry::default();
        registry.register::<MockTool>().await;
        
        let prompt = registry.generate_tools_prompt().await;
        assert!(prompt.contains("mock_tool"));
        assert!(prompt.contains("A mock tool for testing"));
    }
}
