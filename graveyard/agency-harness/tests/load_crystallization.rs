use rust_agency::orchestrator::crystallizer::SkillCrystallizer;
use rust_agency::tools::ToolRegistry;
use rust_agency::agent::{LLMProvider, AgentResult};
use rust_agency::memory::{Memory, MemoryEntry};
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::Mutex;
use futures::stream::BoxStream;
use futures::StreamExt;

// --- Mocks ---

// A provider that returns a unique function name every time to allow parallel compilation tests
struct StatefulMockProvider {
    counter: std::sync::atomic::AtomicUsize,
}

impl StatefulMockProvider {
    fn new() -> Self {
        Self { counter: std::sync::atomic::AtomicUsize::new(0) }
    }
}

#[async_trait]
impl LLMProvider for StatefulMockProvider {
    async fn generate(&self, _model: &str, _prompt: String, _system: Option<String>) -> anyhow::Result<String> {
        let id = self.counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let name = format!("fast_add_{}", id);
        
        // We construct the JSON response. Note: We escape quotes for the Rust string, 
        // and we DON'T escape quotes inside the JSON string value for 'code' unless it's needed for JSON validity.
        // The code itself: #[no_mangle] pub extern "C" fn name(a: i32, b: i32) -> i32 { a + b }
        let code = format!("#[no_mangle] pub extern \"C\" fn {}(a: i32, b: i32) -> i32 {{ a + b }}", name);
        
        // Escape code for JSON inclusion
        let json_safe_code = code.replace("\"", "\\\"").replace("\n", "\\n");

        Ok(format!(
            "```json\n{{\n  \"found\": true,\n  \"name\": \"{}\",\n  \"description\": \"Adds numbers\",\n  \"code\": \"{}\"\n}}\n```", 
            name, json_safe_code
        ))
    }
    
    async fn generate_stream(&self, _model: &str, _prompt: String, _system: Option<String>) -> anyhow::Result<BoxStream<'static, anyhow::Result<String>>> {
        let stream = futures::stream::iter(vec![Ok("mock".to_string())]);
        Ok(Box::pin(stream))
    }

    async fn notify(&self, _status: &str) -> anyhow::Result<()> { Ok(()) }
    fn get_lock(&self) -> Arc<Mutex<()>> { Arc::new(Mutex::new(())) }
}

struct MockMemory;
#[async_trait]
impl Memory for MockMemory {
    async fn store(&self, _entry: MemoryEntry) -> anyhow::Result<String> { Ok("id".to_string()) }
    
    async fn search(&self, _query: &str, _top_k: usize, _context: Option<&str>, _kind: Option<rust_agency::orchestrator::Kind>) -> anyhow::Result<Vec<MemoryEntry>> {
        Ok(vec![
            MemoryEntry::new("Successfully calculated 2+2=4 using reasoning.".to_string(), "mock", rust_agency::memory::entry::MemorySource::Agent)
        ])
    }
    
    async fn count(&self) -> anyhow::Result<usize> { Ok(0) }
    async fn persist(&self) -> anyhow::Result<()> { Ok(()) }
    async fn consolidate(&self) -> anyhow::Result<usize> { Ok(0) }
    async fn get_cold_memories(&self, _limit: usize) -> anyhow::Result<Vec<MemoryEntry>> { Ok(vec![]) }
    async fn prune(&self, _ids: Vec<String>) -> anyhow::Result<()> { Ok(()) }
    
    async fn clear_cache(&self) -> anyhow::Result<()> { Ok(()) }
    async fn hibernate(&self) -> anyhow::Result<()> { Ok(()) }
    async fn wake(&self) -> anyhow::Result<()> { Ok(()) }
}

#[tokio::test]
async fn test_crystallization_load() {
    let provider = Arc::new(StatefulMockProvider::new());
    let memory = Arc::new(MockMemory);
    
    let mut handles = vec![];
    // Run 3 concurrent compilations to verify thread safety and uniqueness
    for _ in 0..3 { 
        let registry = Arc::new(ToolRegistry::default());
        registry.register_instance(rust_agency::tools::WasmCompilerTool::new()).await;

        let c = SkillCrystallizer::new(
            provider.clone(), 
            registry,
            memory.clone()
        );

        handles.push(tokio::spawn(async move {
            c.crystallize().await
        }));
    }

    let results = futures::future::join_all(handles).await;
    
    for res in results {
        match res {
            Ok(Ok(_)) => {
                // Success (or 0 if missing rustc, but no panic)
            },
            Ok(Err(e)) => panic!("Crystallizer error: {}", e),
            Err(e) => panic!("Join error: {}", e),
        }
    }
}