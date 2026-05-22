use rust_agency::orchestrator::curiosity::CuriosityEngine;
use rust_agency::agent::{LLMProvider, AgentResult};
use rust_agency::memory::{Memory, MemoryEntry};
use rust_agency::orchestrator::queue::TaskQueue;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::Mutex;
use futures::stream::BoxStream;

// --- Mocks ---

struct MockProvider;
#[async_trait]
impl LLMProvider for MockProvider {
    async fn generate(&self, _model: &str, _prompt: String, _system: Option<String>) -> anyhow::Result<String> {
        Ok(r#"{"goal": "explore tools", "rationale": "need more knowledge", "priority": 0.8}"#.to_string())
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
    async fn search(&self, _query: &str, _top_k: usize, _context: Option<&str>, _kind: Option<rust_agency::orchestrator::Kind>) -> anyhow::Result<Vec<MemoryEntry>> { Ok(vec![]) }
    async fn count(&self) -> anyhow::Result<usize> { Ok(0) }
    async fn persist(&self) -> anyhow::Result<()> { Ok(()) }
    async fn consolidate(&self) -> anyhow::Result<usize> { Ok(0) }
    async fn get_cold_memories(&self, _limit: usize) -> anyhow::Result<Vec<MemoryEntry>> { Ok(vec![]) }
    async fn get_recent(&self, _limit: usize) -> anyhow::Result<Vec<MemoryEntry>> { Ok(vec![]) }
    async fn prune(&self, _ids: Vec<String>) -> anyhow::Result<()> { Ok(()) }
    async fn clear_cache(&self) -> anyhow::Result<()> { Ok(()) }
    async fn hibernate(&self) -> anyhow::Result<()> { Ok(()) }
    async fn wake(&self) -> anyhow::Result<()> { Ok(()) }
}

struct MockQueue {
    enqueued: Mutex<Vec<String>>,
}
#[async_trait]
impl TaskQueue for MockQueue {
    async fn enqueue(&self, kind: &str, payload: serde_json::Value) -> anyhow::Result<String> {
        let mut g = self.enqueued.lock().await;
        g.push(kind.to_string());
        Ok("id".to_string())
    }
    async fn dequeue(&self) -> anyhow::Result<Option<rust_agency::orchestrator::queue::Task>> { Ok(None) }
    async fn complete(&self, _id: &str) -> anyhow::Result<()> { Ok(()) }
    async fn fail(&self, _id: &str, _err: &str, _retry: bool) -> anyhow::Result<()> { Ok(()) }
    async fn get_status(&self, _id: &str) -> anyhow::Result<Option<String>> { Ok(None) }
    async fn count(&self, _kind: &str) -> anyhow::Result<i64> { Ok(0) }
}

#[tokio::test]
async fn test_curiosity_generation() {
    let provider = Arc::new(MockProvider);
    let memory = Arc::new(MockMemory);
    let queue = Arc::new(MockQueue { enqueued: Mutex::new(vec![]) });

    let engine = CuriosityEngine::new(provider, memory, queue.clone());

    let res = engine.spark_curiosity().await.unwrap();
    assert!(res);

    let enqueued = queue.enqueued.lock().await;
    assert_eq!(enqueued.len(), 1);
    assert_eq!(enqueued[0], "autonomous_goal");
}
