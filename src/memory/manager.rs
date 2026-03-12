//! Memory Manager - Resource-aware memory management
//! 
//! Monitors system resources (RAM/VRAM) and provides cleanup capabilities
//! inspired by high-performance memory management systems.

use anyhow::{Context, Result};
use sysinfo::System;
use tracing::{info, warn, debug};
use std::sync::Arc;
use tokio::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use ollama_rs::{
    generation::chat::{request::ChatMessageRequest, ChatMessage},
    Ollama,
};

use crate::memory::{Memory, EpisodicMemory, MemoryEntry, entry::MemorySource};

/// Configuration for memory management
#[derive(Debug, Clone)]
pub struct MemoryManagerConfig {
    /// RAM usage percentage that triggers a warning (0.0 - 100.0)
    pub ram_warning_threshold: f64,
    /// RAM usage percentage that triggers aggressive cleanup
    pub ram_critical_threshold: f64,
    /// Whether to automatically clean up cache on high RAM
    #[allow(dead_code)]
    pub auto_cleanup: bool,
}

impl Default for MemoryManagerConfig {
    fn default() -> Self {
        Self {
            ram_warning_threshold: 75.0,
            ram_critical_threshold: 85.0,
            auto_cleanup: true,
        }
    }
}

/// Resource status for the system
#[derive(Debug, Clone, serde::Serialize)]
pub struct ResourceStatus {
    pub ram_usage_percent: f64,
    pub used_memory_mb: u64,
    pub total_memory_mb: u64,
    pub process_memory_mb: u64,
    pub swap_usage_percent: f64,
    pub status_level: String, // "Healthy", "Warning", "Critical"
    pub os_type: String, // "mac", "linux", "windows", etc.
}

/// Resource-aware manager for semantic and episodic memory
pub struct MemoryManager {
    #[allow(dead_code)]
    config: MemoryManagerConfig,
    system: Arc<Mutex<System>>,
    vector_memory: Arc<dyn Memory>,
    #[allow(dead_code)]
    last_check: AtomicU64, // Timestamp in seconds
}

impl MemoryManager {
    const STATUS_HEALTHY: &'static str = "Healthy";
    const STATUS_WARNING: &'static str = "Warning";
    const STATUS_CRITICAL: &'static str = "Critical";

    /// Create a new MemoryManager with default config
    pub fn new(vector_memory: Arc<dyn Memory>) -> Self {
        let mut sys = System::new_all();
        sys.refresh_all();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        Self {
            config: MemoryManagerConfig::default(),
            system: Arc::new(Mutex::new(sys)),
            vector_memory,
            last_check: AtomicU64::new(now - 60), // Start with an old timestamp
        }
    }

    fn status_level_from_ram_usage(&self, ram_percent: f64) -> String {
        if ram_percent > self.config.ram_critical_threshold {
            Self::STATUS_CRITICAL.to_string()
        } else if ram_percent > self.config.ram_warning_threshold {
            Self::STATUS_WARNING.to_string()
        } else {
            Self::STATUS_HEALTHY.to_string()
        }
    }

    fn detect_os_type() -> String {
        match System::name().unwrap_or_default().to_lowercase().as_str() {
            name if name.contains("mac") || name.contains("darwin") => "mac",
            name if name.contains("windows") => "windows",
            _ => "linux",
        }
        .to_string()
    }

    async fn store_reflection_entry(
        &self,
        content: &str,
        tags: &[&str],
        importance: Option<f32>,
    ) -> Result<()> {
        let mut entry = MemoryEntry::new(content, "MemoryManager", MemorySource::Reflection);
        if let Some(importance) = importance {
            entry.metadata.importance = importance;
        }
        entry
            .metadata
            .tags
            .extend(tags.iter().map(|tag| (*tag).to_string()));
        self.vector_memory.store(entry).await
    }

    /// Get current resource status
    pub async fn get_status(&self) -> ResourceStatus {
        let mut sys = self.system.lock().await;
        sys.refresh_memory();
        // Skip process refresh unless specifically needed, as it's slow
        // sys.refresh_processes(ProcessesToUpdate::All, true);
        
        let total_mem = sys.total_memory();
        let used_mem = sys.used_memory();
        let total_swap = sys.total_swap();
        let used_swap = sys.used_swap();
        
        // let pid = sysinfo::get_current_pid().ok();
        // let process_mem = pid.and_then(|p| sys.process(p)).map(|p| p.memory()).unwrap_or(0);
        let process_mem = 0; // Optimization: don't calculate per-process memory here

        let ram_percent = if total_mem > 0 { (used_mem as f64 / total_mem as f64) * 100.0 } else { 0.0 };
        
        let level = self.status_level_from_ram_usage(ram_percent);
        let os = Self::detect_os_type();

        ResourceStatus {
            ram_usage_percent: ram_percent,
            used_memory_mb: used_mem / 1024 / 1024,
            total_memory_mb: total_mem / 1024 / 1024,
            process_memory_mb: process_mem / 1024 / 1024,
            swap_usage_percent: if total_swap > 0 { (used_swap as f64 / total_swap as f64) * 100.0 } else { 0.0 },
            status_level: level,
            os_type: os,
        }
    }

    /// Check current resource status and perform cleanup if necessary
    #[allow(dead_code)]
    pub async fn monitor_and_optimize(&self) -> Result<()> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let last = self.last_check.load(Ordering::Relaxed);
        if now - last < 60 {
            return Ok(());
        }
        
        // Update last check time
        self.last_check.store(now, Ordering::Relaxed);

        let status = self.get_status().await;

        if status.status_level == Self::STATUS_CRITICAL {
            warn!("CRITICAL RAM: {:.1}%. Performing emergency cleanup...", status.ram_usage_percent);
            self.cleanup_internal().await?;
        } else if status.status_level == Self::STATUS_WARNING {
            debug!("High RAM Usage: {:.1}%. Optimizing context windows.", status.ram_usage_percent);
        }

        // Periodic background persistence
        debug!("Triggering background persistence check...");
        let _ = self.vector_memory.persist().await;

        Ok(())
    }

    /// Explicitly trigger memory persistence
    #[allow(dead_code)]
    pub async fn persist_memory(&self) -> Result<()> {
        debug!("MemoryManager: Forcing memory persistence to disk.");
        self.vector_memory.persist().await
    }

    /// Internal cleanup logic
    #[allow(dead_code)]
    async fn cleanup_internal(&self) -> Result<()> {
        info!("MemoryManager: Purging transient caches and triggering process GC.");
        // Clear vector memory in-memory cache to free up significant RAM
        let _ = self.vector_memory.clear_cache().await;
        Ok(())
    }

    /// Distill episodic memory into long-term facts and store in vector memory
    pub async fn distill_and_consolidate(
        &self,
        ollama: &Ollama,
        episodic: &EpisodicMemory,
    ) -> Result<usize> {
        if episodic.is_empty() {
            return Ok(0);
        }

        info!("Starting memory consolidation and fact distillation...");
        
        let history = episodic.format_for_prompt();
        let prompt = format!(
            r#"You are a memory consolidation assistant. Analyze the following conversation history and extract 3-5 key long-term facts or entities.

## Rules:
1. ONLY extract facts that are likely to be useful in FUTURE conversations.
2. Format each fact as a clear, standalone sentence.
3. Identify entities (people, projects, files, technologies) and their relationships.
4. Avoid conversational filler or transient state (e.g., "the user said hi").

## History:
{}

## Output Format:
FACT: [Standalone fact sentence]
TAGS: [tag1, tag2]
ENTITY: [Entity Name] -> [Relationship] -> [Target]
"#,
            history
        );

        let response = ollama
            .send_chat_messages(ChatMessageRequest::new(
                "llama3.2:3b".to_string(),
                vec![ChatMessage::user(prompt)],
            ))
            .await
            .context("Failed to get distillation response")?;

        let distilled = &response.message.content;
        let mut count = 0;

        for line in distilled.lines() {
            if let Some(fact) = line.strip_prefix("FACT:") {
                let fact = fact.trim();
                if !fact.is_empty() {
                    self.store_reflection_entry(fact, &["distilled"], Some(0.8))
                        .await?;
                    count += 1;
                }
            } else if let Some(entity) = line.strip_prefix("ENTITY:") {
                let entity = entity.trim();
                if !entity.is_empty() {
                    self.store_reflection_entry(entity, &["knowledge_graph", "entity"], None)
                        .await?;
                    // Entities are stored as memories for now, but tagged for future graph conversion
                }
            }
        }

        if count > 0 {
            info!("Consolidated {} long-term facts into vector memory", count);
        }

        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::VectorMemory;
    use tempfile::{tempdir, TempDir};

    fn should_skip_memory_tests() -> bool {
        std::env::set_var("AGENCY_USE_REMOTE_MEMORY", "0");
        std::env::var("ORT_DYLIB_PATH").is_err() && !std::path::Path::new("libonnxruntime.dylib").exists()
    }

    fn create_test_manager() -> Option<(MemoryManager, TempDir)> {
        if should_skip_memory_tests() {
            return None;
        }
        let temp_dir = tempdir().ok()?;
        let path = temp_dir.path().join("test_memory.json");
        let vector_memory = Arc::new(VectorMemory::new(path).ok()?);
        Some((MemoryManager::new(vector_memory), temp_dir))
    }

    #[tokio::test]
    async fn test_get_status() {
        let Some((manager, _temp_dir)) = create_test_manager() else {
            return;
        };
        
        let status = manager.get_status().await;
        assert!(status.ram_usage_percent >= 0.0);
        assert!(status.total_memory_mb > 0);
    }

    #[tokio::test]
    async fn test_monitor_and_optimize_throttle() {
        let Some((manager, _temp_dir)) = create_test_manager() else {
            return;
        };
        
        // First call should proceed
        manager.monitor_and_optimize().await.unwrap();
        
        // Second call immediately after should throttle
        // We can't easily verify the internal side effects without more complex mocking,
        // but we ensure it doesn't crash and returns Ok.
        let res = manager.monitor_and_optimize().await;
        assert!(res.is_ok());
    }
}
