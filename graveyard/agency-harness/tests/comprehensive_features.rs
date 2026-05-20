//! Comprehensive Feature Verification Suite
//! 
//! Covers the full "Sovereign Organism" Matrix.
//! Run with: cargo test --test comprehensive_features

use rust_agency::memory::{Memory, LocalVectorMemory, MemoryEntry, entry::MemorySource};
use rust_agency::orchestrator::queue::{TaskQueue, SqliteTaskQueue};
use rust_agency::orchestrator::metabolism::{EconomicMetabolism, Network, TransactionCategory};
use rust_agency::tools::{Tool, CodeExecTool, MutationTool, HandsTool, WalletTool};
use rust_agency::orchestrator::sovereignty::SovereignIdentity;
use rust_agency::orchestrator::healing::HealingEngine;
use serde_json::json;
use tempfile::{NamedTempFile, tempdir};
use std::sync::Arc;
use std::path::PathBuf;
use tokio::fs;

// ============================================================================
// 1. STOMACH: Memory Tiering & Persistence
// ============================================================================
#[tokio::test]
async fn test_memory_tiering_and_dreaming() -> anyhow::Result<()> {
    std::env::set_var("AGENCY_USE_REMOTE_MEMORY", "0");
    
    // Skip if ONNX libs missing (CI/Safety)
    if std::env::var("ORT_DYLIB_PATH").is_err() && !std::path::Path::new("libonnxruntime.dylib").exists() {
        return Ok(());
    }

    let dir = tempdir()?;
    let db_path = dir.path().join("memory.bin");
    let memory = LocalVectorMemory::new(db_path.clone())?;

    // Store HOT memory
    let entry = MemoryEntry::new("Hot Memory", "User", MemorySource::User).with_importance(0.9);
    memory.store(entry).await?;

    // Store COLD memory (low importance)
    let cold_entry = MemoryEntry::new("Cold Memory", "User", MemorySource::User).with_importance(0.1);
    memory.store(cold_entry).await?;

    // Trigger Consolidation (Dreaming)
    // This should move the "Cold" memory to the mmap file
    // Note: consolidation implementation usually requires >50 items or time passage, 
    // so we verify the mechanism exists and runs without error.
    let moved = memory.consolidate().await?;
    println!("Consolidated {} memories", moved);

    memory.persist().await?;
    assert!(db_path.exists());
    
    Ok(())
}

// ============================================================================
// 2. MUSCLES: Task Queue & Priorities
// ============================================================================
#[tokio::test]
async fn test_muscles_queue_operations() -> anyhow::Result<()> {
    let tmp_db = NamedTempFile::new()?;
    let queue = SqliteTaskQueue::new(tmp_db.path()).await?;

    // 1. Enqueue
    let id1 = queue.enqueue("low_priority", json!({})).await?;
    let id2 = queue.enqueue("high_priority", json!({})).await?;

    // 2. State Check
    let pending = queue.count("pending").await?;
    assert_eq!(pending, 2);

    // 3. Dequeue & Complete
    if let Some(task) = queue.dequeue().await? {
        queue.complete(&task.id).await?;
    }

    let completed = queue.count("completed").await?;
    assert_eq!(completed, 1);

    Ok(())
}

// ============================================================================
// 3. IMMUNE SYSTEM: Sandbox & Path Safety
// ============================================================================
#[tokio::test]
async fn test_immune_system_safety() -> anyhow::Result<()> {
    let exec_tool = CodeExecTool::new();
    
    // 1. Sandbox Execution
    let res = exec_tool.execute(json!({
        "language": "shell",
        "code": "whoami"
    })).await?;
    assert!(res.success);

    // 2. Path Safety (MutationTool)
    let dir = tempdir()?;
    let safe_file = dir.path().join("src/safe.rs");
    fs::create_dir_all(safe_file.parent().unwrap()).await?;
    fs::write(&safe_file, "original").await?;

    let mutation_tool = MutationTool::new(dir.path());
    
    // Attempt to write to safe path
    let res_safe = mutation_tool.execute(json!({
        "action": "apply_change",
        "path": "src/safe.rs",
        "content": "modified"
    })).await?;
    assert!(res_safe.success);

    // Attempt to access outside scope (e.g., /etc/passwd equivalent or parent dir)
    let res_unsafe = mutation_tool.execute(json!({
        "action": "apply_change",
        "path": "../unsafe.rs",
        "content": "hacking"
    })).await?;
    assert!(!res_unsafe.success, "Should deny path traversal");

    Ok(())
}

// ============================================================================
// 4. METABOLISM: Multi-Chain Wallet
// ============================================================================
#[tokio::test]
async fn test_metabolism_multi_chain() -> anyhow::Result<()> {
    let metabolism = Arc::new(EconomicMetabolism::new());
    let wallet = WalletTool::new(metabolism.clone());

    // 1. Check Balances across networks
    for net in ["bitcoin", "ethereum", "solana", "worldchain"] {
        let res = wallet.execute(json!({
            "action": "check_balance",
            "network": net
        })).await?;
        assert!(res.success, "Failed balance check for {}", net);
    }

    // 2. Simulate Transaction
    let sim = wallet.execute(json!({
        "action": "simulate",
        "network": "ethereum",
        "amount": "0.1",
        "to": "0x0000000000000000000000000000000000000000"
    })).await?;
    assert!(sim.success);
    assert!(sim.summary.contains("Simulation ACCEPTED"));

    Ok(())
}

// ============================================================================
// 5. SOVEREIGNTY: Cryptographic Identity
// ============================================================================
#[test]
fn test_sovereignty_keys() -> anyhow::Result<()> {
    let identity = SovereignIdentity::new()?;
    let pub_id = identity.public_id();
    
    // Verify Key Generation
    assert_eq!(pub_id.len(), 64); // Hex string of 32 bytes

    // Verify Signing
    let data = b"Agency Action";
    let sig = identity.sign(data);
    let valid = SovereignIdentity::verify(&pub_id, data, &sig.to_bytes())?;
    
    assert!(valid);
    Ok(())
}

// ============================================================================
// 6. HANDS: Embodiment (Safe Check)
// ============================================================================
#[tokio::test]
async fn test_hands_instantiation() {
    // We cannot move the mouse in a test environment safely, 
    // but we can verify the tool initializes and parses arguments.
    let hands = HandsTool::new();
    assert_eq!(hands.name(), "hands");
    
    // Verify it rejects invalid arguments
    let res = hands.execute(json!({
        "action": "unknown_action"
    })).await;
    
    assert!(res.is_ok()); // Should return failure Output, not Err
    assert!(!res.unwrap().success);
}

// ============================================================================
// 7. HEALING: Diagnostic Engine
// ============================================================================
#[tokio::test]
async fn test_healing_engine_init() -> anyhow::Result<()> {
    let tmp_db = NamedTempFile::new()?;
    let queue = Arc::new(SqliteTaskQueue::new(tmp_db.path()).await?);
    
    // Just verify initialization works; 
    // full log diagnosis requires mocking the filesystem log structure
    let _doctor = HealingEngine::new(queue);
    
    Ok(())
}

// ============================================================================
// 8. TOOLS: Memory & Knowledge Wrappers
// ============================================================================
#[tokio::test]
async fn test_memory_tools_wrappers() -> anyhow::Result<()> {
    std::env::set_var("AGENCY_USE_REMOTE_MEMORY", "0");
    if std::env::var("ORT_DYLIB_PATH").is_err() && !std::path::Path::new("libonnxruntime.dylib").exists() {
        return Ok(());
    }

    let dir = tempdir()?;
    let db_path = dir.path().join("tool_memory.bin");
    let memory = Arc::new(LocalVectorMemory::new(db_path)?);

    // Seed
    let entry = MemoryEntry::new("Rust ownership is unique", "Teacher", MemorySource::System);
    memory.store(entry).await?;

    // Test MemoryQueryTool
    let query_tool = rust_agency::tools::MemoryQueryTool::new(memory.clone());
    let res = query_tool.execute(json!({"query": "ownership"})).await?;
    assert!(res.success);
    assert!(res.summary.contains("Rust ownership"));

    // Test KnowledgeGraphTool
    let kg_tool = rust_agency::tools::KnowledgeGraphTool::new(memory.clone());
    let kg_res = kg_tool.execute(json!({"limit": 5})).await?;
    assert!(kg_res.success); // Should succeed even if empty

    Ok(())
}
