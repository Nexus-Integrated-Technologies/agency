//! Load Test Suite (Stress Testing)
//! 
//! Pushes the organism to its limits to verify stability under load.
//! Run with: cargo test --test load_test --release

use rust_agency::orchestrator::queue::{TaskQueue, SqliteTaskQueue};
use rust_agency::memory::{LocalVectorMemory, MemoryEntry, entry::MemorySource, Memory};
use rust_agency::orchestrator::metabolism::{EconomicMetabolism, Network, TransactionCategory};
use tempfile::NamedTempFile;
use serde_json::json;
use std::sync::Arc;
use tokio::time::Instant;

// 1. MUSCLE STRESS: High-throughput Task Queue
#[tokio::test]
async fn test_queue_throughput() -> anyhow::Result<()> {
    let tmp = NamedTempFile::new()?;
    let queue = Arc::new(SqliteTaskQueue::new(tmp.path()).await?);
    
    let count = 10_000;
    println!("\n🏋️  MUSCLE STRESS: Enqueuing {} tasks...", count);
    
    let start = Instant::now();
    
    // Serial enqueue (SQLite handles concurrency via WAL, but connection is usually single writer)
    // We test the raw write speed of the abstraction layer
    for i in 0..count {
        queue.enqueue("load_test", json!({"i": i})).await?;
    }
    
    let duration = start.elapsed();
    println!("   -> Time: {:.2?}", duration);
    println!("   -> Rate: {:.0} tasks/sec", count as f64 / duration.as_secs_f64());
    
    let pending = queue.count("pending").await?;
    assert_eq!(pending as usize, count);
    
    Ok(())
}

// 2. STOMACH STRESS: Parallel Memory Insertion
#[tokio::test]
async fn test_memory_concurrency() -> anyhow::Result<()> {
    // Force local
    std::env::set_var("AGENCY_USE_REMOTE_MEMORY", "0");
    std::env::set_var("ORT_STRATEGY", "download");
    
    // Skip if ONNX missing (don't break CI)
    if std::env::var("ORT_DYLIB_PATH").is_err() && !std::path::Path::new("libonnxruntime.dylib").exists() {
        println!("⚠️ Skipping memory load test (ONNX missing).");
        return Ok(())
    }

    let dir = tempfile::tempdir()?;
    let path = dir.path().join("load_test.mem");
    let memory = Arc::new(LocalVectorMemory::new(path)?);
    
    let count = 100; // Lower count because embeddings are heavy CPU work
    println!("\n🥪 STOMACH STRESS: Embedding & Storing {} memories in parallel...", count);
    
    let start = Instant::now();
    
    let mut handles = Vec::new();
    for i in 0..count {
        let m = memory.clone();
        handles.push(tokio::spawn(async move {
            let entry = MemoryEntry::new(
                format!("Load test memory entry #{}", i),
                "StressTester",
                MemorySource::System
            );
            m.store(entry).await
        }));
    }
    
    for h in handles {
        h.await??;
    }
    
    let duration = start.elapsed();
    println!("   -> Time: {:.2?}", duration);
    println!("   -> Rate: {:.0} memories/sec", count as f64 / duration.as_secs_f64());
    
    assert_eq!(memory.count().await?, count);
    
    Ok(())
}

// 3. METABOLISM STRESS: Ledger Contention
#[tokio::test]
async fn test_metabolism_contention() -> anyhow::Result<()> {
    let metabolism = Arc::new(EconomicMetabolism::new());
    let count = 5_000;
    
    println!("\n🔥 METABOLISM STRESS: {} concurrent transaction requests...", count);
    
    let start = Instant::now();
    
    let mut handles = Vec::new();
    for _ in 0..count {
        let m = metabolism.clone();
        handles.push(tokio::spawn(async move {
            m.spend(
                Network::Bitcoin,
                "0.0001",
                "Micro-transaction",
                TransactionCategory::SwarmLabor
            ).await
        }));
    }
    
    let mut successes = 0;
    for h in handles {
        if h.await?.is_ok() {
            successes += 1;
        }
    }
    
    let duration = start.elapsed();
    println!("   -> Time: {:.2?}", duration);
    println!("   -> Rate: {:.0} tx/sec", count as f64 / duration.as_secs_f64());
    
    // We expect some failures due to "Insufficient Funds" after we drain the wallet
    // Initial BTC is 10000. We spend 0.0001 * 5000 = 0.5 BTC.
    // So all should actually succeed!
    assert_eq!(successes, count);
    
    Ok(())
}
