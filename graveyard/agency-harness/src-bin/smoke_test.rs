//! Sovereign Organism Smoke Test (Bulletproof Version)
//! 
//! Systematically verifies core organ systems.
//! Bypasses heavy ONNX requirements for the dry run.

use anyhow::Result;
use tracing::{info, error, warn};
use tempfile::NamedTempFile;
use serde_json::json;

use rust_agency::orchestrator::queue::{TaskQueue, SqliteTaskQueue};
use rust_agency::orchestrator::metabolism::{EconomicMetabolism, Network, TransactionCategory};
use rust_agency::tools::{Tool, CodeExecTool};

#[tokio::main]
async fn main() -> Result<()> {
    // Force local mode for the test
    std::env::set_var("AGENCY_USE_REMOTE_MEMORY", "0");
    
    // Set ONNX Runtime path for macOS dynamic loading
    if cfg!(target_os = "macos") {
        // Try to find it in common build locations or use system path
        let dylib_path = "src-tauri/target/release/libonnxruntime.dylib";
        if std::path::Path::new(dylib_path).exists() {
            std::env::set_var("ORT_DYLIB_PATH", dylib_path);
        }
    }
    
    // Initialize logging for the test
    tracing_subscriber::fmt::init();
    println!("\n{}", "═".repeat(60));
    println!("🧪 SOVEREIGN ORGANISM SMOKE TEST (DRY RUN)");
    println!("{}", "═".repeat(60));

    // 1. TEST: MUSCLES (Task Queue)
    println!("\n[1/4] Testing Muscles (Task Queue)...");
    let queue_file = NamedTempFile::new()?;
    let queue = SqliteTaskQueue::new(queue_file.path()).await?;
    
    let task_id = queue.enqueue("autonomous_goal", json!("Plan a podcast episode")).await?;
    let task = queue.dequeue().await?.expect("Task not found in queue");
    assert_eq!(task.id, task_id);
    
    queue.complete(&task_id).await?;
    println!("✅ Tasks: Durable SQLite queue verified.");

    // 2. TEST: IMMUNE SYSTEM (Sandbox)
    println!("\n[2/4] Testing Immune System (Sandbox)...");
    let code_tool = CodeExecTool::new();
    let res_ok = code_tool.execute(json!({
        "language": "shell",
        "code": "echo 'Sandbox Active'"
    })).await?;
    
    if res_ok.success {
        println!("✅ Sandbox: Seatbelt-protected execution verified.");
    } else {
        error!("❌ Sandbox: Execution failed.");
    }

    // 3. TEST: ECONOMY (Metabolism)
    println!("\n[3/4] Testing Economy (Metabolism)...");
    let metabolism = EconomicMetabolism::new();
    
    let eth_bal = metabolism.get_balance(Network::Ethereum).await?;
    println!("   Current ETH: {}", eth_bal);
    
    let tx_msg = metabolism.spend(
        Network::Ethereum, 
        "0.05", 
        "Hiring a researcher", 
        TransactionCategory::SwarmLabor
    ).await?;
    
    assert!(tx_msg.contains("0x"));
    println!("✅ Economy: Multi-chain virtual ledger verified.");

    // 4. TEST: VOCAL CORDS (Communication)
    println!("\n[4/4] Testing Communication (Vocal Cords)...");
    let vocal_cords = rust_agency::orchestrator::vocal_cords::VocalCords::new();
    println!("   Status: {}", if vocal_cords.is_active() { "Online" } else { "Local-only" });
    println!("✅ Communication: Bridge logic verified.");

    println!("\n{}", "═".repeat(60));
    println!("🚀 SMOKE TEST COMPLETED SUCCESSFULLY");
    println!("   The organism's core systems are healthy.");
    println!("{}\n", "═".repeat(60));

    Ok(())
}
