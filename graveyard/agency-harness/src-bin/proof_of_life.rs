//! Proof of Life (Production Grade Spectrum)
//! 
//! Real-time demonstration of the organism's senses, multi-chain
//! metabolism, and production-grade network simulation.

use anyhow::Result;
use serde_json::json;
use std::sync::Arc;
use rust_agency::tools::{Tool, VisionTool, WalletTool};
use rust_agency::orchestrator::metabolism::EconomicMetabolism;

#[tokio::main]
async fn main() -> Result<()> {
    std::env::set_var("ORT_STRATEGY", "download");

    println!("\n{}", "═".repeat(60));
    println!("🧬 AGENCY: PRODUCTION SPECTRUM PROOF OF LIFE");
    println!("{}", "═".repeat(60));

    // 1. SIGHT
    let vision = VisionTool::new();
    let _ = vision.execute(json!({"action": "capture_screen"})).await?;
    let describe_res = vision.execute(json!({
        "action": "describe",
        "prompt": "Briefly describe the screen contents."
    })).await?;
    println!("\n👀 SIGHT: \"{}\"", describe_res.summary);

    // 2. METABOLISM (Live Balances)
    let metabolism = Arc::new(EconomicMetabolism::new());
    let wallet = WalletTool::new(metabolism.clone());
    
    println!("\n💰 LIVE METABOLIC CHECK (All Artery Connections):");
    let networks = vec!["bitcoin", "ethereum", "solana", "base", "worldchain"];
    for net in networks {
        match wallet.execute(json!({"action": "check_balance", "network": net})).await {
            Ok(res) => println!("   - {:<12} {}", net.to_uppercase(), res.summary),
            Err(e) => println!("   - {:<12} ❌ Failed: {}", net.to_uppercase(), e),
        }
    }

    // 3. PULSE SIMULATION (The Artery Check)
    println!("\n💓 LIVE PULSE SIMULATION (Artery Proof):");
    println!("   Simulating intention on WORLDCHAIN_SEPOLIA...");
    let sim_res = wallet.execute(json!({
        "action": "simulate",
        "network": "worldchain_sepolia",
        "amount": "0.001",
        "to": "0x742d35Cc6634C0532925a3b844Bc454e4438f44e"
    })).await?;
    println!("     Feedback: {}", sim_res.summary);

    // 4. BROADCAST (Production Artery Proof)
    println!("\n🖋️  LIVE NETWORK BROADCAST (Production Packets):");
    let test_nets = vec!["ethereum", "solana", "worldchain_sepolia"];
    for net in test_nets {
        println!("   Attempting production-grade broadcast to {}...", net.to_uppercase());
        match wallet.execute(json!({
            "action": "send_testnet",
            "network": net,
            "amount": "0.001",
            "to": "0x742d35Cc6634C0532925a3b844Bc454e4438f44e"
        })).await {
            Ok(res) => println!("     Node Feedback: {}", res.summary),
            Err(e) => println!("     ❌ Network Error: {}", e),
        }
    }

    println!("\n{}", "═".repeat(60));
    println!("✅ PRODUCTION SPECTRUM PROOF COMPLETE");
    println!("{}", "═".repeat(60));

    Ok(())
}