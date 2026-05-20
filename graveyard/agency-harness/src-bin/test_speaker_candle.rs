use anyhow::Result;
use rust_agency::agent::Speaker;
use tracing_subscriber::FmtSubscriber;
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_env_filter("rust_agency=debug")
        .finish();
    tracing::subscriber::set_global_default(subscriber).ok();

    println!("🚀 Testing ASYNC PIPELINE High-Fidelity Speaker...");
    let mut speaker = Speaker::new()?;
    
    // CRITICAL: Initialize voice in async context
    speaker.init_default_voice().await?;
    
    let text = "First sentence is being generated now. Second sentence should follow immediately without any gaps. The third sentence confirms the pipeline is seamless.";
    println!("🗣️  Synthesizing: {}", text);
    
    let start = Instant::now();
    speaker.say(text).await?;
    println!("✅ speaker.say() completed in: {:?}", start.elapsed());
    
    println!("✅ Test complete!");
    Ok(())
}