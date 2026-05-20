use anyhow::Result;
use rust_agency::agent::Speaker;
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() -> Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_env_filter("rust_agency=debug")
        .finish();
    tracing::subscriber::set_global_default(subscriber).ok();

    println!("🚀 Testing Paralinguistics with High-Fidelity Speaker...");
    let mut speaker = Speaker::new()?;
    
    let text = "That's actually quite funny [laugh]. Anyway [um], we should continue with our task [chuckle].";
    println!("🗣️  Synthesizing with tags: {}", text);
    
    speaker.say(text).await?;
    
    // Give it time to play
    tokio::time::sleep(std::time::Duration::from_secs(8)).await;
    
    println!("✅ Test complete!");
    Ok(())
}