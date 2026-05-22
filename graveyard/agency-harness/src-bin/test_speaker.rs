use rust_agency::agent::speaker_rs::Speaker;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut speaker = Speaker::new()?;
    speaker.say("Hello. This is a test of the 100% native engine.").await?;
    Ok(())
}