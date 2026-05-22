use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    rust_agency::services::speaker::run_speaker_server().await
}