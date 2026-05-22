use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    rust_agency::services::memory::run_memory_server().await
}