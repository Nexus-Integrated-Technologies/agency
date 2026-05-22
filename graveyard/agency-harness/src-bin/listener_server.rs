use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    rust_agency::services::listener::run_listener_server().await
}
