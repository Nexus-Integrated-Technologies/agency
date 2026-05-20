use anyhow::Result;

fn main() -> Result<()> {
    rust_agency::run_nanoclaw_cli(std::env::args().skip(1))
}
