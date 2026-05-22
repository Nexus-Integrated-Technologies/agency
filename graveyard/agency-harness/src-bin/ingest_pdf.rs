use anyhow::{Context, Result};
use std::path::Path;
use std::sync::Arc;
use tracing::info;
use rust_agency::memory::{VectorMemory, Memory, MemoryEntry};
use rust_agency::memory::entry::MemorySource;
use rust_agency::orchestrator::Kind;
use pdf_extract::extract_text;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env().add_directive(tracing::Level::INFO.into()))
        .init();

    let pdf_path = "data/BlacksLawDictionary.pdf";
    let memory_file = "memory.json";

    if !Path::new(pdf_path).exists() {
        anyhow::bail!("PDF file not found at {}", pdf_path);
    }

    info!("Initializing Vector Memory...");
    let memory: Arc<dyn Memory> = Arc::new(VectorMemory::new(memory_file).expect("Failed to init memory"));
    
    // Ensure memory is awake
    memory.wake().await?;

    info!("Extracting text from PDF: {}...", pdf_path);
    let text = extract_text(pdf_path).context("Failed to extract text from PDF")?;
    info!("Total characters extracted: {}", text.len());

    let chunks = chunk_text(&text, 1500, 200);
    info!("Generated {} chunks for indexing", chunks.len());

    let mut count = 0;
    for (i, chunk) in chunks.iter().enumerate() {
        if i % 100 == 0 {
            info!("Processing chunk {}/{}", i, chunks.len());
        }

        let mut entry = MemoryEntry::new(
            chunk,
            "PdfIngestor",
            MemorySource::Codebase // Reusing Codebase source for now or adding a Doc source
        );
        entry.metadata.context = "BlacksLawDictionary".to_string();
        entry.metadata.kind = Kind::Evidence;
        entry.metadata.tags.push("legal".to_string());
        entry.metadata.tags.push("dictionary".to_string());
        entry.metadata.grounding_holon = Some(format!("file://{}", pdf_path));

        memory.store(entry).await?;
        count += 1;

        // Yield to prevent blocking the executor too long
        if i % 10 == 0 {
            tokio::task::yield_now().await;
        }
    }

    info!("Persistence check...");
    memory.persist().await?;
    info!("Ingestion complete. {} chunks stored in vector memory.", count);

    Ok(())
}

fn chunk_text(text: &str, chunk_size: usize, overlap: usize) -> Vec<String> {
    let mut chunks = Vec::new();
    let words: Vec<&str> = text.split_whitespace().collect();
    
    let mut i = 0;
    while i < words.len() {
        let end = std::cmp::min(i + chunk_size, words.len());
        let chunk = words[i..end].join(" ");
        chunks.push(chunk);
        
        if end == words.len() {
            break;
        }
        
        i += chunk_size - overlap;
    }
    
    chunks
}
