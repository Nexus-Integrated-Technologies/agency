//! Codebase Indexer Tool
//! 
//! Allows agents to search and read the project's own source code.
//! This helps agents understand their own capabilities and tool definitions.

use async_trait::async_trait;
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use tokio::fs;

use crate::agent::{AgentResult, AgentError};
use super::{Tool, ToolOutput};

/// Tool for exploring the agency's own codebase
pub struct CodebaseTool {
    src_dir: PathBuf,
}

impl CodebaseTool {
    pub fn new(src_dir: impl Into<PathBuf>) -> Self {
        let path = src_dir.into();
        // Try to get absolute path if possible for better safety checks
        let src_dir = std::fs::canonicalize(&path).unwrap_or(path);
        Self { src_dir }
    }

    fn is_safe_path(&self, path: &Path) -> bool {
        // Canonicalize the input path to resolve ".." and symlinks
        let canonical = match std::fs::canonicalize(path) {
            Ok(p) => p,
            Err(_) => return false, // If it doesn't exist or can't be resolved, it's not safe to read
        };

        let path_str = canonical.to_string_lossy();
        
        canonical.starts_with(&self.src_dir) || 
        path_str.contains("rust_agency/src") ||
        path_str.ends_with("Cargo.toml") ||
        path_str.ends_with("Cargo.lock") ||
        path_str.ends_with(".gitignore")
    }

    fn should_skip_dir(path: &Path) -> bool {
        path.file_name()
            .and_then(|name| name.to_str())
            .map(|name| matches!(name, "target" | ".git" | ".fastembed_cache"))
            .unwrap_or(false)
    }

    async fn collect_files(&self) -> AgentResult<Vec<PathBuf>> {
        let mut files = Vec::new();
        let mut dirs = vec![self.src_dir.clone()];

        while let Some(dir) = dirs.pop() {
            let mut entries = match fs::read_dir(dir).await {
                Ok(e) => e,
                Err(e) => return Err(AgentError::Io(e)),
            };
            while let Some(entry) = entries.next_entry().await.map_err(AgentError::Io)? {
                let path = entry.path();
                if path.is_dir() {
                    if !Self::should_skip_dir(&path) {
                        dirs.push(path);
                    }
                } else {
                    files.push(path);
                }
            }
        }

        Ok(files)
    }
}

impl Default for CodebaseTool {
    fn default() -> Self {
        Self::new("src")
    }
}

#[async_trait]
impl Tool for CodebaseTool {
    fn name(&self) -> String {
        "codebase_explorer".to_string()
    }

    fn description(&self) -> String {
        "Explore and analyze the current project's codebase. \n        Supports 'list_files', 'read_file', and 'search' operations.".to_string()
    }

    fn parameters(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "enum": ["list_files", "read_file", "search"],
                    "description": "The action to perform"
                },
                "path": {
                    "type": "string",
                    "description": "File path (if action is 'read_file')"
                },
                "query": {
                    "type": "string",
                    "description": "Search query (if action is 'search')"
                }
            },
            "required": ["action"]
        })
    }

    fn work_scope(&self) -> Value {
        json!({
            "status": "constrained",
            "environment": "local project root",
            "access": "read-only",
            "data_scope": "source code and configuration"
        })
    }

    async fn execute(&self, params: Value) -> AgentResult<ToolOutput> {
        let action = params["action"].as_str().unwrap_or("list_files");

        match action {
            "list_files" => {
                let files = self
                    .collect_files()
                    .await
                    .map_err(|e| AgentError::Tool(format!("Failed to collect files: {}", e)))?;
                let file_strings: Vec<String> = files
                    .iter()
                    .map(|path| path.to_string_lossy().to_string())
                    .collect();
                let mut tree_summary = String::from("Codebase Files:\n");
                for f in &file_strings {
                    tree_summary.push_str(&format!("- {}\n", f));
                }
                
                Ok(ToolOutput::success(json!({ "files": file_strings }), tree_summary))
            },
            "read_file" => {
                let rel_path = params["path"].as_str().ok_or_else(|| AgentError::Validation("Missing path".to_string()))?;
                
                // Construct the path carefully
                let path = self.src_dir.join(rel_path);
                
                // SAFETY CHECK FIRST
                if !self.is_safe_path(&path) {
                    return Ok(ToolOutput::failure("Access denied: Path outside allowed areas"));
                }

                // Check existence after safety check
                if !path.exists() {
                    return Ok(ToolOutput::failure(format!("File not found: {}", rel_path)));
                }

                let content = match fs::read_to_string(&path).await {
                    Ok(c) => c,
                    Err(e) => return Ok(ToolOutput::failure(format!("Failed to read {}: {}", rel_path, e))),
                };
                
                Ok(ToolOutput::success(
                    json!({ "path": rel_path, "content": content }),
                    format!("Content of {}:\n\n{}", rel_path, content)
                ))
            },
            "search" => {
                let query = params["query"]
                    .as_str()
                    .ok_or_else(|| AgentError::Validation("Missing query".to_string()))?;
                let query_lower = query.to_lowercase();
                let files = self
                    .collect_files()
                    .await
                    .map_err(|e| AgentError::Tool(format!("Failed to collect files: {}", e)))?;

                let mut matches = Vec::new();
                for path in files {
                    let content = match fs::read_to_string(&path).await {
                        Ok(content) => content,
                        Err(_) => continue,
                    };
                    let display_path = path
                        .strip_prefix(&self.src_dir)
                        .unwrap_or(&path)
                        .to_string_lossy()
                        .to_string();

                    for (line_number, line) in content.lines().enumerate() {
                        if line.to_lowercase().contains(&query_lower) {
                            matches.push(json!({
                                "path": display_path,
                                "line": line_number + 1,
                                "content": line.trim(),
                            }));
                            if matches.len() >= 100 {
                                break;
                            }
                        }
                    }
                    if matches.len() >= 100 {
                        break;
                    }
                }

                let summary = if matches.is_empty() {
                    format!("No matches found for '{}'.", query)
                } else {
                    format!("Found {} matches for '{}'.", matches.len(), query)
                };
                let total_matches = matches.len();
                Ok(ToolOutput::success(
                    json!({
                        "query": query,
                        "matches": matches,
                        "total_matches": total_matches,
                    }),
                    summary
                ))
            }
            _ => Ok(ToolOutput::failure("Unsupported codebase action"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use std::fs::File;
    use std::io::Write;

    #[tokio::test]
    async fn test_codebase_list_files() {
        let dir = tempdir().expect("Failed to create temp dir");
        let src_path = dir.path().join("src");
        fs::create_dir(&src_path).await.expect("Failed to create src dir");
        
        let file_path = src_path.join("lib.rs");
        let mut file = File::create(file_path).expect("Failed to create lib.rs");
        writeln!(file, "fn main() {{}}").expect("Failed to write to lib.rs");
        
        // We need real absolute paths for canonicalize to work in tests
        let tool = CodebaseTool::new(src_path);
        let res = tool.execute(json!({"action": "list_files"})).await.expect("Tool execution failed");
        
        assert!(res.success);
        let files = res.data["files"].as_array().expect("No files in data");
        assert!(files.iter().any(|f| f.as_str().expect("Not a string").contains("lib.rs")));
    }

    #[tokio::test]
    async fn test_codebase_read_file() {
        let dir = tempdir().expect("Failed to create temp dir");
        let src_path = dir.path().join("src");
        fs::create_dir(&src_path).await.expect("Failed to create src dir");
        
        let file_path = src_path.join("lib.rs");
        let mut file = File::create(&file_path).expect("Failed to create lib.rs");
        let content = "pub fn hello() {}";
        writeln!(file, "{}", content).expect("Failed to write to lib.rs");
        
        let tool = CodebaseTool::new(&src_path);
        let res = tool.execute(json!({
            "action": "read_file",
            "path": "lib.rs"
        })).await.expect("Tool execution failed");
        
        assert!(res.success);
        assert!(res.data["content"].as_str().expect("No content in data").contains(content));
    }

    #[tokio::test]
    async fn test_codebase_safety() {
        let dir = tempdir().expect("Failed to create temp dir");
        let src_path = dir.path().join("src");
        fs::create_dir(&src_path).await.expect("Failed to create src dir");
        
        let tool = CodebaseTool::new(&src_path);
        
        // Attempt to read something outside using path traversal
        let res = tool.execute(json!({
            "action": "read_file",
            "path": "../secret.txt"
        })).await.expect("Tool execution failed");
        
        assert!(!res.success);
        assert!(res.summary.contains("Access denied"));
    }

    #[tokio::test]
    async fn test_codebase_search() {
        let dir = tempdir().expect("Failed to create temp dir");
        let src_path = dir.path().join("src");
        fs::create_dir(&src_path).await.expect("Failed to create src dir");

        let file_path = src_path.join("lib.rs");
        let mut file = File::create(&file_path).expect("Failed to create lib.rs");
        writeln!(file, "pub fn hello_world() {{}}").expect("Failed to write line");

        let tool = CodebaseTool::new(&src_path);
        let res = tool
            .execute(json!({
                "action": "search",
                "query": "hello_world"
            }))
            .await
            .expect("Tool execution failed");

        assert!(res.success);
        let total = res.data["total_matches"].as_u64().expect("Missing total_matches");
        assert!(total >= 1);
        let first = &res.data["matches"][0];
        assert!(first["path"].as_str().expect("Missing path").contains("lib.rs"));
    }
}
