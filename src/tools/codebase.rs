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
    project_root: PathBuf,
}

impl CodebaseTool {
    const MAX_SEARCH_MATCHES: usize = 100;

    pub fn new(src_dir: impl Into<PathBuf>) -> Self {
        let path = src_dir.into();
        // Try to get absolute path if possible for better safety checks
        let src_dir = std::fs::canonicalize(&path).unwrap_or(path);
        let project_root = src_dir
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| src_dir.clone());
        Self {
            src_dir,
            project_root,
        }
    }

    fn is_allowed_root_file(path: &Path) -> bool {
        path.file_name()
            .and_then(|name| name.to_str())
            .map(|name| matches!(name, "Cargo.toml" | "Cargo.lock" | ".gitignore"))
            .unwrap_or(false)
    }

    fn resolve_path_for_validation(path: &Path) -> Option<PathBuf> {
        if let Ok(canonical) = std::fs::canonicalize(path) {
            return Some(canonical);
        }

        let parent = path.parent()?;
        let canonical_parent = std::fs::canonicalize(parent).ok()?;
        let file_name = path.file_name()?;
        Some(canonical_parent.join(file_name))
    }

    fn is_safe_path(&self, path: &Path) -> bool {
        let canonical = match Self::resolve_path_for_validation(path) {
            Some(path) => path,
            None => return false,
        };

        if canonical.starts_with(&self.src_dir) {
            return true;
        }

        canonical.parent() == Some(self.project_root.as_path()) && Self::is_allowed_root_file(&canonical)
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
            let mut discovered = Vec::new();
            while let Some(entry) = entries.next_entry().await.map_err(AgentError::Io)? {
                discovered.push(entry.path());
            }
            discovered.sort();
            for path in discovered {
                if path.is_dir() {
                    if !Self::should_skip_dir(&path) {
                        dirs.push(path);
                    }
                    continue;
                }
                files.push(path);
            }
        }

        // Stable ordering keeps list/search output deterministic for callers and tests.
        files.sort();
        Ok(files)
    }

    fn to_display_path(&self, path: &Path) -> String {
        path
            .strip_prefix(&self.src_dir)
            .unwrap_or(path)
            .to_string_lossy()
            .to_string()
    }

    async fn execute_list_files(&self) -> AgentResult<ToolOutput> {
        let files = self
            .collect_files()
            .await
            .map_err(|e| AgentError::Tool(format!("Failed to collect files: {}", e)))?;
        let file_strings: Vec<String> = files
            .iter()
            .map(|path| self.to_display_path(path))
            .collect();
        let mut tree_summary = String::from("Codebase Files:\n");
        for f in &file_strings {
            tree_summary.push_str(&format!("- {}\n", f));
        }

        Ok(ToolOutput::success(json!({ "files": file_strings }), tree_summary))
    }

    async fn execute_read_file(&self, rel_path: &str) -> AgentResult<ToolOutput> {
        // Construct the path carefully.
        let path = self.src_dir.join(rel_path);

        // Validate access boundaries before touching the filesystem.
        if !self.is_safe_path(&path) {
            return Ok(ToolOutput::failure("Access denied: Path outside allowed areas"));
        }

        // Report missing files after access validation to avoid leaking information.
        if !path.exists() {
            return Ok(ToolOutput::failure(format!("File not found: {}", rel_path)));
        }

        let content = match fs::read_to_string(&path).await {
            Ok(c) => c,
            Err(e) => {
                return Ok(ToolOutput::failure(format!(
                    "Failed to read {}: {}",
                    rel_path, e
                )));
            }
        };

        Ok(ToolOutput::success(
            json!({ "path": rel_path, "content": content }),
            format!("Content of {}:\n\n{}", rel_path, content),
        ))
    }

    async fn execute_search(&self, query: &str) -> AgentResult<ToolOutput> {
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
            let display_path = self.to_display_path(&path);

            for (line_number, line) in content.lines().enumerate() {
                if line.to_lowercase().contains(&query_lower) {
                    matches.push(json!({
                        "path": display_path,
                        "line": line_number + 1,
                        "content": line.trim(),
                    }));
                    if matches.len() >= Self::MAX_SEARCH_MATCHES {
                        break;
                    }
                }
            }
            if matches.len() >= Self::MAX_SEARCH_MATCHES {
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
            summary,
        ))
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
            "list_files" => self.execute_list_files().await,
            "read_file" => {
                let rel_path = params["path"]
                    .as_str()
                    .ok_or_else(|| AgentError::Validation("Missing path".to_string()))?;
                self.execute_read_file(rel_path).await
            }
            "search" => {
                let query = params["query"]
                    .as_str()
                    .ok_or_else(|| AgentError::Validation("Missing query".to_string()))?;
                self.execute_search(query).await
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
    use std::path::MAIN_SEPARATOR;

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
    async fn test_codebase_list_files_relative_and_sorted() {
        let dir = tempdir().expect("Failed to create temp dir");
        let src_path = dir.path().join("src");
        fs::create_dir(&src_path).await.expect("Failed to create src dir");

        let nested = src_path.join("nested");
        fs::create_dir(&nested).await.expect("Failed to create nested dir");

        File::create(src_path.join("zeta.rs")).expect("Failed to create zeta.rs");
        File::create(src_path.join("alpha.rs")).expect("Failed to create alpha.rs");
        File::create(nested.join("beta.rs")).expect("Failed to create beta.rs");

        let tool = CodebaseTool::new(&src_path);
        let res = tool
            .execute(json!({"action": "list_files"}))
            .await
            .expect("Tool execution failed");

        assert!(res.success);
        let files = res.data["files"].as_array().expect("No files in data");
        let strings: Vec<&str> = files
            .iter()
            .map(|f| f.as_str().expect("Not a string"))
            .collect();
        let nested_path = format!("nested{}beta.rs", MAIN_SEPARATOR);
        assert_eq!(strings, vec!["alpha.rs", nested_path.as_str(), "zeta.rs"]);
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

    #[tokio::test]
    async fn test_codebase_blocks_matching_filename_outside_project() {
        let root = tempdir().expect("Failed to create project root");
        let src_path = root.path().join("src");
        fs::create_dir(&src_path).await.expect("Failed to create src dir");

        let parent = root.path().parent().expect("Missing root parent");
        let external = tempfile::tempdir_in(parent).expect("Failed to create external dir");
        let external_cargo = external.path().join("Cargo.toml");
        let mut file = File::create(&external_cargo).expect("Failed to create external Cargo.toml");
        writeln!(file, "[package]").expect("Failed to write external Cargo.toml");

        let tool = CodebaseTool::new(&src_path);
        let traversal = format!(
            "../../{}/Cargo.toml",
            external.path().file_name().expect("Missing file name").to_string_lossy()
        );
        let res = tool
            .execute(json!({
                "action": "read_file",
                "path": traversal
            }))
            .await
            .expect("Tool execution failed");

        assert!(!res.success);
        assert!(res.summary.contains("Access denied"));
    }

    #[tokio::test]
    async fn test_codebase_reports_missing_file_inside_project() {
        let dir = tempdir().expect("Failed to create temp dir");
        let src_path = dir.path().join("src");
        fs::create_dir(&src_path).await.expect("Failed to create src dir");

        let tool = CodebaseTool::new(&src_path);
        let res = tool
            .execute(json!({
                "action": "read_file",
                "path": "missing.rs"
            }))
            .await
            .expect("Tool execution failed");

        assert!(!res.success);
        assert!(res.summary.contains("File not found"));
    }
}
