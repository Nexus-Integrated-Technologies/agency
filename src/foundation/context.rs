use anyhow::{Context, Result};
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextDocument {
    pub path: PathBuf,
    pub body: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectContext {
    pub start_dir: PathBuf,
    pub documents: Vec<ContextDocument>,
}

impl ProjectContext {
    pub fn is_empty(&self) -> bool {
        self.documents.is_empty()
    }

    pub fn render(&self) -> String {
        let mut output = String::new();
        for document in &self.documents {
            output.push_str(&format!(
                "\n--- Context from {} ---\n",
                document.path.display()
            ));
            output.push_str(&document.body);
            if !document.body.ends_with('\n') {
                output.push('\n');
            }
        }
        output
    }
}

pub struct ContextLoader;

impl ContextLoader {
    pub fn load_project_context() -> Result<ProjectContext> {
        let cwd = std::env::current_dir().context("failed to determine current directory")?;
        Self::load_from(&cwd)
    }

    pub fn load_from(start: &Path) -> Result<ProjectContext> {
        let canonical_start = start.canonicalize().with_context(|| {
            format!("failed to canonicalize start directory {}", start.display())
        })?;
        let mut documents = Vec::new();

        for path in Self::discover_context_files(&canonical_start) {
            let body = fs::read_to_string(&path)
                .with_context(|| format!("failed to read context file {}", path.display()))?;
            documents.push(ContextDocument { path, body });
        }

        Ok(ProjectContext {
            start_dir: canonical_start,
            documents,
        })
    }

    pub fn discover_context_files(start: &Path) -> Vec<PathBuf> {
        let mut discovered = Vec::new();
        let mut current = Some(start);

        while let Some(dir) = current {
            if let Some(path) = Self::find_context_file(dir) {
                discovered.push(path);
            }
            current = dir.parent();
        }

        discovered.reverse();
        discovered
    }

    fn find_context_file(dir: &Path) -> Option<PathBuf> {
        const CANDIDATES: [&str; 4] = ["AGENTS.md", "CLAUDE.md", ".cursorrules", ".windsurfrules"];
        for candidate in CANDIDATES {
            let path = dir.join(candidate);
            if path.is_file() {
                return Some(path);
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::ContextLoader;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn loads_context_files_from_parent_chain() {
        let root = tempdir().unwrap();
        let subdir = root.path().join("project").join("src");
        fs::create_dir_all(&subdir).unwrap();
        fs::write(root.path().join("AGENTS.md"), "root rules").unwrap();
        fs::write(
            root.path().join("project").join("CLAUDE.md"),
            "project rules",
        )
        .unwrap();

        let context = ContextLoader::load_from(&subdir).unwrap();
        assert_eq!(context.documents.len(), 2);
        assert!(context.render().contains("root rules"));
        assert!(context.render().contains("project rules"));
    }
}
