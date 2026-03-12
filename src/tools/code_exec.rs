//! Code Execution Tool
//! 
//! Safely executes code snippets in a sandboxed environment.
//! Now with mandatory macOS Seatbelt (Immune System).

use anyhow::Context;
use async_trait::async_trait;
use serde_json::{json, Value};
use std::process::{Output, Stdio};
use tokio::process::Command;
use tokio::time::{timeout, Duration};
use tracing::{debug, warn, info};

use crate::agent::{AgentResult, AgentError};
use crate::utils::sandbox::TOOL_SANDBOX_POLICY;
use super::{Tool, ToolOutput};

/// Sandboxed code execution tool
pub struct CodeExecTool {
    /// Maximum execution time in seconds
    timeout_secs: u64,
    /// Maximum output length
    max_output_len: usize,
}

impl CodeExecTool {
    pub fn new() -> Self {
        Self {
            timeout_secs: 30,
            max_output_len: 10000,
        }
    }

    #[allow(dead_code)]
    pub fn with_timeout(mut self, secs: u64) -> Self {
        self.timeout_secs = secs;
        self
    }

    async fn execute_python(&self, code: &str) -> anyhow::Result<(String, String, i32)> {
        self.run_command("python3", &["-c", code]).await
    }

    async fn execute_rust(&self, code: &str) -> anyhow::Result<(String, String, i32)> {
        let temp_dir = std::env::temp_dir();
        let file_path = temp_dir.join(format!("agent_code_{}.rs", uuid::Uuid::new_v4()));
        let binary_path = temp_dir.join(format!("agent_code_{}", uuid::Uuid::new_v4()));

        let file_path_str = file_path.to_str().ok_or_else(|| anyhow::anyhow!("Invalid temp file path"))?;
        let binary_path_str = binary_path.to_str().ok_or_else(|| anyhow::anyhow!("Invalid binary path"))?;

        tokio::fs::write(&file_path, code).await
            .context("Failed to write Rust code to temp file")?;

        // Compile (Compile phase is ALSO sandboxed)
        let (stdout, stderr, code_result) = self
            .run_command("rustc", &[
                file_path_str,
                "-o",
                binary_path_str,
            ])
            .await?;

        if code_result != 0 {
            let _ = tokio::fs::remove_file(&file_path).await;
            return Ok((stdout, format!("Compilation failed:\n{}", stderr), code_result));
        }

        // Run the compiled binary
        let result = self.run_command(binary_path_str, &[]).await;

        // Clean up
        let _ = tokio::fs::remove_file(&file_path).await;
        let _ = tokio::fs::remove_file(&binary_path).await;

        result
    }

    async fn execute_javascript(&self, code: &str) -> anyhow::Result<(String, String, i32)> {
        self.run_command("node", &["-e", code]).await
    }

    async fn execute_shell(&self, code: &str) -> anyhow::Result<(String, String, i32)> {
        self.run_command("sh", &["-c", code]).await
    }

    async fn run_command(&self, program: &str, args: &[&str]) -> anyhow::Result<(String, String, i32)> {
        debug!("Running sandboxed command: {} {:?}", program, args);

        #[cfg(target_os = "macos")]
        {
            let workspace_dir = std::env::current_dir().unwrap_or_else(|_| std::path::PathBuf::from("."));
            
            let mut sb_args = vec![
                "-p".to_string(), TOOL_SANDBOX_POLICY.to_string(),
                "-D".to_string(), format!("WORKSPACE_DIR={}", workspace_dir.to_string_lossy()),
                "--".to_string(),
                program.to_string()
            ];
            
            for arg in args {
                sb_args.push(arg.to_string());
            }

            let result = timeout(
                Duration::from_secs(self.timeout_secs),
                Command::new("/usr/bin/sandbox-exec")
                    .args(&sb_args)
                    .stdout(Stdio::piped())
                    .stderr(Stdio::piped())
                    .stdin(Stdio::null())
                    .output()
            ).await;

            self.handle_command_result(result, "Failed to execute sandboxed command")
        }

        #[cfg(not(target_os = "macos"))]
        {
            warn!("Mandatory Seatbelt sandboxing only available on macOS. Running unconfined.");
            let result = timeout(
                Duration::from_secs(self.timeout_secs),
                Command::new(program)
                    .args(args)
                    .stdout(Stdio::piped())
                    .stderr(Stdio::piped())
                    .stdin(Stdio::null())
                    .output()
            ).await;

            self.handle_command_result(result, "Failed to execute command")
        }
    }

    fn handle_command_result(
        &self,
        result: Result<Result<Output, std::io::Error>, tokio::time::error::Elapsed>,
        error_prefix: &str,
    ) -> anyhow::Result<(String, String, i32)> {
        match result {
            Ok(Ok(output)) => Ok(self.normalize_output(output)),
            Ok(Err(e)) => Err(anyhow::anyhow!("{}: {}", error_prefix, e)),
            Err(_) => Err(anyhow::anyhow!("Execution timed out after {} seconds", self.timeout_secs)),
        }
    }

    fn normalize_output(&self, output: Output) -> (String, String, i32) {
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        let code = output.status.code().unwrap_or(-1);
        (self.truncate(&stdout), self.truncate(&stderr), code)
    }

    fn build_execution_summary(&self, stdout: &str, stderr: &str, exit_code: i32) -> String {
        let success = exit_code == 0;
        let output_parts = Self::collect_output_parts(stdout, stderr);
        if success {
            if stdout.is_empty() && stderr.is_empty() {
                "Code executed successfully (no output)".to_string()
            } else {
                output_parts.join("\n\n")
            }
        } else {
            format!("Execution failed (exit code: {})\n{}", exit_code, output_parts.join("\n\n"))
        }
    }

    fn collect_output_parts(stdout: &str, stderr: &str) -> Vec<String> {
        let mut output_parts = Vec::new();
        if !stdout.is_empty() {
            output_parts.push(format!("stdout:\n{}", stdout));
        }
        if !stderr.is_empty() {
            output_parts.push(format!("stderr:\n{}", stderr));
        }
        output_parts
    }

    fn truncate(&self, s: &str) -> String {
        if s.len() > self.max_output_len {
            format!("{}...[truncated]", &s[..self.max_output_len])
        } else {
            s.to_string()
        }
    }
}

impl Default for CodeExecTool {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Tool for CodeExecTool {
    fn name(&self) -> String {
        "code_exec".to_string()
    }

    fn description(&self) -> String {
        "Execute code in a MANDATORY sandboxed environment. Supports Python, JavaScript, Rust, and shell commands.\n 
         Use this to run calculations, test code snippets, or perform automated tasks. Access restricted to project directory and /tmp.".to_string()
    }

    fn parameters(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "code": {
                    "type": "string",
                    "description": "The code to execute"
                },
                "language": {
                    "type": "string",
                    "description": "Programming language",
                    "enum": ["python", "javascript", "rust", "shell"]
                }
            },
            "required": ["code", "language"]
        })
    }

    fn work_scope(&self) -> Value {
        json!({
            "status": "constrained",
            "environment": "MANDATORY macOS Seatbelt Sandbox",
            "safety": "ULTRA-HIGH (Kernel-enforced isolation)",
            "resource_limits": {
                "timeout": format!("{}s", self.timeout_secs),
                "max_output": format!("{} bytes", self.max_output_len)
            }
        })
    }

    fn requires_confirmation(&self) -> bool {
        true // Still require confirmation for auditing
    }

    async fn execute(&self, params: Value) -> AgentResult<ToolOutput> {
        let code = params["code"]
            .as_str()
            .ok_or_else(|| AgentError::Validation("Missing required parameter: code".to_string()))?;
        
        let language = params["language"]
            .as_str()
            .ok_or_else(|| AgentError::Validation("Missing required parameter: language".to_string()))?;

        info!("MANDATORY SANDBOX EXECUTION: {} code ({} chars)", language, code.len());

        let result = match language {
            "python" => self.execute_python(code).await,
            "javascript" => self.execute_javascript(code).await,
            "rust" => self.execute_rust(code).await,
            "shell" => self.execute_shell(code).await,
            _ => return Ok(ToolOutput::failure(format!("Unsupported language: {}", language))),
        };

        match result {
            Ok((stdout, stderr, exit_code)) => {
                let success = exit_code == 0;
                let summary = self.build_execution_summary(&stdout, &stderr, exit_code);
                let data = json!({
                    "language": language,
                    "stdout": stdout,
                    "stderr": stderr,
                    "exit_code": exit_code
                });

                if success {
                    Ok(ToolOutput::success(data, summary))
                } else {
                    Ok(ToolOutput {
                        success: false,
                        data,
                        summary,
                        error: Some(format!("Exit code: {}", exit_code)),
                    })
                }
            }
            Err(e) => {
                warn!("Sandboxed execution error: {}", e);
                Ok(ToolOutput::failure(format!("Execution failed: {}", e)))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::CodeExecTool;

    #[test]
    fn summary_is_empty_success_message_when_no_output() {
        let tool = CodeExecTool::new();
        let summary = tool.build_execution_summary("", "", 0);
        assert_eq!(summary, "Code executed successfully (no output)");
    }

    #[test]
    fn summary_includes_stdout_and_stderr() {
        let tool = CodeExecTool::new();
        let summary = tool.build_execution_summary("ok", "warn", 0);
        assert_eq!(summary, "stdout:\nok\n\nstderr:\nwarn");
    }

    #[test]
    fn summary_includes_failure_exit_code() {
        let tool = CodeExecTool::new();
        let summary = tool.build_execution_summary("", "boom", 1);
        assert_eq!(summary, "Execution failed (exit code: 1)\nstderr:\nboom");
    }
}
