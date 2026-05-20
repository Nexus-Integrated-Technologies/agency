use std::collections::BTreeMap;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use super::model_router::WorkerBackend;

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct GroupRuntimeConfig {
    pub provider: Option<String>,
    pub backend: Option<String>,
    pub model: Option<String>,
    pub effort: Option<String>,
    pub assistant_name: Option<String>,
    pub max_messages_per_prompt: Option<usize>,
}

impl GroupRuntimeConfig {
    pub fn from_json(value: Option<&str>) -> Result<Self> {
        let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
            return Ok(Self::default());
        };
        serde_json::from_str(value).context("failed to parse group runtime config")
    }

    pub fn to_json(&self) -> Result<String> {
        serde_json::to_string(self).context("failed to encode group runtime config")
    }

    pub fn backend_override(&self) -> Option<WorkerBackend> {
        self.provider
            .as_deref()
            .or(self.backend.as_deref())
            .map(WorkerBackend::parse)
    }

    pub fn assistant_name<'a>(&'a self, default_name: &'a str) -> &'a str {
        self.assistant_name
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or(default_name)
    }

    pub fn execution_env(&self) -> BTreeMap<String, String> {
        let mut env = BTreeMap::new();
        let backend = self.backend_override();

        if let Some(provider) = self
            .provider
            .as_deref()
            .filter(|value| !value.trim().is_empty())
        {
            env.insert(
                "NANOCLAW_GROUP_PROVIDER".to_string(),
                provider.trim().to_string(),
            );
        }
        if let Some(backend) = backend.as_ref() {
            env.insert(
                "NANOCLAW_WORKER_BACKEND".to_string(),
                backend.as_str().to_string(),
            );
        }
        if let Some(model) = self
            .model
            .as_deref()
            .filter(|value| !value.trim().is_empty())
        {
            apply_model_env(&mut env, backend.as_ref(), model.trim());
        }
        if let Some(effort) = self
            .effort
            .as_deref()
            .filter(|value| !value.trim().is_empty())
        {
            env.insert(
                "NANOCLAW_MODEL_EFFORT".to_string(),
                effort.trim().to_string(),
            );
            env.insert(
                "NANOCLAW_CODEX_REASONING_EFFORT".to_string(),
                effort.trim().to_string(),
            );
        }
        if let Some(max_messages) = self.max_messages_per_prompt.filter(|value| *value > 0) {
            env.insert(
                "NANOCLAW_MAX_MESSAGES_PER_PROMPT".to_string(),
                max_messages.to_string(),
            );
        }

        env
    }
}

fn apply_model_env(
    env: &mut BTreeMap<String, String>,
    backend: Option<&WorkerBackend>,
    model: &str,
) {
    env.insert("NANOCLAW_GROUP_MODEL".to_string(), model.to_string());
    match backend {
        Some(WorkerBackend::Codex) => {
            env.insert("NANOCLAW_CODEX_MODEL".to_string(), model.to_string());
        }
        Some(WorkerBackend::Claude) => {
            env.insert("NANOCLAW_CLAUDE_MODEL".to_string(), model.to_string());
        }
        Some(WorkerBackend::Zai) => {
            env.insert("NANOCLAW_ZAI_MODEL".to_string(), model.to_string());
        }
        Some(WorkerBackend::AzureOpenAI) => {
            env.insert("NANOCLAW_AZURE_OPENAI_MODEL".to_string(), model.to_string());
            env.insert(
                "NANOCLAW_AZURE_OPENAI_DEPLOYMENT".to_string(),
                model.to_string(),
            );
        }
        Some(WorkerBackend::WorkersAI) => {
            env.insert("NANOCLAW_WORKERS_AI_MODEL".to_string(), model.to_string());
        }
        Some(WorkerBackend::GithubCopilot | WorkerBackend::Summary | WorkerBackend::Custom(_))
        | None => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_empty_config_as_default() -> Result<()> {
        assert_eq!(
            GroupRuntimeConfig::from_json(None)?,
            GroupRuntimeConfig::default()
        );
        assert_eq!(
            GroupRuntimeConfig::from_json(Some("  "))?,
            GroupRuntimeConfig::default()
        );
        Ok(())
    }

    #[test]
    fn maps_azure_model_to_provider_env() -> Result<()> {
        let config = GroupRuntimeConfig::from_json(Some(
            r#"{"provider":"azure-openai","model":"gpt-4.1","effort":"medium","assistant_name":"CTO"}"#,
        ))?;
        assert_eq!(config.backend_override(), Some(WorkerBackend::AzureOpenAI));
        assert_eq!(config.assistant_name("Andy"), "CTO");
        let env = config.execution_env();
        assert_eq!(
            env.get("NANOCLAW_WORKER_BACKEND").map(String::as_str),
            Some("azure-openai")
        );
        assert_eq!(
            env.get("NANOCLAW_AZURE_OPENAI_DEPLOYMENT")
                .map(String::as_str),
            Some("gpt-4.1")
        );
        assert_eq!(
            env.get("NANOCLAW_MODEL_EFFORT").map(String::as_str),
            Some("medium")
        );
        Ok(())
    }
}
