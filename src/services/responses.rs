//! Open Responses API Implementation
//! 
//! Implements the standard "Responses" API endpoint (/v1/responses).
//! This endpoint delegates to the Supervisor (Agency), enabling full
//! autonomous capabilities (Tools, Memory, Planning) behind a standard interface.

use axum::{
    extract::{Json, State},
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};
use crate::server::{AppState, ServerError};

const DEFAULT_RESPONSE_MODEL: &str = "rust_agency_sovereign";

#[derive(Deserialize)]
pub struct CreateResponseRequest {
    pub model: Option<String>,
    pub messages: Vec<Message>,
    pub temperature: Option<f32>,
    pub tools: Option<Vec<serde_json::Value>>, // Pass-through for now, or ignored if Agency handles tools
    pub stream: Option<bool>,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct Message {
    pub role: String,
    pub content: String,
}

#[derive(Serialize)]
pub struct ResponseObject {
    pub id: String,
    pub object: String,
    pub created: u64,
    pub model: String,
    pub choices: Vec<ResponseChoice>,
    pub usage: Usage,
}

#[derive(Serialize)]
pub struct ResponseChoice {
    pub index: usize,
    pub message: Message,
    pub finish_reason: String,
}

#[derive(Serialize)]
pub struct Usage {
    pub prompt_tokens: u32,
    pub completion_tokens: u32,
    pub total_tokens: u32,
}

fn latest_message(messages: &[Message]) -> Result<String, ServerError> {
    messages
        .last()
        .map(|m| m.content.clone())
        .ok_or_else(|| anyhow::anyhow!("No messages provided").into())
}

fn unix_timestamp_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

fn estimate_tokens(text: &str) -> u32 {
    let char_count = text.chars().count() as u32;
    if char_count == 0 {
        return 0;
    }
    // Coarse fallback approximation for OpenAI-like token counts.
    (char_count + 3) / 4
}

fn estimate_prompt_tokens(messages: &[Message]) -> u32 {
    messages
        .iter()
        .map(|message| estimate_tokens(&message.content))
        .sum()
}

pub async fn responses_handler(
    State(state): State<AppState>,
    Json(req): Json<CreateResponseRequest>,
) -> Result<impl IntoResponse, ServerError> {
    // 1. Extract the latest user query from messages
    // The Agency Supervisor manages its own history, so we treat this as a "turn".
    // If the client sends a full history, we only really act on the last message.
    // (Future improvement: Sync client history with Agency memory if needed)
    let query = latest_message(&req.messages)?;
    let prompt_tokens = estimate_prompt_tokens(&req.messages);
    let model = req
        .model
        .clone()
        .unwrap_or_else(|| DEFAULT_RESPONSE_MODEL.to_string());

    // 2. Lock Supervisor and Execute
    let mut supervisor = state.supervisor.lock().await;
    
    // Notify dashboard via WebSocket
    let _ = state.tx.send(format!("🚀 Request (API): {}", query));

    // Execute Agentic Loop
    let result = supervisor.handle(&query).await
        .map_err(|e| anyhow::anyhow!("Agency Execution Failed: {}", e))?;

    // 3. Map Result to Response Object
    let response = ResponseObject {
        id: format!("resp_{}", uuid::Uuid::new_v4()),
        object: "response".to_string(),
        created: unix_timestamp_secs(),
        model,
        choices: vec![
            ResponseChoice {
                index: 0,
                message: Message {
                    role: "assistant".to_string(),
                    content: result.answer,
                },
                finish_reason: "stop".to_string(),
            }
        ],
        usage: {
            let completion_tokens = estimate_tokens(&result.answer);
            Usage {
                prompt_tokens,
                completion_tokens,
                total_tokens: prompt_tokens.saturating_add(completion_tokens),
            }
        },
    };

    Ok(Json(response))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn estimate_tokens_handles_empty_content() {
        assert_eq!(estimate_tokens(""), 0);
    }

    #[test]
    fn estimate_tokens_uses_char_based_ceiling() {
        assert_eq!(estimate_tokens("abcd"), 1);
        assert_eq!(estimate_tokens("abcde"), 2);
    }

    #[test]
    fn estimate_prompt_tokens_sums_message_contents() {
        let messages = vec![
            Message { role: "user".into(), content: "abcd".into() },
            Message { role: "assistant".into(), content: "abcdefgh".into() },
        ];
        assert_eq!(estimate_prompt_tokens(&messages), 3);
    }
}
