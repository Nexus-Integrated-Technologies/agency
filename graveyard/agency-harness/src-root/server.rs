use axum::{
    extract::{Json, State, ws::{WebSocketUpgrade, Message as WsMessage}},
    response::{IntoResponse, Html, Response, sse::{Event, Sse}},
    routing::{get, post},
    Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::{Mutex, broadcast};
use anyhow::Result;
use std::convert::Infallible;
use futures_util::{StreamExt, SinkExt};
use axum::http::StatusCode;
use tower_http::trace::TraceLayer;

use crate::agent::{Speaker, LLMProvider};
use crate::memory::EpisodicMemory;
use crate::orchestrator::Supervisor;

// --- SOTA: Robust Error Handling ---
pub struct ServerError(anyhow::Error);

impl IntoResponse for ServerError {
    fn into_response(self) -> Response {
        let (status, message) = (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Nexus Server Error: {}", self.0),
        );
        (status, Json(serde_json::json!({ "error": message }))).into_response()
    }
}

impl<E> From<E> for ServerError where E: Into<anyhow::Error> {
    fn from(err: E) -> Self { Self(err.into()) }
}

#[derive(Clone)]
pub struct AppState {
    pub provider: Arc<dyn LLMProvider>,
    pub start_local: String,
    pub speaker: Arc<Mutex<Speaker>>,
    pub tx: broadcast::Sender<String>,
    pub episodic_memory: Arc<Mutex<EpisodicMemory>>,
    pub supervisor: Arc<Mutex<Supervisor>>,
    pub current_task: Arc<Mutex<Option<tokio::task::AbortHandle>>>
}

#[derive(Deserialize)]
struct ChatRequest {
    messages: Vec<Message>,
    #[serde(default)]
    stream: bool,
}

#[derive(Deserialize, Serialize)]
struct Message {
    role: String,
    content: String,
}

#[derive(Serialize)]
struct ChatResponse {
    choices: Vec<Choice>,
}

#[derive(Serialize)]
struct Choice {
    message: Message,
}

#[derive(Serialize)]
struct StreamResponse {
    choices: Vec<StreamChoice>,
}

#[derive(Serialize)]
struct StreamChoice {
    delta: StreamDelta,
}

#[derive(Serialize)]
struct StreamDelta {
    content: String,
}

// SOTA: Sentence Buffer for Streaming TTS
struct SentenceBuffer {
    buffer: String,
    speaker: Arc<Mutex<Speaker>>,
}

impl SentenceBuffer {
    fn new(speaker: Arc<Mutex<Speaker>>) -> Self {
        Self { buffer: String::new(), speaker }
    }

    async fn push(&mut self, text: &str) {
        self.buffer.push_str(text);
        let split_chars = ['.', '!', '?', '\n', ',', ';', ':'];
        let word_count = self.buffer.split_whitespace().count();
        
        if let Some(last_idx) = self.buffer.rfind(|c| split_chars.contains(&c)) {
            if last_idx > 1 {
                let to_speak = self.buffer[..=last_idx].to_string();
                self.buffer = self.buffer[last_idx+1..].to_string();
                let speaker = self.speaker.clone();
                tokio::spawn(async move {
                    let mut s = speaker.lock().await;
                    let _ = s.say(&to_speak).await;
                });
            }
        } else if word_count > 15 {
            if let Some(space_idx) = self.buffer.rfind(' ') {
                let to_speak = self.buffer[..space_idx].to_string();
                self.buffer = self.buffer[space_idx+1..].to_string();
                let speaker = self.speaker.clone();
                tokio::spawn(async move {
                    let mut s = speaker.lock().await;
                    let _ = s.say(&to_speak).await;
                });
            }
        }
    }

    async fn flush(&mut self) {
        if !self.buffer.trim().is_empty() {
            let to_speak = self.buffer.clone();
            self.buffer.clear();
            let speaker = self.speaker.clone();
            tokio::spawn(async move {
                let mut s = speaker.lock().await;
                let _ = s.say(&to_speak).await;
            });
        }
    }
}

pub async fn run_server(state: AppState) -> Result<()> {
    println!("🏛️  Initializing Nexus SOTA Server...\n");
    
    let tx_metrics = state.tx.clone();
    let mem_metrics = state.episodic_memory.clone();
    let start_local_metrics = state.start_local.clone();
    
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
        loop {
            interval.tick().await;
            let count = mem_metrics.lock().await.len();
            let msg = format!("METRICS:{{ \"since\": \"{}\", \"memory\": {}}}", start_local_metrics, count);
            let _ = tx_metrics.send(msg);
        }
    });

    let app = Router::new()
        .route("/", get(dashboard))
        .route("/ws", get(ws_handler))
        .route("/v1/chat/completions", post(chat_completions))
        .route("/v1/responses", post(crate::services::responses::responses_handler))
        .route("/v1/a2a/interact", post(a2a_interact_handler))
        .route("/v1/memory/clear", post(clear_memory))
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    let addr = "0.0.0.0:8002";
    println!("🚀 SOTA Backend Ready: http://{}", addr);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let _ = axum::serve(listener, app).await;

    Ok(())
}

async fn clear_memory(State(state): State<AppState>) -> impl IntoResponse {
    if let Ok(mut supervisor) = state.supervisor.try_lock() {
        let _ = supervisor.clear_history().await;
    }
    (StatusCode::OK, Json(serde_json::json!({ "status": "cleared" })))
}

async fn a2a_interact_handler(
    State(state): State<AppState>,
    Json(interaction): Json<crate::orchestrator::a2a::AgentInteraction>,
) -> Result<impl IntoResponse, ServerError> {
    let mut supervisor = state.supervisor.lock().await;
    
    // Process the peer request
    let response = supervisor.handle_peer_request(
        interaction.target_agent,
        &interaction.payload,
        None 
    ).await?;

    Ok(Json(response))
}

async fn dashboard(State(state): State<AppState>) -> impl IntoResponse {
    let start_local = state.start_local.clone();
    let initial_model = "-".to_string(); 
    let memory_count = { let memory = state.episodic_memory.lock().await; memory.len() };
    
    Html(format!(r####"<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>NEXUS | First Principles Interface</title>
    <script src="https://cdn.jsdelivr.net/npm/marked/marked.min.js"></script>
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/highlight.js/11.9.0/styles/github-dark.min.css">
    <script src="https://cdnjs.cloudflare.com/ajax/libs/highlight.js/11.9.0/highlight.min.js"></script>
    <style>
        :root {{ --bg-color: #050505; --panel-bg: #0f0f0f; --border-color: #222; --accent-tech: #00ff41; --accent-plain: #ffffff; --accent-warn: #ff9500; --accent-danger: #ff3b30; --accent-assurance: #00e5ff; --font-ui: -apple-system, BlinkMacSystemFont, \"SF Pro Display\", sans-serif; --font-mono: \"SF Mono\", monospace; }}

        body {{ background-color: var(--bg-color); color: #e0e0e0; font-family: var(--font-ui); margin: 0; height: 100vh; display: grid; grid-template-rows: 50px 1fr 60px; overflow: hidden; }}
        .message-nexus pre {{ background: #111; padding: 15px; border-radius: 6px; border: 1px solid #333; overflow-x: auto; }}
        .message-nexus code {{ font-family: var(--font-mono); font-size: 13px; color: var(--accent-assurance); }}
        .message-nexus p {{ margin-top: 0; }}
        .message-nexus table {{ border-collapse: collapse; width: 100%; margin-bottom: 15px; }}
        .message-nexus th, .message-nexus td {{ border: 1px solid #333; padding: 8px; text-align: left; }}
        .message-nexus th {{ background: #1a1a1a; font-size: 12px; text-transform: uppercase; color: #888; }}
        header {{ background: rgba(10, 10, 10, 0.95); border-bottom: 1px solid var(--border-color); display: flex; align-items: center; justify-content: space-between; padding: 0 20px; backdrop-filter: blur(10px); z-index: 100; }}
        .brand {{ font-weight: 700; font-size: 14px; letter-spacing: 1px; }}
        .stats-bar {{ display: flex; gap: 15px; font-size: 11px; font-family: var(--font-mono); color: #666; }}
        .stat-item {{ display: flex; align-items: center; gap: 6px; background: #111; padding: 4px 10px; border-radius: 4px; border: 1px solid #222; }}
        .stat-value {{ color: #ccc; }}
        .mvpk-grid {{ display: grid; grid-template-columns: 35% 45% 20%; gap: 1px; background: var(--border-color); height: 100%; overflow: hidden; }}
        .view-panel {{ background: var(--bg-color); display: flex; flex-direction: column; overflow: hidden; }}
        .panel-header {{ padding: 10px 15px; font-size: 10px; text-transform: uppercase; letter-spacing: 1.5px; color: #555; border-bottom: 1px solid var(--border-color); background: rgba(20, 20, 20, 0.5); display: flex; justify-content: space-between; }}
        .scroll-area {{ flex: 1; overflow-y: auto; padding: 20px; }}
        #tech-content {{ font-family: var(--font-mono); font-size: 12px; line-height: 1.5; color: var(--accent-tech); white-space: pre-wrap; opacity: 0.8; }}
        #plain-content {{ font-family: var(--font-ui); font-size: 15px; line-height: 1.6; color: #eee; white-space: pre-wrap; }}
        .message-nexus {{ color: #fff; margin-bottom: 20px; padding: 15px; background: rgba(255,255,255,0.03); border-radius: 6px; border-left: 2px solid var(--accent-plain); }}
        .message-user {{ color: #888; font-style: italic; border-left: 2px solid #444; padding-left: 10px; margin-bottom: 15px; }}
        .r-value-display {{ font-size: 42px; font-weight: 200; font-family: var(--font-mono); color: var(--accent-assurance); text-align: center; }}
        .assurance-log {{ flex: 1; font-family: var(--font-mono); font-size: 10px; color: #555; overflow-y: auto; padding: 10px; }}
        .input-area {{ background: #0a0a0a; border-top: 1px solid var(--border-color); display: flex; align-items: center; padding: 0 20px; gap: 15px; }}
        #chat-input {{ flex: 1; background: transparent; border: none; color: #fff; outline: none; font-size: 14px; font-family: var(--font-ui); }}
        .btn {{ background: #222; border: 1px solid #333; color: #ccc; padding: 6px 12px; font-size: 11px; border-radius: 4px; cursor: pointer; }}
    </style>
</head>
<body>
    <header>
        <div style="font-weight:700;">❖ NEXUS <span style="font-weight:200; opacity:0.5;">SoTA</span></div>
        <div class="stats-bar">
            <div class="stat-item"><span>MODEL</span><span class="stat-value" id="model-val">{}</span></div>
            <div class="stat-item"><span>SINCE</span><span class="stat-value" id="uptime-val">{}</span></div>
            <div class="stat-item"><span>MEMORY</span><span class="stat-value" id="memory-val">{} Turns</span></div>
        </div>
    </header>

    <div class="mvpk-grid">
        <div class="view-panel">
            <div class="panel-header"><span>TechView</span><span>Internal_Projection</span></div>
            <div class="scroll-area" id="tech-scroll"><div id="tech-content"></div></div>
        </div>
        <div class="view-panel">
            <div class="panel-header"><span>PlainView</span><span>Publication_Surface</span></div>
            <div class="scroll-area" id="plain-scroll"><div id="plain-content"></div></div>
        </div>
        <div class="view-panel">
            <div class="panel-header"><span>Assurance</span><span>Reliability_Metrics</span></div>
            <div style="background:#080808; flex:1; display:flex; flex-direction:column; padding:20px;">
                <div class="r-value-display" id="r-value">1.00</div>
                <div style="font-size:9px; color:#444; text-align:center; margin-bottom:20px;">CONFIDENCE SCORE</div>
                <div class="assurance-log" id="assurance-log"></div>
            </div>
        </div>
    </div>

    <div class="input-area">
        <input type="text" id="chat-input" placeholder="Type a message for Nexus..." autocomplete="off">
        <button class="btn" id="send-btn" onclick="sendQuery()">Send</button>
        <button class="btn" id="stop-btn" style="display:none; background:var(--accent-danger); border-color:#500;" onclick="stopInference()">Stop</button>
        <button class="btn" onclick="clearMemory()">Wipe</button>
    </div>

    <script>
        const ws = new WebSocket('ws://' + location.host + '/ws');
        const techContent = document.getElementById('tech-content');
        const plainContent = document.getElementById('plain-content');
        const assuranceLog = document.getElementById('assurance-log');
        const rValue = document.getElementById('r-value');
        const chatInput = document.getElementById('chat-input');
        const sendBtn = document.getElementById('send-btn');
        const stopBtn = document.getElementById('stop-btn');
        
        let currentTechBlock = null;
        let currentPlainBlock = null;
        let currentPlainRaw = '';
        let isAnswerMode = false;

        marked.setOptions({{
            highlight: function(code, lang) {{
                if (lang && hljs.getLanguage(lang)) {{ try {{ return hljs.highlight(code, {{ language: lang }}).value; }} catch (err) {{}} }} 
                try {{ return hljs.highlightAuto(code).value; }} catch (err) {{}} 
                return '';
            }},
            breaks: true,
            gfm: true
        }});

        ws.onmessage = (e) => {{ 
            const data = e.data;
            if (data.startsWith('METRICS:')) {{ try {{ const m = JSON.parse(data.substring(8)); document.getElementById('uptime-val').textContent = m.since; document.getElementById('memory-val').textContent = m.memory + ' Turns'; }} catch (err) {{}} }} 
            else if (data.startsWith('THOUGHT:') || (!isAnswerMode && data.startsWith('TOKEN:'))) {{
                const token = data.startsWith('TOKEN:') ? data.substring(6) : data.substring(8);
                if (!currentTechBlock) {{ currentTechBlock = document.createElement('span'); techContent.appendChild(currentTechBlock); }}
                currentTechBlock.textContent += token;
                document.getElementById('tech-scroll').scrollTop = techContent.scrollHeight;
                if (data.startsWith('THOUGHT:')) isAnswerMode = false;
            }} else if (data.startsWith('ANSWER:') || (isAnswerMode && data.startsWith('TOKEN:'))) {{
                isAnswerMode = true;
                const token = data.startsWith('TOKEN:') ? data.substring(6) : data.substring(7);
                if (!currentPlainBlock) {{ currentPlainBlock = document.createElement('div'); currentPlainBlock.className = 'message-nexus'; plainContent.appendChild(currentPlainBlock); currentPlainRaw = ''; }} 
                let clean = token.replace(/[[A-Z]ANSWER]|ANSWER:/gi, '');
                if (currentPlainRaw === '') clean = clean.replace(/^]\s*/, '');
                currentPlainRaw += clean;
                currentPlainBlock.innerHTML = marked.parse(currentPlainRaw);
                currentPlainBlock.querySelectorAll('pre code').forEach((block) => hljs.highlightElement(block));
                document.getElementById('plain-scroll').scrollTop = plainContent.scrollHeight;
            }} else if (data.startsWith('FINAL_ANSWER:')) {{
                const answer = data.substring(13);
                if (!currentPlainBlock || currentPlainRaw.trim() === '') {{
                    isAnswerMode = true;
                    if (!currentPlainBlock) {{ currentPlainBlock = document.createElement('div'); currentPlainBlock.className = 'message-nexus'; plainContent.appendChild(currentPlainBlock); }}
                    currentPlainRaw = answer;
                    currentPlainBlock.innerHTML = marked.parse(currentPlainRaw);
                    currentPlainBlock.querySelectorAll('pre code').forEach((block) => hljs.highlightElement(block));
                    document.getElementById('plain-scroll').scrollTop = plainContent.scrollHeight;
                }}
            }} else if (data.startsWith('RELIABILITY:')) {{
                const val = parseFloat(data.substring(12));
                rValue.textContent = val.toFixed(2);
                logAssurance('Audit', 'R-Score: ' + val.toFixed(2));
            }} else if (data.startsWith('PUBLICATION_UPDATE:')) {{ try {{ const pc = JSON.parse(data.substring(19)); logAssurance('PC-Update', `${{pc.pc_type}}: ${{JSON.stringify(pc.value)}} ${{pc.unit || ''}} (Ed: ${{pc.edition}})`); }} catch (err) {{}} }}
            else if (data.startsWith('BOUNDARY_CROSSING:')) {{ try {{ const claim = JSON.parse(data.substring(18)); logAssurance('Security', `🚨 [Quadrant ${{claim.quadrant}}] ${{claim.claim_id}}: ${{claim.content}}`, 'var(--accent-warn)'); }} catch (err) {{}} }}
            else if (data.startsWith('ASSURANCE:')) {{ try {{ const a = JSON.parse(data.substring(10)); logAssurance('Telemetry', `Latency: ${{a.latency}}ms`); logAssurance('Telemetry', `Tool Calls: ${{a.tools}}`); logAssurance('Telemetry', `Evidence Nodes: ${{a.evidence}}`); logAssurance('Telemetry', `Scale Class: ${{a.scale}}`); logAssurance('Telemetry', `Model: ${{a.model}}`); document.getElementById('model-val').textContent = a.model; }} catch (err) {{}} }}
            else if (data.startsWith('STATE:MODEL:')) {{ document.getElementById('model-val').textContent = data.substring(12); }}
            else if (data.startsWith('STATE:')) {{
                if (data.startsWith('STATE:ANSWER_START')) {{ isAnswerMode = true; currentPlainBlock = null; currentPlainRaw = ''; if (currentTechBlock) {{ const full = currentTechBlock.textContent; const match = full.match(/[[A-Z]ANSWER]*|ANSWER:?$/i); if (match) currentTechBlock.textContent = full.substring(0, match.index).trim(); }} }} 
                else if (data.startsWith('STATE:THOUGHT_START')) {{ isAnswerMode = false; currentTechBlock = null; }} 
                else if (data.startsWith('STATE:TURN_COMPLETE') || data.startsWith('STATE:STOPPED')) {{ isAnswerMode = false; currentTechBlock = null; currentPlainBlock = null; currentPlainRaw = ''; sendBtn.style.display = 'inline-block'; stopBtn.style.display = 'none'; }} 
                else if (data.startsWith('STATE:ABORTED')) {{ isAnswerMode = false; currentTechBlock = null; currentPlainBlock = null; currentPlainRaw = ''; }}
                logAssurance('System', data);
            }} else if (data.startsWith('🚀 Request')) {{
                isAnswerMode = false; currentTechBlock = null; currentPlainBlock = null; currentPlainRaw = ''; 
                techContent.innerHTML += '<div style="color:#444; margin:15px 0; border-top:1px solid #222; padding-top:10px;">--- NEW TURN ---</div>'; 
                assuranceLog.innerHTML = ''; rValue.textContent = '1.00'; sendBtn.style.display = 'none'; stopBtn.style.display = 'inline-block';
                logAssurance('System', data);
            }}
        }};

        function logAssurance(source, msg, color) {{ 
            const div = document.createElement('div');
            div.style.marginBottom = '5px';
            if (color) div.style.color = color;
            div.innerHTML = `<span style="color:#333;">[${{new Date().toLocaleTimeString()}}]</span> <b>${{source}}:</b> ${{msg}}`;
            assuranceLog.appendChild(div);
            assuranceLog.scrollTop = assuranceLog.scrollHeight;
        }}

        function sendQuery() {{ 
            const val = chatInput.value.trim();
            if (!val) return;
            const div = document.createElement('div');
            div.className = 'message-user';
            div.textContent = '> ' + val;
            plainContent.appendChild(div);
            ws.send(JSON.stringify({{ type: 'query', content: val }}));
            chatInput.value = '';
            document.getElementById('plain-scroll').scrollTop = plainContent.scrollHeight;
        }}

        function stopInference() {{ 
            ws.send(JSON.stringify({{ type: 'stop' }}));
        }}

        chatInput.addEventListener('keypress', (e) => {{ if (e.key === 'Enter') sendQuery(); }});

        async function clearMemory() {{ 
            if (!confirm('Wipe episodic memory?')) return;
            await fetch('/v1/memory/clear', {{ method: 'POST' }});
            location.reload();
        }}
    </script>
</body>
</html>"####, initial_model, start_local, memory_count))
}

async fn ws_handler(ws: WebSocketUpgrade, State(state): State<AppState>) -> impl IntoResponse {
    ws.on_upgrade(|socket| async move {
        let (mut sender, mut receiver) = socket.split();
        let mut rx = state.tx.subscribe();
        let mut global_rx = crate::orchestrator::event_bus::AGENCY_EVENT_BUS.subscribe();
        
        let state_c = state.clone();
        
        // Forward turn-specific messages
        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(msg) => { if sender.send(WsMessage::Text(msg.into())).await.is_err() { break; } },
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(_) => break,
                }
            }
        });

        // Forward global agency events
        let sender_c = state.tx.clone();
        tokio::spawn(async move {
            loop {
                match global_rx.recv().await {
                    Ok(event) => {
                        let msg = match event {
                            crate::orchestrator::event_bus::AgencyEvent::BoundaryCrossing(claim) => {
                                format!("BOUNDARY_CROSSING:{}", serde_json::to_string(&claim).unwrap_or_default())
                            },
                            crate::orchestrator::event_bus::AgencyEvent::PublicationUpdate { pc } => {
                                format!("PUBLICATION_UPDATE:{}", serde_json::to_string(&pc).unwrap_or_default())
                            },
                            _ => continue,
                        };
                        if sender_c.send(msg).is_err() { break; }
                    },
                    Err(_) => break,
                }
            }
        });

        while let Some(Ok(msg)) = receiver.next().await {
            if let WsMessage::Text(text) = msg {
                if let Ok(json) = serde_json::from_str::<serde_json::Value>(&text) {
                    if json["type"] == "query" {
                        let query = json["content"].as_str().unwrap_or_default().to_string();
                        let supervisor = state_c.supervisor.clone();
                        let tx = state_c.tx.clone();
                        let current_task = state_c.current_task.clone();
                        
                        // Abort existing task
                        { let mut task_guard = current_task.lock().await; if let Some(handle) = task_guard.take() { handle.abort(); let _ = tx.send("STATE:ABORTED".to_string()); } } 

                        let handle = tokio::spawn(async move { 
                            let mut supervisor = supervisor.lock().await;
                            let _ = tx.send(format!("🚀 Request: Orchestrating Agency..."));
                            let result = supervisor.handle(&query).await;
                            
                            match result {
                                Ok(res) => {
                                    // SOTA: Final Answer Fallback
                                    // If the model was tagless, the tokens went to TechView. 
                                    // We send the final projected answer to ensure it appears in PlainView.
                                    let _ = tx.send(format!("FINAL_ANSWER:{}", res.answer));

                                    if let Some(pub_obj) = res.publication {
                                        let _ = tx.send(format!("RELIABILITY:{}", pub_obj.reliability));
                                        let assurance_json = serde_json::json!({
                                            "latency": pub_obj.telemetry.latency_ms,
                                            "tools": pub_obj.telemetry.tool_calls,
                                            "evidence": pub_obj.telemetry.evidence_count,
                                            "scale": format!("{:?}", pub_obj.telemetry.scale),
                                            "model": pub_obj.telemetry.model
                                        });
                                        let _ = tx.send(format!("ASSURANCE:{}", assurance_json));
                                    }
                                },
                                Err(e) => {
                                    let _ = tx.send(format!("THOUGHT:\n🛑 **Error during execution:**\n{}\n", e));
                                    let _ = tx.send(format!("STATE:ABORTED"));
                                }
                            }
                            
                            let _ = tx.send(format!("STATE:TURN_COMPLETE"));
                        });
                        
                        *current_task.lock().await = Some(handle.abort_handle());
                    } else if json["type"] == "stop" {
                        let mut task_guard = state_c.current_task.lock().await;
                        if let Some(handle) = task_guard.take() {
                            handle.abort();
                            let _ = state_c.tx.send("STATE:STOPPED".to_string());
                            let _ = state_c.tx.send("THOUGHT:\n🛑 Inference manually stopped by user.\n".to_string());
                        }
                    }
                }
            }
        }
    })
}

async fn chat_completions(
    State(state): State<AppState>,
    Json(req): Json<ChatRequest>,
) -> Result<impl IntoResponse, ServerError> {
    let last_msg = req.messages.last().map(|m| m.content.clone()).unwrap_or_default();
    let history = { let mut memory = state.episodic_memory.lock().await; if !last_msg.is_empty() { memory.add_user(&last_msg); } memory.format_as_chatml() };

    let prompt = format!(
        "<|im_start|>system\nYou are a high-fidelity intelligence layer. 
Follow the First Principles Framework (FPF): ALWAYS separate internal thought from external communication. 
Use [THOUGHT] for your internal reasoning and [ANSWER] for the final user surface.<|im_end|>\n{}\n<|im_start|>assistant\n[THOUGHT]\n",
        history
    );

    let tx = state.tx.clone();
    let _ = tx.send(format!("🚀 Request (Streaming Inference)"));

    let mut stream = state.provider.generate_stream("standard", prompt, None).await
        .map_err(|e| ServerError(e))?;

    if req.stream {
        let (sse_tx, sse_rx) = tokio::sync::mpsc::unbounded_channel::<Result<Event, Infallible>>();
        let state_c = state.clone();
        
        tokio::task::spawn(async move {
            let mut tts = SentenceBuffer::new(state_c.speaker.clone());
            let mut full_response = String::new();
            let mut answer_started = false;

            while let Some(chunk_res) = stream.next().await {
                if let Ok(text) = chunk_res {
                    full_response.push_str(&text);

                    // SOTA: Proactive Stop Detection (Direct Endpoint)
                    if full_response.ends_with("<|im_end|>") || full_response.ends_with("<|eot_id|>") { break; }

                    if !answer_started && (full_response.contains("[ANSWER]") || full_response.to_uppercase().contains("ANSWER:")) {
                        answer_started = true;
                        let _ = state_c.tx.send("STATE:ANSWER_START".to_string());
                    }
                    if answer_started {
                        let clean = text.replace("[ANSWER]", "").replace("ANSWER:", "");
                        let _ = state_c.tx.send(format!("ANSWER:{}", clean));
                        let _ = tts.push(&clean).await;
                    } else {
                        let _ = state_c.tx.send(format!("THOUGHT:{}", text));
                    }
                    
                    let resp = StreamResponse { choices: vec![StreamChoice { delta: StreamDelta { content: text } } ] };
                    let _ = sse_tx.send(Ok(Event::default().data(serde_json::to_string(&resp).unwrap())));
                }
            }
            tts.flush().await;
            let mut memory = state_c.episodic_memory.lock().await;
            memory.add_assistant(full_response, Some("Nexus".to_string()));
            let _ = sse_tx.send(Ok(Event::default().data("[DONE]")));
        });
        Ok(Sse::new(tokio_stream::wrappers::UnboundedReceiverStream::new(sse_rx)).into_response())
    } else {
        // Non-streaming fallback
        let mut full_response = String::new();
        while let Some(chunk_res) = stream.next().await {
            if let Ok(chunk) = chunk_res {
                full_response.push_str(&chunk);
            }
        }
        let mut memory = state.episodic_memory.lock().await;
        memory.add_assistant(full_response.clone(), Some("Nexus".to_string()));
        Ok(Json(ChatResponse { choices: vec![Choice { message: Message { role: "assistant".to_string(), content: full_response } } ] }).into_response())
    }
}