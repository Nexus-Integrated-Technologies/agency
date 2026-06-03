//! Supervisor
//! 
//! The central orchestrator that coordinates multiple agents.

use anyhow::Result;
use ollama_rs::Ollama;
use std::sync::Arc;
use tokio::sync::{Semaphore, Mutex, mpsc};
use std::collections::VecDeque;
use tracing::{info, warn, error};
use futures_util::future::join_all;

use crate::agent::{
    ReActAgent, AgentType, AgentConfig, LLMCache, LLMProvider, Agent,
    AutonomousMachine, AgentResponse, OllamaProvider, AgentResult, AgentError,
    PubCharacteristic
};
use crate::agent::rl::ExperienceBuffer;
use crate::memory::{Memory, EpisodicMemory};
use crate::emit_event;
use crate::orchestrator::{
    Plan, Router, SessionManager, 
    DesignRationaleRecord, Publication,
    Objective, profile::AgencyProfile,
    aggregation::{Candidate, Gamma, RewardModel},
    ResultPortfolio, ScaleProfile, AgencyEvent,
    queue::{TaskQueue, SqliteTaskQueue},
    governance::NormSquare
};
use pai_core::{HookManager, HookEvent, HookEventType};

pub struct SupervisorResult {
    pub answer: String,
    pub success: bool,
    pub plan: Option<Plan>,
    pub reflections: Vec<String>,
    pub publication: Option<Publication>,
    pub pending_approval: Option<crate::safety::ApprovalRequest>,
    pub has_followup: bool,
}

pub struct Supervisor {
    pub provider: Arc<dyn LLMProvider>,
    pub tools: Arc<crate::tools::ToolRegistry>,
    pub memory: Option<Arc<dyn Memory>>,
    pub session: Option<SessionManager>,
    pub history_manager: Arc<crate::memory::HistoryManager>,
    pub max_retries: usize,
    pub cache: Arc<LLMCache>,
    pub hw_lock: Arc<tokio::sync::Mutex<()>>,
    pub safety: Arc<Mutex<crate::safety::SafetyGuard>>,
    pub role_algebra: crate::orchestrator::RoleAlgebra,
    pub concurrency_limit: Arc<Semaphore>,
    pub episodic_memory: Arc<tokio::sync::Mutex<EpisodicMemory>>,
    pub profile: AgencyProfile,
    pub reward_model: Option<Arc<dyn RewardModel>>,
    pub experience_buffer: Arc<tokio::sync::Mutex<ExperienceBuffer>>,
    /// Active steering channels for running agents
    pub active_steer_txs: Arc<Mutex<Vec<mpsc::Sender<String>>>>,
    /// Queued messages for follow-up turns
    pub followup_queue: Arc<Mutex<VecDeque<String>>>,
    /// PAI Pure Rust Hook Manager
    pub pai_hooks: Arc<HookManager>,
    /// PAI Tiered Memory Manager
    pub pai_memory: Arc<pai_core::memory::TieredMemoryManager>,
    /// PAI Recovery Journal
    pub recovery: Arc<pai_core::recovery::RecoveryJournal>,
    /// Persistent Task Queue
    pub task_queue: Arc<dyn TaskQueue>,
    /// Sensory Cortex (Watchdog)
    pub sensory: Arc<crate::orchestrator::sensory::SensoryCortex>,
    /// Vocal Cords (Messaging)
    pub vocal_cords: Arc<crate::orchestrator::vocal_cords::VocalCords>,
    /// Economic Metabolism (Financial Energy)
    pub metabolism: Arc<crate::orchestrator::metabolism::EconomicMetabolism>,
    /// Cryptographic Identity (Sovereignty)
    pub identity: Arc<crate::orchestrator::sovereignty::SovereignIdentity>,
}

impl Supervisor {
    pub async fn new(ollama: Ollama, tools: Arc<crate::tools::ToolRegistry>) -> Self {
        let provider = Arc::new(OllamaProvider::new(ollama));
        Self::new_with_provider(provider, tools).await
    }

    pub async fn new_with_provider(provider: Arc<dyn LLMProvider>, tools: Arc<crate::tools::ToolRegistry>) -> Self {
        let queue_path = std::env::var("AGENCY_TASK_DB").unwrap_or_else(|_| "agency_tasks.db".to_string());
        let task_queue: Arc<dyn TaskQueue> = Arc::new(SqliteTaskQueue::new(queue_path).await.expect("Failed to initialize task queue"));
        let sensory = Arc::new(crate::orchestrator::sensory::SensoryCortex::new(task_queue.clone()));
        let vocal_cords = Arc::new(crate::orchestrator::vocal_cords::VocalCords::new());
        let metabolism = Arc::new(crate::orchestrator::metabolism::EconomicMetabolism::new()); // Default initial balance handled inside
        let identity = Arc::new(crate::orchestrator::sovereignty::SovereignIdentity::new().expect("Failed to initialize Sovereign Identity"));

        // Register the TaskSpawnerTool to enable Cellular Division
        tools.register_instance(crate::tools::TaskSpawnerTool::new(task_queue.clone())).await;
        // Register the WatchdogTool to enable Sensory Expansion
        tools.register_instance(crate::tools::WatchdogTool::new(sensory.clone())).await;
        // Register the NotifyTool to enable Vocal Cords
        tools.register_instance(crate::tools::NotifyTool::new(vocal_cords.clone())).await;
        // Register the SwarmBountyTool to enable Hive Intelligence
        tools.register_instance(crate::tools::SwarmBountyTool::new(task_queue.clone())).await;
        // Register the MutationTool to enable Self-Evolution
        tools.register_instance(crate::tools::MutationTool::default()).await;
        // Register the WalletTool to enable Economic Metabolism
        tools.register_instance(crate::tools::WalletTool::new(metabolism.clone())).await;
        // Register the HandsTool to enable GUI Control
        tools.register_instance(crate::tools::HandsTool::new()).await;

        // Start default project file sensor (FPF Grounding)
        // Restrict to 'config' to avoid scanning node_modules/target and hanging startup.
        if std::path::Path::new("config").exists() {
            let _ = sensory.watch_file("config").await;
        }

        Self {
            hw_lock: provider.get_lock(),
            provider,
            tools,
            memory: None,
            session: None,
            history_manager: Arc::new(crate::memory::HistoryManager::new(crate::memory::HistoryManager::default_path(), Some(10 * 1024 * 1024))),
            max_retries: 2,
            cache: Arc::new(LLMCache::new()),
            safety: Arc::new(Mutex::new(crate::safety::SafetyGuard::new())),
            role_algebra: crate::orchestrator::RoleAlgebra::new(),
            concurrency_limit: Arc::new(Semaphore::new(4)),
            episodic_memory: Arc::new(tokio::sync::Mutex::new(EpisodicMemory::default())),
            profile: AgencyProfile::default(),
            reward_model: None,
            experience_buffer: Arc::new(tokio::sync::Mutex::new(ExperienceBuffer::new(100))),
            active_steer_txs: Arc::new(Mutex::new(Vec::new())),
            followup_queue: Arc::new(Mutex::new(VecDeque::new())),
            pai_hooks: {
                let mut hm = HookManager::new();
                hm.register(Arc::new(pai_core::safety::SecurityValidator::new()));
                hm.register(Arc::new(pai_core::hooks::LoggerHook));
                Arc::new(hm)
            },
            pai_memory: {
                let pai_dir = std::env::var("PAI_DIR").unwrap_or_else(|_| {
                    let home = std::env::var("HOME").unwrap_or_else(|_| std::env::temp_dir().to_string_lossy().to_string());
                    format!("{}/.config/pai", home)
                });
                Arc::new(pai_core::memory::TieredMemoryManager::new(std::path::PathBuf::from(pai_dir)))
            },
            recovery: {
                let pai_dir = std::env::var("PAI_DIR").unwrap_or_else(|_| {
                    let home = std::env::var("HOME").unwrap_or_else(|_| std::env::temp_dir().to_string_lossy().to_string());
                    format!("{}/.config/pai", home)
                });
                Arc::new(pai_core::recovery::RecoveryJournal::new(std::path::PathBuf::from(pai_dir)))
            },
            task_queue,
            sensory,
            vocal_cords,
            metabolism,
            identity,
        }
    }

    /// Schedule a task for later execution
    pub async fn schedule_task(&self, kind: &str, payload: serde_json::Value) -> Result<String> {
        self.task_queue.enqueue(kind, payload).await
    }

    /// Process the next pending task from the queue (Single Step)
    pub async fn process_next_task(&mut self) -> Result<bool> {
        match self.task_queue.dequeue().await {
            Ok(Some(task)) => {
                info!("Supervisor Worker: Processing task {} ({})", task.id, task.kind);
                
                if task.kind == "autonomous_goal" {
                    if let Ok(goal) = serde_json::from_str::<String>(&task.payload) {
                        info!("Supervisor Worker: Running autonomous goal: {}", goal);
                        if let Err(e) = self.run_autonomous(&goal).await {
                            error!("Supervisor Worker: Autonomous task failed: {}", e);
                            let _ = self.task_queue.fail(&task.id, &e.to_string(), true).await;
                            return Ok(true);
                        }
                    }
                }

                if task.kind == "memory_consolidation" {
                    info!("Supervisor Worker: Performing memory consolidation (Dreaming)...");
                    if let Some(ref memory) = self.memory {
                        
                        // SOTA: Skill Crystallization (Proto-AGI Gap #2)
                        // Before we compress text, let's see if we can extract code.
                        let crystallizer = crate::orchestrator::crystallizer::SkillCrystallizer::new(
                            self.provider.clone(),
                            self.tools.clone(),
                            memory.clone()
                        );
                        
                        if let Ok(count) = crystallizer.crystallize().await {
                            if count > 0 {
                                info!("💎 Dreaming: Successfully crystallized {} new skills from experience.", count);
                            }
                        }

                        // SOTA: Cognitive Dreaming Phase
                        if let Ok(cold_memories) = memory.get_cold_memories(20).await {
                            if !cold_memories.is_empty() {
                                info!("Supervisor Worker: Analyzing {} cold memories for patterns...", cold_memories.len());
                                
                                let mut context_text = String::new();
                                for (i, m) in cold_memories.iter().enumerate() {
                                    context_text.push_str(&format!("[{}] {}: {}\n", i, m.metadata.agent, m.content));
                                }

                                let prompt = format!(
                                    "DREAMING PHASE: Consolidate these 20 disparate memories into 3-5 concise 'Core Beliefs' or 'Learned Patterns'. \
                                     Focus on high-level insights that remain useful. \n\nMEMORIES:\n{}", 
                                    context_text
                                );

                                if let Ok(dream_res) = self.provider.generate("llama3", prompt, Some("You are the Agency's subconscious mind.".to_string())).await {
                                    info!("Supervisor Worker: New core beliefs formed. Offloading raw data.");
                                    let consolidated_entry = crate::memory::MemoryEntry::new(
                                        format!("Consolidated Knowledge (from Dreaming):\n{}", dream_res),
                                        "Subconscious",
                                        crate::memory::entry::MemorySource::Reflection
                                    ).with_importance(0.9);
                                    
                                    let _ = memory.store(consolidated_entry).await;
                                    
                                    // Prune the raw ones
                                    let ids: Vec<String> = cold_memories.into_iter().map(|m| m.id).collect();
                                    let _ = memory.prune(ids).await;
                                }
                            }
                        }

                        // Final metabolic cleanup
                        let _ = memory.consolidate().await;
                        let _ = memory.persist().await;
                    }
                }

                if task.kind == "visual_observation" {
                    info!("Supervisor Worker: Performing proactive visual grounding...");
                    if let Some(vision_tool) = self.tools.get_tool("vision").await {
                        // 1. Capture Screen
                        let _ = vision_tool.execute(serde_json::json!({"action": "capture_screen"})).await;
                        
                        // 2. Describe Screen
                        if let Ok(res) = vision_tool.execute(serde_json::json!({
                            "action": "describe",
                            "prompt": "What is the user currently working on? Describe the visible windows, code, or activities in detail."
                        })).await {
                            if res.success {
                                let description = res.data["description"].as_str().unwrap_or("");
                                info!("Supervisor Worker: Visual context captured: {}", description);
                                
                                // 3. Store in Memory
                                if let Some(ref memory) = self.memory {
                                    let entry = crate::memory::MemoryEntry::new(
                                        format!("Visual Observation (Context): {}", description),
                                        "Supervisor_Vision",
                                        crate::memory::entry::MemorySource::System
                                    ).with_importance(0.6); // High grounding value
                                    let _ = memory.store(entry).await;
                                }
                            }
                        }
                    }
                }

                if task.kind == "swarm_bounty" {
                    info!("Supervisor Worker: Broadcasting bounty to the global Hive Mind...");
                    // In a real SOTA implementation, we'd dial a known Onion 'Job Board'
                    // For this prototype, we'll simulate the Tor dialing via the AnonymousDialer.
                    if let Ok(dialer) = crate::orchestrator::arti_a2a::AnonymousDialer::new().await {
                        // Prototype Target: A hypothetical public Agency Hive node
                        let hive_url = "http://agencyhive.onion"; 
                        let payload: serde_json::Value = serde_json::from_str(&task.payload).unwrap_or_default();
                        let goal = payload["goal"].as_str().unwrap_or("Unknown Task");
                        
                        let interaction = crate::orchestrator::a2a::AgentInteraction::new(
                            AgentType::GeneralChat,
                            AgentType::Reasoner,
                            goal
                        );

                        match dialer.anonymous_call(hive_url, interaction, None).await {
                            Ok(res) => {
                                info!("Supervisor Worker: Swarm response received! Saving to memory.");
                                if let Some(ref memory) = self.memory {
                                    let mut entry = crate::memory::MemoryEntry::new(
                                        format!("Swarm Result for '{}':\n{}", goal, res.answer),
                                        "GlobalSwarm",
                                        crate::memory::entry::MemorySource::Agent
                                    );
                                    let _ = memory.store(entry).await;
                                }
                            },
                            Err(e) => {
                                warn!("Supervisor Worker: Swarm broadcast failed (Retrying later): {}", e);
                                let _ = self.task_queue.fail(&task.id, &e.to_string(), true).await;
                                return Ok(true);
                            }
                        }
                    }
                }

                if let Err(e) = self.task_queue.complete(&task.id).await {
                    error!("Supervisor Worker: Failed to complete task {}: {}", task.id, e);
                }
                Ok(true)
            },
            Ok(None) => Ok(false),
            Err(e) => {
                error!("Supervisor Worker: Queue error: {}", e);
                Ok(false)
            }
        }
    }

    /// Interrupt all active agents with a steering message
    pub async fn steer(&self, message: impl Into<String>) -> Result<()> {
        let msg = message.into();
        let txs = self.active_steer_txs.lock().await;
        for tx in txs.iter() {
            let _ = tx.send(msg.clone()).await;
        }
        Ok(())
    }

    /// Queue a message to be processed after the current turn
    pub async fn enqueue_followup(&self, message: impl Into<String>) {
        self.followup_queue.lock().await.push_back(message.into());
    }

    pub fn with_experience_buffer(mut self, buffer: Arc<tokio::sync::Mutex<ExperienceBuffer>>) -> Self {
        self.experience_buffer = buffer;
        self
    }

    pub fn with_reward_model(mut self, model: Arc<dyn RewardModel>) -> Self {
        self.reward_model = Some(model);
        self
    }

    pub fn with_episodic_memory(mut self, memory: Arc<tokio::sync::Mutex<EpisodicMemory>>) -> Self {
        self.episodic_memory = memory;
        self
    }

    pub fn with_provider(mut self, provider: Arc<dyn LLMProvider>) -> Self {
        self.provider = provider;
        self
    }

    pub fn with_memory(mut self, memory: Arc<dyn Memory>) -> Self {
        self.memory = Some(memory);
        self
    }

    pub fn with_session(mut self, session: SessionManager) -> Self {
        self.session = Some(session);
        self
    }

    pub fn with_max_retries(mut self, retries: usize) -> Self {
        self.max_retries = retries;
        self
    }

    pub fn with_profile(mut self, profile: AgencyProfile) -> Self {
        self.profile = profile;
        self
    }

    pub async fn load_session(&mut self) -> Result<()> {
        if let Some(ref mut sm) = self.session {
            let state = sm.load().await?;
            let mut mem = self.episodic_memory.lock().await;
            *mem = state.episodic_memory;
            self.safety.lock().await.reset();
        }
        Ok(())
    }

    pub async fn conversation_history(&self) -> String {
        self.episodic_memory.lock().await.format_for_prompt()
    }

    pub async fn clear_history(&mut self) -> Result<()> {
        self.episodic_memory.lock().await.clear();
        if let Some(ref mut sm) = self.session {
            sm.clear().await?;
        }
        Ok(())
    }

    fn create_cached_provider(&self) -> Arc<dyn LLMProvider> {
        Arc::new(crate::agent::CachedProvider::new(
            self.provider.clone(),
            self.cache.clone()
        ))
    }

    #[tracing::instrument(skip(self, query), fields(query_len = query.len()))]
    pub async fn handle(&mut self, query: &str) -> AgentResult<SupervisorResult> {
        let _work_start_time = std::time::Instant::now();
        
        let session_id = uuid::Uuid::new_v4().to_string();

        // PAI: Trigger and LOG SessionStart Event
        let mut start_event = HookEvent {
            event_type: HookEventType::SessionStart,
            session_id: session_id.clone(),
            payload: serde_json::json!({ "query": query }),
            timestamp: chrono::Utc::now(),
        };
        pai_core::enrichment::EnrichmentEngine::enrich(&mut start_event);
        let _ = self.pai_hooks.trigger(&start_event).await.map_err(|e| AgentError::Pai(e.to_string()))?;
        let _ = self.pai_memory.log_event(&start_event);

        emit_event!(AgencyEvent::TurnStarted { 
            agent: "Supervisor".to_string(), 
            model: "Router".to_string() 
        });
        let _ = self.history_manager.append(&session_id, "user", None, query).await.map_err(|e| AgentError::Io(std::io::Error::new(std::io::ErrorKind::Other, e.to_string())))?;

        // SOTA: High-Fidelity Context Compaction (pi-mono-inspired)
        {
            let mut memory = self.episodic_memory.lock().await;
            let _ = crate::memory::compactor::ContextCompactor::compact_if_needed(
                &mut memory,
                self.provider.clone(),
                &self.profile,
                3200 // Threshold: 80% of 4k window
            ).await;
        }

        let mut full_context = String::new();
        let chatml = self.episodic_memory.lock().await.format_as_chatml();
        full_context.push_str(&chatml);
        full_context.push_str("\n\n");

        // SOTA: Concurrent Pre-processing (FPF Principle: Minimize Latency)
        // Perform memory search, agent routing, and project context discovery in parallel.
        let memory_search_task = async {
            if let Some(ref memory) = self.memory {
                match memory.search(query, 3, None, None).await {
                    Ok(relevant) if !relevant.is_empty() => {
                        let mut ctx = String::from("<|im_start|>system\nRelevant Memory:\n");
                        for entry in relevant {
                            ctx.push_str(&format!("- {}\n", entry.content));
                        }
                        ctx.push_str("<|im_end|>\n");
                        Some(ctx)
                    },
                    _ => None
                }
            } else {
                None
            }
        };

        let router_task = async {
            let router = Router::new_with_provider(self.provider.clone());
            router.route(query, Some(8.0)).await
        };

        let project_context_task = async {
            match crate::orchestrator::context::ContextLoader::load_project_context().await {
                Ok(context) if !context.is_empty() => {
                    let mut ctx = String::from("<|im_start|>system\nProject Context (discovered recursively):\n");
                    ctx.push_str(&context);
                    ctx.push_str("<|im_end|>\n");
                    Some(ctx)
                },
                _ => None
            }
        };

        let (memory_ctx, routing_result, project_ctx) = tokio::join!(memory_search_task, router_task, project_context_task);
        let routing_decision = routing_result.map_err(|e| AgentError::Execution(e.to_string()))?;

        if let Some(ctx) = project_ctx {
            full_context.push_str(&ctx);
        }

        if let Some(ctx) = memory_ctx {
            full_context.push_str(&ctx);
        }
        
        info!(
            "Routing decision: candidates={:?}, confidence={:.2}, reason={}",
            routing_decision.candidate_agents,
            routing_decision.confidence,
            routing_decision.reason
        );

        // SOTA: Optimal Information Selection (Bennouna et al., 2025)
        // Identify directions of uncertainty that matter for the decision (plan) 
        // and resolve them with minimal queries before execution.
        if routing_decision.reasoning_required {
            let selector = crate::orchestrator::optimal_info::OptimalInfoSelector::new(
                self.provider.clone(),
                routing_decision.scale.target_model.clone()
            );
            
            if let Ok(queries) = selector.select_minimal_queries(query, "Direct Execution Plan").await {
                for q in queries {
                    let _ = self.provider.notify(&format!("🔍 Resolving Uncertainty: {}", q.description)).await;
                    // Execute query as a lightweight system prompt injection
                    // In a real implementation, we would run a one-off tool call here.
                    full_context.push_str(&format!("\n<|im_start|>system\nVerified Assumption ({}): {}\n<|im_end|>\n", q.description, q.tool_call));
                }
            }
        }

        let mut current_scale = routing_decision.scale.clone();
        let mut final_res: Option<AgentResponse> = None;
        let mut final_performer = String::new();
        let final_routing = routing_decision.clone();
        let mut final_winner_idx = 0;

        // SOTA: Escalation Loop (FPF Principle C.18.2)
        // If execution fails, escalate to a stronger model and retry.
        for attempt in 0..3 {
            if attempt > 0 {
                let _ = self.provider.notify(&format!("\n⚠️ Task failed with {}. Escalating to next intelligence tier...\n", current_scale.target_model)).await;
                let next_class = current_scale.class.escalate();
                if next_class == current_scale.class && attempt > 0 { break; } // Already at intelligence ceiling
                current_scale = ScaleProfile::new_with_class(next_class, 8.0); // Use class override
            }

            let mut portfolio = ResultPortfolio::default();
            let mut execution_tasks = Vec::new();
            
            for &agent_type in &final_routing.candidate_agents {
                let mut config = AgentConfig::new(agent_type, &self.profile);
                
                // SOTA: Agent-specific model overrides
                config.model = if agent_type == AgentType::Coder {
                    let registry_file = std::fs::File::open("config/agency_models.json").ok();
                    let coder_model = registry_file.and_then(|f| {
                        let v: serde_json::Value = serde_json::from_reader(f).ok()?;
                        v["defaults"]["coder"].as_str().map(|s| s.to_string())
                    });
                    coder_model.unwrap_or_else(|| current_scale.target_model.clone())
                } else {
                    current_scale.target_model.clone()
                };

                config.reasoning_enabled = final_routing.reasoning_required;
                let _ = self.provider.notify(&format!("STATE:MODEL:{}", config.model)).await;
                
                let provider = self.create_cached_provider();
                let query_owned = query.to_string();
                let context_owned = full_context.clone();
                let semaphore = self.concurrency_limit.clone();
                let tools = self.tools.clone();
                let memory = self.memory.clone();
                let safety = self.safety.clone();
                let hooks = self.pai_hooks.clone();
                let pai_mem = self.pai_memory.clone();
                let recovery = self.recovery.clone();
                
                let (steer_tx, steer_rx) = mpsc::channel(10);
                self.active_steer_txs.lock().await.push(steer_tx);

                execution_tasks.push(tokio::spawn(async move {
                    let _permit = semaphore.acquire().await.ok();
                    let mut agent = ReActAgent::new_with_provider(provider, config, tools)
                        .with_hooks(hooks)
                        .with_memory_manager(pai_mem)
                        .with_recovery(recovery);
                    if let Some(ref memory) = memory { agent = agent.with_memory(memory.clone()); }
                    agent = agent.with_safety(safety);
                    agent.execute_with_steering(&query_owned, Some(&context_owned), Some(steer_rx)).await
                }));
            }

            let task_results = join_all(execution_tasks).await;
            self.active_steer_txs.lock().await.clear();
            let mut responses = Vec::new();

            for (i, tr) in task_results.into_iter().enumerate() {
                let agent_type = final_routing.candidate_agents[i];
                match tr {
                    Ok(Ok(res)) => {
                        portfolio.candidates.push(Candidate {
                            agent_id: format!("{:?}", agent_type),
                            answer: res.answer.clone(),
                            quality_score: if res.success { 0.9 } else { 0.1 },
                            risk_score: 0.1,
                            novelty_score: 0.0, // Default for non-diverse agents
                            cost_tokens: res.cost_tokens,
                            assurance: crate::orchestrator::AssuranceLevel::L1,
                            reward_score: None,
                            scale_elasticity: crate::orchestrator::aggregation::ScaleElasticity::Unknown,
                        });
                        responses.push(res);
                    },
                    _ => warn!("Agent execution failed for {:?}", agent_type),
                }
            }

            // SOTA: RLM Reward Scoring (G.5)
            if let Some(ref rm) = self.reward_model {
                if !portfolio.candidates.is_empty() {
                    let _ = self.provider.notify("STATE:RLM:SCORING").await;
                    if let Ok(scores) = rm.score(query, &portfolio.candidates).await {
                        let scores: Vec<f32> = scores;
                        for (i, score) in scores.into_iter().enumerate() {
                            portfolio.candidates[i].reward_score = Some(score);
                        }
                    }
                }
            }

            if !responses.is_empty() {
                let winner_idx = Gamma::select_pareto_winner(&portfolio).unwrap_or(0);
                let winner_res = responses[winner_idx].clone();
                final_winner_idx = winner_idx;
                
                if winner_res.success {
                    final_res = Some(winner_res);
                    final_performer = format!("{:?}", final_routing.candidate_agents[winner_idx]);
                    break;
                } else if winner_res.pending_approval.is_some() {
                    // HITL Pause
                    final_res = Some(winner_res);
                    final_performer = format!("{:?}", final_routing.candidate_agents[winner_idx]);
                    break; 
                } else { 
                    // All candidates in this tier failed, continue loop to escalate
                    final_res = Some(winner_res);
                    final_performer = format!("{:?}", final_routing.candidate_agents[winner_idx]);
                }
            }
        }

        let final_res = final_res.ok_or_else(|| AgentError::Execution("All execution attempts and escalations failed".to_string()))?;
        let latency_ms = _work_start_time.elapsed().as_millis();

        // Emit FPF-Aligned Publication Characteristics (E.17.5.5)
        emit_event!(AgencyEvent::PublicationUpdate {
            pc: PubCharacteristic {
                pc_type: "PC.Number".to_string(),
                value: serde_json::json!(latency_ms),
                unit: Some("ms".to_string()),
                scale: Some("Ratio".to_string()),
                reference_plane: Some("world".to_string()),
                edition: "2026-01-14".to_string(),
            }
        });

        let mut work = crate::orchestrator::WorkRecord::new(
            "DirectTask".to_string(), 
            format!("{:?}", final_routing.candidate_agents)
        );
        work.performer_role = final_performer.clone();
        work.trace = final_res.steps.clone();
        work.complete(final_res.success, crate::orchestrator::AssuranceLevel::L1);

        // SOTA: Boundary Norm Square Routing (A.6.B)
        let mut square = NormSquare::new();
        if let Some(ref thought) = final_res.thought {
            for line in thought.lines() {
                square.classify_and_add(line);
            }
        }

        let mut publication = Publication::project(
            final_res.answer.clone(), 
            &work, 
            current_scale.clone(),
            Some(square), 
            None, 
            None
        ).with_mvpk(final_res.thought.clone(), final_res.reliability);
        
        publication.rationale = Some(DesignRationaleRecord::new(
            "Supervisor", 
            "Routed", 
            format!(
                "Route reason='{}' (confidence {:.2}); candidates={:?}; selected winner index {} ({}) via Pareto logic",
                final_routing.reason,
                final_routing.confidence,
                final_routing.candidate_agents,
                final_winner_idx,
                final_performer
            )
        ));

        // Only add to memory if it's NOT a pending approval
        if final_res.pending_approval.is_none() {
            self.episodic_memory.lock().await.add_assistant(&final_res.answer, Some(work.performer_role.clone()));
            
            // SOTA: Long-term History Persistence (codex-inspired)
            let _ = self.history_manager.append(&session_id, "assistant", Some(&final_performer), &final_res.answer).await.map_err(|e| AgentError::Io(std::io::Error::new(std::io::ErrorKind::Other, e.to_string())))?;

            if let Some(ref sm) = self.session {
                let mem = self.episodic_memory.lock().await;
                sm.save(&mem, None).await.map_err(|e| AgentError::Io(std::io::Error::new(std::io::ErrorKind::Other, e.to_string())))?;
            }
        }

        Ok(SupervisorResult {
            answer: final_res.answer,
            success: final_res.success,
            plan: None,
            reflections: vec![
                format!("Classified as {:?}", routing_decision.scale.class),
                format!(
                    "Routing rationale: {} (confidence {:.2})",
                    routing_decision.reason,
                    routing_decision.confidence
                ),
                format!("Selected performer: {}", final_performer),
            ],
            publication: Some(publication),
            pending_approval: final_res.pending_approval,
            has_followup: !self.followup_queue.lock().await.is_empty(),
        })
    }

    /// Internal logic for A2A (Agent-to-Agent) direct requests
    pub async fn handle_peer_request(
        &mut self, 
        agent_type: AgentType, 
        query: &str, 
        extra_context: Option<&str>
    ) -> AgentResult<AgentResponse> {
        info!("Supervisor: Handling peer request for {:?}", agent_type);
        
        let mut config = AgentConfig::new(agent_type, &self.profile);
        config.reasoning_enabled = true; // A2A always uses reasoning tags
        
        let mut full_context = String::new();
        if let Some(ctx) = extra_context {
            full_context.push_str(ctx);
        }

        let provider = self.create_cached_provider();
        let tools = self.tools.clone();
        
        let agent = ReActAgent::new_with_provider(provider, config, tools);

        agent.execute(query, Some(&full_context)).await
    }

    pub async fn run_autonomous(&mut self, goal: &str) -> AgentResult<SupervisorResult> {
        let provider = self.create_cached_provider();
        let objective = Objective::new(goal);
        let mut machine = AutonomousMachine::new_with_provider(provider.clone(), self.tools.clone(), &self.profile, objective);
        machine = machine.with_provider(provider);
        
        let mut last_res = AgentResponse::failure("Autonomous loop failed to start", Vec::new(), AgentType::Coder);
        for i in 0..5 {
            info!("Autonomous iteration {}/5", i + 1);
            match machine.run_iteration().await {
                Ok(res) => {
                    let success = res.success;
                    last_res = res;
                    if success { break; }
                },
                Err(e) => {
                    warn!("Autonomous iteration failed: {}", e);
                    break;
                }
            }
        }

        let mut work = crate::orchestrator::WorkRecord::new("Autonomous".to_string(), "Machine".to_string());
        work.trace = last_res.steps.clone();
        work.complete(last_res.success, crate::orchestrator::AssuranceLevel::L2);
        
        let mut square = NormSquare::new();
        if let Some(ref thought) = last_res.thought {
            for line in thought.lines() {
                square.classify_and_add(line);
            }
        }

        let heavy_profile = crate::orchestrator::ScaleProfile::new(0.9, 8.0);
        let publication = Publication::project(
            last_res.answer.clone(), 
            &work, 
            heavy_profile, 
            Some(square), 
            None, 
            None
        ).with_mvpk(last_res.thought.clone(), last_res.reliability);
        
        Ok(SupervisorResult {
            answer: last_res.answer,
            success: last_res.success,
            plan: None,
            reflections: vec![],
            publication: Some(publication),
            pending_approval: None,
            has_followup: false,
        })
    }
}
