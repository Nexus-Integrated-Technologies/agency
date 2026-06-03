use std::sync::Arc;
use std::path::PathBuf;
use anyhow::Result;

use pai_core::algorithm::{AlgorithmEngine, EffortLevel, ISCSource};
use pai_core::identity::{ResponseFormatter, PAIIdentity};
use pai_core::agents::AgentFactory;
use pai_core::prompting::PromptEngine;
use pai_core::telos::TelosEngine;
use pai_core::skills::SkillRegistry;
use pai_core::fabric::FabricRegistry;
use pai_core::classifier::EffortClassifier;
use pai_core::learning::LearningEngine;
use pai_core::orchestration::DynamicCapabilityLoader;
use pai_core::manifest::ManifestEngine;
use pai_core::upgrades::UpgradeMonitor;
use pai_core::privacy::PrivacyGuard;

use crate::orchestrator::Supervisor;

pub struct PAIOrchestrator<'a> {
    pub engine: AlgorithmEngine,
    pub supervisor: Arc<tokio::sync::Mutex<Supervisor>>,
    pub formatter: ResponseFormatter,
    pub agent_factory: AgentFactory,
    pub prompt_engine: PromptEngine<'a>,
    pub telos: TelosEngine,
    pub skill_registry: SkillRegistry,
    pub fabric: FabricRegistry,
    pub classifier: EffortClassifier,
    pub learning: LearningEngine,
    pub capability_loader: DynamicCapabilityLoader,
    pub manifest: ManifestEngine,
    pub sentinel: UpgradeMonitor,
    pub privacy: PrivacyGuard,
}

impl<'a> PAIOrchestrator<'a> {
    fn verification_analysis(
        success: bool,
        has_pending_approval: bool,
        reflections: &[String],
        update_notice: &str,
    ) -> String {
        let verification = if has_pending_approval {
            "Task execution paused: verification pending user approval."
        } else if success {
            "Task verified: supervisor reported successful execution."
        } else {
            "Task verification failed: supervisor reported unresolved issues."
        };

        let rationale = if reflections.is_empty() {
            String::new()
        } else {
            format!(" Rationale: {}", reflections.join(" | "))
        };

        format!("{verification}{rationale}{update_notice}")
    }

    pub fn new(mut _effort: EffortLevel, supervisor: Arc<tokio::sync::Mutex<Supervisor>>) -> Self {
        let home = std::env::var("HOME").unwrap_or_default();
        let pai_dir = PathBuf::from(std::env::var("PAI_DIR").unwrap_or_else(|_| format!("{}/.config/pai", home)));
        
        let classifier = EffortClassifier::new();
        
        // Use standard paths within PAI_DIR
        let traits_path = pai_dir.join("agents").join("Traits.yaml");
        // Fallback or ensure directory exists
        let agent_factory = AgentFactory::from_yaml(&traits_path)
            .unwrap_or_else(|_| AgentFactory::new());

        let prompt_engine = PromptEngine::new();
        // Templates would be loaded here in a full implementation

        let mut skill_registry = SkillRegistry::new();
        let _ = skill_registry.scan_directory(&pai_dir.join("skills"));

        let capability_loader = DynamicCapabilityLoader::new();

        Self {
            engine: AlgorithmEngine::new(EffortLevel::Standard),
            supervisor,
            formatter: ResponseFormatter::new(PAIIdentity::default()),
            agent_factory,
            prompt_engine,
            telos: TelosEngine::new(pai_dir.clone()),
            skill_registry,
            fabric: FabricRegistry::new(pai_dir.clone()),
            classifier,
            learning: LearningEngine::new(pai_dir.clone()),
            capability_loader,
            manifest: ManifestEngine::new(pai_dir),
            sentinel: UpgradeMonitor::new(),
            privacy: PrivacyGuard::new(),
        }
    }

    pub async fn run_task(&mut self, request: &str) -> Result<String> {
        // PAI: Privacy Guard Pre-Check
        if self.privacy.is_leak(request) {
            return Err(anyhow::anyhow!("BLOCKED: Request attempts to access protected PAI files."));
        }

        let effort = self.classifier.classify(request);
        
        self.engine = AlgorithmEngine::new(effort);
        self.engine.add_requirement(request, ISCSource::Explicit);
        
        // PAI: Reinforcement Context (Lessons Learned)
        let lessons = self.learning.load_lessons(request).await?;
        
        // Phase 2: THINK
        self.engine.advance_phase(); 
        
        let agent_identity = self.agent_factory.compose_agent("technical", "meticulous", "systematic")
            .unwrap_or_else(|_| "You are a helpful PAI assistant.".to_string());
        
        // Phase 5: EXECUTE
        self.engine.advance_phase();
        
        let mut supervisor = self.supervisor.lock().await;
        let query = format!("{}\n\nIdentity Context:\n{}\n\nReinforcement Context:\n{}\n\nISC State:\n{}", 
            request, agent_identity, lessons, self.engine.generate_isc_table());
            
        let res = supervisor.handle(&query).await?;
        
        // Phase 6: VERIFY
        self.engine.advance_phase(); 
        
        // Phase 7: LEARN
        self.engine.advance_phase(); 
        
        // PAI: Temporal Sentinel Check
        let mut update_notice = String::new();
        if let Ok(updates) = self.sentinel.check_for_updates().await {
            if !updates.is_empty() {
                update_notice = format!("\n🔔 SYSTEM ALERT: {} new PAI upgrade(s) available.", updates.len());
            }
        }

        let analysis = Self::verification_analysis(
            res.success,
            res.pending_approval.is_some(),
            &res.reflections,
            &update_notice,
        );

        let output = self.formatter.format_response(
            &format!("Executed task: {}", request),
            &analysis,
            &["Observation", "Execution", "Verification", "Sentinel Update Check"],
            &res.answer,
            "PAI cycle complete."
        );
        
        Ok(output)
    }
}

#[cfg(test)]
mod tests {
    use super::PAIOrchestrator;

    #[test]
    fn verification_analysis_reports_success_without_overclaiming() {
        let analysis = PAIOrchestrator::verification_analysis(
            true,
            false,
            &vec!["Selected performer: Coder".to_string()],
            "",
        );
        assert!(analysis.contains("Task verified: supervisor reported successful execution."));
        assert!(!analysis.contains("No issues found"));
        assert!(analysis.contains("Selected performer: Coder"));
    }

    #[test]
    fn verification_analysis_reports_pending_approval() {
        let analysis = PAIOrchestrator::verification_analysis(false, true, &[], "");
        assert!(analysis.contains("verification pending user approval"));
    }

    #[test]
    fn verification_analysis_reports_failure() {
        let analysis = PAIOrchestrator::verification_analysis(false, false, &[], "");
        assert!(analysis.contains("verification failed"));
    }
}
