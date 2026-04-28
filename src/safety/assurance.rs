use serde::{Deserialize, Serialize};
use serde_json::Value;
use crate::tools::Tool;
use std::sync::Arc;

const FORMALITY_STRICT_SCORE: f32 = 1.0;
const FORMALITY_BASE_SCORE: f32 = 0.5;
const HIGH_RISK_STRICT_SCOPE_SCORE: f32 = 0.9;
const HIGH_RISK_VAGUE_SCOPE_SCORE: f32 = 0.4;
const STANDARD_SCOPE_SCORE: f32 = 0.8;
const HIGH_RISK_PARAM_LEN_THRESHOLD: usize = 10;

/// FPF-aligned Trust & Assurance Calculus (B.3)
/// 
/// R = F * G (Simplified for local agency)
/// Reliability = Formality * Scope Alignment
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssuranceScore {
    /// Formality (0.0 - 1.0): Structural integrity of the request
    pub f: f32,
    /// Scope (0.0 - 1.0): Alignment with U.WorkScope
    pub g: f32,
    /// Reliability (0.0 - 1.0): The final trust level
    pub r: f32,
}

impl AssuranceScore {
    pub fn calculate(tool: Arc<dyn Tool>, params: &Value) -> Self {
        let f = Self::formality_score(params);
        let scope = tool.work_scope();
        let g = Self::scope_alignment_score(params, &scope);

        Self {
            f,
            g,
            r: f * g,
        }
    }

    pub fn is_trustworthy(&self) -> bool {
        self.r > 0.6
    }

    pub fn get_warning(&self) -> Option<String> {
        if self.r <= 0.4 {
            Some("CRITICAL: Low assurance. Plan is vague or tool is high-risk.".to_string())
        } else if self.r <= 0.6 {
            Some("WARNING: Medium assurance. Verification recommended.".to_string())
        } else {
            None
        }
    }

    fn formality_score(params: &Value) -> f32 {
        match params.as_object() {
            Some(obj) if !obj.is_empty() => FORMALITY_STRICT_SCORE,
            _ => FORMALITY_BASE_SCORE,
        }
    }

    fn scope_alignment_score(params: &Value, scope: &Value) -> f32 {
        if Self::is_high_risk_scope(scope) {
            if params.to_string().len() > HIGH_RISK_PARAM_LEN_THRESHOLD {
                HIGH_RISK_STRICT_SCOPE_SCORE
            } else {
                HIGH_RISK_VAGUE_SCOPE_SCORE
            }
        } else {
            STANDARD_SCOPE_SCORE
        }
    }

    fn is_high_risk_scope(scope: &Value) -> bool {
        matches!(
            scope.get("status").and_then(Value::as_str),
            Some("highly_constrained" | "highly_agential")
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use serde_json::json;
    use crate::tools::ToolOutput;

    struct MockTool {
        scope: Value,
    }

    #[async_trait]
    impl Tool for MockTool {
        fn name(&self) -> String {
            "mock".to_string()
        }

        fn description(&self) -> String {
            "Mock tool for assurance scoring tests.".to_string()
        }

        fn parameters(&self) -> Value {
            json!({})
        }

        fn work_scope(&self) -> Value {
            self.scope.clone()
        }

        async fn execute(&self, _params: Value) -> crate::agent::AgentResult<ToolOutput> {
            Ok(ToolOutput::success(json!({}), "ok"))
        }
    }

    #[test]
    fn scores_high_for_non_empty_object_params() {
        let tool = Arc::new(MockTool { scope: json!({ "status": "unconstrained" }) });
        let score = AssuranceScore::calculate(tool, &json!({"query": "status"}));
        assert_eq!(score.f, FORMALITY_STRICT_SCORE);
        assert_eq!(score.g, STANDARD_SCOPE_SCORE);
        assert_eq!(score.r, FORMALITY_STRICT_SCORE * STANDARD_SCOPE_SCORE);
    }

    #[test]
    fn lowers_formality_for_empty_or_non_object_params() {
        let tool = Arc::new(MockTool { scope: json!({ "status": "unconstrained" }) });
        let empty_object = AssuranceScore::calculate(tool.clone(), &json!({}));
        let non_object = AssuranceScore::calculate(tool, &json!("raw-input"));

        assert_eq!(empty_object.f, FORMALITY_BASE_SCORE);
        assert_eq!(non_object.f, FORMALITY_BASE_SCORE);
    }

    #[test]
    fn high_risk_scopes_require_descriptive_params() {
        let high_risk = Arc::new(MockTool { scope: json!({ "status": "highly_constrained" }) });
        let vague = AssuranceScore::calculate(high_risk.clone(), &json!({ "a": 1 }));
        let precise = AssuranceScore::calculate(
            high_risk,
            &json!({ "command": "run comprehensive validation checks" }),
        );

        assert_eq!(vague.g, HIGH_RISK_VAGUE_SCOPE_SCORE);
        assert_eq!(precise.g, HIGH_RISK_STRICT_SCOPE_SCORE);
    }
}
