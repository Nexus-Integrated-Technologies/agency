use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentResponse {
    pub answer: String,
    pub quality_score: f32,
    pub risk_score: f32,
}

pub struct SwarmAggregator;

impl SwarmAggregator {
    pub fn select_pareto_winner(responses: &[AgentResponse]) -> Option<&AgentResponse> {
        // PAI Standard: Maximize Quality / Minimize Risk
        responses
            .iter()
            .filter_map(|response| Self::pareto_score(response).map(|score| (response, score)))
            .max_by(|(_, score_a), (_, score_b)| score_a.total_cmp(score_b))
            .map(|(response, _)| response)
    }

    pub fn steelman(responses: &[AgentResponse]) -> String {
        let mut aggregate = String::from("# Swarm Synthesis (Steelmanned)\n\n");
        for (i, res) in responses.iter().enumerate() {
            aggregate.push_str(&format!("### Perspective {}\n{}\n\n", i + 1, res.answer));
        }
        aggregate
    }

    fn pareto_score(response: &AgentResponse) -> Option<f32> {
        let score = response.quality_score * (1.0 - response.risk_score);
        score.is_finite().then_some(score)
    }
}

#[cfg(test)]
mod tests {
    use super::{AgentResponse, SwarmAggregator};

    #[test]
    fn select_pareto_winner_prefers_highest_score() {
        let responses = vec![
            AgentResponse {
                answer: "A".to_string(),
                quality_score: 0.6,
                risk_score: 0.4,
            },
            AgentResponse {
                answer: "B".to_string(),
                quality_score: 0.9,
                risk_score: 0.2,
            },
        ];

        let winner = SwarmAggregator::select_pareto_winner(&responses).expect("winner expected");
        assert_eq!(winner.answer, "B");
    }

    #[test]
    fn select_pareto_winner_ignores_non_finite_scores() {
        let responses = vec![
            AgentResponse {
                answer: "NaN".to_string(),
                quality_score: f32::NAN,
                risk_score: 0.0,
            },
            AgentResponse {
                answer: "Winner".to_string(),
                quality_score: 0.8,
                risk_score: 0.1,
            },
        ];

        let winner = SwarmAggregator::select_pareto_winner(&responses).expect("winner expected");
        assert_eq!(winner.answer, "Winner");
    }

    #[test]
    fn select_pareto_winner_returns_none_for_all_non_finite_scores() {
        let responses = vec![
            AgentResponse {
                answer: "A".to_string(),
                quality_score: f32::NAN,
                risk_score: 0.2,
            },
            AgentResponse {
                answer: "B".to_string(),
                quality_score: f32::INFINITY,
                risk_score: 0.1,
            },
        ];

        assert!(SwarmAggregator::select_pareto_winner(&responses).is_none());
    }
}
