use std::process::Command;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use reqwest::blocking::Client;
use reqwest::header::AUTHORIZATION;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use super::config::NanoclawConfig;
use super::db::NanoclawDb;
use super::pm::{build_pm_issue_key, PmIssueMemory, PmIssueTrackedComment};

const LINEAR_GRAPHQL_URL: &str = "https://api.linear.app/graphql";
const LINEAR_API_KEY_AS_BEARER_ERROR: &str = "trying to use an api key as a bearer token";

const TEAM_FIELDS: &str = r#"
  id
  key
  name
"#;

const WORKFLOW_STATE_FIELDS: &str = r#"
  id
  name
  type
  position
  description
  team {
    id
    key
    name
  }
"#;

const ISSUE_FIELDS: &str = r#"
  id
  identifier
  title
  url
  description
  priority
  dueDate
  createdAt
  updatedAt
  team {
    id
    key
    name
  }
  project {
    id
    name
    slugId
    url
    description
    content
  }
  state {
    id
    name
    type
    position
    description
    team {
      id
      key
      name
    }
  }
  assignee {
    id
    name
    displayName
    email
    active
  }
"#;

const DETAILED_ISSUE_FIELDS: &str = r#"
  id
  identifier
  title
  url
  description
  priority
  dueDate
  createdAt
  updatedAt
  team {
    id
    key
    name
  }
  project {
    id
    name
    slugId
    url
    description
    content
  }
  state {
    id
    name
    type
    position
    description
    team {
      id
      key
      name
    }
  }
  assignee {
    id
    name
    displayName
    email
    active
  }
  labels(first: 20) {
    nodes {
      id
      name
      color
      description
      team {
        id
        key
        name
      }
    }
  }
"#;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct LinearTeamSummary {
    pub id: String,
    pub key: String,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct LinearUserSummary {
    pub id: String,
    pub name: String,
    pub display_name: Option<String>,
    pub email: Option<String>,
    pub active: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct LinearWorkflowStateSummary {
    pub id: String,
    pub name: String,
    #[serde(rename = "type")]
    pub kind: Option<String>,
    pub position: Option<i64>,
    pub description: Option<String>,
    pub team: Option<LinearTeamSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct LinearProjectSummary {
    pub id: String,
    pub name: String,
    pub slug_id: Option<String>,
    pub url: Option<String>,
    pub description: Option<String>,
    pub content: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct LinearLabelSummary {
    pub id: String,
    pub name: String,
    pub color: Option<String>,
    pub description: Option<String>,
    pub team: Option<LinearTeamSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct LinearIssueSummary {
    pub id: String,
    pub identifier: String,
    pub title: String,
    pub url: Option<String>,
    pub description: Option<String>,
    pub priority: Option<i64>,
    pub due_date: Option<String>,
    pub created_at: Option<String>,
    pub updated_at: Option<String>,
    pub team: Option<LinearTeamSummary>,
    pub project: Option<LinearProjectSummary>,
    pub state: Option<LinearWorkflowStateSummary>,
    pub assignee: Option<LinearUserSummary>,
    #[serde(default, deserialize_with = "deserialize_linear_label_nodes")]
    pub labels: Vec<LinearLabelSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LinearIssueCommentSummary {
    pub id: String,
    pub body: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum LinearIssueQualitySeverity {
    Critical,
    Warning,
    Info,
}

impl LinearIssueQualitySeverity {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Critical => "critical",
            Self::Warning => "warning",
            Self::Info => "info",
        }
    }

    fn label(&self) -> &'static str {
        match self {
            Self::Critical => "Critical",
            Self::Warning => "Warning",
            Self::Info => "Info",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LinearIssueQualityGap {
    pub key: String,
    pub severity: LinearIssueQualitySeverity,
    pub description: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LinearPmMemoryView {
    pub summary: Option<String>,
    pub next_action: Option<String>,
    pub blockers: Option<Vec<String>>,
    pub current_state: Option<String>,
    pub repo_hint: Option<String>,
    pub last_source: Option<String>,
    pub details: Option<Value>,
    pub updated_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinearTeamsTaskInput {
    pub limit: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LinearTeamsTaskResult {
    pub ok: bool,
    pub teams: Option<Vec<LinearTeamSummary>>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinearIssueQualityTaskInput {
    pub identifier: String,
    pub apply: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LinearIssueQualityTaskResult {
    pub ok: bool,
    pub identifier: String,
    pub issue: Option<LinearIssueSummary>,
    pub score: Option<i64>,
    pub gaps: Option<Vec<LinearIssueQualityGap>>,
    pub body: Option<String>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LinearPmMemoryTaskInput {
    pub identifier: String,
    pub summary: Option<String>,
    pub next_action: Option<String>,
    pub blockers: Option<Vec<String>>,
    pub current_state: Option<String>,
    pub repo_hint: Option<String>,
    pub last_source: Option<String>,
    pub details: Option<Value>,
    pub merge: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LinearPmMemoryTaskResult {
    pub ok: bool,
    pub identifier: String,
    pub issue_id: Option<String>,
    pub issue_key: Option<String>,
    pub memory: Option<LinearPmMemoryView>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinearIssueCommentUpsertTaskInput {
    pub identifier: String,
    pub body: String,
    pub comment_kind: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LinearCommentTaskResult {
    pub ok: bool,
    pub identifier: String,
    pub issue_id: Option<String>,
    pub comment_id: Option<String>,
    pub deduplicated: Option<bool>,
    pub updated: Option<bool>,
    pub comment_kind: Option<String>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinearIssueTransitionTaskInput {
    pub identifier: String,
    pub state_name: Option<String>,
    pub state_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LinearTransitionTaskResult {
    pub ok: bool,
    pub identifier: String,
    pub issue_id: Option<String>,
    pub previous_state: Option<LinearWorkflowStateSummary>,
    pub next_state: Option<LinearWorkflowStateSummary>,
    pub changed: Option<bool>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinearPolicyAutomationSweepTaskInput {
    pub team_key: Option<String>,
    pub limit: usize,
    pub stale_days: usize,
    pub due_soon_days: usize,
    pub apply: bool,
    pub allow_state_transitions: bool,
    pub max_auto_applied: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LinearPolicyAutomationAction {
    pub identifier: String,
    pub kind: String,
    pub status: String,
    pub details: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LinearPolicyAutomationSweepTaskResult {
    pub ok: bool,
    pub considered_count: Option<usize>,
    pub auto_applied_count: Option<usize>,
    pub needs_approval_count: Option<usize>,
    pub actions: Option<Vec<LinearPolicyAutomationAction>>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinearPmDigestTaskInput {
    pub team_key: Option<String>,
    pub limit: usize,
    pub stale_days: usize,
    pub due_soon_days: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LinearPmDigestTaskResult {
    pub ok: bool,
    pub body: Option<String>,
    pub error: Option<String>,
}

#[derive(Debug, Deserialize)]
struct LinearGraphqlEnvelope<T> {
    data: Option<T>,
    errors: Option<Vec<LinearGraphqlError>>,
}

#[derive(Debug, Deserialize)]
struct LinearGraphqlError {
    message: Option<String>,
    extensions: Option<LinearGraphqlErrorExtensions>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LinearGraphqlErrorExtensions {
    user_presentable_message: Option<String>,
}

#[derive(Debug, Deserialize)]
struct LinearTeamStates {
    key: String,
    states: Option<LinearNodeList<LinearWorkflowStateSummary>>,
}

#[derive(Debug, Deserialize)]
struct LinearNodeList<T> {
    nodes: Option<Vec<T>>,
}

#[derive(Debug, Deserialize)]
struct LinearCommentLookupIssue {
    comments: Option<LinearNodeList<LinearIssueCommentSummary>>,
}

#[derive(Debug, Deserialize)]
struct LinearCommentMutationPayload {
    success: bool,
    comment: Option<LinearIssueCommentSummary>,
}

#[derive(Debug, Deserialize)]
struct LinearIssueUpdatePayload {
    success: bool,
    issue: Option<LinearIssueSummary>,
}

#[derive(Debug, Deserialize)]
struct LinearDetailedIssueWrapper {
    issue: Option<LinearIssueSummary>,
}

#[derive(Debug, Deserialize)]
struct LinearIssueLookupWrapper {
    issue: Option<LinearIssueSummary>,
}

#[derive(Debug, Deserialize)]
struct LinearCommentLookupWrapper {
    issue: Option<LinearCommentLookupIssue>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LinearCommentCreateWrapper {
    comment_create: LinearCommentMutationPayload,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LinearCommentUpdateWrapper {
    comment_update: LinearCommentMutationPayload,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LinearIssueUpdateWrapper {
    issue_update: LinearIssueUpdatePayload,
}

#[derive(Debug, Deserialize)]
struct LinearTeamByIdWrapper {
    team: Option<LinearTeamStates>,
}

#[derive(Debug, Deserialize)]
struct LinearTeamsByKeyWrapper {
    teams: Option<LinearNodeList<LinearTeamStates>>,
}

#[derive(Debug, Deserialize)]
struct LinearTeamsWrapper {
    teams: Option<LinearNodeList<LinearTeamSummary>>,
}

pub fn run_linear_teams_task(
    config: &NanoclawConfig,
    input: LinearTeamsTaskInput,
) -> LinearTeamsTaskResult {
    let token = get_linear_read_key(config);
    if token.is_empty() {
        return LinearTeamsTaskResult {
            ok: false,
            teams: None,
            error: Some(
                "LINEAR_API_KEY or LINEAR_WRITE_API_KEY is not configured on the host.".to_string(),
            ),
        };
    }

    match get_teams(&token, input.limit) {
        Ok(teams) => LinearTeamsTaskResult {
            ok: true,
            teams: Some(teams),
            error: None,
        },
        Err(error) => LinearTeamsTaskResult {
            ok: false,
            teams: None,
            error: Some(error.to_string()),
        },
    }
}

pub fn run_linear_issue_quality_task(
    db: &NanoclawDb,
    config: &NanoclawConfig,
    input: LinearIssueQualityTaskInput,
) -> LinearIssueQualityTaskResult {
    let identifier = input.identifier.trim().to_string();
    let token = if input.apply {
        get_linear_write_key(config)
    } else {
        get_linear_read_key(config)
    };
    if token.is_empty() {
        return LinearIssueQualityTaskResult {
            ok: false,
            identifier,
            issue: None,
            score: None,
            gaps: None,
            body: None,
            error: Some(
                "LINEAR_API_KEY or LINEAR_WRITE_API_KEY is not configured on the host.".to_string(),
            ),
        };
    }

    match lookup_detailed_issue(&identifier, &token) {
        Ok(Some(issue)) => {
            let gaps = build_issue_quality_gaps(&issue);
            let score = compute_issue_quality_score(&gaps);
            let issue_key = build_pm_issue_key(&issue.id);
            let body = (!gaps.is_empty()).then(|| build_issue_quality_body(&issue, &gaps));
            let memory = PmIssueMemory {
                issue_key: issue_key.clone(),
                issue_identifier: issue.identifier.clone(),
                summary: Some(summarize_pm_memory(&issue)),
                next_action: Some(if gaps.is_empty() {
                    "Issue quality is sufficient for autonomous execution.".to_string()
                } else {
                    "Fill in missing PM issue details before the next execution cycle.".to_string()
                }),
                blockers: Some(
                    gaps.iter()
                        .filter(|gap| gap.severity == LinearIssueQualitySeverity::Critical)
                        .map(|gap| gap.description.clone())
                        .collect(),
                ),
                current_state: issue.state.as_ref().map(|state| state.name.clone()),
                repo_hint: issue
                    .project
                    .as_ref()
                    .map(|project| project.name.clone())
                    .or_else(|| {
                        description_has_implementation_path(issue.description.as_deref())
                            .then_some("described-in-issue".to_string())
                    }),
                last_source: Some(if input.apply {
                    "linear-quality-enforcement".to_string()
                } else {
                    "linear-quality-check".to_string()
                }),
                memory: Some(json!({
                    "qualityScore": score,
                    "gaps": gaps,
                })),
                updated_at: chrono::Utc::now().to_rfc3339(),
            };
            if let Err(error) = db.set_pm_issue_memory(&memory) {
                return LinearIssueQualityTaskResult {
                    ok: false,
                    identifier,
                    issue: Some(issue),
                    score: Some(score),
                    gaps: Some(gaps),
                    body,
                    error: Some(error.to_string()),
                };
            }
            if input.apply {
                if let Some(comment_body) = body.clone() {
                    let comment_result = run_linear_issue_comment_upsert_task(
                        db,
                        config,
                        LinearIssueCommentUpsertTaskInput {
                            identifier: issue.identifier.clone(),
                            body: comment_body,
                            comment_kind: "pm-quality".to_string(),
                        },
                    );
                    if !comment_result.ok {
                        return LinearIssueQualityTaskResult {
                            ok: false,
                            identifier,
                            issue: Some(issue),
                            score: Some(score),
                            gaps: Some(gaps),
                            body,
                            error: comment_result.error,
                        };
                    }
                }
            }

            LinearIssueQualityTaskResult {
                ok: true,
                identifier: issue.identifier.clone(),
                issue: Some(issue),
                score: Some(score),
                gaps: Some(gaps),
                body,
                error: None,
            }
        }
        Ok(None) => LinearIssueQualityTaskResult {
            ok: false,
            identifier: identifier.clone(),
            issue: None,
            score: None,
            gaps: None,
            body: None,
            error: Some(format!(
                "Issue {} was not found via the Linear API.",
                identifier
            )),
        },
        Err(error) => LinearIssueQualityTaskResult {
            ok: false,
            identifier,
            issue: None,
            score: None,
            gaps: None,
            body: None,
            error: Some(error.to_string()),
        },
    }
}

pub fn run_linear_pm_memory_task(
    db: &NanoclawDb,
    config: &NanoclawConfig,
    input: LinearPmMemoryTaskInput,
) -> LinearPmMemoryTaskResult {
    let identifier = input.identifier.trim().to_string();
    if identifier.is_empty() {
        return LinearPmMemoryTaskResult {
            ok: false,
            identifier,
            issue_id: None,
            issue_key: None,
            memory: None,
            error: Some("remember_linear_pm_context requires an issue identifier.".to_string()),
        };
    }

    let wants_write = input.summary.is_some()
        || input.next_action.is_some()
        || input.blockers.is_some()
        || input.current_state.is_some()
        || input.repo_hint.is_some()
        || input.last_source.is_some()
        || input.details.is_some();

    if !wants_write {
        if let Ok(Some(existing)) = db.get_pm_issue_memory_by_identifier(&identifier) {
            return LinearPmMemoryTaskResult {
                ok: true,
                identifier: existing.issue_identifier.clone(),
                issue_id: None,
                issue_key: Some(existing.issue_key.clone()),
                memory: Some(to_pm_memory_view(&existing)),
                error: None,
            };
        }
    }

    let token = get_linear_read_key(config);
    if token.is_empty() {
        return LinearPmMemoryTaskResult {
            ok: false,
            identifier,
            issue_id: None,
            issue_key: None,
            memory: None,
            error: Some(
                "LINEAR_API_KEY or LINEAR_WRITE_API_KEY is not configured on the host.".to_string(),
            ),
        };
    }

    match lookup_detailed_issue(&identifier, &token) {
        Ok(Some(issue)) => {
            let issue_key = build_pm_issue_key(&issue.id);
            let existing = db.get_pm_issue_memory(&issue_key).ok().flatten();
            let next_memory = if wants_write {
                PmIssueMemory {
                    issue_key: issue_key.clone(),
                    issue_identifier: issue.identifier.clone(),
                    summary: input
                        .summary
                        .or_else(|| {
                            input
                                .merge
                                .then(|| existing.as_ref()?.summary.clone())
                                .flatten()
                        })
                        .or_else(|| Some(summarize_pm_memory(&issue))),
                    next_action: input.next_action.or_else(|| {
                        input
                            .merge
                            .then(|| existing.as_ref()?.next_action.clone())
                            .flatten()
                    }),
                    blockers: input.blockers.or_else(|| {
                        input
                            .merge
                            .then(|| existing.as_ref()?.blockers.clone())
                            .flatten()
                    }),
                    current_state: input
                        .current_state
                        .or_else(|| {
                            input
                                .merge
                                .then(|| existing.as_ref()?.current_state.clone())
                                .flatten()
                        })
                        .or_else(|| issue.state.as_ref().map(|state| state.name.clone())),
                    repo_hint: input
                        .repo_hint
                        .or_else(|| {
                            input
                                .merge
                                .then(|| existing.as_ref()?.repo_hint.clone())
                                .flatten()
                        })
                        .or_else(|| issue.project.as_ref().map(|project| project.name.clone())),
                    last_source: input
                        .last_source
                        .or_else(|| {
                            input
                                .merge
                                .then(|| existing.as_ref()?.last_source.clone())
                                .flatten()
                        })
                        .or_else(|| Some("manual-pm-memory".to_string())),
                    memory: input.details.or_else(|| {
                        input
                            .merge
                            .then(|| existing.as_ref()?.memory.clone())
                            .flatten()
                    }),
                    updated_at: chrono::Utc::now().to_rfc3339(),
                }
            } else {
                PmIssueMemory {
                    issue_key: issue_key.clone(),
                    issue_identifier: issue.identifier.clone(),
                    summary: existing
                        .as_ref()
                        .and_then(|memory| memory.summary.clone())
                        .or_else(|| Some(summarize_pm_memory(&issue))),
                    next_action: existing
                        .as_ref()
                        .and_then(|memory| memory.next_action.clone()),
                    blockers: existing.as_ref().and_then(|memory| memory.blockers.clone()),
                    current_state: existing
                        .as_ref()
                        .and_then(|memory| memory.current_state.clone())
                        .or_else(|| issue.state.as_ref().map(|state| state.name.clone())),
                    repo_hint: existing
                        .as_ref()
                        .and_then(|memory| memory.repo_hint.clone())
                        .or_else(|| issue.project.as_ref().map(|project| project.name.clone())),
                    last_source: existing
                        .as_ref()
                        .and_then(|memory| memory.last_source.clone())
                        .or_else(|| Some("pm-memory-read".to_string())),
                    memory: existing.as_ref().and_then(|memory| memory.memory.clone()),
                    updated_at: chrono::Utc::now().to_rfc3339(),
                }
            };

            if let Err(error) = db.set_pm_issue_memory(&next_memory) {
                return LinearPmMemoryTaskResult {
                    ok: false,
                    identifier,
                    issue_id: Some(issue.id),
                    issue_key: Some(issue_key),
                    memory: None,
                    error: Some(error.to_string()),
                };
            }

            let stored = db
                .get_pm_issue_memory(&issue_key)
                .ok()
                .flatten()
                .or_else(|| {
                    db.get_pm_issue_memory_by_identifier(&issue.identifier)
                        .ok()
                        .flatten()
                });
            LinearPmMemoryTaskResult {
                ok: true,
                identifier: issue.identifier,
                issue_id: Some(issue.id),
                issue_key: Some(issue_key),
                memory: stored.as_ref().map(to_pm_memory_view),
                error: None,
            }
        }
        Ok(None) => LinearPmMemoryTaskResult {
            ok: false,
            identifier: identifier.clone(),
            issue_id: None,
            issue_key: None,
            memory: None,
            error: Some(format!(
                "Issue {} was not found via the Linear API.",
                identifier
            )),
        },
        Err(error) => LinearPmMemoryTaskResult {
            ok: false,
            identifier,
            issue_id: None,
            issue_key: None,
            memory: None,
            error: Some(error.to_string()),
        },
    }
}

pub fn run_linear_issue_comment_upsert_task(
    db: &NanoclawDb,
    config: &NanoclawConfig,
    input: LinearIssueCommentUpsertTaskInput,
) -> LinearCommentTaskResult {
    let identifier = input.identifier.trim().to_string();
    let body = input.body.trim().to_string();
    let comment_kind = input.comment_kind.trim().to_string();
    if comment_kind.is_empty() {
        return LinearCommentTaskResult {
            ok: false,
            identifier,
            issue_id: None,
            comment_id: None,
            deduplicated: None,
            updated: None,
            comment_kind: None,
            error: Some("upsert_linear_issue_comment requires a comment kind.".to_string()),
        };
    }
    if body.is_empty() {
        return LinearCommentTaskResult {
            ok: false,
            identifier,
            issue_id: None,
            comment_id: None,
            deduplicated: None,
            updated: None,
            comment_kind: Some(comment_kind),
            error: Some("Linear comment body is required.".to_string()),
        };
    }

    let token = get_linear_write_key(config);
    if token.is_empty() {
        return LinearCommentTaskResult {
            ok: false,
            identifier,
            issue_id: None,
            comment_id: None,
            deduplicated: None,
            updated: None,
            comment_kind: Some(comment_kind),
            error: Some(
                "LINEAR_WRITE_API_KEY or LINEAR_API_KEY is not configured on the host.".to_string(),
            ),
        };
    }

    let lookup = match lookup_issue(&identifier, &token) {
        Ok(Some(issue)) => issue,
        Ok(None) => {
            return LinearCommentTaskResult {
                ok: false,
                identifier: identifier.clone(),
                issue_id: None,
                comment_id: None,
                deduplicated: None,
                updated: None,
                comment_kind: Some(comment_kind),
                error: Some(format!(
                    "Issue {} could not be resolved before commenting.",
                    identifier
                )),
            };
        }
        Err(error) => {
            return LinearCommentTaskResult {
                ok: false,
                identifier,
                issue_id: None,
                comment_id: None,
                deduplicated: None,
                updated: None,
                comment_kind: Some(comment_kind),
                error: Some(error.to_string()),
            };
        }
    };

    let issue_key = build_pm_issue_key(&lookup.id);
    let tracked = db
        .get_pm_issue_tracked_comment(&issue_key, &comment_kind)
        .ok()
        .flatten();
    let body_hash = hash_normalized_comment_body(&body);

    if let Some(tracked) = tracked.as_ref() {
        if tracked.body_hash.as_deref() == Some(body_hash.as_str()) {
            return LinearCommentTaskResult {
                ok: true,
                identifier: lookup.identifier,
                issue_id: Some(lookup.id),
                comment_id: Some(tracked.comment_id.clone()),
                deduplicated: Some(true),
                updated: Some(false),
                comment_kind: Some(comment_kind),
                error: None,
            };
        }
    }

    if let Some(tracked) = tracked.as_ref() {
        match update_existing_issue_comment(&tracked.comment_id, &body, &token) {
            Ok(Some(updated_comment)) => {
                let tracked_comment = PmIssueTrackedComment {
                    issue_key: issue_key.clone(),
                    issue_identifier: Some(lookup.identifier.clone()),
                    comment_kind: comment_kind.clone(),
                    comment_id: updated_comment.id.clone(),
                    body_hash: Some(body_hash),
                    body: Some(body),
                    updated_at: chrono::Utc::now().to_rfc3339(),
                };
                if let Err(error) = db.set_pm_issue_tracked_comment(&tracked_comment) {
                    return LinearCommentTaskResult {
                        ok: false,
                        identifier: lookup.identifier,
                        issue_id: Some(lookup.id),
                        comment_id: Some(updated_comment.id),
                        deduplicated: Some(false),
                        updated: Some(true),
                        comment_kind: Some(comment_kind),
                        error: Some(error.to_string()),
                    };
                }
                return LinearCommentTaskResult {
                    ok: true,
                    identifier: lookup.identifier,
                    issue_id: Some(lookup.id),
                    comment_id: Some(updated_comment.id),
                    deduplicated: Some(false),
                    updated: Some(true),
                    comment_kind: Some(comment_kind),
                    error: None,
                };
            }
            Ok(None) => {}
            Err(error) => {
                return LinearCommentTaskResult {
                    ok: false,
                    identifier: lookup.identifier,
                    issue_id: Some(lookup.id),
                    comment_id: Some(tracked.comment_id.clone()),
                    deduplicated: Some(false),
                    updated: Some(false),
                    comment_kind: Some(comment_kind),
                    error: Some(error.to_string()),
                };
            }
        }
    }

    match find_existing_issue_comment(&lookup.id, &body, &token) {
        Ok(Some(existing_comment)) => {
            let tracked_comment = PmIssueTrackedComment {
                issue_key,
                issue_identifier: Some(lookup.identifier.clone()),
                comment_kind: comment_kind.clone(),
                comment_id: existing_comment.id.clone(),
                body_hash: Some(body_hash),
                body: Some(body),
                updated_at: chrono::Utc::now().to_rfc3339(),
            };
            if let Err(error) = db.set_pm_issue_tracked_comment(&tracked_comment) {
                return LinearCommentTaskResult {
                    ok: false,
                    identifier: lookup.identifier,
                    issue_id: Some(lookup.id),
                    comment_id: Some(existing_comment.id),
                    deduplicated: Some(true),
                    updated: Some(false),
                    comment_kind: Some(comment_kind),
                    error: Some(error.to_string()),
                };
            }
            LinearCommentTaskResult {
                ok: true,
                identifier: lookup.identifier,
                issue_id: Some(lookup.id),
                comment_id: Some(existing_comment.id),
                deduplicated: Some(true),
                updated: Some(false),
                comment_kind: Some(comment_kind),
                error: None,
            }
        }
        Ok(None) => match create_issue_comment(&lookup.id, &body, &token) {
            Ok(created_comment) => {
                let tracked_comment = PmIssueTrackedComment {
                    issue_key,
                    issue_identifier: Some(lookup.identifier.clone()),
                    comment_kind: comment_kind.clone(),
                    comment_id: created_comment.id.clone(),
                    body_hash: Some(body_hash),
                    body: Some(body),
                    updated_at: chrono::Utc::now().to_rfc3339(),
                };
                if let Err(error) = db.set_pm_issue_tracked_comment(&tracked_comment) {
                    return LinearCommentTaskResult {
                        ok: false,
                        identifier: lookup.identifier,
                        issue_id: Some(lookup.id),
                        comment_id: Some(created_comment.id),
                        deduplicated: Some(false),
                        updated: Some(false),
                        comment_kind: Some(comment_kind),
                        error: Some(error.to_string()),
                    };
                }
                LinearCommentTaskResult {
                    ok: true,
                    identifier: lookup.identifier,
                    issue_id: Some(lookup.id),
                    comment_id: Some(created_comment.id),
                    deduplicated: Some(false),
                    updated: Some(false),
                    comment_kind: Some(comment_kind),
                    error: None,
                }
            }
            Err(error) => LinearCommentTaskResult {
                ok: false,
                identifier: lookup.identifier,
                issue_id: Some(lookup.id),
                comment_id: None,
                deduplicated: Some(false),
                updated: Some(false),
                comment_kind: Some(comment_kind),
                error: Some(error.to_string()),
            },
        },
        Err(error) => LinearCommentTaskResult {
            ok: false,
            identifier: lookup.identifier,
            issue_id: Some(lookup.id),
            comment_id: None,
            deduplicated: Some(false),
            updated: Some(false),
            comment_kind: Some(comment_kind),
            error: Some(error.to_string()),
        },
    }
}

pub fn run_linear_issue_transition_task(
    config: &NanoclawConfig,
    input: LinearIssueTransitionTaskInput,
) -> LinearTransitionTaskResult {
    let identifier = input.identifier.trim().to_string();
    let token = get_linear_write_key(config);
    if token.is_empty() {
        return LinearTransitionTaskResult {
            ok: false,
            identifier,
            issue_id: None,
            previous_state: None,
            next_state: None,
            changed: None,
            error: Some(
                "LINEAR_WRITE_API_KEY or LINEAR_API_KEY is not configured on the host.".to_string(),
            ),
        };
    }
    if input
        .state_name
        .as_deref()
        .map(str::trim)
        .unwrap_or_default()
        .is_empty()
        && input
            .state_id
            .as_deref()
            .map(str::trim)
            .unwrap_or_default()
            .is_empty()
    {
        return LinearTransitionTaskResult {
            ok: false,
            identifier,
            issue_id: None,
            previous_state: None,
            next_state: None,
            changed: None,
            error: Some(
                "transition_linear_issue requires a target state name or state ID.".to_string(),
            ),
        };
    }

    let lookup = match lookup_issue(&identifier, &token) {
        Ok(Some(issue)) => issue,
        Ok(None) => {
            return LinearTransitionTaskResult {
                ok: false,
                identifier: identifier.clone(),
                issue_id: None,
                previous_state: None,
                next_state: None,
                changed: None,
                error: Some(format!(
                    "Issue {} could not be resolved before transitioning.",
                    identifier
                )),
            };
        }
        Err(error) => {
            return LinearTransitionTaskResult {
                ok: false,
                identifier,
                issue_id: None,
                previous_state: None,
                next_state: None,
                changed: None,
                error: Some(error.to_string()),
            };
        }
    };

    match resolve_workflow_state_reference(
        lookup.team.as_ref().and_then(|team| Some(team.id.as_str())),
        lookup
            .team
            .as_ref()
            .and_then(|team| Some(team.key.as_str())),
        input.state_id.as_deref(),
        input.state_name.as_deref(),
        &token,
    ) {
        Ok(state) => {
            if lookup.state.as_ref().map(|current| current.id.as_str()) == Some(state.id.as_str()) {
                return LinearTransitionTaskResult {
                    ok: true,
                    identifier: lookup.identifier,
                    issue_id: Some(lookup.id),
                    previous_state: lookup.state.clone(),
                    next_state: lookup.state,
                    changed: Some(false),
                    error: None,
                };
            }

            match transition_issue(&lookup.id, &state.id, &token) {
                Ok(next_issue) => LinearTransitionTaskResult {
                    ok: true,
                    identifier: lookup.identifier,
                    issue_id: Some(lookup.id),
                    previous_state: lookup.state,
                    next_state: next_issue.state,
                    changed: Some(true),
                    error: None,
                },
                Err(error) => LinearTransitionTaskResult {
                    ok: false,
                    identifier: lookup.identifier,
                    issue_id: Some(lookup.id),
                    previous_state: lookup.state,
                    next_state: None,
                    changed: Some(false),
                    error: Some(error.to_string()),
                },
            }
        }
        Err(error) => LinearTransitionTaskResult {
            ok: false,
            identifier: lookup.identifier,
            issue_id: Some(lookup.id),
            previous_state: lookup.state,
            next_state: None,
            changed: Some(false),
            error: Some(error.to_string()),
        },
    }
}

pub fn run_linear_policy_automation_sweep_task(
    db: &NanoclawDb,
    config: &NanoclawConfig,
    input: LinearPolicyAutomationSweepTaskInput,
) -> LinearPolicyAutomationSweepTaskResult {
    let token = get_linear_read_key(config);
    if token.is_empty() {
        return LinearPolicyAutomationSweepTaskResult {
            ok: false,
            considered_count: None,
            auto_applied_count: None,
            needs_approval_count: None,
            actions: None,
            error: Some(
                "LINEAR_API_KEY or LINEAR_WRITE_API_KEY is not configured on the host.".to_string(),
            ),
        };
    }

    match get_detailed_issues(&token, input.team_key.as_deref(), input.limit.clamp(1, 250)) {
        Ok(issues) => {
            let max_auto_applied = input.max_auto_applied.clamp(1, 100);
            let mut actions = Vec::new();
            let mut auto_applied_count = 0usize;
            let needs_approval_count = 0usize;

            for issue in issues.iter().filter(|issue| issue_is_open(issue)) {
                let quality = run_linear_issue_quality_task(
                    db,
                    config,
                    LinearIssueQualityTaskInput {
                        identifier: issue.identifier.clone(),
                        apply: false,
                    },
                );
                if quality.ok
                    && quality
                        .gaps
                        .as_ref()
                        .map(|gaps| !gaps.is_empty())
                        .unwrap_or(false)
                {
                    if input.apply && auto_applied_count < max_auto_applied {
                        let applied = run_linear_issue_quality_task(
                            db,
                            config,
                            LinearIssueQualityTaskInput {
                                identifier: issue.identifier.clone(),
                                apply: true,
                            },
                        );
                        actions.push(LinearPolicyAutomationAction {
                            identifier: issue.identifier.clone(),
                            kind: "quality-comment".to_string(),
                            status: if applied.ok {
                                "applied".to_string()
                            } else {
                                "failed".to_string()
                            },
                            details: if applied.ok {
                                format!(
                                    "Posted/upserted PM quality guidance for {} issue gap(s).",
                                    applied.gaps.as_ref().map(Vec::len).unwrap_or(0)
                                )
                            } else {
                                applied.error.unwrap_or_else(|| {
                                    "Failed to enforce PM quality guidance.".to_string()
                                })
                            },
                        });
                        if applied.ok {
                            auto_applied_count += 1;
                        }
                    } else {
                        actions.push(LinearPolicyAutomationAction {
                            identifier: issue.identifier.clone(),
                            kind: "quality-comment".to_string(),
                            status: "recommended".to_string(),
                            details: format!(
                                "Issue quality gaps detected ({}); comment would be upserted on apply.",
                                quality.gaps.as_ref().map(Vec::len).unwrap_or(0)
                            ),
                        });
                    }
                }
                if auto_applied_count >= max_auto_applied {
                    break;
                }
            }

            LinearPolicyAutomationSweepTaskResult {
                ok: true,
                considered_count: Some(issues.len()),
                auto_applied_count: Some(auto_applied_count),
                needs_approval_count: Some(needs_approval_count),
                actions: Some(actions),
                error: None,
            }
        }
        Err(error) => LinearPolicyAutomationSweepTaskResult {
            ok: false,
            considered_count: None,
            auto_applied_count: None,
            needs_approval_count: None,
            actions: None,
            error: Some(error.to_string()),
        },
    }
}

pub fn run_linear_pm_digest_task(
    config: &NanoclawConfig,
    input: LinearPmDigestTaskInput,
) -> LinearPmDigestTaskResult {
    let token = get_linear_read_key(config);
    if token.is_empty() {
        return LinearPmDigestTaskResult {
            ok: false,
            body: None,
            error: Some(
                "LINEAR_API_KEY or LINEAR_WRITE_API_KEY is not configured on the host.".to_string(),
            ),
        };
    }

    match get_detailed_issues(&token, input.team_key.as_deref(), input.limit.clamp(1, 250)) {
        Ok(issues) => {
            let stale_cutoff = chrono::Utc::now() - chrono::Duration::days(input.stale_days as i64);
            let due_soon_date = (chrono::Utc::now()
                + chrono::Duration::days(input.due_soon_days as i64))
            .date_naive();
            let open_issues = issues
                .iter()
                .filter(|issue| issue_is_open(issue))
                .collect::<Vec<_>>();
            let blocked = open_issues
                .iter()
                .filter(|issue| issue_is_blocked(issue))
                .map(|issue| issue.identifier.clone())
                .collect::<Vec<_>>();
            let stale = open_issues
                .iter()
                .filter(|issue| {
                    issue
                        .updated_at
                        .as_deref()
                        .and_then(parse_rfc3339)
                        .map(|updated| updated < stale_cutoff)
                        .unwrap_or(false)
                })
                .map(|issue| issue.identifier.clone())
                .collect::<Vec<_>>();
            let overdue = open_issues
                .iter()
                .filter(|issue| {
                    issue
                        .due_date
                        .as_deref()
                        .and_then(|value| chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d").ok())
                        .map(|due| due < chrono::Utc::now().date_naive())
                        .unwrap_or(false)
                })
                .map(|issue| issue.identifier.clone())
                .collect::<Vec<_>>();
            let due_soon = open_issues
                .iter()
                .filter(|issue| {
                    issue
                        .due_date
                        .as_deref()
                        .and_then(|value| chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d").ok())
                        .map(|due| due >= chrono::Utc::now().date_naive() && due <= due_soon_date)
                        .unwrap_or(false)
                })
                .map(|issue| issue.identifier.clone())
                .collect::<Vec<_>>();
            let mut assignee_counts = std::collections::BTreeMap::new();
            for issue in &open_issues {
                let assignee = issue
                    .assignee
                    .as_ref()
                    .and_then(|user| {
                        user.display_name
                            .clone()
                            .or_else(|| Some(user.name.clone()))
                            .or(user.email.clone())
                    })
                    .unwrap_or_else(|| "Unassigned".to_string());
                *assignee_counts.entry(assignee).or_insert(0usize) += 1;
            }
            let mut top_assignees = assignee_counts.into_iter().collect::<Vec<_>>();
            top_assignees.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
            let title = input
                .team_key
                .as_deref()
                .map(|team| format!("PM digest for team {}", team))
                .unwrap_or_else(|| "PM digest".to_string());
            let mut lines = vec![
                title,
                format!(
                    "Scope: {} issues | {} open | {} blocked | {} stale | {} overdue",
                    issues.len(),
                    open_issues.len(),
                    blocked.len(),
                    stale.len(),
                    overdue.len()
                ),
            ];
            if !top_assignees.is_empty() {
                lines.push(format!(
                    "Capacity hotspots: {}",
                    top_assignees
                        .iter()
                        .take(5)
                        .map(|(name, count)| format!("{} ({})", name, count))
                        .collect::<Vec<_>>()
                        .join(", ")
                ));
            } else {
                lines.push("Capacity hotspots: none".to_string());
            }
            lines.push(format!(
                "Priority follow-ups: {}",
                render_digest_bucket(&[
                    ("blocked", &blocked),
                    ("stale", &stale),
                    ("overdue", &overdue),
                    ("due-soon", &due_soon),
                ])
            ));

            LinearPmDigestTaskResult {
                ok: true,
                body: Some(lines.join("\n")),
                error: None,
            }
        }
        Err(error) => LinearPmDigestTaskResult {
            ok: false,
            body: None,
            error: Some(error.to_string()),
        },
    }
}

fn get_linear_read_key(config: &NanoclawConfig) -> String {
    if !config.linear_api_key.trim().is_empty() {
        config.linear_api_key.trim().to_string()
    } else {
        config.linear_write_api_key.trim().to_string()
    }
}

fn get_linear_write_key(config: &NanoclawConfig) -> String {
    if !config.linear_write_api_key.trim().is_empty() {
        config.linear_write_api_key.trim().to_string()
    } else {
        config.linear_api_key.trim().to_string()
    }
}

fn linear_client() -> Result<Client> {
    Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .context("failed to build Linear HTTP client")
}

fn linear_graphql<T: DeserializeOwned>(
    token: &str,
    query: &str,
    variables: Option<Value>,
) -> Result<T> {
    let request_body = if let Some(variables) = variables {
        json!({ "query": query, "variables": variables })
    } else {
        json!({ "query": query })
    };
    let (status, payload) = send_linear_graphql_via_reqwest(token, &request_body)?;
    if should_retry_linear_graphql_with_curl(&payload) {
        eprintln!(
            "linear graphql retrying via curl after reqwest returned API-key bearer rejection"
        );
        let payload = send_linear_graphql_via_curl(token, &request_body)?;
        return finalize_linear_graphql_response(None, payload);
    }
    finalize_linear_graphql_response(Some(status.to_string()), payload)
}

fn send_linear_graphql_via_reqwest<T: DeserializeOwned>(
    token: &str,
    request_body: &Value,
) -> Result<(reqwest::StatusCode, LinearGraphqlEnvelope<T>)> {
    let client = linear_client()?;
    let response = client
        .post(LINEAR_GRAPHQL_URL)
        .header(AUTHORIZATION, linear_authorization_value(token))
        .json(request_body)
        .send()
        .context("failed to send Linear GraphQL request")?;
    let status = response.status();
    let payload = response
        .json::<LinearGraphqlEnvelope<T>>()
        .context("failed to decode Linear GraphQL response")?;
    Ok((status, payload))
}

fn send_linear_graphql_via_curl<T: DeserializeOwned>(
    token: &str,
    request_body: &Value,
) -> Result<LinearGraphqlEnvelope<T>> {
    let body = serde_json::to_string(request_body)
        .context("failed to encode Linear GraphQL request payload")?;
    let output = Command::new("curl")
        .args([
            "-sS",
            LINEAR_GRAPHQL_URL,
            "-H",
            "Content-Type: application/json",
            "-H",
            &format!("Authorization: {}", linear_authorization_value(token)),
            "--data",
            &body,
        ])
        .output()
        .context("failed to execute curl for Linear GraphQL request")?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        if stderr.is_empty() {
            anyhow::bail!(
                "curl failed for Linear GraphQL request with status {}",
                output.status
            );
        }
        anyhow::bail!("curl failed for Linear GraphQL request: {stderr}");
    }
    serde_json::from_slice::<LinearGraphqlEnvelope<T>>(&output.stdout)
        .context("failed to decode Linear GraphQL response")
}

fn finalize_linear_graphql_response<T>(
    status: Option<String>,
    payload: LinearGraphqlEnvelope<T>,
) -> Result<T> {
    if payload
        .errors
        .as_ref()
        .map(|errors| !errors.is_empty())
        .unwrap_or(false)
    {
        let message = first_linear_graphql_error_message(&payload).unwrap_or_else(|| {
            status
                .map(|value| format!("Linear GraphQL request failed with status {}.", value))
                .unwrap_or_else(|| "Linear GraphQL request failed.".to_string())
        });
        return Err(anyhow!(message));
    }
    payload
        .data
        .ok_or_else(|| anyhow!("Linear GraphQL returned no data."))
}

fn should_retry_linear_graphql_with_curl<T>(payload: &LinearGraphqlEnvelope<T>) -> bool {
    first_linear_graphql_error_message(payload)
        .map(|message| {
            message
                .to_ascii_lowercase()
                .contains(LINEAR_API_KEY_AS_BEARER_ERROR)
        })
        .unwrap_or(false)
}

fn first_linear_graphql_error_message<T>(payload: &LinearGraphqlEnvelope<T>) -> Option<String> {
    payload
        .errors
        .as_ref()
        .and_then(|errors| errors.first())
        .and_then(|error| {
            error
                .extensions
                .as_ref()
                .and_then(|extensions| extensions.user_presentable_message.clone())
                .or_else(|| error.message.clone())
        })
}

fn get_teams(token: &str, limit: usize) -> Result<Vec<LinearTeamSummary>> {
    let first = limit.clamp(1, 100);
    let query = format!(
        "query LinearTeams($first: Int!) {{
          teams(first: $first) {{
            nodes {{
              {}
            }}
          }}
        }}",
        TEAM_FIELDS
    );
    let payload: LinearTeamsWrapper =
        linear_graphql(token, &query, Some(json!({ "first": first })))?;
    Ok(payload
        .teams
        .and_then(|teams| teams.nodes)
        .unwrap_or_default())
}

fn lookup_issue(identifier: &str, token: &str) -> Result<Option<LinearIssueSummary>> {
    let query = format!(
        "query LinearIssueLookup($identifier: String!) {{
          issue(id: $identifier) {{
            {}
          }}
        }}",
        ISSUE_FIELDS
    );
    let payload: LinearIssueLookupWrapper =
        linear_graphql(token, &query, Some(json!({ "identifier": identifier })))?;
    Ok(payload.issue)
}

fn lookup_detailed_issue(identifier: &str, token: &str) -> Result<Option<LinearIssueSummary>> {
    let query = format!(
        "query LinearDetailedIssueLookup($identifier: String!) {{
          issue(id: $identifier) {{
            {}
          }}
        }}",
        DETAILED_ISSUE_FIELDS
    );
    let payload: LinearDetailedIssueWrapper =
        linear_graphql(token, &query, Some(json!({ "identifier": identifier })))?;
    Ok(payload.issue)
}

fn find_existing_issue_comment(
    issue_id: &str,
    body: &str,
    token: &str,
) -> Result<Option<LinearIssueCommentSummary>> {
    let query = r#"
      query LinearIssueComments($issueId: String!) {
        issue(id: $issueId) {
          comments(first: 100) {
            nodes {
              id
              body
            }
          }
        }
      }
    "#;
    let payload: LinearCommentLookupWrapper =
        linear_graphql(token, query, Some(json!({ "issueId": issue_id })))?;
    let normalized = normalize_comment_body(body);
    Ok(payload
        .issue
        .and_then(|issue| issue.comments)
        .and_then(|comments| comments.nodes)
        .unwrap_or_default()
        .into_iter()
        .find(|comment| normalize_comment_body(&comment.body) == normalized))
}

fn update_existing_issue_comment(
    comment_id: &str,
    body: &str,
    token: &str,
) -> Result<Option<LinearIssueCommentSummary>> {
    let query = r#"
      mutation LinearCommentUpdate($id: String!, $input: CommentUpdateInput!) {
        commentUpdate(id: $id, input: $input) {
          success
          comment {
            id
            body
          }
        }
      }
    "#;
    let payload: LinearCommentUpdateWrapper = linear_graphql(
        token,
        query,
        Some(json!({
            "id": comment_id,
            "input": { "body": body }
        })),
    )?;
    Ok(payload
        .comment_update
        .success
        .then_some(payload.comment_update.comment)
        .flatten())
}

fn create_issue_comment(
    issue_id: &str,
    body: &str,
    token: &str,
) -> Result<LinearIssueCommentSummary> {
    let query = r#"
      mutation LinearCommentCreate($issueId: String!, $body: String!) {
        commentCreate(input: { issueId: $issueId, body: $body }) {
          success
          comment {
            id
            body
          }
        }
      }
    "#;
    let payload: LinearCommentCreateWrapper = linear_graphql(
        token,
        query,
        Some(json!({
            "issueId": issue_id,
            "body": body
        })),
    )?;
    if !payload.comment_create.success {
        anyhow::bail!("Linear comment creation did not succeed.");
    }
    payload
        .comment_create
        .comment
        .ok_or_else(|| anyhow!("Linear comment creation returned no comment."))
}

fn get_team_with_states_by_id(team_id: &str, token: &str) -> Result<Option<LinearTeamStates>> {
    let query = format!(
        "query LinearTeamStatesById($teamId: String!) {{
          team(id: $teamId) {{
            {}
            states(first: 100) {{
              nodes {{
                {}
              }}
            }}
          }}
        }}",
        TEAM_FIELDS, WORKFLOW_STATE_FIELDS
    );
    let payload: LinearTeamByIdWrapper =
        linear_graphql(token, &query, Some(json!({ "teamId": team_id })))?;
    Ok(payload.team)
}

fn get_team_with_states_by_key(team_key: &str, token: &str) -> Result<Option<LinearTeamStates>> {
    let query = format!(
        "query LinearTeamStatesByKey($teamKey: String!) {{
          teams(first: 1, filter: {{ key: {{ eqIgnoreCase: $teamKey }} }}) {{
            nodes {{
              {}
              states(first: 100) {{
                nodes {{
                  {}
                }}
              }}
            }}
          }}
        }}",
        TEAM_FIELDS, WORKFLOW_STATE_FIELDS
    );
    let payload: LinearTeamsByKeyWrapper =
        linear_graphql(token, &query, Some(json!({ "teamKey": team_key })))?;
    Ok(payload
        .teams
        .and_then(|teams| teams.nodes)
        .and_then(|mut nodes| nodes.drain(..).next()))
}

fn resolve_workflow_state_reference(
    team_id: Option<&str>,
    team_key: Option<&str>,
    state_id: Option<&str>,
    state_name: Option<&str>,
    token: &str,
) -> Result<LinearWorkflowStateSummary> {
    let team = if let Some(team_id) = team_id.filter(|value| !value.trim().is_empty()) {
        get_team_with_states_by_id(team_id, token)?
    } else if let Some(team_key) = team_key.filter(|value| !value.trim().is_empty()) {
        get_team_with_states_by_key(team_key, token)?
    } else {
        None
    }
    .ok_or_else(|| anyhow!("Linear team was not found while resolving workflow states."))?;

    let mut states = team
        .states
        .and_then(|states| states.nodes)
        .unwrap_or_default();
    states.sort_by(|a, b| {
        a.position
            .unwrap_or(i64::MAX)
            .cmp(&b.position.unwrap_or(i64::MAX))
    });

    if let Some(state_id) = state_id.filter(|value| !value.trim().is_empty()) {
        return states
            .into_iter()
            .find(|state| state.id == state_id)
            .ok_or_else(|| anyhow!("State ID {} was not found for team {}.", state_id, team.key));
    }

    let state_name = state_name
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("A target Linear state is required."))?;
    states
        .into_iter()
        .find(|state| state.name.eq_ignore_ascii_case(state_name))
        .ok_or_else(|| {
            anyhow!(
                "State \"{}\" was not found for team {}.",
                state_name,
                team.key
            )
        })
}

fn transition_issue(issue_id: &str, state_id: &str, token: &str) -> Result<LinearIssueSummary> {
    let query = format!(
        "mutation LinearIssueTransition($id: String!, $input: IssueUpdateInput!) {{
          issueUpdate(id: $id, input: $input) {{
            success
            issue {{
              {}
            }}
          }}
        }}",
        ISSUE_FIELDS
    );
    let payload: LinearIssueUpdateWrapper = linear_graphql(
        token,
        &query,
        Some(json!({
            "id": issue_id,
            "input": { "stateId": state_id }
        })),
    )?;
    if !payload.issue_update.success {
        anyhow::bail!("Linear issue transition did not succeed.");
    }
    payload
        .issue_update
        .issue
        .ok_or_else(|| anyhow!("Linear issue transition returned no issue."))
}

fn get_detailed_issues(
    token: &str,
    team_key: Option<&str>,
    limit: usize,
) -> Result<Vec<LinearIssueSummary>> {
    let first = limit.clamp(1, 250);
    let filter = if let Some(team_key) = team_key.filter(|value| !value.trim().is_empty()) {
        json!({ "team": { "key": { "eqIgnoreCase": team_key } } })
    } else {
        Value::Null
    };
    let query = format!(
        "query LinearDetailedIssueSearch($first: Int!, $filter: IssueFilter) {{
          issues(first: $first, filter: $filter) {{
            nodes {{
              {}
            }}
          }}
        }}",
        DETAILED_ISSUE_FIELDS
    );
    #[derive(Debug, Deserialize)]
    struct IssuesWrapper {
        issues: Option<LinearNodeList<LinearIssueSummary>>,
    }
    let payload: IssuesWrapper = linear_graphql(
        token,
        &query,
        Some(json!({
            "first": first,
            "filter": if filter.is_null() { Value::Null } else { filter }
        })),
    )?;
    Ok(payload
        .issues
        .and_then(|issues| issues.nodes)
        .unwrap_or_default())
}

fn to_pm_memory_view(memory: &PmIssueMemory) -> LinearPmMemoryView {
    LinearPmMemoryView {
        summary: memory.summary.clone(),
        next_action: memory.next_action.clone(),
        blockers: memory.blockers.clone(),
        current_state: memory.current_state.clone(),
        repo_hint: memory.repo_hint.clone(),
        last_source: memory.last_source.clone(),
        details: memory.memory.clone(),
        updated_at: Some(memory.updated_at.clone()),
    }
}

fn normalize_comment_body(body: &str) -> String {
    body.trim().replace("\r\n", "\n")
}

fn linear_authorization_value(token: &str) -> String {
    let trimmed = token.trim();
    let mut parts = trimmed.splitn(2, char::is_whitespace);
    let scheme = parts.next().unwrap_or_default();
    let remainder = parts.next().unwrap_or_default().trim();
    if scheme.eq_ignore_ascii_case("bearer") && !remainder.is_empty() {
        remainder.to_string()
    } else {
        trimmed.to_string()
    }
}

fn hash_normalized_comment_body(body: &str) -> String {
    format!(
        "{:x}",
        Sha256::digest(normalize_comment_body(body).as_bytes())
    )
}

fn description_has_acceptance_criteria(description: Option<&str>) -> bool {
    let Some(description) = description else {
        return false;
    };
    let normalized = description.to_lowercase();
    normalized.contains("acceptance criteria")
        || normalized.contains("success criteria")
        || normalized.contains("done when")
        || normalized.contains("definition of done")
        || description.contains("- [ ]")
        || description.contains("- [x]")
}

fn description_has_implementation_path(description: Option<&str>) -> bool {
    let Some(description) = description else {
        return false;
    };
    let normalized = description.to_lowercase();
    normalized.contains("repo")
        || normalized.contains("repository")
        || normalized.contains("github.com")
        || normalized.contains("/workspace/")
        || normalized.contains("branch")
        || normalized.contains("pr ")
        || normalized.contains("pull request")
        || normalized.contains("symphony-rs")
        || normalized.contains("nanoclaw")
}

fn issue_is_open(issue: &LinearIssueSummary) -> bool {
    let state_type = issue
        .state
        .as_ref()
        .and_then(|state| state.kind.as_ref())
        .map(|kind| kind.to_ascii_lowercase());
    !matches!(state_type.as_deref(), Some("completed" | "canceled"))
}

fn issue_has_label(issue: &LinearIssueSummary, label_name: &str) -> bool {
    issue
        .labels
        .iter()
        .any(|label| label.name.eq_ignore_ascii_case(label_name))
}

fn issue_is_blocked(issue: &LinearIssueSummary) -> bool {
    issue
        .state
        .as_ref()
        .map(|state| state.name.eq_ignore_ascii_case("Needs Attention"))
        .unwrap_or(false)
        || issue_has_label(issue, "blocked")
}

fn build_issue_quality_gaps(issue: &LinearIssueSummary) -> Vec<LinearIssueQualityGap> {
    let description = issue.description.as_deref().unwrap_or("").trim();
    let mut gaps = Vec::new();

    if issue
        .assignee
        .as_ref()
        .map(|assignee| assignee.id.trim())
        .unwrap_or("")
        .is_empty()
    {
        gaps.push(LinearIssueQualityGap {
            key: "missing-assignee".to_string(),
            severity: LinearIssueQualitySeverity::Critical,
            description: "No assignee is set yet.".to_string(),
        });
    }

    if description.len() < 40 {
        gaps.push(LinearIssueQualityGap {
            key: "thin-description".to_string(),
            severity: LinearIssueQualitySeverity::Critical,
            description: "The issue description is too thin to act on confidently.".to_string(),
        });
    }

    if !description_has_acceptance_criteria(issue.description.as_deref()) {
        gaps.push(LinearIssueQualityGap {
            key: "missing-acceptance-criteria".to_string(),
            severity: LinearIssueQualitySeverity::Warning,
            description: "Acceptance criteria or a clear \"done when\" section is missing."
                .to_string(),
        });
    }

    if !description_has_implementation_path(issue.description.as_deref()) {
        gaps.push(LinearIssueQualityGap {
            key: "missing-implementation-path".to_string(),
            severity: LinearIssueQualitySeverity::Warning,
            description: "The repo, code path, or implementation surface is not explicit."
                .to_string(),
        });
    }

    let state_type = issue
        .state
        .as_ref()
        .and_then(|state| state.kind.as_ref())
        .map(|kind| kind.to_ascii_lowercase())
        .unwrap_or_default();
    if issue_is_open(issue)
        && issue.due_date.is_none()
        && state_type != "backlog"
        && state_type != "unstarted"
    {
        gaps.push(LinearIssueQualityGap {
            key: "missing-due-date".to_string(),
            severity: LinearIssueQualitySeverity::Info,
            description: "No due date is set for active work.".to_string(),
        });
    }

    if issue.project.is_none() {
        gaps.push(LinearIssueQualityGap {
            key: "missing-project".to_string(),
            severity: LinearIssueQualitySeverity::Info,
            description: "The issue is not attached to a project.".to_string(),
        });
    }

    gaps
}

fn compute_issue_quality_score(gaps: &[LinearIssueQualityGap]) -> i64 {
    let penalty = gaps.iter().fold(0, |total, gap| {
        total
            + match gap.severity {
                LinearIssueQualitySeverity::Critical => 30,
                LinearIssueQualitySeverity::Warning => 15,
                LinearIssueQualitySeverity::Info => 5,
            }
    });
    (100 - penalty).max(0)
}

fn render_digest_bucket(buckets: &[(&str, &Vec<String>)]) -> String {
    let parts = buckets
        .iter()
        .filter(|(_, values)| !values.is_empty())
        .map(|(label, values)| {
            format!(
                "{}: {}",
                label,
                values
                    .iter()
                    .take(4)
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        })
        .collect::<Vec<_>>();
    if parts.is_empty() {
        "none".to_string()
    } else {
        parts.join("; ")
    }
}

fn parse_rfc3339(value: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    chrono::DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|value| value.with_timezone(&chrono::Utc))
}

fn build_issue_quality_body(issue: &LinearIssueSummary, gaps: &[LinearIssueQualityGap]) -> String {
    let mut lines = vec![
        format!("PM quality check for {}:", issue.identifier),
        String::new(),
        "Gaps to address:".to_string(),
    ];
    for gap in gaps {
        lines.push(format!("- {}: {}", gap.severity.label(), gap.description));
    }
    lines.push(String::new());
    lines.push(
        "Please update the issue so the next execution cycle has clear ownership, scope, and success criteria."
            .to_string(),
    );
    lines.join("\n")
}

fn summarize_pm_memory(issue: &LinearIssueSummary) -> String {
    let state = issue
        .state
        .as_ref()
        .map(|state| state.name.as_str())
        .unwrap_or("Unknown state");
    let assignee = issue
        .assignee
        .as_ref()
        .and_then(|assignee| assignee.display_name.as_deref())
        .or_else(|| {
            issue
                .assignee
                .as_ref()
                .map(|assignee| assignee.name.as_str())
        })
        .or_else(|| {
            issue
                .assignee
                .as_ref()
                .and_then(|assignee| assignee.email.as_deref())
        })
        .unwrap_or("Unassigned");
    format!(
        "{}: {} ({}, owner: {})",
        issue.identifier, issue.title, state, assignee
    )
}

fn deserialize_linear_label_nodes<'de, D>(
    deserializer: D,
) -> std::result::Result<Vec<LinearLabelSummary>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Labels {
        Nodes {
            nodes: Option<Vec<LinearLabelSummary>>,
        },
        Direct(Vec<LinearLabelSummary>),
        Null,
    }

    Ok(match Option::<Labels>::deserialize(deserializer)? {
        Some(Labels::Nodes { nodes }) => nodes.unwrap_or_default(),
        Some(Labels::Direct(values)) => values,
        Some(Labels::Null) | None => Vec::new(),
    })
}

#[cfg(test)]
mod tests {
    use super::{
        build_issue_quality_body, build_issue_quality_gaps, compute_issue_quality_score,
        description_has_acceptance_criteria, description_has_implementation_path,
        hash_normalized_comment_body, linear_authorization_value,
        should_retry_linear_graphql_with_curl, LinearGraphqlEnvelope, LinearIssueQualitySeverity,
        LinearIssueSummary, LinearProjectSummary, LinearTeamSummary, LinearUserSummary,
        LinearWorkflowStateSummary,
    };
    use serde_json::{json, Value};

    fn sample_issue() -> LinearIssueSummary {
        LinearIssueSummary {
            id: "issue-1".to_string(),
            identifier: "BYB-23".to_string(),
            title: "Tighten webhook filtering".to_string(),
            url: None,
            description: Some(
                "Acceptance criteria:\n- [ ] Reject invalid signatures\nRepo: nanoclaw\n"
                    .to_string(),
            ),
            priority: None,
            due_date: Some("2026-04-10".to_string()),
            created_at: None,
            updated_at: None,
            team: Some(LinearTeamSummary {
                id: "team-1".to_string(),
                key: "BYB".to_string(),
                name: "ByBuddha".to_string(),
            }),
            project: Some(LinearProjectSummary {
                id: "project-1".to_string(),
                name: "NanoClaw".to_string(),
                slug_id: None,
                url: None,
                description: None,
                content: None,
            }),
            state: Some(LinearWorkflowStateSummary {
                id: "state-1".to_string(),
                name: "In Progress".to_string(),
                kind: Some("started".to_string()),
                position: Some(1),
                description: None,
                team: None,
            }),
            assignee: Some(LinearUserSummary {
                id: "user-1".to_string(),
                name: "Buddha".to_string(),
                display_name: Some("Buddha".to_string()),
                email: None,
                active: Some(true),
            }),
            labels: Vec::new(),
        }
    }

    #[test]
    fn detects_acceptance_criteria_and_impl_path() {
        assert!(description_has_acceptance_criteria(Some(
            "Acceptance criteria:\n- [ ] done"
        )));
        assert!(description_has_implementation_path(Some("Repo: nanoclaw")));
        assert!(!description_has_acceptance_criteria(Some("just a note")));
        assert!(!description_has_implementation_path(Some("just a note")));
    }

    #[test]
    fn quality_score_penalizes_missing_structure() {
        let mut issue = sample_issue();
        issue.assignee = None;
        issue.project = None;
        issue.description = Some("short".to_string());
        issue.due_date = None;
        let gaps = build_issue_quality_gaps(&issue);
        assert!(gaps.iter().any(|gap| gap.key == "missing-assignee"));
        assert!(gaps.iter().any(|gap| gap.key == "thin-description"));
        assert!(compute_issue_quality_score(&gaps) < 100);
        let body = build_issue_quality_body(&issue, &gaps);
        assert!(body.contains("PM quality check for BYB-23"));
    }

    #[test]
    fn quality_gap_severity_labels_are_stable() {
        assert_eq!(LinearIssueQualitySeverity::Critical.as_str(), "critical");
        assert_eq!(LinearIssueQualitySeverity::Warning.as_str(), "warning");
        assert_eq!(LinearIssueQualitySeverity::Info.as_str(), "info");
    }

    #[test]
    fn comment_hash_normalizes_line_endings() {
        assert_eq!(
            hash_normalized_comment_body("hello\r\nworld"),
            hash_normalized_comment_body("hello\nworld")
        );
    }

    #[test]
    fn linear_authorization_header_uses_raw_api_key() {
        assert_eq!(linear_authorization_value("lin_api_123"), "lin_api_123");
        assert_eq!(
            linear_authorization_value("Bearer lin_api_123"),
            "lin_api_123"
        );
        assert_eq!(
            linear_authorization_value("bearer   lin_api_123"),
            "lin_api_123"
        );
    }

    #[test]
    fn bearer_rejection_triggers_curl_retry() {
        let payload = serde_json::from_value::<LinearGraphqlEnvelope<Value>>(json!({
            "errors": [
                {
                    "message": "bad request",
                    "extensions": {
                        "userPresentableMessage": "It looks like you're trying to use an API key as a Bearer token. Remove the Bearer prefix from the Authorization header."
                    }
                }
            ]
        }))
        .expect("valid LinearGraphqlEnvelope");

        assert!(should_retry_linear_graphql_with_curl(&payload));
    }

    #[test]
    fn non_bearer_rejection_does_not_trigger_curl_retry() {
        let payload = serde_json::from_value::<LinearGraphqlEnvelope<Value>>(json!({
            "errors": [
                {
                    "message": "unauthorized",
                    "extensions": {
                        "userPresentableMessage": "Invalid API key."
                    }
                }
            ]
        }))
        .expect("valid LinearGraphqlEnvelope");

        assert!(!should_retry_linear_graphql_with_curl(&payload));
    }
}
