use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::json;

use super::config::NanoclawConfig;
use super::db::NanoclawDb;
use super::linear::{
    run_linear_issue_comment_upsert_task, run_linear_issue_transition_task,
    run_linear_pm_memory_task, LinearIssueCommentUpsertTaskInput, LinearIssueTransitionTaskInput,
    LinearPmMemoryTaskInput,
};
use super::pm::PmAuditEvent;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GithubRepositoryPayload {
    pub full_name: Option<String>,
    pub html_url: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct GithubPullRequestRefPayload {
    pub number: Option<i64>,
    pub html_url: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GithubPullRequestHeadPayload {
    pub r#ref: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GithubPullRequestPayload {
    pub number: Option<i64>,
    pub title: Option<String>,
    pub body: Option<String>,
    pub html_url: Option<String>,
    pub merged: Option<bool>,
    pub draft: Option<bool>,
    pub head: Option<GithubPullRequestHeadPayload>,
    pub base: Option<GithubPullRequestHeadPayload>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GithubReviewUserPayload {
    pub login: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GithubReviewPayload {
    pub state: Option<String>,
    pub html_url: Option<String>,
    pub body: Option<String>,
    pub user: Option<GithubReviewUserPayload>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GithubCheckSuitePayload {
    pub pull_requests: Option<Vec<GithubPullRequestRefPayload>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GithubCheckPayload {
    pub name: Option<String>,
    pub conclusion: Option<String>,
    pub html_url: Option<String>,
    pub details_url: Option<String>,
    pub pull_requests: Option<Vec<GithubPullRequestRefPayload>>,
    pub check_suite: Option<GithubCheckSuitePayload>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GithubWebhookPayload {
    pub action: Option<String>,
    pub repository: Option<GithubRepositoryPayload>,
    pub pull_request: Option<GithubPullRequestPayload>,
    pub review: Option<GithubReviewPayload>,
    pub check_run: Option<GithubCheckPayload>,
    pub check_suite: Option<GithubCheckPayload>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GithubWebhookNotification {
    pub identifier: String,
    pub target_chat_jid: Option<String>,
    pub body: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GithubWebhookResult {
    pub ok: bool,
    pub ignored: bool,
    pub reason: Option<String>,
    pub handled_identifiers: Vec<String>,
    pub notifications: Vec<GithubWebhookNotification>,
    pub errors: Vec<String>,
}

#[derive(Debug, Clone)]
struct GithubSyncDirective {
    identifier: String,
    target_state_name: Option<String>,
    comment_kind: String,
    comment_body: String,
    slack_body: String,
    memory: GithubMemoryDirective,
}

#[derive(Debug, Clone)]
struct GithubMemoryDirective {
    summary: String,
    next_action: String,
    blockers: Option<Vec<String>>,
    current_state: Option<String>,
    repo_hint: Option<String>,
    last_source: String,
    details: serde_json::Value,
}

pub fn handle_github_webhook(
    db: &NanoclawDb,
    config: &NanoclawConfig,
    payload: &GithubWebhookPayload,
    event_type: &str,
) -> GithubWebhookResult {
    let directives = build_github_sync_directives(event_type, payload);
    if directives.is_empty() {
        return GithubWebhookResult {
            ok: true,
            ignored: true,
            reason: Some("no-linear-identifiers".to_string()),
            handled_identifiers: Vec::new(),
            notifications: Vec::new(),
            errors: Vec::new(),
        };
    }

    let mut handled_identifiers = Vec::new();
    let mut notifications = Vec::new();
    let mut errors = Vec::new();

    for directive in directives {
        let _ = db.record_pm_audit_event(&PmAuditEvent {
            issue_key: None,
            issue_identifier: Some(directive.identifier.clone()),
            thread_jid: None,
            phase: "github_sync_started".to_string(),
            status: "started".to_string(),
            tool: Some("github-webhook".to_string()),
            error_code: None,
            blocking: false,
            metadata: Some(json!({
                "eventType": event_type,
                "action": payload.action,
                "targetStateName": directive.target_state_name,
            })),
            created_at: None,
        });

        let outcome = (|| -> Result<(), String> {
            if let Some(target_state_name) = directive.target_state_name.as_ref() {
                let transition = run_linear_issue_transition_task(
                    config,
                    LinearIssueTransitionTaskInput {
                        identifier: directive.identifier.clone(),
                        state_name: Some(target_state_name.clone()),
                        state_id: None,
                    },
                );
                if !transition.ok {
                    return Err(transition.error.unwrap_or_else(|| {
                        format!(
                            "failed to transition {} to {}",
                            directive.identifier, target_state_name
                        )
                    }));
                }
            }

            let comment = run_linear_issue_comment_upsert_task(
                db,
                config,
                LinearIssueCommentUpsertTaskInput {
                    identifier: directive.identifier.clone(),
                    body: directive.comment_body.clone(),
                    comment_kind: directive.comment_kind.clone(),
                },
            );
            if !comment.ok {
                return Err(comment.error.unwrap_or_else(|| {
                    format!("failed to upsert comment for {}", directive.identifier)
                }));
            }

            let memory = run_linear_pm_memory_task(
                db,
                config,
                LinearPmMemoryTaskInput {
                    identifier: directive.identifier.clone(),
                    summary: Some(directive.memory.summary.clone()),
                    next_action: Some(directive.memory.next_action.clone()),
                    blockers: directive.memory.blockers.clone(),
                    current_state: directive.memory.current_state.clone(),
                    repo_hint: directive.memory.repo_hint.clone(),
                    last_source: Some(directive.memory.last_source.clone()),
                    details: Some(directive.memory.details.clone()),
                    merge: true,
                },
            );
            if !memory.ok {
                return Err(memory.error.unwrap_or_else(|| {
                    format!("failed to update PM memory for {}", directive.identifier)
                }));
            }

            Ok(())
        })();

        match outcome {
            Ok(()) => {
                let target_chat_jid = db
                    .get_linear_issue_thread_by_identifier(&directive.identifier)
                    .ok()
                    .flatten()
                    .map(|thread| thread.chat_jid)
                    .or_else(|| {
                        (!config.linear_chat_jid.trim().is_empty())
                            .then_some(config.linear_chat_jid.clone())
                    });
                notifications.push(GithubWebhookNotification {
                    identifier: directive.identifier.clone(),
                    target_chat_jid,
                    body: directive.slack_body.clone(),
                });
                handled_identifiers.push(directive.identifier.clone());
                let _ = db.record_pm_audit_event(&PmAuditEvent {
                    issue_key: None,
                    issue_identifier: Some(directive.identifier.clone()),
                    thread_jid: notifications
                        .last()
                        .and_then(|notification| notification.target_chat_jid.clone()),
                    phase: "github_sync_succeeded".to_string(),
                    status: "succeeded".to_string(),
                    tool: Some("github-webhook".to_string()),
                    error_code: None,
                    blocking: false,
                    metadata: Some(json!({
                        "eventType": event_type,
                        "action": payload.action,
                        "targetStateName": directive.target_state_name,
                    })),
                    created_at: None,
                });
            }
            Err(error) => {
                errors.push(format!("{}: {}", directive.identifier, error));
                let _ = db.record_pm_audit_event(&PmAuditEvent {
                    issue_key: None,
                    issue_identifier: Some(directive.identifier.clone()),
                    thread_jid: None,
                    phase: "github_sync_failed".to_string(),
                    status: "failed".to_string(),
                    tool: Some("github-webhook".to_string()),
                    error_code: Some("github-sync-failed".to_string()),
                    blocking: false,
                    metadata: Some(json!({
                        "eventType": event_type,
                        "action": payload.action,
                        "message": error,
                    })),
                    created_at: None,
                });
            }
        }
    }

    GithubWebhookResult {
        ok: true,
        ignored: false,
        reason: None,
        handled_identifiers,
        notifications,
        errors,
    }
}

fn extract_linear_identifiers(payload: &GithubWebhookPayload) -> Vec<String> {
    let mut fields = String::new();
    for value in [
        payload
            .pull_request
            .as_ref()
            .and_then(|pr| pr.title.as_deref())
            .unwrap_or_default(),
        payload
            .pull_request
            .as_ref()
            .and_then(|pr| pr.body.as_deref())
            .unwrap_or_default(),
        payload
            .pull_request
            .as_ref()
            .and_then(|pr| pr.head.as_ref())
            .and_then(|head| head.r#ref.as_deref())
            .unwrap_or_default(),
        payload
            .pull_request
            .as_ref()
            .and_then(|pr| pr.base.as_ref())
            .and_then(|base| base.r#ref.as_deref())
            .unwrap_or_default(),
        payload
            .review
            .as_ref()
            .and_then(|review| review.body.as_deref())
            .unwrap_or_default(),
    ] {
        fields.push_str(value);
        fields.push('\n');
    }
    if let Some(checks) = payload
        .check_run
        .as_ref()
        .and_then(|check| check.pull_requests.as_ref())
    {
        for check in checks {
            if let Some(number) = check.number {
                fields.push_str(&number.to_string());
                fields.push('\n');
            }
            if let Some(url) = check.html_url.as_deref() {
                fields.push_str(url);
                fields.push('\n');
            }
        }
    }
    if let Some(checks) = payload
        .check_suite
        .as_ref()
        .and_then(|check| check.pull_requests.as_ref())
    {
        for check in checks {
            if let Some(number) = check.number {
                fields.push_str(&number.to_string());
                fields.push('\n');
            }
            if let Some(url) = check.html_url.as_deref() {
                fields.push_str(url);
                fields.push('\n');
            }
        }
    }

    let regex = Regex::new(r"\b[A-Z][A-Z0-9]+-\d+\b").expect("identifier regex should compile");
    let mut identifiers = Vec::new();
    for capture in regex.find_iter(&fields) {
        let identifier = capture.as_str().to_ascii_uppercase();
        if !identifiers.contains(&identifier) {
            identifiers.push(identifier);
        }
    }
    identifiers
}

fn build_pull_request_slack_header(
    action: &str,
    identifier: &str,
    payload: &GithubWebhookPayload,
) -> String {
    let pr = payload.pull_request.as_ref();
    let repo = payload
        .repository
        .as_ref()
        .and_then(|repo| repo.full_name.as_deref())
        .unwrap_or("unknown repo");
    let pr_label = if let Some(pr) = pr {
        match (pr.number, pr.title.as_deref()) {
            (Some(number), Some(title)) => format!("#{number} {title}"),
            (_, Some(title)) => title.to_string(),
            _ => "PR update".to_string(),
        }
    } else {
        "PR update".to_string()
    };
    let url = pr
        .and_then(|pr| pr.html_url.as_deref())
        .or_else(|| {
            payload
                .review
                .as_ref()
                .and_then(|review| review.html_url.as_deref())
        })
        .or_else(|| {
            payload
                .check_run
                .as_ref()
                .and_then(|check| check.html_url.as_deref())
        })
        .or_else(|| {
            payload
                .check_suite
                .as_ref()
                .and_then(|check| check.html_url.as_deref())
        });
    let mut lines = vec![
        format!("*GitHub Webhook - {}*", action.to_ascii_uppercase()),
        format!("*Issue:* {}", identifier),
        format!("*Repo:* {}", repo),
        format!("*PR:* {}", pr_label),
    ];
    if let Some(url) = url {
        lines.push(format!("*URL:* {}", url));
    }
    lines.join("\n")
}

fn build_github_sync_directives(
    event_type: &str,
    payload: &GithubWebhookPayload,
) -> Vec<GithubSyncDirective> {
    let identifiers = extract_linear_identifiers(payload);
    let repo = payload
        .repository
        .as_ref()
        .and_then(|repository| repository.full_name.clone())
        .unwrap_or_else(|| "unknown repo".to_string());
    let pr = payload.pull_request.as_ref();
    let action = payload
        .action
        .as_deref()
        .map(|value| value.trim().to_ascii_lowercase())
        .unwrap_or_else(|| "unknown".to_string());
    let mut directives = Vec::new();

    for identifier in identifiers {
        if event_type == "pull_request" {
            let pr_url = pr.and_then(|pr| pr.html_url.clone()).unwrap_or_default();
            let pr_label = match pr {
                Some(pr) => match (pr.number, pr.title.as_deref()) {
                    (Some(number), Some(title)) => format!("#{number} {title}"),
                    (_, Some(title)) => title.to_string(),
                    _ => "PR".to_string(),
                },
                None => "PR".to_string(),
            };

            if action == "closed" && pr.and_then(|pr| pr.merged).unwrap_or(false) {
                directives.push(GithubSyncDirective {
                    identifier: identifier.clone(),
                    target_state_name: Some("Done".to_string()),
                    comment_kind: "github-pr-sync".to_string(),
                    comment_body: format!(
                        "GitHub sync: {} was merged in {}.{}",
                        pr_label,
                        repo,
                        if pr_url.is_empty() {
                            String::new()
                        } else {
                            format!(" {}", pr_url)
                        }
                    ),
                    slack_body: format!(
                        "{}\n*State Sync:* moved to Done after merge.",
                        build_pull_request_slack_header(&action, &identifier, payload)
                    ),
                    memory: GithubMemoryDirective {
                        summary: format!("{} merged through {}.", identifier, pr_label),
                        next_action: "Verify production or deployment follow-through after merge."
                            .to_string(),
                        blockers: None,
                        current_state: Some("Done".to_string()),
                        repo_hint: Some(repo.clone()),
                        last_source: "github-webhook".to_string(),
                        details: json!({
                            "repo": repo,
                            "prNumber": pr.and_then(|pr| pr.number),
                            "prUrl": pr_url,
                            "action": action,
                            "merged": true,
                        }),
                    },
                });
                continue;
            }

            if action == "closed" && !pr.and_then(|pr| pr.merged).unwrap_or(false) {
                directives.push(GithubSyncDirective {
                    identifier: identifier.clone(),
                    target_state_name: Some("In Progress".to_string()),
                    comment_kind: "github-pr-sync".to_string(),
                    comment_body: format!(
                        "GitHub sync: {} closed without merge in {}.{}",
                        pr_label,
                        repo,
                        if pr_url.is_empty() {
                            String::new()
                        } else {
                            format!(" {}", pr_url)
                        }
                    ),
                    slack_body: format!(
                        "{}\n*State Sync:* returned to In Progress because the PR closed without merge.",
                        build_pull_request_slack_header(&action, &identifier, payload)
                    ),
                    memory: GithubMemoryDirective {
                        summary: format!(
                            "{} lost its active PR because {} closed without merge.",
                            identifier, pr_label
                        ),
                        next_action:
                            "Decide whether to reopen work, replace the PR, or close the issue another way."
                                .to_string(),
                        blockers: Some(vec!["PR closed without merge".to_string()]),
                        current_state: Some("In Progress".to_string()),
                        repo_hint: Some(repo.clone()),
                        last_source: "github-webhook".to_string(),
                        details: json!({
                            "repo": repo,
                            "prNumber": pr.and_then(|pr| pr.number),
                            "prUrl": pr_url,
                            "action": action,
                            "merged": false,
                        }),
                    },
                });
                continue;
            }

            if action == "converted_to_draft" {
                directives.push(GithubSyncDirective {
                    identifier: identifier.clone(),
                    target_state_name: Some("In Progress".to_string()),
                    comment_kind: "github-pr-sync".to_string(),
                    comment_body: format!(
                        "GitHub sync: {} moved back to draft in {}.{}",
                        pr_label,
                        repo,
                        if pr_url.is_empty() {
                            String::new()
                        } else {
                            format!(" {}", pr_url)
                        }
                    ),
                    slack_body: format!(
                        "{}\n*State Sync:* moved back to In Progress because the PR is draft again.",
                        build_pull_request_slack_header(&action, &identifier, payload)
                    ),
                    memory: GithubMemoryDirective {
                        summary: format!("{} has a draft PR again: {}.", identifier, pr_label),
                        next_action:
                            "Address remaining implementation gaps before review.".to_string(),
                        blockers: None,
                        current_state: Some("In Progress".to_string()),
                        repo_hint: Some(repo.clone()),
                        last_source: "github-webhook".to_string(),
                        details: json!({
                            "repo": repo,
                            "prNumber": pr.and_then(|pr| pr.number),
                            "prUrl": pr_url,
                            "action": action,
                            "draft": true,
                        }),
                    },
                });
                continue;
            }

            let ready_for_review = matches!(
                action.as_str(),
                "opened" | "reopened" | "ready_for_review" | "synchronize"
            );
            if ready_for_review {
                let target_state_name = if pr.and_then(|pr| pr.draft).unwrap_or(false) {
                    "In Progress"
                } else {
                    "In Review"
                };
                directives.push(GithubSyncDirective {
                    identifier: identifier.clone(),
                    target_state_name: Some(target_state_name.to_string()),
                    comment_kind: "github-pr-sync".to_string(),
                    comment_body: format!(
                        "GitHub sync: {} is {} in {}.{}",
                        pr_label,
                        if pr.and_then(|pr| pr.draft).unwrap_or(false) {
                            "currently draft"
                        } else {
                            "active for review"
                        },
                        repo,
                        if pr_url.is_empty() {
                            String::new()
                        } else {
                            format!(" {}", pr_url)
                        }
                    ),
                    slack_body: format!(
                        "{}\n*State Sync:* moved to {}.",
                        build_pull_request_slack_header(&action, &identifier, payload),
                        target_state_name
                    ),
                    memory: GithubMemoryDirective {
                        summary: format!("{} is linked to {} in {}.", identifier, pr_label, repo),
                        next_action: if pr.and_then(|pr| pr.draft).unwrap_or(false) {
                            "Finish implementation before asking for review.".to_string()
                        } else {
                            "Watch review feedback and CI state.".to_string()
                        },
                        blockers: None,
                        current_state: Some(target_state_name.to_string()),
                        repo_hint: Some(repo.clone()),
                        last_source: "github-webhook".to_string(),
                        details: json!({
                            "repo": repo,
                            "prNumber": pr.and_then(|pr| pr.number),
                            "prUrl": pr_url,
                            "action": action,
                            "draft": pr.and_then(|pr| pr.draft).unwrap_or(false),
                        }),
                    },
                });
                continue;
            }
        }

        if event_type == "pull_request_review" {
            if let Some(review_state) = payload
                .review
                .as_ref()
                .and_then(|review| review.state.clone())
            {
                let review_state_upper = review_state.to_ascii_uppercase();
                let reviewer = payload
                    .review
                    .as_ref()
                    .and_then(|review| review.user.as_ref())
                    .and_then(|user| user.login.clone())
                    .unwrap_or_else(|| "reviewer".to_string());
                let pr_label = match pr {
                    Some(pr) => match (pr.number, pr.title.as_deref()) {
                        (Some(number), Some(title)) => format!("#{number} {title}"),
                        (_, Some(title)) => title.to_string(),
                        _ => "PR review".to_string(),
                    },
                    None => "PR review".to_string(),
                };
                let target_state_name = if review_state_upper == "CHANGES_REQUESTED" {
                    Some("Needs Attention".to_string())
                } else if review_state_upper == "APPROVED" {
                    Some("In Review".to_string())
                } else {
                    None
                };
                directives.push(GithubSyncDirective {
                identifier: identifier.clone(),
                target_state_name: target_state_name.clone(),
                comment_kind: "github-review-sync".to_string(),
                comment_body: format!(
                    "GitHub review sync: {} submitted {} on {}.{}",
                    reviewer,
                    review_state_upper.to_ascii_lowercase().replace('_', " "),
                    pr_label,
                    payload
                        .review
                        .as_ref()
                        .and_then(|review| review.html_url.clone())
                        .map(|url| format!(" {}", url))
                        .unwrap_or_default()
                ),
                slack_body: format!(
                    "{}\n*Review:* {} marked the PR as {}.",
                    build_pull_request_slack_header(&review_state_upper, &identifier, payload),
                    reviewer,
                    review_state_upper.to_ascii_lowercase().replace('_', " ")
                ),
                memory: GithubMemoryDirective {
                    summary: format!("{} review state changed to {}.", identifier, review_state_upper),
                    next_action: if review_state_upper == "CHANGES_REQUESTED" {
                        "Address requested changes before re-entering review.".to_string()
                    } else {
                        "Keep the issue in review until merge or further feedback.".to_string()
                    },
                    blockers: (review_state_upper == "CHANGES_REQUESTED")
                        .then_some(vec!["GitHub review requested changes".to_string()]),
                    current_state: target_state_name,
                    repo_hint: Some(repo.clone()),
                    last_source: "github-webhook".to_string(),
                    details: json!({
                        "repo": repo,
                        "prNumber": pr.and_then(|pr| pr.number),
                        "reviewState": review_state_upper,
                        "reviewUrl": payload.review.as_ref().and_then(|review| review.html_url.clone()),
                        "reviewer": reviewer,
                    }),
                },
            });
                continue;
            }
        }

        if matches!(event_type, "check_run" | "check_suite") && action == "completed" {
            let check = payload.check_run.as_ref().or(payload.check_suite.as_ref());
            let conclusion = check
                .and_then(|check| check.conclusion.clone())
                .unwrap_or_else(|| "unknown".to_string())
                .to_ascii_lowercase();
            let is_failure = matches!(
                conclusion.as_str(),
                "failure" | "timed_out" | "cancelled" | "action_required"
            );
            directives.push(GithubSyncDirective {
                identifier: identifier.clone(),
                target_state_name: Some(if is_failure {
                    "Needs Attention".to_string()
                } else {
                    "In Review".to_string()
                }),
                comment_kind: "github-check-sync".to_string(),
                comment_body: format!(
                    "GitHub check sync: {} completed with {} in {}.{}",
                    check
                        .and_then(|check| check.name.clone())
                        .unwrap_or_else(|| event_type.to_string()),
                    conclusion,
                    repo,
                    check
                        .and_then(|check| check.html_url.clone().or(check.details_url.clone()))
                        .map(|url| format!(" {}", url))
                        .unwrap_or_default()
                ),
                slack_body: format!(
                    "*GitHub Webhook - {}*\n*Issue:* {}\n*Repo:* {}\n*Check:* {}\n*Conclusion:* {}",
                    event_type.to_ascii_uppercase(),
                    identifier,
                    repo,
                    check
                        .and_then(|check| check.name.clone())
                        .unwrap_or_else(|| event_type.to_string()),
                    conclusion
                ),
                memory: GithubMemoryDirective {
                    summary: format!("{} received a {} CI result.", identifier, conclusion),
                    next_action: if is_failure {
                        "Inspect CI failure and unblock the PR before review can continue."
                            .to_string()
                    } else {
                        "CI is green; keep the issue moving through review.".to_string()
                    },
                    blockers: is_failure.then_some(vec![format!(
                        "CI concluded with {}",
                        conclusion
                    )]),
                    current_state: Some(if is_failure {
                        "Needs Attention".to_string()
                    } else {
                        "In Review".to_string()
                    }),
                    repo_hint: Some(repo.clone()),
                    last_source: "github-webhook".to_string(),
                    details: json!({
                        "repo": repo,
                        "eventType": event_type,
                        "conclusion": conclusion,
                        "checkName": check.and_then(|check| check.name.clone()),
                        "checkUrl": check.and_then(|check| check.html_url.clone().or(check.details_url.clone())),
                    }),
                },
            });
        }
    }

    directives
}

#[cfg(test)]
mod tests {
    use super::{build_github_sync_directives, extract_linear_identifiers, GithubWebhookPayload};

    #[test]
    fn extracts_identifiers_from_pr_payload() {
        let payload = GithubWebhookPayload {
            action: Some("closed".to_string()),
            repository: None,
            pull_request: Some(super::GithubPullRequestPayload {
                number: Some(42),
                title: Some("BYB-23 tighten webhook filtering".to_string()),
                body: Some("Closes BYB-23 and relates to OPS-9".to_string()),
                html_url: None,
                merged: Some(true),
                draft: Some(false),
                head: Some(super::GithubPullRequestHeadPayload {
                    r#ref: Some("feature/BYB-23-webhooks".to_string()),
                }),
                base: None,
            }),
            review: None,
            check_run: None,
            check_suite: None,
        };
        let identifiers = extract_linear_identifiers(&payload);
        assert_eq!(identifiers, vec!["BYB-23".to_string(), "OPS-9".to_string()]);
    }

    #[test]
    fn maps_merged_pr_to_done_sync() {
        let payload = GithubWebhookPayload {
            action: Some("closed".to_string()),
            repository: Some(super::GithubRepositoryPayload {
                full_name: Some("Nexus-Integrated-Technologies/symphony-rs".to_string()),
                html_url: None,
            }),
            pull_request: Some(super::GithubPullRequestPayload {
                number: Some(42),
                title: Some("BYB-23 tighten webhook filtering".to_string()),
                body: Some("Closes BYB-23".to_string()),
                html_url: Some("https://github.com/example/pull/42".to_string()),
                merged: Some(true),
                draft: Some(false),
                head: Some(super::GithubPullRequestHeadPayload {
                    r#ref: Some("feature/BYB-23-webhooks".to_string()),
                }),
                base: None,
            }),
            review: None,
            check_run: None,
            check_suite: None,
        };

        let directives = build_github_sync_directives("pull_request", &payload);
        assert_eq!(directives.len(), 1);
        let directive = &directives[0];
        assert_eq!(directive.identifier, "BYB-23");
        assert_eq!(directive.target_state_name.as_deref(), Some("Done"));
        assert_eq!(directive.comment_kind, "github-pr-sync");
        assert!(directive.slack_body.contains("moved to Done"));
    }
}
