use std::thread;
use std::time::Duration;

use anyhow::Result;

use super::app::NanoclawApp;
use super::config::NanoclawConfig;
use super::linear::{
    run_linear_pm_digest_task, run_linear_policy_automation_sweep_task, run_linear_teams_task,
    LinearPmDigestTaskInput, LinearPolicyAutomationSweepTaskInput, LinearTeamsTaskInput,
};
use super::service_slack::{now_iso, send_recorded_slack_message};
use super::slack::SlackChannel;

const LOOP_INTERVAL_MS: u64 = 60_000;

pub fn start_pm_automation_loop(config: NanoclawConfig) -> Result<()> {
    if config.linear_chat_jid.trim().is_empty() {
        return Ok(());
    }
    let _ = SlackChannel::from_config(&config, false)?;

    thread::spawn(move || {
        let mut app = match NanoclawApp::open(config.clone()) {
            Ok(app) => app,
            Err(error) => {
                eprintln!("pm automation failed to open app: {error:#}");
                return;
            }
        };
        let mut channel = match SlackChannel::from_config(&config, false) {
            Ok(channel) => channel,
            Err(error) => {
                eprintln!("pm automation failed to init Slack channel: {error:#}");
                return;
            }
        };

        loop {
            if let Err(error) = run_pm_automation_iteration(&mut app, &mut channel) {
                eprintln!("pm automation iteration failed: {error:#}");
            }
            thread::sleep(Duration::from_millis(LOOP_INTERVAL_MS));
        }
    });

    Ok(())
}

fn run_pm_automation_iteration(app: &mut NanoclawApp, channel: &mut SlackChannel) -> Result<()> {
    let team_keys = resolve_team_keys(app, channel)?;
    for team_key in team_keys {
        maybe_run_policy_sweep(app, channel, &team_key)?;
        maybe_run_digest(app, channel, &team_key)?;
    }
    Ok(())
}

fn resolve_team_keys(app: &NanoclawApp, channel: &mut SlackChannel) -> Result<Vec<String>> {
    if !app.config.linear_pm_team_keys.is_empty() {
        return Ok(app.config.linear_pm_team_keys.clone());
    }

    let result = run_linear_teams_task(&app.config, LinearTeamsTaskInput { limit: 25 });
    if !result.ok {
        eprintln!(
            "pm automation team resolution failed: {}",
            result
                .error
                .unwrap_or_else(|| "unknown Linear teams error".to_string())
        );
        return Ok(Vec::new());
    }

    let _ = channel;
    Ok(result
        .teams
        .unwrap_or_default()
        .into_iter()
        .map(|team| team.key)
        .collect())
}

fn maybe_run_policy_sweep(
    app: &mut NanoclawApp,
    channel: &mut SlackChannel,
    team_key: &str,
) -> Result<()> {
    let state_key = format!("pm-automation:policy:{team_key}");
    let last_run = app.db.router_state(&state_key)?;
    let interval_ms = app.config.linear_pm_policy_interval_minutes * 60_000;
    if should_initialize_or_skip(app, &state_key, last_run.as_deref(), interval_ms)? {
        return Ok(());
    }

    let result = run_linear_policy_automation_sweep_task(
        &app.db,
        &app.config,
        LinearPolicyAutomationSweepTaskInput {
            team_key: Some(team_key.to_string()),
            limit: 250,
            stale_days: 7,
            due_soon_days: 3,
            apply: true,
            allow_state_transitions: false,
            max_auto_applied: app.config.linear_pm_guardrail_max_automations,
        },
    );
    if result.ok {
        eprintln!(
            "pm policy sweep completed for {}: considered={} auto_applied={} needs_approval={}",
            team_key,
            result.considered_count.unwrap_or(0),
            result.auto_applied_count.unwrap_or(0),
            result.needs_approval_count.unwrap_or(0)
        );
    } else if let Some(error) = result.error.as_ref() {
        eprintln!("pm policy sweep failed for {}: {}", team_key, error);
    }
    app.db.upsert_router_state(&state_key, &now_iso())?;

    let summary = build_policy_sweep_summary(team_key, &result);
    if let Some(summary) = summary {
        let linear_chat_jid = app.config.linear_chat_jid.clone();
        let assistant_name = app.config.assistant_name.clone();
        let _ = send_recorded_slack_message(
            app,
            channel,
            &linear_chat_jid,
            Some("Linear PM"),
            &summary,
            &assistant_name,
            Some(&assistant_name),
            true,
            true,
        )?;
    }
    Ok(())
}

fn maybe_run_digest(
    app: &mut NanoclawApp,
    channel: &mut SlackChannel,
    team_key: &str,
) -> Result<()> {
    let state_key = format!("pm-automation:digest:{team_key}");
    let last_run = app.db.router_state(&state_key)?;
    let interval_ms = app.config.linear_pm_digest_interval_hours * 60 * 60_000;
    if should_initialize_or_skip(app, &state_key, last_run.as_deref(), interval_ms)? {
        return Ok(());
    }

    let result = run_linear_pm_digest_task(
        &app.config,
        LinearPmDigestTaskInput {
            team_key: Some(team_key.to_string()),
            limit: 250,
            stale_days: 7,
            due_soon_days: 3,
        },
    );
    if result.ok {
        eprintln!(
            "pm digest completed for {}: emitted_body={}",
            team_key,
            result
                .body
                .as_ref()
                .map(|body| !body.trim().is_empty())
                .unwrap_or(false)
        );
    }
    app.db.upsert_router_state(&state_key, &now_iso())?;

    if result.ok {
        if let Some(body) = result.body {
            let linear_chat_jid = app.config.linear_chat_jid.clone();
            let assistant_name = app.config.assistant_name.clone();
            let _ = send_recorded_slack_message(
                app,
                channel,
                &linear_chat_jid,
                Some("Linear PM"),
                &body,
                &assistant_name,
                Some(&assistant_name),
                true,
                true,
            )?;
        }
    } else if let Some(error) = result.error {
        eprintln!("pm digest failed for {}: {}", team_key, error);
    }
    Ok(())
}

fn should_initialize_or_skip(
    app: &NanoclawApp,
    state_key: &str,
    last_run: Option<&str>,
    interval_ms: u64,
) -> Result<bool> {
    let now = now_iso();
    let Some(last_run) = last_run else {
        app.db.upsert_router_state(state_key, &now)?;
        return Ok(true);
    };
    let last_run = chrono::DateTime::parse_from_rfc3339(last_run)
        .map(|value| value.with_timezone(&chrono::Utc))
        .unwrap_or_else(|_| {
            chrono::Utc::now() - chrono::Duration::milliseconds(interval_ms as i64)
        });
    let should_run =
        chrono::Utc::now() - last_run >= chrono::Duration::milliseconds(interval_ms as i64);
    Ok(!should_run)
}

fn build_policy_sweep_summary(
    team_key: &str,
    result: &super::linear::LinearPolicyAutomationSweepTaskResult,
) -> Option<String> {
    if !result.ok {
        return result
            .error
            .as_ref()
            .map(|error| format!("*PM automation sweep ({team_key})*\nError: {error}"));
    }
    let actions = result.actions.as_ref()?;
    if actions.is_empty() {
        return None;
    }
    if result.auto_applied_count.unwrap_or(0) == 0 && result.needs_approval_count.unwrap_or(0) == 0
    {
        return None;
    }

    let mut lines = vec![
        format!("*PM automation sweep ({team_key})*"),
        format!(
            "Considered {} issues.",
            result.considered_count.unwrap_or(0)
        ),
        format!("Auto-applied: {}", result.auto_applied_count.unwrap_or(0)),
        format!(
            "Needs approval: {}",
            result.needs_approval_count.unwrap_or(0)
        ),
    ];

    let highlights = actions.iter().take(6).collect::<Vec<_>>();
    if !highlights.is_empty() {
        lines.push(String::new());
        lines.push("Highlights:".to_string());
        for action in highlights {
            lines.push(format!("- {}: {}", action.identifier, action.details));
        }
    }

    Some(lines.join("\n"))
}
