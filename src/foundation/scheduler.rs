use std::str::FromStr;

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Duration, Utc};
use chrono_tz::Tz;
use croner::Cron;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{
    RequestPlane, ScheduledTask, TaskContextMode, TaskRunLog, TaskRunStatus, TaskScheduleType,
    TaskStatus,
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaskScheduleInput {
    pub group_folder: String,
    pub chat_jid: String,
    pub prompt: String,
    pub script: Option<String>,
    pub request_plane: Option<RequestPlane>,
    pub schedule_type: TaskScheduleType,
    pub schedule_value: String,
    pub context_mode: TaskContextMode,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HabitDefinition {
    pub name: String,
    pub group_folder: String,
    pub chat_jid: String,
    pub prompt: String,
    pub script: Option<String>,
    pub request_plane: Option<RequestPlane>,
    pub schedule_type: TaskScheduleType,
    pub schedule_value: String,
    pub context_mode: TaskContextMode,
}

impl HabitDefinition {
    pub fn into_task_input(&self) -> TaskScheduleInput {
        TaskScheduleInput {
            group_folder: self.group_folder.clone(),
            chat_jid: self.chat_jid.clone(),
            prompt: self.prompt.clone(),
            script: self.script.clone(),
            request_plane: self.request_plane.clone(),
            schedule_type: self.schedule_type.clone(),
            schedule_value: self.schedule_value.clone(),
            context_mode: self.context_mode.clone(),
        }
    }
}

pub fn build_scheduled_task(
    input: TaskScheduleInput,
    timezone_name: &str,
    now: DateTime<Utc>,
) -> Result<ScheduledTask> {
    let next_run = compute_initial_next_run(
        &input.schedule_type,
        &input.schedule_value,
        timezone_name,
        now,
    )?;

    Ok(ScheduledTask {
        id: format!("task-{}", Uuid::new_v4()),
        group_folder: input.group_folder,
        chat_jid: input.chat_jid,
        prompt: input.prompt,
        script: input.script,
        request_plane: input.request_plane,
        schedule_type: input.schedule_type,
        schedule_value: input.schedule_value,
        context_mode: input.context_mode,
        next_run,
        last_run: None,
        last_result: None,
        status: TaskStatus::Active,
        created_at: now.to_rfc3339(),
    })
}

pub fn compute_next_run(
    task: &ScheduledTask,
    timezone_name: &str,
    now: DateTime<Utc>,
) -> Result<Option<String>> {
    match task.schedule_type {
        TaskScheduleType::Once => Ok(None),
        TaskScheduleType::Cron => compute_next_cron_run(&task.schedule_value, timezone_name, now),
        TaskScheduleType::Interval => {
            let interval_ms = parse_interval_ms(&task.schedule_value)?;
            let anchor = task
                .next_run
                .as_deref()
                .map(parse_rfc3339_utc)
                .transpose()?
                .unwrap_or(now);
            let mut next = anchor + Duration::milliseconds(interval_ms);
            while next <= now {
                next += Duration::milliseconds(interval_ms);
            }
            Ok(Some(next.to_rfc3339()))
        }
        TaskScheduleType::Custom(ref value) => {
            bail!("unsupported schedule type: {}", value)
        }
    }
}

pub fn build_run_log(
    task_id: &str,
    duration_ms: i64,
    result: Option<String>,
    error: Option<String>,
    now: DateTime<Utc>,
) -> TaskRunLog {
    let status = if error.is_some() {
        TaskRunStatus::Error
    } else {
        TaskRunStatus::Success
    };

    TaskRunLog {
        task_id: task_id.to_string(),
        run_at: now.to_rfc3339(),
        duration_ms,
        status,
        result,
        error,
    }
}

fn compute_initial_next_run(
    schedule_type: &TaskScheduleType,
    schedule_value: &str,
    timezone_name: &str,
    now: DateTime<Utc>,
) -> Result<Option<String>> {
    match schedule_type {
        TaskScheduleType::Once => Ok(Some(parse_rfc3339_utc(schedule_value)?.to_rfc3339())),
        TaskScheduleType::Interval => {
            let interval_ms = parse_interval_ms(schedule_value)?;
            Ok(Some(
                (now + Duration::milliseconds(interval_ms)).to_rfc3339(),
            ))
        }
        TaskScheduleType::Cron => compute_next_cron_run(schedule_value, timezone_name, now),
        TaskScheduleType::Custom(value) => bail!("unsupported schedule type: {}", value),
    }
}

fn compute_next_cron_run(
    schedule_value: &str,
    timezone_name: &str,
    now: DateTime<Utc>,
) -> Result<Option<String>> {
    let timezone = parse_timezone(timezone_name);
    let cron = Cron::from_str(schedule_value)
        .with_context(|| format!("invalid cron expression: {schedule_value}"))?;
    let next = cron
        .find_next_occurrence(&now.with_timezone(&timezone), false)
        .with_context(|| format!("failed to compute next cron occurrence for {schedule_value}"))?;
    Ok(Some(next.with_timezone(&Utc).to_rfc3339()))
}

fn parse_interval_ms(value: &str) -> Result<i64> {
    let interval = value
        .trim()
        .parse::<i64>()
        .with_context(|| format!("invalid interval milliseconds: {value}"))?;
    if interval <= 0 {
        bail!("interval milliseconds must be positive: {}", value);
    }
    Ok(interval)
}

fn parse_rfc3339_utc(value: &str) -> Result<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(value)
        .with_context(|| format!("invalid RFC3339 timestamp: {value}"))?
        .with_timezone(&Utc))
}

fn parse_timezone(value: &str) -> Tz {
    value.parse::<Tz>().unwrap_or(chrono_tz::UTC)
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};

    use super::{build_scheduled_task, compute_next_run, HabitDefinition, TaskScheduleInput};
    use crate::foundation::{TaskContextMode, TaskScheduleType};

    #[test]
    fn once_task_uses_schedule_value_as_next_run() {
        let now = Utc.with_ymd_and_hms(2026, 4, 5, 12, 0, 0).unwrap();
        let task = build_scheduled_task(
            TaskScheduleInput {
                group_folder: "main".to_string(),
                chat_jid: "main".to_string(),
                prompt: "ping".to_string(),
                script: None,
                request_plane: None,
                schedule_type: TaskScheduleType::Once,
                schedule_value: "2026-04-05T15:00:00Z".to_string(),
                context_mode: TaskContextMode::Isolated,
            },
            "UTC",
            now,
        )
        .unwrap();

        assert_eq!(task.next_run.as_deref(), Some("2026-04-05T15:00:00+00:00"));
    }

    #[test]
    fn interval_task_skips_missed_intervals() {
        let now = Utc.with_ymd_and_hms(2026, 4, 5, 12, 10, 0).unwrap();
        let mut task = build_scheduled_task(
            TaskScheduleInput {
                group_folder: "main".to_string(),
                chat_jid: "main".to_string(),
                prompt: "ping".to_string(),
                script: None,
                request_plane: None,
                schedule_type: TaskScheduleType::Interval,
                schedule_value: "60000".to_string(),
                context_mode: TaskContextMode::Isolated,
            },
            "UTC",
            Utc.with_ymd_and_hms(2026, 4, 5, 12, 0, 0).unwrap(),
        )
        .unwrap();
        task.next_run = Some("2026-04-05T12:01:00Z".to_string());

        let next = compute_next_run(&task, "UTC", now).unwrap();
        assert_eq!(next.as_deref(), Some("2026-04-05T12:11:00+00:00"));
    }

    #[test]
    fn habit_definition_maps_to_task_input() {
        let habit = HabitDefinition {
            name: "daily check".to_string(),
            group_folder: "main".to_string(),
            chat_jid: "main".to_string(),
            prompt: "check status".to_string(),
            script: None,
            request_plane: None,
            schedule_type: TaskScheduleType::Cron,
            schedule_value: "0 9 * * *".to_string(),
            context_mode: TaskContextMode::Group,
        };

        let input = habit.into_task_input();
        assert_eq!(input.prompt, "check status");
        assert_eq!(input.schedule_type, TaskScheduleType::Cron);
    }
}
