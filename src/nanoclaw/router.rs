use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use chrono_tz::Tz;

use crate::foundation::{MessageRecord, RequestPlane, ScheduledTask};

pub fn escape_xml(input: &str) -> String {
    input
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

pub fn format_messages(messages: &[MessageRecord], timezone_name: &str) -> Result<String> {
    let lines = messages
        .iter()
        .map(|message| {
            let display_time = format_local_time(&message.timestamp, timezone_name)?;
            Ok(format!(
                "<message sender=\"{}\" time=\"{}\">{}</message>",
                escape_xml(message.sender_name.as_deref().unwrap_or(&message.sender)),
                escape_xml(&display_time),
                escape_xml(&message.content)
            ))
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(format!(
        "<context timezone=\"{}\" />\n<messages>\n{}\n</messages>",
        escape_xml(timezone_name),
        lines.join("\n")
    ))
}

pub fn format_task_request(
    task: &ScheduledTask,
    context_messages: &[MessageRecord],
    timezone_name: &str,
) -> Result<String> {
    let context = if context_messages.is_empty() {
        format!(
            "<context timezone=\"{}\" />\n<messages />",
            escape_xml(timezone_name)
        )
    } else {
        format_messages(context_messages, timezone_name)?
    };

    let request_plane = task
        .request_plane
        .as_ref()
        .map(RequestPlane::as_str)
        .unwrap_or("none");

    Ok(format!(
        "<task id=\"{}\" schedule_type=\"{}\" schedule_value=\"{}\" request_plane=\"{}\" context_mode=\"{}\">\n<prompt>{}</prompt>\n{}\n</task>\n{}",
        escape_xml(&task.id),
        escape_xml(task.schedule_type.as_str()),
        escape_xml(&task.schedule_value),
        escape_xml(request_plane),
        escape_xml(task.context_mode.as_str()),
        escape_xml(&task.prompt),
        task.script
            .as_deref()
            .map(|script| format!("<script>{}</script>", escape_xml(script)))
            .unwrap_or_default(),
        context
    ))
}

pub fn strip_internal_tags(text: &str) -> String {
    let mut output = String::with_capacity(text.len());
    let mut remaining = text;
    let start_tag = "<internal>";
    let end_tag = "</internal>";

    loop {
        let Some(start) = remaining.find(start_tag) else {
            output.push_str(remaining);
            break;
        };
        output.push_str(&remaining[..start]);
        let after_start = &remaining[start + start_tag.len()..];
        if let Some(end) = after_start.find(end_tag) {
            remaining = &after_start[end + end_tag.len()..];
        } else {
            break;
        }
    }

    output.trim().to_string()
}

pub fn format_outbound(raw_text: &str) -> String {
    strip_internal_tags(raw_text)
}

fn format_local_time(timestamp: &str, timezone_name: &str) -> Result<String> {
    let parsed = DateTime::parse_from_rfc3339(timestamp)
        .with_context(|| format!("invalid message timestamp: {}", timestamp))?
        .with_timezone(&Utc);
    let timezone = timezone_name.parse::<Tz>().unwrap_or(chrono_tz::UTC);
    Ok(parsed
        .with_timezone(&timezone)
        .format("%Y-%m-%d %H:%M:%S %Z")
        .to_string())
}

#[cfg(test)]
mod tests {
    use crate::foundation::{
        MessageRecord, RequestPlane, ScheduledTask, TaskContextMode, TaskScheduleType, TaskStatus,
    };

    use super::{format_messages, format_outbound, format_task_request};

    #[test]
    fn formats_message_context() {
        let xml = format_messages(
            &[MessageRecord {
                id: "m1".to_string(),
                chat_jid: "main".to_string(),
                sender: "user".to_string(),
                sender_name: Some("User".to_string()),
                content: "hello <world>".to_string(),
                timestamp: "2026-04-05T12:00:00Z".to_string(),
                is_from_me: false,
                is_bot_message: false,
            }],
            "UTC",
        )
        .unwrap();
        assert!(xml.contains("&lt;world&gt;"));
        assert!(xml.contains("timezone=\"UTC\""));
    }

    #[test]
    fn strips_internal_tags_from_outbound() {
        assert_eq!(
            format_outbound("hello <internal>secret</internal> world"),
            "hello  world".trim()
        );
    }

    #[test]
    fn formats_task_request_with_context() {
        let xml = format_task_request(
            &ScheduledTask {
                id: "task-1".to_string(),
                group_folder: "main".to_string(),
                chat_jid: "main".to_string(),
                prompt: "Run <check>".to_string(),
                script: Some("echo hi".to_string()),
                request_plane: Some(RequestPlane::Web),
                schedule_type: TaskScheduleType::Once,
                schedule_value: "2026-04-05T13:00:00Z".to_string(),
                context_mode: TaskContextMode::Group,
                next_run: Some("2026-04-05T13:00:00Z".to_string()),
                last_run: None,
                last_result: None,
                status: TaskStatus::Active,
                created_at: "2026-04-05T12:00:00Z".to_string(),
            },
            &[MessageRecord {
                id: "m1".to_string(),
                chat_jid: "main".to_string(),
                sender: "user".to_string(),
                sender_name: Some("User".to_string()),
                content: "hello".to_string(),
                timestamp: "2026-04-05T12:00:00Z".to_string(),
                is_from_me: false,
                is_bot_message: false,
            }],
            "UTC",
        )
        .unwrap();

        assert!(xml.contains("request_plane=\"web\""));
        assert!(xml.contains("&lt;check&gt;"));
        assert!(xml.contains("<script>echo hi</script>"));
        assert!(xml.contains("<messages>"));
    }
}
