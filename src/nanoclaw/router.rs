use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use regex::Regex;

use crate::foundation::{Group, MessageRecord, RequestPlane, ScheduledTask};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DestinationEntry {
    pub name: String,
    pub display_name: String,
    pub chat_jid: String,
    pub group_folder: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboundDelivery {
    pub to: String,
    pub chat_jid: String,
    pub text: String,
}

pub fn escape_xml(input: &str) -> String {
    input
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

pub fn format_messages(messages: &[MessageRecord], timezone_name: &str) -> Result<String> {
    format_messages_with_destinations(messages, timezone_name, &[])
}

pub fn destinations_from_groups(groups: &[Group]) -> Vec<DestinationEntry> {
    groups
        .iter()
        .map(|group| DestinationEntry {
            name: group.folder.clone(),
            display_name: group.name.clone(),
            chat_jid: group.jid.clone(),
            group_folder: group.folder.clone(),
        })
        .collect()
}

pub fn build_system_prompt_addendum(
    assistant_name: Option<&str>,
    destinations: &[DestinationEntry],
) -> String {
    let mut sections = Vec::new();

    if let Some(name) = assistant_name.filter(|value| !value.trim().is_empty()) {
        sections.push(format!(
            "# You are {name}\n\nYour name is **{name}**. Use it when the channel asks who you are, when introducing yourself, and when signing any message that explicitly calls for a signature."
        ));
    }

    sections.push(build_destinations_section(destinations));
    sections.join("\n\n")
}

pub fn format_agent_prompt(
    messages: &[MessageRecord],
    timezone_name: &str,
    assistant_name: &str,
    destinations: &[DestinationEntry],
) -> Result<String> {
    Ok(format!(
        "{}\n\n{}",
        build_system_prompt_addendum(Some(assistant_name), destinations),
        format_messages_with_destinations(messages, timezone_name, destinations)?
    ))
}

pub fn format_messages_with_destinations(
    messages: &[MessageRecord],
    timezone_name: &str,
    destinations: &[DestinationEntry],
) -> Result<String> {
    let lines = messages
        .iter()
        .map(|message| {
            let display_time = format_local_time(&message.timestamp, timezone_name)?;
            let sender = escape_xml(message.sender_name.as_deref().unwrap_or(&message.sender));
            let time = escape_xml(&display_time);
            let content = escape_xml(&message.content);
            if let Some(from) = destination_name_for_chat(destinations, &message.chat_jid) {
                Ok(format!(
                    "<message from=\"{}\" sender=\"{}\" time=\"{}\">{}</message>",
                    escape_xml(&from),
                    sender,
                    time,
                    content
                ))
            } else {
                Ok(format!(
                    "<message sender=\"{}\" time=\"{}\">{}</message>",
                    sender, time, content
                ))
            }
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
    format_task_request_with_destinations(task, context_messages, timezone_name, &[], "")
}

pub fn format_task_request_with_destinations(
    task: &ScheduledTask,
    context_messages: &[MessageRecord],
    timezone_name: &str,
    destinations: &[DestinationEntry],
    assistant_name: &str,
) -> Result<String> {
    let context = if context_messages.is_empty() {
        format!(
            "<context timezone=\"{}\" />\n<messages />",
            escape_xml(timezone_name)
        )
    } else {
        format_messages_with_destinations(context_messages, timezone_name, destinations)?
    };

    let request_plane = task
        .request_plane
        .as_ref()
        .map(RequestPlane::as_str)
        .unwrap_or("none");

    let task_xml = format!(
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
    );

    if destinations.is_empty() && assistant_name.trim().is_empty() {
        Ok(task_xml)
    } else {
        Ok(format!(
            "{}\n\n{}",
            build_system_prompt_addendum(Some(assistant_name), destinations),
            task_xml
        ))
    }
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

pub fn format_outbound_deliveries(
    raw_text: &str,
    default_destination: &DestinationEntry,
    destinations: &[DestinationEntry],
) -> Result<Vec<OutboundDelivery>> {
    let text = format_outbound(raw_text);
    if text.is_empty() {
        return Ok(Vec::new());
    }

    let block_re = Regex::new(r#"(?s)<message\s+([^>]*)>(.*?)</message>"#)
        .context("failed to compile message block parser")?;
    let mut deliveries = Vec::new();
    for captures in block_re.captures_iter(&text) {
        let attrs = captures.get(1).map(|value| value.as_str()).unwrap_or("");
        let Some(to) = extract_to_attr(attrs)? else {
            continue;
        };
        let Some(destination) = resolve_destination(destinations, &to) else {
            bail!("unknown outbound destination '{}'", to);
        };
        let body = captures
            .get(2)
            .map(|value| value.as_str())
            .unwrap_or("")
            .trim()
            .to_string();
        if body.is_empty() {
            continue;
        }
        deliveries.push(OutboundDelivery {
            to: destination.name.clone(),
            chat_jid: destination.chat_jid.clone(),
            text: body,
        });
    }

    if !deliveries.is_empty() {
        return Ok(deliveries);
    }

    Ok(vec![OutboundDelivery {
        to: default_destination.name.clone(),
        chat_jid: default_destination.chat_jid.clone(),
        text,
    }])
}

pub fn destination_for_group<'a>(
    destinations: &'a [DestinationEntry],
    group: &Group,
) -> DestinationEntry {
    destinations
        .iter()
        .find(|destination| {
            destination.chat_jid == group.jid || destination.group_folder == group.folder
        })
        .cloned()
        .unwrap_or_else(|| DestinationEntry {
            name: group.folder.clone(),
            display_name: group.name.clone(),
            chat_jid: group.jid.clone(),
            group_folder: group.folder.clone(),
        })
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

fn build_destinations_section(destinations: &[DestinationEntry]) -> String {
    if destinations.is_empty() {
        return [
            "## Sending messages",
            "",
            "No explicit destinations are configured. Plain text replies go back to the current conversation.",
        ]
        .join("\n");
    }

    let mut lines = vec!["## Sending messages".to_string(), String::new()];
    if destinations.len() == 1 {
        let d = &destinations[0];
        let label = if d.display_name != d.name {
            format!(" ({})", d.display_name)
        } else {
            String::new()
        };
        lines.push(format!("Your destination is `{}`{}.", d.name, label));
    } else {
        lines.push("You can send messages to the following destinations:".to_string());
        lines.push(String::new());
        for d in destinations {
            let label = if d.display_name != d.name {
                format!(" ({})", d.display_name)
            } else {
                String::new()
            };
            lines.push(format!("- `{}`{}", d.name, label));
        }
    }
    lines.push(String::new());
    lines.push(
        "Wrap each delivered message in a `<message to=\"name\">...</message>` block. Use `<internal>...</internal>` for private notes that should not be delivered.".to_string(),
    );
    lines.push(String::new());
    lines.push(
        "When replying to an incoming message, default to the destination in its `from` attribute unless the request asks for another destination.".to_string(),
    );
    lines.join("\n")
}

fn destination_name_for_chat(destinations: &[DestinationEntry], chat_jid: &str) -> Option<String> {
    destinations
        .iter()
        .find(|destination| destination.chat_jid == chat_jid)
        .map(|destination| destination.name.clone())
}

fn resolve_destination<'a>(
    destinations: &'a [DestinationEntry],
    name: &str,
) -> Option<&'a DestinationEntry> {
    let normalized = name.trim();
    destinations
        .iter()
        .find(|destination| {
            destination.name == normalized
                || destination.display_name == normalized
                || destination.chat_jid == normalized
                || destination.group_folder == normalized
        })
        .or_else(|| {
            let lower = normalized.to_ascii_lowercase();
            destinations.iter().find(|destination| {
                destination.name.eq_ignore_ascii_case(&lower)
                    || destination.display_name.eq_ignore_ascii_case(&lower)
                    || destination.group_folder.eq_ignore_ascii_case(&lower)
            })
        })
}

fn extract_to_attr(attrs: &str) -> Result<Option<String>> {
    let double_re =
        Regex::new(r#"(?i)\bto\s*=\s*"([^"]+)""#).context("failed to compile to parser")?;
    if let Some(captures) = double_re.captures(attrs) {
        return Ok(captures
            .get(1)
            .map(|value| value.as_str().trim().to_string()));
    }
    let single_re =
        Regex::new(r#"(?i)\bto\s*=\s*'([^']+)'"#).context("failed to compile to parser")?;
    Ok(single_re.captures(attrs).and_then(|captures| {
        captures
            .get(1)
            .map(|value| value.as_str().trim().to_string())
    }))
}

#[cfg(test)]
mod tests {
    use crate::foundation::{
        Group, MessageRecord, RequestPlane, ScheduledTask, TaskContextMode, TaskScheduleType,
        TaskStatus,
    };

    use super::{
        destination_for_group, destinations_from_groups, format_agent_prompt, format_messages,
        format_outbound, format_outbound_deliveries, format_task_request,
    };

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
    fn formats_destination_aware_agent_prompt() {
        let groups = vec![Group {
            jid: "slack:C123".to_string(),
            name: "Ops".to_string(),
            folder: "ops".to_string(),
            trigger: "@Andy".to_string(),
            added_at: "2026-05-19T00:00:00Z".to_string(),
            requires_trigger: true,
            is_main: false,
        }];
        let destinations = destinations_from_groups(&groups);
        let prompt = format_agent_prompt(
            &[MessageRecord {
                id: "m1".to_string(),
                chat_jid: "slack:C123".to_string(),
                sender: "user".to_string(),
                sender_name: Some("User".to_string()),
                content: "ship it".to_string(),
                timestamp: "2026-04-05T12:00:00Z".to_string(),
                is_from_me: false,
                is_bot_message: false,
            }],
            "UTC",
            "Andy",
            &destinations,
        )
        .unwrap();

        assert!(prompt.contains("Your destination is `ops`"));
        assert!(prompt.contains("<message from=\"ops\" sender=\"User\""));
        assert!(prompt.contains("<message to=\"name\">"));
    }

    #[test]
    fn strips_internal_tags_from_outbound() {
        assert_eq!(
            format_outbound("hello <internal>secret</internal> world"),
            "hello  world".trim()
        );
    }

    #[test]
    fn parses_message_blocks_into_deliveries() {
        let groups = vec![
            Group {
                jid: "local:ops".to_string(),
                name: "Ops".to_string(),
                folder: "ops".to_string(),
                trigger: "@Andy".to_string(),
                added_at: "2026-05-19T00:00:00Z".to_string(),
                requires_trigger: true,
                is_main: false,
            },
            Group {
                jid: "local:exec".to_string(),
                name: "Exec".to_string(),
                folder: "exec".to_string(),
                trigger: "@Andy".to_string(),
                added_at: "2026-05-19T00:00:00Z".to_string(),
                requires_trigger: true,
                is_main: false,
            },
        ];
        let destinations = destinations_from_groups(&groups);
        let default_destination = destination_for_group(&destinations, &groups[0]);
        let deliveries = format_outbound_deliveries(
            "<internal>skip</internal><message to=\"ops\">ack</message><message to='exec'>go</message>",
            &default_destination,
            &destinations,
        )
        .unwrap();

        assert_eq!(deliveries.len(), 2);
        assert_eq!(deliveries[0].chat_jid, "local:ops");
        assert_eq!(deliveries[0].text, "ack");
        assert_eq!(deliveries[1].chat_jid, "local:exec");
        assert_eq!(deliveries[1].text, "go");
    }

    #[test]
    fn plain_text_defaults_to_current_destination() {
        let group = Group {
            jid: "local:ops".to_string(),
            name: "Ops".to_string(),
            folder: "ops".to_string(),
            trigger: "@Andy".to_string(),
            added_at: "2026-05-19T00:00:00Z".to_string(),
            requires_trigger: true,
            is_main: false,
        };
        let destinations = destinations_from_groups(std::slice::from_ref(&group));
        let default_destination = destination_for_group(&destinations, &group);
        let deliveries =
            format_outbound_deliveries("plain reply", &default_destination, &destinations).unwrap();

        assert_eq!(
            deliveries,
            vec![super::OutboundDelivery {
                to: "ops".to_string(),
                chat_jid: "local:ops".to_string(),
                text: "plain reply".to_string(),
            }]
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
