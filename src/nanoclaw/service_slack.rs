use std::collections::BTreeMap;

use anyhow::Result;
use chrono::Utc;

use crate::foundation::{Group, MessageRecord};

use super::app::NanoclawApp;
use super::slack::{SlackChannel, SlackOutboundEnvelope};
use super::slack_threading::{derive_slack_thread_group, get_slack_base_jid};

pub fn ensure_registered_group(
    app: &mut NanoclawApp,
    chat_jid: &str,
    suggested_name: Option<&str>,
) -> Result<Group> {
    if let Some(group) = app
        .groups()?
        .into_iter()
        .find(|group| group.jid == chat_jid)
    {
        return Ok(group);
    }

    let base_jid = get_slack_base_jid(chat_jid);
    if let Some(base_jid) = base_jid {
        if base_jid != chat_jid {
            let base_name = suggested_name
                .map(|value| value.to_string())
                .unwrap_or_else(|| "Slack Thread".to_string());
            let _ = app.ensure_group_for_chat(&base_jid, Some(&base_name))?;
            let groups = app
                .groups()?
                .into_iter()
                .map(|group| (group.jid.clone(), group))
                .collect::<BTreeMap<_, _>>();
            if let Some(group) = derive_slack_thread_group(chat_jid, &groups, suggested_name) {
                return app.register_group(group);
            }
        }
    }

    app.ensure_group_for_chat(chat_jid, suggested_name)
}

pub fn send_recorded_slack_message(
    app: &mut NanoclawApp,
    channel: &mut SlackChannel,
    chat_jid: &str,
    group_name: Option<&str>,
    text: &str,
    sender: &str,
    sender_name: Option<&str>,
    is_from_me: bool,
    is_bot_message: bool,
) -> Result<Option<SlackOutboundEnvelope>> {
    send_recorded_slack_message_as(
        app,
        channel,
        chat_jid,
        chat_jid,
        group_name,
        text,
        sender,
        sender_name,
        is_from_me,
        is_bot_message,
    )
}

pub fn send_recorded_slack_message_as(
    app: &mut NanoclawApp,
    channel: &mut SlackChannel,
    send_chat_jid: &str,
    record_chat_jid: &str,
    group_name: Option<&str>,
    text: &str,
    sender: &str,
    sender_name: Option<&str>,
    is_from_me: bool,
    is_bot_message: bool,
) -> Result<Option<SlackOutboundEnvelope>> {
    let outbound = channel.send_message(send_chat_jid, text)?;
    let Some(outbound) = outbound else {
        return Ok(None);
    };

    record_slack_message(
        app,
        &outbound.timestamp,
        record_chat_jid,
        group_name,
        &outbound.id,
        text,
        sender,
        sender_name,
        is_from_me,
        is_bot_message,
    )?;
    Ok(Some(outbound))
}

pub fn record_slack_message(
    app: &mut NanoclawApp,
    timestamp: &str,
    chat_jid: &str,
    group_name: Option<&str>,
    message_id: &str,
    text: &str,
    sender: &str,
    sender_name: Option<&str>,
    is_from_me: bool,
    is_bot_message: bool,
) -> Result<()> {
    let base_jid = get_slack_base_jid(chat_jid).unwrap_or_else(|| chat_jid.to_string());
    let base_name = group_name.unwrap_or(chat_jid);
    let is_group = !base_jid.trim_start_matches("slack:").starts_with('D');
    app.db.store_chat_metadata(
        &base_jid,
        timestamp,
        Some(base_name),
        Some("slack"),
        Some(is_group),
    )?;
    if chat_jid != base_jid {
        app.db.store_chat_metadata(
            chat_jid,
            timestamp,
            Some(base_name),
            Some("slack"),
            Some(false),
        )?;
    }
    app.db.store_message(&MessageRecord {
        id: message_id.to_string(),
        chat_jid: chat_jid.to_string(),
        sender: sender.to_string(),
        sender_name: sender_name.map(str::to_string),
        content: text.to_string(),
        timestamp: timestamp.to_string(),
        is_from_me,
        is_bot_message,
    })?;
    Ok(())
}

pub fn now_iso() -> String {
    Utc::now().to_rfc3339()
}
