use anyhow::{Context, Result};
use regex::Regex;

use crate::foundation::{Group, MessageRecord};

use super::sender_allowlist::{
    is_sender_allowed, is_trigger_allowed, should_drop_message, SenderAllowlistConfig,
};

pub fn should_drop_inbound_message(
    message: &MessageRecord,
    config: &SenderAllowlistConfig,
) -> bool {
    !message.is_from_me
        && !message.is_bot_message
        && should_drop_message(&message.chat_jid, config)
        && !is_sender_allowed(&message.chat_jid, &message.sender, config)
}

pub fn has_authorized_trigger(
    group: &Group,
    messages: &[MessageRecord],
    config: &SenderAllowlistConfig,
) -> Result<bool> {
    if group.is_main || !group.requires_trigger {
        return Ok(true);
    }

    let trigger_pattern = build_trigger_pattern(&group.trigger)?;
    Ok(messages.iter().any(|message| {
        trigger_pattern.is_match(message.content.trim())
            && (message.is_from_me || is_trigger_allowed(&group.jid, &message.sender, config))
    }))
}

fn build_trigger_pattern(trigger: &str) -> Result<Regex> {
    Regex::new(&format!(r"(?i)^{}\b", regex::escape(trigger.trim())))
        .with_context(|| format!("invalid trigger pattern '{}'", trigger))
}

#[cfg(test)]
mod tests {
    use crate::foundation::{Group, MessageRecord};

    use super::{has_authorized_trigger, should_drop_inbound_message};
    use crate::nanoclaw::sender_allowlist::{
        AllowedSenders, ChatAllowlistEntry, SenderAllowlistConfig, SenderAllowlistMode,
    };

    #[test]
    fn trigger_check_respects_sender_allowlist() {
        let mut config = SenderAllowlistConfig::default();
        config.chats.insert(
            "slack:C123".to_string(),
            ChatAllowlistEntry {
                allow: AllowedSenders::Only(vec!["alice".to_string()]),
                mode: SenderAllowlistMode::Trigger,
            },
        );
        let group = Group {
            jid: "slack:C123".to_string(),
            name: "Room".to_string(),
            folder: "room".to_string(),
            trigger: "@Andy".to_string(),
            added_at: "2026-04-06T00:00:00Z".to_string(),
            requires_trigger: true,
            is_main: false,
        };
        let denied = MessageRecord {
            id: "m1".to_string(),
            chat_jid: group.jid.clone(),
            sender: "eve".to_string(),
            sender_name: None,
            content: "@Andy hi".to_string(),
            timestamp: "2026-04-06T00:00:00Z".to_string(),
            is_from_me: false,
            is_bot_message: false,
        };
        let allowed = MessageRecord {
            sender: "alice".to_string(),
            ..denied.clone()
        };

        assert!(!has_authorized_trigger(&group, &[denied], &config).unwrap());
        assert!(has_authorized_trigger(&group, &[allowed], &config).unwrap());
    }

    #[test]
    fn drop_mode_discards_denied_messages() {
        let mut config = SenderAllowlistConfig::default();
        config.chats.insert(
            "slack:C123".to_string(),
            ChatAllowlistEntry {
                allow: AllowedSenders::Only(vec!["alice".to_string()]),
                mode: SenderAllowlistMode::Drop,
            },
        );
        let message = MessageRecord {
            id: "m1".to_string(),
            chat_jid: "slack:C123".to_string(),
            sender: "eve".to_string(),
            sender_name: None,
            content: "hi".to_string(),
            timestamp: "2026-04-06T00:00:00Z".to_string(),
            is_from_me: false,
            is_bot_message: false,
        };

        assert!(should_drop_inbound_message(&message, &config));
    }
}
