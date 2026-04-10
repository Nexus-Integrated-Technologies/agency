use std::collections::BTreeMap;

use sha2::{Digest, Sha256};

use crate::foundation::Group;

pub const SLACK_THREAD_DELIMITER: &str = "::thread:";
const MAX_GROUP_FOLDER_LENGTH: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlackThreadTarget {
    pub base_jid: String,
    pub channel_id: String,
    pub thread_ts: String,
}

pub fn build_slack_thread_jid(jid_or_channel_id: &str, thread_ts: &str) -> String {
    let channel_id = jid_or_channel_id
        .strip_prefix("slack:")
        .unwrap_or(jid_or_channel_id);
    format!("slack:{channel_id}{SLACK_THREAD_DELIMITER}{thread_ts}")
}

pub fn parse_slack_thread_jid(jid: &str) -> Option<SlackThreadTarget> {
    let remainder = jid.strip_prefix("slack:")?;
    let delimiter_index = remainder.find(SLACK_THREAD_DELIMITER)?;
    let channel_id = &remainder[..delimiter_index];
    let thread_ts = &remainder[delimiter_index + SLACK_THREAD_DELIMITER.len()..];
    if channel_id.is_empty() || thread_ts.is_empty() {
        return None;
    }
    Some(SlackThreadTarget {
        base_jid: format!("slack:{channel_id}"),
        channel_id: channel_id.to_string(),
        thread_ts: thread_ts.to_string(),
    })
}

pub fn get_slack_base_jid(jid: &str) -> Option<String> {
    if !jid.starts_with("slack:") {
        return None;
    }
    Some(
        parse_slack_thread_jid(jid)
            .map(|parsed| parsed.base_jid)
            .unwrap_or_else(|| jid.to_string()),
    )
}

pub fn build_slack_thread_group_folder(base_folder: &str, chat_jid: &str) -> String {
    let hash = format!("{:x}", Sha256::digest(chat_jid.as_bytes()));
    let suffix = format!("_thr_{}", &hash[..10]);
    let max_base_length = MAX_GROUP_FOLDER_LENGTH.saturating_sub(suffix.len());
    let safe_base = if max_base_length == 0 {
        ""
    } else {
        &base_folder[..base_folder.len().min(max_base_length)]
    };
    format!("{safe_base}{suffix}")
}

pub fn derive_slack_thread_group(
    chat_jid: &str,
    groups: &BTreeMap<String, Group>,
    name: Option<&str>,
) -> Option<Group> {
    let parsed = parse_slack_thread_jid(chat_jid)?;
    let base_group = groups.get(&parsed.base_jid)?;
    Some(Group {
        jid: chat_jid.to_string(),
        name: name
            .map(str::to_string)
            .unwrap_or_else(|| format!("{} Thread", base_group.name)),
        folder: build_slack_thread_group_folder(&base_group.folder, chat_jid),
        trigger: base_group.trigger.clone(),
        added_at: chrono::Utc::now().to_rfc3339(),
        requires_trigger: base_group.requires_trigger,
        is_main: false,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::foundation::Group;

    use super::{
        build_slack_thread_group_folder, build_slack_thread_jid, derive_slack_thread_group,
        get_slack_base_jid, parse_slack_thread_jid,
    };

    #[test]
    fn parses_and_builds_slack_thread_jids() {
        let jid = build_slack_thread_jid("slack:C0TEST", "1774956749.012119");
        assert_eq!(jid, "slack:C0TEST::thread:1774956749.012119");
        let parsed = parse_slack_thread_jid(&jid).unwrap();
        assert_eq!(parsed.base_jid, "slack:C0TEST");
        assert_eq!(parsed.channel_id, "C0TEST");
        assert_eq!(parsed.thread_ts, "1774956749.012119");
        assert_eq!(get_slack_base_jid(&jid), Some("slack:C0TEST".to_string()));
    }

    #[test]
    fn derives_thread_groups_from_registered_base_groups() {
        let mut groups = BTreeMap::new();
        groups.insert(
            "slack:C0TEST".to_string(),
            Group {
                jid: "slack:C0TEST".to_string(),
                name: "Linear Issues".to_string(),
                folder: "slack_linear-issues".to_string(),
                trigger: "@Andy".to_string(),
                added_at: "2026-04-06T00:00:00Z".to_string(),
                requires_trigger: false,
                is_main: false,
            },
        );

        let chat_jid = "slack:C0TEST::thread:1774956749.012119";
        let group = derive_slack_thread_group(chat_jid, &groups, None).unwrap();
        assert_eq!(group.jid, chat_jid);
        assert_eq!(group.name, "Linear Issues Thread");
        assert_eq!(
            group.folder,
            build_slack_thread_group_folder("slack_linear-issues", chat_jid)
        );
        assert!(!group.requires_trigger);
    }
}
