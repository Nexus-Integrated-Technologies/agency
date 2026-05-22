use regex::Regex;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandSafetySeverity {
    Review,
    Block,
}

impl CommandSafetySeverity {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Review => "review",
            Self::Block => "block",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandSafetyViolation {
    pub kind: String,
    pub severity: CommandSafetySeverity,
    pub message: String,
}

impl CommandSafetyViolation {
    fn block(kind: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            kind: kind.into(),
            severity: CommandSafetySeverity::Block,
            message: message.into(),
        }
    }
}

pub fn first_blocking_command_safety_violation(command: &str) -> Option<CommandSafetyViolation> {
    classify_command_safety(command)
        .into_iter()
        .find(|violation| violation.severity == CommandSafetySeverity::Block)
}

pub fn classify_command_safety(command: &str) -> Vec<CommandSafetyViolation> {
    let normalized = normalize_command(command);
    let mut violations = Vec::new();

    if normalized.is_empty() {
        return violations;
    }

    if normalized.replace(' ', "").contains(":(){:|:&};:") {
        violations.push(CommandSafetyViolation::block(
            "fork_bomb",
            "blocked fork-bomb shell pattern",
        ));
    }

    for segment in split_shell_segments(&normalized) {
        let tokens = tokenize_segment(segment);
        if tokens.is_empty() {
            continue;
        }
        collect_segment_violations(&tokens, &mut violations);
    }

    violations
}

fn collect_segment_violations(tokens: &[String], violations: &mut Vec<CommandSafetyViolation>) {
    let mut offset = 0;
    let mut sudo = false;
    while tokens
        .get(offset)
        .is_some_and(|token| matches!(token.as_str(), "sudo" | "env" | "command"))
    {
        match tokens[offset].as_str() {
            "sudo" => {
                sudo = true;
                offset += 1;
            }
            "env" => {
                offset += 1;
                while tokens.get(offset).is_some_and(|token| {
                    token.starts_with('-') || (token.contains('=') && !token.starts_with('='))
                }) {
                    offset += 1;
                }
            }
            "command" => {
                offset += 1;
            }
            _ => break,
        }
    }

    let Some(command) = tokens.get(offset).map(|token| command_basename(token)) else {
        return;
    };

    match command.as_str() {
        "rm" => collect_rm_violations(&tokens[offset + 1..], sudo, violations),
        "dd" => collect_dd_violations(&tokens[offset + 1..], violations),
        "diskutil" => collect_diskutil_violations(&tokens[offset + 1..], violations),
        "shutdown" | "reboot" | "halt" | "poweroff" => {
            violations.push(CommandSafetyViolation::block(
                "power_control",
                format!("blocked host power-control command `{command}`"),
            ));
        }
        value if value.starts_with("mkfs") => {
            violations.push(CommandSafetyViolation::block(
                "filesystem_format",
                format!("blocked filesystem format command `{value}`"),
            ));
        }
        "chmod" => {
            collect_recursive_permission_violations("chmod", &tokens[offset + 1..], violations)
        }
        "chown" => {
            collect_recursive_permission_violations("chown", &tokens[offset + 1..], violations)
        }
        _ => {}
    }
}

fn collect_rm_violations(
    args: &[String],
    sudo: bool,
    violations: &mut Vec<CommandSafetyViolation>,
) {
    let recursive_forced = args
        .iter()
        .take_while(|arg| arg.starts_with('-') && arg.as_str() != "--")
        .any(|arg| {
            let flags = arg.trim_start_matches('-');
            flags.contains('r') && flags.contains('f')
        });
    if !recursive_forced {
        return;
    }

    if sudo {
        violations.push(CommandSafetyViolation::block(
            "sudo_rm_rf",
            "blocked sudo recursive forced removal",
        ));
        return;
    }

    for target in command_targets(args) {
        if is_high_risk_destructive_target(&target) {
            violations.push(CommandSafetyViolation::block(
                "recursive_forced_remove",
                format!("blocked recursive forced removal of high-risk target `{target}`"),
            ));
            return;
        }
    }
}

fn collect_dd_violations(args: &[String], violations: &mut Vec<CommandSafetyViolation>) {
    if args
        .iter()
        .map(|arg| strip_quotes(arg))
        .any(|arg| arg.starts_with("of=/dev/"))
    {
        violations.push(CommandSafetyViolation::block(
            "raw_device_write",
            "blocked raw device write with dd",
        ));
    }
}

fn collect_diskutil_violations(args: &[String], violations: &mut Vec<CommandSafetyViolation>) {
    let joined = args.join(" ");
    let destructive = Regex::new(r"\b(erase|erasedisk|partitiondisk|deletecontainer)\b")
        .expect("valid diskutil safety regex");
    if destructive.is_match(&joined) {
        violations.push(CommandSafetyViolation::block(
            "disk_management_destructive",
            "blocked destructive diskutil operation",
        ));
    }
}

fn collect_recursive_permission_violations(
    command: &str,
    args: &[String],
    violations: &mut Vec<CommandSafetyViolation>,
) {
    let recursive = args
        .iter()
        .take_while(|arg| arg.starts_with('-') && arg.as_str() != "--")
        .any(|arg| arg.trim_start_matches('-').contains('r'));
    if !recursive {
        return;
    }

    for target in command_targets(args) {
        if is_high_risk_destructive_target(&target) {
            violations.push(CommandSafetyViolation::block(
                "recursive_permission_change",
                format!("blocked recursive `{command}` against high-risk target `{target}`"),
            ));
            return;
        }
    }
}

fn normalize_command(command: &str) -> String {
    command
        .replace("\\\n", " ")
        .replace(['\n', '\r', '\t'], " ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase()
}

fn split_shell_segments(command: &str) -> Vec<&str> {
    command
        .split([';', '|', '&'])
        .map(str::trim)
        .filter(|segment| !segment.is_empty())
        .collect()
}

fn tokenize_segment(segment: &str) -> Vec<String> {
    segment
        .split_whitespace()
        .map(strip_quotes)
        .filter(|token| !token.is_empty())
        .collect()
}

fn command_basename(command: &str) -> String {
    command
        .rsplit('/')
        .next()
        .unwrap_or(command)
        .trim()
        .to_string()
}

fn command_targets(args: &[String]) -> Vec<String> {
    let mut targets = Vec::new();
    let mut after_options = false;

    for arg in args {
        let value = strip_quotes(arg);
        if value == "--" {
            after_options = true;
            continue;
        }
        if !after_options && value.starts_with('-') {
            continue;
        }
        targets.push(value);
    }

    targets
}

fn strip_quotes(value: impl AsRef<str>) -> String {
    value
        .as_ref()
        .trim()
        .trim_matches('"')
        .trim_matches('\'')
        .to_string()
}

fn is_high_risk_destructive_target(target: &str) -> bool {
    let target = strip_quotes(target);
    matches!(
        target.as_str(),
        "/" | "/*"
            | "/."
            | "/.."
            | "."
            | "./"
            | "./*"
            | ".."
            | "../"
            | "../*"
            | "~"
            | "~/"
            | "~/*"
            | "$home"
            | "$home/"
            | "$home/*"
            | "${home}"
            | "${home}/"
            | "${home}/*"
            | "*"
            | ".*"
    ) || target.starts_with("~/")
        || target.starts_with("$home/")
        || target.starts_with("${home}/")
}

#[cfg(test)]
mod tests {
    use super::{
        classify_command_safety, first_blocking_command_safety_violation, CommandSafetySeverity,
    };

    #[test]
    fn permits_scoped_cleanup_commands() {
        let violations = classify_command_safety("rm -rf target node_modules .cache/build");
        assert!(violations.is_empty());
    }

    #[test]
    fn blocks_high_risk_recursive_removal_targets() {
        let cases = [
            "rm -rf /",
            "rm -rf ./*",
            "rm -rf ~",
            "rm -rf $HOME/Library",
            "sudo rm -rf /tmp/nanoclaw-safe-fixture",
            "env FOO=bar rm -rf /",
            "sudo env -i FOO=bar rm -rf /tmp/nanoclaw-safe-fixture",
        ];

        for command in cases {
            let violation = first_blocking_command_safety_violation(command)
                .unwrap_or_else(|| panic!("expected block for {command}"));
            assert_eq!(violation.severity, CommandSafetySeverity::Block);
        }
    }

    #[test]
    fn blocks_disk_and_power_commands() {
        let cases = [
            "mkfs.ext4 /dev/disk1",
            "dd if=image.img of=/dev/disk2 bs=1m",
            "diskutil eraseDisk APFS Scratch /dev/disk2",
            "shutdown -h now",
            "reboot",
        ];

        for command in cases {
            assert!(
                first_blocking_command_safety_violation(command).is_some(),
                "expected block for {command}"
            );
        }
    }

    #[test]
    fn blocks_recursive_permission_changes_on_broad_targets() {
        let chmod = first_blocking_command_safety_violation("chmod -R 777 /").unwrap();
        assert_eq!(chmod.kind, "recursive_permission_change");

        let chown = first_blocking_command_safety_violation("chown -R me:staff ~/").unwrap();
        assert_eq!(chown.kind, "recursive_permission_change");
    }
}
