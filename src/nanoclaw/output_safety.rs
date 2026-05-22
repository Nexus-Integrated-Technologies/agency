use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::json;

pub const OUTPUT_SAFETY_SCHEMA_VERSION: &str = "2026-05-20";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OutputSafetyFinding {
    pub kind: String,
    pub label: String,
    pub count: usize,
}

struct SensitivePattern {
    kind: &'static str,
    label: &'static str,
    pattern: &'static str,
}

const SENSITIVE_PATTERNS: &[SensitivePattern] = &[
    SensitivePattern {
        kind: "secret_assignment",
        label: "secret-like assignment",
        pattern: r#"(?i)\b(?:api[_-]?key|token|secret|password|passwd)\b\s*[:=]\s*['"]?[^\s'"]{8,}['"]?"#,
    },
    SensitivePattern {
        kind: "authorization_bearer",
        label: "bearer authorization credential",
        pattern: r#"(?i)\bauthorization\b\s*[:=]\s*bearer\s+[A-Za-z0-9._~+/=-]{12,}"#,
    },
    SensitivePattern {
        kind: "bearer_token",
        label: "bearer token",
        pattern: r#"(?i)\bbearer\s+[A-Za-z0-9._~+/=-]{12,}"#,
    },
    SensitivePattern {
        kind: "provider_secret_key",
        label: "provider secret key",
        pattern: r#"\b(?:sk|rk)-[A-Za-z0-9_-]{12,}\b|\bsk_(?:live|test)_[A-Za-z0-9]{12,}\b"#,
    },
    SensitivePattern {
        kind: "github_token",
        label: "GitHub token",
        pattern: r#"\b(?:ghp|gho|ghu|ghs|ghr|github_pat)_[A-Za-z0-9_]{12,}\b"#,
    },
];

pub fn classify_sensitive_output(text: &str) -> Vec<OutputSafetyFinding> {
    let mut findings = Vec::new();
    for pattern in SENSITIVE_PATTERNS {
        let Ok(regex) = Regex::new(pattern.pattern) else {
            continue;
        };
        let count = regex.find_iter(text).count();
        if count > 0 {
            findings.push(OutputSafetyFinding {
                kind: pattern.kind.to_string(),
                label: pattern.label.to_string(),
                count,
            });
        }
    }
    findings
}

pub fn redact_sensitive_output(text: &str) -> String {
    let mut redacted = text.to_string();
    redacted = replace_all(
        &redacted,
        r#"(?i)(\b(?:api[_-]?key|token|secret|password|passwd)\b\s*[:=]\s*['"]?)[^\s'"]{8,}(['"]?)"#,
        "${1}[redacted:secret_assignment]${2}",
    );
    redacted = replace_all(
        &redacted,
        r#"(?i)(\bauthorization\b\s*[:=]\s*bearer\s+)[A-Za-z0-9._~+/=-]{12,}"#,
        "${1}[redacted:authorization_bearer]",
    );
    redacted = replace_all(
        &redacted,
        r#"(?i)\bbearer\s+[A-Za-z0-9._~+/=-]{12,}"#,
        "Bearer [redacted:bearer_token]",
    );
    redacted = replace_all(
        &redacted,
        r#"\b(?:sk|rk)-[A-Za-z0-9_-]{12,}\b|\bsk_(?:live|test)_[A-Za-z0-9]{12,}\b"#,
        "[redacted:provider_secret_key]",
    );
    replace_all(
        &redacted,
        r#"\b(?:ghp|gho|ghu|ghs|ghr|github_pat)_[A-Za-z0-9_]{12,}\b"#,
        "[redacted:github_token]",
    )
}

pub fn output_safety_report_body(source: &str, text: &str) -> Option<String> {
    let findings = classify_sensitive_output(text);
    if findings.is_empty() {
        return None;
    }
    serde_json::to_string_pretty(&json!({
        "schemaVersion": OUTPUT_SAFETY_SCHEMA_VERSION,
        "source": source,
        "redactionApplied": true,
        "findings": findings,
    }))
    .ok()
}

fn replace_all(text: &str, pattern: &str, replacement: &str) -> String {
    Regex::new(pattern)
        .map(|regex| regex.replace_all(text, replacement).to_string())
        .unwrap_or_else(|_| text.to_string())
}

#[cfg(test)]
mod tests {
    use super::{classify_sensitive_output, output_safety_report_body, redact_sensitive_output};

    #[test]
    fn redacts_secret_assignments_and_bearer_tokens() {
        let raw = "api_key='sk-testsecretvalue'\nauthorization: Bearer abcdefghijklmnop\n";
        let redacted = redact_sensitive_output(raw);

        assert!(!redacted.contains("sk-testsecretvalue"));
        assert!(!redacted.contains("abcdefghijklmnop"));
        assert!(redacted.contains("[redacted:secret_assignment]"));
        assert!(redacted.contains("[redacted:authorization_bearer]"));
    }

    #[test]
    fn classifies_sensitive_output_for_operator_reports() {
        let findings = classify_sensitive_output(
            "token=ghp_1234567890abcdef\nAuthorization: Bearer abcdefghijklmnop\n",
        );

        assert!(findings
            .iter()
            .any(|finding| finding.kind == "secret_assignment"));
        assert!(findings
            .iter()
            .any(|finding| finding.kind == "authorization_bearer"));
        assert!(output_safety_report_body("test-log", "stdout=plain text").is_none());
        assert!(output_safety_report_body("test-log", "secret=abcdefghijkl").is_some());
    }
}
