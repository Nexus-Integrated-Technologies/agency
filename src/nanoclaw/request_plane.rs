use regex::Regex;

use crate::foundation::RequestPlane;

pub const REQUEST_PLANE_ENV_KEY: &str = "NANOCLAW_REQUEST_PLANE";

const WEB_REQUEST_PATTERNS: &[&str] = &[
    r"\b(?:curl|wget|httpie|xh|lynx|w3m|links|elinks)\b",
    r"\bgh\b(?:\s+[^\n]*)?\b(?:api|browse|repo|pr|issue|workflow|run|release|search)\b",
    r"\bgit\b\s+(?:clone|fetch|pull|push|ls-remote)\b",
    r"\b(?:python|python3)\b[^\n]*\b(?:requests|urllib|httpx|aiohttp)\b",
    r"\b(?:node|bun|deno)\b[^\n]*\b(?:fetch\s*\(|axios|undici)\b",
    r"\b(?:open|xdg-open)\s+https?://",
    r"\bfetch\s*\(",
    r"https?://",
];

const EMAIL_REQUEST_PATTERNS: &[&str] = &[
    r"\b(?:imap|smtp|sendmail|mailx?|msmtp|mutt|notmuch)\b",
    r"\b(?:python|python3)\b[^\n]*\b(?:smtplib|imaplib|poplib)\b",
    r"\b(?:node|bun|deno)\b[^\n]*\b(?:nodemailer|imapflow|imap-simple)\b",
    r"\bimap://",
    r"\bsmtp://",
    r"\bgmail\b",
];

const WEB_REQUEST_DETECTION_PATTERNS: &[&str] = &[
    r"\bweb\b",
    r"\bbrowser\b",
    r"\bwebsite\b",
    r"\burl\b",
    r"\blink\b",
    r"\bfetch\b",
    r"\bsearch\b",
    r"\blook up\b",
    r"\bgoogle\b",
    r"\bgithub\b",
    r"\bapi\b",
    r"\bdocs?\b",
    r"\bcurl\b",
    r"https?://",
];

const EMAIL_REQUEST_DETECTION_PATTERNS: &[&str] = &[
    r"\bemail\b",
    r"\bgmail\b",
    r"\binbox\b",
    r"\breply\b",
    r"\bdraft\b",
    r"\bforward\b",
    r"\bmailbox\b",
    r"\boutbox\b",
    r"\bsend\b.+\bemail\b",
    r"\bmessage\b.+\bemail\b",
    r"\bimap\b",
    r"\bsmtp\b",
];

pub fn normalize_request_plane(value: &str) -> Option<RequestPlane> {
    match value.trim().to_ascii_lowercase().as_str() {
        "web" => Some(RequestPlane::Web),
        "email" => Some(RequestPlane::Email),
        "" | "none" => Some(RequestPlane::None),
        _ => None,
    }
}

pub fn default_request_plane() -> RequestPlane {
    std::env::var("NANOCLAW_DEFAULT_REQUEST_PLANE")
        .ok()
        .and_then(|value| normalize_request_plane(&value))
        .unwrap_or(RequestPlane::Web)
}

pub fn get_request_plane_env(request_plane: &RequestPlane) -> [(&'static str, &'static str); 3] {
    match request_plane {
        RequestPlane::Web => [
            (REQUEST_PLANE_ENV_KEY, "web"),
            ("NANOCLAW_WEB_REQUESTS_ENABLED", "1"),
            ("NANOCLAW_EMAIL_REQUESTS_ENABLED", "0"),
        ],
        RequestPlane::Email => [
            (REQUEST_PLANE_ENV_KEY, "email"),
            ("NANOCLAW_WEB_REQUESTS_ENABLED", "0"),
            ("NANOCLAW_EMAIL_REQUESTS_ENABLED", "1"),
        ],
        _ => [
            (REQUEST_PLANE_ENV_KEY, "none"),
            ("NANOCLAW_WEB_REQUESTS_ENABLED", "0"),
            ("NANOCLAW_EMAIL_REQUESTS_ENABLED", "0"),
        ],
    }
}

pub fn detect_request_plane_requirements(text: &str) -> Vec<RequestPlane> {
    let haystack = text.trim();
    if haystack.is_empty() {
        return Vec::new();
    }

    let mut requirements = Vec::new();
    if matches_any(WEB_REQUEST_DETECTION_PATTERNS, haystack) {
        requirements.push(RequestPlane::Web);
    }
    if matches_any(EMAIL_REQUEST_DETECTION_PATTERNS, haystack) {
        requirements.push(RequestPlane::Email);
    }
    requirements
}

pub fn get_request_plane_text_error(text: &str, request_plane: &RequestPlane) -> Option<String> {
    let requirements = detect_request_plane_requirements(text);
    if requirements.is_empty() {
        return None;
    }

    match request_plane {
        RequestPlane::Web => requirements
            .iter()
            .any(|value| matches!(value, RequestPlane::Email))
            .then(|| {
                "This worker is web-scoped and cannot take on email-request work from the task prompt. Use a separate email-scoped process instead.".to_string()
            }),
        RequestPlane::Email => requirements
            .iter()
            .any(|value| matches!(value, RequestPlane::Web))
            .then(|| {
                "This worker is email-scoped and cannot take on web-request work from the task prompt. Use a separate web-scoped process instead.".to_string()
            }),
        _ => {
            if requirements
                .iter()
                .any(|value| matches!(value, RequestPlane::Web))
            {
                Some(
                    "This worker is request-isolated and cannot take on web-request work from the task prompt."
                        .to_string(),
                )
            } else {
                Some(
                    "This worker is request-isolated and cannot take on email-request work from the task prompt."
                        .to_string(),
                )
            }
        }
    }
}

pub fn command_violates_request_plane(
    command: &str,
    request_plane: &RequestPlane,
) -> Option<RequestPlane> {
    let trimmed = command.trim();
    if trimmed.is_empty() {
        return None;
    }

    let matches_web = matches_any(WEB_REQUEST_PATTERNS, trimmed);
    let matches_email = matches_any(EMAIL_REQUEST_PATTERNS, trimmed);

    match request_plane {
        RequestPlane::Web if matches_email => Some(RequestPlane::Email),
        RequestPlane::Email if matches_web => Some(RequestPlane::Web),
        RequestPlane::None if matches_web => Some(RequestPlane::Web),
        RequestPlane::None if matches_email => Some(RequestPlane::Email),
        _ => None,
    }
}

fn matches_any(patterns: &[&str], haystack: &str) -> bool {
    patterns.iter().any(|pattern| {
        Regex::new(&format!("(?i){pattern}"))
            .map(|regex| regex.is_match(haystack))
            .unwrap_or(false)
    })
}

#[cfg(test)]
mod tests {
    use crate::foundation::RequestPlane;

    use super::{
        command_violates_request_plane, detect_request_plane_requirements,
        get_request_plane_text_error,
    };

    #[test]
    fn detects_web_and_email_requirements() {
        let requirements =
            detect_request_plane_requirements("Search GitHub docs and draft an email reply");
        assert!(requirements.contains(&RequestPlane::Web));
        assert!(requirements.contains(&RequestPlane::Email));
    }

    #[test]
    fn rejects_cross_plane_text() {
        let error = get_request_plane_text_error(
            "Draft an email reply to the customer",
            &RequestPlane::Web,
        );
        assert!(error.unwrap().contains("web-scoped"));
    }

    #[test]
    fn rejects_cross_plane_commands() {
        let violation =
            command_violates_request_plane("python3 -c 'import smtplib'", &RequestPlane::Web);
        assert_eq!(violation, Some(RequestPlane::Email));
    }
}
