use crate::algorithm::EffortLevel;
use regex::RegexSet;
use std::sync::OnceLock;

pub struct EffortClassifier;

const QUICK_PATTERNS: &[&str] = &[
    r"(?i)quick",
    r"(?i)simple",
    r"(?i)typo",
    r"(?i)just",
];

const THOROUGH_PATTERNS: &[&str] = &[
    r"(?i)thorough",
    r"(?i)comprehensive",
    r"(?i)refactor",
    r"(?i)architect",
];

const DETERMINED_PATTERNS: &[&str] = &[
    r"(?i)until done",
    r"(?i)don't stop",
    r"(?i)whatever it takes",
];

fn compile_pattern_set(patterns: &[&str], label: &str) -> RegexSet {
    RegexSet::new(patterns).unwrap_or_else(|err| panic!("Failed to compile {label} patterns: {err}"))
}

impl EffortClassifier {
    pub fn new() -> Self {
        Self
    }

    pub fn classify(&self, request: &str) -> EffortLevel {
        static QUICK_SET: OnceLock<RegexSet> = OnceLock::new();
        static THOROUGH_SET: OnceLock<RegexSet> = OnceLock::new();
        static DETERMINED_SET: OnceLock<RegexSet> = OnceLock::new();

        let quick = QUICK_SET.get_or_init(|| compile_pattern_set(QUICK_PATTERNS, "quick"));
        let thorough = THOROUGH_SET.get_or_init(|| compile_pattern_set(THOROUGH_PATTERNS, "thorough"));
        let determined = DETERMINED_SET.get_or_init(|| compile_pattern_set(DETERMINED_PATTERNS, "determined"));

        if determined.is_match(request) {
            return EffortLevel::Determined;
        }

        if thorough.is_match(request) {
            return EffortLevel::Thorough;
        }

        if quick.is_match(request) {
            return EffortLevel::Quick;
        }

        EffortLevel::Standard
    }
}

impl Default for EffortClassifier {
    fn default() -> Self {
        Self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_case_insensitively() {
        let classifier = EffortClassifier::new();
        assert_eq!(classifier.classify("Need a QUICK fix"), EffortLevel::Quick);
    }

    #[test]
    fn applies_determined_precedence() {
        let classifier = EffortClassifier::new();
        assert_eq!(
            classifier.classify("Do a thorough rewrite and keep going until done"),
            EffortLevel::Determined
        );
    }
}
