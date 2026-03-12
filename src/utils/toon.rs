//! Token-Oriented Object Notation (TOON) Formatter
//! 
//! Provides a compact, non-redundant serialization format for LLMs
//! to reduce token consumption compared to standard JSON.

use serde_json::Value;

pub struct ToonFormatter;

impl ToonFormatter {
    /// Convert JSON to compact TOON notation
    pub fn format(value: &Value) -> String {
        Self::format_recursive(value, 0)
    }

    fn format_recursive(value: &Value, indent: usize) -> String {
        let indent_str = "  ".repeat(indent);
        match value {
            Value::Null => "null".to_string(),
            Value::Bool(b) => b.to_string(),
            Value::Number(n) => n.to_string(),
            Value::String(s) => Self::format_string(s, &indent_str),
            Value::Array(arr) => {
                if arr.is_empty() {
                    return "[]".to_string();
                }

                // SOTA: Tabular Array Optimization
                // If all elements are objects with the same keys, use TOON tabular format.
                if let Some(tabular) = Self::try_format_tabular_array(arr, &indent_str) {
                    return tabular;
                }

                // Standard array
                let mut out = String::from("[
");
                for item in arr {
                    out.push_str(&format!("{}  - {}\n", indent_str, Self::format_recursive(item, indent + 1)));
                }
                out.push_str(&format!("{}]", indent_str));
                out
            },
            Value::Object(map) => {
                if map.is_empty() {
                    return "{}".to_string();
                }
                let mut out = String::new();
                for (key, val) in map {
                    out.push_str(&format!("{}{}: {}\n", indent_str, key, Self::format_recursive(val, indent + 1)));
                }
                out.trim_end().to_string()
            }
        }
    }

    fn format_string(s: &str, indent_str: &str) -> String {
        // If the string contains newlines, preserve them in a block-like style.
        if s.contains('\n') {
            format!(
                "|\n{}",
                s.lines()
                    .map(|line| format!("{}  {}", indent_str, line))
                    .collect::<Vec<_>>()
                    .join("\n")
            )
        } else {
            serde_json::to_string(s).unwrap_or_else(|_| format!("\"{}\"", s))
        }
    }

    fn try_format_tabular_array(arr: &[Value], indent_str: &str) -> Option<String> {
        let first = arr.first()?.as_object()?;
        let keys: Vec<&str> = first.keys().map(String::as_str).collect();
        let all_match = arr.iter().all(|value| {
            value
                .as_object()
                .map(|object| {
                    object.len() == keys.len() && keys.iter().all(|key| object.contains_key(*key))
                })
                .unwrap_or(false)
        });

        if !all_match {
            return None;
        }

        let header = format!("[#{} {}:]\n", arr.len(), keys.join(", "));
        let body = arr
            .iter()
            .map(|item| {
                let values = keys
                    .iter()
                    .map(|key| item.get(*key).map(Self::format_tabular_cell).unwrap_or_else(|| "null".to_string()))
                    .collect::<Vec<_>>()
                    .join(" | ");
                format!("{}  - {}", indent_str, values)
            })
            .collect::<Vec<_>>()
            .join("\n");

        Some(format!("{header}{body}"))
    }

    fn format_tabular_cell(value: &Value) -> String {
        match value {
            Value::Null | Value::Bool(_) | Value::Number(_) => value.to_string(),
            Value::String(s) => {
                let should_quote =
                    s.is_empty() || s.contains('\n') || s.contains('|') || s.contains('"') || s.trim() != s;
                if should_quote {
                    serde_json::to_string(s).unwrap_or_else(|_| format!("\"{}\"", s))
                } else {
                    s.clone()
                }
            }
            Value::Array(_) | Value::Object(_) => value.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ToonFormatter;
    use serde_json::json;

    #[test]
    fn formats_tabular_arrays_when_keys_align() {
        let value = json!([
            {"name": "alice", "score": 10},
            {"name": "bob", "score": 20}
        ]);

        let formatted = ToonFormatter::format(&value);

        assert_eq!(
            formatted,
            "[#2 name, score:]\n  - alice | 10\n  - bob | 20"
        );
    }

    #[test]
    fn falls_back_to_standard_arrays_when_shapes_differ() {
        let value = json!([
            {"name": "alice", "score": 10},
            {"name": "bob"}
        ]);

        let formatted = ToonFormatter::format(&value);

        assert!(formatted.starts_with("[\n"));
        assert!(formatted.contains("name: \"alice\""));
        assert!(formatted.contains("name: \"bob\""));
    }

    #[test]
    fn formats_multiline_strings_as_blocks() {
        let value = json!({"msg": "hello\nworld"});

        let formatted = ToonFormatter::format(&value);

        assert!(formatted.contains("msg: |"));
        assert!(formatted.contains("  hello"));
        assert!(formatted.contains("  world"));
    }

    #[test]
    fn formats_tabular_arrays_even_when_key_order_differs() {
        let value = json!([
            {"name": "alice", "score": 10},
            {"score": 20, "name": "bob"}
        ]);

        let formatted = ToonFormatter::format(&value);

        assert_eq!(
            formatted,
            "[#2 name, score:]\n  - alice | 10\n  - bob | 20"
        );
    }

    #[test]
    fn escapes_quotes_in_standard_strings() {
        let value = json!({"msg": "say \"hello\""});

        let formatted = ToonFormatter::format(&value);

        assert!(formatted.contains("msg: \"say \\\"hello\\\"\""));
    }

    #[test]
    fn quotes_unsafe_tabular_cells() {
        let value = json!([
            {"name": "alice|ops", "score": 10},
            {"name": " bob ", "score": 20}
        ]);

        let formatted = ToonFormatter::format(&value);

        assert_eq!(
            formatted,
            "[#2 name, score:]\n  - \"alice|ops\" | 10\n  - \" bob \" | 20"
        );
    }
}
