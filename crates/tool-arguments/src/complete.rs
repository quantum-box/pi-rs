//! Parse complete provider tool arguments, repairing only string escapes.
use serde_json::{Map, Value};

/// Parse a completed tool call. The returned object is separate from the raw
/// journaled model response. Invalid or incomplete documents are never repaired
/// by adding members, values, or closing delimiters.
pub fn parse_tool_arguments(raw: &str) -> std::result::Result<Value, String> {
    if raw.trim().is_empty() {
        return Ok(Value::Object(Map::new()));
    }
    let value = serde_json::from_str::<Value>(raw)
        .or_else(|_| serde_json::from_str::<Value>(&repair_string_escapes(raw)));
    match value {
        Ok(value @ Value::Object(_)) => Ok(value),
        Ok(_) => Err(format!(
            "Tool arguments must be a JSON object\n\nReceived arguments:\n{raw}"
        )),
        Err(error) => Err(format!("{error}\n\nReceived arguments:\n{raw}")),
    }
}

fn repair_string_escapes(raw: &str) -> String {
    let mut result = String::with_capacity(raw.len());
    let mut chars = raw.chars().peekable();
    let mut in_string = false;
    while let Some(character) = chars.next() {
        if !in_string {
            result.push(character);
            if character == '"' {
                in_string = true;
            }
            continue;
        }
        match character {
            '"' => {
                result.push(character);
                in_string = false;
            }
            '\\' => {
                let valid = match chars.peek().copied() {
                    Some('"' | '\\' | '/' | 'b' | 'f' | 'n' | 'r' | 't') => true,
                    Some('u') => {
                        let mut digits = chars.clone().skip(1);
                        (0..4).all(|_| digits.next().is_some_and(|c| c.is_ascii_hexdigit()))
                    }
                    _ => false,
                };
                if valid {
                    result.push('\\');
                    result.push(chars.next().expect("valid escape"));
                    // Unicode digits are ordinary characters on subsequent iterations.
                } else {
                    result.push_str("\\\\");
                }
            }
            c if c <= '\u{1f}' => {
                use std::fmt::Write as _;
                let _ = write!(result, "\\u{:04x}", c as u32);
            }
            c => result.push(c),
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn repairs_only_invalid_escapes_and_raw_controls_in_strings() {
        assert_eq!(
            parse_tool_arguments(r#"{"path":"C:\work\document","content":"\url{x}"}"#).unwrap(),
            json!({"path":r"C:\work\document","content":r"\url{x}"})
        );
        assert_eq!(
            parse_tool_arguments("{\"text\":\"a\nb\\t\\u65e5\"}").unwrap(),
            json!({"text":"a\nb\t日"})
        );
        assert_eq!(
            parse_tool_arguments(r#"{"text":"\uZZZZ"}"#).unwrap(),
            json!({"text":r"\uZZZZ"})
        );
        assert_eq!(parse_tool_arguments("  ").unwrap(), json!({}));
    }
    #[test]
    fn rejects_incomplete_documents_and_non_objects_with_original_input() {
        for raw in [r#"{"path":"unfinished"#, "[1]", "null", "42", r#"{"x":}"#] {
            let error = parse_tool_arguments(raw).unwrap_err();
            assert!(error.contains("Received arguments:"));
            assert!(error.ends_with(raw));
        }
    }
}
