//! Parse complete provider tool arguments, repairing only string escapes.
use serde_json::{Map, Value};

use crate::json_syntax_diagnostic;

/// Parse a completed tool call. The returned object is separate from the raw
/// journaled model response. Invalid or incomplete documents are never repaired
/// by adding members, values, or closing delimiters.
pub fn parse_tool_arguments(raw: &str) -> std::result::Result<Value, String> {
    if raw.trim().is_empty() {
        return Ok(Value::Object(Map::new()));
    }
    let value = match serde_json::from_str::<Value>(raw) {
        Ok(value) => Ok(value),
        Err(error) => {
            let repaired = repair_string_escapes(raw);
            if repaired == raw {
                Err(json_syntax_diagnostic(raw, &error))
            } else {
                // Pi 1.6 reports positions in its second parse and echoes the
                // original provider text under Received arguments.
                serde_json::from_str(&repaired)
                    .map_err(|error| json_syntax_diagnostic(&repaired, &error))
            }
        }
    };
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
            '\u{8}' => result.push_str(r"\b"),
            '\u{c}' => result.push_str(r"\f"),
            '\n' => result.push_str(r"\n"),
            '\r' => result.push_str(r"\r"),
            '\t' => result.push_str(r"\t"),
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

    #[test]
    fn reports_pi_diagnostics_for_malformed_arguments() {
        for (raw, expected) in [
            (r#"[]"#, r#"Tool arguments must be a JSON object"#),
            (r#""str""#, r#"Tool arguments must be a JSON object"#),
            (r#"null"#, r#"Tool arguments must be a JSON object"#),
            (r#"7"#, r#"Tool arguments must be a JSON object"#),
            (
                r#"{"path":"unfinished"#,
                r#"Unterminated string in JSON at position 19 (line 1 column 20)"#,
            ),
            (r#"{"path":"#, r#"Unexpected end of JSON input"#),
            (
                r#"{"path""#,
                r#"Expected ':' after property name in JSON at position 7 (line 1 column 8)"#,
            ),
            (
                r#"{"path":"x""#,
                r#"Expected ',' or '}' after property value in JSON at position 11 (line 1 column 12)"#,
            ),
            (
                r#"{"path":"ok",}"#,
                r#"Expected double-quoted property name in JSON at position 13 (line 1 column 14)"#,
            ),
            (
                r#"{"path" "ok"}"#,
                r#"Expected ':' after property name in JSON at position 8 (line 1 column 9)"#,
            ),
            (
                r#"{"path":01}"#,
                r#"Unexpected number in JSON at position 9 (line 1 column 10)"#,
            ),
            (
                r#"{"path":"ok"}tail"#,
                r#"Unexpected non-whitespace character after JSON at position 13 (line 1 column 14)"#,
            ),
            (
                r#"{unquoted:1}"#,
                r#"Expected property name or '}' in JSON at position 1 (line 1 column 2)"#,
            ),
            (
                r#"{"path":[1,]}"#,
                r#"Unexpected token ']', "{"path":[1,]}" is not valid JSON"#,
            ),
            (
                r#"{"path":?}"#,
                r#"Unexpected token '?', "{"path":?}" is not valid JSON"#,
            ),
            (
                r#"{"abcdef":?         }"#,
                r#"Unexpected token '?', ..."{"abcdef":?         "... is not valid JSON"#,
            ),
            (
                r#"{"path":"C:\work",}"#,
                "Expected double-quoted property name in JSON at position 19 (line 1 column 20)",
            ),
            (
                "{\"x\":\"a\nb\",}",
                "Expected double-quoted property name in JSON at position 12 (line 1 column 13)",
            ),
        ] {
            assert_eq!(
                parse_tool_arguments(raw).unwrap_err(),
                format!("{expected}\n\nReceived arguments:\n{raw}"),
                "{raw}"
            );
        }
    }
}
