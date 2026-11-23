//! Pi/V8 syntax diagnostics for complete JSON input.
use serde_json::Value;

fn json_number_error(line: &str, error: &serde_json::Error, message: &str) -> Option<String> {
    // EOF already points after the last byte. For an invalid number with a
    // following character, serde points at that character in one-based columns.
    let end = if error.is_eof() && message.starts_with("EOF while parsing a value") {
        let offset = serde_error_offset(line, error)?;
        (offset == line.len()).then_some(offset)?
    } else if message.starts_with("invalid number") {
        serde_error_offset(line, error)?.checked_sub(1)?
    } else {
        return None;
    };
    let prefix = line.get(..end)?;
    let bytes = prefix.as_bytes();
    let mantissa = if matches!(bytes.last(), Some(b'+' | b'-')) {
        &bytes[..bytes.len() - 1]
    } else {
        bytes
    };
    let reason = match bytes {
        [.., digit, b'.'] if digit.is_ascii_digit() => "Unterminated fractional number in JSON",
        _ if matches!(mantissa, [.., digit, b'e' | b'E'] if digit.is_ascii_digit()) => {
            "Exponent part is missing a number in JSON"
        }
        [.., b'-'] => "No number after minus sign in JSON",
        [.., b'0'] if line.as_bytes().get(end).is_some_and(u8::is_ascii_digit) => {
            "Unexpected number in JSON"
        }
        _ => return None,
    };
    Some(json_error_at(reason, prefix))
}

/// Format a JSON parsing error using the corresponding Pi/V8 diagnostic
/// where supported, falling back to the original serde diagnostic.
pub fn json_syntax_diagnostic(line: &str, error: &serde_json::Error) -> String {
    let message = error.to_string();
    if let Some(diagnostic) = json_number_error(line, error, &message) {
        return diagnostic;
    }
    if let Some((offset, start)) = json_string_control_offset(line)
        && (message.starts_with("control character")
            || ((message.starts_with("lone leading surrogate in hex escape")
                || message.starts_with("unexpected end of hex escape"))
                && invalid_json_escape(line).is_none()
                && serde_error_offset(line, error)
                    .is_some_and(|position| position >= start && position <= offset + 1))
            || surrogate_retry_control(line, error, offset))
    {
        return json_error_at(
            "Bad control character in string literal in JSON",
            &line[..offset],
        );
    }
    if (message.starts_with("invalid escape")
        || error.is_eof() && message.starts_with("EOF while parsing a string"))
        && let Some((reason, offset)) = invalid_json_escape(line)
    {
        return json_error_at(reason, &line[..offset]);
    }
    if error.is_eof() && message.starts_with("EOF while parsing a string") {
        match unfinished_json_escape(line) {
            Some(false) => return "Unexpected end of JSON input".into(),
            Some(true) => return json_error_at("Bad Unicode escape in JSON", line),
            None => {}
        }
    }
    let surrogate_eof = (message.starts_with("lone leading surrogate in hex escape")
        || message.starts_with("unexpected end of hex escape"))
        && unterminated_surrogate_string(line, error);
    if !error.is_eof() && !surrogate_eof {
        return json_structure_error(line, error, &message).unwrap_or(message);
    }
    let tail = line.trim_end_matches([' ', '\t', '\n', '\r']);
    // The remaining unterminated-string case has complete escapes; incomplete
    // escapes above use different V8 diagnostics.
    let reason = if surrogate_eof
        || message.starts_with("EOF while parsing a string") && complete_json_escapes(line)
    {
        Some("Unterminated string in JSON")
    } else if message.starts_with("EOF while parsing a list") {
        if tail.ends_with('[') {
            return "Unexpected end of JSON input".into();
        }
        Some("Expected ',' or ']' after array element in JSON")
    } else if message.starts_with("EOF while parsing an object") && tail.ends_with('{') {
        Some("Expected property name or '}' in JSON")
    } else if let Some(diagnostic) = incomplete_json_structure(line, tail, &message) {
        return diagnostic;
    } else if message.starts_with("EOF while parsing a value")
        && (tail.is_empty()
            || tail.ends_with(':')
            || tail.ends_with('[')
            || tail.ends_with(',') && tail.starts_with('[') && !tail.contains('{')
            || ["t", "tr", "tru", "f", "fa", "fal", "fals", "n", "nu", "nul"]
                .iter()
                .any(|word| tail.ends_with(word)))
    {
        return "Unexpected end of JSON input".into();
    } else {
        None
    };
    reason.map_or(message, |reason| json_error_at(reason, line))
}

fn incomplete_json_structure(line: &str, tail: &str, message: &str) -> Option<String> {
    let in_object = message.starts_with("EOF while parsing an object");
    let after_comma = message.starts_with("EOF while parsing a value") && tail.ends_with(',');
    if line.len() > JSON_DIAGNOSTIC_RETRY_LIMIT || !(in_object || after_comma) {
        return None;
    }

    // The closing brace distinguishes a missing colon from a missing object
    // delimiter. After a comma, it distinguishes an object key from an array
    // element even when requests contain nested objects and arrays.
    let mut probe = String::with_capacity(line.len() + 1);
    probe.push_str(line);
    probe.push('}');
    let separator = || json_error_at("Expected ',' or '}' after property value in JSON", line);
    match serde_json::from_str::<Value>(&probe) {
        Ok(_) if in_object => Some(separator()),
        Err(error) if in_object && error.is_eof() => Some(separator()),
        Err(error) if serde_error_offset(&probe, &error) == Some(probe.len()) => {
            let diagnostic = error.to_string();
            match diagnostic.split_once(" at line ")?.0 {
                "expected `:`" if in_object => Some(json_error_at(
                    "Expected ':' after property name in JSON",
                    line,
                )),
                "trailing comma" if after_comma => Some(json_error_at(
                    "Expected double-quoted property name in JSON",
                    line,
                )),
                "expected value" if after_comma => Some("Unexpected end of JSON input".into()),
                _ => None,
            }
        }
        _ => None,
    }
}

fn json_structure_error(line: &str, error: &serde_json::Error, message: &str) -> Option<String> {
    // For these syntax errors, serde reports the one-based byte column of the
    // unexpected character. Pi reports zero-based UTF-16 positions.
    let offset = serde_error_offset(line, error)?.checked_sub(1)?;
    let prefix = line.get(..offset)?;
    let reason = match message.split_once(" at line ")?.0 {
        "expected `:`" => "Expected ':' after property name in JSON",
        "expected `,` or `]`" => "Expected ',' or ']' after array element in JSON",
        "expected `,` or `}`" => "Expected ',' or '}' after property value in JSON",
        "trailing characters" => "Unexpected non-whitespace character after JSON",
        "key must be a string"
            if prefix
                .trim_end_matches([' ', '\t', '\r', '\n'])
                .ends_with('{') =>
        {
            "Expected property name or '}' in JSON"
        }
        "key must be a string" => "Expected double-quoted property name in JSON",
        "trailing comma" if line.as_bytes().get(offset) == Some(&b'}') => {
            "Expected double-quoted property name in JSON"
        }
        "expected value" | "expected ident" => return unexpected_token(line, offset),
        "trailing comma" if line.as_bytes().get(offset) == Some(&b']') => {
            return unexpected_token(line, offset);
        }
        _ => return None,
    };
    Some(json_error_at(reason, prefix))
}

fn unexpected_token(line: &str, offset: usize) -> Option<String> {
    if matches!(line, "undefined" | "NaN" | "Infinity" | "[object Object]") {
        return Some(format!(r#""{line}" is not valid JSON"#));
    }
    let token = line.get(offset..)?.chars().next()?;
    if line.encode_utf16().count() <= 20 {
        return Some(format!(
            r#"Unexpected token '{token}', "{line}" is not valid JSON"#
        ));
    }

    let position = line[..offset].encode_utf16().count();
    let start = position.saturating_sub(10);
    let mut units = line.encode_utf16().skip(start);
    let excerpt: Vec<u16> = units.by_ref().take(position + 10 - start).collect();
    let before = if position < 10 { "" } else { "..." };
    let after = if units.next().is_some() { "..." } else { "" };
    let excerpt = String::from_utf16_lossy(&excerpt);
    Some(format!(
        r#"Unexpected token '{token}', {before}"{excerpt}"{after} is not valid JSON"#
    ))
}

fn json_error_at(reason: &str, prefix: &str) -> String {
    let position = prefix.encode_utf16().count();
    let row = prefix.bytes().filter(|byte| *byte == b'\r').count()
        + prefix.bytes().filter(|byte| *byte == b'\n').count()
        - prefix.matches("\r\n").count()
        + 1;
    let column = prefix
        .rsplit(['\r', '\n'])
        .next()
        .unwrap_or_default()
        .encode_utf16()
        .count()
        + 1;
    format!("{reason} at position {position} (line {row} column {column})")
}

/// Maximum input size for diagnostic retries on untrusted JSON.
pub const JSON_DIAGNOSTIC_RETRY_LIMIT: usize = 1024 * 1024;

fn surrogate_retry_control(line: &str, error: &serde_json::Error, offset: usize) -> bool {
    let message = error.to_string();
    if line.len() > JSON_DIAGNOSTIC_RETRY_LIMIT
        || !(message.starts_with("lone leading surrogate in hex escape")
            || message.starts_with("unexpected end of hex escape"))
    {
        return false;
    }
    let bytes = line.as_bytes();
    let mut sanitized: Option<Vec<u8>> = None;
    let mut in_string = false;
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'"' => {
                in_string = !in_string;
                i += 1;
            }
            b'\\' if in_string => {
                if bytes.get(i + 1) == Some(&b'u')
                    && let Some(hex) = bytes.get(i + 2..i + 6)
                    && hex.iter().all(u8::is_ascii_hexdigit)
                {
                    let code = hex.iter().fold(0u16, |value, digit| {
                        (value << 4) | u16::from((*digit as char).to_digit(16).unwrap() as u8)
                    });
                    if (0xD800..=0xDFFF).contains(&code) {
                        let copy = sanitized.get_or_insert_with(|| bytes.to_vec());
                        copy[i + 2..i + 6].copy_from_slice(b"FFFD");
                    }
                    i += 6;
                } else {
                    // Escaped backslashes and quotes must not start new escapes
                    // or strings. Invalid escapes remain for serde to diagnose.
                    i += 2;
                }
            }
            _ => i += 1,
        }
    }
    let Some(sanitized) = sanitized else {
        return false;
    };
    let Err(retry) = serde_json::from_slice::<Value>(&sanitized) else {
        return false;
    };
    retry.to_string().starts_with("control character")
        && serde_error_offset(line, &retry) == Some(offset + 1)
}

fn serde_error_offset(line: &str, error: &serde_json::Error) -> Option<usize> {
    let row = error.line().checked_sub(1)?;
    let offset: usize = line.split_inclusive('\n').take(row).map(str::len).sum();
    Some(offset.saturating_add(error.column()))
}

fn json_string_control_offset(line: &str) -> Option<(usize, usize)> {
    let mut start = None;
    let mut escaped = false;
    for (offset, character) in line.char_indices() {
        if escaped {
            escaped = false;
        } else if start.is_some() && character == '\\' {
            escaped = true;
        } else if character == '"' {
            start = if start.is_some() { None } else { Some(offset) };
        } else if let Some(start) = start
            && character < ' '
        {
            return Some((offset, start));
        }
    }
    None
}

// Locate the first malformed escape without interpreting subsequent text.
// The caller only uses this after serde reports an invalid escape or an
// unfinished string, so preceding structural errors retain their diagnostics.
fn invalid_json_escape(line: &str) -> Option<(&'static str, usize)> {
    let mut chars = line.char_indices();
    let mut in_string = false;
    while let Some((_, ch)) = chars.next() {
        match ch {
            '"' => in_string = !in_string,
            '\\' if in_string => match chars.next() {
                Some((_, 'u')) => {
                    for _ in 0..4 {
                        match chars.next() {
                            Some((_, hex)) if hex.is_ascii_hexdigit() => {}
                            Some((offset, _)) => {
                                return Some(("Bad Unicode escape in JSON", offset));
                            }
                            None => return None,
                        }
                    }
                }
                Some((_, '"' | '\\' | '/' | 'b' | 'f' | 'n' | 'r' | 't')) => {}
                Some((offset, _)) => return Some(("Bad escaped character in JSON", offset)),
                None => return None,
            },
            ch if in_string && ch < ' ' => return None,
            _ => {}
        }
    }
    None
}

// Identify only an incomplete escape inside an open string. Earlier invalid
// escapes and control characters must retain their own diagnostics.
fn unfinished_json_escape(line: &str) -> Option<bool> {
    let mut chars = line.chars();
    let mut in_string = false;
    while let Some(ch) = chars.next() {
        match ch {
            '"' => in_string = !in_string,
            '\\' if in_string => match chars.next() {
                None => return Some(false),
                Some('u') => {
                    for _ in 0..4 {
                        match chars.next() {
                            None => return Some(true),
                            Some(hex) if hex.is_ascii_hexdigit() => {}
                            Some(_) => return None,
                        }
                    }
                }
                Some('"' | '\\' | '/' | 'b' | 'f' | 'n' | 'r' | 't') => {}
                Some(_) => return None,
            },
            ch if in_string && ch < ' ' => return None,
            _ => {}
        }
    }
    None
}

// serde rejects lone surrogate escapes that V8 permits in JSON strings. Only
// project its diagnostic when that same string is still open and its escape
// syntax is complete; errors after a closed string must remain unchanged.
fn unterminated_surrogate_string(line: &str, error: &serde_json::Error) -> bool {
    let Some(error_line) = error.line().checked_sub(1) else {
        return false;
    };
    let mut lines = line.split_inclusive('\n');
    let offset: usize = lines.by_ref().take(error_line).map(str::len).sum();
    let error_offset = offset.saturating_add(error.column());
    let bytes = line.as_bytes();
    let mut index = 0;
    let mut start = None;
    let mut surrogate = None;
    while index < bytes.len() {
        match bytes[index] {
            b'"' => {
                if start.is_some() {
                    start = None;
                    surrogate = None;
                } else {
                    start = Some(index);
                }
                index += 1;
            }
            b'\\' if start.is_some() => {
                let Some(&escape) = bytes.get(index + 1) else {
                    return false;
                };
                if escape == b'u' {
                    let Some(hex) = bytes.get(index + 2..index + 6) else {
                        return false;
                    };
                    if !hex.iter().all(u8::is_ascii_hexdigit) {
                        return false;
                    }
                    let code =
                        u16::from_str_radix(std::str::from_utf8(hex).unwrap_or_default(), 16)
                            .unwrap_or_default();
                    if (0xD800..=0xDFFF).contains(&code) {
                        surrogate.get_or_insert(index);
                    }
                    index += 6;
                } else if b"\"\\/bfnrt".contains(&escape) {
                    index += 2;
                } else {
                    return false;
                }
            }
            byte if start.is_some() && byte < 0x20 => return false,
            _ => index += 1,
        }
    }
    start.is_some_and(|start| start < error_offset)
        && surrogate.is_some_and(|surrogate| surrogate < error_offset)
}

fn complete_json_escapes(line: &str) -> bool {
    let mut chars = line.chars();
    let mut in_string = false;
    while let Some(ch) = chars.next() {
        match ch {
            '"' => in_string = !in_string,
            '\\' if in_string => match chars.next() {
                None => return false,
                Some('u') => {
                    for _ in 0..4 {
                        if chars.next().is_none() {
                            return false;
                        }
                    }
                }
                Some(_) => {}
            },
            _ => {}
        }
    }
    true
}
