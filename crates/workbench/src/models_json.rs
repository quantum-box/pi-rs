//! Normalize Pi model-file comments and trailing commas without repairing invalid JSON.

pub(crate) fn normalize(input: &[u8]) -> Vec<u8> {
    let mut output = input.to_vec();
    let mut in_string = false;
    let mut escaped = false;
    let mut in_comment = false;
    let mut previous = None;
    let mut trailing_comma = None;
    for (index, &byte) in input.iter().enumerate() {
        if in_comment {
            if byte == b'\n' {
                in_comment = false;
            } else {
                output[index] = b' ';
            }
        } else if in_string {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                in_string = false;
                previous = Some(b'"');
            }
        } else {
            match byte {
                b'/' if input.get(index + 1) == Some(&b'/') => {
                    in_comment = true;
                    output[index] = b' ';
                }
                b',' => {
                    trailing_comma = matches!(
                        previous,
                        Some(b'"' | b']' | b'}' | b'0'..=b'9' | b'e' | b'l')
                    )
                    .then_some(index);
                    previous = Some(byte);
                }
                b'}' | b']' => {
                    if let Some(comma) = trailing_comma.take() {
                        output[comma] = b' ';
                    }
                    previous = Some(byte);
                }
                b'"' => {
                    in_string = true;
                    previous = None;
                    trailing_comma = None;
                }
                b' ' | b'\t' | b'\n' | b'\r' => {}
                _ => {
                    previous = Some(byte);
                    trailing_comma = None;
                }
            }
        }
    }
    output
}

#[cfg(test)]
mod tests {
    #[test]
    fn normalizes_only_commas_after_values() {
        for input in [
            "{,}",
            "[,]",
            "{ // comment\n ,}",
            "[ // comment\n ,]",
            "{\"x\":[,]}",
            "{\"x\":1,,}",
            "[1,,]",
        ] {
            assert!(
                serde_json::from_slice::<serde_json::Value>(&super::normalize(input.as_bytes()))
                    .is_err(),
                "{input}"
            );
        }
        for input in [
            r#"{"x":[{},[],true,false,null,-1.5e2,"//,]",],}"#,
            "{\"x\": 1, // comment\n }",
        ] {
            assert!(
                serde_json::from_slice::<serde_json::Value>(&super::normalize(input.as_bytes()))
                    .is_ok(),
                "{input}"
            );
        }
    }
}
