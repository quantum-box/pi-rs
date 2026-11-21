use serde_json::Value;

/// Resolve Pi's local JSON Pointer references without opening external schemas.
pub(crate) fn resolve<'a>(root: &'a Value, reference: &str) -> Option<&'a Value> {
    let mut schema = root;
    if reference != "#" {
        for segment in reference
            .strip_prefix("#/")?
            .split('/')
            .filter(|part| !part.is_empty())
        {
            let key = decode_segment(segment)?;
            schema = schema.as_object()?.get(&key)?;
        }
    }
    (schema.is_object() || schema.is_boolean()).then_some(schema)
}

fn decode_segment(segment: &str) -> Option<String> {
    let mut bytes = segment.bytes();
    let mut decoded = Vec::with_capacity(segment.len());
    while let Some(byte) = bytes.next() {
        let decoded_byte = if byte == b'%' {
            let high = char::from(bytes.next()?).to_digit(16)?;
            let low = char::from(bytes.next()?).to_digit(16)?;
            ((high << 4) | low) as u8
        } else {
            byte
        };
        decoded.push(decoded_byte);
    }
    Some(
        String::from_utf8(decoded)
            .ok()?
            .replace("~1", "/")
            .replace("~0", "~"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn resolves_local_schema_pointers_with_only_valid_uri_escapes() {
        let schema = json!({"$defs": {
            "a/b~ c": {"type":"number"},
            "理": false,
            "%XZ": true,
            "notSchema": "ignore"
        }});
        assert_eq!(resolve(&schema, "#"), Some(&schema));
        assert_eq!(
            resolve(&schema, "#/$defs/a~1b~0%20c"),
            Some(&json!({"type":"number"}))
        );
        assert_eq!(resolve(&schema, "#/$defs/%E7%90%86"), Some(&json!(false)));
        for unresolved in [
            "https://example.com/#/$defs/a",
            "#anchor",
            "#/$defs/%XZ",
            "#/$defs/%FF",
            "#/$defs/%",
            "#/$defs/missing",
            "#/$defs/notSchema",
        ] {
            assert_eq!(resolve(&schema, unresolved), None, "{unresolved}");
        }
    }
}
