use serde_json::Value;

/// Resolve local JSON Pointer references without opening external schemas.
pub(crate) fn resolve<'a>(root: &'a Value, reference: &str) -> Option<&'a Value> {
    let fragment = reference.strip_prefix('#')?;
    let pointer = decode_fragment(fragment)?;
    if let Some(schema) = root
        .pointer(&pointer)
        .filter(|schema| schema.is_object() || schema.is_boolean())
    {
        return Some(schema);
    }

    // Pi also accepts a percent-encoded slash inside an object key.
    let mut schema = root;
    for segment in fragment.strip_prefix('/')?.split('/') {
        let key = decode_fragment(segment)?
            .replace("~1", "/")
            .replace("~0", "~");
        schema = schema.as_object()?.get(&key)?;
    }
    (schema.is_object() || schema.is_boolean()).then_some(schema)
}

fn decode_fragment(fragment: &str) -> Option<String> {
    let mut bytes = fragment.bytes();
    let mut decoded = Vec::with_capacity(fragment.len());
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
    String::from_utf8(decoded).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn resolves_local_schema_pointers_with_only_valid_uri_escapes() {
        let schema = json!({
            "$defs": {
                "": true,
                "a/b~ c": {"type":"number"},
                "a/b": false,
                "理": false,
                "%XZ": true,
                "notSchema": "ignore"
            },
            "oneOf": [{"type":"string"}]
        });
        assert_eq!(resolve(&schema, "#"), Some(&schema));
        assert_eq!(
            resolve(&schema, "#/$defs/a~1b~0%20c"),
            Some(&json!({"type":"number"}))
        );
        assert_eq!(resolve(&schema, "#/$defs/%E7%90%86"), Some(&json!(false)));
        assert_eq!(resolve(&schema, "#/$defs/"), Some(&json!(true)));
        assert_eq!(resolve(&schema, "#/$defs/a%2Fb"), Some(&json!(false)));
        assert_eq!(
            resolve(&schema, "#%2FoneOf%2F0"),
            Some(&json!({"type":"string"}))
        );
        for unresolved in [
            "https://example.com/#/$defs/a",
            "#anchor",
            "#/$defs/%XZ",
            "#/$defs/%FF",
            "#/$defs/%",
            "#/$defs/missing",
            "#/$defs/notSchema",
            "#/oneOf/00",
        ] {
            assert_eq!(resolve(&schema, unresolved), None, "{unresolved}");
        }
    }
}
