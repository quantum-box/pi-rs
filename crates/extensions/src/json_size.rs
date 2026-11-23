//! Count the compact serde_json::Value representation without building the value.
use std::collections::HashMap;
use std::io::{self, Write};

use serde::{
    Deserialize, Serialize,
    de::{MapAccess, SeqAccess, Visitor},
};

pub(super) const MAX_NESTED_ARGUMENT_BYTES: usize = 1024 * 1024;

struct Counter(usize);
impl Write for Counter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0 = self.0.saturating_add(bytes.len());
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn encoded_size(value: &impl Serialize) -> usize {
    let mut counter = Counter(0);
    serde_json::to_writer(&mut counter, value).expect("counting writer cannot fail");
    counter.0
}

struct CompactSize(usize);
impl<'de> Deserialize<'de> for CompactSize {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct SizeVisitor;
        impl<'de> Visitor<'de> for SizeVisitor {
            type Value = CompactSize;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a JSON value")
            }
            fn visit_bool<E: serde::de::Error>(self, value: bool) -> Result<Self::Value, E> {
                Ok(CompactSize(encoded_size(&value)))
            }
            fn visit_i64<E: serde::de::Error>(self, value: i64) -> Result<Self::Value, E> {
                Ok(CompactSize(encoded_size(&value)))
            }
            fn visit_u64<E: serde::de::Error>(self, value: u64) -> Result<Self::Value, E> {
                Ok(CompactSize(encoded_size(&value)))
            }
            fn visit_f64<E: serde::de::Error>(self, value: f64) -> Result<Self::Value, E> {
                Ok(CompactSize(encoded_size(&value)))
            }
            fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Self::Value, E> {
                Ok(CompactSize(encoded_size(&value)))
            }
            fn visit_string<E: serde::de::Error>(self, value: String) -> Result<Self::Value, E> {
                self.visit_str(&value)
            }
            fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {
                Ok(CompactSize(4))
            }
            fn visit_none<E: serde::de::Error>(self) -> Result<Self::Value, E> {
                Ok(CompactSize(4))
            }
            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
                let mut size: usize = 2;
                let mut first = true;
                while let Some(CompactSize(child)) = seq.next_element()? {
                    size = size
                        .saturating_add(child)
                        .saturating_add(usize::from(!first));
                    first = false;
                    if size > MAX_NESTED_ARGUMENT_BYTES {
                        while seq.next_element::<serde::de::IgnoredAny>()?.is_some() {}
                        return Ok(CompactSize(MAX_NESTED_ARGUMENT_BYTES + 1));
                    }
                }
                Ok(CompactSize(size))
            }
            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                // Value keeps the last value for duplicate keys.
                let mut entries = HashMap::<String, usize>::new();
                let mut minimum_size: usize = 2;
                while let Some(key) = map.next_key::<String>()? {
                    let CompactSize(value) = map.next_value()?;
                    if !entries.contains_key(&key) {
                        minimum_size = minimum_size
                            .saturating_add(encoded_size(&key))
                            .saturating_add(2)
                            .saturating_add(usize::from(!entries.is_empty()));
                        if minimum_size > MAX_NESTED_ARGUMENT_BYTES {
                            while map
                                .next_entry::<serde::de::IgnoredAny, serde::de::IgnoredAny>()?
                                .is_some()
                            {}
                            return Ok(CompactSize(MAX_NESTED_ARGUMENT_BYTES + 1));
                        }
                    }
                    entries.insert(key, value);
                }
                let mut size: usize = 2;
                for (index, (key, value)) in entries.iter().enumerate() {
                    size = size
                        .saturating_add(encoded_size(key))
                        .saturating_add(1)
                        .saturating_add(*value)
                        .saturating_add(usize::from(index > 0));
                }
                Ok(CompactSize(size))
            }
        }
        deserializer.deserialize_any(SizeVisitor)
    }
}

pub(super) fn compact_size(raw: &str) -> serde_json::Result<usize> {
    serde_json::from_str::<CompactSize>(raw).map(|size| size.0)
}

// When wire JSON is larger than the compact limit, materializing it directly
// can allocate a huge discarded value before a duplicate key replaces it.
// Retain only raw slices for the last value of each key, then normalize those
// surviving values into the already size-checked compact buffer.
pub(super) fn compact_value(raw: &str) -> serde_json::Result<Option<serde_json::Value>> {
    let limit = MAX_NESTED_ARGUMENT_BYTES;
    if compact_size(raw)? > limit {
        return Ok(None);
    }
    if raw.len() <= limit {
        return serde_json::from_str(raw).map(Some);
    }
    let mut output = Vec::with_capacity(limit.min(raw.len()));
    normalize(raw, &mut output)?;
    serde_json::from_slice(&output).map(Some)
}

fn normalize(raw: &str, output: &mut Vec<u8>) -> serde_json::Result<()> {
    use serde_json::value::RawValue;
    struct RawArray<'a>(Vec<&'a RawValue>);
    impl<'de> Deserialize<'de> for RawArray<'de> {
        fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
            struct V;
            impl<'de> Visitor<'de> for V {
                type Value = RawArray<'de>;
                fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    f.write_str("a JSON array")
                }
                fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
                    let mut values = Vec::new();
                    while let Some(value) = seq.next_element::<&'de RawValue>()? {
                        values.push(value);
                    }
                    Ok(RawArray(values))
                }
            }
            deserializer.deserialize_seq(V)
        }
    }
    struct RawObject<'a>(Vec<(String, &'a RawValue)>);
    impl<'de> Deserialize<'de> for RawObject<'de> {
        fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
            struct V;
            impl<'de> Visitor<'de> for V {
                type Value = RawObject<'de>;
                fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    f.write_str("a JSON object")
                }
                fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                    let mut positions = HashMap::<String, usize>::new();
                    let mut values: Vec<(String, &'de RawValue)> = Vec::new();
                    while let Some(key) = map.next_key::<String>()? {
                        let value = map.next_value::<&'de RawValue>()?;
                        if let Some(index) = positions.get(&key) {
                            values[*index].1 = value;
                        } else {
                            positions.insert(key.clone(), values.len());
                            values.push((key, value));
                        }
                    }
                    Ok(RawObject(values))
                }
            }
            deserializer.deserialize_map(V)
        }
    }
    match raw.as_bytes().first() {
        Some(b'[') => {
            let RawArray(values) = serde_json::from_str(raw)?;
            output.push(b'[');
            for (index, value) in values.iter().enumerate() {
                if index > 0 {
                    output.push(b',');
                }
                normalize(value.get(), output)?;
            }
            output.push(b']');
        }
        Some(b'{') => {
            let RawObject(values) = serde_json::from_str(raw)?;
            output.push(b'{');
            for (index, (key, value)) in values.iter().enumerate() {
                if index > 0 {
                    output.push(b',');
                }
                serde_json::to_writer(&mut *output, key)?;
                output.push(b':');
                normalize(value.get(), output)?;
            }
            output.push(b'}');
        }
        _ => {
            let value: serde_json::Value = serde_json::from_str(raw)?;
            serde_json::to_writer(output, &value)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn matches_value_reserialization() {
        for text in [
            r#" { "a" : ["\u0061","\u0000", 1e3, -0.0, true, null] } "#,
            r#"{"a":[1,2,3],"b":{},"a":"last"}"#,
            r#"[1.2300,1E-9,-1,0,18446744073709551615]"#,
        ] {
            let value: serde_json::Value = serde_json::from_str(text).unwrap();
            assert_eq!(super::compact_size(text).unwrap(), value.to_string().len());
        }
    }
}
