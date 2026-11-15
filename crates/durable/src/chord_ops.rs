//! Bounded, conservative Chord operations derived from complete JSON states.
use serde_json::{Value, json};
use std::collections::BTreeSet;

pub(crate) fn replacement(value: &Value) -> Vec<Value> {
    vec![json!(["r", value])]
}

pub(crate) fn diff(previous: &Value, current: &Value) -> Vec<Value> {
    let (Some(old), Some(new)) = (previous.as_object(), current.as_object()) else {
        return replacement(current);
    };
    let keys: BTreeSet<_> = old.keys().chain(new.keys()).collect();
    let mut ops = Vec::new();
    for key in keys {
        let (before, after) = (old.get(key), new.get(key));
        if before == after {
            continue;
        }
        if matches!(key.as_str(), "__proto__" | "constructor" | "prototype") || ops.len() >= 128 {
            return replacement(current);
        }
        match (before, after) {
            (_, None) => ops.push(json!(["d", [key]])),
            (Some(Value::Array(a)), Some(Value::Array(b))) if b.starts_with(a) => {
                ops.push(json!(["p", [key], a.len(), 0, &b[a.len()..]]));
            }
            (_, Some(value)) => ops.push(json!(["s", [key], value])),
        }
    }
    let root = replacement(current);
    // A replacement is simpler when the delta is no smaller.
    if serde_json::to_vec(&ops).map_or(true, |bytes| {
        bytes.len() >= serde_json::to_vec(&root).map_or(0, |v| v.len())
    }) {
        root
    } else {
        ops
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn unsafe_paths_and_excessive_changes_use_replacement() {
        let old = json!({"__proto__":1,"stable":"x".repeat(3000)});
        let new = json!({"__proto__":2,"stable":"x".repeat(3000)});
        assert_eq!(diff(&old, &new), replacement(&new));
        let old = Value::Object((0..130).map(|i| (format!("k{i}"), json!(0))).collect());
        let new = Value::Object((0..130).map(|i| (format!("k{i}"), json!(1))).collect());
        assert_eq!(diff(&old, &new), replacement(&new));
        assert_eq!(diff(&json!(1), &json!(2)), replacement(&json!(2)));
    }
}
