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

/// Progress deltas additionally descend through bounded object paths and use
/// Chord's string append operation when the new text extends the old text.
pub(crate) fn progress_diff(previous: &Value, current: &Value) -> Vec<Value> {
    fn walk(
        old: Option<&Value>,
        new: Option<&Value>,
        path: &mut Vec<String>,
        ops: &mut Vec<Value>,
    ) -> bool {
        if old == new {
            return true;
        }
        if ops.len() >= 128 || path.len() > 8 {
            return false;
        }
        if let (Some(Value::Object(a)), Some(Value::Object(b))) = (old, new) {
            let keys: BTreeSet<_> = a.keys().chain(b.keys()).collect();
            for key in keys {
                if a.get(key) == b.get(key) {
                    continue;
                }
                if matches!(key.as_str(), "__proto__" | "constructor" | "prototype") {
                    return false;
                }
                path.push(key.clone());
                let valid = walk(a.get(key), b.get(key), path, ops);
                path.pop();
                if !valid {
                    return false;
                }
            }
            return true;
        }
        if path.is_empty() {
            return false;
        }
        match (old, new) {
            (_, None) => ops.push(json!(["d", path])),
            (Some(Value::String(a)), Some(Value::String(b))) if b.starts_with(a) => {
                ops.push(json!(["a", path, &b[a.len()..]]));
            }
            (Some(Value::Array(a)), Some(Value::Array(b))) if b.starts_with(a) => {
                ops.push(json!(["p", path, a.len(), 0, &b[a.len()..]]));
            }
            (_, Some(value)) => ops.push(json!(["s", path, value])),
        }
        true
    }
    let mut ops = Vec::new();
    if !walk(Some(previous), Some(current), &mut Vec::new(), &mut ops) {
        return replacement(current);
    }
    let root = replacement(current);
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
    fn progress_nested_ops_and_fallbacks_are_bounded() {
        let old = json!({"kind":"generation","state":{"partial":{"text":"x".repeat(3000),"items":[1]},"state":"running"}});
        let mut new = old.clone();
        new["state"]["partial"]["text"] = json!(format!("{}🚀", "x".repeat(3000)));
        new["state"]["partial"]["items"] = json!([1, 2]);
        let ops = progress_diff(&old, &new);
        assert!(ops.contains(&json!(["a", ["state", "partial", "text"], "🚀"])));
        assert!(ops.contains(&json!(["p", ["state", "partial", "items"], 1, 0, [2]])));
        let mut unsafe_value = new.clone();
        unsafe_value["state"]["partial"]["__proto__"] = json!(1);
        assert_eq!(
            progress_diff(&new, &unsafe_value),
            replacement(&unsafe_value)
        );
        let mut deep = json!(0);
        for _ in 0..9 {
            deep = json!({"x":deep});
        }
        let deep_new = json!({"x":deep});
        let deep_old = json!({"x":{"x":{"x":{"x":{"x":{"x":{"x":{"x":{"x":{"x":1}}}}}}}}}});
        assert_eq!(progress_diff(&deep_old, &deep_new), replacement(&deep_new));
    }
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
