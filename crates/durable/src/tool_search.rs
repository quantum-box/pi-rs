//! Opt-in durable deferred tool discovery. Ranking is adapted from pi tool search.
//! Copyright (c) 2025 Mario Zechner. See ../../../crates/extensions/PI-LICENSE.
use crate::{
    extension::Extension,
    keyed_tool::{KeyedHost, KeyedTool, KeyedToolAdapter},
    recovery::ReplayPolicy,
    reset::ToolControl,
};
use pi_rs_core::{ExtensionState, Outcome, Tool, ToolContext, ToolReply, ToolSpec};
use regex::Regex;
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, LazyLock, Mutex, OnceLock, Weak};
use unicode_normalization::UnicodeNormalization;
use unicode_segmentation::UnicodeSegmentation;

pub const NAME: &str = "tool_search";
pub const EXTENSION_NAME: &str = "tool-search";
const MAX_LIMIT: u64 = 128;
const MAX_QUERY_BYTES: usize = 4096;
const MAX_CANDIDATES: usize = 256;
const MAX_METADATA_BYTES: usize = 1_048_576;

#[derive(Clone)]
pub struct SearchDocument {
    pub tool: ToolSpec,
    pub namespace: String,
}
static TOOLS: OnceLock<Mutex<Vec<Weak<dyn Tool>>>> = OnceLock::new();
tokio::task_local! { static CANDIDATES: Vec<SearchDocument>; }

/// Install and select this extension on a conversation to offer `tool_search`.
pub fn extension() -> Extension {
    let tool = Arc::new(KeyedToolAdapter::new(SearchTool))
        .register(ReplayPolicy::Safe)
        .tool;
    let mut tools = TOOLS
        .get_or_init(Default::default)
        .lock()
        .expect("search tool registry poisoned");
    tools.retain(|entry| entry.strong_count() > 0);
    tools.push(Arc::downgrade(&tool));
    Extension {
        name: EXTENSION_NAME.into(),
        tools: vec![tool],
        ..Default::default()
    }
}
pub(crate) fn is_search(tool: &Arc<dyn Tool>) -> bool {
    let Some(entries) = TOOLS.get() else {
        return false;
    };
    let mut entries = entries
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    entries.retain(|entry| entry.strong_count() > 0);
    entries
        .iter()
        .any(|entry| entry.ptr_eq(&Arc::downgrade(tool)))
}
pub(crate) async fn scope<F: std::future::Future>(
    candidates: Vec<ToolSpec>,
    future: F,
) -> F::Output {
    CANDIDATES
        .scope(
            candidates
                .into_iter()
                .map(|tool| SearchDocument {
                    tool,
                    namespace: String::new(),
                })
                .collect(),
            future,
        )
        .await
}
struct SearchTool;
#[async_trait::async_trait]
impl KeyedTool for SearchTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
        name: NAME.into(),
        description: "Search deferred tool metadata and load matching tools for the next model call. Use this to discover tools that were not provided upfront.".into(),
        parameters: json!({"type":"object","properties":{"query":{"type":"string"},"limit":{"type":"integer","minimum":1}},"required":["query"]}),
    }
    }
    async fn invoke(
        &self,
        _: Value,
        _: ToolContext,
        _: Option<&ExtensionState>,
        _: KeyedHost,
    ) -> ToolReply {
        Outcome::Skipped("tool search requires model-issued durable execution".into()).into()
    }
    async fn invoke_controlled(
        &self,
        args: Value,
        _: ToolContext,
        _: Option<&ExtensionState>,
        _: KeyedHost,
    ) -> (ToolReply, ToolControl) {
        let query = args
            .get("query")
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim();
        if query.is_empty() || query.len() > MAX_QUERY_BYTES {
            return (
                Outcome::Failed("query must be nonempty and at most 4096 bytes".into()).into(),
                ToolControl::default(),
            );
        }
        let limit = args.get("limit").map_or(Some(8), |value| {
            value.as_u64().or_else(|| {
                value
                    .as_f64()
                    .filter(|n| {
                        n.is_finite() && n.fract() == 0.0 && *n >= 1.0 && *n <= MAX_LIMIT as f64
                    })
                    .map(|n| n as u64)
            })
        });
        let Some(limit) = limit.filter(|limit| *limit > 0 && *limit <= MAX_LIMIT) else {
            return (
                Outcome::Failed("limit must be an integer from 1 to 128".into()).into(),
                ToolControl::default(),
            );
        };
        let result = CANDIDATES
            .try_with(|documents| {
                if documents.len() > MAX_CANDIDATES
                    || documents
                        .iter()
                        .map(|d| serde_json::to_vec(&d.tool).map_or(usize::MAX, |v| v.len()))
                        .fold(0usize, usize::saturating_add)
                        > MAX_METADATA_BYTES
                {
                    return None;
                }
                let matches = rank(query, documents, limit as usize);
                let names: Vec<String> = matches
                    .iter()
                    .map(|(index, _)| documents[*index].tool.name.clone())
                    .collect();
                let output = if names.is_empty() {
                    "No matching tools found.".into()
                } else {
                    format!(
                        "Loaded {} tool{}. They are available from your next call:\n{}",
                        names.len(),
                        if names.len() == 1 { "" } else { "s" },
                        matches
                            .iter()
                            .map(|(index, _)| {
                                let tool = &documents[*index].tool;
                                format!(
                                    "- {}: {}",
                                    tool.name,
                                    tool.description.trim().lines().next().unwrap_or("")
                                )
                            })
                            .collect::<Vec<_>>()
                            .join("\n")
                    )
                };
                Some((
                    Outcome::SucceededWithDetails {
                        output,
                        details: json!({"loaded": names}),
                    },
                    names,
                ))
            })
            .ok()
            .flatten();
        match result {
            Some((outcome, names)) => (
                outcome.into(),
                ToolControl {
                    add_tools: names,
                    ..Default::default()
                },
            ),
            None => (
                Outcome::Failed(
                    "tool search candidate context unavailable or exceeds limits".into(),
                )
                .into(),
                ToolControl::default(),
            ),
        }
    }
}
struct Indexed {
    name: BTreeSet<String>,
    local: BTreeSet<String>,
    counts: BTreeMap<String, usize>,
    length: usize,
    namespace: BTreeSet<String>,
}
/// Return document indices in relevance order, preserving input order for ties.
pub fn rank(query: &str, documents: &[SearchDocument], limit: usize) -> Vec<(usize, f64)> {
    let query_terms: BTreeSet<_> = tokenize(query).into_iter().collect();
    if query_terms.is_empty() || documents.is_empty() || limit == 0 {
        return vec![];
    }
    let indexed: Vec<_> = documents
        .iter()
        .map(|document| {
            let mut text = document.tool.description.clone();
            schema_text(&document.tool.parameters, &mut text);
            let tokens = tokenize(&text);
            let mut counts = BTreeMap::new();
            for token in &tokens {
                *counts.entry(token.clone()).or_default() += 1;
            }
            Indexed {
                name: tokenize(&document.tool.name).into_iter().collect(),
                local: tokenize(document.tool.name.rsplit("__").next().unwrap_or_default())
                    .into_iter()
                    .collect(),
                counts,
                length: tokens.len(),
                namespace: tokenize(&document.namespace).into_iter().collect(),
            }
        })
        .collect();
    let average = indexed
        .iter()
        .map(|document| document.length)
        .sum::<usize>() as f64
        / indexed.len() as f64;
    let average = if average == 0.0 { 1.0 } else { average };
    let weights: BTreeMap<_, _> = query_terms
        .iter()
        .map(|term| {
            let frequency = indexed
                .iter()
                .filter(|document| {
                    document.name.contains(term)
                        || document.counts.contains_key(term)
                        || document.namespace.contains(term)
                })
                .count() as f64;
            (
                term,
                (1.0 + (indexed.len() as f64 - frequency + 0.5) / (frequency + 0.5)).ln(),
            )
        })
        .collect();
    let full_name_bonus: f64 = weights.values().sum();
    let exact_query = normalize_name(query);
    let mut matches = Vec::new();
    for (index, document) in indexed.iter().enumerate() {
        let norm = 1.2 * (1.0 - 0.75 + 0.75 * document.length as f64 / average);
        let mut score = 0.0;
        let mut covered = 0usize;
        for term in &query_terms {
            let count = *document.counts.get(term).unwrap_or(&0) as f64;
            let weight = if document.name.contains(term) {
                3.0
            } else {
                0.0
            } + if document.namespace.contains(term) {
                0.5
            } else {
                0.0
            } + count * 2.2 / (count + norm);
            if weight > 0.0 {
                covered += 1;
            }
            score += weights[term] * weight;
        }
        if score == 0.0 {
            continue;
        }
        if !document.local.is_empty() && document.local.is_subset(&query_terms) {
            score += full_name_bonus;
        }
        let name = normalize_name(&documents[index].tool.name);
        let exact = name == exact_query || name.ends_with(&format!("__{exact_query}"));
        matches.push((index, score, covered, exact));
    }
    matches.sort_by(|a, b| b.3.cmp(&a.3).then(b.2.cmp(&a.2)).then(b.1.total_cmp(&a.1)));
    matches
        .into_iter()
        .take(limit)
        .map(|(index, score, _, _)| (index, score))
        .collect()
}
fn schema_text(value: &Value, text: &mut String) {
    schema_text_inner(value, value, text, &mut BTreeSet::new(), 0);
}
fn schema_text_inner(
    value: &Value,
    root: &Value,
    text: &mut String,
    seen: &mut BTreeSet<String>,
    depth: usize,
) {
    if depth >= 64 || text.len() >= MAX_METADATA_BYTES {
        return;
    }
    if let Some(reference) = value.get("$ref").and_then(Value::as_str)
        && let Some(pointer) = reference.strip_prefix('#')
        && seen.insert(pointer.to_owned())
        && let Some(target) = root.pointer(pointer)
    {
        schema_text_inner(target, root, text, seen, depth + 1);
    }
    if let Some(description) = value["description"].as_str() {
        text.push(' ');
        text.push_str(description);
    }
    if let Some(properties) = value["properties"].as_object() {
        for (name, property) in properties {
            text.push(' ');
            text.push_str(name);
            schema_text_inner(property, root, text, seen, depth + 1);
        }
    }
    if let Some(items) = value.get("items") {
        schema_text_inner(items, root, text, seen, depth + 1);
    }
    for key in ["anyOf", "oneOf", "allOf"] {
        if let Some(variants) = value[key].as_array() {
            for variant in variants {
                schema_text_inner(variant, root, text, seen, depth + 1);
            }
        }
    }
}
fn normalize_name(name: &str) -> String {
    name.trim()
        .to_lowercase()
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || "_$".contains(character) {
                character
            } else {
                '_'
            }
        })
        .collect()
}
fn tokenize(text: &str) -> Vec<String> {
    static CAMEL: LazyLock<Regex> = LazyLock::new(|| Regex::new("([a-z0-9])([A-Z])").unwrap());
    static ACRONYM: LazyLock<Regex> = LazyLock::new(|| Regex::new("([A-Z]+)([A-Z][a-z])").unwrap());
    static NON_WORD: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"[^\p{L}\p{M}\p{N}]+").unwrap());
    let text: String = text.nfc().collect();
    let text = CAMEL.replace_all(&text, "$1 $2");
    let text = ACRONYM.replace_all(&text, "$1 $2");
    let text = NON_WORD.replace_all(&text.to_lowercase(), " ").into_owned();
    text.unicode_words()
        .filter(|word| {
            ![
                "a", "an", "and", "are", "as", "at", "be", "by", "for", "from", "in", "is", "it",
                "of", "on", "or", "that", "the", "this", "to", "with",
            ]
            .contains(word)
        })
        .map(|word| {
            if !word.is_ascii() {
                return word.into();
            }
            if word.len() > 4 && word.ends_with("ies") {
                return format!("{}y", &word[..word.len() - 3]);
            }
            if word.len() > 4
                && ["ches", "shes", "sses", "xes", "zes"]
                    .iter()
                    .any(|ending| word.ends_with(ending))
            {
                return word[..word.len() - 2].into();
            }
            if word.len() > 3 && word.ends_with('s') && !word.ends_with("ss") {
                return word[..word.len() - 1].into();
            }
            word.into()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    fn doc(name: &str, description: &str, schema: Value) -> SearchDocument {
        SearchDocument {
            tool: ToolSpec {
                name: name.into(),
                description: description.into(),
                parameters: schema,
            },
            namespace: String::new(),
        }
    }
    #[test]
    fn ranking_matches_coverage_exact_name_and_unicode() {
        let docs = vec![
            doc("mcp__desktop__window", "window window window", json!({})),
            doc(
                "mcp__desktop__screenshot",
                "capture screenshot of window",
                json!({}),
            ),
        ];
        assert_eq!(rank("screenshot window", &docs, 8)[0].0, 1);
        assert_eq!(rank("window", &docs, 8)[0].0, 0);
        assert_eq!(
            tokenize("searchIssues HTTPServer café"),
            vec!["search", "issue", "http", "server", "café"]
        );
    }
    #[test]
    fn schema_local_refs_are_followed_without_cycles() {
        let docs = vec![
            doc(
                "one",
                "",
                json!({"properties":{"value":{"$ref":"#/$defs/a"}},"$defs":{"a":{"description":"nectarine","properties":{"next":{"$ref":"#/$defs/a"}}}}}),
            ),
            doc("two", "other", json!({})),
        ];
        assert_eq!(rank("nectarine", &docs, 8)[0].0, 0);
    }
}
