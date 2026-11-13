//! Deferred tool discovery, with ranking adapted from Pi v1.5.0 under MIT.
//! Copyright (c) 2025 Mario Zechner. See ../PI-LICENSE.
#[cfg(test)]
use pi_rs_core::ModelRequest;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, LazyLock};

use pi_rs_core::{
    Extension, ExtensionResult, ExtensionState, Outcome, RequestPurpose, Tool, ToolContext,
    ToolExecution, ToolSpec,
};
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use unicode_normalization::UnicodeNormalization;
use unicode_segmentation::UnicodeSegmentation;

const ID: &str = "tool-search";

#[derive(Clone)]
pub struct SearchDocument {
    pub tool: ToolSpec,
    pub namespace: String,
}

/// Hidden tools remain executable, but enter model requests only after discovery.
pub struct ToolSearch {
    documents: Arc<Vec<SearchDocument>>,
}
impl ToolSearch {
    pub fn new(documents: Vec<SearchDocument>) -> Self {
        Self {
            documents: Arc::new(documents),
        }
    }
}
#[derive(Default, Deserialize, Serialize, PartialEq)]
struct SearchState {
    loaded: BTreeSet<String>,
}
fn state(previous: Option<&ExtensionState>) -> ExtensionResult<SearchState> {
    match previous {
        Some(previous) if previous.version == 1 => {
            serde_json::from_value(previous.data.clone()).map_err(|error| error.to_string())
        }
        Some(_) => Err("unsupported tool-search state version".into()),
        None => Ok(SearchState::default()),
    }
}
#[async_trait::async_trait]
impl Extension for ToolSearch {
    fn id(&self) -> &str {
        ID
    }
    fn tools(&self) -> Vec<Arc<dyn Tool>> {
        vec![Arc::new(SearchTool(self.documents.clone()))]
    }
    fn migrate(
        &self,
        previous: Option<&ExtensionState>,
    ) -> ExtensionResult<Option<ExtensionState>> {
        let current = state(previous)?;
        Ok(previous.is_none().then(|| ExtensionState {
            version: 1,
            data: json!(current),
        }))
    }
    async fn before_request(
        &self,
        request: &mut pi_rs_core::ContextRequest<'_>,
        _: RequestPurpose,
        previous: Option<&ExtensionState>,
    ) -> ExtensionResult<pi_rs_core::ContextChange> {
        let original_tools = request.tools.clone();
        let loaded = state(previous)?.loaded;
        request.tools_mut().retain(|tool| {
            loaded.contains(&tool.name)
                || !self
                    .documents
                    .iter()
                    .any(|document| document.tool.name == tool.name)
        });
        for document in self
            .documents
            .iter()
            .filter(|document| loaded.contains(&document.tool.name))
        {
            if !request
                .tools
                .iter()
                .any(|tool| tool.name == document.tool.name)
            {
                request.tools_mut().push(document.tool.clone());
            }
        }
        Ok(if request.tools == original_tools {
            pi_rs_core::ContextChange::Unchanged
        } else {
            pi_rs_core::ContextChange::Changed
        })
    }
    fn after_tool(
        &self,
        name: &str,
        outcome: &Outcome,
        previous: Option<&ExtensionState>,
    ) -> ExtensionResult<Option<ExtensionState>> {
        if name != "tool_search" {
            return Ok(None);
        }
        let Outcome::Structured { data, .. } = outcome else {
            return Ok(None);
        };
        let mut current = state(previous)?;
        let before = current.loaded.clone();
        let names: Vec<String> =
            serde_json::from_value(data["loaded"].clone()).map_err(|error| error.to_string())?;
        current.loaded.extend(names.into_iter().filter(|name| {
            self.documents
                .iter()
                .any(|document| document.tool.name == *name)
        }));
        Ok((current.loaded != before).then(|| ExtensionState {
            version: 1,
            data: json!(current),
        }))
    }
}
struct SearchTool(Arc<Vec<SearchDocument>>);
#[async_trait::async_trait]
impl Tool for SearchTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec { name: "tool_search".into(), description: "Search deferred tool metadata with BM25 and load matching tools for the next model call. Use this to discover tools that were not provided upfront.".into(), parameters: json!({"type":"object","properties":{"query":{"type":"string","minLength":1},"limit":{"type":"integer","minimum":1}},"required":["query"],"additionalProperties":false}) }
    }
    fn execution(&self) -> ToolExecution {
        ToolExecution::ParallelRead
    }
    async fn invoke(&self, arguments: Value, context: ToolContext) -> Outcome {
        self.invoke_with_state(arguments, context, None).await
    }
    async fn invoke_with_state(
        &self,
        arguments: Value,
        _: ToolContext,
        previous: Option<&ExtensionState>,
    ) -> Outcome {
        let query = arguments["query"].as_str().unwrap_or_default().trim();
        if query.is_empty() {
            return Outcome::Failed("query must not be empty".into());
        }
        let limit = arguments.get("limit").map_or(Some(8), Value::as_u64);
        let Some(limit) = limit.filter(|limit| *limit > 0) else {
            return Outcome::Failed("limit must be a positive integer".into());
        };
        let previous = match state(previous) {
            Ok(state) => state,
            Err(error) => return Outcome::Failed(error),
        };
        let documents: Vec<_> = self
            .0
            .iter()
            .filter(|document| !previous.loaded.contains(&document.tool.name))
            .cloned()
            .collect();
        let matches = rank(
            query,
            &documents,
            usize::try_from(limit).unwrap_or(usize::MAX),
        );
        let loaded: Vec<_> = matches
            .iter()
            .map(|(index, _)| documents[*index].tool.name.clone())
            .collect();
        let tools: Vec<_> = matches
            .iter()
            .map(|(index, score)| {
                json!({
                    "name": documents[*index].tool.name,
                    "description": documents[*index].tool.description,
                    "score": score,
                })
            })
            .collect();
        Outcome::Structured {
            data: json!({"tools":tools,"loaded":loaded}),
            attachments: vec![],
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
    if let Some(description) = value["description"].as_str() {
        text.push(' ');
        text.push_str(description);
    }
    if let Some(properties) = value["properties"].as_object() {
        for (name, property) in properties {
            text.push(' ');
            text.push_str(name);
            schema_text(property, text);
        }
    }
    if let Some(items) = value.get("items") {
        schema_text(items, text);
    }
    for key in ["anyOf", "oneOf", "allOf"] {
        if let Some(variants) = value[key].as_array() {
            for variant in variants {
                schema_text(variant, text);
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
    fn document(name: &str, description: &str) -> SearchDocument {
        SearchDocument {
            tool: ToolSpec {
                name: name.into(),
                description: description.into(),
                parameters: json!({"type":"object"}),
            },
            namespace: "Desktop operations".into(),
        }
    }
    #[test]
    fn ranking_prefers_coverage_then_exact_names_and_normalizes_identifiers() {
        let documents = [
            document("mcp__desktop__window", "window window window"),
            document(
                "mcp__desktop__screenshot",
                "Capture a screenshot of a window",
            ),
        ];
        assert_eq!(rank("screenshot window", &documents, 8)[0].0, 1);
        assert_eq!(rank("window", &documents, 8)[0].0, 0);
        assert_eq!(
            tokenize("searchIssues HTTPServer"),
            ["search", "issue", "http", "server"]
        );
        assert_eq!(tokenize("cafe\u{301}"), ["café"]);
    }
    struct HiddenTool;
    #[async_trait::async_trait]
    impl Tool for HiddenTool {
        fn spec(&self) -> ToolSpec {
            document("mcp__tickets__read", "Read support tickets").tool
        }
        async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
            Outcome::Succeeded("ticket".into())
        }
    }
    struct HiddenExtension;
    impl Extension for HiddenExtension {
        fn id(&self) -> &str {
            "tickets"
        }
        fn tools(&self) -> Vec<Arc<dyn Tool>> {
            vec![Arc::new(HiddenTool)]
        }
    }
    struct DiscoveryModel(std::sync::atomic::AtomicUsize);
    #[async_trait::async_trait]
    impl pi_rs_core::Model for DiscoveryModel {
        async fn stream(
            &self,
            request: ModelRequest,
        ) -> pi_rs_core::Result<pi_rs_core::ModelStream> {
            use pi_rs_core::{Message, ModelEvent, ToolCall};
            use std::sync::atomic::Ordering;
            let step = self.0.fetch_add(1, Ordering::SeqCst) % 3;
            assert_eq!(
                request
                    .tools
                    .iter()
                    .any(|tool| tool.name == "mcp__tickets__read"),
                step > 0
            );
            if step == 2 {
                let result = request
                    .messages
                    .iter()
                    .rev()
                    .find_map(|message| match message {
                        Message::Tool { content, .. } => Some(content),
                        _ => None,
                    })
                    .unwrap();
                let result: Value = serde_json::from_str(result).unwrap();
                assert_eq!(result["data"]["loaded"], json!([]));
                return Ok(Box::pin(futures_util::stream::iter([
                    Ok(ModelEvent::Text("done".into())),
                    Ok(ModelEvent::Complete {
                        calls: vec![],
                        usage: None,
                    }),
                ])));
            }
            Ok(Box::pin(futures_util::stream::iter([Ok(
                ModelEvent::Complete {
                    calls: vec![ToolCall {
                        id: format!("search-{step}"),
                        name: "tool_search".into(),
                        arguments: json!({"query":"tickets"}).to_string(),
                    }],
                    usage: None,
                },
            )])))
        }
    }
    #[test]
    fn loaded_tools_follow_the_branch_and_queries_skip_already_loaded_tools() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let model = Arc::new(DiscoveryModel(std::sync::atomic::AtomicUsize::new(0)));
            let mut session = pi_rs_core::SessionBuilder::new(model, "discovery")
                .extension(Arc::new(HiddenExtension))
                .unwrap()
                .extension(Arc::new(ToolSearch::new(vec![document(
                    "mcp__tickets__read",
                    "Read support tickets",
                )])))
                .unwrap()
                .in_memory()
                .unwrap();
            let initial = session.journal().unwrap().head();
            assert_eq!(session.prompt("first").await.unwrap(), "done");
            let mut fork = session.fork_at(initial).unwrap();
            assert_eq!(fork.prompt("fork").await.unwrap(), "done");
            fork.close().await.unwrap();
            session.close().await.unwrap();
        });
    }
}
