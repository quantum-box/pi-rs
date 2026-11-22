//! Pi session conversion shared by the importer and journal validation.
use crate::persistent_map::PersistentMap;
use crate::{Error, Event, Invocation, Outcome, Result, ToolCall, Usage};
use base64::Engine;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::sync::Arc;

struct StoreView<'a>(&'a PersistentMap<Arc<str>, Arc<Value>>);

impl serde::Serialize for StoreView<'_> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(None)?;
        for (key, value) in self.0.iter() {
            map.serialize_entry(key.as_ref(), value.as_ref())?;
        }
        map.end()
    }
}
use uuid::Uuid;

// Mirrors the Google adapter without making core depend on a provider crate.
// The workbench parity test must be updated if either model-family rule changes.
pub fn google_uses_tool_call_ids(model: &str) -> bool {
    let id = model.strip_prefix("models/").unwrap_or(model);
    let lower = id.to_ascii_lowercase();
    let version = lower
        .strip_prefix("gemini-")
        .map(|s| s.strip_prefix("live-").unwrap_or(s));
    let major = version.and_then(|s| {
        s.bytes()
            .take_while(u8::is_ascii_digit)
            .fold(None, |n: Option<u32>, d| {
                Some(
                    n.unwrap_or(0)
                        .saturating_mul(10)
                        .saturating_add((d - b'0') as u32),
                )
            })
    });
    major.is_some_and(|n| n >= 3) || id.starts_with("claude-") || id.starts_with("gpt-oss-")
}

fn imported_cost(value: &Value) -> Option<serde_json::Number> {
    match &value["cost"]["total"] {
        Value::Number(number)
            if number
                .as_f64()
                .is_some_and(|value| value >= 0.0 && value.is_finite()) =>
        {
            Some(number.clone())
        }
        _ => None,
    }
}

fn append_imported_usage(events: &mut Vec<Event>, value: &Value) -> Option<Usage> {
    value.as_object()?;
    let cache_read = value["cacheRead"].as_u64().unwrap_or(0);
    let cache_write = value["cacheWrite"].as_u64().unwrap_or(0);
    let usage = Usage {
        input_tokens: value["input"]
            .as_u64()
            .unwrap_or(0)
            .saturating_add(cache_read)
            .saturating_add(cache_write),
        output_tokens: value["output"].as_u64().unwrap_or(0),
    };
    events.push(Event::ModelFinished {
        request_id: Uuid::new_v4(),
        metadata: crate::ModelMetadata {
            cost_usd: imported_cost(value),
            cost_breakdown: crate::UsageCostBreakdown::from_value(&value["cost"]),
            cost_from_provider: value["cost"]["totalFromProvider"].as_bool(),
            cache_write_1h_tokens: value["cacheWrite1h"].as_u64(),
            cached_input_tokens: Some(cache_read),
            cache_creation_input_tokens: Some(cache_write),
            ..Default::default()
        },
        usage: crate::UsageReport::Measured { usage },
        failure: None,
        partial: String::new(),
        reply: None,
    });
    Some(usage)
}

fn summary_usage_metadata(events: &[Event]) -> Option<Box<crate::SummaryUsageMetadata>> {
    let (_, metadata) = crate::SummaryUsageMetadata::from_completion(events.last()?)?;
    Some(Box::new(metadata))
}

#[derive(Clone, Default)]
pub struct Branch {
    pub head: u64,
    pub entry_start: u64,
    pub context_target: Option<u64>,
    pub(crate) calls: PersistentMap<Arc<str>, Uuid>,
    pub(crate) ambiguous_calls: PersistentMap<Arc<str>, ()>,
    pub missing_ancestors: bool,
    pub ancestry_index: usize,
    pub model: Arc<str>,
    pub provider: Option<Arc<str>>,
    pub thinking: Option<Arc<str>>,
    pub(crate) store: PersistentMap<Arc<str>, Arc<Value>>,
}

/// A buffering hint only. Ownership must still be established by replay.
pub fn conversion_count_hint(entry: &Value) -> Option<u64> {
    let usage = |value: &Value| u64::from(value.is_object());
    Some(match entry["type"].as_str()? {
        "message" => match entry["message"]["role"].as_str()? {
            "user" => {
                1 + u64::from(
                    entry["message"]["content"]
                        .as_array()
                        .is_some_and(|blocks| blocks.iter().any(|block| block["type"] == "image")),
                )
            }
            "assistant" => {
                let failed = matches!(
                    entry["message"]["stopReason"].as_str(),
                    Some("error" | "aborted")
                );
                2 + if failed {
                    0
                } else {
                    entry["message"]["content"].as_array().map_or(0, |blocks| {
                        blocks
                            .iter()
                            .filter(|block| block["type"] == "toolCall")
                            .count() as u64
                    })
                }
            }
            "toolResult" => 1 + usage(&entry["message"]["usage"]),
            "custom" | "hookMessage" | "bashExecution" | "branchSummary" | "compactionSummary"
            | "system" => 1,
            _ => return None,
        },
        "model_change"
        | "thinking_level_change"
        | "session_info"
        | "label"
        | "custom_message"
        | "context_edit" => 1,
        "compaction" | "branch_summary" => 1 + usage(&entry["usage"]),
        "custom" => {
            1 + u64::from(
                entry["customType"] == "codemode-store"
                    && entry["data"]["set"].is_object()
                    && entry["data"]["delete"]
                        .as_array()
                        .is_some_and(|items| items.iter().all(Value::is_string)),
            )
        }
        "usage" => usage(&entry["usage"]),
        _ => return None,
    })
}

/// Compare an imported conversion with a replay of the same source entry.
/// Only importer-generated request and operation UUIDs may be renamed; the
/// mapping is bijective and is shared across the whole replay.
fn conversion_equal(
    expected: &Event,
    actual: &Event,
    ids: &mut PersistentMap<Uuid, Uuid>,
    reverse: &mut PersistentMap<Uuid, Uuid>,
) -> Result<bool> {
    fn bind(
        expected: Uuid,
        actual: Uuid,
        ids: &mut PersistentMap<Uuid, Uuid>,
        reverse: &mut PersistentMap<Uuid, Uuid>,
    ) -> bool {
        if ids.get(&expected).is_some_and(|value| *value != actual)
            || reverse.get(&actual).is_some_and(|value| *value != expected)
        {
            return false;
        }
        ids.insert(expected, actual);
        reverse.insert(actual, expected);
        true
    }
    let mut expected = serde_json::to_value(expected)?;
    let actual = serde_json::to_value(actual)?;
    let kind = expected["type"].as_str().unwrap_or_default().to_owned();
    let mut paths = Vec::new();
    match kind.as_str() {
        "model_finished" | "assistant" => paths.push("/request_id".to_owned()),
        "tool_started" | "tool_finished" | "tool_reconciled" => {
            paths.push("/operation_id".to_owned())
        }
        "compacted" | "branch_summary" => paths.push("/usage_metadata/request_id".to_owned()),
        _ => {}
    }
    if kind == "assistant"
        && let Some(invocations) = expected["invocations"].as_array()
    {
        for index in 0..invocations.len() {
            paths.push(format!("/invocations/{index}/operation_id"));
        }
    }
    for path in paths {
        let left = expected
            .pointer(&path)
            .and_then(Value::as_str)
            .and_then(|value| Uuid::parse_str(value).ok());
        let right = actual
            .pointer(&path)
            .and_then(Value::as_str)
            .and_then(|value| Uuid::parse_str(value).ok());
        match (left, right) {
            (Some(left), Some(right)) if bind(left, right, ids, reverse) => {
                *expected.pointer_mut(&path).expect("UUID path") = Value::String(right.to_string());
            }
            (None, None) => {}
            _ => return Ok(false),
        }
    }
    Ok(expected == actual)
}

/// Incremental verifier for append-only projection streams. The generated-ID
/// mapping and persistent branch snapshots are retained across entries.
pub struct StreamVerifier {
    processed: usize,
    validated_records: usize,
    nearest: Vec<Option<u64>>,
    completed: Vec<bool>,
    source_ids: BTreeMap<String, u64>,
    unresolved_parents: std::collections::BTreeSet<String>,
    states: BTreeMap<u64, Branch>,
    branches: BTreeMap<String, Branch>,
    names: Vec<Option<String>>,
    ids: PersistentMap<Uuid, Uuid>,
    reverse: PersistentMap<Uuid, Uuid>,
    legacy: bool,
    counts: BTreeMap<u64, u64>,
    new_counts: BTreeMap<u64, u64>,
    checked_sources: usize,
    pending: Option<StagedConversion>,
    authoritative_source_count: Option<usize>,
    legacy_candidates: BTreeMap<u64, (Event, Option<String>)>,
}

struct StagedConversion {
    source: u64,
    name: String,
    branch: Branch,
    events: Vec<Event>,
}

impl Default for StreamVerifier {
    fn default() -> Self {
        Self {
            processed: 0,
            validated_records: 0,
            nearest: vec![None],
            completed: vec![false],
            source_ids: Default::default(),
            unresolved_parents: Default::default(),
            states: Default::default(),
            branches: Default::default(),
            names: Default::default(),
            ids: Default::default(),
            reverse: Default::default(),
            legacy: false,
            counts: Default::default(),
            new_counts: Default::default(),
            checked_sources: 0,
            pending: None,
            authoritative_source_count: None,
            legacy_candidates: Default::default(),
        }
    }
}

impl StreamVerifier {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn counts(&self) -> &BTreeMap<u64, u64> {
        &self.counts
    }
    pub fn take_new_counts(&mut self) -> BTreeMap<u64, u64> {
        std::mem::take(&mut self.new_counts)
    }
    pub fn checked_sources(&self) -> usize {
        self.checked_sources
    }
    pub fn processed(&self) -> usize {
        self.processed
    }

    pub fn set_authoritative_end(&mut self, records: &[crate::Record]) {
        self.authoritative_source_count = Some(records.iter().filter(|record|
            matches!(&record.event, Event::SourceEntry { format, .. } if format == "pi")).count());
    }

    /// Verify as much of `records` as possible. A returned ID is the last
    /// record needed for the pending conversion; no ownership is committed yet.
    /// Each call must supply an unchanged prefix followed by any new records.
    /// New records must have sequential one-based IDs and only prior parents.
    pub fn advance(
        &mut self,
        records: &[crate::Record],
        metadata: &BTreeMap<u64, Value>,
        authoritative: bool,
    ) -> Result<Option<u64>> {
        if records.len() < self.validated_records {
            return Err(Error::Session(
                "Pi stream record prefix was truncated".into(),
            ));
        }
        let start = self.validated_records.min(records.len());
        for (index, record) in records.iter().enumerate().skip(start) {
            let expected = u64::try_from(index)
                .ok()
                .and_then(|index| index.checked_add(1))
                .ok_or_else(|| Error::Session("invalid Pi stream record ID".into()))?;
            if record.id != expected {
                return Err(Error::Session("invalid Pi stream record ID".into()));
            }
            if record
                .parent
                .is_some_and(|parent| parent == 0 || parent >= expected)
            {
                return Err(Error::Session("invalid Pi stream record parent".into()));
            }
        }
        self.validated_records = records.len();
        if authoritative && self.authoritative_source_count.is_none() {
            self.authoritative_source_count = Some(records.iter().filter(|record| matches!(&record.event, Event::SourceEntry { format, .. } if format == "pi")).count());
        }
        while self.processed < records.len() {
            let record = &records[self.processed];
            if let Some(parent) = record.parent
                && record.id != parent + 1
                && let Some((expected, timestamp)) = self.legacy_candidates.get(&parent)
                && record.timestamp == *timestamp
                && record.run_id.is_none()
                && conversion_equal(
                    expected,
                    &record.event,
                    &mut PersistentMap::new(),
                    &mut PersistentMap::new(),
                )?
            {
                return Err(Error::Session(
                    "ambiguous legacy imported Pi conversion provenance".into(),
                ));
            }
            let parent_source = record
                .parent
                .and_then(|parent| self.nearest.get(parent as usize).copied().flatten());
            let Event::SourceEntry { format, entry } = &record.event else {
                self.nearest.push(parent_source);
                self.completed
                    .push(record.parent.is_some_and(|id| self.completed[id as usize]));
                self.processed += 1;
                continue;
            };
            if format != "pi" {
                self.nearest.push(parent_source);
                self.completed
                    .push(record.parent.is_some_and(|id| self.completed[id as usize]));
                self.processed += 1;
                continue;
            }
            if entry["type"] == "session" {
                self.legacy = entry["version"].as_u64().unwrap_or(1) == 1;
                self.names.push(None);
                self.nearest.push(parent_source);
                self.completed
                    .push(record.parent.is_some_and(|id| self.completed[id as usize]));
                self.processed += 1;
                continue;
            }
            if self.pending.is_none() {
                self.checked_sources += 1;
                let index = self.names.len();
                let name = entry["id"]
                    .as_str()
                    .map(str::to_owned)
                    .or_else(|| self.legacy.then(|| format!("legacy-{index}")))
                    .filter(|name| !name.is_empty())
                    .ok_or_else(|| {
                        Error::Session("missing or invalid imported Pi source ID".into())
                    })?;
                if self.unresolved_parents.contains(&name) {
                    return Err(Error::Session(
                        "imported Pi parent is a forward reference".into(),
                    ));
                }
                if !self.legacy {
                    match entry.get("parentId") {
                        None | Some(Value::Null) => {
                            if parent_source.is_some() {
                                return Err(Error::Session(
                                    "imported Pi root has a physical source parent".into(),
                                ));
                            }
                        }
                        Some(Value::String(parent)) if parent == &name => {
                            return Err(Error::Session(
                                "imported Pi source is its own parent".into(),
                            ));
                        }
                        Some(Value::String(parent)) => {
                            if let Some(id) = self.source_ids.get(parent) {
                                if parent_source != Some(*id) {
                                    return Err(Error::Session(
                                        "imported Pi source parent does not match its branch"
                                            .into(),
                                    ));
                                }
                            } else {
                                self.unresolved_parents.insert(parent.clone());
                            }
                        }
                        _ => return Err(Error::Session("invalid imported Pi parent ID".into())),
                    }
                }
                if parent_source.is_some()
                    && record.parent.is_none_or(|id| !self.completed[id as usize])
                {
                    return Err(Error::Session(
                        "imported Pi source follows a partial conversion".into(),
                    ));
                }
                if self.branches.contains_key(&name) {
                    return Err(Error::Session(
                        "ambiguous duplicate imported Pi source ID".into(),
                    ));
                }
                let mut branch = parent_source
                    .and_then(|source| self.states.get(&source).cloned())
                    .unwrap_or_default();
                branch.head = record.parent.unwrap_or(0);
                let mut migrated = entry.clone();
                if self.legacy
                    && entry["type"] == "compaction"
                    && entry["firstKeptEntryId"].is_null()
                    && let Some(kept) = entry["firstKeptEntryIndex"]
                        .as_u64()
                        .and_then(|n| usize::try_from(n).ok())
                {
                    let target = if kept == index {
                        Some(name.as_str())
                    } else if kept > index {
                        let total = self.authoritative_source_count.ok_or_else(|| Error::Session("streaming v1 forward compaction requires an authoritative end of file".into()))?;
                        (kept < total).then_some(name.as_str())
                    } else {
                        self.names.get(kept).and_then(Option::as_deref)
                    };
                    if let Some(target) = target {
                        migrated["id"] = Value::String(name.clone());
                        migrated["firstKeptEntryId"] = Value::String(target.to_owned());
                    }
                }
                let marker = metadata
                    .get(&record.id)
                    .and_then(|value| value.get("pi_rs_import_conversion_count_v1"));
                let events = convert(&migrated, &mut branch, &self.branches)?;
                let events = if marker.is_none()
                    && entry["type"] == "model_change"
                    && records.get(record.id as usize).is_some_and(|next| {
                        next.parent == Some(record.id) && next.timestamp != record.timestamp
                    }) {
                    Vec::new()
                } else {
                    events
                };
                self.pending = Some(StagedConversion {
                    source: record.id,
                    name,
                    branch,
                    events,
                });
            }
            if metadata
                .get(&record.id)
                .and_then(|value| value.get("pi_rs_import_conversion_count_v1"))
                .is_none()
                && entry["type"] == "model_change"
                && records.get(record.id as usize).is_some_and(|next| {
                    next.parent == Some(record.id) && next.timestamp != record.timestamp
                })
            {
                self.pending.as_mut().expect("staged source").events.clear();
            }
            let staged = self.pending.as_ref().expect("staged source");
            if staged.source != record.id {
                return Err(Error::Session("changed pending Pi source".into()));
            }
            let events = &staged.events;
            let count = events.len() as u64;
            if let Some(marker) = metadata
                .get(&record.id)
                .and_then(|value| value.get("pi_rs_import_conversion_count_v1"))
                && marker.as_u64() != Some(count)
            {
                return Err(Error::Session(
                    "invalid imported Pi conversion provenance".into(),
                ));
            }
            let end = record.id.checked_add(count).ok_or_else(|| {
                Error::Session("invalid imported Pi conversion provenance".into())
            })?;
            if end > records.len() as u64 {
                return Ok(Some(end));
            }
            let mut ids = self.ids.clone();
            let mut reverse = self.reverse.clone();
            let mut parent = record.id;
            for (offset, event) in events.iter().enumerate() {
                if let Event::ToolFinished { operation_id, .. }
                | Event::ToolReconciled { operation_id, .. } = event
                    && !ids.contains_key(operation_id)
                {
                    return Err(Error::Session(
                        "cannot verify imported Pi tool operation identity".into(),
                    ));
                }
                let actual = &records[record.id as usize + offset];
                if actual.parent != Some(parent)
                    || actual.timestamp != record.timestamp
                    || actual.run_id.is_some()
                    || matches!(
                        actual.event,
                        Event::SourceEntry { .. } | Event::Checkout { .. }
                    )
                    || !conversion_equal(event, &actual.event, &mut ids, &mut reverse)?
                {
                    return Err(Error::Session(
                        "invalid imported Pi conversion provenance".into(),
                    ));
                }
                parent = actual.id;
            }
            let legacy_candidate = metadata
                .get(&record.id)
                .and_then(|value| value.get("pi_rs_import_conversion_count_v1"))
                .is_none()
                .then(|| events.first().cloned())
                .flatten();
            let context_target = events
                .iter()
                .enumerate()
                .filter_map(|(offset, event)| {
                    matches!(
                        event,
                        Event::User { .. }
                            | Event::CustomMessage { .. }
                            | Event::CustomMessageDelivered { .. }
                            | Event::Assistant { .. }
                            | Event::ToolFinished { .. }
                            | Event::ToolReconciled { .. }
                            | Event::ImportedUnattributedToolResult { .. }
                    )
                    .then_some(record.id + offset as u64 + 1)
                })
                .next_back();
            let StagedConversion {
                name, mut branch, ..
            } = self.pending.take().expect("staged source");
            self.ids = ids;
            self.reverse = reverse;
            self.nearest.push(Some(record.id));
            self.completed.push(count == 0);
            for offset in 0..count {
                self.nearest.push(Some(record.id));
                self.completed.push(offset + 1 == count);
            }
            self.source_ids.insert(name.clone(), record.id);
            branch.entry_start = record.id + u64::from(count != 0);
            branch.context_target = context_target;
            branch.head = end;
            self.states.insert(record.id, branch.clone());
            self.branches.insert(name.clone(), branch);
            self.names.push(Some(name));
            self.counts.insert(record.id, count);
            self.new_counts.insert(record.id, count);
            if let Some(candidate) = legacy_candidate {
                self.legacy_candidates
                    .insert(record.id, (candidate, record.timestamp.clone()));
            }
            self.processed += count as usize + 1;
        }
        Ok(None)
    }
}

/// Replay source entries with the importer's conversion rules and verify the
/// complete claimed conversion for each relevant source. Non-relevant source
/// entries are replayed only to reconstruct branch state.
pub(crate) fn validate_conversions(
    session: &crate::Session,
    relevant: &std::collections::HashSet<u64>,
) -> Result<BTreeMap<u64, u64>> {
    let records = session.records();
    let sources: Vec<_> = records
        .iter()
        .filter(
            |record| matches!(&record.event, Event::SourceEntry { format, .. } if format == "pi"),
        )
        .collect();
    let legacy = sources.first().is_some_and(|record| matches!(&record.event, Event::SourceEntry { entry, .. } if entry["type"] == "session" && entry["version"].as_u64().unwrap_or(1) == 1));
    let source_names: BTreeMap<u64, String> = sources
        .iter()
        .enumerate()
        .filter_map(|(index, record)| {
            let Event::SourceEntry { entry, .. } = &record.event else {
                return None;
            };
            if entry["type"] == "session" {
                return None;
            }
            entry["id"]
                .as_str()
                .map(str::to_owned)
                .or_else(|| (legacy && index > 0).then(|| format!("legacy-{index}")))
                .map(|id| (record.id, id))
        })
        .collect();
    // Source names are the importer's reference namespace, including v1's
    // synthetic names. Never let a later record silently replace an earlier
    // one when a selected entry or one of its references uses that name.
    let mut name_counts = BTreeMap::<&str, usize>::new();
    for name in source_names.values() {
        *name_counts.entry(name.as_str()).or_default() += 1;
    }
    for source in &sources {
        if !relevant.contains(&source.id) {
            continue;
        }
        if source_names
            .get(&source.id)
            .is_some_and(|name| name_counts.get(name.as_str()).copied().unwrap_or(0) > 1)
        {
            return Err(Error::Session(
                "ambiguous duplicate imported Pi source ID".into(),
            ));
        }
        let Event::SourceEntry { entry, .. } = &source.event else {
            continue;
        };
        for name in [
            entry["targetId"].as_str(),
            entry["fromId"].as_str(),
            entry["firstKeptEntryId"].as_str(),
            entry["message"]["fromId"].as_str(),
        ]
        .into_iter()
        .flatten()
        {
            if name_counts.get(name).copied().unwrap_or(0) > 1 {
                return Err(Error::Session(
                    "ambiguous duplicate imported Pi source reference".into(),
                ));
            }
        }
        if legacy
            && let Some(index) = entry["firstKeptEntryIndex"]
                .as_u64()
                .and_then(|n| usize::try_from(n).ok())
            && let Some(name) = sources
                .get(index)
                .and_then(|record| source_names.get(&record.id))
            && name_counts.get(name.as_str()).copied().unwrap_or(0) > 1
        {
            return Err(Error::Session(
                "ambiguous duplicate imported Pi source reference".into(),
            ));
        }
    }
    let source_indices: BTreeMap<u64, usize> = sources
        .iter()
        .enumerate()
        .map(|(index, record)| (record.id, index))
        .collect();
    let mut nearest = vec![None; records.len() + 1];
    let mut direct_children = BTreeMap::<u64, Vec<u64>>::new();
    let mut children = BTreeMap::<u64, usize>::new();
    for record in records {
        if let Some(parent) = record.parent {
            direct_children.entry(parent).or_default().push(record.id);
        }
        let parent = record.parent.and_then(|id| nearest[id as usize]);
        nearest[record.id as usize] = parent;
        if matches!(&record.event, Event::SourceEntry { format, entry } if format == "pi" && entry["type"] != "session")
        {
            if let Some(parent) = parent {
                *children.entry(parent).or_default() += 1;
            }
            nearest[record.id as usize] = Some(record.id);
        }
    }
    // Precompute ancestry intervals for checking that a source's physical
    // parent contains the complete conversion of its preceding source.
    let mut entered = vec![0usize; records.len() + 1];
    let mut exited = vec![0usize; records.len() + 1];
    let mut tick = 0usize;
    let mut stack = Vec::new();
    for root in records
        .iter()
        .filter(|record| record.parent.is_none())
        .rev()
    {
        stack.push((root.id, false));
    }
    while let Some((id, leaving)) = stack.pop() {
        if leaving {
            exited[id as usize] = tick;
        } else {
            tick += 1;
            entered[id as usize] = tick;
            stack.push((id, true));
            if let Some(children) = direct_children.get(&id) {
                stack.extend(children.iter().rev().map(|child| (*child, false)));
            }
        }
    }
    let is_ancestor = |ancestor: u64, child: u64| {
        entered[ancestor as usize] <= entered[child as usize]
            && exited[child as usize] <= exited[ancestor as usize]
    };
    // Only source ancestors and source IDs actually referenced by relevant
    // entries are needed to reconstruct the relevant branch states.
    let mut by_name = BTreeMap::<&str, Vec<u64>>::new();
    for (id, name) in &source_names {
        by_name.entry(name.as_str()).or_default().push(*id);
    }
    let mut needed: std::collections::HashSet<u64> = sources
        .iter()
        .filter(|record| {
            relevant.contains(&record.id)
                && matches!(&record.event,
            Event::SourceEntry { entry, .. } if entry["type"] != "session")
        })
        .map(|record| record.id)
        .collect();
    let mut pending: Vec<_> = needed.iter().copied().collect();
    while let Some(id) = pending.pop() {
        let source = &records[(id - 1) as usize];
        if let Some(parent) = source.parent.and_then(|parent| nearest[parent as usize])
            && needed.insert(parent)
        {
            pending.push(parent);
        }
        let Event::SourceEntry { entry, .. } = &source.event else {
            continue;
        };
        let refs = [
            entry["targetId"].as_str(),
            entry["fromId"].as_str(),
            entry["firstKeptEntryId"].as_str(),
            entry["message"]["fromId"].as_str(),
        ];
        for reference in refs.into_iter().flatten() {
            if let Some(ids) = by_name.get(reference) {
                if ids.len() > 1 {
                    return Err(Error::Session(
                        "ambiguous duplicate imported Pi source reference".into(),
                    ));
                }
                if needed.insert(ids[0]) {
                    pending.push(ids[0]);
                }
            }
        }
        if legacy
            && let Some(index) = entry["firstKeptEntryIndex"]
                .as_u64()
                .and_then(|n| usize::try_from(n).ok())
            && let Some(reference) = sources.get(index)
            && source_names.contains_key(&reference.id)
            && needed.insert(reference.id)
        {
            pending.push(reference.id);
        }
    }
    for id in &needed {
        if source_names.get(id).is_none_or(String::is_empty) {
            return Err(Error::Session(
                "missing or invalid imported Pi source ID".into(),
            ));
        }
        if source_names
            .get(id)
            .is_some_and(|name| name_counts.get(name.as_str()).copied().unwrap_or(0) > 1)
        {
            return Err(Error::Session(
                "ambiguous duplicate imported Pi source ID".into(),
            ));
        }
    }
    children.clear();
    for id in &needed {
        let source = &records[(id - 1) as usize];
        if let Some(parent) = source.parent.and_then(|parent| nearest[parent as usize]) {
            *children.entry(parent).or_default() += 1;
        }
    }
    let mut states = BTreeMap::<u64, Branch>::new();
    let mut branches = BTreeMap::<String, Branch>::new();
    let mut ids = PersistentMap::new();
    let mut reverse = PersistentMap::new();
    let mut verified = BTreeMap::new();
    for record in records {
        let parent_source = record.parent.and_then(|id| nearest[id as usize]);
        let Event::SourceEntry { format, entry } = &record.event else {
            continue;
        };
        if format != "pi" || entry["type"] == "session" {
            continue;
        }
        if !needed.contains(&record.id) {
            continue;
        }
        if !legacy {
            let name = source_names.get(&record.id).expect("validated source name");
            match entry.get("parentId") {
                None | Some(Value::Null) => {
                    if parent_source.is_some() {
                        return Err(Error::Session(
                            "imported Pi root has a physical source parent".into(),
                        ));
                    }
                }
                Some(Value::String(parent)) if parent == name => {
                    return Err(Error::Session(
                        "imported Pi source is its own parent".into(),
                    ));
                }
                Some(Value::String(parent)) => {
                    if let Some(candidates) = by_name.get(parent.as_str())
                        && (candidates.len() != 1 || parent_source != Some(candidates[0]))
                    {
                        return Err(Error::Session(
                            "imported Pi source parent does not match its branch".into(),
                        ));
                    }
                    // An absent parent may be an original orphan or a label
                    // removed by an existing fork; neither can be distinguished
                    // using the retained source records alone.
                }
                _ => return Err(Error::Session("invalid imported Pi parent ID".into())),
            }
        }
        if let Some(parent) = parent_source {
            let tail = states
                .get(&parent)
                .ok_or_else(|| {
                    Error::Session("cannot reconstruct imported Pi branch state".into())
                })?
                .head;
            if record
                .parent
                .is_none_or(|physical| !is_ancestor(tail, physical))
            {
                return Err(Error::Session(
                    "imported Pi source follows a partial conversion".into(),
                ));
            }
        }
        let mut branch = if let Some(parent) = parent_source {
            let remaining = children.get_mut(&parent).expect("source child count");
            *remaining -= 1;
            if *remaining == 0 {
                match states.remove(&parent) {
                    Some(state) => state,
                    None if !needed.contains(&record.id) => continue,
                    None => {
                        return Err(Error::Session(
                            "cannot reconstruct imported Pi branch state".into(),
                        ));
                    }
                }
            } else {
                let Some(state) = states.get(&parent) else {
                    if needed.contains(&record.id) {
                        return Err(Error::Session(
                            "cannot reconstruct imported Pi branch state".into(),
                        ));
                    }
                    continue;
                };
                state.clone()
            }
        } else {
            Branch::default()
        };
        branch.head = record.parent.unwrap_or(0);
        let mut migrated = entry.clone();
        if legacy
            && entry["type"] == "compaction"
            && entry["firstKeptEntryId"].is_null()
            && let Some(index) = entry["firstKeptEntryIndex"]
                .as_u64()
                .and_then(|n| usize::try_from(n).ok())
        {
            let current = source_indices[&record.id];
            let boundary = if index >= current && index < sources.len() {
                current
            } else {
                index
            };
            if let (Some(id), Some(target)) = (
                source_names.get(&record.id),
                sources
                    .get(boundary)
                    .and_then(|source| source_names.get(&source.id)),
            ) {
                migrated["id"] = Value::String(id.clone());
                migrated["firstKeptEntryId"] = Value::String(target.clone());
            }
        }
        let marker = session
            .record_metadata(record.id)
            .and_then(|value| value.get("pi_rs_import_conversion_count_v1"));
        let events = convert(&migrated, &mut branch, &branches)?;
        let mut count = events.len() as u64;
        if marker.is_none() {
            let mut local_ids = PersistentMap::new();
            let mut local_reverse = PersistentMap::new();
            let mut parent = record.id;
            let mut matches = count <= records.len() as u64 - record.id;
            for (offset, event) in
                events
                    .iter()
                    .enumerate()
                    .take(if matches { events.len() } else { 0 })
            {
                if let Event::ToolFinished { operation_id, .. }
                | Event::ToolReconciled { operation_id, .. } = event
                    && !ids.contains_key(operation_id)
                    && !local_ids.contains_key(operation_id)
                {
                    matches = false;
                    break;
                }
                let actual = &records[record.id as usize + offset];
                if actual.parent != Some(parent)
                    || actual.timestamp != record.timestamp
                    || actual.run_id.is_some()
                    || matches!(
                        actual.event,
                        Event::SourceEntry { .. } | Event::Checkout { .. }
                    )
                    || !conversion_equal(event, &actual.event, &mut local_ids, &mut local_reverse)?
                {
                    matches = false;
                    break;
                }
                parent = actual.id;
            }
            if matches && !events.is_empty() {
                for child in direct_children
                    .get(&record.id)
                    .into_iter()
                    .flatten()
                    .copied()
                    .filter(|id| *id != record.id + 1)
                {
                    let candidate = &records[(child - 1) as usize];
                    if candidate.timestamp == record.timestamp
                        && candidate.run_id.is_none()
                        && conversion_equal(
                            &events[0],
                            &candidate.event,
                            &mut PersistentMap::new(),
                            &mut PersistentMap::new(),
                        )?
                    {
                        matches = false;
                        break;
                    }
                }
            }
            if matches
                && local_ids.iter().all(|(expected, actual)| {
                    ids.get(expected).is_none_or(|old| old == actual)
                        && reverse.get(actual).is_none_or(|old| old == expected)
                })
            {
                ids.extend(local_ids.iter().map(|(key, value)| (*key, *value)));
                reverse.extend(local_reverse.iter().map(|(key, value)| (*key, *value)));
                verified.insert(record.id, count);
            } else if entry["type"] == "model_change"
                && records.get(record.id as usize).is_some_and(|next| {
                    next.parent == Some(record.id) && next.timestamp != record.timestamp
                })
            {
                // Older journals may retain the Pi selection source and then
                // append a timestamp-distinct native selection directly.
                count = 0;
                verified.insert(record.id, 0);
            } else if relevant.contains(&record.id) {
                return Err(Error::Session(
                    "ambiguous legacy imported Pi conversion provenance".into(),
                ));
            }
        }
        if needed.contains(&record.id)
            && let Some(marker) = marker
        {
            if marker.as_u64() != Some(count) || count > records.len() as u64 - record.id {
                return Err(Error::Session(
                    "invalid imported Pi conversion provenance".into(),
                ));
            }
            let mut parent = record.id;
            for (offset, event) in events.iter().enumerate() {
                if let Event::ToolFinished { operation_id, .. }
                | Event::ToolReconciled { operation_id, .. } = event
                    && !ids.contains_key(operation_id)
                {
                    return Err(Error::Session(
                        "cannot verify imported Pi tool operation identity".into(),
                    ));
                }
                let actual = &records[record.id as usize + offset];
                if actual.parent != Some(parent)
                    || actual.timestamp != record.timestamp
                    || actual.run_id.is_some()
                    || matches!(
                        actual.event,
                        Event::SourceEntry { .. } | Event::Checkout { .. }
                    )
                    || !conversion_equal(event, &actual.event, &mut ids, &mut reverse)?
                {
                    return Err(Error::Session(format!(
                        "invalid imported Pi conversion provenance for {} at offset {offset}",
                        entry["type"].as_str().unwrap_or("unknown")
                    )));
                }
                parent = actual.id;
            }
            verified.insert(record.id, count);
        }
        branch.entry_start = record.id + u64::from(count != 0);
        branch.context_target = None;
        for (offset, event) in events.iter().enumerate() {
            if matches!(
                event,
                Event::User { .. }
                    | Event::CustomMessage { .. }
                    | Event::CustomMessageDelivered { .. }
                    | Event::Assistant { .. }
                    | Event::ToolFinished { .. }
                    | Event::ToolReconciled { .. }
                    | Event::ImportedUnattributedToolResult { .. }
            ) {
                branch.context_target = Some(record.id + offset as u64 + 1);
            }
        }
        branch.head = record.id + count;
        if let Some(name) = source_names.get(&record.id) {
            branches.insert(
                name.clone(),
                Branch {
                    head: branch.head,
                    entry_start: branch.entry_start,
                    context_target: branch.context_target,
                    ..Branch::default()
                },
            );
        }
        if children.get(&record.id).copied().unwrap_or(0) > 0 {
            states.insert(record.id, branch);
        }
    }
    Ok(verified)
}
pub fn required<'a>(value: &'a Value, field: &str) -> Result<&'a str> {
    value[field]
        .as_str()
        .ok_or_else(|| Error::Session(format!("imported entry is missing {field}")))
}
fn content(value: &Value) -> Result<String> {
    if value.is_null() {
        return Ok(String::new());
    }
    if let Some(text) = value.as_str() {
        return Ok(text.into());
    }
    let blocks = value
        .as_array()
        .ok_or_else(|| Error::Session("invalid imported message content".into()))?;
    let mut text = String::new();
    for block in blocks {
        match block["type"].as_str() {
            Some("text") => text.push_str(required(block, "text")?),
            Some("thinking" | "toolCall") => {}
            _ => {
                return Err(Error::Session(
                    "this imported message content requires multimodal support".into(),
                ));
            }
        }
    }
    Ok(text)
}
fn user_content(value: &Value) -> Result<(String, Vec<crate::ImageContent>)> {
    let Some(blocks) = value.as_array() else {
        return Ok((content(value)?, Vec::new()));
    };
    let mut text = String::new();
    let mut images = Vec::new();
    for block in blocks {
        if block["type"] == "image" {
            let mime = required(block, "mimeType")?;
            let data = required(block, "data")?;
            if !matches!(
                mime,
                "image/png" | "image/jpeg" | "image/gif" | "image/webp"
            ) || base64::engine::general_purpose::STANDARD
                .decode(data)
                .is_err()
            {
                return Err(Error::Session("invalid imported image".into()));
            }
            images.push(crate::ImageContent {
                data: data.into(),
                mime_type: mime.into(),
            });
        } else {
            text.push_str(&content(&Value::Array(vec![block.clone()]))?);
        }
    }
    Ok((text, images))
}
fn push_user(events: &mut Vec<Event>, value: &Value) -> Result<()> {
    let (content, images) = user_content(value)?;
    events.push(Event::User {
        content,
        input_id: None,
    });
    if !images.is_empty() {
        events.push(Event::UserImages { images });
    }
    Ok(())
}
pub fn imported_custom_message(entry: &Value, legacy: bool) -> Result<crate::CustomMessage> {
    let value = entry.get("content");
    let content = if !legacy && value.is_none_or(Value::is_null) {
        // Pi projects missing or null custom_message content as empty blocks.
        crate::ContextContent::Blocks(Vec::new())
    } else if let Some(text) = value.and_then(Value::as_str) {
        crate::ContextContent::Text(text.into())
    } else {
        let blocks = value
            .and_then(Value::as_array)
            .ok_or_else(|| Error::Session("invalid imported custom message content".into()))?;
        let mut result = Vec::with_capacity(blocks.len());
        for block in blocks {
            match block["type"].as_str() {
                Some("text") => result.push(crate::ContentBlock::Text {
                    text: required(block, "text")?.into(),
                    text_signature: block["textSignature"].as_str().map(str::to_owned),
                }),
                Some("image") => {
                    let (_, images) = user_content(&Value::Array(vec![block.clone()]))?;
                    let image = &images[0];
                    result.push(crate::ContentBlock::Image {
                        data: image.data.clone(),
                        mime_type: image.mime_type.clone(),
                    });
                }
                _ => {
                    return Err(Error::Session(
                        "invalid imported custom message block".into(),
                    ));
                }
            }
        }
        crate::ContextContent::Blocks(result)
    };
    Ok(crate::CustomMessage {
        custom_type: if legacy {
            entry["customType"].as_str().unwrap_or_default()
        } else {
            required(entry, "customType")?
        }
        .into(),
        content,
        display: if legacy {
            entry["display"].as_bool().unwrap_or(false)
        } else {
            entry["display"].as_bool().ok_or_else(|| {
                Error::Session("imported custom message is missing display".into())
            })?
        },
        details: entry.get("details").cloned(),
    })
}

fn anthropic_replay(message: &Value) -> Option<Box<crate::ProviderReplay>> {
    if message["api"] != "anthropic-messages" {
        return None;
    }
    let effort = message["providerThinkingLevel"]
        .as_str()
        .and_then(crate::ProviderReplay::valid_anthropic_effort);
    let direct = message["provider"] == "anthropic";
    let provider = if direct {
        "anthropic".into()
    } else if message["provider"] == "openrouter" && effort.is_some() {
        format!(
            "anthropic-messages:openrouter:{:x}",
            Sha256::digest("https://openrouter.ai/api/v1/messages")
        )
    } else {
        return None;
    };
    let model = message["model"].as_str()?;
    let mut signed = false;
    let mut replay = Vec::new();
    for block in message["content"].as_array()? {
        let raw = match block["type"].as_str()? {
            "text" => serde_json::json!({"type":"text","text":block["text"].as_str()?}),
            "toolCall" => serde_json::json!({
                "type":"tool_use", "id":block["id"].as_str()?, "name":block["name"].as_str()?,
                "input":block["arguments"].as_object()?,
            }),
            "thinking" => {
                let Some(signature) = block["thinkingSignature"]
                    .as_str()
                    .filter(|value| !value.trim().is_empty())
                else {
                    continue;
                };
                signed = true;
                if block["redacted"] == true {
                    serde_json::json!({"type":"redacted_thinking","data":signature})
                } else {
                    serde_json::json!({"type":"thinking","thinking":block["thinking"].as_str()?,"signature":signature})
                }
            }
            _ => return None,
        };
        replay.push(raw);
    }
    (signed || effort.is_some()).then(|| {
        let mut replay = crate::ProviderReplay {
            identity: crate::ResolvedModelIdentity {
                provider,
                model: model.into(),
            },
            blocks: replay,
        };
        if let Some(effort) = effort {
            replay.record_anthropic_effort(effort);
        }
        Box::new(replay)
    })
}

fn google_replay(message: &Value) -> Option<Box<crate::ProviderReplay>> {
    if message["provider"] != "google" || message["api"] != "google-generative-ai" {
        return None;
    }
    let model = message["model"].as_str()?;
    let mut signed = false;
    let mut parts = Vec::new();
    for block in message["content"].as_array()? {
        let (mut part, signature) = match block["type"].as_str()? {
            "text" => (
                serde_json::json!({"text":block["text"].as_str()?}),
                &block["textSignature"],
            ),
            "thinking" => (
                serde_json::json!({"thought":true,"text":block["thinking"].as_str()?}),
                &block["thinkingSignature"],
            ),
            "toolCall" => {
                let mut call = serde_json::json!({
                    "name":block["name"].as_str()?,"args":block["arguments"].as_object()?,
                });
                if google_uses_tool_call_ids(model) {
                    call["id"] = serde_json::json!(block["id"].as_str()?);
                }
                (
                    serde_json::json!({"functionCall":call}),
                    &block["thoughtSignature"],
                )
            }
            _ => return None,
        };
        if let Some(signature) = signature.as_str().filter(|signature| !signature.is_empty()) {
            signed = true;
            part["thoughtSignature"] = serde_json::json!(signature);
        }
        parts.push(part);
    }
    signed.then(|| {
        Box::new(crate::ProviderReplay {
            identity: crate::ResolvedModelIdentity {
                provider: "google".into(),
                model: model.strip_prefix("models/").unwrap_or(model).into(),
            },
            blocks: parts,
        })
    })
}

pub fn convert(
    entry: &Value,
    branch: &mut Branch,
    branches: &BTreeMap<String, Branch>,
) -> Result<Vec<Event>> {
    let mut events = Vec::new();
    match required(entry, "type")? {
        "message" => {
            let message = &entry["message"];
            match required(message, "role")? {
                "user" => push_user(&mut events, &message["content"])?,
                "assistant" => {
                    let failure = match message["stopReason"].as_str() {
                        Some("error") => Some(crate::ModelFailure::Failed),
                        Some("aborted") => Some(crate::ModelFailure::Cancelled),
                        _ => None,
                    };
                    let mut invocations = Vec::new();
                    let mut current_ids = std::collections::BTreeSet::new();
                    if failure.is_none()
                        && let Some(blocks) = message["content"].as_array()
                    {
                        for block in blocks.iter().filter(|block| block["type"] == "toolCall") {
                            let id = required(block, "id")?.to_owned();
                            let operation_id = Uuid::new_v4();
                            if !current_ids.insert(id.clone()) {
                                return Err(Error::Session(format!(
                                    "duplicate unfinished imported tool call {id}"
                                )));
                            }
                            // A result with a reused unfinished ID cannot be attributed
                            // safely. Preserve both operations for reconciliation.
                            if branch
                                .calls
                                .insert(Arc::from(id.as_str()), operation_id)
                                .is_some()
                            {
                                branch.ambiguous_calls.insert(Arc::from(id.as_str()), ());
                            }
                            invocations.push(Invocation {
                                operation_id,
                                call: ToolCall {
                                    id,
                                    name: required(block, "name")?.into(),
                                    arguments: block["arguments"].to_string(),
                                },
                            });
                        }
                    }
                    let started: Vec<_> = invocations
                        .iter()
                        .map(|invocation| Event::ToolStarted {
                            operation_id: invocation.operation_id,
                            tool_execution: None,
                        })
                        .collect();
                    let usage = message.get("usage").map(|usage| Usage {
                        input_tokens: usage["input"]
                            .as_u64()
                            .unwrap_or(0)
                            .saturating_add(usage["cacheRead"].as_u64().unwrap_or(0))
                            .saturating_add(usage["cacheWrite"].as_u64().unwrap_or(0)),
                        output_tokens: usage["output"].as_u64().unwrap_or(0),
                    });
                    let request_id = Uuid::new_v4();
                    let reasoning = message["content"]
                        .as_array()
                        .map(|blocks| {
                            blocks
                                .iter()
                                .filter(|block| block["type"] == "thinking")
                                .filter_map(|block| block["thinking"].as_str())
                                .collect::<Vec<_>>()
                                .join("\n")
                        })
                        .filter(|text| !text.is_empty());
                    events.push(Event::ModelFinished {
                        request_id,
                        metadata: crate::ModelMetadata {
                            cost_usd: imported_cost(&message["usage"]),
                            cost_breakdown: crate::UsageCostBreakdown::from_value(
                                &message["usage"]["cost"],
                            ),
                            cost_from_provider: message["usage"]["cost"]["totalFromProvider"]
                                .as_bool(),
                            cache_write_1h_tokens: message["usage"]["cacheWrite1h"].as_u64(),
                            cached_input_tokens: message["usage"]["cacheRead"].as_u64(),
                            cache_creation_input_tokens: message["usage"]["cacheWrite"].as_u64(),
                            provider_api: message["api"].as_str().and_then(crate::ModelApi::named),
                            finish_reason: message["stopReason"].as_str().map(str::to_owned),
                            reasoning,
                            diagnostics: crate::AssistantDiagnostic::from_pi_diagnostics(
                                &message["diagnostics"],
                            ),
                            replay: anthropic_replay(message).or_else(|| google_replay(message)),
                            resolved_model: message["model"].as_str().map(|model| {
                                crate::ResolvedModelIdentity {
                                    provider: message["provider"]
                                        .as_str()
                                        .unwrap_or_default()
                                        .into(),
                                    model: if message["api"] == "google-generative-ai" {
                                        model.strip_prefix("models/").unwrap_or(model)
                                    } else {
                                        model
                                    }
                                    .into(),
                                }
                            }),
                            ..Default::default()
                        },
                        usage: usage
                            .map(|usage| crate::UsageReport::Measured { usage })
                            .unwrap_or(crate::UsageReport::Unconfirmed),
                        failure,
                        partial: String::new(),
                        reply: None,
                    });
                    events.push(Event::Assistant {
                        content: content(&message["content"])?,
                        invocations,
                        usage,
                        request_id: Some(request_id),
                    });
                    events.extend(started);
                }
                "toolResult" => {
                    append_imported_usage(&mut events, &message["usage"]);
                    let id = required(message, "toolCallId")?;
                    let (text, images) = user_content(&message["content"])?;
                    let outcome = if !images.is_empty() {
                        let data = serde_json::json!({"content":message["content"],"isError":message["isError"] == true});
                        if let Some(details) =
                            message.get("details").filter(|details| !details.is_null())
                        {
                            Outcome::StructuredWithDetails {
                                data,
                                attachments: Vec::new(),
                                details: details.clone(),
                            }
                        } else {
                            Outcome::Structured {
                                data,
                                attachments: Vec::new(),
                            }
                        }
                    } else if message["isError"] == true {
                        Outcome::TextResult {
                            output: text.clone(),
                            details: message.get("details").cloned(),
                            is_error: true,
                        }
                    } else if let Some(details) =
                        message.get("details").filter(|details| !details.is_null())
                    {
                        Outcome::SucceededWithDetails {
                            output: text.clone(),
                            details: details.clone(),
                        }
                    } else {
                        Outcome::Succeeded(text.clone())
                    };
                    if branch.ambiguous_calls.contains_key(id) {
                        let content = if let Some(blocks) = message["content"].as_array() {
                            crate::ContextContent::Blocks(
                                blocks
                                    .iter()
                                    .filter_map(|block| match block["type"].as_str() {
                                        Some("text") => Some(crate::ContentBlock::Text {
                                            text: block["text"].as_str().unwrap_or_default().into(),
                                            text_signature: block["textSignature"]
                                                .as_str()
                                                .map(str::to_owned),
                                        }),
                                        Some("image") => Some(crate::ContentBlock::Image {
                                            data: block["data"].as_str().unwrap_or_default().into(),
                                            mime_type: block["mimeType"]
                                                .as_str()
                                                .unwrap_or_default()
                                                .into(),
                                        }),
                                        _ => None,
                                    })
                                    .collect(),
                            )
                        } else {
                            crate::ContextContent::Text(text.clone())
                        };
                        events.push(Event::ImportedUnattributedToolResult {
                            call_id: id.into(),
                            tool_name: message["toolName"].as_str().unwrap_or_default().into(),
                            content,
                            is_error: message["isError"] == true,
                            details: message.get("details").cloned(),
                        });
                    } else {
                        let operation_id = branch.calls.remove(id).ok_or_else(|| {
                            Error::Session(format!("imported tool result has no call {id}"))
                        })?;
                        events.push(Event::ToolFinished {
                            operation_id,
                            outcome,
                            states: Default::default(),
                        });
                    }
                }
                "custom" | "hookMessage" => events.push(Event::CustomMessage {
                    message: imported_custom_message(message, true)?,
                }),
                "bashExecution" => events.push(Event::BashExecution {
                    execution: serde_json::from_value(message.clone())?,
                }),
                "branchSummary" => events.push(Event::BranchSummary {
                    fallback_model: None,
                    summary: required(message, "summary")?.into(),
                    extension_summary: None,
                    from: message["fromId"]
                        .as_str()
                        .and_then(|id| branches.get(id))
                        .map(|branch| branch.head),
                    target: None,
                    usage: None,
                    usage_metadata: None,
                    read_files: Vec::new(),
                    modified_files: Vec::new(),
                }),
                "compactionSummary" => events.push(Event::CompactionSummaryMessage {
                    summary: required(message, "summary")?.into(),
                    tokens_before: message["tokensBefore"].as_u64().unwrap_or(0),
                }),
                "system" => {
                    let crate::CompactedSystemPrompt {
                        content,
                        sections,
                        tools: tools_added,
                    } = crate::CompactedSystemPrompt::from_pi(message)?;
                    let tools_removed = message
                        .get("toolsRemoved")
                        .map(|value| {
                            value
                                .as_array()
                                .ok_or_else(|| Error::Session("invalid removed tools".into()))?
                                .iter()
                                .map(|tool| required(tool, "name").map(str::to_owned))
                                .collect::<Result<Vec<_>>>()
                        })
                        .transpose()?
                        .unwrap_or_default();
                    events.push(Event::SystemCheckpoint {
                        content,
                        sections,
                        tools_added,
                        tools_removed,
                    });
                }
                role => {
                    return Err(Error::Session(format!(
                        "unsupported imported message role {role}"
                    )));
                }
            }
        }
        "model_change" => {
            branch.model = required(entry, "modelId")?.into();
            branch.provider = entry["provider"].as_str().map(Arc::from);
            events.push(Event::ModelSelected {
                model: branch.model.to_string(),
                thinking: branch.thinking.as_deref().map(str::to_owned),
                provider: branch.provider.as_deref().map(str::to_owned),
            });
        }
        "thinking_level_change" => {
            branch.thinking = Some(required(entry, "thinkingLevel")?.into());
            if !branch.model.is_empty() {
                events.push(Event::ModelSelected {
                    model: branch.model.to_string(),
                    thinking: branch.thinking.as_deref().map(str::to_owned),
                    provider: branch.provider.as_deref().map(str::to_owned),
                });
            }
        }
        "session_info" => events.push(Event::SessionInfo {
            name: entry["name"].as_str().map(str::to_owned),
            cwd: None,
        }),
        "label" => {
            let target = required(entry, "targetId")?;
            let target = branches
                .get(target)
                .ok_or_else(|| Error::Session(format!("missing imported label target {target}")))?
                .entry_start;
            events.push(Event::Label {
                target,
                label: entry["label"].as_str().map(str::to_owned),
            });
        }
        "compaction" => {
            let target = required(entry, "firstKeptEntryId")?;
            let retain_from = if Some(target) == entry["id"].as_str() {
                None
            } else {
                Some(
                    branches
                        .get(target)
                        .ok_or_else(|| {
                            Error::Session(format!("missing imported compaction target {target}"))
                        })?
                        .entry_start,
                )
            };
            let usage = append_imported_usage(&mut events, &entry["usage"]);
            let usage_metadata = summary_usage_metadata(&events);
            events.push(Event::Compacted {
                summary: required(entry, "summary")?.into(),
                system_prompt: entry
                    .get("systemMessage")
                    .filter(|message| !message.is_null())
                    .map(crate::CompactedSystemPrompt::from_pi)
                    .transpose()?
                    .map(Box::new),
                retain_from,
                tokens_before: entry["tokensBefore"].as_u64(),
                states: Default::default(),
                usage,
                usage_metadata,
            });
        }
        "branch_summary" => {
            let usage = append_imported_usage(&mut events, &entry["usage"]);
            let usage_metadata = summary_usage_metadata(&events);
            let files = |key: &str| {
                entry["details"][key]
                    .as_array()
                    .filter(|_| entry["fromHook"] != true)
                    .into_iter()
                    .flatten()
                    .filter_map(Value::as_str)
                    .map(str::to_owned)
                    .collect()
            };
            events.push(Event::BranchSummary {
                fallback_model: None,
                summary: required(entry, "summary")?.into(),
                extension_summary: (entry["fromHook"] == true).then(|| {
                    crate::ExtensionTreeSummary {
                        details: entry.get("details").cloned(),
                    }
                }),
                from: entry["fromId"]
                    .as_str()
                    .and_then(|id| branches.get(id))
                    .map(|branch| branch.head),
                target: None,
                usage,
                usage_metadata,
                read_files: files("readFiles"),
                modified_files: files("modifiedFiles"),
            });
        }
        "custom_message" => events.push(Event::CustomMessage {
            message: imported_custom_message(entry, false)?,
        }),
        "custom" => {
            events.push(Event::CustomEntry {
                entry: crate::CustomEntry {
                    custom_type: required(entry, "customType")?.into(),
                    data: entry.get("data").cloned(),
                },
            });
            if entry["customType"] == "codemode-store"
                && let (Some(set), Some(deleted)) = (
                    entry["data"]["set"].as_object(),
                    entry["data"]["delete"].as_array(),
                )
                && deleted.iter().all(Value::is_string)
            {
                for key in deleted.iter().filter_map(Value::as_str) {
                    branch.store.remove(key);
                }
                branch.store.extend(
                    set.iter()
                        .map(|(key, value)| (Arc::from(key.as_str()), Arc::new(value.clone()))),
                );
                let data = serde_json::to_value(StoreView(&branch.store))?;
                if data.to_string().len() > 4 * 1024 * 1024 {
                    return Err(Error::Session(
                        "imported codemode store exceeds size limit".into(),
                    ));
                }
                events.push(Event::ExtensionStates {
                    states: [(
                        "codemode".into(),
                        crate::ExtensionState { version: 1, data },
                    )]
                    .into(),
                });
            }
        }
        "usage" => {
            append_imported_usage(&mut events, &entry["usage"]);
        }
        "context_edit" => {
            let source = required(entry, "targetId")?;
            let target = branches
                .get(source)
                .and_then(|branch| branch.context_target)
                .ok_or_else(|| {
                    Error::Session(format!("invalid imported context edit target {source}"))
                })?;
            let replacement = if entry["replacement"].is_null() {
                None
            } else {
                Some(serde_json::from_value(
                    entry["replacement"]["content"].clone(),
                )?)
            };
            events.push(Event::ContextEdited {
                target,
                replacement,
            });
        }
        kind => {
            return Err(Error::Session(format!(
                "unsupported imported entry type {kind}"
            )));
        }
    }
    Ok(events)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn pi_anthropic_effort_import_keeps_valid_levels_and_scopes_gateway_replay() {
        let mut message = json!({
            "provider":"anthropic","api":"anthropic-messages","model":"fixture",
            "providerThinkingLevel":"xhigh",
            "content":[{"type":"text","text":"reply"}]
        });
        let direct = anthropic_replay(&message).unwrap();
        assert_eq!(direct.anthropic_effort(), Some("xhigh"));
        assert_eq!(direct.identity.provider, "anthropic");
        message["provider"] = json!("openrouter");
        message["content"].as_array_mut().unwrap().insert(
            0,
            json!({
                "type":"thinking","thinking":"gateway thinking","thinkingSignature":"signature",
            }),
        );
        let gateway = anthropic_replay(&message).unwrap();
        assert_eq!(gateway.anthropic_effort(), Some("xhigh"));
        assert!(
            gateway
                .identity
                .provider
                .starts_with("anthropic-messages:openrouter:")
        );
        assert!(
            gateway
                .blocks
                .iter()
                .any(|block| block["type"] == "thinking" && block["signature"] == "signature")
        );
        message["providerThinkingLevel"] = json!("unrecognized");
        assert!(anthropic_replay(&message).is_none());
    }
}
