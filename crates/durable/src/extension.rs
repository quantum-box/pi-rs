//! Independent extension resolution and hook execution. Nothing here schedules
//! work or persists state; a host must commit memo writes before returning.
use std::{
    collections::HashSet,
    panic::AssertUnwindSafe,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use async_trait::async_trait;
use futures_util::FutureExt;
use pi_rs_core::Tool;
use serde_json::Value;

// Convert only inputs whose content order is representable by core's user message.
pub(crate) fn yield_input(value: &Value) -> Option<pi_rs_core::Message> {
    if let Some(text) = value.as_str() {
        return Some(pi_rs_core::Message::User {
            content: text.into(),
        });
    }
    let blocks = value.as_array().filter(|blocks| !blocks.is_empty())?;
    let mut text = None;
    let mut images = Vec::new();
    for block in blocks {
        let fields = block.as_object()?;
        match block.get("type")?.as_str()? {
            "text" if text.is_none() && images.is_empty() && fields.len() == 2 => {
                text = Some(block.get("text")?.as_str()?.to_owned());
            }
            "image" if fields.len() == 3 => {
                let image = pi_rs_core::ImageContent {
                    data: block.get("data")?.as_str()?.to_owned(),
                    mime_type: block.get("mimeType")?.as_str()?.to_owned(),
                };
                image.validate().ok()?;
                images.push(image);
            }
            _ => return None,
        }
    }
    let content = text.unwrap_or_default();
    Some(if images.is_empty() {
        pi_rs_core::Message::User { content }
    } else {
        pi_rs_core::Message::UserWithImages { content, images }
    })
}

#[cfg(test)]
mod yield_input_tests {
    use super::yield_input;
    use serde_json::json;

    #[test]
    fn rejects_unrepresentable_or_invalid_content_without_partial_conversion() {
        for value in [
            json!([]),
            json!([{"type":"text"}]),
            json!([{"type":"text","text":"a","textSignature":"opaque"}]),
            json!([{"type":"image","data":"aGk=","mimeType":"image/png","extra":true}]),
            json!([{"type":"text","text":"a"},{"type":"text","text":"b"}]),
            json!([{"type":"image","data":"aGk=","mimeType":"image/png"},{"type":"text","text":"after"}]),
            json!([{"type":"image","data":"invalid","mimeType":"image/png"}]),
        ] {
            assert!(yield_input(&value).is_none(), "{value}");
        }
        assert!(
            yield_input(&json!([{"type":"image","data":"aGk=","mimeType":"image/png"}])).is_some()
        );
    }
}

/// Host-provided task-scoped memo storage. `get_or_insert` must atomically
/// persist the first value before returning it, including across process restarts.
#[async_trait]
pub trait Memo: Send + Sync {
    async fn get(&self, key: &str) -> Result<Option<Value>, String>;
    async fn get_or_insert(&self, key: &str, value: Value) -> Result<Value, String>;
}

/// Durable first-writer-wins values scoped to one stable task identity.
/// A failed store operation poisons all memo handles sharing the runtime's
/// failure flag, because a failed commit may nevertheless have committed.
pub struct DurableMemo {
    owner: Arc<durable_store::owner::OwnedStore>,
    task: uuid::Uuid,
    failed: Arc<AtomicBool>,
}

impl DurableMemo {
    pub(crate) fn new(
        owner: Arc<durable_store::owner::OwnedStore>,
        task: uuid::Uuid,
        failed: Arc<AtomicBool>,
    ) -> Self {
        Self {
            owner,
            task,
            failed,
        }
    }

    fn key(&self, key: &str) -> Vec<u8> {
        let mut result = format!("runtime/extension-memo/{}/", self.task).into_bytes();
        result.extend_from_slice(key.as_bytes());
        result
    }

    fn read(&self, key: &[u8]) -> Result<Option<Value>, String> {
        if self.failed.load(Ordering::Acquire) {
            return Err("store failed; reopen before using memo".into());
        }
        let entry = self.owner.store().get(key).map_err(|e| {
            self.failed.store(true, Ordering::Release);
            e.to_string()
        })?;
        entry
            .map(|e| serde_json::from_slice(&e.value).map_err(|e| e.to_string()))
            .transpose()
    }
}

#[async_trait]
impl Memo for DurableMemo {
    async fn get(&self, key: &str) -> Result<Option<Value>, String> {
        self.read(&self.key(key))
    }

    async fn get_or_insert(&self, key: &str, value: Value) -> Result<Value, String> {
        let key = self.key(key);
        if let Some(existing) = self.read(&key)? {
            return Ok(existing);
        }
        let encoded = serde_json::to_vec(&value).map_err(|e| e.to_string())?;
        match self.owner.store().commit(
            &[durable_store::Check {
                key: key.clone(),
                expected: durable_store::Expected::Missing,
            }],
            &[durable_store::Mutation::Put {
                key: key.clone(),
                value: encoded,
            }],
        ) {
            Ok(()) => Ok(value),
            Err(durable_store::Error::Conflict { .. }) => self
                .read(&key)?
                .ok_or_else(|| "memo disappeared after conflict".into()),
            Err(error) => {
                self.failed.store(true, Ordering::Release);
                Err(error.to_string())
            }
        }
    }
}

/// Committed data and durable task-scoped memo access supplied by the host.
pub struct Context<'a> {
    pub snapshot: &'a Value,
    pub memo: &'a dyn Memo,
}

#[async_trait]
pub trait Section: Send + Sync {
    fn key(&self) -> &str;
    fn tagged(&self) -> bool {
        true
    }
    async fn render(&self, context: &Context<'_>) -> Result<Option<String>, String>;
}

#[derive(Clone, Debug, PartialEq)]
pub struct ToolCall {
    pub name: String,
    pub arguments: Value,
}
#[derive(Clone, Debug, PartialEq)]
pub struct Sleep {
    pub signals: Vec<String>,
    pub until_millis: Option<i64>,
}
#[derive(Clone, Debug, PartialEq)]
pub enum BeforeTool {
    Pass,
    Rewrite(Value),
    Block(String),
    Sleep(Sleep),
}

/// Generation events have separate chains. Values are host-defined JSON; the
/// host validates their shape before using them or scheduling requested tasks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GenerationEvent {
    BeforeRequest,
    BeforeProviderRequest,
    AfterResponse,
    OnYield,
    AfterTools,
    AfterRun,
}
#[derive(Clone, Debug, PartialEq)]
pub enum GenerationDecision {
    Pass,
    Replace(Value),
    Continue(Value),
    Tasks(Vec<HookTask>),
}
#[derive(Clone, Debug, PartialEq)]
pub struct HookTask {
    pub name: String,
    pub input: Value,
}

/// The selected message range; entry identifiers are not available in this host.
#[derive(Clone, Debug, PartialEq)]
pub struct CompactionInput {
    pub reason: CompactionReason,
    pub messages: Vec<pi_rs_core::Message>,
    pub first_kept: usize,
    pub instructions: Option<String>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum CompactionReason {
    Manual,
    Threshold,
    Overflow,
}
#[derive(Clone, Debug, PartialEq)]
pub enum BeforeCompact {
    Pass,
    Decline,
    Summary(String),
}

#[async_trait]
pub trait Hook: Send + Sync {
    async fn before_compact(
        &self,
        _input: &CompactionInput,
        _ctx: &Context<'_>,
    ) -> Result<BeforeCompact, String> {
        Ok(BeforeCompact::Pass)
    }
    async fn before_tool(
        &self,
        _call: &ToolCall,
        _ctx: &Context<'_>,
    ) -> Result<BeforeTool, String> {
        Ok(BeforeTool::Pass)
    }
    async fn after_tool(
        &self,
        _call: &ToolCall,
        _result: &Value,
        _ctx: &Context<'_>,
    ) -> Result<Option<Value>, String> {
        Ok(None)
    }
    async fn generation(
        &self,
        _event: GenerationEvent,
        _value: &Value,
        _ctx: &Context<'_>,
    ) -> Result<GenerationDecision, String> {
        Ok(GenerationDecision::Pass)
    }
}

#[derive(Clone, Default)]
pub struct Extension {
    pub name: String,
    pub tools: Vec<Arc<dyn Tool>>,
    pub sections: Vec<Arc<dyn Section>>,
    pub hooks: Vec<Arc<dyn Hook>>,
    /// Real scheduler handlers, keyed by their registered task kind.
    pub tasks: Vec<(String, Arc<dyn crate::task_scheduler::Handler>)>,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RegistryError {
    #[error("extension name must not be empty")]
    EmptyName,
    #[error("duplicate {kind} name {name} in extension {extension}")]
    Duplicate {
        kind: &'static str,
        name: String,
        extension: String,
    },
    #[error("invalid or reserved section key {0}")]
    InvalidSection(String),
    #[error("task name {0} is already installed or reserved")]
    TaskConflict(String),
}

#[derive(Clone, Default)]
pub struct Registry {
    current: Snapshot,
}
#[derive(Clone, Default)]
pub struct Snapshot {
    extensions: Vec<Arc<Extension>>,
}
impl Registry {
    pub fn snapshot(&self) -> Snapshot {
        self.current.clone()
    }
    /// Validate the entire candidate before publishing; replacement keeps position.
    pub fn install(&mut self, extension: Extension) -> Result<(), RegistryError> {
        validate(&extension)?;
        let mut next = self.current.extensions.clone();
        if let Some(i) = next.iter().position(|x| x.name == extension.name) {
            next[i] = Arc::new(extension);
        } else {
            next.push(Arc::new(extension));
        }
        let mut tasks = HashSet::new();
        for ext in &next {
            for (name, _) in &ext.tasks {
                if name.is_empty()
                    || matches!(name.as_str(), "pi.generation" | "pi.tool" | "pi.compaction")
                    || !tasks.insert(name.clone())
                {
                    return Err(RegistryError::TaskConflict(name.clone()));
                }
            }
        }
        self.current = Snapshot { extensions: next };
        Ok(())
    }
    pub fn uninstall(&mut self, name: &str) {
        self.current.extensions.retain(|x| x.name != name);
    }
}
fn validate(ext: &Extension) -> Result<(), RegistryError> {
    if ext.name.trim().is_empty() {
        return Err(RegistryError::EmptyName);
    }
    let mut tools = HashSet::new();
    for t in &ext.tools {
        let name = t.spec().name;
        if name.is_empty() || !tools.insert(name.clone()) {
            return Err(RegistryError::Duplicate {
                kind: "tool",
                name,
                extension: ext.name.clone(),
            });
        }
    }
    let mut sections = HashSet::new();
    for s in &ext.sections {
        let key = s.key();
        let mut chars = key.chars();
        if !matches!(chars.next(), Some('a'..='z'))
            || !chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '-')
            || key == "instructions"
        {
            return Err(RegistryError::InvalidSection(key.into()));
        }
        if !sections.insert(key.to_string()) {
            return Err(RegistryError::Duplicate {
                kind: "section",
                name: key.into(),
                extension: ext.name.clone(),
            });
        }
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Report {
    pub extension: String,
    pub operation: String,
    pub error: String,
}
fn failure(extension: &str, operation: &'static str, error: String) -> Report {
    Report {
        extension: extension.into(),
        operation: operation.into(),
        error,
    }
}
// Callbacks must be invoked inside the future passed here: a hand-written
// async-trait implementation can panic before it returns its future.
async fn protect<T>(f: impl std::future::Future<Output = Result<T, String>>) -> Result<T, String> {
    match AssertUnwindSafe(f).catch_unwind().await {
        Ok(result) => result,
        Err(payload) => {
            discard_panic_payload(payload);
            Err("extension panicked".into())
        }
    }
}

pub(crate) fn discard_panic_payload(payload: Box<dyn std::any::Any + Send>) {
    // A user-defined payload may panic when dropped. Its new panic payload may
    // have the same property, so it cannot safely be dropped here.
    if let Err(secondary) = std::panic::catch_unwind(AssertUnwindSafe(|| drop(payload))) {
        std::mem::forget(secondary);
    }
}

#[cfg(test)]
mod panic_boundary_tests {
    use super::protect;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    #[tokio::test]
    async fn cancelling_pending_callback_drops_it_without_completing() {
        struct OnDrop(Arc<AtomicBool>);
        impl Drop for OnDrop {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }
        let dropped = Arc::new(AtomicBool::new(false));
        let guard = OnDrop(dropped.clone());
        let future = protect(async move {
            let _guard = guard;
            std::future::pending::<Result<(), String>>().await
        });
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), future)
                .await
                .is_err()
        );
        assert!(dropped.load(Ordering::SeqCst));
    }
}

/// An immutable phase-local selection, in the requested order. Missing names are
/// ignored and duplicate selections use their first occurrence.
#[derive(Clone, Default)]
pub struct Resolved {
    extensions: Vec<Arc<Extension>>,
}
impl Snapshot {
    pub fn names(&self) -> Vec<&str> {
        self.extensions.iter().map(|x| x.name.as_str()).collect()
    }
    pub fn resolve(&self, names: Option<&[String]>) -> Resolved {
        let extensions = match names {
            None => self.extensions.clone(),
            Some(names) => {
                let mut seen = HashSet::new();
                names
                    .iter()
                    .filter(|n| seen.insert((*n).clone()))
                    .filter_map(|n| self.extensions.iter().find(|x| x.name == *n).cloned())
                    .collect()
            }
        };
        Resolved { extensions }
    }
}
impl Resolved {
    pub fn tool(&self, name: &str) -> Option<Arc<dyn Tool>> {
        self.extensions
            .iter()
            .rev()
            .flat_map(|e| e.tools.iter().rev())
            .find(|t| t.spec().name == name)
            .cloned()
    }
    /// Effective tools, with later selected extensions replacing earlier ones.
    pub fn tools(&self) -> Vec<Arc<dyn Tool>> {
        let mut tools = std::collections::BTreeMap::new();
        for extension in &self.extensions {
            for tool in &extension.tools {
                tools.insert(tool.spec().name, tool.clone());
            }
        }
        tools.into_values().collect()
    }
    pub fn task(&self, name: &str) -> Option<Arc<dyn crate::task_scheduler::Handler>> {
        self.extensions
            .iter()
            .flat_map(|e| e.tasks.iter())
            .find(|(n, _)| n == name)
            .map(|(_, h)| h.clone())
    }
    pub(crate) fn task_owner(&self, name: &str) -> Option<&str> {
        self.extensions
            .iter()
            .find(|e| e.tasks.iter().any(|(kind, _)| kind == name))
            .map(|e| e.name.as_str())
    }
    /// Later sections replace by key without moving the key's original position.
    pub async fn prompt(
        &self,
        ctx: &Context<'_>,
        instructions: Option<&str>,
    ) -> (String, Vec<Report>) {
        let mut sections: Vec<(&str, &Arc<dyn Section>)> = Vec::new();
        for e in &self.extensions {
            for s in &e.sections {
                if let Some(i) = sections.iter().position(|(_, old)| old.key() == s.key()) {
                    sections[i] = (&e.name, s);
                } else {
                    sections.push((&e.name, s));
                }
            }
        }
        let mut parts = Vec::new();
        let mut reports = Vec::new();
        for (owner, s) in sections {
            match protect(async { s.render(ctx).await }).await {
                Ok(Some(text)) => parts.push(if s.tagged() {
                    format!("<{}>\n{}\n</{}>", s.key(), text, s.key())
                } else {
                    text
                }),
                Ok(None) => (),
                Err(e) => reports.push(failure(owner, "section", e)),
            }
        }
        if let Some(text) = instructions {
            parts.push(format!("<instructions>\n{text}\n</instructions>"));
        }
        (parts.join("\n\n"), reports)
    }
    /// First decision wins; ordinary hook errors are collected and skipped.
    pub async fn before_compact(
        &self,
        input: &CompactionInput,
        ctx: &Context<'_>,
    ) -> (BeforeCompact, Vec<Report>) {
        let mut reports = vec![];
        for e in &self.extensions {
            for h in &e.hooks {
                match protect(async { h.before_compact(input, ctx).await }).await {
                    Ok(BeforeCompact::Pass) => (),
                    Ok(decision) => return (decision, reports),
                    Err(err) => reports.push(failure(&e.name, "before_compact", err)),
                }
            }
        }
        (BeforeCompact::Pass, reports)
    }
    /// Any hook failure stops dispatch; a host must also validate a sleep and
    /// re-run the complete chain on wake before dispatching.
    pub async fn before_tool(
        &self,
        mut call: ToolCall,
        ctx: &Context<'_>,
    ) -> (ToolCall, BeforeTool, Vec<Report>) {
        for e in &self.extensions {
            for h in &e.hooks {
                match protect(async { h.before_tool(&call, ctx).await }).await {
                    Ok(BeforeTool::Pass) => (),
                    Ok(BeforeTool::Rewrite(args)) => call.arguments = args,
                    Ok(stop @ (BeforeTool::Block(_) | BeforeTool::Sleep(_))) => {
                        return (call, stop, vec![]);
                    }
                    Err(err) => {
                        let report = failure(&e.name, "before_tool", err);
                        return (
                            call,
                            BeforeTool::Block("before-tool hook failed".into()),
                            vec![report],
                        );
                    }
                }
            }
        }
        (call, BeforeTool::Pass, vec![])
    }
    pub async fn after_tool(
        &self,
        call: &ToolCall,
        mut result: Value,
        ctx: &Context<'_>,
    ) -> (Value, Vec<Report>) {
        let mut reports = vec![];
        for e in &self.extensions {
            for h in &e.hooks {
                match protect(async { h.after_tool(call, &result, ctx).await }).await {
                    Ok(Some(value)) => result = value,
                    Ok(None) => (),
                    Err(err) => reports.push(failure(&e.name, "after_tool", err)),
                }
            }
        }
        (result, reports)
    }
    /// Request replacements pass down the chain; the first onYield decision wins.
    /// Returned task requests are only collected, never scheduled here.
    pub async fn generation(
        &self,
        event: GenerationEvent,
        value: Value,
        ctx: &Context<'_>,
    ) -> (Value, Option<Value>, Vec<HookTask>, Vec<Report>) {
        self.generation_inner(event, value, ctx, false).await
    }
    pub(crate) async fn lifecycle_generation(
        &self,
        event: GenerationEvent,
        value: Value,
        ctx: &Context<'_>,
    ) -> (Value, Option<Value>, Vec<HookTask>, Vec<Report>) {
        self.generation_inner(event, value, ctx, true).await
    }
    async fn generation_inner(
        &self,
        event: GenerationEvent,
        mut value: Value,
        ctx: &Context<'_>,
        validate_owner: bool,
    ) -> (Value, Option<Value>, Vec<HookTask>, Vec<Report>) {
        let mut tasks = vec![];
        let mut reports = vec![];
        for e in &self.extensions {
            for h in &e.hooks {
                match protect(async { h.generation(event, &value, ctx).await }).await {
                    Ok(GenerationDecision::Pass) => (),
                    Ok(GenerationDecision::Replace(v))
                        if matches!(
                            event,
                            GenerationEvent::BeforeRequest | GenerationEvent::BeforeProviderRequest
                        ) =>
                    {
                        if event == GenerationEvent::BeforeProviderRequest {
                            if let Some(payload) = v.get("payload") {
                                value["payload"] = payload.clone();
                            } else {
                                reports.push(failure(
                                    &e.name,
                                    "generation",
                                    "provider replacement omitted payload".into(),
                                ));
                            }
                        } else {
                            value = v;
                        }
                    }
                    Ok(GenerationDecision::Continue(v)) if event == GenerationEvent::OnYield => {
                        if yield_input(&v).is_some() {
                            return (value, Some(v), tasks, reports);
                        }
                        reports.push(failure(
                            &e.name,
                            "generation",
                            "OnYield continuation is invalid or cannot be represented".into(),
                        ));
                    }
                    Ok(GenerationDecision::Tasks(mut t))
                        if matches!(
                            event,
                            GenerationEvent::OnYield
                                | GenerationEvent::AfterTools
                                | GenerationEvent::AfterRun
                        ) =>
                    {
                        if validate_owner {
                            t.retain(|task| {
                                let valid = e.tasks.iter().any(|(name, _)| name == &task.name);
                                if !valid {
                                    reports.push(failure(&e.name, "generation", format!("unselected task registration or task owned by another extension: {}", task.name)));
                                }
                                valid
                            });
                        }
                        let has_tasks = !t.is_empty();
                        tasks.append(&mut t);
                        if event == GenerationEvent::OnYield && has_tasks {
                            return (value, None, tasks, reports);
                        }
                    }
                    Ok(_) => reports.push(failure(
                        &e.name,
                        "generation",
                        "decision is invalid for this event".into(),
                    )),
                    Err(err) => reports.push(failure(&e.name, "generation", err)),
                }
            }
        }
        (value, None, tasks, reports)
    }
}
