//! Observation hooks for sessions, agent runs, and completed model turns.
use crate::runtime::time as platform_time;
use std::path::PathBuf;
use std::time::Duration;
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    ExtensionStates, Message, Notification, Observer, Record, Registry, RunState, Session,
    ToolHost, Usage,
};

/// Branch navigation as selected by the user, before any session changes.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TreePreparation {
    #[serde(alias = "target_id")]
    pub target_id: u64,
    #[serde(alias = "old_leaf_id")]
    pub old_leaf_id: u64,
    #[serde(alias = "common_ancestor_id")]
    pub common_ancestor_id: Option<u64>,
    #[serde(alias = "entries_to_summarize")]
    pub entries_to_summarize: Vec<Record>,
    #[serde(alias = "user_wants_summary")]
    pub user_wants_summary: bool,
    #[serde(alias = "custom_instructions")]
    pub custom_instructions: Option<String>,
    #[serde(default, alias = "replace_instructions")]
    pub replace_instructions: bool,
    #[serde(default)]
    pub label: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct TreeDecision {
    pub cancel: bool,
    pub summary: Option<TreeSummary>,
    #[serde(alias = "custom_instructions")]
    pub custom_instructions: Option<String>,
    #[serde(alias = "replace_instructions")]
    pub replace_instructions: Option<bool>,
    pub label: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TreeSummary {
    pub summary: String,
    pub details: Option<serde_json::Value>,
    pub usage: Option<Usage>,
}

/// Presence marks a branch summary supplied by an extension.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ExtensionTreeSummary {
    pub details: Option<serde_json::Value>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionStartReason {
    Startup,
    Reload,
    New,
    Resume,
    Fork,
}

/// How a model was selected by the host.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelSelectSource {
    Set,
    Cycle,
    Restore,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionShutdownReason {
    Quit,
    Reload,
    New,
    Resume,
    Fork,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionSwitchReason {
    New,
    Resume,
}

/// What initiated a model-generated context compaction.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompactionReason {
    Manual,
    Threshold,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ForkPosition {
    Before,
    At,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum BeforeSessionTransition {
    #[serde(rename = "session_before_switch")]
    Switch {
        reason: SessionSwitchReason,
        #[serde(rename = "targetSessionFile", alias = "target_session_file")]
        target_session_file: Option<PathBuf>,
    },
    #[serde(rename = "session_before_fork")]
    Fork {
        #[serde(rename = "entryId", alias = "entry_id")]
        entry_id: u64,
        position: ForkPosition,
    },
}

impl BeforeSessionTransition {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Switch { .. } => "session_before_switch",
            Self::Fork { .. } => "session_before_fork",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum LifecycleEvent {
    ModelSelect {
        model: serde_json::Value,
        #[serde(rename = "previousModel")]
        previous_model: Option<serde_json::Value>,
        source: ModelSelectSource,
    },
    ThinkingLevelSelect {
        level: String,
        #[serde(rename = "previousLevel")]
        previous_level: String,
    },
    SessionInfoChanged {
        name: Option<String>,
    },
    SessionCompact {
        #[serde(rename = "compactionEntry")]
        compaction_entry: Box<Record>,
        #[serde(rename = "fromExtension")]
        from_extension: bool,
        reason: CompactionReason,
        #[serde(rename = "willRetry")]
        will_retry: bool,
    },
    SessionCompactFailed {
        reason: CompactionReason,
        #[serde(rename = "errorMessage", skip_serializing_if = "Option::is_none")]
        error_message: Option<String>,
        aborted: bool,
        #[serde(rename = "willRetry")]
        will_retry: bool,
        #[serde(rename = "fromExtension")]
        from_extension: bool,
    },
    SessionStart {
        reason: SessionStartReason,
        #[serde(rename = "previousSessionFile", alias = "previous_session_file")]
        previous_session_file: Option<PathBuf>,
    },
    SessionShutdown {
        reason: SessionShutdownReason,
        #[serde(rename = "targetSessionFile", alias = "target_session_file")]
        target_session_file: Option<PathBuf>,
    },
    AgentStart {
        #[serde(rename = "sessionId", alias = "session_id")]
        session_id: Uuid,
        #[serde(rename = "runId", alias = "run_id")]
        run_id: Uuid,
        model: String,
    },
    AgentEnd {
        #[serde(rename = "sessionId", alias = "session_id")]
        session_id: Uuid,
        #[serde(rename = "runId", alias = "run_id")]
        run_id: Uuid,
        state: RunState,
        messages: Vec<LifecycleMessage>,
    },
    /// The run is idle after retries, compaction and queued continuations.
    AgentSettled {
        aborted: bool,
    },
    UiPromptStart {
        reason: crate::UiPromptReason,
        kind: crate::UiPromptKind,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        title: Option<String>,
    },
    UiPromptEnd {
        reason: crate::UiPromptReason,
        kind: crate::UiPromptKind,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        title: Option<String>,
    },
    TurnStart {
        #[serde(rename = "runId", alias = "run_id")]
        run_id: Uuid,
        #[serde(rename = "turnIndex", alias = "turn_index")]
        turn_index: u32,
        timestamp: u64,
        recovered: bool,
    },
    TurnEnd {
        #[serde(rename = "runId", alias = "run_id")]
        run_id: Uuid,
        #[serde(rename = "turnIndex", alias = "turn_index")]
        turn_index: u32,
        messages: Vec<LifecycleMessage>,
    },
    McpServersChange {
        servers: Vec<crate::RegisteredMcpServer>,
    },
    ToolExecutionStart {
        #[serde(rename = "toolCallId")]
        tool_call_id: String,
        #[serde(rename = "toolName")]
        tool_name: String,
        args: serde_json::Value,
        #[serde(rename = "parentToolCallId", skip_serializing_if = "Option::is_none")]
        parent_tool_call_id: Option<String>,
    },
    ToolExecutionUpdate {
        #[serde(rename = "toolCallId")]
        tool_call_id: String,
        #[serde(rename = "toolName")]
        tool_name: String,
        args: serde_json::Value,
        #[serde(rename = "partialResult")]
        partial_result: serde_json::Value,
        #[serde(rename = "parentToolCallId", skip_serializing_if = "Option::is_none")]
        parent_tool_call_id: Option<String>,
    },
    ToolExecutionEnd {
        #[serde(rename = "toolCallId")]
        tool_call_id: String,
        #[serde(rename = "toolName")]
        tool_name: String,
        result: crate::Outcome,
        #[serde(rename = "isError")]
        is_error: bool,
        #[serde(rename = "durationMs", skip_serializing_if = "Option::is_none")]
        duration_ms: Option<u64>,
        #[serde(rename = "parentToolCallId", skip_serializing_if = "Option::is_none")]
        parent_tool_call_id: Option<String>,
    },
    SessionTree {
        #[serde(rename = "newLeafId", alias = "new_leaf_id")]
        new_leaf_id: u64,
        #[serde(rename = "oldLeafId", alias = "old_leaf_id")]
        old_leaf_id: u64,
        #[serde(rename = "summaryEntry", alias = "summary_entry")]
        summary_entry: Option<Box<Record>>,
        #[serde(rename = "fromExtension", alias = "from_extension")]
        from_extension: Option<bool>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LifecycleMessage {
    #[serde(alias = "record_id")]
    pub record_id: u64,
    pub message: Message,
}

impl LifecycleEvent {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::ModelSelect { .. } => "model_select",
            Self::ThinkingLevelSelect { .. } => "thinking_level_select",
            Self::SessionInfoChanged { .. } => "session_info_changed",
            Self::SessionCompact { .. } => "session_compact",
            Self::SessionCompactFailed { .. } => "session_compact_failed",
            Self::SessionStart { .. } => "session_start",
            Self::SessionShutdown { .. } => "session_shutdown",
            Self::AgentStart { .. } => "agent_start",
            Self::AgentEnd { .. } => "agent_end",
            Self::AgentSettled { .. } => "agent_settled",
            Self::UiPromptStart { .. } => "ui_prompt_start",
            Self::UiPromptEnd { .. } => "ui_prompt_end",
            Self::TurnStart { .. } => "turn_start",
            Self::TurnEnd { .. } => "turn_end",
            Self::McpServersChange { .. } => "mcp_servers_change",
            Self::ToolExecutionStart { .. } => "tool_execution_start",
            Self::ToolExecutionUpdate { .. } => "tool_execution_update",
            Self::ToolExecutionEnd { .. } => "tool_execution_end",
            Self::SessionTree { .. } => "session_tree",
        }
    }
}

pub(crate) fn now() -> u64 {
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    {
        js_sys::Date::now().max(0.0) as u64
    }
    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }
}

pub(crate) fn messages(session: &Session, include: impl Fn(u64) -> bool) -> Vec<LifecycleMessage> {
    session
        .current_message_entries()
        .into_iter()
        .filter(|(id, _)| include(*id))
        .map(|(record_id, mut message)| {
            if let Message::Assistant { replay, .. } = &mut message {
                *replay = None;
            }
            LifecycleMessage { record_id, message }
        })
        .collect()
}

pub(crate) fn enabled(registry: &Registry) -> bool {
    registry
        .extensions
        .iter()
        .any(|extension| extension.handles_lifecycle())
}

pub async fn notify(
    registry: &Registry,
    event: LifecycleEvent,
    states: &ExtensionStates,
    timeout: Duration,
    observer: &mut Observer<'_>,
) {
    notify_filtered(registry, event, states, timeout, observer, |_| true).await;
}

#[doc(hidden)]
pub async fn notify_filtered(
    registry: &Registry,
    event: LifecycleEvent,
    states: &ExtensionStates,
    timeout: Duration,
    observer: &mut Observer<'_>,
    mut include: impl FnMut(&str) -> bool + Send,
) {
    notify_inner(
        registry,
        event,
        states,
        timeout,
        (observer, &mut include),
        None,
        None,
    )
    .await;
}

pub(crate) async fn notify_with_host(
    registry: &Registry,
    event: LifecycleEvent,
    states: &ExtensionStates,
    timeout: Duration,
    observer: &mut Observer<'_>,
    host: std::sync::Arc<dyn crate::ToolHost>,
) {
    notify_inner(
        registry,
        event,
        states,
        timeout,
        (observer, &mut |_| true),
        Some(host),
        None,
    )
    .await;
}

pub(crate) async fn notify_idle_with_host(
    registry: &Registry,
    session: &mut Session,
    event: LifecycleEvent,
    timeout: Duration,
    observer: &mut Observer<'_>,
) -> crate::Result<()> {
    notify_session_with_host(registry, session, event, timeout, observer, true).await
}

/// Deliver an idle lifecycle event while the caller owns the session journal.
pub async fn notify_in_session(
    registry: &Registry,
    session: &mut Session,
    event: LifecycleEvent,
    timeout: Duration,
    observer: &mut Observer<'_>,
) -> crate::Result<()> {
    notify_idle_with_host(registry, session, event, timeout, observer).await
}

pub(crate) async fn notify_session_with_host(
    registry: &Registry,
    session: &mut Session,
    event: LifecycleEvent,
    timeout: Duration,
    observer: &mut Observer<'_>,
    idle: bool,
) -> crate::Result<()> {
    notify_session_compaction_with_host(registry, session, event, timeout, observer, idle, None)
        .await
}

pub(crate) async fn notify_session_compaction_with_host(
    registry: &Registry,
    session: &mut Session,
    event: LifecycleEvent,
    timeout: Duration,
    observer: &mut Observer<'_>,
    idle: bool,
    compaction: Option<(crate::CompactionTrigger, Option<serde_json::Value>)>,
) -> crate::Result<()> {
    if idle
        && compaction.is_none()
        && matches!(
            event,
            LifecycleEvent::SessionStart { .. }
                | LifecycleEvent::SessionShutdown { .. }
                | LifecycleEvent::SessionInfoChanged { .. }
        )
    {
        return notify_idle_session_entries(registry, session, event, timeout, observer).await;
    }
    notify_session_callback(
        registry,
        session,
        Some(event),
        timeout,
        observer,
        idle,
        compaction,
    )
    .await
}

pub(crate) async fn notify_mcp_registrations_in_session(
    registry: &Registry,
    session: &mut Session,
    timeout: Duration,
    observer: &mut Observer<'_>,
) -> crate::Result<()> {
    notify_session_callback(registry, session, None, timeout, observer, true, None).await
}

async fn notify_session_callback(
    registry: &Registry,
    session: &mut Session,
    event: Option<LifecycleEvent>,
    timeout: Duration,
    observer: &mut Observer<'_>,
    idle: bool,
    compaction: Option<(crate::CompactionTrigger, Option<serde_json::Value>)>,
) -> crate::Result<()> {
    let states = session.extension_states();
    let (sender, mut incoming) = tokio::sync::mpsc::channel(32);
    let host = std::sync::Arc::new(crate::agent::LifecycleMessageHost { sender, idle });
    let mut errors = Vec::new();
    let mut host_error = None;
    let mut collect = |event: Notification<'_>| {
        if let Notification::ExtensionLifecycleError {
            extension,
            event,
            message,
        } = event
        {
            errors.push((extension.to_owned(), event, message.to_owned()));
        }
    };
    {
        let mut include_all = |_: &str| true;
        let future = async {
            match event {
                Some(event) => {
                    notify_inner(
                        registry,
                        event,
                        &states,
                        timeout,
                        (&mut collect, &mut include_all),
                        Some(host),
                        compaction,
                    )
                    .await
                }
                None => {
                    deliver_mcp_changes(registry, &states, timeout, &mut collect, Some(host)).await
                }
            }
        };
        tokio::pin!(future);
        loop {
            tokio::select! {
                biased;
                Some((input, reply)) = incoming.recv() => {
                    let result = if !idle || matches!(input.delivery, crate::CustomMessageDelivery::NextTurn | crate::CustomMessageDelivery::TriggerTurn) {
                        crate::agent::queue_custom_message(session, input, observer)
                    } else {
                        session.append_custom_message(input.message).map(|()| observer(Notification::Committed(session.records().last().expect("appended custom message"))))
                    };
                    match result {
                        Ok(()) => { let _ = reply.send(Ok(())); }
                        Err(error) => {
                            let _ = reply.send(Err(error.to_string()));
                            if host_error.is_none() { host_error = Some(error); }
                        }
                    }
                }
                _ = &mut future => break,
            }
        }
    }
    for (extension, event, message) in &errors {
        observer(Notification::ExtensionLifecycleError {
            extension,
            event,
            message,
        });
    }
    match host_error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

async fn notify_idle_session_entries(
    registry: &Registry,
    session: &mut Session,
    event: LifecycleEvent,
    timeout: Duration,
    observer: &mut Observer<'_>,
) -> crate::Result<()> {
    use crate::extension::{SessionEntryHost, SessionEntryRequest, branch_custom_entries};
    let states = session.extension_states();
    let deadline = platform_time::Instant::now() + timeout.min(Duration::from_secs(10));
    let mut errors = Vec::new();
    let mut host_error = None;
    for extension in registry
        .extensions
        .iter()
        .filter(|extension| extension.handles_lifecycle() && extension.handles_session_lifecycle())
    {
        let (sender, mut messages) = tokio::sync::mpsc::channel(32);
        let (entry_sender, mut entries) = tokio::sync::mpsc::channel(32);
        let host = std::sync::Arc::new(SessionEntryHost {
            messages: std::sync::Arc::new(crate::agent::LifecycleMessageHost {
                sender,
                idle: true,
            }),
            entries: entry_sender,
        });
        enum Request {
            Entry(SessionEntryRequest),
            Message(
                crate::QueuedCustomMessage,
                tokio::sync::oneshot::Sender<crate::ExtensionResult<()>>,
            ),
        }
        let result = {
            let future = platform_time::timeout_at(
                deadline,
                crate::sdk::scope_extension_callback(host.callback_scope_owner(), async {
                    let host = crate::sdk::scope_callback_host(host);
                    if let Some(scope) = registry.mcp_callback_scope(extension.as_ref()) {
                        extension
                            .lifecycle_with_mcp_registry(
                                &event,
                                states.get(extension.id()),
                                host,
                                scope.host(),
                            )
                            .await
                    } else {
                        extension
                            .lifecycle_with_host(&event, states.get(extension.id()), host)
                            .await
                    }
                }),
            );
            tokio::pin!(future);
            'callback: loop {
                tokio::select! {
                    biased;
                    result = &mut future => break result,
                    request = async {
                        tokio::select! {
                            Some(entry) = entries.recv() => Request::Entry(entry),
                            Some((message, reply)) = messages.recv() => Request::Message(message, reply),
                            else => std::future::pending().await,
                        }
                    } => match request {
                        Request::Entry(SessionEntryRequest::Append(entry, reply)) => {
                            if reply.is_closed() { continue; }
                            if let Err(error) = session.ensure_settled() { let _ = reply.send(Err(error.to_string())); continue; }
                            let result = session.append_custom_entry(entry).map(|()| observer(Notification::Committed(session.records().last().expect("appended custom entry"))));
                            match result {
                                Ok(()) => { let _ = reply.send(Ok(())); }
                                Err(error) => { let _ = reply.send(Err(error.to_string())); if host_error.is_none() { host_error = Some(error); } }
                            }
                        }
                        Request::Entry(SessionEntryRequest::List(custom_type, mut reply)) => {
                            if reply.is_closed() { continue; }
                            tokio::select! {
                                biased;
                                result = &mut future => break 'callback result,
                                _ = reply.closed() => {},
                                result = branch_custom_entries(session, custom_type.as_deref()) => { let _ = reply.send(result); }
                            }
                        }
                        Request::Message(input, reply) => {
                            let result = if matches!(input.delivery, crate::CustomMessageDelivery::NextTurn | crate::CustomMessageDelivery::TriggerTurn) {
                                crate::agent::queue_custom_message(session, input, observer)
                            } else {
                                session.append_custom_message(input.message).map(|()| observer(Notification::Committed(session.records().last().expect("appended custom message"))))
                            };
                            match result {
                                Ok(()) => { let _ = reply.send(Ok(())); }
                                Err(error) => { let _ = reply.send(Err(error.to_string())); if host_error.is_none() { host_error = Some(error); } }
                            }
                        }
                    }
                }
            }
        };
        match result {
            Ok(Ok(())) => {}
            Ok(Err(error)) => errors.push((extension.id().to_owned(), error)),
            Err(_) => errors.push((extension.id().to_owned(), "lifecycle hook timed out".into())),
        }
    }
    for (extension, message) in &errors {
        observer(Notification::ExtensionLifecycleError {
            extension,
            event: event.kind(),
            message,
        });
    }
    let change_result =
        notify_mcp_registrations_in_session(registry, session, timeout, observer).await;
    match host_error {
        Some(error) => Err(error),
        None => change_result,
    }
}

async fn notify_inner(
    registry: &Registry,
    event: LifecycleEvent,
    states: &ExtensionStates,
    timeout: Duration,
    (observer, include): (&mut Observer<'_>, &mut (dyn FnMut(&str) -> bool + Send)),
    host: Option<std::sync::Arc<dyn crate::ToolHost>>,
    compaction: Option<(crate::CompactionTrigger, Option<serde_json::Value>)>,
) {
    notify_inner_once(
        registry,
        event,
        states,
        timeout,
        (observer, include),
        host.clone(),
        compaction,
    )
    .await;
    deliver_mcp_changes(registry, states, timeout, observer, host).await;
}

async fn deliver_mcp_changes(
    registry: &Registry,
    states: &ExtensionStates,
    timeout: Duration,
    observer: &mut Observer<'_>,
    host: Option<std::sync::Arc<dyn crate::ToolHost>>,
) {
    let runtime = &registry.mcp_servers;
    let Some(mut delivery) = runtime.begin_delivery() else {
        return;
    };
    let deadline = platform_time::Instant::now() + timeout.min(Duration::from_secs(10));
    // A callback can enqueue new changes while processing a notification.
    // Limit one delivery wave even when the runtime clock is paused.
    for _ in 0..32 {
        let remaining = deadline.saturating_duration_since(platform_time::Instant::now());
        if remaining.is_zero() {
            return;
        }
        let Some(servers) = delivery.pop_change() else {
            return;
        };
        notify_inner_once(
            registry,
            LifecycleEvent::McpServersChange { servers },
            states,
            remaining,
            (observer, &mut |_| true),
            host.clone(),
            None,
        )
        .await;
        tokio::task::yield_now().await;
    }
}

async fn notify_inner_once(
    registry: &Registry,
    event: LifecycleEvent,
    states: &ExtensionStates,
    timeout: Duration,
    (observer, include): (&mut Observer<'_>, &mut (dyn FnMut(&str) -> bool + Send)),
    host: Option<std::sync::Arc<dyn crate::ToolHost>>,
    compaction: Option<(crate::CompactionTrigger, Option<serde_json::Value>)>,
) {
    let deadline = platform_time::Instant::now() + timeout.min(Duration::from_secs(10));
    for extension in registry.extensions.iter().filter(|extension| match event {
        LifecycleEvent::McpServersChange { .. } => extension.handles_mcp_servers_change(),
        _ if !extension.handles_lifecycle() => false,
        LifecycleEvent::ModelSelect { .. } | LifecycleEvent::ThinkingLevelSelect { .. } => {
            extension.handles_model_selection()
        }
        LifecycleEvent::SessionStart { .. }
        | LifecycleEvent::SessionShutdown { .. }
        | LifecycleEvent::SessionInfoChanged { .. }
        | LifecycleEvent::SessionCompact { .. }
        | LifecycleEvent::SessionCompactFailed { .. } => extension.handles_session_lifecycle(),
        LifecycleEvent::SessionTree { .. } => extension.handles_tree_navigation(),
        LifecycleEvent::UiPromptStart { .. } | LifecycleEvent::UiPromptEnd { .. } => {
            extension.handles_ui_prompt_lifecycle()
        }
        LifecycleEvent::ToolExecutionStart { .. } | LifecycleEvent::ToolExecutionEnd { .. } => {
            extension.handles_tool_execution_lifecycle()
        }
        LifecycleEvent::ToolExecutionUpdate { .. } => {
            extension.handles_tool_execution_lifecycle()
                && extension.handles_tool_execution_updates()
        }
        _ => true,
    }) {
        let error = match platform_time::timeout_at(
            deadline,
            crate::sdk::scope_extension_callback(
                host.as_ref().and_then(|host| host.callback_scope_owner()),
                async {
                    if !include(extension.id()) {
                        return Ok(());
                    }
                    match &host {
                        Some(host) => {
                            let scoped = crate::sdk::scope_callback_host(
                                host.for_extension(extension.id())
                                    .unwrap_or_else(|| host.clone()),
                            );
                            if let Some((trigger, metadata)) = &compaction {
                                extension
                                    .compaction_lifecycle_with_host(
                                        &event,
                                        *trigger,
                                        metadata.as_ref(),
                                        states.get(extension.id()),
                                        scoped,
                                    )
                                    .await
                            } else if let Some(scope) =
                                registry.mcp_callback_scope(extension.as_ref())
                            {
                                extension
                                    .lifecycle_with_mcp_registry(
                                        &event,
                                        states.get(extension.id()),
                                        scoped,
                                        scope.host(),
                                    )
                                    .await
                            } else {
                                extension
                                    .lifecycle_with_host(&event, states.get(extension.id()), scoped)
                                    .await
                            }
                        }
                        None => {
                            if let Some(scope) = registry.mcp_callback_scope(extension.as_ref()) {
                                extension
                                    .lifecycle_with_mcp_registry(
                                        &event,
                                        states.get(extension.id()),
                                        std::sync::Arc::new(McpOnlyHost),
                                        scope.host(),
                                    )
                                    .await
                            } else {
                                extension
                                    .lifecycle(&event, states.get(extension.id()))
                                    .await
                            }
                        }
                    }
                },
            ),
        )
        .await
        {
            Ok(Ok(())) => continue,
            Ok(Err(error)) => error,
            Err(_) => "lifecycle hook timed out".into(),
        };
        observer(Notification::ExtensionLifecycleError {
            extension: extension.id(),
            event: event.kind(),
            message: &error,
        });
    }
}

struct McpOnlyHost;

#[async_trait::async_trait]
impl crate::ToolHost for McpOnlyHost {
    fn tools(&self) -> Vec<crate::ToolSpec> {
        Vec::new()
    }

    async fn call(&self, _: &str, _: serde_json::Value) -> crate::Outcome {
        crate::Outcome::Skipped("tool calls are unavailable in this lifecycle callback".into())
    }
}

#[cfg(test)]
mod wire_tests {
    use super::*;
    use serde_json::json;

    struct SessionEntryHook(&'static str, std::sync::Arc<std::sync::atomic::AtomicUsize>);
    #[async_trait::async_trait]
    impl crate::Extension for SessionEntryHook {
        fn id(&self) -> &str {
            self.0
        }
        fn handles_lifecycle(&self) -> bool {
            true
        }
        fn handles_session_lifecycle(&self) -> bool {
            true
        }
        async fn lifecycle_with_host(
            &self,
            _: &LifecycleEvent,
            _: Option<&crate::ExtensionState>,
            host: std::sync::Arc<dyn crate::ToolHost>,
        ) -> crate::ExtensionResult<()> {
            self.1.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let previous = host.custom_entries(Some("lifecycle")).await?;
            host.append_entry(crate::CustomEntry {
                custom_type: "lifecycle".into(),
                data: Some(json!({"prior":previous.len()})),
            })
            .await?;
            Ok(())
        }
    }

    #[tokio::test]
    async fn session_lifecycle_reads_and_writes_entries_in_extension_order() {
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };
        let seen = Arc::new(AtomicUsize::new(0));
        let mut registry = Registry::default();
        registry
            .register(Arc::new(SessionEntryHook("first", seen.clone())))
            .unwrap();
        registry
            .register(Arc::new(SessionEntryHook("second", seen.clone())))
            .unwrap();
        let mut session = Session::with_store(crate::MemoryStore::default()).unwrap();
        notify_idle_with_host(
            &registry,
            &mut session,
            LifecycleEvent::SessionStart {
                reason: SessionStartReason::Startup,
                previous_session_file: None,
            },
            Duration::from_secs(2),
            &mut |_| {},
        )
        .await
        .unwrap();
        let entries = crate::extension::branch_custom_entries(&mut session, Some("lifecycle"))
            .await
            .unwrap();
        assert_eq!(seen.load(Ordering::SeqCst), 2);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].entry.data, Some(json!({"prior":0})));
        assert_eq!(entries[1].entry.data, Some(json!({"prior":1})));
    }

    #[tokio::test]
    async fn session_lifecycle_continues_after_entry_store_failure() {
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };
        struct FailingStore(crate::MemoryStore);
        impl crate::SessionStore for FailingStore {
            fn load(&mut self) -> crate::Result<Vec<crate::Record>> {
                self.0.load()
            }
            fn append(&mut self, record: &crate::Record) -> crate::Result<()> {
                if matches!(record.event, crate::Event::CustomEntry { .. }) {
                    return Err(crate::Error::Session("fixture write failed".into()));
                }
                self.0.append(record)
            }
        }
        let seen = Arc::new(AtomicUsize::new(0));
        let mut registry = Registry::default();
        registry
            .register(Arc::new(SessionEntryHook("first", seen.clone())))
            .unwrap();
        registry
            .register(Arc::new(SessionEntryHook("second", seen.clone())))
            .unwrap();
        let mut session = Session::with_store(FailingStore(crate::MemoryStore::default())).unwrap();
        let result = notify_idle_with_host(
            &registry,
            &mut session,
            LifecycleEvent::SessionShutdown {
                reason: SessionShutdownReason::Quit,
                target_session_file: None,
            },
            Duration::from_secs(2),
            &mut |_| {},
        )
        .await;
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("fixture write failed")
        );
        assert_eq!(seen.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn session_shutdown_runs_after_startup_entry_failure() {
        use std::sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        };
        struct FailingStore(crate::MemoryStore);
        impl crate::SessionStore for FailingStore {
            fn load(&mut self) -> crate::Result<Vec<crate::Record>> {
                self.0.load()
            }
            fn append(&mut self, record: &crate::Record) -> crate::Result<()> {
                if matches!(record.event, crate::Event::CustomEntry { .. }) {
                    return Err(crate::Error::Session("startup write failed".into()));
                }
                self.0.append(record)
            }
        }
        struct Cleanup(Arc<AtomicBool>, Arc<AtomicBool>);
        #[async_trait::async_trait]
        impl crate::Extension for Cleanup {
            fn id(&self) -> &str {
                "cleanup"
            }
            fn handles_lifecycle(&self) -> bool {
                true
            }
            fn handles_session_lifecycle(&self) -> bool {
                true
            }
            async fn lifecycle(
                &self,
                event: &LifecycleEvent,
                _: Option<&crate::ExtensionState>,
            ) -> crate::ExtensionResult<()> {
                if matches!(event, LifecycleEvent::SessionShutdown { .. }) {
                    self.0.store(true, Ordering::SeqCst);
                }
                Ok(())
            }
            async fn shutdown(&self) -> crate::ExtensionResult<()> {
                self.1.store(true, Ordering::SeqCst);
                Ok(())
            }
        }
        let shutdown = Arc::new(AtomicBool::new(false));
        let closed = Arc::new(AtomicBool::new(false));
        let mut registry = Registry::default();
        registry
            .register(Arc::new(SessionEntryHook(
                "writer",
                Arc::new(AtomicUsize::new(0)),
            )))
            .unwrap();
        registry
            .register(Arc::new(Cleanup(shutdown.clone(), closed.clone())))
            .unwrap();
        let mut session = Session::with_store(FailingStore(crate::MemoryStore::default())).unwrap();
        let start = notify_idle_with_host(
            &registry,
            &mut session,
            LifecycleEvent::SessionStart {
                reason: SessionStartReason::Startup,
                previous_session_file: None,
            },
            Duration::from_secs(2),
            &mut |_| {},
        )
        .await;
        assert!(
            start
                .unwrap_err()
                .to_string()
                .contains("startup write failed")
        );
        let _ = notify_idle_with_host(
            &registry,
            &mut session,
            LifecycleEvent::SessionShutdown {
                reason: SessionShutdownReason::Quit,
                target_session_file: None,
            },
            Duration::from_secs(2),
            &mut |_| {},
        )
        .await;
        registry.shutdown().await.unwrap();
        assert!(shutdown.load(Ordering::SeqCst));
        assert!(closed.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn detailed_compaction_callback_receives_trigger_and_committed_metadata() {
        use std::sync::{Arc, Mutex};
        struct Hook(Arc<Mutex<Option<serde_json::Value>>>);
        #[async_trait::async_trait]
        impl crate::Extension for Hook {
            fn id(&self) -> &str {
                "detailed-compaction"
            }
            fn handles_lifecycle(&self) -> bool {
                true
            }
            fn handles_session_lifecycle(&self) -> bool {
                true
            }
            async fn compaction_lifecycle_with_host(
                &self,
                _: &LifecycleEvent,
                trigger: crate::CompactionTrigger,
                metadata: Option<&serde_json::Value>,
                _: Option<&crate::ExtensionState>,
                _: Arc<dyn crate::ToolHost>,
            ) -> crate::ExtensionResult<()> {
                *self.0.lock().unwrap() = Some(json!({"trigger":trigger,"metadata":metadata}));
                Ok(())
            }
        }
        let seen = Arc::new(Mutex::new(None));
        let mut registry = Registry::default();
        registry.register(Arc::new(Hook(seen.clone()))).unwrap();
        let mut session = Session::with_store(crate::MemoryStore::default()).unwrap();
        let metadata = json!({"details":{"source":"hook"}});
        let record = session
            .append_with_record_metadata(
                crate::Event::Compacted {
                    summary: "summary".into(),
                    system_prompt: None,
                    retain_from: None,
                    tokens_before: None,
                    states: Default::default(),
                    usage: None,
                    usage_metadata: None,
                },
                metadata.clone(),
            )
            .unwrap()
            .clone();
        notify_session_compaction_with_host(
            &registry,
            &mut session,
            LifecycleEvent::SessionCompact {
                compaction_entry: Box::new(record),
                from_extension: true,
                reason: CompactionReason::Threshold,
                will_retry: true,
            },
            Duration::from_secs(1),
            &mut |_| {},
            true,
            Some((crate::CompactionTrigger::Overflow, Some(metadata.clone()))),
        )
        .await
        .unwrap();
        assert_eq!(
            *seen.lock().unwrap(),
            Some(json!({"trigger":"overflow","metadata":metadata}))
        );
    }

    #[test]
    fn transition_and_lifecycle_fields_use_pi_names() {
        let fork = BeforeSessionTransition::Fork {
            entry_id: 42,
            position: ForkPosition::Before,
        };
        assert_eq!(
            serde_json::to_value(&fork).unwrap(),
            json!({"type":"session_before_fork","entryId":42,"position":"before"})
        );
        assert!(
            serde_json::from_value::<BeforeSessionTransition>(
                json!({"type":"session_before_fork","entry_id":42,"position":"before"})
            )
            .is_ok()
        );
        let start = LifecycleEvent::SessionStart {
            reason: SessionStartReason::Resume,
            previous_session_file: Some("previous.jsonl".into()),
        };
        let value = serde_json::to_value(&start).unwrap();
        assert_eq!(value["previousSessionFile"], "previous.jsonl");
        assert!(value.get("previous_session_file").is_none());
        let tree = LifecycleEvent::SessionTree {
            new_leaf_id: 4,
            old_leaf_id: 2,
            summary_entry: None,
            from_extension: Some(true),
        };
        let value = serde_json::to_value(&tree).unwrap();
        assert_eq!(value["newLeafId"], 4);
        assert_eq!(value["oldLeafId"], 2);
        assert_eq!(value["fromExtension"], true);
        assert!(value.get("summary_entry").is_none());
    }

    #[test]
    fn tree_decisions_accept_pi_and_previous_native_field_names() {
        for value in [
            json!({"customInstructions":"custom","replaceInstructions":true}),
            json!({"custom_instructions":"custom","replace_instructions":true}),
        ] {
            let decision: TreeDecision = serde_json::from_value(value).unwrap();
            assert_eq!(decision.custom_instructions.as_deref(), Some("custom"));
            assert_eq!(decision.replace_instructions, Some(true));
            let encoded = serde_json::to_value(decision).unwrap();
            assert_eq!(encoded["customInstructions"], "custom");
            assert_eq!(encoded["replaceInstructions"], true);
        }
        let preparation = TreePreparation {
            target_id: 3,
            old_leaf_id: 5,
            common_ancestor_id: Some(2),
            entries_to_summarize: Vec::new(),
            user_wants_summary: true,
            custom_instructions: Some("custom".into()),
            replace_instructions: false,
            label: None,
        };
        let encoded = serde_json::to_value(preparation).unwrap();
        assert_eq!(encoded["targetId"], 3);
        assert_eq!(encoded["oldLeafId"], 5);
        assert_eq!(encoded["commonAncestorId"], 2);
        assert_eq!(encoded["userWantsSummary"], true);
        assert_eq!(encoded["customInstructions"], "custom");
        assert!(encoded.get("target_id").is_none());
    }
}
