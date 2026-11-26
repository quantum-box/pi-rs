//! Preserve the original version and commit sequence of expired inline partials.
use super::{PAGE_ROWS, generation::terminal_for_publication};
use crate::{
    observer::{PROGRESS_HISTORY_PREFIX as PREFIX, TOOL_PROGRESS_EVENTS_PREFIX},
    progress_retention::{TerminalObservation, observation_key},
    runtime::{
        Error, MAX_PROGRESS_HISTORY_BYTES, ProgressHistoryChange, ProgressHistoryRecord,
        ToolAttempt,
    },
    task::Task,
    task_store, tool_progress,
};
use durable_store::{Entry, Store};
use uuid::Uuid;

#[derive(Clone, Copy)]
pub(crate) enum Scope {
    Generation,
    Tools,
}

fn invalid() -> Error {
    Error::Invalid("invalid inline progress record".into())
}

pub(crate) fn sweep(
    store: &Store,
    after: &mut Vec<u8>,
    scope: Scope,
    now_ms: u64,
    retain_ms: u64,
) -> Result<(), Error> {
    sweep_namespace(store, PREFIX, after, |key, frame| {
        expire_one(store, key, frame, scope, now_ms, retain_ms)
    })
}

pub(crate) fn sweep_tool_attempts(
    store: &Store,
    after: &mut Vec<u8>,
    now_ms: u64,
    retain_ms: u64,
) -> Result<(), Error> {
    sweep_namespace(store, TOOL_PROGRESS_EVENTS_PREFIX, after, |key, frame| {
        expire_tool_attempt(store, key, frame, now_ms, retain_ms)
    })
}

fn sweep_namespace(
    store: &Store,
    prefix: &[u8],
    after: &mut Vec<u8>,
    mut expire: impl FnMut(&[u8], &Entry) -> Result<(), Error>,
) -> Result<(), Error> {
    let page = store.prefix_page_bounded(
        prefix,
        after,
        PAGE_ROWS,
        prefix.len() + 26,
        MAX_PROGRESS_HISTORY_BYTES,
    )?;
    let next = page
        .last()
        .filter(|_| page.len() == PAGE_ROWS)
        .map(|(key, _)| key.clone());
    for (key, frame) in page {
        if let Err(error) = expire(&key, &frame)
            && !matches!(error, Error::Storage(durable_store::Error::Conflict { .. }))
        {
            return Err(error);
        }
    }
    *after = next.unwrap_or_default();
    Ok(())
}

fn expire_one(
    store: &Store,
    key: &[u8],
    frame: &Entry,
    scope: Scope,
    now_ms: u64,
    retain_ms: u64,
) -> Result<(), Error> {
    let Some(conversation) = record_stream(key, PREFIX)? else {
        return Ok(());
    };
    let record: ProgressHistoryRecord = serde_json::from_slice(&frame.value)?;
    let Some(change) = record.inline_progress_change(conversation) else {
        return Ok(());
    };
    let Some(commit) = store.commit_for_version(key, frame.version)? else {
        return Ok(());
    };
    let terminal = match (scope, change) {
        (Scope::Generation, ProgressHistoryChange::Generation { request_id, state })
            if !request_id.is_nil()
                && matches!(state.as_ref(), crate::progress::State::Running(_)) =>
        {
            terminal_for_publication(store, *request_id, commit)?.map(|(_, entry)| entry)
        }
        (
            Scope::Tools,
            ProgressHistoryChange::Tool {
                attempt,
                state: tool_progress::State::Running(_),
                ..
            },
        ) if !attempt.attempt_id.is_nil() && !attempt.operation_id.is_nil() => {
            terminal_tool_attempt(store, attempt.attempt_id, commit)?
        }
        (Scope::Tools, ProgressHistoryChange::CheckpointableTool { task, attempt, .. })
            if !task.is_nil() && !attempt.is_nil() =>
        {
            let key = task_store::key(*task);
            let Some(entry) = store.get(&key)? else {
                return Ok(());
            };
            let settled: Task = serde_json::from_slice(&entry.value)?;
            if settled.id() != *task {
                return Err(invalid());
            }
            terminal_after(
                store,
                &key,
                entry,
                commit,
                settled.is_task_tool() && settled.is_terminal(),
            )?
        }
        _ => None,
    };
    let Some(terminal) = terminal else {
        return Ok(());
    };
    retire_observed(store, key, frame, &terminal, scope, now_ms, retain_ms)
}

fn record_stream(key: &[u8], prefix: &[u8]) -> Result<Option<Uuid>, Error> {
    let suffix = key.strip_prefix(prefix).ok_or_else(invalid)?;
    if suffix.len() == 18 && suffix[16..] == *b"/h" {
        return Ok(None);
    }
    if suffix.len() != 26 || suffix[16..18] != *b"/r" || suffix[18..].iter().all(|byte| *byte == 0)
    {
        return Err(invalid());
    }
    let stream = Uuid::from_slice(&suffix[..16]).map_err(|_| invalid())?;
    if stream.is_nil() {
        return Err(invalid());
    }
    Ok(Some(stream))
}

fn expire_tool_attempt(
    store: &Store,
    key: &[u8],
    frame: &Entry,
    now_ms: u64,
    retain_ms: u64,
) -> Result<(), Error> {
    let Some(stream) = record_stream(key, TOOL_PROGRESS_EVENTS_PREFIX)? else {
        return Ok(());
    };
    let event: serde_json::Value = serde_json::from_slice(&frame.value)?;
    if event["expired"] == true || event["state"]["state"] != "running" {
        return Ok(());
    }
    let attempt: ToolAttempt = serde_json::from_value(event["attempt"].clone())?;
    if attempt.attempt_id != stream || attempt.operation_id.is_nil() {
        return Err(invalid());
    }
    let Some(commit) = store.commit_for_version(key, frame.version)? else {
        return Ok(());
    };
    let Some(terminal) = terminal_tool_attempt(store, stream, commit)? else {
        return Ok(());
    };
    retire_observed(
        store,
        key,
        frame,
        &terminal,
        Scope::Tools,
        now_ms,
        retain_ms,
    )
}

fn terminal_tool_attempt(
    store: &Store,
    attempt_id: Uuid,
    commit: u64,
) -> Result<Option<Entry>, Error> {
    let key = tool_progress::key(attempt_id);
    let Some(entry) = store.get_bounded(&key, tool_progress::MAX_COMPLETED_BYTES)? else {
        return Ok(None);
    };
    let state: tool_progress::State = serde_json::from_slice(&entry.value)?;
    terminal_after(
        store,
        &key,
        entry,
        commit,
        !matches!(state, tool_progress::State::Running(_)),
    )
}

fn retire_observed(
    store: &Store,
    key: &[u8],
    frame: &Entry,
    terminal: &Entry,
    scope: Scope,
    now_ms: u64,
    retain_ms: u64,
) -> Result<(), Error> {
    let marker = observation_key(key);
    let prior = store.get_bounded(&marker, 256)?;
    if let Some(prior) = &prior {
        let observation: TerminalObservation = serde_json::from_slice(&prior.value)?;
        if observation.task_version == terminal.version
            && observation.progress_version == frame.version
        {
            if now_ms.saturating_sub(observation.observed_ms) >= retain_ms {
                match scope {
                    Scope::Generation => store.expire_inline_generation_progress(
                        key,
                        frame,
                        terminal,
                        prior.version,
                    )?,
                    Scope::Tools => {
                        store.expire_inline_tool_progress(key, frame, terminal, prior.version)?
                    }
                }
            }
            return Ok(());
        }
    }
    let prior_version = prior.map(|prior| prior.version);
    match scope {
        Scope::Generation => {
            store.observe_inline_generation_progress(key, frame, terminal, prior_version, now_ms)?
        }
        Scope::Tools => {
            store.observe_inline_tool_progress(key, frame, terminal, prior_version, now_ms)?
        }
    }
    Ok(())
}

fn terminal_after(
    store: &Store,
    key: &[u8],
    entry: Entry,
    commit: u64,
    terminal: bool,
) -> Result<Option<Entry>, Error> {
    if !terminal
        || store
            .commit_for_version(key, entry.version)?
            .is_none_or(|version| version <= commit)
    {
        return Ok(None);
    }
    Ok(Some(entry))
}
