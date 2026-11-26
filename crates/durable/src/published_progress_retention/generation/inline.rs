//! Preserve the original version and commit sequence of expired inline partials.
use super::{PAGE_ROWS, invalid, terminal_for_publication};
use crate::{
    observer::{Cursor, Observer, PROGRESS_HISTORY_PREFIX as PREFIX},
    progress_retention::{TerminalObservation, observation_key},
    runtime::{Error, MAX_PROGRESS_HISTORY_BYTES, ProgressHistoryRecord},
};
use durable_store::{Entry, Store};
use uuid::Uuid;

pub(crate) fn sweep(
    store: &Store,
    after: &mut Vec<u8>,
    now_ms: u64,
    retain_ms: u64,
) -> Result<(), Error> {
    let page = store.prefix_page_bounded(
        PREFIX,
        after,
        PAGE_ROWS,
        PREFIX.len() + 26,
        MAX_PROGRESS_HISTORY_BYTES,
    )?;
    let next = page
        .last()
        .filter(|_| page.len() == PAGE_ROWS)
        .map(|(key, _)| key.clone());
    for (key, frame) in page {
        if let Err(error) = expire_one(store, &key, &frame, now_ms, retain_ms)
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
    now_ms: u64,
    retain_ms: u64,
) -> Result<(), Error> {
    let suffix = key.strip_prefix(PREFIX).ok_or_else(invalid)?;
    if suffix.len() == 18 && suffix[16..] == *b"/h" {
        return Ok(());
    }
    if suffix.len() != 26 {
        return Err(invalid());
    }
    let conversation = Uuid::from_slice(&suffix[..16]).map_err(|_| invalid())?;
    let position = u64::from_be_bytes(suffix[18..].try_into().map_err(|_| invalid())?);
    if conversation.is_nil()
        || position == 0
        || Observer::progress_history(store, conversation).record_key(Cursor(position)) != key
    {
        return Err(invalid());
    }
    let record: ProgressHistoryRecord = serde_json::from_slice(&frame.value)?;
    let Some(request_id) = record.inline_running_request(conversation) else {
        return Ok(());
    };
    let Some(commit) = store.commit_for_version(key, frame.version)? else {
        return Ok(());
    };
    let Some((_, terminal)) = terminal_for_publication(store, request_id, commit)? else {
        return Ok(());
    };
    let marker = observation_key(key);
    let prior = store.get_bounded(&marker, 256)?;
    if let Some(prior) = &prior {
        let observation: TerminalObservation = serde_json::from_slice(&prior.value)?;
        if observation.task_version == terminal.version
            && observation.progress_version == frame.version
        {
            if now_ms.saturating_sub(observation.observed_ms) >= retain_ms {
                store.expire_inline_generation_progress(key, frame, &terminal, prior.version)?;
            }
            return Ok(());
        }
    }
    store.observe_inline_generation_progress(
        key,
        frame,
        &terminal,
        prior.map(|prior| prior.version),
        now_ms,
    )?;
    Ok(())
}
