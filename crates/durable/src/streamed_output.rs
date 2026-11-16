//! Committed text from an opt-in streamed tool attempt. This record is not a result.
use durable_store::Store;
use uuid::Uuid;

pub(crate) fn key(attempt: Uuid) -> Vec<u8> {
    format!("durable/streamed-tool-output/{attempt}").into_bytes()
}

/// Read the last committed text for an attempt, including after reopen.
/// A missing record means no append was committed; it does not imply completion.
pub fn load(
    store: &Store,
    attempt: Uuid,
) -> Result<Option<String>, Box<dyn std::error::Error + Send + Sync>> {
    let Some(entry) = store.get(&key(attempt))? else {
        return Ok(None);
    };
    decode(entry).map(Some)
}

pub(crate) fn decode(
    entry: durable_store::Entry,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    if entry.value.len() > crate::keyed_tool::MAX_STREAMED_OUTPUT_BYTES {
        return Err("streamed output exceeds byte limit".into());
    }
    Ok(String::from_utf8(entry.value)?)
}

/// Sidecar key for bounded stream metadata. The original text record remains
/// unchanged for existing readers and older attempts.
pub(crate) fn checkpoint_key(attempt: Uuid) -> Vec<u8> {
    format!("durable/bounded-stream-checkpoint/{attempt}").into_bytes()
}

/// Read a bounded attempt's latest checkpoint; legacy streams return `None`.
pub fn load_checkpoint(
    store: &Store,
    attempt: Uuid,
) -> Result<Option<crate::output::OutputCheckpoint>, Box<dyn std::error::Error + Send + Sync>> {
    store
        .get(&checkpoint_key(attempt))?
        .map(decode_checkpoint)
        .transpose()
}

pub(crate) fn decode_checkpoint(
    entry: durable_store::Entry,
) -> Result<crate::output::OutputCheckpoint, Box<dyn std::error::Error + Send + Sync>> {
    if entry.value.len() > 12 * crate::keyed_tool::MAX_STREAMED_OUTPUT_BYTES + 4096 {
        return Err("bounded stream checkpoint exceeds size limit".into());
    }
    let checkpoint: crate::output::OutputCheckpoint = serde_json::from_slice(&entry.value)?;
    if checkpoint.version != 1
        || checkpoint.current.text.len() > crate::keyed_tool::MAX_STREAMED_OUTPUT_BYTES
        || checkpoint.finished.text.len() > crate::keyed_tool::MAX_STREAMED_OUTPUT_BYTES
        || checkpoint.pending_utf8.len() > 3
        || (!checkpoint.pending_utf8.is_empty()
            && !matches!(std::str::from_utf8(&checkpoint.pending_utf8), Err(e) if e.valid_up_to() == 0 && e.error_len().is_none()))
        || (checkpoint.pending_utf8.is_empty() && checkpoint.current != checkpoint.finished)
    {
        return Err("invalid bounded stream checkpoint".into());
    }
    Ok(checkpoint)
}
