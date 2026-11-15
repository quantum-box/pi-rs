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
