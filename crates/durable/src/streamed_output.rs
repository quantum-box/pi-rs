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
    if entry.value.len() > crate::keyed_tool::MAX_STREAMED_OUTPUT_BYTES {
        return Err("streamed output exceeds byte limit".into());
    }
    Ok(Some(String::from_utf8(entry.value)?))
}
