//! Append-only application records written by directly conversation-owned tasks.
//! This independent log is not the conversation runtime's transcript.
use durable_store::Store;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ApplicationEntry {
    pub id: Uuid,
    pub conversation_id: Uuid,
    pub author_task_id: Uuid,
    pub kind: String,
    pub data: Value,
}

pub(crate) fn key(conversation: Uuid) -> Vec<u8> {
    format!("durable/application-entries/{conversation}").into_bytes()
}

// This runtime-owned record is written in the same commit as fork creation.
#[derive(Serialize, Deserialize)]
pub(crate) struct Lineage {
    pub parent: Uuid,
    pub parent_revision: u64,
}

pub(crate) fn lineage_key(conversation: Uuid) -> Vec<u8> {
    format!("durable/application-lineage/{conversation}").into_bytes()
}

/// Read the committed records in append order. The result is a snapshot.
pub fn list(
    store: &Store,
    conversation: Uuid,
) -> Result<Vec<ApplicationEntry>, crate::document::Error> {
    let Some(record) = store.get(&key(conversation))? else {
        return Ok(Vec::new());
    };
    Ok(serde_json::from_slice(&record.value)?)
}
