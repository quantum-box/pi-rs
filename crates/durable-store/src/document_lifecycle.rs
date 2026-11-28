use crate::{Error, Mutation};
use rusqlite::{OptionalExtension, Transaction};
use serde_json::Value;
use std::collections::HashMap;

const DOCUMENT: &[u8] = b"durable/document/";
const POLICY: &[u8] = b"durable/document-policy/";

#[derive(Default)]
struct Writes<'a> {
    document: Option<&'a Mutation>,
    policy: Option<&'a Mutation>,
}

fn definition(bytes: &[u8]) -> Option<Value> {
    let mut policy: Value = serde_json::from_slice(bytes).ok()?;
    policy
        .as_object_mut()?
        .remove("definition")
        .filter(|value| !value.is_null())
}

/// Prepare the retirement of the definition selected by the settling commit.
/// Older untyped policies require no metadata update.
pub(crate) fn retirement_policy(
    tx: &Transaction<'_>,
    policy_key: Vec<u8>,
    staged: Option<&Mutation>,
) -> Result<Option<Mutation>, Error> {
    let stored;
    let value = match staged {
        Some(Mutation::Put { value, .. }) => Some(value.as_slice()),
        Some(Mutation::Delete { .. }) => None,
        None => {
            stored = tx
                .query_row(
                    "SELECT value FROM durable_store_entries WHERE key=?1",
                    [&policy_key],
                    |row| row.get::<_, Vec<u8>>(0),
                )
                .optional()?;
            stored.as_deref()
        }
    };
    let Some(mut policy) = value.and_then(|value| serde_json::from_slice::<Value>(value).ok())
    else {
        return Ok(None);
    };
    let Some(incarnation) = policy
        .pointer_mut("/definition/incarnation")
        .filter(|incarnation| !incarnation.is_null())
    else {
        return Ok(None);
    };
    *incarnation = Value::Null;
    let value = serde_json::to_vec(&policy).map_err(|_| Error::InvalidDocumentLifecycle)?;
    Ok(Some(Mutation::Put {
        key: policy_key,
        value,
    }))
}

/// Validate the final writes while the SQLite writer lock is held.
pub(crate) fn validate(tx: &Transaction<'_>, mutations: &[Mutation]) -> Result<(), Error> {
    let mut touched = HashMap::<&[u8], Writes<'_>>::new();
    for mutation in mutations {
        let key = match mutation {
            Mutation::Put { key, .. } | Mutation::Delete { key } => key,
        };
        if let Some(suffix) = key.strip_prefix(DOCUMENT) {
            touched.entry(suffix).or_default().document = Some(mutation);
        } else if let Some(suffix) = key.strip_prefix(POLICY) {
            touched.entry(suffix).or_default().policy = Some(mutation);
        }
    }
    if touched.is_empty() {
        return Ok(());
    }
    let mut read = tx.prepare_cached("SELECT value FROM durable_store_entries WHERE key=?1")?;
    for (suffix, writes) in touched {
        let policy_key = [POLICY, suffix].concat();
        let previous = read
            .query_row([&policy_key], |row| row.get::<_, Vec<u8>>(0))
            .optional()?;
        let previous = previous.as_deref().and_then(definition);
        let policy = match writes.policy {
            Some(Mutation::Put { value, .. }) => definition(value),
            Some(Mutation::Delete { .. }) => None,
            None => previous.clone(),
        };
        let Some(policy) = policy else {
            if previous.is_some() {
                return Err(Error::InvalidDocumentLifecycle);
            }
            continue;
        };
        let incarnation: Option<uuid::Uuid> = policy
            .get("incarnation")
            .cloned()
            .ok_or(Error::InvalidDocumentLifecycle)
            .and_then(|value| {
                serde_json::from_value(value).map_err(|_| Error::InvalidDocumentLifecycle)
            })?;
        let present = match writes.document {
            Some(mutation) => matches!(mutation, Mutation::Put { .. }),
            None => read.exists([[DOCUMENT, suffix].concat()])?,
        };
        if present != incarnation.is_some() || incarnation.is_some_and(|id| id.is_nil()) {
            return Err(Error::InvalidDocumentLifecycle);
        }
    }
    Ok(())
}
