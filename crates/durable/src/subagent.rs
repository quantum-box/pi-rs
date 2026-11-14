//! Durable parent/key to child-conversation identity for a local runtime owner.
//!
//! A reservation commits before the child is created. Retrying after interruption
//! completes creation with the same ID. The runtime must be reopened after an
//! uncertain storage error. This is a linkage, not an ownership or abort tree.
//! No reporting task is installed and no external effects are made exactly once.

use crate::runtime::{Config, Error, Harness, Scheduler};
use crate::state::InputMode;
use durable_store::{Check, Expected, Mutation};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Link {
    pub parent: Uuid,
    pub key: String,
    pub child: Uuid,
    pub config: Config,
}

fn key(parent: Uuid, name: &str) -> Vec<u8> {
    let mut key = format!("runtime/subagent/{parent}/").into_bytes();
    key.extend_from_slice(name.as_bytes());
    key
}

impl Harness {
    /// Read a reserved identity. A crash may have occurred before creation;
    /// call `ensure_subagent` to finish it before using the child.
    pub fn subagent(&self, parent: Uuid, name: &str) -> Result<Option<Link>, Error> {
        self.conversation(parent)?;
        let entry = self.subagent_store()?.get(&key(parent, name))?;
        entry
            .map(|entry| {
                let link: Link = serde_json::from_slice(&entry.value)?;
                if link.parent != parent || link.key != name || link.child == parent {
                    return Err(Error::Invalid("invalid subagent link".into()));
                }
                Ok(link)
            })
            .transpose()
    }

    /// Create or restore a named child. Reuse with a different configuration is
    /// rejected. Child creation starts empty; it does not fork parent history.
    pub fn ensure_subagent(
        &mut self,
        parent: Uuid,
        name: &str,
        config: Config,
    ) -> Result<Link, Error> {
        if name.is_empty() || config.model.is_empty() || config.max_output_tokens == 0 {
            return Err(Error::Invalid(
                "subagent key, model and output limit required".into(),
            ));
        }
        self.conversation(parent)?;
        let k = key(parent, name);
        let link = loop {
            if let Some(link) = self.subagent(parent, name)? {
                if link.config != config {
                    return Err(Error::Invalid("subagent configuration changed".into()));
                }
                break link;
            }
            let link = Link {
                parent,
                key: name.into(),
                child: Uuid::new_v4(),
                config: config.clone(),
            };
            let result = self.subagent_store()?.commit(
                &[Check {
                    key: k.clone(),
                    expected: Expected::Missing,
                }],
                &[Mutation::Put {
                    key: k.clone(),
                    value: serde_json::to_vec(&link)?,
                }],
            );
            match result {
                Ok(()) => break link,
                Err(durable_store::Error::Conflict { .. }) => continue,
                Err(e) => {
                    self.subagent_storage_failed();
                    return Err(e.into());
                }
            }
        };
        match self.conversation(link.child) {
            Ok(child) if child.id == link.child && child.config == link.config => {}
            Ok(_) => {
                return Err(Error::Invalid(
                    "subagent child identity or configuration mismatch".into(),
                ));
            }
            Err(Error::NotFound) => {
                self.create_conversation_with_id(link.child, link.config.clone())?;
            }
            Err(e) => return Err(e),
        }
        Ok(link)
    }

    /// Idempotent admission is scoped to the child conversation, matching the
    /// runtime's first-request-wins semantics even if content changes on retry.
    pub fn submit_subagent(
        &mut self,
        parent: Uuid,
        name: &str,
        config: Config,
        request_id: &str,
        content: String,
        mode: InputMode,
    ) -> Result<(Link, Uuid), Error> {
        if request_id.is_empty() {
            return Err(Error::Invalid("subagent request ID required".into()));
        }
        let link = self.ensure_subagent(parent, name, config)?;
        let submission =
            self.submit_with_mode(link.child, Some(request_id.into()), content, mode)?;
        Ok((link, submission))
    }
}

impl Scheduler {
    pub fn subagent(&mut self, parent: Uuid, name: &str) -> Result<Option<Link>, Error> {
        self.subagent_harness().subagent(parent, name)
    }
    pub fn ensure_subagent(
        &mut self,
        parent: Uuid,
        name: &str,
        config: Config,
    ) -> Result<Link, Error> {
        self.subagent_harness()
            .ensure_subagent(parent, name, config)
    }
    /// Schedule after durable admission; reopening also schedules all children.
    pub fn submit_subagent(
        &mut self,
        parent: Uuid,
        name: &str,
        config: Config,
        request_id: &str,
        content: String,
        mode: InputMode,
    ) -> Result<(Link, Uuid), Error> {
        let result = self
            .subagent_harness()
            .submit_subagent(parent, name, config, request_id, content, mode)?;
        self.schedule(result.0.child);
        Ok(result)
    }
}
