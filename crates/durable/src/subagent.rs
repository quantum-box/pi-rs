//! Durable named child conversations and background answer reports.
//!
//! Reservations and report intents precede execution. The scheduler retries
//! intents after restart; stable request IDs deduplicate both admissions. The
//! link is not an ownership or cancellation tree.

use crate::runtime::{Config, Conversation, Error, Harness, Scheduler, Status};
use crate::state::InputMode;
use durable_store::{Check, Expected, Mutation};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub(crate) const REPORT_REQUEST_PREFIX: &str = "__pi_rs_subagent_report/";
const REPORTS: &[u8] = b"runtime/subagent-report/";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Link {
    pub parent: Uuid,
    pub key: String,
    pub child: Uuid,
    pub config: Config,
}
/// A named reservation and its child's committed state. A reservation can
/// survive a crash before the child conversation is created.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChildStatus {
    pub link: Link,
    pub state: ChildState,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChildState {
    Reserved,
    Working,
    Idle,
}

/// Format status for a user, following the upstream example for created children.
pub fn format_status(children: &[ChildStatus]) -> String {
    if children.is_empty() {
        return "No subagents.".into();
    }
    children
        .iter()
        .map(|child| {
            format!(
                "{}: {}",
                child.link.key,
                match child.state {
                    ChildState::Reserved => "reserved",
                    ChildState::Working => "working",
                    ChildState::Idle => "idle",
                }
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}
#[derive(Serialize, Deserialize)]
struct Reservation {
    #[serde(flatten)]
    link: Link,
    #[serde(default)]
    history: bool,
    #[serde(default)]
    allowed_tools: Option<Vec<String>>,
    #[serde(default)]
    seed: Option<Conversation>,
}
/// The durable input and creation mode for a background report.
pub struct ReportedInput {
    pub history: bool,
    pub request_id: String,
    pub content: String,
    pub mode: InputMode,
}
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Reporter {
    link: Link,
    request_id: String,
    content: String,
    mode: InputMode,
    delivered: bool,
    #[serde(default)]
    generation: u64,
}
pub(crate) fn generation_key(parent: Uuid) -> Vec<u8> {
    format!("runtime/subagent-generation/{parent}").into_bytes()
}
pub(crate) fn generation_check(
    store: &durable_store::Store,
    parent: Uuid,
) -> Result<(u64, Check), Error> {
    let key = generation_key(parent);
    let entry = store.get(&key)?;
    let generation = entry
        .as_ref()
        .map(|e| serde_json::from_slice(&e.value))
        .transpose()?
        .unwrap_or(0);
    let expected = entry.map_or(Expected::Missing, |e| Expected::Version(e.version));
    Ok((generation, Check { key, expected }))
}
fn key(parent: Uuid, name: &str) -> Vec<u8> {
    let mut key = format!("runtime/subagent/{parent}/").into_bytes();
    key.extend_from_slice(name.as_bytes());
    key
}
fn reporter_key(parent: Uuid, request_id: &str) -> Vec<u8> {
    let mut key = format!("runtime/subagent-report/{parent}/").into_bytes();
    key.extend_from_slice(request_id.as_bytes());
    key
}
impl Harness {
    fn reservation(&self, parent: Uuid, name: &str) -> Result<Option<Reservation>, Error> {
        self.conversation(parent)?;
        let entry = self.subagent_store()?.get(&key(parent, name))?;
        entry
            .map(|entry| {
                let reservation: Reservation = serde_json::from_slice(&entry.value)?;
                let link = &reservation.link;
                if link.parent != parent
                    || link.key != name
                    || link.child == parent
                    || reservation.history != reservation.seed.is_some()
                {
                    return Err(Error::Invalid("invalid subagent link".into()));
                }
                Ok(reservation)
            })
            .transpose()
    }
    /// Returns a reservation, which may not yet have a created child.
    pub fn subagent(&self, parent: Uuid, name: &str) -> Result<Option<Link>, Error> {
        Ok(self.reservation(parent, name)?.map(|r| r.link))
    }
    /// Inspect one or all named children without creating or scheduling work.
    /// Queued inputs count as working, including before a scheduler picks them up.
    pub fn subagent_status(
        &self,
        parent: Uuid,
        name: Option<&str>,
    ) -> Result<Vec<ChildStatus>, Error> {
        self.conversation(parent)?;
        let names = if let Some(name) = name {
            vec![name.to_owned()]
        } else {
            let prefix = format!("runtime/subagent/{parent}/");
            self.subagent_store()?
                .list(prefix.as_bytes())?
                .into_iter()
                .map(|(key, _)| {
                    String::from_utf8(key[prefix.len()..].to_vec())
                        .map_err(|_| Error::Invalid("invalid subagent key".into()))
                })
                .collect::<Result<Vec<_>, _>>()?
        };
        let mut result = Vec::new();
        for name in names {
            let Some(link) = self.subagent(parent, &name)? else {
                continue;
            };
            let state = match self.conversation(link.child) {
                Ok(child) if child.id == link.child && child.config == link.config => {
                    if child.is_working() {
                        ChildState::Working
                    } else {
                        ChildState::Idle
                    }
                }
                Ok(_) => {
                    return Err(Error::Invalid(
                        "subagent child identity or configuration mismatch".into(),
                    ));
                }
                Err(Error::NotFound) => ChildState::Reserved,
                Err(error) => return Err(error),
            };
            result.push(ChildStatus { link, state });
        }
        result.sort_by(|a, b| a.link.key.cmp(&b.link.key));
        Ok(result)
    }
    pub fn ensure_subagent(
        &mut self,
        parent: Uuid,
        name: &str,
        config: Config,
    ) -> Result<Link, Error> {
        self.ensure_subagent_with_history(parent, name, config, false)
    }
    /// Optionally seed from a settled parent. The selected snapshot is persisted
    /// with the reservation, so retry after a crash uses precisely that snapshot.
    pub fn ensure_subagent_with_history(
        &mut self,
        parent: Uuid,
        name: &str,
        config: Config,
        history: bool,
    ) -> Result<Link, Error> {
        self.ensure_subagent_restricted(parent, name, config, history, None)
    }

    /// Reserve a child with its initial tool selection before any input can be admitted.
    pub fn ensure_subagent_restricted(
        &mut self,
        parent: Uuid,
        name: &str,
        config: Config,
        history: bool,
        allowed_tools: Option<Vec<String>>,
    ) -> Result<Link, Error> {
        crate::runtime::validate_allowed_tools(&allowed_tools)?;
        if name.is_empty() || config.model.is_empty() || config.max_output_tokens == 0 {
            return Err(Error::Invalid(
                "subagent key, model and output limit required".into(),
            ));
        }
        self.conversation(parent)?;
        let k = key(parent, name);
        let reservation = loop {
            if let Some(reservation) = self.reservation(parent, name)? {
                if reservation.link.config != config
                    || reservation.history != history
                    || reservation.allowed_tools != allowed_tools
                {
                    return Err(Error::Invalid(
                        "subagent configuration or history mode changed".into(),
                    ));
                }
                break reservation;
            }
            let link = Link {
                parent,
                key: name.into(),
                child: Uuid::new_v4(),
                config: config.clone(),
            };
            let seed = if history {
                let mut seed = self.subagent_seed(parent, link.child, config.clone())?;
                if allowed_tools.is_some() {
                    seed.allowed_tools = allowed_tools.clone();
                }
                Some(seed)
            } else {
                None
            };
            let reservation = Reservation {
                link,
                history,
                allowed_tools: allowed_tools.clone(),
                seed,
            };
            match self.subagent_store()?.commit(
                &[Check {
                    key: k.clone(),
                    expected: Expected::Missing,
                }],
                &[Mutation::Put {
                    key: k.clone(),
                    value: serde_json::to_vec(&reservation)?,
                }],
            ) {
                Ok(()) => break reservation,
                Err(durable_store::Error::Conflict { .. }) => continue,
                Err(e) => {
                    self.subagent_storage_failed();
                    return Err(e.into());
                }
            }
        };
        let link = reservation.link;
        match self.conversation(link.child) {
            Ok(child) if child.id == link.child && child.config == link.config => {}
            Ok(_) => {
                return Err(Error::Invalid(
                    "subagent child identity or configuration mismatch".into(),
                ));
            }
            Err(Error::NotFound) => {
                if let Some(seed) = reservation.seed {
                    if seed.id != link.child || seed.config != link.config {
                        return Err(Error::Invalid("invalid subagent seed".into()));
                    }
                    match self.create_subagent_from_seed(&seed) {
                        Err(Error::Storage(durable_store::Error::Conflict { .. })) => {
                            self.conversation(link.child)?;
                        }
                        other => other?,
                    };
                } else {
                    let mut child = Harness::empty_conversation(link.child, link.config.clone());
                    child.allowed_tools = reservation.allowed_tools;
                    match self.create_subagent_from_seed(&child) {
                        Err(Error::Storage(durable_store::Error::Conflict { .. })) => {
                            self.conversation(link.child)?;
                        }
                        other => other?,
                    };
                }
            }
            Err(e) => return Err(e),
        }
        Ok(link)
    }
    /// Create managed reservations, their child and background boundary in one commit.
    /// Existing explicit reservations are upgraded before any managed submission.
    pub(crate) fn ensure_managed_subagent(
        &mut self,
        parent: Uuid,
        name: &str,
        config: Config,
        allowed_tools: Option<Vec<String>>,
    ) -> Result<Link, Error> {
        use crate::ownership_edge::{self, Edge, Mode};
        use crate::task::{Checkpoint, Task, TaskOwner};
        use crate::task_store;
        crate::runtime::validate_allowed_tools(&allowed_tools)?;
        if name.is_empty() || config.model.is_empty() || config.max_output_tokens == 0 {
            return Err(Error::Invalid(
                "subagent key, model and output limit required".into(),
            ));
        }
        let reservation_key = key(parent, name);
        let anchor_key = format!("runtime/subagent-anchor/{parent}/{name}").into_bytes();
        loop {
            // An existing anchor is reusable, but it does not grant admission
            // while the parent has an outstanding abort request.
            if self.conversation(parent)?.abort_requested {
                return Err(Error::Invalid("parent conversation is aborting".into()));
            }
            if self.reservation(parent, name)?.is_some() {
                let link = self.ensure_subagent_restricted(
                    parent,
                    name,
                    config.clone(),
                    false,
                    allowed_tools.clone(),
                )?;
                self.anchor_subagent(parent, name)?;
                return Ok(link);
            }
            let store = self.subagent_store()?;
            let parent_key = format!("runtime/conversation/{parent}").into_bytes();
            let parent_entry = store.get(&parent_key)?.ok_or(Error::NotFound)?;
            let parent_state: Conversation = serde_json::from_slice(&parent_entry.value)?;
            if parent_state.id != parent || parent_state.abort_requested {
                return Err(Error::Invalid("parent conversation is aborting".into()));
            }
            let link = Link {
                parent,
                key: name.into(),
                child: Uuid::new_v4(),
                config: config.clone(),
            };
            let reservation = Reservation {
                link: link.clone(),
                history: false,
                allowed_tools: allowed_tools.clone(),
                seed: None,
            };
            let mut child = Harness::empty_conversation(link.child, config.clone());
            child.allowed_tools = allowed_tools.clone();
            let id = Uuid::new_v4();
            let owner = TaskOwner::Conversation(parent);
            let mut task = Task::new(
                id,
                owner.clone(),
                Checkpoint {
                    phase: "subagent-anchor".into(),
                    data: serde_json::Value::Null,
                },
                1,
            )
            .and_then(Task::background)
            .map_err(|e| Error::Invalid(format!("anchor task: {e:?}")))?;
            task.start()
                .and_then(|()| task.succeed(serde_json::Value::Null))
                .map_err(|e| Error::Invalid(format!("anchor completion: {e:?}")))?;
            let edge = Edge {
                task: id,
                task_owner: owner.clone(),
                child: link.child,
                mode: Mode::Foreground,
            };
            let edge_key = ownership_edge::edge_key(link.child);
            let task_key = task_store::key(id);
            let mut prepared = self.prepare_save(&child, Expected::Missing)?;
            prepared.checks.extend(
                ownership_edge::owner_chain_from(store, owner, link.child)
                    .map_err(|e| Error::Invalid(format!("anchor owner chain: {e}")))?,
            );
            prepared.checks.extend([
                Check {
                    key: parent_key.clone(),
                    expected: Expected::Version(parent_entry.version),
                },
                Check {
                    key: reservation_key.clone(),
                    expected: Expected::Missing,
                },
                Check {
                    key: anchor_key.clone(),
                    expected: Expected::Missing,
                },
                Check {
                    key: edge_key.clone(),
                    expected: Expected::Missing,
                },
                Check {
                    key: task_key.clone(),
                    expected: Expected::Missing,
                },
            ]);
            prepared.mutations.extend([
                Mutation::Put {
                    key: parent_key,
                    value: parent_entry.value,
                },
                Mutation::Put {
                    key: reservation_key.clone(),
                    value: serde_json::to_vec(&reservation)?,
                },
                Mutation::Put {
                    key: anchor_key.clone(),
                    value: serde_json::to_vec(&id)?,
                },
                Mutation::Put {
                    key: edge_key,
                    value: serde_json::to_vec(&edge)?,
                },
                Mutation::Put {
                    key: task_key,
                    value: serde_json::to_vec(&task)?,
                },
            ]);
            match store.commit(&prepared.checks, &prepared.mutations) {
                Ok(()) => return Ok(link),
                Err(durable_store::Error::Conflict { .. }) => continue,
                Err(e) => {
                    self.subagent_storage_failed();
                    return Err(e.into());
                }
            }
        }
    }
    /// Attach a persistent background ownership boundary to an existing named child.
    /// The task and edge are committed atomically. Retrying returns the same task,
    /// including after reopening. The anchor succeeds immediately while retaining its
    /// ownership edge. Stopping child work leaves it reusable. A storage error requires reopen.
    pub fn anchor_subagent(&mut self, parent: Uuid, name: &str) -> Result<Uuid, Error> {
        use crate::ownership_edge::{self, Edge, Mode};
        use crate::task::{Checkpoint, Task, TaskOwner};
        use crate::task_store::{self, TaskStore};
        let anchor_key = format!("runtime/subagent-anchor/{parent}/{name}").into_bytes();
        loop {
            let link = self.subagent(parent, name)?.ok_or(Error::NotFound)?;
            let store = self.subagent_store()?;
            if let Some(entry) = store.get(&anchor_key)? {
                let id: Uuid = serde_json::from_slice(&entry.value)?;
                let task = TaskStore::new(store)
                    .load(id)
                    .map_err(|e| Error::Invalid(format!("anchor task: {e}")))?
                    .ok_or(Error::NotFound)?;
                let edge = ownership_edge::EdgeStore::new(store)
                    .get(link.child)
                    .map_err(|e| Error::Invalid(format!("anchor edge: {e}")))?
                    .ok_or(Error::NotFound)?;
                if task.task.owner() != &TaskOwner::Conversation(parent)
                    || !task.task.is_background()
                    || edge.edge
                        != (Edge {
                            task: id,
                            task_owner: TaskOwner::Conversation(parent),
                            child: link.child,
                            mode: Mode::Foreground,
                        })
                {
                    return Err(Error::Invalid("invalid subagent anchor".into()));
                }
                return Ok(id);
            }
            let parent_key = format!("runtime/conversation/{parent}").into_bytes();
            let parent_entry = store.get(&parent_key)?.ok_or(Error::NotFound)?;
            let parent_state: Conversation = serde_json::from_slice(&parent_entry.value)?;
            if parent_state.id != parent || parent_state.abort_requested {
                return Err(Error::Invalid("parent conversation is aborting".into()));
            }
            let child_key = format!("runtime/conversation/{}", link.child).into_bytes();
            let child_entry = store.get(&child_key)?.ok_or(Error::NotFound)?;
            let child: Conversation = serde_json::from_slice(&child_entry.value)?;
            if child.id != link.child || child.config != link.config {
                return Err(Error::Invalid("invalid subagent child".into()));
            }
            let reservation_key = key(parent, name);
            let reservation = store.get(&reservation_key)?.ok_or(Error::NotFound)?;
            let id = Uuid::new_v4();
            let owner = TaskOwner::Conversation(parent);
            let mut task = Task::new(
                id,
                owner.clone(),
                Checkpoint {
                    phase: "subagent-anchor".into(),
                    data: serde_json::Value::Null,
                },
                1,
            )
            .and_then(Task::background)
            .map_err(|e| Error::Invalid(format!("anchor task: {e:?}")))?;
            task.start()
                .and_then(|()| task.succeed(serde_json::Value::Null))
                .map_err(|e| Error::Invalid(format!("anchor completion: {e:?}")))?;
            let edge = Edge {
                task: id,
                task_owner: owner.clone(),
                child: link.child,
                mode: Mode::Foreground,
            };
            let edge_key = ownership_edge::edge_key(link.child);
            let task_key = task_store::key(id);
            let mut checks = ownership_edge::owner_chain_from(store, owner, link.child)
                .map_err(|e| Error::Invalid(format!("anchor owner chain: {e}")))?;
            checks.extend([
                Check {
                    key: parent_key.clone(),
                    expected: Expected::Version(parent_entry.version),
                },
                Check {
                    key: anchor_key.clone(),
                    expected: Expected::Missing,
                },
                Check {
                    key: task_key.clone(),
                    expected: Expected::Missing,
                },
                Check {
                    key: edge_key.clone(),
                    expected: Expected::Missing,
                },
                Check {
                    key: child_key,
                    expected: Expected::Version(child_entry.version),
                },
                Check {
                    key: reservation_key,
                    expected: Expected::Version(reservation.version),
                },
            ]);
            let mutations = [
                Mutation::Put {
                    key: parent_key,
                    value: parent_entry.value,
                },
                Mutation::Put {
                    key: anchor_key.clone(),
                    value: serde_json::to_vec(&id)?,
                },
                Mutation::Put {
                    key: task_key,
                    value: serde_json::to_vec(&task)?,
                },
                Mutation::Put {
                    key: edge_key,
                    value: serde_json::to_vec(&edge)?,
                },
            ];
            match store.commit(&checks, &mutations) {
                Ok(()) => return Ok(id),
                Err(durable_store::Error::Conflict { .. }) => {
                    if store.get(&ownership_edge::edge_key(link.child))?.is_some()
                        && store.get(&anchor_key)?.is_none()
                    {
                        return Err(Error::Invalid("subagent already has an owner".into()));
                    }
                }
                Err(e) => {
                    self.subagent_storage_failed();
                    return Err(e.into());
                }
            }
        }
    }
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
    /// Persist a reporter before admission. Retry uses the first request content.
    pub fn submit_subagent_reported(
        &mut self,
        parent: Uuid,
        name: &str,
        config: Config,
        input: ReportedInput,
    ) -> Result<(Link, Uuid), Error> {
        self.submit_subagent_reported_restricted(parent, name, config, input, None)
    }
    /// Submit with the selection bound to the named reservation.
    pub fn submit_subagent_reported_restricted(
        &mut self,
        parent: Uuid,
        name: &str,
        config: Config,
        input: ReportedInput,
        allowed_tools: Option<Vec<String>>,
    ) -> Result<(Link, Uuid), Error> {
        let ReportedInput {
            history,
            request_id,
            content,
            mode,
        } = input;
        if request_id.is_empty()
            || request_id.starts_with(REPORT_REQUEST_PREFIX)
            || content.trim().is_empty()
        {
            return Err(Error::Invalid(
                "valid subagent request ID and content required".into(),
            ));
        }
        let link = self.ensure_subagent_restricted(parent, name, config, history, allowed_tools)?;
        let k = reporter_key(parent, &request_id);
        // Older databases keyed intents by child. Preserve their parent-wide
        // identity when upgrading, including retired and delivered records.
        for (old_key, entry) in self.subagent_store()?.list(REPORTS)? {
            let old: Reporter = serde_json::from_slice(&entry.value)?;
            if old.link.parent == parent && old.request_id == request_id && old_key != k {
                if old.link != link {
                    return Err(Error::Invalid(
                        "reporter operation ID already belongs to another child".into(),
                    ));
                }
                return self
                    .submit_reporter_input(
                        link.child,
                        old.request_id,
                        old.content,
                        old.mode,
                        parent,
                        old.generation,
                    )
                    .map(|id| (link, id));
            }
        }
        let reporter = loop {
            if let Some(entry) = self.subagent_store()?.get(&k)? {
                let reporter: Reporter = serde_json::from_slice(&entry.value)?;
                if reporter.link != link || reporter.request_id != request_id {
                    return Err(Error::Invalid("invalid reporter record".into()));
                }
                break reporter;
            }
            let parent_entry = self
                .subagent_store()?
                .get(&Harness::key(parent))?
                .ok_or(Error::NotFound)?;
            let parent_state: Conversation = serde_json::from_slice(&parent_entry.value)?;
            if parent_state.abort_requested {
                return Err(Error::Invalid("parent conversation is aborting".into()));
            }
            let (generation, generation_check) = generation_check(self.subagent_store()?, parent)?;
            let reporter = Reporter {
                generation,
                link: link.clone(),
                request_id: request_id.clone(),
                content: content.clone(),
                mode,
                delivered: false,
            };
            match self.subagent_store()?.commit(
                &[
                    Check {
                        key: k.clone(),
                        expected: Expected::Missing,
                    },
                    Check {
                        key: Harness::key(parent),
                        expected: Expected::Version(parent_entry.version),
                    },
                    generation_check,
                ],
                &[Mutation::Put {
                    key: k.clone(),
                    value: serde_json::to_vec(&reporter)?,
                }],
            ) {
                Ok(()) => break reporter,
                Err(durable_store::Error::Conflict { .. }) => continue,
                Err(e) => {
                    self.subagent_storage_failed();
                    return Err(e.into());
                }
            }
        };
        let id = self.submit_reporter_input(
            link.child,
            reporter.request_id,
            reporter.content,
            reporter.mode,
            parent,
            reporter.generation,
        )?;
        Ok((link, id))
    }
    pub(crate) fn pending_reporters(&self) -> Result<Vec<(Vec<u8>, Reporter)>, Error> {
        let mut result = Vec::new();
        for (key, entry) in self.subagent_store()?.list(REPORTS)? {
            let reporter: Reporter = serde_json::from_slice(&entry.value)?;
            if key != reporter_key(reporter.link.parent, &reporter.request_id)
                && key != reporter_key(reporter.link.child, &reporter.request_id)
            {
                return Err(Error::Invalid("invalid reporter key".into()));
            }
            if !reporter.delivered {
                let (generation, check) =
                    generation_check(self.subagent_store()?, reporter.link.parent)?;
                if generation != reporter.generation {
                    let mut retired = reporter;
                    retired.delivered = true;
                    match self.subagent_store()?.commit(
                        &[
                            check,
                            Check {
                                key: key.clone(),
                                expected: Expected::Version(entry.version),
                            },
                        ],
                        &[Mutation::Put {
                            key,
                            value: serde_json::to_vec(&retired)?,
                        }],
                    ) {
                        Ok(()) | Err(durable_store::Error::Conflict { .. }) => (),
                        Err(e) => return Err(e.into()),
                    }
                } else {
                    result.push((key, reporter));
                }
            }
        }
        Ok(result)
    }
    pub(crate) fn reporter_child(&mut self, reporter: &Reporter) -> Result<Option<Uuid>, Error> {
        if generation_check(self.subagent_store()?, reporter.link.parent)?.0 != reporter.generation
        {
            return Ok(None);
        }
        let link = self
            .reservation(reporter.link.parent, &reporter.link.key)?
            .ok_or(Error::NotFound)?;
        if link.link != reporter.link {
            return Err(Error::Invalid("reporter link mismatch".into()));
        }
        self.ensure_subagent_restricted(
            reporter.link.parent,
            &reporter.link.key,
            reporter.link.config.clone(),
            link.history,
            link.allowed_tools,
        )?;
        match self.submit_reporter_input(
            reporter.link.child,
            reporter.request_id.clone(),
            reporter.content.clone(),
            reporter.mode,
            reporter.link.parent,
            reporter.generation,
        ) {
            Ok(_) => Ok(Some(reporter.link.child)),
            Err(Error::Invalid(message)) if message == "stale reporter intent" => Ok(None),
            Err(e) => Err(e),
        }
    }
    /// Admission is idempotent even if interrupted before the receipt is saved.
    pub(crate) fn deliver_report(
        &mut self,
        key: &[u8],
        reporter: &Reporter,
    ) -> Result<Option<Uuid>, Error> {
        let child = self.conversation(reporter.link.child)?;
        let submission = child
            .submissions
            .iter()
            .find(|s| s.request_id.as_deref() == Some(&reporter.request_id))
            .ok_or(Error::NotFound)?;
        let (identity, text) = match &submission.status {
            Status::Done { answer } => (
                child.answer_id(submission.id).ok_or_else(|| {
                    Error::Invalid("answer predates durable reporter identity".into())
                })?,
                format!(
                    "[subagent {} answered, no reply needed] {}",
                    reporter.link.key, answer
                ),
            ),
            Status::Unanswered { reason } => (
                submission.id,
                format!("[subagent {} failed: {}]", reporter.link.key, reason),
            ),
            _ => return Ok(None),
        };
        let request = format!(
            "{REPORT_REQUEST_PREFIX}{}/{}",
            reporter.link.child, identity
        );
        match self.submit_reporter_input(
            reporter.link.parent,
            request,
            text,
            InputMode::FollowUp,
            reporter.link.parent,
            reporter.generation,
        ) {
            Ok(_) => (),
            Err(Error::Invalid(message)) if message == "stale reporter intent" => return Ok(None),
            Err(e) => return Err(e),
        }
        let entry = self.subagent_store()?.get(key)?.ok_or(Error::NotFound)?;
        let mut current: Reporter = serde_json::from_slice(&entry.value)?;
        if !current.delivered {
            current.delivered = true;
            match self.subagent_store()?.commit(
                &[Check {
                    key: key.to_vec(),
                    expected: Expected::Version(entry.version),
                }],
                &[Mutation::Put {
                    key: key.to_vec(),
                    value: serde_json::to_vec(&current)?,
                }],
            ) {
                Ok(()) | Err(durable_store::Error::Conflict { .. }) => (),
                Err(e) => {
                    self.subagent_storage_failed();
                    return Err(e.into());
                }
            }
        }
        Ok(Some(reporter.link.parent))
    }
}
impl Scheduler {
    pub fn subagent_status(
        &mut self,
        parent: Uuid,
        name: Option<&str>,
    ) -> Result<Vec<ChildStatus>, Error> {
        self.subagent_harness().subagent_status(parent, name)
    }
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
    pub fn ensure_subagent_with_history(
        &mut self,
        parent: Uuid,
        name: &str,
        config: Config,
        history: bool,
    ) -> Result<Link, Error> {
        self.subagent_harness()
            .ensure_subagent_with_history(parent, name, config, history)
    }
    pub fn ensure_subagent_restricted(
        &mut self,
        parent: Uuid,
        name: &str,
        config: Config,
        history: bool,
        allowed_tools: Option<Vec<String>>,
    ) -> Result<Link, Error> {
        self.subagent_harness().ensure_subagent_restricted(
            parent,
            name,
            config,
            history,
            allowed_tools,
        )
    }
    pub fn submit_subagent_reported_restricted(
        &mut self,
        parent: Uuid,
        name: &str,
        config: Config,
        input: ReportedInput,
        allowed_tools: Option<Vec<String>>,
    ) -> Result<(Link, Uuid), Error> {
        let result = self
            .subagent_harness()
            .submit_subagent_reported_restricted(parent, name, config, input, allowed_tools)?;
        self.schedule(result.0.child);
        Ok(result)
    }
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
    pub fn submit_subagent_reported(
        &mut self,
        parent: Uuid,
        name: &str,
        config: Config,
        input: ReportedInput,
    ) -> Result<(Link, Uuid), Error> {
        let result = self
            .subagent_harness()
            .submit_subagent_reported(parent, name, config, input)?;
        self.schedule(result.0.child);
        Ok(result)
    }
}
