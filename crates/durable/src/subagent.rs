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
}
fn key(parent: Uuid, name: &str) -> Vec<u8> {
    let mut key = format!("runtime/subagent/{parent}/").into_bytes();
    key.extend_from_slice(name.as_bytes());
    key
}
fn reporter_key(child: Uuid, request_id: &str) -> Vec<u8> {
    let mut key = format!("runtime/subagent-report/{child}/").into_bytes();
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
        if name.is_empty() || config.model.is_empty() || config.max_output_tokens == 0 {
            return Err(Error::Invalid(
                "subagent key, model and output limit required".into(),
            ));
        }
        self.conversation(parent)?;
        let k = key(parent, name);
        let reservation = loop {
            if let Some(reservation) = self.reservation(parent, name)? {
                if reservation.link.config != config || reservation.history != history {
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
                Some(self.subagent_seed(parent, link.child, config.clone())?)
            } else {
                None
            };
            let reservation = Reservation {
                link,
                history,
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
                    self.create_subagent_from_seed(&seed)?;
                } else {
                    self.create_conversation_with_id(link.child, link.config.clone())?;
                }
            }
            Err(e) => return Err(e),
        }
        Ok(link)
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
        let link = self.ensure_subagent_with_history(parent, name, config, history)?;
        let k = reporter_key(link.child, &request_id);
        let reporter = loop {
            if let Some(entry) = self.subagent_store()?.get(&k)? {
                let reporter: Reporter = serde_json::from_slice(&entry.value)?;
                if reporter.link != link || reporter.request_id != request_id {
                    return Err(Error::Invalid("invalid reporter record".into()));
                }
                break reporter;
            }
            let reporter = Reporter {
                link: link.clone(),
                request_id: request_id.clone(),
                content: content.clone(),
                mode,
                delivered: false,
            };
            match self.subagent_store()?.commit(
                &[Check {
                    key: k.clone(),
                    expected: Expected::Missing,
                }],
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
        let id = self.submit_with_mode(
            link.child,
            Some(reporter.request_id),
            reporter.content,
            reporter.mode,
        )?;
        Ok((link, id))
    }
    pub(crate) fn pending_reporters(&self) -> Result<Vec<(Vec<u8>, Reporter)>, Error> {
        let mut result = Vec::new();
        for (key, entry) in self.subagent_store()?.list(REPORTS)? {
            let reporter: Reporter = serde_json::from_slice(&entry.value)?;
            if key != reporter_key(reporter.link.child, &reporter.request_id) {
                return Err(Error::Invalid("invalid reporter key".into()));
            }
            if !reporter.delivered {
                result.push((key, reporter));
            }
        }
        Ok(result)
    }
    pub(crate) fn reporter_child(&mut self, reporter: &Reporter) -> Result<Uuid, Error> {
        let link = self
            .reservation(reporter.link.parent, &reporter.link.key)?
            .ok_or(Error::NotFound)?;
        if link.link != reporter.link {
            return Err(Error::Invalid("reporter link mismatch".into()));
        }
        self.ensure_subagent_with_history(
            reporter.link.parent,
            &reporter.link.key,
            reporter.link.config.clone(),
            link.history,
        )?;
        self.submit_with_mode(
            reporter.link.child,
            Some(reporter.request_id.clone()),
            reporter.content.clone(),
            reporter.mode,
        )?;
        Ok(reporter.link.child)
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
        self.submit_internal(
            reporter.link.parent,
            Some(request),
            text,
            InputMode::FollowUp,
        )?;
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
