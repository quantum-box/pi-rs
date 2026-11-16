//! Opt-in task-owned subagent admission. Harness abort traversal follows foreground edges.
use crate::{
    ownership_edge::{self, Edge, Mode},
    runtime::{Config, Conversation, Error, Harness, Scheduler, SubmissionNotify},
    task::{Task, TaskOwner, TaskState},
    task_store,
};
use durable_store::{Check, Expected, Mutation};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A key is scoped to one task and permanently binds its configuration and mode.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Admission {
    pub task: Uuid,
    pub task_owner: TaskOwner,
    pub key: String,
    pub child: Uuid,
    pub config: Config,
    pub mode: Mode,
    /// Initial tool selection, bound to this reservation.
    #[serde(default)]
    pub allowed_tools: Option<Vec<String>>,
    /// Whether this reservation copied the owning conversation's tool configuration.
    #[serde(default, skip_serializing_if = "is_false")]
    pub inherit_parent_tools: bool,
    /// Runtime proof that the inherited admission rejected extension-dependent policy.
    #[serde(default, skip_serializing_if = "is_false")]
    pub inherited_policy_checked: bool,
    /// Initial submission, if this reservation was created with input.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission: Option<InitialSubmission>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InitialSubmission {
    pub id: Uuid,
    pub content: String,
    /// Settlement destination, permanently bound to this admission.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub notify: Option<SubmissionNotify>,
}

fn is_false(value: &bool) -> bool {
    !value
}

fn reservation_key(task: Uuid, name: &str) -> Vec<u8> {
    let mut key = format!("durable/owned-subagent/{task}/").into_bytes();
    key.extend_from_slice(name.as_bytes());
    key
}
pub(crate) const NOTIFICATION_BINDING_PREFIX: &[u8] = b"durable/owned-subagent-notify/";
/// Reserved destinations are permanent, including after settlement and reopen.
pub(crate) fn notification_binding_key(notify: &SubmissionNotify) -> Vec<u8> {
    use std::fmt::Write;
    let mut key = format!("durable/owned-subagent-notify/{}/", notify.task_id);
    for byte in notify.key.as_bytes() {
        write!(&mut key, "{byte:02x}").expect("writing to a String cannot fail");
    }
    key.into_bytes()
}

fn child_key(id: Uuid) -> Vec<u8> {
    format!("runtime/conversation/{id}").into_bytes()
}
fn invalid() -> Error {
    Error::Invalid("invalid owned subagent admission".into())
}

// Resolve the task's own conversation, stopping at the first conversation edge.
// Every traversed task and the copied conversation are fenced with the child commit.
fn owning_conversation(
    store: &durable_store::Store,
    owner: &TaskOwner,
) -> Result<(Conversation, Vec<Check>), Error> {
    let mut owner = owner.clone();
    let mut seen = std::collections::HashSet::new();
    let mut checks = Vec::new();
    loop {
        match owner {
            TaskOwner::Conversation(id) => {
                let key = child_key(id);
                let entry = store.get(&key)?.ok_or_else(invalid)?;
                let conversation: Conversation = serde_json::from_slice(&entry.value)?;
                if conversation.id != id {
                    return Err(invalid());
                }
                checks.push(Check {
                    key,
                    expected: Expected::Version(entry.version),
                });
                return Ok((conversation, checks));
            }
            TaskOwner::Task(id) => {
                if !seen.insert(id) {
                    return Err(invalid());
                }
                let key = task_store::key(id);
                let entry = store.get(&key)?.ok_or_else(invalid)?;
                let task: Task = serde_json::from_slice(&entry.value)?;
                if task.id() != id {
                    return Err(invalid());
                }
                checks.push(Check {
                    key,
                    expected: Expected::Version(entry.version),
                });
                owner = task.owner().clone();
            }
        }
    }
}

// Admissions created before the extension-policy check did not record enough
// information to prove their child is safe to execute. Fail closed after upgrade.
pub(crate) fn validate_inherited_policy(
    store: &durable_store::Store,
    child: Uuid,
) -> Result<(), Error> {
    let Some(edge) = ownership_edge::EdgeStore::new(store)
        .get(child)
        .map_err(|e| Error::Invalid(format!("owned subagent edge: {e}")))?
    else {
        return Ok(());
    };
    let prefix = format!("durable/owned-subagent/{}/", edge.edge.task);
    let mut matched = false;
    for (_, entry) in store.list(prefix.as_bytes())? {
        let admission: Admission = serde_json::from_slice(&entry.value)?;
        if admission.child == child {
            if matched
                || admission.task != edge.edge.task
                || admission.task_owner != edge.edge.task_owner
                || admission.mode != edge.edge.mode
            {
                return Err(invalid());
            }
            matched = true;
            if admission.inherit_parent_tools && !admission.inherited_policy_checked {
                return Err(Error::UnsafeInheritedPolicy);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
thread_local! {
    static BEFORE_INHERITED_ADMISSION: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = std::cell::RefCell::new(None);
}

impl Harness {
    /// Atomically creates an empty child, its immutable ownership edge and its
    /// reservation, while advancing the task's storage version. Retrying the
    /// same key and parameters returns the original child, including after an
    /// abort or reopen. Different parameters for the key are rejected.
    ///
    /// A storage error has an uncertain outcome: reopen before retrying. This
    /// API does not submit input, schedule, traverse, or abort the child.
    pub fn ensure_task_owned_subagent(
        &mut self,
        task: Uuid,
        owner: TaskOwner,
        name: &str,
        config: Config,
        mode: Mode,
    ) -> Result<Admission, Error> {
        self.admit_task_owned_subagent(task, owner, name, config, mode, None, None, None, false)
    }

    /// Atomically reserve a child and admit its first input. The key permanently
    /// binds the input as well as the owner, configuration and ownership mode.
    /// A storage error has an uncertain outcome; reopen before retrying.
    pub fn submit_task_owned_subagent(
        &mut self,
        task: Uuid,
        owner: TaskOwner,
        name: &str,
        config: Config,
        mode: Mode,
        content: String,
    ) -> Result<Admission, Error> {
        if content.trim().is_empty() {
            return Err(invalid());
        }
        self.admit_task_owned_subagent(
            task,
            owner,
            name,
            config,
            mode,
            Some(content),
            None,
            None,
            false,
        )
    }

    /// Atomically create an owned child with its initial tool selection.
    pub fn ensure_task_owned_subagent_restricted(
        &mut self,
        task: Uuid,
        owner: TaskOwner,
        name: &str,
        config: Config,
        mode: Mode,
        allowed_tools: Option<Vec<String>>,
    ) -> Result<Admission, Error> {
        self.admit_task_owned_subagent(
            task,
            owner,
            name,
            config,
            mode,
            None,
            allowed_tools,
            None,
            false,
        )
    }
    /// Atomically create a restricted child and queue its first input.
    #[allow(clippy::too_many_arguments)]
    pub fn submit_task_owned_subagent_restricted(
        &mut self,
        task: Uuid,
        owner: TaskOwner,
        name: &str,
        config: Config,
        mode: Mode,
        content: String,
        allowed_tools: Option<Vec<String>>,
    ) -> Result<Admission, Error> {
        if content.trim().is_empty() {
            return Err(invalid());
        }
        self.admit_task_owned_subagent(
            task,
            owner,
            name,
            config,
            mode,
            Some(content),
            allowed_tools,
            None,
            false,
        )
    }

    /// Atomically submit a foreground child and notify its owning task when it settles.
    #[allow(clippy::too_many_arguments)]
    pub fn submit_task_owned_subagent_notified(
        &mut self,
        task: Uuid,
        owner: TaskOwner,
        name: &str,
        config: Config,
        content: String,
        allowed_tools: Option<Vec<String>>,
        signal_key: String,
    ) -> Result<Admission, Error> {
        if content.trim().is_empty() || signal_key.is_empty() {
            return Err(invalid());
        }
        self.admit_task_owned_subagent(
            task,
            owner,
            name,
            config,
            Mode::Foreground,
            Some(content),
            allowed_tools,
            Some(SubmissionNotify {
                task_id: task,
                key: signal_key,
            }),
            false,
        )
    }

    /// Create a child with a snapshot of the owner conversation's tool settings.
    /// The copy and reservation commit atomically; later parent changes do not propagate.
    pub fn ensure_task_owned_subagent_inheriting_tools(
        &mut self,
        task: Uuid,
        owner: TaskOwner,
        name: &str,
        config: Config,
        mode: Mode,
    ) -> Result<Admission, Error> {
        self.admit_task_owned_subagent(task, owner, name, config, mode, None, None, None, true)
    }

    /// Create a child with inherited tool settings and its first queued input.
    pub fn submit_task_owned_subagent_inheriting_tools(
        &mut self,
        task: Uuid,
        owner: TaskOwner,
        name: &str,
        config: Config,
        mode: Mode,
        content: String,
    ) -> Result<Admission, Error> {
        if content.trim().is_empty() {
            return Err(invalid());
        }
        self.admit_task_owned_subagent(
            task,
            owner,
            name,
            config,
            mode,
            Some(content),
            None,
            None,
            true,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn admit_task_owned_subagent(
        &mut self,
        task: Uuid,
        owner: TaskOwner,
        name: &str,
        config: Config,
        mode: Mode,
        content: Option<String>,
        allowed_tools: Option<Vec<String>>,
        notify: Option<SubmissionNotify>,
        inherit_parent_tools: bool,
    ) -> Result<Admission, Error> {
        crate::runtime::validate_allowed_tools(&allowed_tools)?;
        if name.is_empty() {
            return Err(invalid());
        }
        crate::runtime::validate_config(&config).map_err(|_| invalid())?;
        // Serialize admission with the scheduler's post-handler graph decision.
        let owner_store = self.owner.clone();
        let _gate = owner_store.task_gate().lock().map_err(|_| Error::Failed)?;
        let publication = self
            .task_publication
            .lock()
            .map_err(|_| Error::Failed)?
            .clone();
        let _publication = publication
            .as_ref()
            .map(|publication| publication.admission())
            .transpose()
            .map_err(|_| Error::Failed)?;
        if let Some(publication) = &_publication {
            let snapshot = self
                .subagent_store()?
                .get(&task_store::key(task))?
                .ok_or_else(invalid)?;
            let parent: Task = serde_json::from_slice(&snapshot.value)?;
            if parent.id() != task || parent.owner() != &owner {
                return Err(invalid());
            }
            if !publication.permits_admission(&owner_store, Uuid::nil(), Some(&parent)) {
                return Err(Error::TaskPublicationBusy);
            }
        }
        let key = reservation_key(task, name);
        let mut inherited_conflicts = 0;
        loop {
            let store = self.subagent_store()?;
            if let Some(entry) = store.get(&key)? {
                let a: Admission = serde_json::from_slice(&entry.value)?;
                if a.task != task
                    || a.task_owner != owner
                    || a.key != name
                    || a.config != config
                    || a.mode != mode
                    || a.allowed_tools != allowed_tools
                    || a.inherit_parent_tools != inherit_parent_tools
                    || (a.inherit_parent_tools && !a.inherited_policy_checked)
                    || a.submission.as_ref().map(|s| &s.content) != content.as_ref()
                    || a.submission.as_ref().and_then(|s| s.notify.as_ref()) != notify.as_ref()
                    || a.task_owner == TaskOwner::Conversation(a.child)
                {
                    return Err(invalid());
                }
                if let Some(notify) = &notify {
                    let binding = store
                        .get(&notification_binding_key(notify))?
                        .ok_or_else(invalid)?;
                    let bound: (Uuid, Uuid) = serde_json::from_slice(&binding.value)?;
                    if Some(bound)
                        != a.submission
                            .as_ref()
                            .map(|submission| (a.child, submission.id))
                    {
                        return Err(invalid());
                    }
                }
                let edge = store
                    .get(&ownership_edge::edge_key(a.child))?
                    .ok_or_else(invalid)?;
                let edge: Edge = serde_json::from_slice(&edge.value)?;
                let child = store.get(&child_key(a.child))?.ok_or_else(invalid)?;
                let child: Conversation = serde_json::from_slice(&child.value)?;
                if edge
                    != (Edge {
                        task,
                        task_owner: owner.clone(),
                        child: a.child,
                        mode,
                    })
                    || child.id != a.child
                {
                    return Err(invalid());
                }
                return Ok(a);
            }
            let tk = task_store::key(task);
            let snapshot = store.get(&tk)?.ok_or_else(invalid)?;
            let t: Task = serde_json::from_slice(&snapshot.value)?;
            if t.id() != task
                || t.owner() != &owner
                || t.abort_requested()
                || !matches!(
                    t.state(),
                    TaskState::Ready | TaskState::Running | TaskState::Waiting(_)
                )
            {
                return Err(invalid());
            }
            // Fence legacy conversation notifications and signal receipts as well
            // as the new binding. Other admission paths check the binding in their
            // conversation save, so neither order of a concurrent race can win twice.
            let boundary = if let Some(notify) = &notify {
                let boundary = store.put_version()?;
                if store.get(&notification_binding_key(notify))?.is_some() {
                    return Err(invalid());
                }
                let revision_key = task_store::signal_revision_key(task);
                let revision = store.get(&revision_key)?;
                if task_store::TaskStore::new(store)
                    .read_typed_signal_state(
                        task,
                        &crate::runtime::submission_settled_signal(),
                        &notify.key,
                    )
                    .map_err(|_| invalid())?
                    .is_some()
                {
                    return Err(invalid());
                }
                for (_, entry) in store.list(b"runtime/conversation/")? {
                    let conversation: Conversation = serde_json::from_slice(&entry.value)?;
                    if conversation.has_submission_notification(notify) {
                        return Err(invalid());
                    }
                }
                Some((
                    boundary,
                    revision_key,
                    revision.map_or(Expected::Missing, |e| Expected::Version(e.version)),
                ))
            } else {
                None
            };
            let child = Uuid::new_v4();
            if owner == TaskOwner::Conversation(child) {
                continue;
            }
            let a = Admission {
                task,
                task_owner: owner.clone(),
                key: name.into(),
                child,
                config: config.clone(),
                mode,
                allowed_tools: allowed_tools.clone(),
                inherit_parent_tools,
                inherited_policy_checked: inherit_parent_tools,
                submission: content.as_ref().map(|content| InitialSubmission {
                    id: Uuid::new_v4(),
                    content: content.clone(),
                    notify: notify.clone(),
                }),
            };
            let edge = Edge {
                task,
                task_owner: owner.clone(),
                child,
                mode,
            };
            let ek = ownership_edge::edge_key(child);
            let mut child_state = Harness::empty_conversation(child, config.clone());
            child_state.allowed_tools = allowed_tools.clone();
            let inherited = if inherit_parent_tools {
                let (parent, parent_checks) = owning_conversation(store, &owner)?;
                if !parent.extensions.is_empty() {
                    return Err(Error::Invalid(
                        "cannot inherit tool policy while parent extensions are selected".into(),
                    ));
                }
                child_state.allowed_tools = parent.allowed_tools;
                child_state.model_tools = parent.model_tools;
                child_state.tool_filters = parent.tool_filters;
                parent_checks
            } else {
                Vec::new()
            };
            if let Some(initial) = &a.submission {
                child_state.admit_initial_input(initial.id, initial.content.clone());
                if let Some(notify) = &initial.notify {
                    child_state.set_initial_notification(initial.id, notify.clone());
                }
            }
            let mut prepared = self.prepare_save(&child_state, Expected::Missing)?;
            let mut checks = ownership_edge::owner_chain_admission_checks(store, task, child)
                .map_err(|e| match e {
                    ownership_edge::Error::Store(e) => Error::from(e),
                    ownership_edge::Error::Json(e) => Error::from(e),
                    _ => invalid(),
                })?;
            checks.extend(inherited);
            checks.extend([
                Check {
                    key: tk.clone(),
                    expected: Expected::Version(snapshot.version),
                },
                Check {
                    key: key.clone(),
                    expected: Expected::Missing,
                },
                Check {
                    key: ek.clone(),
                    expected: Expected::Missing,
                },
            ]);
            let mut mutations = vec![
                Mutation::Put {
                    key: tk,
                    value: snapshot.value,
                },
                Mutation::Put {
                    key: key.clone(),
                    value: serde_json::to_vec(&a)?,
                },
                Mutation::Put {
                    key: ek,
                    value: serde_json::to_vec(&edge)?,
                },
            ];
            if let Some((_, revision_key, expected)) = &boundary {
                checks.push(Check {
                    key: revision_key.clone(),
                    expected: expected.clone(),
                });
                let initial = a.submission.as_ref().ok_or_else(invalid)?;
                mutations.push(Mutation::Put {
                    key: notification_binding_key(notify.as_ref().ok_or_else(invalid)?),
                    value: serde_json::to_vec(&(child, initial.id))?,
                });
            }
            prepared.checks.extend(checks);
            prepared.mutations.extend(mutations);
            #[cfg(test)]
            if inherit_parent_tools {
                BEFORE_INHERITED_ADMISSION.with(|hook| {
                    if let Some(hook) = hook.borrow_mut().take() {
                        hook();
                    }
                });
            }
            let result = if let Some((boundary, _, _)) = boundary {
                store.commit_with_prefix_put_fence(
                    &prepared.checks,
                    &prepared.mutations,
                    &[b"runtime/conversation/"],
                    boundary,
                )
            } else {
                store.commit(&prepared.checks, &prepared.mutations)
            };
            match result {
                Ok(()) => return Ok(a),
                Err(error @ durable_store::Error::Conflict { .. }) if inherit_parent_tools => {
                    inherited_conflicts += 1;
                    if inherited_conflicts >= 8 {
                        return Err(error.into());
                    }
                    continue;
                }
                Err(durable_store::Error::Conflict { .. }) => continue,
                Err(e) => {
                    self.subagent_storage_failed();
                    return Err(e.into());
                }
            }
        }
    }
}
impl Scheduler {
    /// Commit ownership and initial input before scheduling the child. Reopening
    /// also schedules committed children if the process stops before scheduling.
    pub fn submit_task_owned_subagent(
        &mut self,
        task: Uuid,
        owner: TaskOwner,
        name: &str,
        config: Config,
        mode: Mode,
        content: String,
    ) -> Result<Admission, Error> {
        let admission = self
            .subagent_harness()
            .submit_task_owned_subagent(task, owner, name, config, mode, content)?;
        self.schedule(admission.child);
        Ok(admission)
    }

    /// Submit a foreground child and schedule it after the notification is committed.
    #[allow(clippy::too_many_arguments)]
    pub fn submit_task_owned_subagent_notified(
        &mut self,
        task: Uuid,
        owner: TaskOwner,
        name: &str,
        config: Config,
        content: String,
        allowed_tools: Option<Vec<String>>,
        signal_key: String,
    ) -> Result<Admission, Error> {
        let admission = self
            .subagent_harness()
            .submit_task_owned_subagent_notified(
                task,
                owner,
                name,
                config,
                content,
                allowed_tools,
                signal_key,
            )?;
        self.schedule(admission.child);
        Ok(admission)
    }

    /// Submit and schedule a child with its owner's tool settings captured atomically.
    pub fn submit_task_owned_subagent_inheriting_tools(
        &mut self,
        task: Uuid,
        owner: TaskOwner,
        name: &str,
        config: Config,
        mode: Mode,
        content: String,
    ) -> Result<Admission, Error> {
        let admission = self
            .subagent_harness()
            .submit_task_owned_subagent_inheriting_tools(
                task, owner, name, config, mode, content,
            )?;
        self.schedule(admission.child);
        Ok(admission)
    }

    /// Reserve a child with its owner's tool settings captured atomically.
    pub fn ensure_task_owned_subagent_inheriting_tools(
        &mut self,
        task: Uuid,
        owner: TaskOwner,
        name: &str,
        config: Config,
        mode: Mode,
    ) -> Result<Admission, Error> {
        self.subagent_harness()
            .ensure_task_owned_subagent_inheriting_tools(task, owner, name, config, mode)
    }

    /// Commit a restricted child and initial input before scheduling it.
    #[allow(clippy::too_many_arguments)]
    pub fn submit_task_owned_subagent_restricted(
        &mut self,
        task: Uuid,
        owner: TaskOwner,
        name: &str,
        config: Config,
        mode: Mode,
        content: String,
        allowed_tools: Option<Vec<String>>,
    ) -> Result<Admission, Error> {
        let admission = self
            .subagent_harness()
            .submit_task_owned_subagent_restricted(
                task,
                owner,
                name,
                config,
                mode,
                content,
                allowed_tools,
            )?;
        self.schedule(admission.child);
        Ok(admission)
    }
    pub fn ensure_task_owned_subagent_restricted(
        &mut self,
        task: Uuid,
        owner: TaskOwner,
        name: &str,
        config: Config,
        mode: Mode,
        allowed_tools: Option<Vec<String>>,
    ) -> Result<Admission, Error> {
        self.subagent_harness()
            .ensure_task_owned_subagent_restricted(task, owner, name, config, mode, allowed_tools)
    }

    /// Reserve an owned child without scheduling it.
    pub fn ensure_task_owned_subagent(
        &mut self,
        task: Uuid,
        owner: TaskOwner,
        name: &str,
        config: Config,
        mode: Mode,
    ) -> Result<Admission, Error> {
        self.subagent_harness()
            .ensure_task_owned_subagent(task, owner, name, config, mode)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ownership_edge::EdgeStore,
        recovery::UncertainPolicy,
        runtime::Options,
        task::{Checkpoint, Task},
        task_store::{Save, TaskStore},
    };
    use async_trait::async_trait;
    use pi_rs_core::{Error as ModelError, Model, ModelRequest, ModelStream};
    use std::sync::{Arc, Barrier};

    struct TestTool;
    #[async_trait]
    impl pi_rs_core::Tool for TestTool {
        fn spec(&self) -> pi_rs_core::ToolSpec {
            pi_rs_core::ToolSpec {
                name: "beta".into(),
                description: String::new(),
                parameters: serde_json::json!({"type":"object"}),
            }
        }
        async fn invoke(
            &self,
            _: serde_json::Value,
            _: pi_rs_core::ToolContext,
        ) -> pi_rs_core::Outcome {
            pi_rs_core::Outcome::Succeeded("ok".into())
        }
    }
    struct RestrictedTool;
    #[async_trait]
    impl pi_rs_core::Tool for RestrictedTool {
        fn spec(&self) -> pi_rs_core::ToolSpec {
            pi_rs_core::ToolSpec {
                name: "beta".into(),
                description: "restricted extension implementation".into(),
                parameters: serde_json::json!({"type":"object"}),
            }
        }
        async fn invoke(
            &self,
            _: serde_json::Value,
            _: pi_rs_core::ToolContext,
        ) -> pi_rs_core::Outcome {
            pi_rs_core::Outcome::Failed("denied".into())
        }
    }
    fn policy_extension() -> crate::extension::Extension {
        crate::extension::Extension {
            name: "policy".into(),
            tools: vec![Arc::new(RestrictedTool)],
            ..Default::default()
        }
    }

    struct Stub;
    #[async_trait]
    impl Model for Stub {
        async fn stream(&self, _: ModelRequest) -> Result<ModelStream, ModelError> {
            unreachable!()
        }
    }
    fn options() -> Options {
        Options {
            model: Arc::new(Stub),
            tools: vec![],
            restart_limit: 1,
            max_turns: 1,
            uncertain_policy: UncertainPolicy::InterruptedResult,
        }
    }
    fn config() -> Config {
        Config {
            model: "test".into(),
            system_prompt: String::new(),
            max_output_tokens: 10,
        }
    }
    fn setup(h: &mut Harness) -> (Uuid, TaskOwner) {
        let parent = h.create_conversation(config()).unwrap();
        let owner = TaskOwner::Conversation(parent);
        let task = Task::new(
            Uuid::new_v4(),
            owner.clone(),
            Checkpoint {
                phase: "start".into(),
                data: serde_json::Value::Null,
            },
            1,
        )
        .unwrap();
        TaskStore::new(h.subagent_store().unwrap())
            .save(&[Save::Create(&task)])
            .unwrap();
        (task.id(), owner)
    }

    #[test]
    fn inherited_tool_settings_are_snapshot_bound_and_survive_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut configured = options();
        configured.tools.push(crate::runtime::RegisteredTool {
            tool: Arc::new(TestTool),
            replay: crate::recovery::ReplayPolicy::Safe,
        });
        let mut h = Harness::open_paused(&path, configured).unwrap();
        let (task, owner) = setup(&mut h);
        let TaskOwner::Conversation(parent) = owner else {
            unreachable!()
        };
        h.select_tools(parent, Some(vec!["alpha".into(), "beta".into()]))
            .unwrap();
        h.select_model_tools(parent, Some(vec!["beta".into()]))
            .unwrap();
        h.set_deferred_tools(parent, vec!["beta".into()]).unwrap();
        h.set_tool_callers(
            parent,
            "beta".into(),
            Some(crate::runtime::ToolCallers::ToolsOnly),
        )
        .unwrap();
        let child_task = Task::new(
            Uuid::new_v4(),
            TaskOwner::Task(task),
            Checkpoint {
                phase: "start".into(),
                data: serde_json::Value::Null,
            },
            1,
        )
        .unwrap();
        TaskStore::new(h.subagent_store().unwrap())
            .save(&[Save::Create(&child_task)])
            .unwrap();
        let child_owner = TaskOwner::Task(task);
        let a = h
            .submit_task_owned_subagent_inheriting_tools(
                child_task.id(),
                child_owner.clone(),
                "child",
                config(),
                Mode::Foreground,
                "hello".into(),
            )
            .unwrap();
        let original = h.conversation(parent).unwrap();
        let child = h.conversation(a.child).unwrap();
        assert_eq!(child.allowed_tools, original.allowed_tools);
        assert_eq!(child.model_tools, original.model_tools);
        assert_eq!(child.tool_filters, original.tool_filters);
        h.set_deferred_tools(parent, vec!["new".into()]).unwrap();
        drop(h);
        let mut h = Harness::open_paused(&path, options()).unwrap();
        let replay = h
            .submit_task_owned_subagent_inheriting_tools(
                child_task.id(),
                child_owner.clone(),
                "child",
                config(),
                Mode::Foreground,
                "hello".into(),
            )
            .unwrap();
        assert_eq!(a, replay);
        assert_eq!(
            h.conversation(a.child).unwrap().tool_filters,
            original.tool_filters
        );
        assert!(
            h.submit_task_owned_subagent(
                child_task.id(),
                child_owner,
                "child",
                config(),
                Mode::Foreground,
                "hello".into()
            )
            .is_err()
        );
        let legacy = h
            .ensure_task_owned_subagent(
                task,
                TaskOwner::Conversation(parent),
                "legacy",
                config(),
                Mode::Background,
            )
            .unwrap();
        assert_eq!(h.conversation(legacy.child).unwrap().allowed_tools, None);
        assert_eq!(
            h.conversation(legacy.child).unwrap().tool_filters,
            Default::default()
        );
    }

    #[test]
    fn inherited_settings_use_the_nearest_owned_conversation() {
        let dir = tempfile::tempdir().unwrap();
        let mut h = Harness::open_paused(dir.path().join("db"), options()).unwrap();
        let (task, owner) = setup(&mut h);
        let TaskOwner::Conversation(root) = owner.clone() else {
            unreachable!()
        };
        h.set_deferred_tools(root, vec!["root".into()]).unwrap();
        let middle = h
            .ensure_task_owned_subagent(task, owner, "middle", config(), Mode::Background)
            .unwrap();
        h.set_deferred_tools(middle.child, vec!["middle".into()])
            .unwrap();
        let middle_task = Task::new(
            Uuid::new_v4(),
            TaskOwner::Conversation(middle.child),
            Checkpoint {
                phase: "start".into(),
                data: serde_json::Value::Null,
            },
            1,
        )
        .unwrap();
        TaskStore::new(h.subagent_store().unwrap())
            .save(&[Save::Create(&middle_task)])
            .unwrap();
        let leaf = h
            .ensure_task_owned_subagent_inheriting_tools(
                middle_task.id(),
                middle_task.owner().clone(),
                "leaf",
                config(),
                Mode::Foreground,
            )
            .unwrap();
        assert_eq!(
            h.conversation(leaf.child).unwrap().tool_filters.deferred,
            vec!["middle"]
        );
    }

    #[test]
    fn inherited_policy_rejects_extension_override_and_retries_after_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut opts = options();
        opts.tools.push(crate::runtime::RegisteredTool {
            tool: Arc::new(TestTool),
            replay: crate::recovery::ReplayPolicy::Safe,
        });
        let mut h = Harness::open_paused(&path, opts).unwrap();
        h.install_extension(policy_extension()).unwrap();
        let (task, owner) = setup(&mut h);
        let TaskOwner::Conversation(parent) = owner.clone() else {
            unreachable!()
        };
        h.select_tools(parent, Some(vec!["beta".into()])).unwrap();
        h.select_extensions(parent, vec!["policy".into()]).unwrap();
        assert!(
            h.ensure_task_owned_subagent_inheriting_tools(
                task,
                owner.clone(),
                "blocked",
                config(),
                Mode::Foreground
            )
            .is_err()
        );
        assert!(
            h.subagent_store()
                .unwrap()
                .get(&reservation_key(task, "blocked"))
                .unwrap()
                .is_none()
        );
        drop(h);
        let mut h = Harness::open_paused(&path, options()).unwrap();
        assert!(
            h.ensure_task_owned_subagent_inheriting_tools(
                task,
                owner.clone(),
                "blocked",
                config(),
                Mode::Foreground
            )
            .is_err()
        );
        h.select_extensions(parent, vec![]).unwrap();
        let safe = h
            .ensure_task_owned_subagent_inheriting_tools(
                task,
                owner.clone(),
                "blocked",
                config(),
                Mode::Foreground,
            )
            .unwrap();
        assert!(safe.inherited_policy_checked);
        drop(h);
        let mut h = Harness::open_paused(&path, options()).unwrap();
        assert_eq!(
            h.ensure_task_owned_subagent_inheriting_tools(
                task,
                owner,
                "blocked",
                config(),
                Mode::Foreground
            )
            .unwrap(),
            safe
        );
        assert_eq!(
            h.conversation(safe.child).unwrap().allowed_tools,
            Some(vec!["beta".into()])
        );
    }

    #[test]
    fn inherited_policy_rechecks_extensions_changed_before_commit() {
        let dir = tempfile::tempdir().unwrap();
        let mut h = Harness::open_paused(dir.path().join("db"), options()).unwrap();
        h.install_extension(policy_extension()).unwrap();
        let (task, owner) = setup(&mut h);
        let TaskOwner::Conversation(parent) = owner.clone() else {
            unreachable!()
        };
        let mut concurrent = h.worker();
        BEFORE_INHERITED_ADMISSION.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || {
                concurrent
                    .select_extensions(parent, vec!["policy".into()])
                    .unwrap();
            }))
        });
        assert!(
            h.ensure_task_owned_subagent_inheriting_tools(
                task,
                owner,
                "race-policy",
                config(),
                Mode::Background
            )
            .is_err()
        );
        assert!(
            h.subagent_store()
                .unwrap()
                .get(&reservation_key(task, "race-policy"))
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn precheck_inherited_child_is_blocked_after_upgrade_and_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut h = Harness::open_paused(&path, options()).unwrap();
        let (task, owner) = setup(&mut h);
        let a = h
            .submit_task_owned_subagent_inheriting_tools(
                task,
                owner.clone(),
                "old",
                config(),
                Mode::Foreground,
                "work".into(),
            )
            .unwrap();
        let key = reservation_key(task, "old");
        let store = h.subagent_store().unwrap();
        let entry = store.get(&key).unwrap().unwrap();
        let mut value: serde_json::Value = serde_json::from_slice(&entry.value).unwrap();
        value
            .as_object_mut()
            .unwrap()
            .remove("inherited_policy_checked");
        store
            .commit(
                &[Check {
                    key: key.clone(),
                    expected: Expected::Version(entry.version),
                }],
                &[Mutation::Put {
                    key,
                    value: serde_json::to_vec(&value).unwrap(),
                }],
            )
            .unwrap();
        drop(h);
        let mut h = Harness::open_paused(&path, options()).unwrap();
        assert!(h.conversation(a.child).is_err());
        assert!(
            h.submit_task_owned_subagent_inheriting_tools(
                task,
                owner,
                "old",
                config(),
                Mode::Foreground,
                "work".into()
            )
            .is_err()
        );
        assert!(matches!(
            h.resume_conversation(a.child, &pi_rs_core::CancellationToken::new())
                .await,
            Err(Error::UnsafeInheritedPolicy)
        ));
    }

    #[test]
    fn inherited_tool_settings_retry_parent_change_before_commit() {
        let dir = tempfile::tempdir().unwrap();
        let mut h = Harness::open_paused(dir.path().join("db"), options()).unwrap();
        let (task, owner) = setup(&mut h);
        let TaskOwner::Conversation(parent) = owner.clone() else {
            unreachable!()
        };
        h.set_deferred_tools(parent, vec!["before".into()]).unwrap();
        let mut concurrent = h.worker();
        BEFORE_INHERITED_ADMISSION.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || {
                concurrent
                    .set_deferred_tools(parent, vec!["after".into()])
                    .unwrap();
            }))
        });
        let a = h
            .ensure_task_owned_subagent_inheriting_tools(
                task,
                owner,
                "race",
                config(),
                Mode::Background,
            )
            .unwrap();
        assert_eq!(
            h.conversation(a.child).unwrap().tool_filters.deferred,
            vec!["after"]
        );
    }

    #[test]
    fn published_failure_fences_owned_conversation_admission_before_settlement() {
        use crate::task_scheduler::{Registry, Step, TickReservation};
        let dir = tempfile::tempdir().unwrap();
        let mut harness = Harness::open_paused(dir.path().join("db"), options()).unwrap();
        let conversation = harness.create_conversation(config()).unwrap();
        let mut registry = Registry::default();
        registry.register("fail", false, |_: &Task| Ok(Step::Fail("failure".into())));
        let mut scheduler = harness.attach_tasks(registry).unwrap();
        let root = Task::new(
            Uuid::from_u128(1),
            TaskOwner::Conversation(conversation),
            Checkpoint {
                phase: "start".into(),
                data: serde_json::Value::Null,
            },
            1,
        )
        .unwrap()
        .registered("fail".into(), false);
        scheduler.submit(&root).unwrap();
        let background = harness
            .ensure_task_owned_subagent(
                root.id(),
                root.owner().clone(),
                "background",
                config(),
                Mode::Background,
            )
            .unwrap();
        let child = Task::new(
            Uuid::from_u128(2),
            TaskOwner::Task(root.id()),
            Checkpoint {
                phase: "start".into(),
                data: serde_json::Value::Null,
            },
            1,
        )
        .unwrap();
        let separate = Task::new(
            Uuid::from_u128(3),
            TaskOwner::Conversation(background.child),
            Checkpoint {
                phase: "start".into(),
                data: serde_json::Value::Null,
            },
            1,
        )
        .unwrap();
        TaskStore::new(harness.subagent_store().unwrap())
            .save(&[Save::Create(&child), Save::Create(&separate)])
            .unwrap();
        let existing = harness
            .submit_task_owned_subagent(
                child.id(),
                child.owner().clone(),
                "existing",
                config(),
                Mode::Foreground,
                "work".into(),
            )
            .unwrap();
        let TickReservation::Invocation(invocation) = scheduler.reserve_tick(0).unwrap() else {
            panic!("expected root invocation");
        };
        let completed = invocation.execute().unwrap();
        assert_eq!(
            TaskStore::new(harness.subagent_store().unwrap())
                .load(root.id())
                .unwrap()
                .unwrap()
                .task
                .state(),
            &TaskState::Running
        );
        assert!(matches!(
            harness.submit_task_owned_subagent(
                child.id(),
                child.owner().clone(),
                "existing",
                config(),
                Mode::Foreground,
                "work".into()
            ),
            Err(Error::TaskPublicationBusy)
        ));
        for mode in [Mode::Foreground, Mode::Background] {
            assert!(matches!(
                harness.ensure_task_owned_subagent(
                    child.id(),
                    child.owner().clone(),
                    "blocked",
                    config(),
                    mode
                ),
                Err(Error::TaskPublicationBusy)
            ));
            assert!(matches!(
                harness.submit_task_owned_subagent(
                    child.id(),
                    child.owner().clone(),
                    "blocked",
                    config(),
                    mode,
                    "work".into()
                ),
                Err(Error::TaskPublicationBusy)
            ));
        }
        assert!(matches!(
            harness.submit_task_owned_subagent_notified(
                child.id(),
                child.owner().clone(),
                "blocked",
                config(),
                "work".into(),
                None,
                "done".into()
            ),
            Err(Error::TaskPublicationBusy)
        ));
        assert!(harness.owner.task_gate().try_lock().is_ok());
        assert!(
            EdgeStore::new(harness.subagent_store().unwrap())
                .children(child.id())
                .unwrap()
                .iter()
                .all(|edge| edge.edge.child == existing.child)
        );
        let permitted = harness
            .submit_task_owned_subagent(
                separate.id(),
                separate.owner().clone(),
                "independent",
                config(),
                Mode::Foreground,
                "work".into(),
            )
            .unwrap();
        assert_eq!(
            harness
                .conversation(permitted.child)
                .unwrap()
                .submissions
                .len(),
            1
        );
        scheduler.settle_invocation(completed).unwrap();
        assert!(
            harness
                .submit_task_owned_subagent(
                    child.id(),
                    child.owner().clone(),
                    "blocked",
                    config(),
                    Mode::Foreground,
                    "work".into()
                )
                .is_err()
        );
    }

    #[test]
    fn published_handler_error_and_panic_also_fence_owned_admission() {
        use crate::task_scheduler::{Registry, TickReservation};
        for panic_handler in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let mut harness = Harness::open_paused(dir.path().join("db"), options()).unwrap();
            let conversation = harness.create_conversation(config()).unwrap();
            let mut registry = Registry::default();
            registry.register("failure", false, move |_: &Task| {
                if panic_handler {
                    panic!("handler panic");
                }
                Err("handler failure".into())
            });
            let mut scheduler = harness.attach_tasks(registry).unwrap();
            let root = Task::new(
                Uuid::from_u128(1),
                TaskOwner::Conversation(conversation),
                Checkpoint {
                    phase: "start".into(),
                    data: serde_json::Value::Null,
                },
                1,
            )
            .unwrap()
            .registered("failure".into(), false);
            scheduler.submit(&root).unwrap();
            let child = Task::new(
                Uuid::from_u128(2),
                TaskOwner::Task(root.id()),
                Checkpoint {
                    phase: "start".into(),
                    data: serde_json::Value::Null,
                },
                1,
            )
            .unwrap();
            TaskStore::new(harness.subagent_store().unwrap())
                .save(&[Save::Create(&child)])
                .unwrap();
            let TickReservation::Invocation(invocation) = scheduler.reserve_tick(0).unwrap() else {
                panic!("expected invocation");
            };
            let completed =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| invocation.execute()));
            assert_eq!(completed.is_err(), panic_handler);
            assert_eq!(
                TaskStore::new(harness.subagent_store().unwrap())
                    .load(root.id())
                    .unwrap()
                    .unwrap()
                    .task
                    .state(),
                &TaskState::Running
            );
            assert!(matches!(
                harness.submit_task_owned_subagent(
                    child.id(),
                    child.owner().clone(),
                    "blocked",
                    config(),
                    Mode::Foreground,
                    "work".into()
                ),
                Err(Error::TaskPublicationBusy)
            ));
            assert!(
                EdgeStore::new(harness.subagent_store().unwrap())
                    .children(child.id())
                    .unwrap()
                    .is_empty()
            );
            drop(completed);
        }
    }

    #[test]
    fn publication_busy_retry_does_not_hold_task_gate_or_reserve_child() {
        use crate::task_scheduler::Registry;
        let dir = tempfile::tempdir().unwrap();
        let mut harness = Harness::open_paused(dir.path().join("db"), options()).unwrap();
        let (task, owner) = setup(&mut harness);
        let scheduler = harness.attach_tasks(Registry::default()).unwrap();
        let publication = scheduler.failure_publication();
        publication.test_set_published(task, true);
        assert!(matches!(
            harness.submit_task_owned_subagent_notified(
                task,
                owner.clone(),
                "job",
                config(),
                "work".into(),
                None,
                "done".into()
            ),
            Err(Error::TaskPublicationBusy)
        ));
        assert!(harness.owner.task_gate().try_lock().is_ok());
        assert!(
            EdgeStore::new(harness.subagent_store().unwrap())
                .children(task)
                .unwrap()
                .is_empty()
        );
        publication.test_set_published(task, false);
        let admission = harness
            .submit_task_owned_subagent_notified(
                task,
                owner.clone(),
                "job",
                config(),
                "work".into(),
                None,
                "done".into(),
            )
            .unwrap();
        assert_eq!(
            harness
                .submit_task_owned_subagent_notified(
                    task,
                    owner,
                    "job",
                    config(),
                    "work".into(),
                    None,
                    "done".into()
                )
                .unwrap(),
            admission
        );
    }

    #[test]
    fn admission_during_handler_keeps_non_replayable_result_and_staged_effects() {
        use crate::document::{Address, Documents, TaskDocuments};
        use crate::task::{TaskState, WaitFor};
        use crate::task_scheduler::{Handler, Registry, Step};
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct Parent {
            harness: std::sync::Mutex<Harness>,
            calls: Arc<AtomicUsize>,
        }
        impl Handler for Parent {
            fn run(&self, _: &Task) -> Result<Step, String> {
                unreachable!()
            }
            fn run_with_documents(
                &self,
                task: &Task,
                docs: &mut TaskDocuments<'_>,
            ) -> Result<Step, String> {
                self.calls.fetch_add(1, Ordering::SeqCst);
                let address = Address {
                    scope: "race".into(),
                    name: "doc".into(),
                    key: "".into(),
                };
                docs.acquire_owned(&address).map_err(|e| e.to_string())?;
                docs.put(
                    &address,
                    serde_json::json!({"saved": true})
                        .as_object()
                        .unwrap()
                        .clone(),
                )
                .map_err(|e| e.to_string())?;
                let child = Task::new(
                    Uuid::from_u128(77),
                    TaskOwner::Task(task.id()),
                    Checkpoint {
                        phase: "child".into(),
                        data: serde_json::Value::Null,
                    },
                    1,
                )
                .unwrap()
                .registered("child".into(), true);
                let id = docs.stage_keyed_child("key", "tool", child.input(), child.clone())?;
                self.harness
                    .lock()
                    .unwrap()
                    .ensure_task_owned_subagent(
                        task.id(),
                        task.owner().clone(),
                        "concurrent",
                        config(),
                        Mode::Background,
                    )
                    .map_err(|e| e.to_string())?;
                Ok(Step::Wait {
                    checkpoint: Checkpoint {
                        phase: "waiting".into(),
                        data: serde_json::Value::Null,
                    },
                    condition: WaitFor::TasksAny(vec![id]),
                    children: vec![],
                })
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let mut harness = Harness::open_paused(dir.path().join("db"), options()).unwrap();
        let conversation = harness.create_conversation(config()).unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let mut registry = Registry::default();
        registry.register(
            "parent",
            false,
            Parent {
                harness: std::sync::Mutex::new(harness.worker()),
                calls: calls.clone(),
            },
        );
        registry.register("child", true, |_: &Task| {
            Ok(Step::Succeed(serde_json::Value::Null))
        });
        let mut scheduler =
            crate::task_scheduler::Scheduler::attach(harness.owner.clone(), registry).unwrap();
        let task = Task::new(
            Uuid::from_u128(76),
            TaskOwner::Conversation(conversation),
            Checkpoint {
                phase: "start".into(),
                data: serde_json::Value::Null,
            },
            1,
        )
        .unwrap()
        .registered("parent".into(), false);
        scheduler.submit(&task).unwrap();
        assert!(scheduler.tick(0).unwrap());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            scheduler.get(task.id()).unwrap().unwrap().state(),
            &TaskState::Waiting(WaitFor::TasksAny(vec![Uuid::from_u128(77)]))
        );
        assert!(scheduler.get(Uuid::from_u128(77)).unwrap().is_some());
        let address = Address {
            scope: "race".into(),
            name: "doc".into(),
            key: "".into(),
        };
        assert_eq!(
            Documents::new(harness.owner.store())
                .get(&address)
                .unwrap()
                .unwrap()
                .value["saved"],
            true
        );
        assert_eq!(
            harness
                .owner
                .store()
                .task_document_owner(&address.storage_key().unwrap())
                .unwrap(),
            Some(task.id())
        );
    }

    #[test]
    fn admission_followed_by_changed_task_does_not_commit_handler_effects() {
        use crate::document::{Address, Documents, TaskDocuments};
        use crate::task::TaskState;
        use crate::task_scheduler::{Handler, Registry, Step};
        struct Parent {
            harness: std::sync::Mutex<Harness>,
            abort: bool,
        }
        impl Handler for Parent {
            fn run(&self, _: &Task) -> Result<Step, String> {
                unreachable!()
            }
            fn run_with_documents(
                &self,
                task: &Task,
                docs: &mut TaskDocuments<'_>,
            ) -> Result<Step, String> {
                let address = Address {
                    scope: "race".into(),
                    name: "discard".into(),
                    key: "".into(),
                };
                docs.acquire_owned(&address).map_err(|e| e.to_string())?;
                docs.put(
                    &address,
                    serde_json::json!({"saved": true})
                        .as_object()
                        .unwrap()
                        .clone(),
                )
                .map_err(|e| e.to_string())?;
                let mut harness = self.harness.lock().unwrap();
                harness
                    .ensure_task_owned_subagent(
                        task.id(),
                        task.owner().clone(),
                        "concurrent",
                        config(),
                        Mode::Background,
                    )
                    .map_err(|e| e.to_string())?;
                let ts = TaskStore::new(harness.owner.store());
                let snapshot = ts.load(task.id()).unwrap().unwrap();
                ts.update(&snapshot, |task| {
                    if self.abort {
                        task.request_abort()
                    } else {
                        task.advance(Checkpoint {
                            phase: "external".into(),
                            data: serde_json::Value::Null,
                        })
                    }
                })
                .unwrap();
                Ok(Step::Succeed(serde_json::json!("result")))
            }
        }
        for abort in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let mut harness = Harness::open_paused(dir.path().join("db"), options()).unwrap();
            let conversation = harness.create_conversation(config()).unwrap();
            let mut registry = Registry::default();
            registry.register(
                "parent",
                false,
                Parent {
                    harness: std::sync::Mutex::new(harness.worker()),
                    abort,
                },
            );
            let mut scheduler =
                crate::task_scheduler::Scheduler::attach(harness.owner.clone(), registry).unwrap();
            let task = Task::new(
                Uuid::from_u128(78),
                TaskOwner::Conversation(conversation),
                Checkpoint {
                    phase: "start".into(),
                    data: serde_json::Value::Null,
                },
                1,
            )
            .unwrap()
            .registered("parent".into(), false);
            scheduler.submit(&task).unwrap();
            let result = scheduler.tick(0);
            if abort {
                assert!(result.unwrap());
            } else {
                assert!(result.is_err());
            }
            let saved = TaskStore::new(harness.owner.store())
                .load(task.id())
                .unwrap()
                .unwrap()
                .task;
            assert_eq!(saved.abort_requested(), abort);
            assert_eq!(
                saved.state(),
                if abort {
                    &TaskState::Ready
                } else {
                    &TaskState::Running
                }
            );
            if !abort {
                assert_eq!(saved.checkpoint().phase, "external");
            }
            let address = Address {
                scope: "race".into(),
                name: "discard".into(),
                key: "".into(),
            };
            assert!(
                Documents::new(harness.owner.store())
                    .get(&address)
                    .unwrap()
                    .is_none()
            );
            assert_eq!(
                harness
                    .owner
                    .store()
                    .task_document_owner(&address.storage_key().unwrap())
                    .unwrap(),
                None
            );
        }
    }

    #[test]
    fn self_signals_survive_multiple_identical_admissions() {
        use crate::document::TaskDocuments;
        use crate::task_scheduler::{Handler, Registry, Step};
        use crate::task_store::{SignalResult, SignalToken};
        use std::sync::atomic::{AtomicUsize, Ordering};
        struct Parent {
            harness: std::sync::Mutex<Harness>,
            calls: Arc<AtomicUsize>,
        }
        impl Handler for Parent {
            fn run(&self, _: &Task) -> Result<Step, String> {
                unreachable!()
            }
            fn run_with_documents(
                &self,
                task: &Task,
                docs: &mut TaskDocuments<'_>,
            ) -> Result<Step, String> {
                self.calls.fetch_add(1, Ordering::SeqCst);
                let token = SignalToken::new("answer", 1).unwrap();
                for (index, name) in ["a", "b", "c"].into_iter().enumerate() {
                    assert_eq!(
                        docs.signal_at(task.id(), &token, name, serde_json::json!(index), 0)
                            .unwrap(),
                        SignalResult::Delivered
                    );
                    self.harness
                        .lock()
                        .unwrap()
                        .ensure_task_owned_subagent(
                            task.id(),
                            task.owner().clone(),
                            name,
                            config(),
                            Mode::Background,
                        )
                        .map_err(|e| e.to_string())?;
                }
                Ok(Step::Continue(Checkpoint {
                    phase: "saved".into(),
                    data: serde_json::Value::Null,
                }))
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let mut harness = Harness::open_paused(dir.path().join("db"), options()).unwrap();
        let conversation = harness.create_conversation(config()).unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let mut registry = Registry::default();
        registry.register(
            "parent",
            false,
            Parent {
                harness: std::sync::Mutex::new(harness.worker()),
                calls: calls.clone(),
            },
        );
        let mut scheduler =
            crate::task_scheduler::Scheduler::attach(harness.owner.clone(), registry).unwrap();
        let task = Task::new(
            Uuid::from_u128(83),
            TaskOwner::Conversation(conversation),
            Checkpoint {
                phase: "start".into(),
                data: serde_json::Value::Null,
            },
            1,
        )
        .unwrap()
        .registered("parent".into(), false);
        scheduler.submit(&task).unwrap();
        assert!(scheduler.tick(0).unwrap());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            scheduler
                .get(task.id())
                .unwrap()
                .unwrap()
                .checkpoint()
                .phase,
            "saved"
        );
        let ts = TaskStore::new(harness.owner.store());
        let token = SignalToken::new("answer", 1).unwrap();
        for (index, name) in ["a", "b", "c"].into_iter().enumerate() {
            assert_eq!(
                ts.read_typed_signal(task.id(), &token, name).unwrap(),
                Some(serde_json::json!(index))
            );
        }
    }

    #[test]
    fn keyed_child_parent_reload_retries_only_identical_running_task() {
        use crate::document::TaskDocuments;
        use crate::task::{TaskState, WaitFor};
        use crate::task_scheduler::{Handler, Registry, Step};
        struct Parent {
            harness: std::sync::Mutex<Harness>,
            change: u8,
        }
        impl Handler for Parent {
            fn run(&self, _: &Task) -> Result<Step, String> {
                unreachable!()
            }
            fn run_with_documents(
                &self,
                task: &Task,
                docs: &mut TaskDocuments<'_>,
            ) -> Result<Step, String> {
                let child = Task::new(
                    Uuid::from_u128(85),
                    TaskOwner::Task(task.id()),
                    Checkpoint {
                        phase: "child".into(),
                        data: serde_json::Value::Null,
                    },
                    1,
                )
                .unwrap()
                .registered("child".into(), true);
                let mut injected = false;
                let result = docs.stage_keyed_child_after_load(
                    "key",
                    "tool",
                    child.input(),
                    child.clone(),
                    || {
                        if injected {
                            return;
                        }
                        injected = true;
                        let mut h = self.harness.lock().unwrap();
                        h.ensure_task_owned_subagent(
                            task.id(),
                            task.owner().clone(),
                            "race",
                            config(),
                            Mode::Background,
                        )
                        .unwrap();
                        if self.change != 0 {
                            let ts = TaskStore::new(h.owner.store());
                            let parent = ts.load(task.id()).unwrap().unwrap();
                            ts.update(&parent, |t| {
                                if self.change == 1 {
                                    t.request_abort()
                                } else {
                                    t.advance(Checkpoint {
                                        phase: "changed".into(),
                                        data: serde_json::Value::Null,
                                    })
                                }
                            })
                            .unwrap();
                        }
                    },
                );
                if self.change != 0 {
                    assert!(result.is_err());
                    return Ok(Step::Succeed(serde_json::json!("discard")));
                }
                let id = result?;
                Ok(Step::Wait {
                    checkpoint: Checkpoint {
                        phase: "waiting".into(),
                        data: serde_json::Value::Null,
                    },
                    condition: WaitFor::TasksAny(vec![id]),
                    children: vec![],
                })
            }
        }
        for change in 0..=2 {
            let dir = tempfile::tempdir().unwrap();
            let mut harness = Harness::open_paused(dir.path().join("db"), options()).unwrap();
            let conversation = harness.create_conversation(config()).unwrap();
            let mut registry = Registry::default();
            registry.register(
                "parent",
                false,
                Parent {
                    harness: std::sync::Mutex::new(harness.worker()),
                    change,
                },
            );
            registry.register("child", true, |_: &Task| {
                Ok(Step::Succeed(serde_json::Value::Null))
            });
            let mut scheduler =
                crate::task_scheduler::Scheduler::attach(harness.owner.clone(), registry).unwrap();
            let task = Task::new(
                Uuid::from_u128(84),
                TaskOwner::Conversation(conversation),
                Checkpoint {
                    phase: "start".into(),
                    data: serde_json::Value::Null,
                },
                1,
            )
            .unwrap()
            .registered("parent".into(), false);
            scheduler.submit(&task).unwrap();
            let result = scheduler.tick(0);
            if change == 2 {
                assert!(result.is_err());
            } else {
                assert!(result.unwrap());
            }
            let saved = TaskStore::new(harness.owner.store())
                .load(task.id())
                .unwrap()
                .unwrap()
                .task;
            match change {
                0 => {
                    assert_eq!(
                        saved.state(),
                        &TaskState::Waiting(WaitFor::TasksAny(vec![Uuid::from_u128(85)]))
                    );
                    assert!(scheduler.get(Uuid::from_u128(85)).unwrap().is_some());
                }
                1 => {
                    assert!(saved.abort_requested());
                    assert_eq!(saved.state(), &TaskState::Ready);
                    assert!(scheduler.get(Uuid::from_u128(85)).unwrap().is_none());
                }
                _ => {
                    assert_eq!(saved.checkpoint().phase, "changed");
                    assert!(
                        TaskStore::new(harness.owner.store())
                            .load(Uuid::from_u128(85))
                            .unwrap()
                            .is_none()
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn admission_during_handler_does_not_stop_the_driver() {
        use crate::task::TaskState;
        use crate::task_scheduler::{Registry, Step};
        use pi_rs_core::CancellationToken;
        use std::time::Duration;
        let dir = tempfile::tempdir().unwrap();
        let mut harness = Harness::open_paused(dir.path().join("db"), options()).unwrap();
        let conversation = harness.create_conversation(config()).unwrap();
        let concurrent = std::sync::Mutex::new(harness.worker());
        let mut registry = Registry::default();
        registry.register("parent", false, move |task: &Task| {
            concurrent
                .lock()
                .unwrap()
                .ensure_task_owned_subagent(
                    task.id(),
                    task.owner().clone(),
                    "concurrent",
                    config(),
                    Mode::Background,
                )
                .map_err(|e| e.to_string())?;
            Ok(Step::Succeed(serde_json::json!("result")))
        });
        registry.register("next", true, |_: &Task| {
            Ok(Step::Succeed(serde_json::json!("next")))
        });
        let scheduler =
            crate::task_scheduler::Scheduler::attach(harness.owner.clone(), registry).unwrap();
        let driver = crate::task_driver::Driver::start(scheduler);
        let task = |id, kind: &str, safe| {
            Task::new(
                Uuid::from_u128(id),
                TaskOwner::Conversation(conversation),
                Checkpoint {
                    phase: "start".into(),
                    data: serde_json::Value::Null,
                },
                1,
            )
            .unwrap()
            .registered(kind.into(), safe)
        };
        let first = task(81, "parent", false);
        driver.submit(&first).unwrap();
        let result = tokio::time::timeout(
            Duration::from_secs(10),
            driver.wait_for_task(first.id(), &CancellationToken::new()),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            result.state(),
            &TaskState::Succeeded(serde_json::json!("result"))
        );
        let second = task(82, "next", true);
        driver.submit(&second).unwrap();
        let result = tokio::time::timeout(
            Duration::from_secs(10),
            driver.wait_for_task(second.id(), &CancellationToken::new()),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            result.state(),
            &TaskState::Succeeded(serde_json::json!("next"))
        );
        driver.shutdown().unwrap();
    }
    #[tokio::test]
    async fn completing_owner_waits_for_active_owned_conversation() {
        use crate::task::TaskState;
        use crate::task_scheduler::{Registry, Scheduler, Step};
        use futures_util::{StreamExt, stream};
        use pi_rs_core::ModelEvent;

        struct Answer;
        #[async_trait]
        impl Model for Answer {
            async fn stream(&self, _: ModelRequest) -> Result<ModelStream, ModelError> {
                Ok(stream::iter(vec![
                    Ok(ModelEvent::Text("done".into())),
                    Ok(ModelEvent::Complete {
                        calls: vec![],
                        usage: None,
                    }),
                ])
                .boxed())
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let mut opts = options();
        opts.model = Arc::new(Answer);
        let mut h = Harness::open_paused(dir.path().join("db"), opts).unwrap();
        let conversation = h.create_conversation(config()).unwrap();
        let owner = TaskOwner::Conversation(conversation);
        let root = Task::new(
            Uuid::from_u128(1),
            owner.clone(),
            Checkpoint {
                phase: "start".into(),
                data: serde_json::Value::Null,
            },
            3,
        )
        .unwrap()
        .registered("root".into(), true);
        TaskStore::new(h.owner.store())
            .save(&[Save::Create(&root)])
            .unwrap();
        let child = h
            .ensure_task_owned_subagent(root.id(), owner, "fg", config(), Mode::Foreground)
            .unwrap();
        h.submit(child.child, None, "hello".into()).unwrap();
        let mut registry = Registry::default();
        registry.register("root", true, |_: &Task| {
            Ok(Step::Succeed(serde_json::json!("root")))
        });
        let mut scheduler = Scheduler::attach(h.owner.clone(), registry).unwrap();
        scheduler.tick(0).unwrap();
        assert_eq!(
            scheduler.get(root.id()).unwrap().unwrap().state(),
            &TaskState::Completing(serde_json::json!("root"))
        );
        assert!(!scheduler.tick(0).unwrap());
        h.resume_conversation(child.child, &pi_rs_core::CancellationToken::new())
            .await
            .unwrap();
        scheduler.tick(0).unwrap();
        assert_eq!(
            scheduler.get(root.id()).unwrap().unwrap().state(),
            &TaskState::Succeeded(serde_json::json!("root"))
        );
    }

    #[tokio::test]
    async fn failed_owner_aborts_active_owned_conversation_before_finalizing() {
        use crate::task::TaskState;
        use crate::task_scheduler::{Registry, Step};

        let dir = tempfile::tempdir().unwrap();
        let mut h = Harness::open_paused(dir.path().join("db"), options()).unwrap();
        let conversation = h.create_conversation(config()).unwrap();
        let owner = TaskOwner::Conversation(conversation);
        let root = Task::new(
            Uuid::from_u128(1),
            owner.clone(),
            Checkpoint {
                phase: "start".into(),
                data: serde_json::Value::Null,
            },
            3,
        )
        .unwrap()
        .registered("root".into(), true);
        TaskStore::new(h.owner.store())
            .save(&[Save::Create(&root)])
            .unwrap();
        let child = h
            .ensure_task_owned_subagent(root.id(), owner, "fg", config(), Mode::Foreground)
            .unwrap();
        h.submit(child.child, None, "hello".into()).unwrap();
        let mut registry = Registry::default();
        registry.register("root", true, |_: &Task| Ok(Step::Fail("failure".into())));
        let mut scheduler = h.attach_tasks(registry).unwrap();
        scheduler.tick(0).unwrap();
        assert_eq!(
            scheduler.get(root.id()).unwrap().unwrap().state(),
            &TaskState::CompletingFailed("failure".into())
        );
        scheduler.tick(0).unwrap();
        assert!(h.conversation(child.child).unwrap().abort_requested);
        assert!(!scheduler.tick(0).unwrap());
        h.resume_conversation(child.child, &pi_rs_core::CancellationToken::new())
            .await
            .unwrap();
        scheduler.tick(0).unwrap();
        assert_eq!(
            scheduler.get(root.id()).unwrap().unwrap().state(),
            &TaskState::Failed("failure".into())
        );
    }

    struct FailingModel;
    #[async_trait]
    impl Model for FailingModel {
        async fn stream(&self, _: ModelRequest) -> Result<ModelStream, ModelError> {
            Err(ModelError::Model("failed".into()))
        }
    }

    async fn parked_owned_inputs(h: &mut Harness, settle: bool) -> (Uuid, Uuid, Uuid, Uuid) {
        use crate::runtime::{AdmissionOptions, OnAbort, Status};
        let parent = h.create_conversation(config()).unwrap();
        let owner = TaskOwner::Conversation(parent);
        let root = Task::new(
            Uuid::new_v4(),
            owner.clone(),
            Checkpoint {
                phase: "start".into(),
                data: serde_json::Value::Null,
            },
            3,
        )
        .unwrap()
        .registered("parked-root".into(), true);
        TaskStore::new(h.owner.store())
            .save(&[Save::Create(&root)])
            .unwrap();
        let child = h
            .ensure_task_owned_subagent(root.id(), owner, "fg", config(), Mode::Foreground)
            .unwrap()
            .child;
        h.submit(child, None, "first".into()).unwrap();
        let ordinary = h.submit(child, None, "ordinary".into()).unwrap();
        let kept = h
            .submit_with_options(
                child,
                None,
                "kept".into(),
                AdmissionOptions {
                    on_abort: OnAbort::Keep,
                    ..Default::default()
                },
            )
            .unwrap();
        if settle {
            h.resume_conversation(child, &pi_rs_core::CancellationToken::new())
                .await
                .unwrap();
        }
        let c = h.conversation(child).unwrap();
        assert_eq!(
            c.submissions
                .iter()
                .find(|s| s.id == ordinary)
                .unwrap()
                .status,
            Status::Queued
        );
        assert_eq!(
            c.submissions.iter().find(|s| s.id == kept).unwrap().status,
            Status::Queued
        );
        (root.id(), child, ordinary, kept)
    }

    #[tokio::test]
    async fn parked_owned_inputs_do_not_hold_success_across_reopen() {
        use crate::runtime::Status;
        use crate::task::TaskState;
        use crate::task_scheduler::{Registry, Step};
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut opts = options();
        opts.model = Arc::new(FailingModel);
        let mut h = Harness::open_paused(&path, opts.clone()).unwrap();
        let (root, child, ordinary, kept) = parked_owned_inputs(&mut h, false).await;
        let mut registry = Registry::default();
        registry.register("parked-root", true, |_: &Task| {
            Ok(Step::Succeed(serde_json::json!("done")))
        });
        let mut scheduler = h.attach_tasks(registry.clone()).unwrap();
        scheduler.tick(0).unwrap();
        assert_eq!(
            scheduler.get(root).unwrap().unwrap().state(),
            &TaskState::Completing(serde_json::json!("done"))
        );
        h.resume_conversation(child, &pi_rs_core::CancellationToken::new())
            .await
            .unwrap();
        drop(scheduler);
        drop(h);
        let h = Harness::open_paused(&path, opts).unwrap();
        let mut scheduler = h.attach_tasks(registry).unwrap();
        scheduler.tick(0).unwrap();
        assert_eq!(
            scheduler.get(root).unwrap().unwrap().state(),
            &TaskState::Succeeded(serde_json::json!("done"))
        );
        let c = h.conversation(child).unwrap();
        for id in [ordinary, kept] {
            assert_eq!(
                c.submissions.iter().find(|s| s.id == id).unwrap().status,
                Status::Queued
            );
        }
    }

    #[tokio::test]
    async fn failed_owner_cascades_parked_inputs_and_drains_kept_after_reopen() {
        use crate::runtime::Status;
        use crate::task::TaskState;
        use crate::task_scheduler::{Registry, Step};
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut opts = options();
        opts.model = Arc::new(FailingModel);
        let mut h = Harness::open_paused(&path, opts.clone()).unwrap();
        let (root, child, ordinary, kept) = parked_owned_inputs(&mut h, true).await;
        let mut registry = Registry::default();
        registry.register("parked-root", true, |_: &Task| {
            Ok(Step::Fail("failure".into()))
        });
        let mut scheduler = h.attach_tasks(registry.clone()).unwrap();
        scheduler.tick(0).unwrap();
        assert_eq!(
            scheduler.get(root).unwrap().unwrap().state(),
            &TaskState::CompletingFailed("failure".into())
        );
        drop(scheduler);
        drop(h);
        let h = Harness::open_paused(&path, opts.clone()).unwrap();
        let mut scheduler = h.attach_tasks(registry.clone()).unwrap();
        scheduler.tick(0).unwrap();
        let c = h.conversation(child).unwrap();
        assert!(c.abort_requested);
        assert_eq!(
            c.submissions
                .iter()
                .find(|s| s.id == ordinary)
                .unwrap()
                .status,
            Status::Unanswered {
                reason: "aborted".into()
            }
        );
        assert_eq!(
            c.submissions.iter().find(|s| s.id == kept).unwrap().status,
            Status::Queued
        );
        drop(scheduler);
        drop(h);
        let mut h = Harness::open_paused(&path, opts).unwrap();
        h.resume_conversation(child, &pi_rs_core::CancellationToken::new())
            .await
            .unwrap();
        let mut scheduler = h.attach_tasks(registry).unwrap();
        scheduler.tick(0).unwrap();
        assert_eq!(
            scheduler.get(root).unwrap().unwrap().state(),
            &TaskState::Failed("failure".into())
        );
        assert_eq!(
            h.conversation(child)
                .unwrap()
                .submissions
                .iter()
                .find(|s| s.id == kept)
                .unwrap()
                .status,
            Status::Queued
        );
    }

    #[tokio::test]
    async fn deferred_failure_maintenance_rechecks_parked_abortable_inputs() {
        use crate::runtime::{Status, WithdrawResult};
        use crate::task::TaskState;
        use crate::task_scheduler::{Registry, Step, TickReservation};
        for withdrawn_before_retry in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let mut opts = options();
            opts.model = Arc::new(FailingModel);
            let mut h = Harness::open_paused(dir.path().join("db"), opts).unwrap();
            let (root, child, ordinary, kept) = parked_owned_inputs(&mut h, true).await;
            let mut registry = Registry::default();
            registry.register("parked-root", true, |_: &Task| {
                Ok(Step::Fail("failure".into()))
            });
            let mut scheduler = h.attach_tasks(registry).unwrap();
            let TickReservation::Invocation(invocation) = scheduler.try_reserve_tick(0).unwrap()
            else {
                panic!("expected detached task invocation");
            };
            scheduler
                .try_settle_invocation(invocation.execute().unwrap())
                .unwrap();
            assert_eq!(
                scheduler.get(root).unwrap().unwrap().state(),
                &TaskState::CompletingFailed("failure".into())
            );

            let owner = h.owner.clone();
            let control = owner.conversation_controls().get(child);
            let poll = control.gate.lock().unwrap();
            let TickReservation::Maintenance(pending) = scheduler.try_reserve_tick(0).unwrap()
            else {
                panic!("expected deferred failure maintenance");
            };
            let mut worker = h.worker();
            assert!(!pending.try_cleanup(&mut worker).unwrap());
            drop(poll);
            if withdrawn_before_retry {
                assert!(matches!(
                    h.withdraw_submission(child, ordinary).unwrap(),
                    WithdrawResult::Aborted
                ));
            }
            assert!(pending.try_cleanup(&mut worker).unwrap());
            let c = h.conversation(child).unwrap();
            assert_eq!(c.abort_requested, !withdrawn_before_retry);
            assert_eq!(
                c.submissions
                    .iter()
                    .find(|s| s.id == ordinary)
                    .unwrap()
                    .status,
                Status::Unanswered {
                    reason: "aborted".into()
                }
            );
            assert_eq!(
                c.submissions.iter().find(|s| s.id == kept).unwrap().status,
                Status::Queued
            );
            if !withdrawn_before_retry {
                h.resume_conversation(child, &pi_rs_core::CancellationToken::new())
                    .await
                    .unwrap();
            }
            scheduler.tick(0).unwrap();
            assert_eq!(
                scheduler.get(root).unwrap().unwrap().state(),
                &TaskState::Failed("failure".into())
            );
        }
    }

    #[tokio::test]
    async fn explicit_owner_abort_with_parked_inputs_survives_reopen() {
        use crate::runtime::Status;
        use crate::task::TaskState;
        use crate::task_scheduler::{Registry, Step};
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut opts = options();
        opts.model = Arc::new(FailingModel);
        let mut h = Harness::open_paused(&path, opts.clone()).unwrap();
        let (root, child, ordinary, kept) = parked_owned_inputs(&mut h, true).await;
        h.request_abort_task(root).unwrap();
        drop(h);
        let mut h = Harness::open_paused(&path, opts).unwrap();
        let c = h.conversation(child).unwrap();
        assert_eq!(
            c.submissions
                .iter()
                .find(|s| s.id == ordinary)
                .unwrap()
                .status,
            Status::Unanswered {
                reason: "aborted".into()
            }
        );
        assert_eq!(
            c.submissions.iter().find(|s| s.id == kept).unwrap().status,
            Status::Queued
        );
        h.resume_conversation(child, &pi_rs_core::CancellationToken::new())
            .await
            .unwrap();
        let mut registry = Registry::default();
        registry.register("parked-root", true, |_: &Task| {
            Ok(Step::Succeed(serde_json::json!(null)))
        });
        registry.set_abort("parked-root", |_: &Task| {
            Ok(Step::Succeed(serde_json::json!(null)))
        });
        let mut scheduler = h.attach_tasks(registry).unwrap();
        for _ in 0..4 {
            if scheduler.get(root).unwrap().unwrap().is_terminal() {
                break;
            }
            scheduler.tick(0).unwrap();
        }
        assert_eq!(
            scheduler.get(root).unwrap().unwrap().state(),
            &TaskState::Cancelled
        );
        assert_eq!(
            h.conversation(child)
                .unwrap()
                .submissions
                .iter()
                .find(|s| s.id == kept)
                .unwrap()
                .status,
            Status::Queued
        );
    }

    #[tokio::test]
    async fn fail_fast_waits_for_sibling_owned_conversation_cleanup() {
        use crate::task::{TaskState, WaitFor};
        use crate::task_scheduler::{Registry, Step};
        let dir = tempfile::tempdir().unwrap();
        let mut h = Harness::open_paused(dir.path().join("db"), options()).unwrap();
        let conversation = h.create_conversation(config()).unwrap();
        let owner = TaskOwner::Conversation(conversation);
        let checkpoint = Checkpoint {
            phase: "start".into(),
            data: serde_json::Value::Null,
        };
        let mut parent = Task::new(Uuid::from_u128(1), owner.clone(), checkpoint.clone(), 3)
            .unwrap()
            .registered("parent".into(), true);
        parent.start().unwrap();
        parent
            .wait(WaitFor::TasksFailFast(vec![
                Uuid::from_u128(2),
                Uuid::from_u128(3),
            ]))
            .unwrap();
        let mut failed = Task::new(
            Uuid::from_u128(2),
            TaskOwner::Task(parent.id()),
            checkpoint.clone(),
            3,
        )
        .unwrap()
        .registered("child".into(), true);
        failed.start().unwrap();
        failed.fail("failure".into()).unwrap();
        let sibling = Task::new(
            Uuid::from_u128(3),
            TaskOwner::Task(parent.id()),
            checkpoint.clone(),
            3,
        )
        .unwrap()
        .registered("child".into(), true);
        let unrelated = Task::new(Uuid::from_u128(4), owner, checkpoint, 3)
            .unwrap()
            .registered("other".into(), true);
        TaskStore::new(h.owner.store())
            .save(&[
                Save::Create(&parent),
                Save::Create(&failed),
                Save::Create(&sibling),
                Save::Create(&unrelated),
            ])
            .unwrap();
        let owned = h
            .ensure_task_owned_subagent(
                sibling.id(),
                TaskOwner::Task(parent.id()),
                "fg",
                config(),
                Mode::Foreground,
            )
            .unwrap();
        h.submit(owned.child, None, "hello".into()).unwrap();
        let mut registry = Registry::default();
        registry.register("parent", true, |_: &Task| {
            Ok(Step::Succeed(serde_json::Value::Null))
        });
        registry.register("other", true, |_: &Task| {
            Ok(Step::Succeed(serde_json::Value::Null))
        });
        registry.register_with_abort(
            "child",
            true,
            |_: &Task| Ok(Step::Succeed(serde_json::Value::Null)),
            |_: &Task| Ok(Step::Succeed(serde_json::Value::Null)),
        );
        let mut scheduler = h.attach_tasks(registry).unwrap();
        scheduler.tick(0).unwrap();
        assert!(
            scheduler
                .get(sibling.id())
                .unwrap()
                .unwrap()
                .abort_requested()
        );
        assert!(h.conversation(owned.child).unwrap().abort_requested);
        assert!(matches!(
            scheduler.get(parent.id()).unwrap().unwrap().state(),
            TaskState::Waiting(_)
        ));
        assert!(scheduler.tick(0).unwrap());
        assert!(
            scheduler
                .get(unrelated.id())
                .unwrap()
                .unwrap()
                .is_terminal()
        );
        assert!(!scheduler.tick(0).unwrap());
        h.resume_conversation(owned.child, &pi_rs_core::CancellationToken::new())
            .await
            .unwrap();
        for _ in 0..8 {
            scheduler.tick(0).unwrap();
            if scheduler.get(parent.id()).unwrap().unwrap().is_terminal() {
                break;
            }
        }
        assert_eq!(
            scheduler.get(sibling.id()).unwrap().unwrap().state(),
            &TaskState::Cancelled
        );
        assert!(scheduler.get(parent.id()).unwrap().unwrap().is_terminal());
    }

    #[tokio::test]
    async fn fail_fast_reopen_defers_running_conversation_owner_until_graph_marked() {
        use crate::task::{TaskState, WaitFor};
        use crate::task_scheduler::{Registry, Step};
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut h = Harness::open_paused(&path, options()).unwrap();
        let conversation = h.create_conversation(config()).unwrap();
        let checkpoint = Checkpoint {
            phase: "start".into(),
            data: serde_json::Value::Null,
        };
        let mut parent = Task::new(
            Uuid::from_u128(1),
            TaskOwner::Conversation(conversation),
            checkpoint.clone(),
            3,
        )
        .unwrap()
        .registered("parent".into(), false);
        parent.start().unwrap();
        parent
            .wait(WaitFor::TasksFailFast(vec![
                Uuid::from_u128(2),
                Uuid::from_u128(3),
            ]))
            .unwrap();
        let mut failed = Task::new(
            Uuid::from_u128(2),
            TaskOwner::Task(parent.id()),
            checkpoint.clone(),
            3,
        )
        .unwrap()
        .registered("child".into(), false);
        failed.start().unwrap();
        failed.fail("failure".into()).unwrap();
        let mut sibling = Task::new(
            Uuid::from_u128(3),
            TaskOwner::Task(parent.id()),
            checkpoint,
            3,
        )
        .unwrap()
        .registered("child".into(), false);
        sibling.start().unwrap();
        TaskStore::new(h.owner.store())
            .save(&[
                Save::Create(&parent),
                Save::Create(&failed),
                Save::Create(&sibling),
            ])
            .unwrap();
        let owned = h
            .ensure_task_owned_subagent(
                sibling.id(),
                TaskOwner::Task(parent.id()),
                "fg",
                config(),
                Mode::Foreground,
            )
            .unwrap();
        h.submit(owned.child, None, "hello".into()).unwrap();
        drop(h);
        let mut h = Harness::open_paused(&path, options()).unwrap();
        let mut registry = Registry::default();
        registry.register("parent", false, |_: &Task| {
            Ok(Step::Succeed(serde_json::Value::Null))
        });
        registry.register_with_abort(
            "child",
            false,
            |_: &Task| panic!("must not replay"),
            |_: &Task| Ok(Step::Succeed(serde_json::Value::Null)),
        );
        let mut scheduler = h.attach_tasks(registry).unwrap();
        assert_eq!(
            scheduler.get(sibling.id()).unwrap().unwrap().state(),
            &TaskState::Running
        );
        scheduler.tick(0).unwrap();
        assert_eq!(
            scheduler.get(sibling.id()).unwrap().unwrap().state(),
            &TaskState::Running
        );
        assert!(
            scheduler
                .get(sibling.id())
                .unwrap()
                .unwrap()
                .abort_requested()
        );
        assert!(h.conversation(owned.child).unwrap().abort_requested);
        scheduler.tick(0).unwrap();
        assert!(!scheduler.get(sibling.id()).unwrap().unwrap().is_terminal());
        h.resume_conversation(owned.child, &pi_rs_core::CancellationToken::new())
            .await
            .unwrap();
        for _ in 0..8 {
            scheduler.tick(0).unwrap();
            if scheduler.get(parent.id()).unwrap().unwrap().is_terminal() {
                break;
            }
        }
        assert_eq!(
            scheduler.get(sibling.id()).unwrap().unwrap().state(),
            &TaskState::Cancelled
        );
        assert!(scheduler.get(parent.id()).unwrap().unwrap().is_terminal());
    }

    #[test]
    fn external_wait_rejects_cycle_through_owned_conversation() {
        use crate::task::WaitFor;
        use crate::task_scheduler::{Registry, Step};
        let dir = tempfile::tempdir().unwrap();
        let mut h = Harness::open_paused(dir.path().join("db"), options()).unwrap();
        let conversation = h.create_conversation(config()).unwrap();
        let owner = TaskOwner::Conversation(conversation);
        let checkpoint = Checkpoint {
            phase: "start".into(),
            data: serde_json::Value::Null,
        };
        let root = Task::new(Uuid::from_u128(2), owner.clone(), checkpoint.clone(), 3)
            .unwrap()
            .registered("root".into(), true);
        TaskStore::new(h.owner.store())
            .save(&[Save::Create(&root)])
            .unwrap();
        let child = h
            .ensure_task_owned_subagent(root.id(), owner, "fg", config(), Mode::Foreground)
            .unwrap();
        let waiter = Task::new(
            Uuid::from_u128(1),
            TaskOwner::Conversation(child.child),
            checkpoint,
            3,
        )
        .unwrap()
        .registered("waiter".into(), true);
        TaskStore::new(h.owner.store())
            .save(&[Save::Create(&waiter)])
            .unwrap();
        let mut registry = Registry::default();
        registry.register("root", true, |_: &Task| {
            Ok(Step::Succeed(serde_json::Value::Null))
        });
        registry.register("waiter", true, |_: &Task| {
            Ok(Step::Wait {
                checkpoint: Checkpoint {
                    phase: "waiting".into(),
                    data: serde_json::Value::Null,
                },
                condition: WaitFor::TasksAny(vec![Uuid::from_u128(2)]),
                children: vec![],
            })
        });
        let mut scheduler = h.attach_tasks(registry).unwrap();
        assert!(scheduler.tick(0).unwrap());
        assert!(
            matches!(scheduler.get(waiter.id()).unwrap().unwrap().state(), crate::task::TaskState::Failed(reason) if reason.contains("invalid task wait"))
        );
        assert!(scheduler.tick(0).unwrap());
        assert!(scheduler.get(root.id()).unwrap().unwrap().is_terminal());
    }

    #[test]
    fn external_wait_rejects_cycle_through_terminal_owner_and_conversation() {
        use crate::task::WaitFor;
        use crate::task_scheduler::{Registry, Step};
        let dir = tempfile::tempdir().unwrap();
        let mut h = Harness::open_paused(dir.path().join("db"), options()).unwrap();
        let conversation = h.create_conversation(config()).unwrap();
        let checkpoint = Checkpoint {
            phase: "start".into(),
            data: serde_json::Value::Null,
        };
        let root = Task::new(
            Uuid::from_u128(2),
            TaskOwner::Conversation(conversation),
            checkpoint.clone(),
            3,
        )
        .unwrap()
        .registered("root".into(), true);
        let middle = Task::new(
            Uuid::from_u128(3),
            TaskOwner::Task(root.id()),
            checkpoint.clone(),
            3,
        )
        .unwrap()
        .registered("middle".into(), true);
        let shared = h.owner.clone();
        let store = TaskStore::new(shared.store());
        store
            .save(&[Save::Create(&root), Save::Create(&middle)])
            .unwrap();
        let child = h
            .ensure_task_owned_subagent(
                middle.id(),
                TaskOwner::Task(root.id()),
                "fg",
                config(),
                Mode::Foreground,
            )
            .unwrap();
        let mut middle = store.load(middle.id()).unwrap().unwrap();
        middle.task.start().unwrap();
        middle.task.succeed(serde_json::Value::Null).unwrap();
        store.save(&[Save::Update(&middle)]).unwrap();
        let waiter = Task::new(
            Uuid::from_u128(1),
            TaskOwner::Conversation(child.child),
            checkpoint,
            3,
        )
        .unwrap()
        .registered("waiter".into(), true);
        store.save(&[Save::Create(&waiter)]).unwrap();
        let mut registry = Registry::default();
        registry.register("root", true, |_: &Task| {
            Ok(Step::Succeed(serde_json::Value::Null))
        });
        registry.register("middle", true, |_: &Task| {
            Ok(Step::Succeed(serde_json::Value::Null))
        });
        registry.register("waiter", true, |_: &Task| {
            Ok(Step::Wait {
                checkpoint: Checkpoint {
                    phase: "waiting".into(),
                    data: serde_json::Value::Null,
                },
                condition: WaitFor::TasksAny(vec![Uuid::from_u128(2)]),
                children: vec![],
            })
        });
        let mut scheduler = h.attach_tasks(registry).unwrap();
        assert!(scheduler.tick(0).unwrap());
        assert!(
            matches!(scheduler.get(waiter.id()).unwrap().unwrap().state(), crate::task::TaskState::Failed(reason) if reason.contains("invalid task wait"))
        );
        assert!(scheduler.tick(0).unwrap());
        assert!(scheduler.get(root.id()).unwrap().unwrap().is_terminal());
    }

    #[test]
    fn external_wait_on_terminal_owner_is_already_satisfied() {
        use crate::task::WaitFor;
        use crate::task_scheduler::{Registry, Step};
        let dir = tempfile::tempdir().unwrap();
        let mut h = Harness::open_paused(dir.path().join("db"), options()).unwrap();
        let conversation = h.create_conversation(config()).unwrap();
        let owner = TaskOwner::Conversation(conversation);
        let checkpoint = Checkpoint {
            phase: "start".into(),
            data: serde_json::Value::Null,
        };
        let root = Task::new(Uuid::from_u128(2), owner.clone(), checkpoint.clone(), 3)
            .unwrap()
            .registered("root".into(), true);
        let shared = h.owner.clone();
        let store = TaskStore::new(shared.store());
        store.save(&[Save::Create(&root)]).unwrap();
        let child = h
            .ensure_task_owned_subagent(root.id(), owner, "fg", config(), Mode::Foreground)
            .unwrap();
        let mut root = store.load(root.id()).unwrap().unwrap();
        root.task.start().unwrap();
        root.task.succeed(serde_json::Value::Null).unwrap();
        store.save(&[Save::Update(&root)]).unwrap();
        let waiter = Task::new(
            Uuid::from_u128(1),
            TaskOwner::Conversation(child.child),
            checkpoint,
            3,
        )
        .unwrap()
        .registered("waiter".into(), true);
        store.save(&[Save::Create(&waiter)]).unwrap();
        let mut registry = Registry::default();
        registry.register("waiter", true, |task: &Task| {
            if task.checkpoint().phase == "start" {
                Ok(Step::Wait {
                    checkpoint: Checkpoint {
                        phase: "woken".into(),
                        data: serde_json::Value::Null,
                    },
                    condition: WaitFor::TasksAny(vec![Uuid::from_u128(2)]),
                    children: vec![],
                })
            } else {
                Ok(Step::Succeed(serde_json::Value::Null))
            }
        });
        let mut scheduler = h.attach_tasks(registry).unwrap();
        assert!(scheduler.tick(0).unwrap());
        assert!(scheduler.tick(0).unwrap());
        assert!(scheduler.get(waiter.id()).unwrap().unwrap().is_terminal());
    }
    #[test]
    fn completing_owner_tracks_work_added_to_owned_conversation() {
        use crate::task::TaskState;
        use crate::task_scheduler::{Registry, Scheduler, Step};
        let dir = tempfile::tempdir().unwrap();
        let mut h = Harness::open_paused(dir.path().join("db"), options()).unwrap();
        let conversation = h.create_conversation(config()).unwrap();
        let owner = TaskOwner::Conversation(conversation);
        let root = Task::new(
            Uuid::from_u128(1),
            owner.clone(),
            Checkpoint {
                phase: "start".into(),
                data: serde_json::Value::Null,
            },
            3,
        )
        .unwrap()
        .registered("root".into(), true);
        TaskStore::new(h.owner.store())
            .save(&[Save::Create(&root)])
            .unwrap();
        let owned = h
            .ensure_task_owned_subagent(root.id(), owner, "fg", config(), Mode::Foreground)
            .unwrap();
        let mut child = Task::new(
            Uuid::from_u128(2),
            TaskOwner::Conversation(owned.child),
            Checkpoint {
                phase: "start".into(),
                data: serde_json::Value::Null,
            },
            3,
        )
        .unwrap()
        .registered("child".into(), true);
        child.start().unwrap();
        child
            .wait(crate::task::WaitFor::DeadlineMillis(10))
            .unwrap();
        TaskStore::new(h.owner.store())
            .save(&[Save::Create(&child)])
            .unwrap();
        let mut registry = Registry::default();
        registry.register("root", true, |_: &Task| {
            Ok(Step::Succeed(serde_json::json!("root")))
        });
        registry.register("child", true, |_: &Task| {
            Ok(Step::Succeed(serde_json::json!("child")))
        });
        let mut scheduler = Scheduler::attach(h.owner.clone(), registry).unwrap();
        scheduler.tick(0).unwrap();
        assert_eq!(
            scheduler.get(root.id()).unwrap().unwrap().state(),
            &TaskState::Completing(serde_json::json!("root"))
        );
        let late = Task::new(
            Uuid::from_u128(3),
            TaskOwner::Conversation(owned.child),
            Checkpoint {
                phase: "start".into(),
                data: serde_json::Value::Null,
            },
            3,
        )
        .unwrap()
        .registered("child".into(), true);
        TaskStore::new(h.owner.store())
            .save(&[Save::Create(&late)])
            .unwrap();
        scheduler.tick(0).unwrap();
        assert_eq!(
            scheduler.get(late.id()).unwrap().unwrap().state(),
            &TaskState::Succeeded(serde_json::json!("child"))
        );
        assert_eq!(
            scheduler.get(root.id()).unwrap().unwrap().state(),
            &TaskState::Completing(serde_json::json!("root"))
        );
        scheduler.tick(10).unwrap();
        scheduler.tick(10).unwrap();
        assert_eq!(
            scheduler.get(root.id()).unwrap().unwrap().state(),
            &TaskState::Succeeded(serde_json::json!("root"))
        );
    }

    #[test]
    fn abort_of_completing_owner_marks_owned_conversation_work() {
        use crate::task::TaskState;
        use crate::task_scheduler::{Registry, Scheduler, Step};
        let dir = tempfile::tempdir().unwrap();
        let mut h = Harness::open_paused(dir.path().join("db"), options()).unwrap();
        let conversation = h.create_conversation(config()).unwrap();
        let owner = TaskOwner::Conversation(conversation);
        let checkpoint = Checkpoint {
            phase: "start".into(),
            data: serde_json::Value::Null,
        };
        let root = Task::new(Uuid::from_u128(1), owner.clone(), checkpoint.clone(), 3)
            .unwrap()
            .registered("root".into(), true);
        TaskStore::new(h.owner.store())
            .save(&[Save::Create(&root)])
            .unwrap();
        let owned = h
            .ensure_task_owned_subagent(root.id(), owner, "fg", config(), Mode::Foreground)
            .unwrap();
        let child = Task::new(
            Uuid::from_u128(2),
            TaskOwner::Conversation(owned.child),
            checkpoint,
            3,
        )
        .unwrap()
        .registered("child".into(), true);
        TaskStore::new(h.owner.store())
            .save(&[Save::Create(&child)])
            .unwrap();
        let mut registry = Registry::default();
        registry.register("root", true, |_: &Task| {
            Ok(Step::Succeed(serde_json::json!("root")))
        });
        registry.register_with_abort(
            "child",
            true,
            |_: &Task| Ok(Step::Succeed(serde_json::Value::Null)),
            |_: &Task| Ok(Step::Succeed(serde_json::Value::Null)),
        );
        let mut scheduler = Scheduler::attach(h.owner.clone(), registry).unwrap();
        scheduler.tick(0).unwrap();
        h.request_abort_task(root.id()).unwrap();
        assert!(scheduler.get(root.id()).unwrap().unwrap().abort_requested());
        assert!(
            scheduler
                .get(child.id())
                .unwrap()
                .unwrap()
                .abort_requested()
        );
        assert!(h.conversation(owned.child).unwrap().abort_requested);
        scheduler.tick(0).unwrap();
        assert_eq!(
            scheduler.get(child.id()).unwrap().unwrap().state(),
            &TaskState::Cancelled
        );
        assert_eq!(
            scheduler.get(root.id()).unwrap().unwrap().state(),
            &TaskState::Completing(serde_json::json!("root"))
        );
        assert!(!scheduler.tick(0).unwrap());
    }

    #[test]
    fn restricted_admission_binds_selection_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut h = Harness::open_paused(&path, options()).unwrap();
        let (task, owner) = setup(&mut h);
        let allowed = Some(vec!["read".to_string()]);
        let a = h
            .submit_task_owned_subagent_restricted(
                task,
                owner.clone(),
                "restricted",
                config(),
                Mode::Foreground,
                "work".into(),
                allowed.clone(),
            )
            .unwrap();
        assert_eq!(h.conversation(a.child).unwrap().allowed_tools, allowed);
        assert_eq!(h.conversation(a.child).unwrap().submissions.len(), 1);
        assert!(
            h.submit_task_owned_subagent_restricted(
                task,
                owner.clone(),
                "restricted",
                config(),
                Mode::Foreground,
                "work".into(),
                None
            )
            .is_err()
        );
        assert!(
            h.ensure_task_owned_subagent_restricted(
                task,
                owner.clone(),
                "bad",
                config(),
                Mode::Foreground,
                Some(vec!["x".into(), "x".into()])
            )
            .is_err()
        );
        drop(h);
        let mut h = Harness::open_paused(&path, options()).unwrap();
        assert_eq!(
            h.submit_task_owned_subagent_restricted(
                task,
                owner,
                "restricted",
                config(),
                Mode::Foreground,
                "work".into(),
                allowed
            )
            .unwrap(),
            a
        );
    }

    #[test]
    fn restricted_concurrent_admissions_share_one_child() {
        let dir = tempfile::tempdir().unwrap();
        let mut h = Harness::open_paused(dir.path().join("db"), options()).unwrap();
        let (task, owner) = setup(&mut h);
        let barrier = Arc::new(Barrier::new(8));
        let workers: Vec<_> = (0..8)
            .map(|_| {
                let mut worker = h.worker();
                let owner = owner.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    worker
                        .submit_task_owned_subagent_restricted(
                            task,
                            owner,
                            "job",
                            config(),
                            Mode::Foreground,
                            "work".into(),
                            Some(vec!["read".into()]),
                        )
                        .unwrap()
                })
            })
            .collect();
        let results: Vec<_> = workers.into_iter().map(|w| w.join().unwrap()).collect();
        assert!(results.iter().all(|a| a == &results[0]));
        let child = h.conversation(results[0].child).unwrap();
        assert_eq!(child.allowed_tools, Some(vec!["read".into()]));
        assert_eq!(child.submissions.len(), 1);
    }

    #[test]
    fn submitted_child_is_atomic_idempotent_and_fenced() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut h = Harness::open_paused(&path, options()).unwrap();
        let (task, owner) = setup(&mut h);
        let barrier = Arc::new(Barrier::new(8));
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let mut worker = h.worker();
                let owner = owner.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    worker
                        .submit_task_owned_subagent(
                            task,
                            owner,
                            "job",
                            config(),
                            Mode::Foreground,
                            "hello".into(),
                        )
                        .unwrap()
                })
            })
            .collect();
        let results: Vec<_> = threads.into_iter().map(|t| t.join().unwrap()).collect();
        assert!(results.iter().all(|a| a == &results[0]));
        let a = &results[0];
        let child = h.conversation(a.child).unwrap();
        assert_eq!(child.submissions.len(), 1);
        assert_eq!(child.submissions[0].id, a.submission.as_ref().unwrap().id);
        assert_eq!(child.submissions[0].content, "hello");
        let history = h
            .history(
                a.child,
                crate::observer::Cursor(0),
                std::num::NonZeroUsize::new(10).unwrap(),
            )
            .unwrap();
        assert_eq!(history.records.len(), 1);
        assert!(history.records[0].value.to_string().contains("hello"));
        assert_eq!(
            EdgeStore::new(h.subagent_store().unwrap())
                .children(task)
                .unwrap()
                .len(),
            1
        );
        assert!(
            h.submit_task_owned_subagent(
                task,
                owner.clone(),
                "job",
                config(),
                Mode::Foreground,
                "changed".into()
            )
            .is_err()
        );
        assert!(
            h.ensure_task_owned_subagent(task, owner.clone(), "job", config(), Mode::Foreground)
                .is_err()
        );
        h.request_abort_task(task).unwrap();
        assert!(h.conversation(a.child).unwrap().abort_requested);
        assert!(
            h.submit_task_owned_subagent(
                task,
                owner.clone(),
                "other",
                config(),
                Mode::Foreground,
                "hello".into()
            )
            .is_err()
        );
        drop(h);
        let mut h = Harness::open_paused(&path, options()).unwrap();
        assert_eq!(
            h.submit_task_owned_subagent(
                task,
                owner,
                "job",
                config(),
                Mode::Foreground,
                "hello".into()
            )
            .unwrap(),
            *a
        );
    }

    #[test]
    fn notification_destination_is_exclusive_across_races_and_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut h = Harness::open_paused(&path, options()).unwrap();
        let (task, owner) = setup(&mut h);
        let barrier = Arc::new(Barrier::new(2));
        let workers: Vec<_> = ["first", "second"]
            .into_iter()
            .map(|name| {
                let mut worker = h.worker();
                let owner = owner.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    worker.submit_task_owned_subagent_notified(
                        task,
                        owner,
                        name,
                        config(),
                        "work".into(),
                        None,
                        "same".into(),
                    )
                })
            })
            .collect();
        let results: Vec<_> = workers
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .collect();
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        let winner = results.into_iter().find_map(Result::ok).unwrap();
        let other = h.create_conversation(config()).unwrap();
        let notify = SubmissionNotify {
            task_id: task,
            key: "same".into(),
        };
        assert!(
            h.submit_with_notification(
                other,
                None,
                "other".into(),
                crate::runtime::AdmissionOptions::default(),
                notify.clone()
            )
            .is_err()
        );
        assert!(
            h.submit_write_with_notification(other, None, "note".into(), None, notify)
                .is_err()
        );
        let forged = task_store::Signal::new_typed(
            Uuid::new_v4(),
            task,
            &crate::runtime::submission_settled_signal(),
            "same",
            serde_json::json!({"id": winner.submission.as_ref().unwrap().id, "conversationId": winner.child,
                "type":"input", "status":"done", "answer":"wrong"}),
        )
        .unwrap();
        assert!(
            TaskStore::new(h.subagent_store().unwrap())
                .send_signal(&forged)
                .is_err()
        );
        drop(h);
        let mut h = Harness::open_paused(&path, options()).unwrap();
        assert_eq!(
            h.submit_task_owned_subagent_notified(
                task,
                owner.clone(),
                &winner.key,
                config(),
                "work".into(),
                None,
                "same".into()
            )
            .unwrap(),
            winner
        );
        assert!(
            h.submit_task_owned_subagent_notified(
                task,
                owner,
                "third",
                config(),
                "work".into(),
                None,
                "same".into()
            )
            .is_err()
        );
    }

    #[test]
    fn notification_reservation_races_ordinary_admission() {
        for _ in 0..12 {
            let dir = tempfile::tempdir().unwrap();
            let mut h = Harness::open_paused(dir.path().join("db"), options()).unwrap();
            let (task, owner) = setup(&mut h);
            let other = h.create_conversation(config()).unwrap();
            let barrier = Arc::new(Barrier::new(2));
            let mut worker = h.worker();
            let gate = barrier.clone();
            let ordinary = std::thread::spawn(move || {
                gate.wait();
                worker.submit_with_notification(
                    other,
                    None,
                    "ordinary".into(),
                    crate::runtime::AdmissionOptions::default(),
                    SubmissionNotify {
                        task_id: task,
                        key: "race".into(),
                    },
                )
            });
            barrier.wait();
            let foreground = h.submit_task_owned_subagent_notified(
                task,
                owner,
                "child",
                config(),
                "work".into(),
                None,
                "race".into(),
            );
            let ordinary = ordinary.join().unwrap();
            assert_ne!(foreground.is_ok(), ordinary.is_ok());
        }
    }

    #[test]
    fn notification_destination_rejects_legacy_admissions_and_receipts() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut h = Harness::open_paused(&path, options()).unwrap();
        let (task, owner) = setup(&mut h);
        let other = h.create_conversation(config()).unwrap();
        h.submit_with_notification(
            other,
            None,
            "legacy".into(),
            crate::runtime::AdmissionOptions::default(),
            SubmissionNotify {
                task_id: task,
                key: "legacy".into(),
            },
        )
        .unwrap();
        let token = crate::runtime::submission_settled_signal();
        let receipt = task_store::Signal::new_typed(
            Uuid::new_v4(),
            task,
            &token,
            "receipt",
            serde_json::json!({"id": Uuid::new_v4(), "conversationId": other,
                "type":"input", "status":"done", "answer":"old"}),
        )
        .unwrap();
        TaskStore::new(h.subagent_store().unwrap())
            .send_signal(&receipt)
            .unwrap();
        drop(h);
        let mut h = Harness::open_paused(&path, options()).unwrap();
        for key in ["legacy", "receipt"] {
            assert!(
                h.submit_task_owned_subagent_notified(
                    task,
                    owner.clone(),
                    key,
                    config(),
                    "work".into(),
                    None,
                    key.into()
                )
                .is_err()
            );
        }
    }

    #[tokio::test]
    async fn notified_foreground_submission_recovers_and_delivers_to_owner() {
        use futures_util::{StreamExt, stream};
        use pi_rs_core::ModelEvent;

        struct Answer;
        #[async_trait]
        impl Model for Answer {
            async fn stream(&self, _: ModelRequest) -> Result<ModelStream, ModelError> {
                Ok(stream::iter(vec![
                    Ok(ModelEvent::Text("child answer".into())),
                    Ok(ModelEvent::Complete {
                        calls: vec![],
                        usage: None,
                    }),
                ])
                .boxed())
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut h = Harness::open_paused(&path, options()).unwrap();
        let (task, owner) = setup(&mut h);
        let admission = h
            .submit_task_owned_subagent_notified(
                task,
                owner.clone(),
                "child",
                config(),
                "work".into(),
                None,
                "answered".into(),
            )
            .unwrap();
        assert!(
            h.submit_task_owned_subagent_notified(
                task,
                owner.clone(),
                "child",
                config(),
                "work".into(),
                None,
                "other".into(),
            )
            .is_err()
        );
        assert!(
            h.submit_task_owned_subagent_notified(
                task,
                owner.clone(),
                "invalid",
                config(),
                "work".into(),
                None,
                String::new(),
            )
            .is_err()
        );
        let store = TaskStore::new(h.subagent_store().unwrap());
        let forged = serde_json::json!({
            "id": admission.submission.as_ref().unwrap().id, "conversationId": admission.child,
            "type": "input", "status": "done", "answer": "forged"
        });
        let forged_status = serde_json::json!({
            "id": admission.submission.as_ref().unwrap().id, "conversationId": admission.child,
            "type": "input", "status": "unanswered", "reason": "forged"
        });
        let forged_receipt = task_store::Signal::new_typed(
            Uuid::new_v4(),
            task,
            &crate::runtime::submission_settled_signal(),
            "answered",
            forged_status,
        )
        .unwrap();
        assert!(store.send_signal(&forged_receipt).is_err());
        assert!(
            store
                .deliver_signal(
                    task,
                    &crate::runtime::submission_settled_signal(),
                    "answered",
                    forged.clone()
                )
                .is_err()
        );
        let mut waiting = store.load(task).unwrap().unwrap();
        waiting.task.start().unwrap();
        waiting
            .task
            .wait(crate::task::WaitFor::TypedSignal(
                crate::runtime::submission_settled_signal()
                    .key("answered")
                    .unwrap(),
            ))
            .unwrap();
        store.save(&[Save::Update(&waiting)]).unwrap();
        drop(h);
        let mut h = Harness::open_paused(&path, options()).unwrap();
        assert_eq!(
            h.submit_task_owned_subagent_notified(
                task,
                owner,
                "child",
                config(),
                "work".into(),
                None,
                "answered".into(),
            )
            .unwrap(),
            admission
        );
        let store = TaskStore::new(h.subagent_store().unwrap());
        assert!(
            store
                .deliver_signal_at(
                    task,
                    &crate::runtime::submission_settled_signal(),
                    "answered",
                    forged.clone(),
                    0
                )
                .is_err()
        );
        drop(h);
        let mut opts = options();
        opts.model = Arc::new(Answer);
        let mut scheduler = Scheduler::open(&path, opts).unwrap();
        let attacker = scheduler.subagent_harness().worker();
        let forger = std::thread::spawn(move || {
            for _ in 0..64 {
                assert!(
                    TaskStore::new(attacker.subagent_store().unwrap())
                        .deliver_signal(
                            task,
                            &crate::runtime::submission_settled_signal(),
                            "answered",
                            forged.clone(),
                        )
                        .is_err()
                );
            }
        });
        tokio::time::timeout(
            std::time::Duration::from_secs(10),
            scheduler.wait(admission.child),
        )
        .await
        .unwrap()
        .unwrap();
        forger.join().unwrap();
        scheduler.shutdown().await.unwrap();
        let h = Harness::open_paused(&path, options()).unwrap();
        let signal = TaskStore::new(h.subagent_store().unwrap())
            .read_typed_signal(
                task,
                &crate::runtime::submission_settled_signal(),
                "answered",
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            signal["id"],
            serde_json::json!(admission.submission.unwrap().id)
        );
        assert_eq!(signal["status"], "done");
        assert_eq!(signal["answer"], "child answer");
        let store = TaskStore::new(h.subagent_store().unwrap());
        let waiting = store.load(task).unwrap().unwrap();
        assert!(matches!(
            store.poll_wait(&waiting, 0).unwrap(),
            crate::task_store::WakeResult::Woken { .. }
        ));
    }

    #[test]
    fn submission_races_parent_abort_without_stranding_foreground_child() {
        for _ in 0..30 {
            let dir = tempfile::tempdir().unwrap();
            let mut h = Harness::open_paused(dir.path().join("db"), options()).unwrap();
            let (task, owner) = setup(&mut h);
            let barrier = Arc::new(Barrier::new(2));
            let mut worker = h.worker();
            let b = barrier.clone();
            let admission = std::thread::spawn(move || {
                b.wait();
                worker.submit_task_owned_subagent(
                    task,
                    owner,
                    "job",
                    config(),
                    Mode::Foreground,
                    "hello".into(),
                )
            });
            barrier.wait();
            h.request_abort_task(task).unwrap();
            if let Ok(a) = admission.join().unwrap() {
                let child = h.conversation(a.child).unwrap();
                assert!(child.abort_requested);
                assert_eq!(child.submissions.len(), 1);
            } else {
                assert!(
                    EdgeStore::new(h.subagent_store().unwrap())
                        .children(task)
                        .unwrap()
                        .is_empty()
                );
            }
        }
    }

    #[tokio::test]
    async fn submitted_children_schedule_and_recover_across_background_boundary() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        struct Pending(Arc<AtomicUsize>);
        #[async_trait]
        impl Model for Pending {
            async fn stream(&self, _: ModelRequest) -> Result<ModelStream, ModelError> {
                self.0.fetch_add(1, Ordering::SeqCst);
                std::future::pending().await
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut h = Harness::open_paused(&path, options()).unwrap();
        let (task, owner) = setup(&mut h);
        let fg = h
            .submit_task_owned_subagent(
                task,
                owner.clone(),
                "fg",
                config(),
                Mode::Foreground,
                "recover".into(),
            )
            .unwrap();
        drop(h); // committed, before any scheduler was started
        let started = Arc::new(AtomicUsize::new(0));
        let mut opts = options();
        opts.model = Arc::new(Pending(started.clone()));
        let mut scheduler = Scheduler::open(&path, opts).unwrap();
        let bg = scheduler
            .submit_task_owned_subagent(
                task,
                owner,
                "bg",
                config(),
                Mode::Background,
                "background".into(),
            )
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while started.load(Ordering::SeqCst) < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        scheduler.abort_task(task).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), scheduler.wait(fg.child))
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            scheduler.conversation(bg.child).unwrap().submissions[0].status,
            crate::runtime::Status::Running
        ));
        scheduler.shutdown().await.unwrap();
    }

    #[test]
    fn duplicate_admission_and_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut h = Harness::open_paused(&path, options()).unwrap();
        let (task, owner) = setup(&mut h);
        let old = TaskStore::new(h.subagent_store().unwrap())
            .load(task)
            .unwrap()
            .unwrap();
        let barrier = Arc::new(Barrier::new(8));
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let mut h = h.worker();
                let b = barrier.clone();
                let owner = owner.clone();
                std::thread::spawn(move || {
                    b.wait();
                    h.ensure_task_owned_subagent(task, owner, "same", config(), Mode::Foreground)
                        .unwrap()
                })
            })
            .collect();
        let admissions: Vec<_> = threads.into_iter().map(|t| t.join().unwrap()).collect();
        assert!(admissions.iter().all(|a| a == &admissions[0]));
        let store = h.subagent_store().unwrap();
        assert!(matches!(
            TaskStore::new(store).save(&[Save::Update(&old)]),
            Err(crate::task_store::Error::Store(
                durable_store::Error::Conflict { .. }
            ))
        ));
        assert_eq!(EdgeStore::new(store).children(task).unwrap().len(), 1);
        assert!(
            h.ensure_task_owned_subagent(task, owner.clone(), "same", config(), Mode::Background)
                .is_err()
        );
        drop(h);
        let mut h = Harness::open_paused(&path, options()).unwrap();
        assert_eq!(
            h.ensure_task_owned_subagent(task, owner, "same", config(), Mode::Foreground)
                .unwrap(),
            admissions[0]
        );
        assert_eq!(
            h.conversation(admissions[0].child).unwrap().id,
            admissions[0].child
        );
    }
    #[test]
    fn abort_first_never_creates_a_child() {
        let dir = tempfile::tempdir().unwrap();
        let mut h = Harness::open_paused(dir.path().join("db"), options()).unwrap();
        let (task, owner) = setup(&mut h);
        crate::task_scheduler::request_abort_subtree(
            &TaskStore::new(h.subagent_store().unwrap()),
            task,
        )
        .unwrap();
        assert!(
            h.ensure_task_owned_subagent(task, owner, "child", config(), Mode::Foreground)
                .is_err()
        );
        assert!(
            EdgeStore::new(h.subagent_store().unwrap())
                .children(task)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn foreground_traversal_persists_and_skips_background() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut h = Harness::open_paused(&path, options()).unwrap();
        let (task, owner) = setup(&mut h);
        let parent = match owner {
            TaskOwner::Conversation(id) => id,
            _ => unreachable!(),
        };
        let child = h
            .ensure_task_owned_subagent(task, owner.clone(), "fg", config(), Mode::Foreground)
            .unwrap()
            .child;
        let background = h
            .ensure_task_owned_subagent(task, owner, "bg", config(), Mode::Background)
            .unwrap()
            .child;
        let nested = Task::new(
            Uuid::new_v4(),
            TaskOwner::Conversation(child),
            Checkpoint {
                phase: "start".into(),
                data: serde_json::Value::Null,
            },
            1,
        )
        .unwrap();
        TaskStore::new(h.subagent_store().unwrap())
            .save(&[Save::Create(&nested)])
            .unwrap();
        h.request_abort(parent).unwrap();
        drop(h);
        let h = Harness::open_paused(&path, options()).unwrap();
        assert!(h.conversation(parent).unwrap().abort_requested);
        assert!(h.conversation(child).unwrap().abort_requested);
        assert!(!h.conversation(background).unwrap().abort_requested);
        let ts = TaskStore::new(h.subagent_store().unwrap());
        assert!(ts.load(task).unwrap().unwrap().task.abort_requested());
        assert!(
            ts.load(nested.id())
                .unwrap()
                .unwrap()
                .task
                .abort_requested()
        );
    }

    #[tokio::test]
    async fn background_abort_cancels_active_children_and_waits_for_tasks() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        struct Pending(Arc<AtomicUsize>);
        #[async_trait]
        impl Model for Pending {
            async fn stream(&self, _: ModelRequest) -> Result<ModelStream, ModelError> {
                self.0.fetch_add(1, Ordering::SeqCst);
                std::future::pending().await
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut h = Harness::open_paused(&path, options()).unwrap();
        let (task, owner) = setup(&mut h);
        let parent = match owner {
            TaskOwner::Conversation(id) => id,
            _ => unreachable!(),
        };
        let child = h
            .ensure_task_owned_subagent(task, owner, "bg", config(), Mode::Background)
            .unwrap()
            .child;
        drop(h);
        let started = Arc::new(AtomicUsize::new(0));
        let mut opts = options();
        opts.model = Arc::new(Pending(started.clone()));
        let mut scheduler = Scheduler::open(&path, opts).unwrap();
        let driver = scheduler.start_tasks(Default::default()).unwrap();
        scheduler.submit(child, None, "active".into()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while started.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        tokio::time::timeout(
            std::time::Duration::from_secs(8),
            scheduler.abort_and_wait_with_background(parent),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(!scheduler.conversation(child).unwrap().abort_requested);
        assert!(!matches!(
            scheduler.conversation(child).unwrap().submissions[0].status,
            crate::runtime::Status::Running
        ));
        assert!(driver.get(task).unwrap().unwrap().is_terminal());
        driver.shutdown().unwrap();
        scheduler.shutdown().await.unwrap();
    }

    #[test]
    fn task_abort_traverses_foreground_after_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut h = Harness::open_paused(&path, options()).unwrap();
        let (task, owner) = setup(&mut h);
        let child = h
            .ensure_task_owned_subagent(task, owner, "fg", config(), Mode::Foreground)
            .unwrap()
            .child;
        h.request_abort_task(task).unwrap();
        drop(h);
        let h = Harness::open_paused(&path, options()).unwrap();
        assert!(
            TaskStore::new(h.subagent_store().unwrap())
                .load(task)
                .unwrap()
                .unwrap()
                .task
                .abort_requested()
        );
        assert!(h.conversation(child).unwrap().abort_requested);
    }

    #[test]
    fn driver_recovers_child_abort_when_owner_is_still_live() {
        for mode in [Mode::Foreground, Mode::Background] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("db");
            let mut h = Harness::open_paused(&path, options()).unwrap();
            let (task, owner) = setup(&mut h);
            let child = h
                .ensure_task_owned_subagent(task, owner, "child", config(), mode)
                .unwrap()
                .child;
            // A separate legacy edge has a mismatched owner. Its unmarked
            // conversation must not prevent this child's recovery.
            let unrelated = h.create_conversation(config()).unwrap();
            let corrupt = Edge {
                task,
                task_owner: TaskOwner::Task(Uuid::new_v4()),
                child: unrelated,
                mode: Mode::Foreground,
            };
            h.subagent_store()
                .unwrap()
                .commit(
                    &[],
                    &[Mutation::Put {
                        key: ownership_edge::edge_key(unrelated),
                        value: serde_json::to_vec(&corrupt).unwrap(),
                    }],
                )
                .unwrap();
            h.request_abort(child).unwrap();
            assert!(
                !TaskStore::new(h.subagent_store().unwrap())
                    .load(task)
                    .unwrap()
                    .unwrap()
                    .task
                    .abort_requested()
            );
            drop(h);
            let h = Harness::open_paused(&path, options()).unwrap();
            let driver = h.start_tasks(Default::default()).unwrap();
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(4);
            while h.conversation(child).unwrap().abort_requested {
                assert!(
                    std::time::Instant::now() < deadline,
                    "marked child was not recovered"
                );
                std::thread::yield_now();
            }
            assert!(!driver.get(task).unwrap().unwrap().abort_requested());
            driver.shutdown().unwrap();
            drop(h);
            let reopened = Harness::open_paused(&path, options()).unwrap();
            assert!(!reopened.conversation(child).unwrap().abort_requested);
        }
    }

    #[test]
    fn driver_reports_mismatched_marked_edge_without_clearing_abort() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut h = Harness::open_paused(&path, options()).unwrap();
        let (task, _) = setup(&mut h);
        let child = h.create_conversation(config()).unwrap();
        h.request_abort(child).unwrap();
        let corrupt = Edge {
            task,
            task_owner: TaskOwner::Task(Uuid::new_v4()),
            child,
            mode: Mode::Foreground,
        };
        h.subagent_store()
            .unwrap()
            .commit(
                &[],
                &[Mutation::Put {
                    key: ownership_edge::edge_key(child),
                    value: serde_json::to_vec(&corrupt).unwrap(),
                }],
            )
            .unwrap();
        drop(h);
        let h = Harness::open_paused(&path, options()).unwrap();
        let driver = h.start_tasks(Default::default()).unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(4);
        loop {
            if let Err(error) = driver.get(task) {
                assert!(
                    error.to_string().contains("ownership edge owner mismatch"),
                    "{error}"
                );
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "corrupt ownership was not reported"
            );
            std::thread::yield_now();
        }
        assert!(h.conversation(child).unwrap().abort_requested);
        assert!(driver.shutdown().is_err());
        drop(h);
        let reopened = Harness::open_paused(&path, options()).unwrap();
        assert!(reopened.conversation(child).unwrap().abort_requested);
    }

    #[test]
    fn driver_recovers_persisted_owned_cleanup_after_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut h = Harness::open_paused(&path, options()).unwrap();
        let (task, owner) = setup(&mut h);
        let child = h
            .ensure_task_owned_subagent(task, owner.clone(), "fg", config(), Mode::Foreground)
            .unwrap()
            .child;
        let background = h
            .ensure_task_owned_subagent(task, owner, "bg", config(), Mode::Background)
            .unwrap()
            .child;
        h.request_abort_task(task).unwrap();
        drop(h);
        let h = Harness::open_paused(&path, options()).unwrap();
        let driver = h.start_tasks(Default::default()).unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while h.conversation(child).unwrap().abort_requested
            || !driver.get(task).unwrap().unwrap().is_terminal()
        {
            assert!(std::time::Instant::now() < deadline);
            std::thread::yield_now();
        }
        assert!(!h.conversation(background).unwrap().abort_requested);
        driver.shutdown().unwrap();
        drop(h);
        let h = Harness::open_paused(&path, options()).unwrap();
        assert!(!h.conversation(child).unwrap().abort_requested);
        assert!(!h.conversation(background).unwrap().abort_requested);
    }

    #[tokio::test]
    async fn owner_shutdown_drains_cleanup_before_joining_handler() {
        use crate::{
            extension,
            runtime::RuntimeOwner,
            task_scheduler::{Registry, Step},
        };
        use std::time::{Duration, Instant};
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut h = Harness::open_paused(&path, options()).unwrap();
        let (task, task_owner) = setup(&mut h);
        let child = h
            .ensure_task_owned_subagent(task, task_owner.clone(), "fg", config(), Mode::Foreground)
            .unwrap()
            .child;
        let waiting = Task::new(
            Uuid::new_v4(),
            task_owner,
            Checkpoint {
                phase: "start".into(),
                data: serde_json::Value::Null,
            },
            1,
        )
        .unwrap()
        .registered("wait-cleanup".into(), true);
        TaskStore::new(h.subagent_store().unwrap())
            .save(&[Save::Create(&waiting)])
            .unwrap();
        drop(h);

        // Publish a worker to the handler only after opening the owner. Hold the
        // child's worker lock so cleanup cannot win the shutdown race early.
        let worker_slot = Arc::new(std::sync::Mutex::new(None::<Harness>));
        let slot = worker_slot.clone();
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let mut registry = Registry::default();
        registry.register("wait-cleanup", true, move |_: &Task| {
            entered_tx.send(()).unwrap();
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                if slot
                    .lock()
                    .unwrap()
                    .as_ref()
                    .is_some_and(|h| !h.conversation(child).unwrap().abort_requested)
                {
                    return Ok(Step::Succeed(serde_json::Value::Null));
                }
                if Instant::now() >= deadline {
                    return Err("cleanup did not complete".into());
                }
                std::thread::sleep(Duration::from_millis(5));
            }
        });
        let mut owner =
            RuntimeOwner::open(&path, options(), extension::Registry::default(), registry).unwrap();
        let h = owner.scheduler_mut().subagent_harness();
        let root = h.owner.conversation_controls().root();
        let guard = h
            .owner
            .conversation_controls()
            .get(child)
            .worker
            .clone()
            .lock_owned()
            .await;
        h.request_abort_task(task).unwrap();
        *worker_slot.lock().unwrap() = Some(h.worker());
        tokio::task::spawn_blocking(move || {
            entered_rx.recv_timeout(Duration::from_secs(5)).unwrap()
        })
        .await
        .unwrap();
        let shutdown = tokio::spawn(owner.shutdown());
        tokio::time::timeout(Duration::from_secs(5), root.cancelled())
            .await
            .unwrap();
        drop(guard);
        tokio::time::timeout(Duration::from_secs(8), shutdown)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        drop(worker_slot.lock().unwrap().take());
        let reopened = Harness::open_paused(&path, options()).unwrap();
        assert!(matches!(
            TaskStore::new(reopened.subagent_store().unwrap())
                .load(waiting.id())
                .unwrap()
                .unwrap()
                .task
                .state(),
            crate::task::TaskState::Succeeded(_)
        ));
    }

    #[test]
    fn standalone_driver_abort_races_foreground_admission_atomically() {
        for _ in 0..20 {
            let dir = tempfile::tempdir().unwrap();
            let mut h = Harness::open_paused(dir.path().join("db"), options()).unwrap();
            let (task, owner) = setup(&mut h);
            let driver = h.start_tasks(Default::default()).unwrap();
            let barrier = Arc::new(Barrier::new(2));
            let mut worker = h.worker();
            let b = barrier.clone();
            let admission = std::thread::spawn(move || {
                b.wait();
                worker.ensure_task_owned_subagent(task, owner, "fg", config(), Mode::Foreground)
            });
            barrier.wait();
            let abort = driver.request_abort(task);
            let admitted = admission.join().unwrap();
            assert!(abort.is_ok());
            assert!(driver.get(task).unwrap().unwrap().abort_requested());
            if let Ok(child) = admitted {
                let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
                while h.conversation(child.child).unwrap().abort_requested {
                    assert!(std::time::Instant::now() < deadline);
                    std::thread::yield_now();
                }
            }
            driver.shutdown().unwrap();
        }
    }

    #[tokio::test]
    async fn scheduler_cancels_active_foreground_but_not_background() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        struct Pending(Arc<AtomicUsize>);
        #[async_trait]
        impl Model for Pending {
            async fn stream(&self, _: ModelRequest) -> Result<ModelStream, ModelError> {
                self.0.fetch_add(1, Ordering::SeqCst);
                std::future::pending().await
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut h = Harness::open_paused(&path, options()).unwrap();
        let (task, owner) = setup(&mut h);
        let parent = match owner {
            TaskOwner::Conversation(id) => id,
            _ => unreachable!(),
        };
        let child = h
            .ensure_task_owned_subagent(task, owner.clone(), "fg", config(), Mode::Foreground)
            .unwrap()
            .child;
        let background = h
            .ensure_task_owned_subagent(task, owner, "bg", config(), Mode::Background)
            .unwrap()
            .child;
        drop(h);
        let started = Arc::new(AtomicUsize::new(0));
        let mut opts = options();
        opts.model = Arc::new(Pending(started.clone()));
        let mut scheduler = Scheduler::open(&path, opts).unwrap();
        scheduler.submit(child, None, "child".into()).unwrap();
        scheduler
            .submit(background, None, "background".into())
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while started.load(Ordering::SeqCst) < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        scheduler.abort(parent).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), scheduler.wait(child))
            .await
            .unwrap()
            .unwrap();
        assert!(!scheduler.conversation(child).unwrap().abort_requested);
        assert!(!scheduler.conversation(background).unwrap().abort_requested);
        assert!(scheduler.conversation(parent).unwrap().abort_requested);
        scheduler.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn driver_abort_joins_active_owned_conversation() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        struct Pending(Arc<AtomicUsize>);
        #[async_trait]
        impl Model for Pending {
            async fn stream(&self, _: ModelRequest) -> Result<ModelStream, ModelError> {
                self.0.fetch_add(1, Ordering::SeqCst);
                std::future::pending().await
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut h = Harness::open_paused(&path, options()).unwrap();
        let (task, owner) = setup(&mut h);
        let parent = match owner {
            TaskOwner::Conversation(id) => id,
            _ => unreachable!(),
        };
        let child = h
            .ensure_task_owned_subagent(task, owner.clone(), "fg", config(), Mode::Foreground)
            .unwrap()
            .child;
        let background = h
            .ensure_task_owned_subagent(task, owner, "bg", config(), Mode::Background)
            .unwrap()
            .child;
        drop(h);
        let started = Arc::new(AtomicUsize::new(0));
        let mut opts = options();
        opts.model = Arc::new(Pending(started.clone()));
        let mut scheduler = Scheduler::open(&path, opts).unwrap();
        let driver = scheduler.start_tasks(Default::default()).unwrap();
        scheduler.submit(child, None, "active".into()).unwrap();
        scheduler
            .submit(background, None, "active background".into())
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while started.load(Ordering::SeqCst) < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        driver.request_abort(task).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(8), async {
            loop {
                if !scheduler.conversation(child).unwrap().abort_requested
                    && driver.get(task).unwrap().unwrap().is_terminal()
                {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert!(!scheduler.conversation(child).unwrap().abort_requested);
        assert!(!scheduler.conversation(parent).unwrap().abort_requested);
        assert!(!scheduler.conversation(background).unwrap().abort_requested);
        assert!(matches!(
            scheduler.conversation(background).unwrap().submissions[0].status,
            crate::runtime::Status::Running
        ));
        assert!(driver.get(task).unwrap().unwrap().is_terminal());
        driver.shutdown().unwrap();
        scheduler.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn cleanup_waits_for_owned_conversation_after_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut h = Harness::open_paused(&path, options()).unwrap();
        let (parent_task, owner) = setup(&mut h);
        let child = h
            .ensure_task_owned_subagent(parent_task, owner, "fg", config(), Mode::Foreground)
            .unwrap()
            .child;
        let nested = Task::new(
            Uuid::new_v4(),
            TaskOwner::Conversation(child),
            Checkpoint {
                phase: "start".into(),
                data: serde_json::Value::Null,
            },
            3,
        )
        .unwrap();
        TaskStore::new(h.subagent_store().unwrap())
            .save(&[Save::Create(&nested)])
            .unwrap();
        let ts = TaskStore::new(h.subagent_store().unwrap());
        ts.reserve(&ts.load(parent_task).unwrap().unwrap()).unwrap();
        h.request_abort_task(parent_task).unwrap();
        drop(h); // crash with the parent handler reserved

        let mut h = Harness::open_paused(&path, options()).unwrap();
        let mut tasks = h.attach_tasks(Default::default()).unwrap();
        assert!(tasks.tick(0).unwrap()); // nested task cleanup
        assert!(tasks.get(nested.id()).unwrap().unwrap().is_terminal());
        assert!(!tasks.tick(0).unwrap()); // child conversation has not settled
        assert!(!tasks.get(parent_task).unwrap().unwrap().is_terminal());
        h.resume_conversation(child, &pi_rs_core::CancellationToken::new())
            .await
            .unwrap();
        assert!(!h.conversation(child).unwrap().abort_requested);
        assert!(tasks.tick(0).unwrap());
        assert!(tasks.get(parent_task).unwrap().unwrap().is_terminal());
    }

    #[tokio::test]
    async fn direct_legacy_cycle_abort_and_wait_finishes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut h = Harness::open_paused(&path, options()).unwrap();
        let (task, owner) = setup(&mut h);
        let parent = match owner {
            TaskOwner::Conversation(id) => id,
            _ => unreachable!(),
        };
        let edge = Edge {
            task,
            task_owner: TaskOwner::Conversation(parent),
            child: parent,
            mode: Mode::Foreground,
        };
        let store = h.subagent_store().unwrap();
        assert!(EdgeStore::new(store).prepare_create(&edge).is_err());
        store
            .commit(
                &[],
                &[Mutation::Put {
                    key: ownership_edge::edge_key(parent),
                    value: serde_json::to_vec(&edge).unwrap(),
                }],
            )
            .unwrap();
        drop(h);
        let mut scheduler = Scheduler::open(&path, options()).unwrap();
        let driver = scheduler.start_tasks(Default::default()).unwrap();
        tokio::time::timeout(
            std::time::Duration::from_secs(30),
            scheduler.abort_and_wait(parent),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(driver.get(task).unwrap().unwrap().is_terminal());
        assert!(!scheduler.conversation(parent).unwrap().abort_requested);
        driver.shutdown().unwrap();
        scheduler.shutdown().await.unwrap();
    }

    #[test]
    fn cyclic_conversation_wait_does_not_overtake_acyclic_descendant() {
        let dir = tempfile::tempdir().unwrap();
        let mut h = Harness::open_paused(dir.path().join("db"), options()).unwrap();
        let parent = h.create_conversation(config()).unwrap();
        let checkpoint = || Checkpoint {
            phase: "start".into(),
            data: serde_json::Value::Null,
        };
        let root_task = Task::new(
            Uuid::from_u128(1),
            TaskOwner::Conversation(parent),
            checkpoint(),
            1,
        )
        .unwrap();
        TaskStore::new(h.subagent_store().unwrap())
            .save(&[Save::Create(&root_task)])
            .unwrap();
        let child = h
            .ensure_task_owned_subagent(
                root_task.id(),
                TaskOwner::Conversation(parent),
                "child",
                config(),
                Mode::Foreground,
            )
            .unwrap()
            .child;
        let nested = Task::new(
            Uuid::from_u128(2),
            TaskOwner::Conversation(child),
            checkpoint(),
            1,
        )
        .unwrap();
        let leaf = Task::new(
            Uuid::from_u128(3),
            TaskOwner::Task(nested.id()),
            checkpoint(),
            1,
        )
        .unwrap();
        let store = h.subagent_store().unwrap();
        TaskStore::new(store)
            .save(&[Save::Create(&nested), Save::Create(&leaf)])
            .unwrap();
        let edge = Edge {
            task: nested.id(),
            task_owner: TaskOwner::Conversation(child),
            child: parent,
            mode: Mode::Foreground,
        };
        store
            .commit(
                &[],
                &[Mutation::Put {
                    key: ownership_edge::edge_key(parent),
                    value: serde_json::to_vec(&edge).unwrap(),
                }],
            )
            .unwrap();
        h.request_abort(parent).unwrap();
        let mut scheduler = h.attach_tasks(Default::default()).unwrap();
        assert!(scheduler.tick(0).unwrap());
        assert!(scheduler.get(leaf.id()).unwrap().unwrap().is_terminal());
        assert!(
            !scheduler
                .get(root_task.id())
                .unwrap()
                .unwrap()
                .is_terminal()
        );
        assert!(!scheduler.get(nested.id()).unwrap().unwrap().is_terminal());
    }

    #[test]
    fn task_only_cycle_break_keeps_acyclic_children_first_after_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut h = Harness::open_paused(&path, options()).unwrap();
        let a = Uuid::from_u128(1);
        let b = Uuid::from_u128(2);
        let c = Uuid::from_u128(3);
        let task = |id, owner| {
            Task::new(
                id,
                TaskOwner::Task(owner),
                Checkpoint {
                    phase: "start".into(),
                    data: serde_json::Value::Null,
                },
                1,
            )
            .unwrap()
        };
        let store = h.subagent_store().unwrap();
        let tasks = [task(a, b), task(b, a), task(c, a)];
        let mutations: Vec<_> = tasks
            .iter()
            .map(|t| Mutation::Put {
                key: crate::task_store::key(t.id()),
                value: serde_json::to_vec(t).unwrap(),
            })
            .collect();
        store.commit(&[], &mutations).unwrap();
        h.request_abort_task(a).unwrap();
        drop(h);
        let h = Harness::open_paused(&path, options()).unwrap();
        let mut scheduler = h.attach_tasks(Default::default()).unwrap();
        for _ in 0..3 {
            assert!(scheduler.tick(0).unwrap());
            if !scheduler.get(c).unwrap().unwrap().is_terminal() {
                assert!(!scheduler.get(a).unwrap().unwrap().is_terminal());
                assert!(!scheduler.get(b).unwrap().unwrap().is_terminal());
            }
        }
        for id in [a, b, c] {
            assert!(scheduler.get(id).unwrap().unwrap().is_terminal());
        }
        assert!(!scheduler.tick(0).unwrap());
    }

    #[tokio::test]
    async fn cyclic_legacy_cleanup_recovers_after_racing_admission_and_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut h = Harness::open_paused(&path, options()).unwrap();
        let (task, owner) = setup(&mut h);
        let parent = match owner {
            TaskOwner::Conversation(id) => id,
            _ => unreachable!(),
        };
        let child = h
            .ensure_task_owned_subagent(task, owner.clone(), "fg", config(), Mode::Foreground)
            .unwrap()
            .child;
        let background = h
            .ensure_task_owned_subagent(task, owner, "bg", config(), Mode::Background)
            .unwrap()
            .child;
        let nested = Task::new(
            Uuid::new_v4(),
            TaskOwner::Conversation(child),
            Checkpoint {
                phase: "start".into(),
                data: serde_json::Value::Null,
            },
            1,
        )
        .unwrap();
        let store = h.subagent_store().unwrap();
        TaskStore::new(store)
            .save(&[Save::Create(&nested)])
            .unwrap();
        let edge = Edge {
            task: nested.id(),
            task_owner: TaskOwner::Conversation(child),
            child: parent,
            mode: Mode::Foreground,
        };
        store
            .commit(
                &[],
                &[Mutation::Put {
                    key: ownership_edge::edge_key(parent),
                    value: serde_json::to_vec(&edge).unwrap(),
                }],
            )
            .unwrap();

        // A writer prepared before the abort must not overwrite the persisted
        // mark, even when the legacy graph contains a cycle.
        let stale = TaskStore::new(h.subagent_store().unwrap())
            .load(nested.id())
            .unwrap()
            .unwrap();
        let barrier = Arc::new(Barrier::new(3));
        let mut other = h.worker();
        let b = barrier.clone();
        let nested_id = nested.id();
        let admission = std::thread::spawn(move || {
            b.wait();
            other.ensure_task_owned_subagent(
                nested_id,
                TaskOwner::Conversation(child),
                "racing",
                config(),
                Mode::Foreground,
            )
        });
        let writer_harness = h.worker();
        let mut competing = stale.clone();
        let b = barrier.clone();
        let writer = std::thread::spawn(move || {
            competing.task.request_abort().unwrap();
            b.wait();
            TaskStore::new(writer_harness.subagent_store().unwrap())
                .save(&[Save::Update(&competing)])
        });
        barrier.wait();
        h.request_abort(parent).unwrap();
        assert!(matches!(
            writer.join().unwrap(),
            Ok(())
                | Err(crate::task_store::Error::Store(
                    durable_store::Error::Conflict { .. }
                ))
        ));
        assert!(matches!(
            TaskStore::new(h.subagent_store().unwrap()).save(&[Save::Update(&stale)]),
            Err(crate::task_store::Error::Store(
                durable_store::Error::Conflict { .. }
            ))
        ));
        let admitted = admission.join().unwrap().ok().map(|a| a.child);
        if let Some(id) = admitted {
            assert!(h.conversation(id).unwrap().abort_requested);
        }
        assert!(!h.conversation(background).unwrap().abort_requested);
        drop(h);

        let scheduler = Scheduler::open(&path, options()).unwrap();
        let driver = scheduler.start_tasks(Default::default()).unwrap();
        tokio::time::timeout(
            std::time::Duration::from_secs(30),
            scheduler.wait_for_idle(parent, &pi_rs_core::CancellationToken::new()),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(driver.get(task).unwrap().unwrap().is_terminal());
        assert!(driver.get(nested_id).unwrap().unwrap().is_terminal());
        assert!(!scheduler.conversation(parent).unwrap().abort_requested);
        assert!(!scheduler.conversation(child).unwrap().abort_requested);
        assert!(!scheduler.conversation(background).unwrap().abort_requested);
        if let Some(id) = admitted {
            assert!(!scheduler.conversation(id).unwrap().abort_requested);
        }
        driver.shutdown().unwrap();
        scheduler.shutdown().await.unwrap();
        let reopened = Harness::open_paused(&path, options()).unwrap();
        assert!(!reopened.conversation(parent).unwrap().abort_requested);
        assert!(!reopened.conversation(child).unwrap().abort_requested);
    }

    #[test]
    fn traversal_terminates_on_ownership_cycle() {
        let dir = tempfile::tempdir().unwrap();
        let mut h = Harness::open_paused(dir.path().join("db"), options()).unwrap();
        let (task, owner) = setup(&mut h);
        let parent = match owner {
            TaskOwner::Conversation(id) => id,
            _ => unreachable!(),
        };
        let child = h
            .ensure_task_owned_subagent(task, owner, "child", config(), Mode::Foreground)
            .unwrap()
            .child;
        let nested = Task::new(
            Uuid::new_v4(),
            TaskOwner::Conversation(child),
            Checkpoint {
                phase: "start".into(),
                data: serde_json::Value::Null,
            },
            1,
        )
        .unwrap();
        let store = h.subagent_store().unwrap();
        TaskStore::new(store)
            .save(&[Save::Create(&nested)])
            .unwrap();
        let edge = Edge {
            task: nested.id(),
            task_owner: TaskOwner::Conversation(child),
            child: parent,
            mode: Mode::Foreground,
        };
        assert!(EdgeStore::new(store).prepare_create(&edge).is_err());
        // Simulate a corrupt legacy store to exercise defensive abort traversal.
        store
            .commit(
                &[],
                &[Mutation::Put {
                    key: ownership_edge::edge_key(parent),
                    value: serde_json::to_vec(&edge).unwrap(),
                }],
            )
            .unwrap();
        h.request_abort(parent).unwrap();
        assert!(h.conversation(child).unwrap().abort_requested);
        assert!(
            TaskStore::new(h.subagent_store().unwrap())
                .load(nested.id())
                .unwrap()
                .unwrap()
                .task
                .abort_requested()
        );
    }

    #[test]
    fn admission_races_abort_and_retry_after_reopen() {
        for _ in 0..20 {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("db");
            let mut h = Harness::open_paused(&path, options()).unwrap();
            let (task, owner) = setup(&mut h);
            let barrier = Arc::new(Barrier::new(2));
            let mut worker = h.worker();
            let b = barrier.clone();
            let o = owner.clone();
            let admission = std::thread::spawn(move || {
                b.wait();
                worker.ensure_task_owned_subagent(task, o, "child", config(), Mode::Foreground)
            });
            barrier.wait();
            let abort = h.request_abort_task(task);
            let result = admission.join().unwrap();
            let store = h.subagent_store().unwrap();
            let marked = TaskStore::new(store)
                .load(task)
                .unwrap()
                .unwrap()
                .task
                .abort_requested();
            assert_eq!(marked, abort.is_ok());
            if let Ok(a) = &result {
                assert!(h.conversation(a.child).unwrap().abort_requested);
            }
            let edges = EdgeStore::new(store).children(task).unwrap();
            assert_eq!(edges.len(), usize::from(result.is_ok()));
            drop(h);
            let mut h = Harness::open_paused(&path, options()).unwrap();
            let retry =
                h.ensure_task_owned_subagent(task, owner, "child", config(), Mode::Foreground);
            match result {
                Ok(a) => assert_eq!(retry.unwrap(), a),
                Err(_) => assert!(retry.is_err()),
            }
        }
    }
}
