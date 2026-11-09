use pi_rs_core::{CancellationToken, Error, Event, Observer, Result, Session};
use pi_rs_review::{ID, ReviewContext, ReviewLink, ReviewStatus};
use tokio::sync::mpsc;

use crate::HostTurn;

/// Review an authorized branch with a distinct read-only AIP child Run.
/// The existing AIP budget service owns parent/child reservations.
pub struct ManagedReview<'a> {
    pub parent: &'a HostTurn<'a>,
    pub child: &'a HostTurn<'a>,
    pub from_record: u64,
    pub task: &'a str,
    pub resume: bool,
}

impl ManagedReview<'_> {
    pub async fn run(
        &self,
        parent: &mut Session,
        child: &mut Session,
        cancel: &CancellationToken,
        observer: &mut Observer<'_>,
    ) -> Result<String> {
        let parent_scope = self.parent.scope;
        let child_scope = self.child.scope;
        if child_scope.parent_run.as_deref() != Some(&parent_scope.aip_run)
            || child_scope.aip_run == parent_scope.aip_run
            || child_scope.tenant != parent_scope.tenant
            || child_scope.actor != parent_scope.actor
            || child_scope.workspace != parent_scope.workspace
            || child_scope.journal_run_id == parent_scope.journal_run_id
            || child.session_id() == parent.session_id()
            || !self
                .child
                .registry
                .delegated
                .is_read_only_subset_of(&self.parent.registry.delegated)
            || self.child.limits.model_timeout > self.parent.limits.model_timeout
            || self.child.limits.tool_timeout > self.parent.limits.tool_timeout
            || self.child.limits.output_tokens_per_run > self.parent.limits.output_tokens_per_run
            || self.child.limits.output_tokens_per_request
                > self.parent.limits.output_tokens_per_request
            || self.child.limits.model_requests > self.parent.limits.model_requests
            || self.task.trim().is_empty()
            || self.task.len() > 8192
        {
            return Err(Error::Config(
                "invalid scope or permissions for an AIP child review".into(),
            ));
        }
        // A child journal belongs to one review run. A second host run ID
        // must never turn a resume into a fresh submission to the model.
        if child.records().iter().any(|record| {
            matches!(&record.event, Event::HostRunBound { run_id, .. }
                if *run_id != child_scope.journal_run_id)
        }) {
            return Err(Error::Session(
                "child review journal belongs to a different run".into(),
            ));
        }
        let child_id = child.session_id();
        let link = match ReviewContext::find(parent, child_id)? {
            Some(link)
                if self.resume
                    && link.task == self.task
                    && link.parent_record == self.from_record =>
            {
                link
            }
            Some(_) => return Err(Error::Session("AIP child review already exists".into())),
            None if !self.resume
                && !child.records().iter().any(|record| record.run_id.is_some()) =>
            {
                ReviewLink {
                    child: child_id,
                    parent_record: self.from_record,
                    task: self.task.into(),
                    status: ReviewStatus::Running,
                }
            }
            None => return Err(Error::Session("missing AIP child review binding".into())),
        };
        self.parent.check_profile(parent)?;
        self.child.check_profile(child)?;
        if let ReviewStatus::Done { answer } = link.status {
            return Ok(answer);
        }
        let snapshot = serde_json::to_string(&parent.conversation_at(self.from_record)?)?;
        if snapshot.len() > 64 * 1024 {
            return Err(Error::Config("review snapshot is too large".into()));
        }
        let input = (!child
            .records()
            .iter()
            .any(|record| record.run_id == Some(child_scope.journal_run_id)))
        .then(|| {
            format!(
                "Independent review task:\n{}\n\nParent conversation as data:\n{snapshot}",
                self.task
            )
        });
        self.parent.extension_command(
            parent,
            ID,
            "record",
            &serde_json::to_string(&link)?,
            observer,
        )?;
        let (_, mut inputs) = mpsc::channel(1);
        let token = cancel.child_token();
        let _guard = token.clone().drop_guard();
        let result = self
            .child
            .run(child, input, &mut inputs, &token, observer)
            .await;
        let status = match &result {
            Ok(answer) => ReviewStatus::Done {
                answer: answer.chars().take(8192).collect(),
            },
            Err(_) => ReviewStatus::Stopped,
        };
        self.parent.extension_command(
            parent,
            ID,
            "record",
            &serde_json::to_string(&ReviewLink { status, ..link })?,
            observer,
        )?;
        result
    }
}
