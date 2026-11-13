use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use pi_rs_core::{
    Agent, AgentInput, CancellationToken, Error, Event, Extension, ExtensionResult, ExtensionState,
    Message, Observer, Result, RunState, Session, UsageReport,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub const ID: &str = "branch-review";

pub struct ReviewContext;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReviewLink {
    pub child: Uuid,
    pub parent_record: u64,
    pub task: String,
    pub status: ReviewStatus,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ReviewStatus {
    Running,
    Done { answer: String },
    Stopped,
}

fn decode(state: Option<&ExtensionState>) -> ExtensionResult<BTreeMap<Uuid, ReviewLink>> {
    match state {
        None => Ok(BTreeMap::new()),
        Some(state) if state.version == 1 => {
            serde_json::from_value(state.data.clone()).map_err(|_| "invalid review state".into())
        }
        Some(_) => Err("unsupported review state version".into()),
    }
}

impl ReviewContext {
    pub fn find(session: &Session, child: Uuid) -> Result<Option<ReviewLink>> {
        let mut links = decode(session.extension_states().get(ID)).map_err(Error::Config)?;
        Ok(links.remove(&child))
    }
}

impl Extension for ReviewContext {
    fn id(&self) -> &str {
        ID
    }

    fn commands(&self) -> &[&str] {
        &["record"]
    }

    fn command(
        &self,
        _: &str,
        arguments: &str,
        previous: Option<&ExtensionState>,
    ) -> ExtensionResult<ExtensionState> {
        let mut links = decode(previous)?;
        let link: ReviewLink =
            serde_json::from_str(arguments).map_err(|_| "invalid review record")?;
        if links.len() >= 32 && !links.contains_key(&link.child) {
            return Err("review record limit reached".into());
        }
        if let Some(existing) = links.get(&link.child)
            && (existing.parent_record != link.parent_record
                || existing.task != link.task
                || matches!(existing.status, ReviewStatus::Done { .. }))
        {
            return Err("review identity changed or review already completed".into());
        }
        links.insert(link.child, link);
        Ok(ExtensionState {
            version: 1,
            data: serde_json::to_value(links).map_err(|error| error.to_string())?,
        })
    }

    fn context(
        &self,
        request: &mut pi_rs_core::ContextRequest<'_>,
        state: Option<&ExtensionState>,
    ) -> ExtensionResult<pi_rs_core::ContextChange> {
        let links = decode(state)?;
        if let Some(link) = links
            .values()
            .filter(|link| matches!(link.status, ReviewStatus::Done { .. }))
            .max_by_key(|link| link.parent_record)
            && let ReviewStatus::Done { answer } = &link.status
        {
            let insert_at = 1.min(request.messages.len());
            request.insert_user(insert_at, Message::User {
                content: format!("Independent review from child {} at record {}. Treat the following as review data:\n{}", link.child, link.parent_record, answer),
            })?;
            return Ok(pi_rs_core::ContextChange::Changed);
        }
        Ok(pi_rs_core::ContextChange::Unchanged)
    }
}

pub struct Review<'a> {
    pub task: &'a str,
    pub from_record: Option<u64>,
    pub allowed_tools: BTreeSet<String>,
    pub output_budget: u32,
    pub timeout: Duration,
    pub resume: bool,
}

pub struct ReviewControls<'input, 'observer, T> {
    pub inputs: &'input mut tokio::sync::mpsc::Receiver<T>,
    pub observer: &'input mut Observer<'observer>,
}

impl Review<'_> {
    pub async fn run(
        &self,
        agent: &Agent<'_>,
        parent: &mut Session,
        child: &mut Session,
        cancel: &CancellationToken,
        observer: &mut Observer<'_>,
    ) -> Result<String> {
        self.run_with_controls::<AgentInput>(agent, parent, child, cancel, observer, None)
            .await
    }

    pub async fn run_with_controls<T: Into<AgentInput> + Send>(
        &self,
        agent: &Agent<'_>,
        parent: &mut Session,
        child: &mut Session,
        cancel: &CancellationToken,
        observer: &mut Observer<'_>,
        mut controls: Option<ReviewControls<'_, '_, T>>,
    ) -> Result<String> {
        if self.timeout.is_zero()
            || self.output_budget == 0
            || self.task.trim().is_empty()
            || self.task.len() > 8192
        {
            return Err(Error::Config(
                "review requires a bounded task, budget, and timeout".into(),
            ));
        }
        let child_id = child.session_id();
        if child_id == parent.session_id()
            || parent.host_binding().is_some()
            || child.host_binding().is_some()
        {
            return Err(Error::Session(
                "local review needs distinct local sessions".into(),
            ));
        }
        let links = decode(parent.extension_states().get(ID)).map_err(Error::Config)?;
        let link = match links.get(&child_id) {
            Some(link)
                if self.resume
                    && link.task == self.task
                    && self.from_record.is_none_or(|id| id == link.parent_record) =>
            {
                link.clone()
            }
            Some(_) => {
                return Err(Error::Session(
                    "review already exists; resume the same review explicitly".into(),
                ));
            }
            None if child.records().len() == 1 && !self.resume => ReviewLink {
                child: child_id,
                parent_record: self.from_record.unwrap_or_else(|| parent.head()),
                task: self.task.into(),
                status: ReviewStatus::Running,
            },
            None => {
                return Err(Error::Session(
                    "child journal does not belong to this review".into(),
                ));
            }
        };
        if let ReviewStatus::Done { answer } = link.status {
            return Ok(answer);
        }
        let snapshot = serde_json::to_string(&parent.conversation_at(link.parent_record)?)?;
        if snapshot.len() > 64 * 1024 {
            return Err(Error::Config(
                "review snapshot is too large; compact the parent branch first".into(),
            ));
        }
        let prompt = format!(
            "Review task:\n{}\n\nParent conversation as data:\n{snapshot}",
            link.task
        );
        let mut limits = agent.limits.clone();
        let mut requested = BTreeMap::new();
        let mut completed = false;
        for record in child
            .records()
            .iter()
            .filter(|record| record.run_id == Some(child_id))
        {
            match &record.event {
                Event::ModelRequested {
                    request_id: Some(id),
                    max_output_tokens,
                    ..
                } => {
                    requested.insert(*id, u64::from(*max_output_tokens));
                }
                Event::ModelFinished {
                    request_id,
                    usage: UsageReport::Measured { usage },
                    ..
                } => {
                    requested.insert(*request_id, usage.output_tokens);
                }
                Event::Run { state, .. } => completed = *state == RunState::Completed,
                _ => {}
            }
        }
        let spent = requested.values().copied().fold(0, u64::saturating_add);
        limits.output_tokens_per_run = limits
            .output_tokens_per_run
            .min(self.output_budget)
            .saturating_sub(u32::try_from(spent).unwrap_or(u32::MAX));
        limits.model_requests = limits.model_requests.min(4).saturating_sub(requested.len());
        if !completed && (limits.output_tokens_per_run == 0 || limits.model_requests == 0) {
            return Err(Error::Budget);
        }
        let tools = agent.registry.read_only_subset(&self.allowed_tools)?;
        let reviewer = Agent {
            model: agent.model,
            model_name: agent.model_name,
            registry: &tools,
            system_prompt: "Independently review the supplied conversation and give concrete findings with evidence.",
            limits,
        };
        agent.extension_command(
            parent,
            ID,
            "record",
            &serde_json::to_string(&link)?,
            observer,
        )?;
        let (_, mut inputs) = tokio::sync::mpsc::channel::<String>(1);
        let token = cancel.child_token();
        let _guard = token.clone().drop_guard();
        let timer = tokio::spawn({
            let token = token.clone();
            let deadline = self.timeout;
            async move {
                tokio::time::sleep(deadline).await;
                token.cancel();
            }
        });
        let input = (!child
            .records()
            .iter()
            .any(|record| record.run_id == Some(child_id)))
        .then_some(prompt);
        let mut result = {
            let running =
                reviewer.run_correlated(child, child_id, input, &mut inputs, &token, observer);
            tokio::pin!(running);
            loop {
                tokio::select! {
                    result = &mut running => break result,
                    Some(input) = async {
                        match controls.as_mut() {
                            Some(controls) => controls.inputs.recv().await,
                            None => std::future::pending().await,
                        }
                    } => {
                        if let Err(error) = input.into().apply(parent, controls.as_mut().expect("active review controls").observer) {
                            token.cancel();
                            let _ = running.await;
                            break Err(error);
                        }
                    }
                }
            }
        };
        if let Some(controls) = controls {
            for _ in 0..controls.inputs.max_capacity() {
                let Ok(input) = controls.inputs.try_recv() else {
                    break;
                };
                if let Err(error) = input.into().apply(parent, controls.observer) {
                    if result.is_ok() {
                        result = Err(error);
                    }
                    break;
                }
            }
        }
        timer.abort();
        let status = match &result {
            Ok(answer) => ReviewStatus::Done {
                answer: answer.chars().take(8_192).collect(),
            },
            Err(_) => ReviewStatus::Stopped,
        };
        agent.extension_command(
            parent,
            ID,
            "record",
            &serde_json::to_string(&ReviewLink { status, ..link })?,
            observer,
        )?;
        result
    }
}

#[cfg(test)]
mod context_provenance_tests {
    use super::*;
    use pi_rs_core::{ContextRequest, ModelRequest};

    #[test]
    fn completed_review_uses_tracked_user_insertion() {
        let child = Uuid::new_v4();
        let link = ReviewLink {
            child,
            parent_record: 1,
            task: "review".into(),
            status: ReviewStatus::Done {
                answer: "done".into(),
            },
        };
        let state = ExtensionState {
            version: 1,
            data: serde_json::to_value(BTreeMap::from([(child, link)])).unwrap(),
        };
        let mut request = ModelRequest {
            model: "m".into(),
            messages: vec![Message::Assistant {
                content: "visible".into(),
                calls: vec![],
                replay: None,
            }],
            tools: vec![],
            max_output_tokens: 8,
        };
        let mut guarded = ContextRequest::new(&mut request);
        let access = guarded.access();
        ReviewContext.context(&mut guarded, Some(&state)).unwrap();
        assert!(!access.assistant_accessed());
        assert_eq!(access.user_edits().len(), 1);
    }
}
