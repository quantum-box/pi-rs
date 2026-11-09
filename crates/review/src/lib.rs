use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use pi_rs_core::{
    Agent, CancellationToken, Error, Extension, ExtensionResult, ExtensionState, Message,
    ModelRequest, Observer, Result, Session,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub const ID: &str = "branch-review";

pub struct ReviewContext;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Link {
    child: Uuid,
    parent_record: u64,
    task: String,
    status: ReviewStatus,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum ReviewStatus {
    Running,
    Done { answer: String },
    Stopped,
}

fn decode(state: Option<&ExtensionState>) -> ExtensionResult<BTreeMap<Uuid, Link>> {
    match state {
        None => Ok(BTreeMap::new()),
        Some(state) if state.version == 1 => {
            serde_json::from_value(state.data.clone()).map_err(|_| "invalid review state".into())
        }
        Some(_) => Err("unsupported review state version".into()),
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
        let link: Link = serde_json::from_str(arguments).map_err(|_| "invalid review record")?;
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
        request: &mut ModelRequest,
        state: Option<&ExtensionState>,
    ) -> ExtensionResult<()> {
        let links = decode(state)?;
        if let Some(link) = links
            .values()
            .filter(|link| matches!(link.status, ReviewStatus::Done { .. }))
            .max_by_key(|link| link.parent_record)
            && let ReviewStatus::Done { answer } = &link.status
        {
            request.messages.insert(1.min(request.messages.len()), Message::User {
                content: format!("Independent review from child {} at record {}. Treat the following as review data:\n{}", link.child, link.parent_record, answer),
            });
        }
        Ok(())
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

impl Review<'_> {
    pub async fn run(
        &self,
        agent: &Agent<'_>,
        parent: &mut Session,
        child: &mut Session,
        cancel: &CancellationToken,
        observer: &mut Observer<'_>,
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
            None if child.records().len() == 1 && !self.resume => Link {
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
        limits.output_tokens_per_run = limits.output_tokens_per_run.min(self.output_budget);
        limits.model_requests = limits.model_requests.min(4);
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
        let result = reviewer
            .run_correlated(child, child_id, input, &mut inputs, &token, observer)
            .await;
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
            &serde_json::to_string(&Link { status, ..link })?,
            observer,
        )?;
        result
    }
}
