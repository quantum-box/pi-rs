//! Ordered mode applications and settings writes for the RPC worker.
use std::sync::Arc;

use pi_rs::{AgentInput, Error, InputKind, QueueMode, Result};
use serde_json::{Value, json};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};

use super::{control_response, output};

enum Submission {
    Apply(Applied),
}
struct Applied {
    kind: InputKind,
    mode: QueueMode,
    id: Option<Value>,
    reply: oneshot::Receiver<Result<()>>,
    permit: OwnedSemaphorePermit,
}
struct Write {
    kind: InputKind,
    mode: QueueMode,
    id: Option<Value>,
    result: Result<()>,
    _permit: OwnedSemaphorePermit,
}

#[derive(Clone)]
pub(super) struct Coordinator {
    input: mpsc::Sender<AgentInput>,
    submissions: mpsc::Sender<Submission>,
    slots: Arc<Semaphore>,
}

impl Coordinator {
    pub fn matches(&self, input: &mpsc::Sender<AgentInput>) -> bool {
        self.input.same_channel(input)
    }
    pub fn start(input: mpsc::Sender<AgentInput>) -> (Self, tokio::task::JoinHandle<()>) {
        let (submissions, mut accepted) = mpsc::channel::<Submission>(32);
        let (writes, mut writer) = mpsc::unbounded_channel::<Write>();
        let task = tokio::spawn(async move {
            let writing = tokio::spawn(async move {
                while let Some(write) = writer.recv().await {
                    let result = match write.result {
                        Ok(()) => tokio::task::spawn_blocking(move || {
                            let key = if write.kind == InputKind::Steer {
                                "steeringMode"
                            } else {
                                "followUpMode"
                            };
                            crate::config::update_object(
                                &crate::config::agent_dir().join("settings.json"),
                                |settings| {
                                    settings.insert(key.into(), json!(write.mode));
                                    Ok(())
                                },
                            )
                        })
                        .await
                        .map_err(|error| Error::Config(error.to_string()))
                        .and_then(|result| result),
                        Err(error) => Err(error),
                    };
                    control_response(command(write.kind), write.id, result.map(|()| Value::Null));
                }
            });
            while let Some(submission) = accepted.recv().await {
                let Submission::Apply(request) = submission;
                let result = request
                    .reply
                    .await
                    .map_err(|_| Error::Config("queue control channel closed".into()))
                    .and_then(|result| result);
                if result.is_ok() {
                    output(
                        json!({"type":"internal_queue_mode_applied", "id":request.id, "kind":request.kind, "mode":request.mode}),
                    );
                }
                let _ = writes.send(Write {
                    kind: request.kind,
                    mode: request.mode,
                    id: request.id,
                    result,
                    _permit: request.permit,
                });
            }
            drop(writes);
            let _ = writing.await;
        });
        (
            Self {
                input,
                submissions,
                slots: Arc::new(Semaphore::new(32)),
            },
            task,
        )
    }
    pub fn submit(&self, kind: InputKind, mode: QueueMode, id: Option<Value>) -> Result<()> {
        let permit = self
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Config("queue control channel is full or closed".into()))?;
        let slot = self
            .submissions
            .try_reserve()
            .map_err(|_| Error::Config("queue control channel is full or closed".into()))?;
        let (reply, receiver) = oneshot::channel();
        self.input
            .try_send(AgentInput::SetQueueMode { kind, mode, reply })
            .map_err(|_| Error::Config("queue control channel is full or closed".into()))?;
        slot.send(Submission::Apply(Applied {
            kind,
            mode,
            id,
            reply: receiver,
            permit,
        }));
        Ok(())
    }
}
fn command(kind: InputKind) -> &'static str {
    if kind == InputKind::Steer {
        "set_steering_mode"
    } else {
        "set_follow_up_mode"
    }
}
