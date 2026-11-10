//! Prepare queue submissions in order without blocking cancellation or extension UI replies.
use std::sync::{Arc, Mutex};

use pi_rs::{
    AgentInput, Error, InputEvent, InputKind, InputSource, QueueMode, QueuedInput, QueuedMessage,
    Registry, Result, Session,
};
use serde_json::{Value, json};
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

use super::{ControlState, Request, SessionSnapshot, control_response, output};

pub(super) struct Processor {
    pub commands: mpsc::Sender<Request>,
    pub inputs: mpsc::Sender<AgentInput>,
    pub active: Arc<Mutex<ControlState>>,
    pub snapshot: Arc<Mutex<SessionSnapshot>>,
    pub resources: crate::resources::Resources,
    pub registry: Registry,
}

impl Processor {
    pub async fn run(self: Arc<Self>, mut requests: mpsc::Receiver<Request>) {
        while let Some(request) = requests.recv().await {
            self.handle(request).await;
        }
    }

    pub async fn handle(&self, request: Request) {
        let steering = matches!(&request, Request::Steer { .. });
        match request {
            Request::Steer { content, images } | Request::Followup { content, images } => {
                let message = QueuedMessage {
                    input: QueuedInput {
                        id: Uuid::new_v4(),
                        kind: if steering {
                            InputKind::Steer
                        } else {
                            InputKind::FollowUp
                        },
                        content,
                    },
                    images,
                };
                let result = async {
                    if let Some(message) = self.prepare(message, InputSource::Interactive).await? {
                        self.inputs
                            .try_send(message.into())
                            .map_err(|_| queue_full())?;
                    }
                    Ok::<(), Error>(())
                }
                .await;
                if let Err(error) = result {
                    output(json!({"type":"input_rejected","reason":error.to_string()}));
                }
            }
            Request::QueueInput {
                message,
                command,
                id,
                ..
            } => match self.prepare(message, InputSource::Rpc).await {
                Ok(Some(message)) => self.queue_rpc(message, command, id),
                Ok(None) => control_response(&command, id, Ok(json!({"disposition":"handled"}))),
                Err(error) => control_response(&command, id, Err(error)),
            },
            Request::ClearQueue { id } => self.clear(id),
            Request::SetQueueMode { kind, mode, id } => self.set_mode(kind, mode, id),
            _ => unreachable!("only queue requests are routed to this task"),
        }
    }

    async fn prepare(
        &self,
        mut message: QueuedMessage,
        source: InputSource,
    ) -> Result<Option<QueuedMessage>> {
        if let Some(name) = message
            .input
            .content
            .strip_prefix('/')
            .and_then(|text| text.split_whitespace().next())
            && self
                .registry
                .registered_commands()
                .iter()
                .any(|command| command.invocation_name == name)
        {
            return Err(Error::Config(
                "an extension command cannot be queued".into(),
            ));
        }
        if self.registry.has_input_hooks() {
            let streaming = {
                let active = self.active.lock().expect("worker control");
                active.token.is_some() || active.queued > 0
            };
            let records = self
                .snapshot
                .lock()
                .expect("session snapshot")
                .records
                .clone();
            let states = Session::from_records(records)?.extension_states();
            let event = InputEvent {
                text: message.input.content,
                images: message.images,
                source,
                streaming_behavior: streaming.then_some(message.input.kind),
            };
            let Some(prepared) = self.registry.prepare_input(event, &states).await? else {
                return Ok(None);
            };
            message.input.content = prepared.text;
            message.images = prepared.images;
        }
        if let Some(expanded) = self.resources.expand(&message.input.content)? {
            message.input.content = expanded;
        }
        message.validate()?;
        Ok(Some(message))
    }

    fn queue_rpc(&self, message: QueuedMessage, command: String, id: Option<Value>) {
        let delivery = {
            let state = self.active.lock().expect("worker control");
            if state.token.is_some() || state.queued > 0 {
                let (reply, receiver) = oneshot::channel();
                self.inputs
                    .try_send(AgentInput::Enqueue { message, reply })
                    .map(|()| Some(receiver))
                    .map_err(|_| queue_full())
            } else {
                self.commands
                    .try_send(Request::QueueInput {
                        message,
                        command: command.clone(),
                        id: id.clone(),
                    })
                    .map(|()| None)
                    .map_err(|_| queue_full())
            }
        };
        match delivery {
            Ok(Some(receiver)) => {
                tokio::spawn(async move {
                    let result = receiver
                        .await
                        .map_err(|_| channel_closed())
                        .and_then(|result| result)
                        .map(|()| json!({"disposition":"queued"}));
                    control_response(&command, id, result);
                });
            }
            Ok(None) => {}
            Err(error) => control_response(&command, id, Err(error)),
        }
    }

    fn clear(&self, id: Option<Value>) {
        let delivery = {
            let state = self.active.lock().expect("worker control");
            if state.token.is_none() && state.queued == 0 {
                self.commands
                    .try_send(Request::ClearQueue { id: id.clone() })
                    .map(|()| None)
                    .map_err(|_| queue_full())
            } else {
                let (reply, receiver) = oneshot::channel();
                self.inputs
                    .try_send(AgentInput::ClearQueue { reply })
                    .map(|()| Some(receiver))
                    .map_err(|_| queue_full())
            }
        };
        match delivery {
            Ok(Some(receiver)) => {
                tokio::spawn(async move {
                    let result = receiver
                        .await
                        .map_err(|_| channel_closed())
                        .and_then(|result| result)
                        .map(clear_payload);
                    control_response("clear_queue", id, result);
                });
            }
            Ok(None) => {}
            Err(error) => control_response("clear_queue", id, Err(error)),
        }
    }

    fn set_mode(&self, kind: InputKind, mode: QueueMode, id: Option<Value>) {
        let (reply, receiver) = oneshot::channel();
        let _ = self
            .inputs
            .try_send(AgentInput::SetQueueMode { kind, mode, reply });
        tokio::spawn(async move {
            let result = async {
                receiver.await.map_err(|_| channel_closed())??;
                tokio::task::spawn_blocking(move || {
                    let key = if kind == InputKind::Steer {
                        "steeringMode"
                    } else {
                        "followUpMode"
                    };
                    crate::config::update_object(
                        &crate::config::agent_dir().join("settings.json"),
                        |settings| {
                            settings.insert(key.into(), json!(mode));
                            Ok(())
                        },
                    )
                })
                .await
                .map_err(|error| Error::Config(error.to_string()))??;
                Ok(json!({"mode":mode}))
            }
            .await;
            control_response(mode_command(kind), id, result);
        });
    }
}

pub(super) fn clear_payload(messages: Vec<QueuedMessage>) -> Value {
    let steering: Vec<_> = messages
        .iter()
        .filter(|message| message.input.kind == InputKind::Steer)
        .map(|message| message.input.content.as_str())
        .collect();
    let follow_up: Vec<_> = messages
        .iter()
        .filter(|message| message.input.kind == InputKind::FollowUp)
        .map(|message| message.input.content.as_str())
        .collect();
    json!({"steering":steering,"followUp":follow_up,"messages":messages})
}

pub(super) fn reject(request: Request) {
    match request {
        Request::QueueInput { command, id, .. } => {
            control_response(&command, id, Err(queue_full()))
        }
        Request::ClearQueue { id } => control_response("clear_queue", id, Err(queue_full())),
        Request::SetQueueMode { kind, id, .. } => {
            control_response(mode_command(kind), id, Err(queue_full()))
        }
        _ => output(json!({"type":"input_rejected","reason":queue_full().to_string()})),
    }
}

fn mode_command(kind: InputKind) -> &'static str {
    if kind == InputKind::Steer {
        "set_steering_mode"
    } else {
        "set_follow_up_mode"
    }
}

fn queue_full() -> Error {
    Error::Config("queue control channel is full or closed".into())
}

fn channel_closed() -> Error {
    Error::Config("queue control channel closed".into())
}
