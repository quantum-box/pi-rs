//! Read-only tap for subscribers that need wire fields before Bedrock normalizes them.
use std::{
    collections::VecDeque,
    fmt,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use aws_smithy_eventstream::{
    frame::{DecodedFrame, MessageFrameDecoder},
    smithy::parse_response_headers,
};
use aws_smithy_runtime_api::{
    box_error::BoxError,
    client::{
        interceptors::{
            Intercept,
            context::{
                BeforeDeserializationInterceptorContextMut, BeforeTransmitInterceptorContextRef,
            },
        },
        runtime_components::RuntimeComponents,
    },
};
use aws_smithy_types::{body::SdkBody, config_bag::ConfigBag, event_stream::Message};
use bytes::{Bytes, BytesMut};
use http::HeaderMap;
use http_body::{Body, Frame, SizeHint};
use serde_json::{Map, Value};

use crate::response::{copy_request_headers, request_secrets};

#[derive(Clone, Default)]
pub(crate) struct Capture(Arc<Mutex<Attempt>>);

#[derive(Default)]
struct Attempt {
    headers: HeaderMap,
    frames: Option<Arc<Mutex<Frames>>>,
}

impl fmt::Debug for Capture {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("BedrockStreamCapture")
    }
}

impl Intercept for Capture {
    fn name(&self) -> &'static str {
        "PiBedrockStreamCapture"
    }

    fn read_before_transmit(
        &self,
        context: &BeforeTransmitInterceptorContextRef<'_>,
        _: &RuntimeComponents,
        _: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        *self.0.lock().expect("Bedrock stream capture") = Attempt {
            headers: copy_request_headers(context.request().headers()),
            frames: None,
        };
        Ok(())
    }

    fn modify_before_deserialization(
        &self,
        context: &mut BeforeDeserializationInterceptorContextMut<'_>,
        _: &RuntimeComponents,
        _: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let response = context.response_mut();
        if response.status().is_success() {
            let frames = Arc::new(Mutex::new(Frames::default()));
            self.0.lock().expect("Bedrock stream capture").frames = Some(frames.clone());
            *response.body_mut() = response.take_body().map_preserve_contents(move |body| {
                SdkBody::from_body_1_x(Tap {
                    body,
                    frames: frames.clone(),
                })
            });
        }
        Ok(())
    }
}

impl Capture {
    pub(crate) fn finish(self, configured_secrets: &[String]) -> Option<Observation> {
        let Attempt { headers, frames } =
            std::mem::take(&mut *self.0.lock().expect("Bedrock stream capture"));
        Some(Observation {
            frames: frames?,
            secrets: request_secrets(configured_secrets, &headers),
            headers,
        })
    }
}

pub(crate) struct Observation {
    frames: Arc<Mutex<Frames>>,
    secrets: Vec<String>,
    headers: HeaderMap,
}

impl Observation {
    pub(crate) async fn notify_next(&self, model: &str) {
        let frame = self.frames.lock().expect("Bedrock stream frames").pop();
        let Some(data) = frame.and_then(WireFrame::into_value) else {
            return;
        };
        pi_rs_core::observe_provider_stream(
            "amazon-bedrock",
            pi_rs_core::ModelApi::BedrockConverseStream,
            model,
            &data,
            &self.secrets,
            &self.headers,
        )
        .await;
    }
}

struct WireFrame {
    tag: String,
    payload: Bytes,
}

impl WireFrame {
    fn from_message(message: Message) -> Option<Self> {
        let tag = parse_response_headers(&message).ok()?.smithy_type.as_str();
        Some(Self {
            tag: tag.to_owned(),
            payload: message.payload().clone(),
        })
    }

    fn size(&self) -> usize {
        self.tag.len().saturating_add(self.payload.len())
    }

    fn into_value(self) -> Option<Value> {
        let payload = serde_json::from_slice(&self.payload).ok()?;
        Some(Value::Object(Map::from_iter([(self.tag, payload)])))
    }
}

fn frame_header<'a>(message: &'a Message, name: &str) -> Option<&'a str> {
    message
        .headers()
        .iter()
        .find(|header| header.name().as_str() == name)
        .and_then(|header| header.value().as_string().ok())
        .map(|value| value.as_str())
}

struct Tap {
    body: SdkBody,
    frames: Arc<Mutex<Frames>>,
}

impl Body for Tap {
    type Data = Bytes;
    type Error = aws_smithy_types::body::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        let this = self.get_mut();
        let frame = Pin::new(&mut this.body).poll_frame(cx);
        if let Poll::Ready(Some(Ok(frame))) = &frame
            && let Some(data) = frame.data_ref()
        {
            this.frames
                .lock()
                .expect("Bedrock stream frames")
                .push(data);
        }
        frame
    }

    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.body.size_hint()
    }
}

// Bound only the observer's buffering. The AWS receiver still sees each original
// byte and remains responsible for checksums, limits and errors.
const MAX_PENDING_BYTES: usize = 17 * 1024 * 1024;
const MAX_PENDING_FRAMES: usize = 4096;

#[derive(Default)]
struct Frames {
    bytes: BytesMut,
    decoder: MessageFrameDecoder,
    ready: VecDeque<Option<WireFrame>>,
    queued_bytes: usize,
    saw_first_frame: bool,
    disabled: bool,
}

impl Frames {
    fn pop(&mut self) -> Option<WireFrame> {
        let frame = self.ready.pop_front().flatten()?;
        self.queued_bytes -= frame.size();
        Some(frame)
    }

    fn push(&mut self, bytes: &[u8]) {
        if self.disabled {
            return;
        }
        if self.bytes.len().saturating_add(bytes.len()) <= MAX_PENDING_BYTES {
            self.bytes.extend_from_slice(bytes);
            loop {
                match self.decoder.decode_frame(&mut self.bytes) {
                    Ok(DecodedFrame::Incomplete) => return,
                    Ok(DecodedFrame::Complete(message)) => {
                        let first = !std::mem::replace(&mut self.saw_first_frame, true);
                        // Smithy consumes a first initial-response in send(),
                        // before returning the stream that recv() reads.
                        if first
                            && frame_header(&message, ":event-type") == Some("initial-response")
                        {
                            continue;
                        }
                        let frame = WireFrame::from_message(message);
                        let next = self
                            .queued_bytes
                            .saturating_add(frame.as_ref().map_or(0, WireFrame::size));
                        if self.ready.len() == MAX_PENDING_FRAMES || next > MAX_PENDING_BYTES {
                            break;
                        }
                        self.queued_bytes = next;
                        self.ready.push_back(frame);
                    }
                    Err(_) => break,
                }
            }
        }
        // Later events cannot be aligned after missing a frame.
        self.bytes = BytesMut::new();
        self.disabled = true;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_smithy_eventstream::frame::write_message_to;
    use aws_smithy_types::event_stream::{Header, HeaderValue};

    #[test]
    fn wire_observer_preserves_valid_frames_with_any_chunk_width() {
        let mut bytes = Vec::new();
        let mut encode = |message| write_message_to(&message, &mut bytes).unwrap();
        encode(
            Message::new(Bytes::from_static(b"{}")).add_header(Header::new(
                ":event-type",
                HeaderValue::String("initial-response".into()),
            )),
        );
        encode(
            Message::new(Bytes::from_static(br#"{"role":"assistant"}"#))
                .add_header(Header::new(
                    ":message-type",
                    HeaderValue::String("event".into()),
                ))
                .add_header(Header::new(
                    ":event-type",
                    HeaderValue::String("overridden".into()),
                ))
                .add_header(Header::new(
                    ":event-type",
                    HeaderValue::String("messageStart".into()),
                )),
        );
        encode(
            Message::new(Bytes::from_static(br#"{"message":"busy"}"#))
                .add_header(Header::new(
                    ":message-type",
                    HeaderValue::String("exception".into()),
                ))
                .add_header(Header::new(
                    ":exception-type",
                    HeaderValue::String("throttlingException".into()),
                )),
        );
        encode(Message::new(Bytes::new()));
        *bytes.last_mut().unwrap() ^= 1;

        for width in 1..=bytes.len() {
            let mut observed = Frames::default();
            for chunk in bytes.chunks(width) {
                observed.push(chunk);
            }
            assert_eq!(
                observed.pop().and_then(WireFrame::into_value),
                Some(serde_json::json!({"messageStart": {"role": "assistant"}})),
            );
            assert_eq!(
                observed.pop().and_then(WireFrame::into_value),
                Some(serde_json::json!({"throttlingException": {"message": "busy"}})),
            );
            assert!(observed.pop().is_none());
            assert!(observed.disabled);
        }
    }
}
