//! Snapshot the final Smithy response without touching its event-stream body.
use std::{fmt, future::Future, sync::Mutex};

use aws_smithy_runtime_api::{
    box_error::BoxError,
    client::{
        interceptors::{
            Intercept,
            context::{
                BeforeDeserializationInterceptorContextRef, BeforeTransmitInterceptorContextRef,
            },
        },
        runtime_components::RuntimeComponents,
    },
    http::{Headers, Response, StatusCode},
};
use aws_smithy_types::config_bag::ConfigBag;
use http::HeaderMap;
use tokio::sync::mpsc;

pub(crate) struct Capture {
    request_headers: Mutex<HeaderMap>,
    sender: mpsc::UnboundedSender<Observation>,
}

pub(crate) struct Observation {
    request_headers: HeaderMap,
    response: http::Response<()>,
}

impl Capture {
    pub(crate) fn channel() -> (Self, mpsc::UnboundedReceiver<Observation>) {
        let (sender, receiver) = mpsc::unbounded_channel();
        (
            Self {
                request_headers: Mutex::default(),
                sender,
            },
            receiver,
        )
    }
}

impl fmt::Debug for Capture {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("BedrockResponseCapture")
    }
}

impl Intercept for Capture {
    fn name(&self) -> &'static str {
        "PiBedrockResponseCapture"
    }

    fn read_before_transmit(
        &self,
        context: &BeforeTransmitInterceptorContextRef<'_>,
        _: &RuntimeComponents,
        _: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let request_headers = copy_request_headers(context.request().headers());
        *self
            .request_headers
            .lock()
            .expect("Bedrock request headers") = request_headers;
        Ok(())
    }

    fn read_after_transmit(
        &self,
        context: &BeforeDeserializationInterceptorContextRef<'_>,
        _: &RuntimeComponents,
        _: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let response = context.response();
        if response.status().is_success()
            && let Some(response) = copy_metadata(response.status(), response.headers())
        {
            let request_headers = std::mem::take(
                &mut *self
                    .request_headers
                    .lock()
                    .expect("Bedrock request headers"),
            );
            let _ = self.sender.send(Observation {
                request_headers,
                response,
            });
        }
        Ok(())
    }
}

pub(crate) async fn observe_while_sending<F: Future>(
    send: F,
    mut observations: mpsc::UnboundedReceiver<Observation>,
    configured_secrets: &[String],
) -> F::Output {
    tokio::pin!(send);
    // The SDK can wait on the event stream while resolving the operation.
    // Service the HTTP metadata as soon as Smithy supplies it.
    let output = loop {
        tokio::select! {
            biased;
            Some(metadata) = observations.recv() => metadata.notify(configured_secrets).await,
            output = &mut send => break output,
        }
    };
    // The SDK may have produced metadata and completed in the same poll.
    while let Ok(metadata) = observations.try_recv() {
        metadata.notify(configured_secrets).await;
    }
    output
}

impl Observation {
    async fn notify(self, configured_secrets: &[String]) {
        let Self {
            request_headers,
            response,
        } = self;
        let secrets = request_secrets(configured_secrets, &request_headers);
        pi_rs_core::observe_provider_response(
            response.status(),
            response.headers(),
            &secrets,
            &request_headers,
        )
        .await;
    }
}

pub(crate) fn request_secrets(
    configured_secrets: &[String],
    request_headers: &HeaderMap,
) -> Vec<String> {
    let mut secrets = configured_secrets.to_vec();
    secrets.extend(
        request_headers
            .get_all("x-amz-security-token")
            .iter()
            .filter_map(|value| value.to_str().ok().map(str::to_owned)),
    );
    for auth in request_headers
        .get_all("authorization")
        .iter()
        .filter_map(|value| value.to_str().ok())
    {
        secrets.push(auth.to_owned());
        if let Some((scheme, value)) = auth.split_once(' ') {
            if scheme.eq_ignore_ascii_case("bearer") {
                secrets.push(value.to_owned());
            } else if scheme == "AWS4-HMAC-SHA256" || scheme == "AWS4-ECDSA-P256-SHA256" {
                for parameter in value.split(',').map(str::trim) {
                    if let Some(signature) = parameter.strip_prefix("Signature=") {
                        secrets.push(signature.to_owned());
                    } else if let Some((key_id, _)) = parameter
                        .strip_prefix("Credential=")
                        .and_then(|credential| credential.split_once('/'))
                    {
                        secrets.push(key_id.to_owned());
                    }
                }
            }
        }
    }
    secrets
}

pub(crate) fn copy_request_headers(headers: &Headers) -> HeaderMap {
    copy_metadata(http::StatusCode::OK.into(), headers)
        .map(|response| response.into_parts().0.headers)
        .unwrap_or_default()
}

fn copy_metadata(status: StatusCode, headers: &Headers) -> Option<http::Response<()>> {
    // Converting the header iterator to a HeaderMap would discard sensitivity
    // flags. Convert a bodyless copy through Smithy's HTTP compatibility API.
    let mut metadata = Response::new(status, ());
    *metadata.headers_mut() = headers.clone();
    // Smithy keeps each header's original HTTP version internally. Its
    // cross-version conversion can preserve the bytes but drop sensitivity.
    let mut legacy = Response::new(status, ());
    *legacy.headers_mut() = headers.clone();
    let legacy = legacy.try_into_http02x().ok()?;
    let mut response = metadata.try_into_http1x().ok()?;
    for (name, value) in response.headers_mut() {
        if legacy
            .headers()
            .get_all(name.as_str())
            .iter()
            .any(|previous| previous.is_sensitive() && previous.as_bytes() == value.as_bytes())
        {
            value.set_sensitive(true);
        }
    }
    Some(response)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_response_snapshot_preserves_sensitivity_from_both_smithy_http_versions() {
        let make_legacy = || {
            let mut map = http02::HeaderMap::new();
            map.append("x-header", http02::HeaderValue::from_static("public"));
            let mut value = http02::HeaderValue::from_static("private");
            value.set_sensitive(true);
            map.append("x-header", value);
            Headers::try_from(map).unwrap()
        };
        let make_current = || {
            let mut map = http::HeaderMap::new();
            map.append("x-header", http::HeaderValue::from_static("public"));
            let mut value = http::HeaderValue::from_static("private");
            value.set_sensitive(true);
            map.append("x-header", value);
            Headers::try_from(map).unwrap()
        };
        for headers in [make_legacy(), make_current()] {
            let response = copy_metadata(http::StatusCode::OK.into(), &headers).unwrap();
            let mut values = response.headers().get_all("x-header").iter();
            assert!(!values.next().unwrap().is_sensitive());
            assert!(values.next().unwrap().is_sensitive());
        }
    }
}
