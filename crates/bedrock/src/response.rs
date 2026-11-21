//! Snapshot the final Smithy response without touching its event-stream body.
use std::{
    fmt,
    sync::{Arc, Mutex},
};

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

#[derive(Clone, Default)]
pub(crate) struct Capture(Arc<Mutex<Attempt>>);

#[derive(Default)]
struct Attempt {
    request_headers: HeaderMap,
    response: Option<http::Response<()>>,
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
        let request_headers =
            copy_metadata(http::StatusCode::OK.into(), context.request().headers())
                .map(|response| response.into_parts().0.headers)
                .unwrap_or_default();
        *self.0.lock().expect("Bedrock response capture") = Attempt {
            request_headers,
            response: None,
        };
        Ok(())
    }

    fn read_after_transmit(
        &self,
        context: &BeforeDeserializationInterceptorContextRef<'_>,
        _: &RuntimeComponents,
        _: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let response = context.response();
        self.0.lock().expect("Bedrock response capture").response =
            copy_metadata(response.status(), response.headers());
        Ok(())
    }
}

impl Capture {
    pub(crate) async fn notify(self, configured_secrets: &[String]) {
        let Attempt {
            request_headers,
            response,
        } = std::mem::take(&mut *self.0.lock().expect("Bedrock response capture"));
        let Some(response) = response else { return };
        let mut secrets = configured_secrets.to_vec();
        if let Some(token) = request_headers
            .get("x-amz-security-token")
            .and_then(|value| value.to_str().ok())
        {
            secrets.push(token.to_owned());
        }
        if let Some(auth) = request_headers
            .get("authorization")
            .and_then(|value| value.to_str().ok())
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
        pi_rs_core::observe_provider_response(
            response.status(),
            response.headers(),
            &secrets,
            &request_headers,
        )
        .await;
    }
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
    fn snapshot_preserves_sensitivity_from_both_smithy_http_versions() {
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
