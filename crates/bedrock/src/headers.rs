//! Pi-compatible Bedrock request-header filtering.
use std::collections::BTreeMap;

pub(crate) fn filtered(
    headers: http::HeaderMap,
) -> pi_rs_core::Result<BTreeMap<String, Vec<String>>> {
    let mut result: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (name, value) in headers.iter() {
        let name = name.as_str();
        if name == "authorization" || name == "host" || name.starts_with("x-amz-") {
            continue;
        }
        let value = value
            .to_str()
            .map_err(|_| pi_rs_core::Error::Config("invalid Bedrock custom header value".into()))?;
        result
            .entry(name.to_owned())
            .or_default()
            .push(value.to_owned());
    }
    Ok(result)
}

/// Replace only the SDK header map, retaining request extensions of either HTTP version.
pub(crate) fn apply(
    request: &mut aws_smithy_runtime_api::client::orchestrator::HttpRequest,
    headers: &BTreeMap<String, Vec<String>>,
) {
    if headers.is_empty() {
        return;
    }
    let mut map = http::HeaderMap::new();
    // The Smithy iterator does not expose existing sensitivity flags, so mark
    // every retained value sensitive when rebuilding the map.
    for (name, value) in request.headers().iter() {
        let mut value = http::HeaderValue::from_str(value).expect("SDK header value");
        value.set_sensitive(true);
        map.append(
            http::HeaderName::from_bytes(name.as_bytes()).expect("SDK header name"),
            value,
        );
    }
    for (name, values) in headers {
        let name = http::HeaderName::from_bytes(name.as_bytes()).expect("validated header name");
        map.remove(&name);
        for value in values {
            let mut value = http::HeaderValue::from_str(value).expect("validated header value");
            value.set_sensitive(true);
            map.append(name.clone(), value);
        }
    }
    *request.headers_mut() =
        aws_smithy_runtime_api::http::Headers::try_from(map).expect("validated headers");
}

// SDK signers, deserializers and event-stream decoders can log secrets in
// plain strings or raw frames. Filter SDK and HTTP transport events only for
// this request and stream while forwarding application events.
pub(crate) struct SigningTraceGuard(pub tracing::Dispatch);
impl tracing::Subscriber for SigningTraceGuard {
    fn register_callsite(
        &self,
        _: &'static tracing::Metadata<'static>,
    ) -> tracing::subscriber::Interest {
        tracing::subscriber::Interest::sometimes()
    }
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        let target = metadata.target();
        !(metadata.is_event()
            && (target.starts_with("aws_")
                || target == "hyper"
                || target.starts_with("hyper::")
                || target.starts_with("hyper_util::")
                || target == "h2"
                || target.starts_with("h2::")))
            && self.0.enabled(metadata)
    }
    fn new_span(&self, attributes: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        self.0.new_span(attributes)
    }
    fn record(&self, id: &tracing::span::Id, values: &tracing::span::Record<'_>) {
        self.0.record(id, values)
    }
    fn record_follows_from(&self, id: &tracing::span::Id, follows: &tracing::span::Id) {
        self.0.record_follows_from(id, follows)
    }
    fn event(&self, event: &tracing::Event<'_>) {
        if self.enabled(event.metadata()) {
            self.0.event(event)
        }
    }
    fn enter(&self, id: &tracing::span::Id) {
        self.0.enter(id)
    }
    fn exit(&self, id: &tracing::span::Id) {
        self.0.exit(id)
    }
    fn clone_span(&self, id: &tracing::span::Id) -> tracing::span::Id {
        self.0.clone_span(id)
    }
    fn try_close(&self, id: tracing::span::Id) -> bool {
        self.0.try_close(id)
    }
    fn current_span(&self) -> tracing_core::span::Current {
        self.0.current_span()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn repeated_custom_values_replace_all_existing_values() {
        let mut original = http::Request::new(aws_smithy_types::body::SdkBody::empty());
        original
            .headers_mut()
            .append("x-custom", http::HeaderValue::from_static("sdk-first"));
        original
            .headers_mut()
            .append("x-custom", http::HeaderValue::from_static("sdk-second"));
        let mut request = aws_smithy_runtime_api::http::Request::try_from(original).unwrap();
        let mut headers = http::HeaderMap::new();
        headers.append("x-custom", http::HeaderValue::from_static("first"));
        headers.append("x-custom", http::HeaderValue::from_static("second"));
        headers.append("authorization", http::HeaderValue::from_static("ignored"));
        let filtered = filtered(headers).unwrap();
        apply(&mut request, &filtered);
        let result = request.try_into_http1x().unwrap();
        let values: Vec<_> = result.headers().get_all("x-custom").iter().collect();
        assert_eq!(
            values
                .iter()
                .map(|value| value.to_str().unwrap())
                .collect::<Vec<_>>(),
            ["first", "second"]
        );
        assert!(values.iter().all(|value| value.is_sensitive()));
        assert!(!result.headers().contains_key("authorization"));
    }

    #[test]
    fn preserves_http02_extensions_with_and_without_custom_headers() {
        for custom in [false, true] {
            let mut original = http02::Request::new(aws_smithy_types::body::SdkBody::empty());
            original
                .extensions_mut()
                .insert(String::from("retained-02"));
            original
                .headers_mut()
                .append("x-existing", http02::HeaderValue::from_static("one"));
            original
                .headers_mut()
                .append("x-existing", http02::HeaderValue::from_static("two"));
            let mut request = aws_smithy_runtime_api::http::Request::try_from(original).unwrap();
            let headers = if custom {
                [("x-custom".into(), vec!["private".into()])]
                    .into_iter()
                    .collect()
            } else {
                BTreeMap::new()
            };
            apply(&mut request, &headers);
            let result = request.try_into_http02x().unwrap();
            assert_eq!(
                result.extensions().get::<String>().map(String::as_str),
                Some("retained-02")
            );
            assert_eq!(result.headers().get_all("x-existing").iter().count(), 2);
            assert_eq!(result.headers().get("x-custom").is_some(), custom);
        }
    }
    #[test]
    fn preserves_http1_extensions_with_custom_headers() {
        let mut original = http::Request::new(aws_smithy_types::body::SdkBody::empty());
        original.extensions_mut().insert(String::from("retained-1"));
        let mut request = aws_smithy_runtime_api::http::Request::try_from(original).unwrap();
        apply(
            &mut request,
            &[("x-custom".into(), vec!["private".into()])]
                .into_iter()
                .collect(),
        );
        let result = request.try_into_http1x().unwrap();
        assert_eq!(
            result.extensions().get::<String>().map(String::as_str),
            Some("retained-1")
        );
        assert!(result.headers()["x-custom"].is_sensitive());
    }
}
