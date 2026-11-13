//! Pi-compatible Bedrock request-header filtering.
use std::collections::BTreeMap;

pub(crate) fn filtered(headers: http::HeaderMap) -> pi_rs_core::Result<BTreeMap<String, String>> {
    let mut result = BTreeMap::new();
    for (name, value) in headers.iter() {
        let name = name.as_str();
        if name == "authorization" || name == "host" || name.starts_with("x-amz-") {
            continue;
        }
        let value = value
            .to_str()
            .map_err(|_| pi_rs_core::Error::Config("invalid Bedrock custom header value".into()))?;
        result.insert(name.to_owned(), value.to_owned());
    }
    Ok(result)
}

// SigV4 traces include the canonical request as a plain string, bypassing
// HTTP header sensitivity flags. Forward the caller's subscriber while
// filtering only those trace events during this request's signing future.
pub(crate) struct SigningTraceGuard(pub tracing::Dispatch);
impl tracing::Subscriber for SigningTraceGuard {
    fn register_callsite(
        &self,
        _: &'static tracing::Metadata<'static>,
    ) -> tracing::subscriber::Interest {
        tracing::subscriber::Interest::sometimes()
    }
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        !(metadata.is_event()
            && metadata.target().starts_with("aws_sigv4::")
            && *metadata.level() == tracing::Level::TRACE)
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
