//! Explicit native provider clients for hosts that own endpoints and credentials.
use std::sync::Arc;

use pi_rs::{Error, HostAuthSnapshot, HostCredentialKind, Model, ModelCatalogBuilder, Result};

/// Provider protocol selected by the host. This is not inferred from a model or
/// from session history.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NativeModelApi {
    OpenAiChat,
    OpenAiResponses,
    ChatGptResponses,
    AnthropicMessages,
    GoogleGemini,
}

/// Construct clients only from the immutable credentials supplied by the host.
/// This does not read environment variables, discover models, run commands,
/// refresh OAuth, select endpoints, or perform network requests during creation.
#[derive(Clone, Default)]
pub struct ModelClientFactory {
    credentials: HostAuthSnapshot,
}

impl ModelClientFactory {
    pub fn new(credentials: HostAuthSnapshot) -> Self {
        Self { credentials }
    }

    /// Construct a client for an exact registered provider and endpoint.
    /// The host must authorize the endpoint and manage the lifetime of any
    /// returned client. Replacing a snapshot does not revoke existing clients.
    /// Chat, Responses and Messages endpoints are complete request URLs; Gemini uses
    /// an API base URL and appends its model-specific streaming path. Granting a
    /// Gemini base authorizes those derived paths. Model discovery probes,
    /// redirects, and implicit proxy discovery are disabled.
    pub fn client(
        &self,
        api: NativeModelApi,
        provider: &str,
        endpoint: &str,
    ) -> Result<Arc<dyn Model>> {
        let credential = self
            .credentials
            .get(provider, endpoint)
            .ok_or_else(|| Error::Config("model credential is not registered".into()))?;
        match (api, credential.kind()) {
            (
                NativeModelApi::OpenAiChat,
                HostCredentialKind::ApiKey | HostCredentialKind::Bearer,
            ) => Ok(Arc::new(
                pi_rs::ollama::Ollama::new(endpoint, Some(credential.secret().to_owned()))?
                    .with_provider(provider)
                    .with_native_probes(false),
            )),
            (NativeModelApi::OpenAiResponses, HostCredentialKind::ApiKey) => {
                Ok(Arc::new(pi_rs::openai::OpenAiResponses::new_api_provider(
                    endpoint,
                    credential.secret().to_owned(),
                    provider,
                )?))
            }
            (NativeModelApi::ChatGptResponses, HostCredentialKind::Bearer)
                if provider == "openai-chatgpt" =>
            {
                Ok(Arc::new(pi_rs::openai::OpenAiResponses::new(
                    endpoint,
                    credential.secret().to_owned(),
                )?))
            }
            (NativeModelApi::AnthropicMessages, HostCredentialKind::ApiKey) => Ok(Arc::new(
                pi_rs::anthropic::Anthropic::new(endpoint, credential.secret())?
                    .with_provider(provider, endpoint)
                    .with_context_probe(false),
            )),
            (NativeModelApi::AnthropicMessages, HostCredentialKind::Bearer) => Ok(Arc::new(
                pi_rs::anthropic::Anthropic::with_bearer_token(endpoint, credential.secret())?
                    .with_provider(provider, endpoint)
                    .with_context_probe(false),
            )),
            (NativeModelApi::GoogleGemini, HostCredentialKind::ApiKey) => Ok(Arc::new(
                pi_rs::google::Google::new(endpoint, credential.secret())?
                    .with_provider(provider, endpoint)
                    .with_context_probe(false),
            )),
            _ => Err(Error::Config(
                "credential kind is not supported for this model API".into(),
            )),
        }
    }

    /// Construct an unauthenticated chat client only when the host explicitly
    /// chooses that mode. No credential lookup or environment fallback occurs.
    pub fn anonymous_chat(&self, provider: &str, endpoint: &str) -> Result<Arc<dyn Model>> {
        if provider.trim().is_empty() {
            return Err(Error::Config("model provider is empty".into()));
        }
        Ok(Arc::new(
            pi_rs::ollama::Ollama::new(endpoint, None)?
                .with_provider(provider)
                .with_native_probes(false),
        ))
    }

    /// Register one host-authorized identity in a catalog builder. A duplicate
    /// identity remains an error, so existing clients cannot be replaced silently.
    pub fn register(
        &self,
        catalog: &mut ModelCatalogBuilder,
        api: NativeModelApi,
        provider: &str,
        endpoint: &str,
        model: &str,
    ) -> Result<()> {
        let client = self.client(api, provider, endpoint)?;
        catalog.register(provider, model, client)?;
        Ok(())
    }
}
