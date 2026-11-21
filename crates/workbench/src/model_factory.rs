//! Explicit native provider clients for hosts that own endpoints and credentials.
use std::sync::Arc;

use pi_rs::{
    CatalogModelInfo, Error, HostAuthSnapshot, HostCredentialKind, Model, ModelCatalog,
    ModelCatalogBuilder, Result,
};

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

/// A catalog entry with an explicitly authorized provider protocol and route.
/// Optional metadata is descriptive and cannot change the route.
#[derive(Clone, Debug)]
pub struct NativeModelDefinition<'a> {
    pub api: NativeModelApi,
    pub provider: &'a str,
    pub endpoint: &'a str,
    pub model: &'a str,
    pub info: Option<CatalogModelInfo>,
}

/// Construct clients only from the immutable credentials supplied by the host.
/// This does not discover credentials or models, run commands, refresh OAuth,
/// select endpoints, or perform network requests during creation.
#[derive(Clone, Default)]
pub struct ModelClientFactory {
    credentials: HostAuthSnapshot,
}

impl ModelClientFactory {
    pub fn new(credentials: HostAuthSnapshot) -> Self {
        Self { credentials }
    }

    /// Build an independent catalog from host-supplied definitions. Any
    /// invalid route, missing credential or duplicate identity fails the
    /// entire construction. Provider access is not probed.
    pub fn catalog<'a>(
        &self,
        definitions: impl IntoIterator<Item = NativeModelDefinition<'a>>,
    ) -> Result<ModelCatalog> {
        let mut catalog = ModelCatalogBuilder::new();
        for definition in definitions {
            let client = self.client(definition.api, definition.provider, definition.endpoint)?;
            if let Some(info) = definition.info {
                catalog.register_with_info(definition.provider, definition.model, client, info)?;
            } else {
                catalog.register(definition.provider, definition.model, client)?;
            }
        }
        Ok(catalog.build())
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
        match api {
            NativeModelApi::OpenAiChat => Ok(Arc::new(self.chat_client(provider, endpoint)?)),
            NativeModelApi::OpenAiResponses => {
                Ok(Arc::new(self.responses_client(provider, endpoint)?))
            }
            NativeModelApi::ChatGptResponses => {
                Ok(Arc::new(self.chatgpt_client(provider, endpoint)?))
            }
            NativeModelApi::AnthropicMessages => {
                Ok(Arc::new(self.anthropic_client(provider, endpoint)?))
            }
            NativeModelApi::GoogleGemini => Ok(Arc::new(self.gemini_client(provider, endpoint)?)),
        }
    }

    fn credential(&self, provider: &str, endpoint: &str) -> Result<&pi_rs::HostCredential> {
        self.credentials
            .get(provider, endpoint)
            .ok_or_else(|| Error::Config("model credential is not registered".into()))
    }

    /// A typed Responses client, configurable by the host before registration.
    pub fn responses_client(
        &self,
        provider: &str,
        endpoint: &str,
    ) -> Result<pi_rs::openai::OpenAiResponses> {
        let credential = self.credential(provider, endpoint)?;
        if credential.kind() != HostCredentialKind::ApiKey {
            return Err(Error::Config(
                "credential kind is not supported for this model API".into(),
            ));
        }
        pi_rs::openai::OpenAiResponses::new_api_provider(
            endpoint,
            credential.secret().to_owned(),
            provider,
        )
    }

    /// A typed ChatGPT plan client. Only the exact `openai-chatgpt` identity
    /// and a bearer credential are accepted; endpoint validation is delegated to
    /// the native adapter.
    pub fn chatgpt_client(
        &self,
        provider: &str,
        endpoint: &str,
    ) -> Result<pi_rs::openai::OpenAiResponses> {
        let credential = self.credential(provider, endpoint)?;
        if provider != "openai-chatgpt" || credential.kind() != HostCredentialKind::Bearer {
            return Err(Error::Config(
                "credential kind is not supported for this model API".into(),
            ));
        }
        pi_rs::openai::OpenAiResponses::new(endpoint, credential.secret().to_owned())
    }

    /// A typed Messages client with implicit metadata probes disabled.
    pub fn anthropic_client(
        &self,
        provider: &str,
        endpoint: &str,
    ) -> Result<pi_rs::anthropic::Anthropic> {
        let credential = self.credential(provider, endpoint)?;
        let client = match credential.kind() {
            HostCredentialKind::ApiKey => {
                pi_rs::anthropic::Anthropic::new(endpoint, credential.secret())?
            }
            HostCredentialKind::Bearer => {
                pi_rs::anthropic::Anthropic::with_bearer_token(endpoint, credential.secret())?
            }
        };
        Ok(client
            .with_provider(provider, endpoint)
            .with_context_probe(false))
    }

    /// A typed Gemini client with implicit metadata probes disabled.
    pub fn gemini_client(&self, provider: &str, endpoint: &str) -> Result<pi_rs::google::Google> {
        let credential = self.credential(provider, endpoint)?;
        if credential.kind() != HostCredentialKind::ApiKey {
            return Err(Error::Config(
                "credential kind is not supported for this model API".into(),
            ));
        }
        Ok(pi_rs::google::Google::new(endpoint, credential.secret())?
            .with_provider(provider, endpoint)
            .with_context_probe(false))
    }

    /// Construct a typed chat client so the host can explicitly configure
    /// provider/model compatibility before adding it to a catalog. Defaults do
    /// not infer Pi provider profiles or enable native endpoint probes.
    pub fn chat_client(&self, provider: &str, endpoint: &str) -> Result<pi_rs::ollama::Ollama> {
        let credential = self.credential(provider, endpoint)?;
        Ok(
            pi_rs::ollama::Ollama::new(endpoint, Some(credential.secret().to_owned()))?
                .with_provider(provider)
                .with_native_probes(false),
        )
    }

    /// Construct a typed unauthenticated chat client only when the host
    /// explicitly chooses that mode. No credential lookup or fallback occurs.
    pub fn anonymous_chat(&self, provider: &str, endpoint: &str) -> Result<pi_rs::ollama::Ollama> {
        if provider.trim().is_empty() {
            return Err(Error::Config("model provider is empty".into()));
        }
        Ok(pi_rs::ollama::Ollama::new(endpoint, None)?
            .with_provider(provider)
            .with_native_probes(false))
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
