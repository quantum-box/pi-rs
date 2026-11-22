//! Read explicit Pi model declarations into a host-authorized native catalog.
use std::collections::BTreeMap;
use std::path::{Component, PathBuf};

use pi_rs::{CatalogModelCost, CatalogModelInfo, Error, ModelCatalog, Result};
use serde::Deserialize;
use serde_json::Value;

use crate::model_factory::{ModelClientFactory, NativeModelApi, NativeModelDefinition};

/// Authorize one model-file route. Route fields use exact, case-sensitive
/// matches. The declared base URL identifies a file entry; only the host's
/// request endpoint is passed to the native client and credential lookup.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ModelFileRoute {
    pub provider: String,
    pub api: NativeModelApi,
    pub base_url: String,
    pub endpoint: String,
}

/// A host-granted `models.json` file and the provider routes it can declare.
#[derive(Clone, Debug)]
pub struct ModelFileReadGrants {
    pub file: PathBuf,
    pub routes: Vec<ModelFileRoute>,
}

impl ModelFileReadGrants {
    /// Load a fresh immutable catalog using the factory's own credentials.
    /// A missing file produces an empty catalog. Only chat declarations on
    /// granted routes are eligible. A model can override its provider's API
    /// and base URL when that combination is also granted. The file does not
    /// supply credentials, request headers or provider compatibility options
    /// to clients. There is no built-in lookup or network discovery.
    pub fn load(&self, factory: &ModelClientFactory) -> Result<ModelCatalog> {
        if !self.file.is_absolute()
            || self
                .file
                .components()
                .any(|part| part == Component::ParentDir)
        {
            return Err(Error::Config(
                "models file grant must be absolute without parent components".into(),
            ));
        }
        let mut authorized = BTreeMap::new();
        for route in &self.routes {
            if [&route.provider, &route.base_url, &route.endpoint]
                .iter()
                .any(|value| value.trim().is_empty())
            {
                return Err(Error::Config(
                    "model file route contains an empty field".into(),
                ));
            }
            let routes = authorized
                .entry(route.provider.as_str())
                .or_insert_with(BTreeMap::new);
            if routes
                .insert((api_name(route.api), route.base_url.as_str()), route)
                .is_some()
            {
                return Err(Error::Config("duplicate model file route".into()));
            }
        }
        if authorized.is_empty() {
            return factory.catalog([]);
        }
        let Some(text) =
            crate::prompt_resources::read_optional_resource(&self.file, "models file")?
        else {
            return factory.catalog([]);
        };
        let config: ModelFile = serde_json::from_str(&text)
            .map_err(|_| Error::Config("invalid models file JSON".into()))?;
        let mut selected = Vec::new();
        for (provider, routes) in authorized {
            let Some(value) = config.providers.get(provider) else {
                continue;
            };
            let declaration =
                ProviderDeclaration::deserialize(value).map_err(|_| invalid_provider(provider))?;
            let Some(models) = value.get("models") else {
                continue;
            };
            let models = models
                .as_array()
                .ok_or_else(|| invalid_provider(provider))?;
            let overrides = value
                .get("modelOverrides")
                .map(|value| value.as_object().ok_or_else(|| invalid_provider(provider)))
                .transpose()?;
            for model in models {
                let route =
                    RouteDeclaration::deserialize(model).map_err(|_| invalid_provider(provider))?;
                if route.kind.is_some_and(|kind| kind != "chat") {
                    continue;
                }
                let api = route.api.or(declaration.api);
                let base = route.base_url.or(declaration.base_url);
                let Some(grant) = api.zip(base).and_then(|key| routes.get(&key)) else {
                    continue;
                };
                let model =
                    ModelMetadata::deserialize(model).map_err(|_| invalid_provider(provider))?;
                let id = model.id;
                let mut metadata = model.metadata;
                if let Some(value) = overrides.and_then(|overrides| overrides.get(id)) {
                    let overrides = DisplayMetadata::deserialize(value)
                        .map_err(|_| invalid_provider(provider))?;
                    metadata.apply_overrides(overrides);
                }
                let info = metadata.into_catalog_info(id, api_name(grant.api));
                if info.context_window == Some(0) || info.max_output_tokens == Some(0) {
                    return Err(invalid_provider(provider));
                }
                selected.push(NativeModelDefinition {
                    api: grant.api,
                    provider,
                    endpoint: &grant.endpoint,
                    model: id,
                    info: Some(info),
                });
            }
        }
        factory.catalog(selected)
    }
}

fn api_name(api: NativeModelApi) -> &'static str {
    match api {
        NativeModelApi::OpenAiChat => "openai-completions",
        NativeModelApi::OpenAiResponses | NativeModelApi::ChatGptResponses => "openai-responses",
        NativeModelApi::OpenAiCodexResponses => "openai-codex-responses",
        NativeModelApi::AnthropicMessages => "anthropic-messages",
        NativeModelApi::GoogleGemini => "google-generative-ai",
    }
}

fn invalid_provider(provider: &str) -> Error {
    Error::Config(format!(
        "invalid models file declaration for granted provider {provider}"
    ))
}

#[derive(Deserialize)]
struct ModelFile {
    #[serde(default)]
    providers: serde_json::Map<String, Value>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ProviderDeclaration<'a> {
    api: Option<&'a str>,
    base_url: Option<&'a str>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RouteDeclaration<'a> {
    #[serde(rename = "type")]
    kind: Option<&'a str>,
    api: Option<&'a str>,
    base_url: Option<&'a str>,
}

#[derive(Deserialize)]
struct ModelMetadata<'a> {
    id: &'a str,
    #[serde(flatten)]
    metadata: DisplayMetadata,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct DisplayMetadata {
    name: Option<String>,
    context_window: Option<u32>,
    max_tokens: Option<u32>,
    reasoning: Option<bool>,
    input: Option<Vec<String>>,
    cost: Option<CatalogModelCost>,
}

impl DisplayMetadata {
    fn apply_overrides(&mut self, overrides: Self) {
        self.name = overrides.name.or(self.name.take());
        self.context_window = overrides.context_window.or(self.context_window);
        self.max_tokens = overrides.max_tokens.or(self.max_tokens);
        self.reasoning = overrides.reasoning.or(self.reasoning);
        self.input = overrides.input.or(self.input.take());
        if let Some(cost) = overrides.cost {
            let base = self.cost.get_or_insert_with(CatalogModelCost::default);
            base.input = cost.input.or(base.input.take());
            base.output = cost.output.or(base.output.take());
            base.cache_read = cost.cache_read.or(base.cache_read.take());
            base.cache_write = cost.cache_write.or(base.cache_write.take());
        }
    }

    fn into_catalog_info(self, id: &str, api: &str) -> CatalogModelInfo {
        CatalogModelInfo {
            display_name: Some(self.name.unwrap_or_else(|| id.into())),
            api: Some(api.into()),
            context_window: Some(self.context_window.unwrap_or(128_000)),
            max_output_tokens: Some(self.max_tokens.unwrap_or(16_384)),
            reasoning: Some(self.reasoning.unwrap_or(false)),
            images: Some(
                self.input
                    .as_ref()
                    .is_some_and(|input| input.iter().any(|kind| kind == "image")),
            ),
            cost: self.cost,
        }
    }
}
