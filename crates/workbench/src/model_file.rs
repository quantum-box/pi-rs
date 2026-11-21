//! Read explicit Pi model declarations into a host-authorized native catalog.
use std::collections::{BTreeMap, BTreeSet, btree_map::Entry};
use std::path::{Component, PathBuf};
use std::sync::Arc;

use pi_rs::{
    CatalogModelCost, CatalogModelInfo, Error, HostAuthBuilder, HostCredential, HostCredentialKind,
    Model, ModelCatalog, ModelCatalogBuilder, Result,
};
use serde::Deserialize;
use serde_json::Value;

use crate::auth_factory::{MAX_API_KEY_BYTES, expand_config_value, valid_api_key};
use crate::model_factory::{ModelClientFactory, NativeModelApi};

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
        self.load_with_api_keys(factory, &[], |_, _| None)
    }

    /// Use a provider's configured API key as a fallback for the exact routes
    /// separately granted in the key route list. Credentials bound in the
    /// factory take precedence. The caller supplies variable values for each
    /// route. Commands and unresolved variables do not produce a credential.
    /// Run this synchronous file read off the frontend thread.
    pub fn load_with_api_keys(
        &self,
        factory: &ModelClientFactory,
        key_routes: &[ModelFileRoute],
        mut lookup: impl FnMut(&ModelFileRoute, &str) -> Option<String>,
    ) -> Result<ModelCatalog> {
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
        let mut file_key_routes = BTreeSet::new();
        for grant in key_routes {
            let key = (api_name(grant.api), grant.base_url.as_str());
            if authorized
                .get(grant.provider.as_str())
                .and_then(|routes| routes.get(&key))
                .is_none_or(|route| *route != grant)
            {
                return Err(Error::Config(
                    "API key route is outside the models file grant".into(),
                ));
            }
            if grant.api == NativeModelApi::ChatGptResponses {
                return Err(Error::Config(
                    "ChatGPT routes require host-provided OAuth credentials".into(),
                ));
            }
            file_key_routes.insert((grant.provider.as_str(), key));
        }

        let mut catalog = ModelCatalogBuilder::new();
        if authorized.is_empty() {
            return Ok(catalog.build());
        }
        let Some(text) =
            crate::prompt_resources::read_optional_resource(&self.file, "models file")?
        else {
            return Ok(catalog.build());
        };
        let config: ModelFile = serde_json::from_slice(&normalize_model_json(text))
            .map_err(|_| Error::Config("invalid models file JSON".into()))?;
        for (provider, routes) in authorized {
            let mut clients = BTreeMap::<_, Arc<dyn Model>>::new();
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
                let Some(key) = api.zip(base) else {
                    continue;
                };
                let Some(grant) = routes.get(&key) else {
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
                let client = match clients.entry(key) {
                    Entry::Occupied(entry) => entry.into_mut(),
                    Entry::Vacant(entry) => entry.insert(client_for_route(
                        factory,
                        grant,
                        file_key_routes.contains(&(provider, key)),
                        value,
                        &mut lookup,
                    )?),
                };
                catalog.register_with_info(provider, id, client.clone(), info)?;
            }
        }
        Ok(catalog.build())
    }
}

fn client_for_route(
    factory: &ModelClientFactory,
    route: &ModelFileRoute,
    allow_file_key: bool,
    declaration: &Value,
    lookup: &mut impl FnMut(&ModelFileRoute, &str) -> Option<String>,
) -> Result<Arc<dyn Model>> {
    let provider = &route.provider;
    let endpoint = &route.endpoint;
    if factory.has_credential(provider, endpoint) || !allow_file_key {
        return factory.client(route.api, provider, endpoint);
    }
    let Some(value) = declaration.get("apiKey") else {
        return factory.client(route.api, provider, endpoint);
    };
    let value = value
        .as_str()
        .ok_or_else(|| Error::Config("configured model API key must be a string".into()))?;
    let Some(key) = expand_config_value(value, MAX_API_KEY_BYTES, |name| lookup(route, name))?
    else {
        return factory.client(route.api, provider, endpoint);
    };
    if !valid_api_key(&key) {
        return Err(Error::Config("invalid configured model API key".into()));
    }
    let mut auth = HostAuthBuilder::new();
    auth.register(
        provider,
        endpoint,
        HostCredential::new(HostCredentialKind::ApiKey, key)?,
    )?;
    ModelClientFactory::new(auth.build()).client(route.api, provider, endpoint)
}

// Pi accepts line comments and trailing commas. Mask them without changing
// string contents, then let serde_json validate the remaining syntax.
fn normalize_model_json(text: String) -> Vec<u8> {
    let mut bytes = text.into_bytes();
    let mut index = 0;
    let mut in_string = false;
    let mut previous = None;
    while index < bytes.len() {
        let byte = bytes[index];
        if in_string {
            match byte {
                b'\\' => index += 1,
                b'"' => in_string = false,
                _ => {}
            }
        } else if byte == b'/' && bytes.get(index + 1) == Some(&b'/') {
            let end = bytes[index + 2..]
                .iter()
                .position(|byte| *byte == b'\n')
                .map_or(bytes.len(), |offset| index + 2 + offset);
            bytes[index..end].fill(b' ');
            index = end;
            continue;
        } else if !matches!(byte, b' ' | b'\r' | b'\n' | b'\t') {
            if matches!(byte, b']' | b'}')
                && let Some(comma) = previous
                && bytes[comma] == b','
            {
                bytes[comma] = b' ';
            }
            in_string = byte == b'"';
            previous = Some(index);
        }
        index += 1;
    }
    bytes
}

fn api_name(api: NativeModelApi) -> &'static str {
    match api {
        NativeModelApi::OpenAiChat => "openai-completions",
        NativeModelApi::OpenAiResponses => "openai-responses",
        NativeModelApi::ChatGptResponses => "openai-codex-responses",
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
