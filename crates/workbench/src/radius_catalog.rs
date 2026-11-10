//! Account-scoped discovery for Radius gateway models.
use std::collections::{BTreeMap, HashSet};
use std::io::Read;
use std::path::PathBuf;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use pi_rs::{Error, Result, ollama::ModelDetails};
use reqwest::{Client, Url};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

const MAX_BYTES: usize = 2 * 1024 * 1024;
pub(super) struct Catalog {
    pub endpoint: Url,
    pub models: Vec<Model>,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct Model {
    pub id: String,
    pub name: String,
    reasoning: bool,
    input: Vec<String>,
    context_window: u32,
    max_tokens: u32,
    #[serde(default)]
    thinking_level_map: BTreeMap<String, Option<String>>,
}

impl Model {
    pub fn details(&self) -> ModelDetails {
        let thinking_levels = super::thinking::LEVELS
            .iter()
            .copied()
            .filter(|level| {
                *level == "off"
                    || self.reasoning
                        && self
                            .thinking_level_map
                            .get(*level)
                            .map_or(!matches!(*level, "xhigh" | "max"), Option::is_some)
            })
            .map(str::to_owned)
            .collect();
        ModelDetails {
            context_window: Some(self.context_window),
            max_output_tokens: Some(self.max_tokens),
            reasoning: Some(self.reasoning),
            images: Some(self.input.iter().any(|input| input == "image")),
            thinking_levels: Some(thinking_levels),
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Config {
    base_url: String,
    models: Vec<Value>,
}

pub(super) async fn load(client: &Client, gateway: &Url, token: &str) -> Result<Catalog> {
    let url = gateway
        .join("/v1/config")
        .expect("valid Radius config endpoint");
    let mut response = client
        .get(url)
        .bearer_auth(token)
        .header(reqwest::header::ACCEPT, "application/json")
        .timeout(Duration::from_secs(15))
        .send()
        .await
        .map_err(|_| Error::RetryableModel("Radius model discovery failed".into()))?;
    if !response.status().is_success() {
        let status = response.status();
        let message = format!("Radius model discovery failed ({status})");
        return Err(if status.is_server_error() || status.as_u16() == 429 {
            Error::RetryableModel(message)
        } else {
            Error::Config(message)
        });
    }
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| Error::RetryableModel("Could not read Radius model catalog".into()))?
    {
        if body.len() + chunk.len() > MAX_BYTES {
            return Err(Error::Config("Radius model catalog exceeds 2 MiB".into()));
        }
        body.extend_from_slice(&chunk);
    }
    let config: Config = serde_json::from_slice(&body)
        .map_err(|_| Error::Config("Radius returned an invalid model catalog".into()))?;
    parse(config, gateway)
}

fn parse(config: Config, gateway: &Url) -> Result<Catalog> {
    let mut endpoint = Url::parse(&config.base_url)
        .map_err(|_| Error::Config("Radius returned an invalid model endpoint".into()))?;
    if endpoint.origin() != gateway.origin()
        || !endpoint.username().is_empty()
        || endpoint.password().is_some()
        || endpoint.query().is_some()
        || endpoint.fragment().is_some()
    {
        return Err(Error::Config(
            "Radius model endpoint must match the authenticated gateway origin".into(),
        ));
    }
    let path = endpoint.path().trim_end_matches('/');
    let path = if path.ends_with("/messages") {
        path.to_owned()
    } else {
        format!("{path}/messages")
    };
    endpoint.set_path(&path);
    let mut seen = HashSet::new();
    let models = config
        .models
        .into_iter()
        .filter_map(|model| serde_json::from_value::<Model>(model).ok())
        .filter(|model| {
            !model.id.is_empty()
                && !model.name.is_empty()
                && model.context_window > 0
                && model.max_tokens > 0
                && seen.insert(model.id.clone())
        })
        .collect();
    Ok(Catalog { endpoint, models })
}

pub(super) fn baseline(gateway: &Url) -> Option<Arc<Catalog>> {
    static PUBLIC: LazyLock<Arc<Catalog>> = LazyLock::new(|| {
        let source = include_str!("../assets/radius-models-v1.5.0.json");
        let config = serde_json::from_str(source).expect("bundled Radius catalog");
        let gateway =
            Url::parse(super::radius_auth::DEFAULT_GATEWAY).expect("public Radius gateway");
        Arc::new(parse(config, &gateway).expect("valid public Radius catalog"))
    });
    (gateway.as_str() == super::radius_auth::DEFAULT_GATEWAY).then(|| PUBLIC.clone())
}

fn cache_path() -> PathBuf {
    super::config::agent_dir().join("radius-models.json")
}

pub(super) fn cached(gateway: &Url, account: &str) -> Option<Arc<Catalog>> {
    #[derive(Deserialize)]
    struct Cached {
        origin: String,
        account: String,
        #[serde(flatten)]
        catalog: Config,
    }
    let mut file = std::fs::File::open(cache_path())
        .ok()?
        .take((MAX_BYTES + 1) as u64);
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).ok()?;
    if bytes.len() > MAX_BYTES {
        return None;
    }
    let cached: Cached = serde_json::from_slice(&bytes).ok()?;
    if cached.origin != gateway.as_str() || cached.account != account {
        return None;
    }
    parse(cached.catalog, gateway).ok().map(Arc::new)
}

pub(super) fn remember(gateway: &Url, account: &str, catalog: &Catalog) -> Result<()> {
    super::config::write_json(
        &cache_path(),
        &json!({
            "origin":gateway.as_str(), "account":account,
            "baseUrl":catalog.endpoint.as_str(), "models":catalog.models,
        }),
    )
}
