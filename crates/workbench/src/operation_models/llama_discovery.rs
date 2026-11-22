use super::{Api, Configured, Cost, Entry, RESPONSE_LIMIT, llama_cpp, operation_endpoints};
use crate::model_auth::{Credential, Keys, valid_key};

use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    time::Duration,
};

use reqwest::{Client, Url};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::io::AsyncReadExt;

const TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Deserialize)]
struct Models {
    data: Vec<Model>,
}

#[derive(Deserialize)]
struct Model {
    id: String,
    status: Status,
    source: Option<String>,
    architecture: Option<Architecture>,
    meta: Option<Meta>,
}

#[derive(Deserialize)]
struct Status {
    value: String,
    #[serde(default)]
    failed: bool,
    #[serde(default)]
    args: Vec<String>,
}

#[derive(Deserialize)]
struct Architecture {
    #[serde(default)]
    output_modalities: Vec<String>,
}

#[derive(Deserialize)]
struct Meta {
    n_ctx: Option<u32>,
    n_ctx_train: Option<u32>,
}

impl Model {
    fn unloaded_preset(&self) -> bool {
        self.status.value == "unloaded" && self.source.as_deref() == Some("preset")
    }

    fn selectable(&self, autoload: bool) -> bool {
        matches!(self.status.value.as_str(), "loaded" | "sleeping")
            || (autoload && self.unloaded_preset() && !self.status.failed)
    }

    fn context_window(&self, cached: Option<u32>) -> u32 {
        let positive = |value: Option<u32>| value.filter(|value| *value > 0);
        let meta = self.meta.as_ref();
        positive(meta.and_then(|meta| meta.n_ctx))
            .or_else(|| {
                self.status.args.windows(2).find_map(|args| {
                    if matches!(args[0].as_str(), "--ctx-size" | "-c" | "-ctx") {
                        positive(args[1].parse().ok())
                    } else {
                        None
                    }
                })
            })
            .or_else(|| positive(cached))
            .or_else(|| positive(meta.and_then(|meta| meta.n_ctx_train)))
            .unwrap_or(128_000)
    }

    fn classifier(self, cached_window: Option<u32>) -> Classifier {
        let context_window = self.context_window(cached_window);
        let decisions = self.architecture.is_some_and(|arch| {
            arch.output_modalities
                .iter()
                .any(|kind| kind == "decisions")
        });
        Classifier {
            id: self.id,
            context_window,
            decisions,
        }
    }
}

#[derive(Deserialize, Serialize)]
struct Classifier {
    id: String,
    context_window: u32,
    decisions: bool,
}

impl Classifier {
    fn entry(self, root: &Url) -> Option<Entry> {
        let api = if self.decisions {
            Api::SystemOne
        } else {
            Api::LlamaCppClassifier
        };
        let (base, endpoint) = classifier_endpoints(root, api)?;
        Some(Entry {
            provider: "llama.cpp".into(),
            model: Configured {
                id: self.id,
                name: None,
                api: Some(api),
                base_url: None,
                input: None,
                output: None,
                input_limits: None,
                context_window: Some(self.context_window),
                cost: Cost::default(),
                headers: Default::default(),
                compat: Default::default(),
            },
            api,
            base,
            endpoint,
            api_key: None,
            cloudflare_default: false,
            llama_root: Some(root.clone()),
        })
    }
}

pub(super) fn classifier_endpoints(root: &Url, api: Api) -> Option<(Url, Url)> {
    match api {
        Api::SystemOne => {
            let base = llama_cpp::endpoint(root, "v1");
            operation_endpoints(base.as_str(), api, false).ok()
        }
        Api::LlamaCppClassifier => Some((root.clone(), llama_cpp::endpoint(root, api.route()))),
        _ => None,
    }
}

fn server_root(server: &str) -> Option<Url> {
    operation_endpoints(server.trim(), Api::LlamaCppClassifier, false)
        .ok()
        .map(|(root, _)| root)
}

pub(super) fn configured_server(
    keys: &Keys,
    env: impl Fn(&str) -> Option<String>,
) -> Option<(Url, Option<Credential>)> {
    let environment_url = env("LLAMA_BASE_URL").filter(|url| !url.trim().is_empty());
    let environment_root = environment_url.as_deref().and_then(server_root);
    let environment_key = || {
        env("LLAMA_API_KEY")
            .filter(|key| !key.is_empty())
            .map(|key| {
                if valid_key(&key) {
                    Credential::Literal(key)
                } else {
                    Credential::Invalid
                }
            })
    };
    if let Some((server, bound_endpoint, key)) = keys.llama_server() {
        let root = server_root(server)?;
        if let Some(bound_endpoint) = bound_endpoint
            && server_root(bound_endpoint).as_ref() != Some(&root)
        {
            return None;
        }
        let key = key.cloned().or_else(|| {
            if environment_url.is_none() || environment_root.as_ref() == Some(&root) {
                environment_key()
            } else {
                None
            }
        });
        return Some((root, key));
    }
    Some((environment_root?, environment_key()))
}

pub(super) async fn catalogue(
    client: Option<&Client>,
    root: &Url,
    key: Option<&str>,
    cache: PathBuf,
) -> Vec<Entry> {
    if key.is_some_and(|key| !valid_key(key)) {
        return Vec::new();
    }
    let mut fingerprint = Sha256::new();
    fingerprint.update(b"pi-rs llama classifier catalog v1\0");
    fingerprint.update(root.as_str());
    fingerprint.update([0]);
    fingerprint.update(key.unwrap_or_default());
    let scope = format!("{:x}", fingerprint.finalize());
    let stored = cached(&cache, &scope).await.unwrap_or_default();
    let refreshed = match client {
        Some(client) => discover(client, root, key, &stored).await,
        None => None,
    };
    let classifiers = match refreshed {
        Some(classifiers) => {
            let value = json!({ "scope": scope, "models": classifiers });
            let _ = tokio::task::spawn_blocking(move || crate::config::write_json(&cache, &value))
                .await;
            classifiers
        }
        None => stored,
    };
    let mut seen = BTreeSet::new();
    classifiers
        .into_iter()
        .filter(|model| !model.id.trim().is_empty() && model.id.len() <= 256)
        .filter(|model| model.context_window > 0)
        .filter(|model| seen.insert(model.id.clone()))
        .filter_map(|model| model.entry(root))
        .collect()
}

async fn cached(cache: &std::path::Path, scope: &str) -> Option<Vec<Classifier>> {
    #[derive(Deserialize)]
    struct Snapshot {
        scope: String,
        models: Vec<Classifier>,
    }
    let file = tokio::fs::File::open(cache).await.ok()?;
    let mut bytes = Vec::new();
    file.take((RESPONSE_LIMIT + 1) as u64)
        .read_to_end(&mut bytes)
        .await
        .ok()?;
    if bytes.len() > RESPONSE_LIMIT {
        return None;
    }
    let snapshot = serde_json::from_slice::<Snapshot>(&bytes).ok()?;
    (snapshot.scope == scope).then_some(snapshot.models)
}

async fn discover(
    client: &Client,
    root: &Url,
    key: Option<&str>,
    stored: &[Classifier],
) -> Option<Vec<Classifier>> {
    let models = get_json::<Models>(client, root, "models", key).await?;
    let autoload = if models.data.iter().any(Model::unloaded_preset) {
        get_json::<Value>(client, root, "props", key)
            .await
            .is_some_and(|props| props["models_autoload"] == true)
    } else {
        false
    };
    let cached_windows: BTreeMap<_, _> = stored
        .iter()
        .map(|model| (model.id.as_str(), model.context_window))
        .collect();
    Some(
        models
            .data
            .into_iter()
            .filter(|model| model.selectable(autoload))
            .map(|model| {
                let window = cached_windows.get(model.id.as_str()).copied();
                model.classifier(window)
            })
            .collect(),
    )
}

async fn get_json<T: serde::de::DeserializeOwned>(
    client: &Client,
    root: &Url,
    route: &str,
    key: Option<&str>,
) -> Option<T> {
    let mut request = client
        .get(llama_cpp::endpoint(root, route))
        .timeout(TIMEOUT);
    if let Some(key) = key {
        request = request.bearer_auth(key);
    }
    let mut response = request.send().await.ok()?.error_for_status().ok()?;
    if response
        .content_length()
        .is_some_and(|size| size > RESPONSE_LIMIT as u64)
    {
        return None;
    }
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.ok()? {
        if body.len() + chunk.len() > RESPONSE_LIMIT {
            return None;
        }
        body.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&body).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

    #[test]
    fn saved_router_credential_overrides_environment_without_crossing_endpoints() {
        let env = |name: &str| match name {
            "LLAMA_BASE_URL" => Some("https://env.example/v1".into()),
            "LLAMA_API_KEY" => Some("env-key".into()),
            _ => None,
        };
        let login = |key: Option<&str>, endpoint: Option<&str>| {
            let mut entry = json!({"type":"api_key","env":{"LLAMA_BASE_URL":"https://saved.example/prefix/v1/"}});
            if let Some(key) = key {
                entry["key"] = json!(key);
            }
            if let Some(endpoint) = endpoint {
                entry["endpoint"] = json!(endpoint);
            }
            Keys::parse(&json!({"llama.cpp":entry}))
        };
        let keys = login(Some("saved-key"), Some("https://saved.example/prefix"));
        let (root, key) = configured_server(&keys, env).unwrap();
        assert_eq!(root.as_str(), "https://saved.example/prefix");
        assert!(matches!(key, Some(Credential::Literal(key)) if key == "saved-key"));

        let keys = login(None, None);
        let (root, key) = configured_server(&keys, env).unwrap();
        assert_eq!(root.as_str(), "https://saved.example/prefix");
        assert!(key.is_none());

        let mismatched = login(Some("saved-key"), Some("https://other.example/prefix"));
        assert!(configured_server(&mismatched, env).is_none());

        let (root, key) = configured_server(&Keys::default(), env).unwrap();
        assert_eq!(root.as_str(), "https://env.example/");
        assert!(matches!(key, Some(Credential::Literal(key)) if key == "env-key"));
    }

    #[tokio::test]
    async fn selects_router_models_and_restores_the_matching_cached_catalog() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let models = json!({"data":[
            {"id":"chat","status":{"value":"loaded","args":["-c","4096"]},"meta":{"n_ctx":8192}},
            {"id":"decision","status":{"value":"sleeping"},"architecture":{"output_modalities":["decisions"]}},
            {"id":"preset","source":"preset","status":{"value":"unloaded","args":["--ctx-size","2048"]}},
            {"id":"failed","source":"preset","status":{"value":"unloaded","failed":true}},
            {"id":"downloaded","source":"cache","status":{"value":"unloaded"}},
            {"id":"loading","status":{"value":"loading"}},
        ]});
        let fixture = tokio::spawn(async move {
            for (route, status, response) in [
                ("/prefix/models", "200 OK", models.clone()),
                ("/prefix/props", "200 OK", json!({"models_autoload":true})),
                ("/prefix/models", "503 Unavailable", json!({})),
                ("/prefix/models", "200 OK", models),
                ("/prefix/props", "503 Unavailable", json!({})),
                (
                    "/prefix/models",
                    "200 OK",
                    json!({"data":[{"id":"chat","status":{"value":"sleeping"},
                        "meta":{"n_ctx_train":4096}}]}),
                ),
                ("/prefix/models", "200 OK", json!({"data":[]})),
            ] {
                let (stream, _) = listener.accept().await.unwrap();
                let mut stream = BufReader::new(stream);
                let mut line = String::new();
                stream.read_line(&mut line).await.unwrap();
                assert_eq!(line, format!("GET {route} HTTP/1.1\r\n"));
                let mut authorized = false;
                loop {
                    line.clear();
                    assert_ne!(stream.read_line(&mut line).await.unwrap(), 0);
                    if line == "\r\n" {
                        break;
                    }
                    authorized |= line
                        .trim()
                        .eq_ignore_ascii_case("authorization: Bearer fixture");
                }
                assert!(authorized);
                let response = response.to_string();
                stream
                    .write_all(format!(
                        "HTTP/1.1 {status}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{response}",
                        response.len()
                    ).as_bytes())
                    .await
                    .unwrap();
            }
        });
        let client = Client::builder().no_proxy().build().unwrap();
        let home = tempfile::tempdir().unwrap();
        let cache = home.path().join("llama-classifiers.json");
        let root = server_root(&format!("http://{address}/prefix/v1/")).unwrap();
        let entries = catalogue(Some(&client), &root, Some("fixture"), cache.clone()).await;
        assert_eq!(
            entries
                .iter()
                .map(|entry| entry.model.id.as_str())
                .collect::<Vec<_>>(),
            ["chat", "decision", "preset"]
        );
        assert_eq!(entries[0].endpoint.path(), "/prefix/completion");
        assert_eq!(entries[0].model.context_window, Some(8192));
        assert_eq!(entries[1].endpoint.path(), "/prefix/v1/systemone");
        assert!(matches!(entries[1].api, Api::SystemOne));
        assert!(entries[1].allows_keyless(&Keys::default()));
        assert_eq!(entries[2].model.context_window, Some(2048));
        let expected = entries.iter().map(Entry::descriptor).collect::<Vec<_>>();
        for connection in [None, Some(&client)] {
            let restored = catalogue(connection, &root, Some("fixture"), cache.clone()).await;
            assert_eq!(
                restored.iter().map(Entry::descriptor).collect::<Vec<_>>(),
                expected
            );
            assert_eq!(restored[1].endpoint, entries[1].endpoint);
        }
        let saved = tokio::fs::read_to_string(&cache).await.unwrap();
        assert!(!saved.contains("fixture"));
        assert!(!saved.contains(root.as_str()));
        assert!(
            catalogue(None, &root, Some("other"), cache.clone())
                .await
                .is_empty()
        );
        let different_root = server_root(&format!("http://{address}/other/v1/")).unwrap();
        assert!(
            catalogue(None, &different_root, Some("fixture"), cache.clone())
                .await
                .is_empty()
        );
        for connection in [Some(&client), None] {
            let restored = catalogue(connection, &root, Some("fixture"), cache.clone()).await;
            assert_eq!(
                restored
                    .iter()
                    .map(|entry| entry.model.id.as_str())
                    .collect::<Vec<_>>(),
                ["chat", "decision"]
            );
        }
        let updated = catalogue(Some(&client), &root, Some("fixture"), cache.clone()).await;
        assert_eq!(updated.len(), 1);
        assert_eq!(updated[0].model.id, "chat");
        assert_eq!(updated[0].model.context_window, Some(8192));
        assert!(
            catalogue(Some(&client), &root, Some("fixture"), cache.clone())
                .await
                .is_empty()
        );
        assert!(
            catalogue(None, &root, Some("fixture"), cache)
                .await
                .is_empty()
        );
        fixture.await.unwrap();
    }
}
