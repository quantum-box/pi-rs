//! Model scopes and thinking suffixes shared by startup and terminal cycling.
use pi_rs::{Error, Result};

use super::{Cli, Provider};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Choice {
    pub model: String,
    pub thinking: Option<String>,
}

#[derive(Clone)]
pub struct ScopedChoice {
    pub provider: Provider,
    pub model: String,
    pub thinking: Option<String>,
}

impl ScopedChoice {
    pub fn qualified(&self) -> String {
        format!("{}/{}", self.provider.id(), self.model)
    }

    pub fn pattern(&self) -> String {
        let mut name = self.qualified();
        if let Some(level) = &self.thinking {
            name.push(':');
            name.push_str(level);
        }
        name
    }
}

pub fn qualified_model<'a>(
    value: &'a str,
    profiles: &super::model_profiles::Profiles,
) -> Option<(Provider, &'a str)> {
    let (provider, model) = value.split_once('/')?;
    Some((profiles.provider(provider)?, model))
}

// The order is Pi's startup preference across authenticated providers.
const PROVIDER_DEFAULTS: &[(&str, &str)] = &[
    ("amazon-bedrock", "us.anthropic.claude-opus-5-5"),
    ("ant-ling", "Ring-2.6-1T"),
    ("anthropic", "claude-opus-5-5"),
    ("openai", "gpt-6.2-sol"),
    ("azure", "gpt-6.2-sol"),
    ("openai-codex", "gpt-6.2-sol"),
    ("radius", "balanced"),
    ("nvidia", "nvidia/nemotron-3.5-super-vl-120b-a12b"),
    ("deepseek", "deepseek-v4-pro"),
    ("google", "gemini-4-argon"),
    ("google-vertex", "gemini-4-argon"),
    ("github-copilot", "gpt-6.1-sol"),
    ("openrouter", "moonshotai/kimi-k2.7-code"),
    ("vercel-ai-gateway", "zai/glm-5.3"),
    ("xai", "grok-4.7"),
    ("groq", "openai/gpt-oss-120b"),
    ("cerebras", "gpt-oss-120b"),
    ("zai", "glm-5.3"),
    ("zai-coding-cn", "glm-5.3"),
    ("mistral", "devstral-medium-latest"),
    ("minimax", "MiniMax-M3"),
    ("minimax-cn", "MiniMax-M3"),
    ("moonshotai", "kimi-k2.7-code"),
    ("moonshotai-cn", "kimi-k2.7-code"),
    ("huggingface", "moonshotai/Kimi-K2.7-Code"),
    ("fireworks", "accounts/fireworks/models/kimi-k3"),
    ("together", "moonshotai/Kimi-K3"),
    ("baseten", "zai-org/GLM-5.3"),
    ("opencode", "kimi-k2.7-code"),
    ("opencode-go", "kimi-k3.1-pro"),
    ("kimi-coding", "kimi-for-coding"),
    ("meta", "muse-spark-1.3"),
    ("cloudflare-workers-ai", "@cf/moonshotai/kimi-k2.7-code"),
    (
        "cloudflare-ai-gateway",
        "workers-ai/@cf/moonshotai/kimi-k2.7-code",
    ),
    ("qwen-token-plan", "qwen3.8-max"),
    ("qwen-token-plan-cn", "qwen3.8-max"),
    ("qwen-token-plan-individual", "qwen3.8-max"),
    ("qwen-token-plan-individual-cn", "qwen3.8-max"),
    ("xiaomi", "mimo-v2.6-pro"),
    ("xiaomi-token-plan-cn", "mimo-v2.6-pro"),
    ("xiaomi-token-plan-ams", "mimo-v2.6-pro"),
    ("xiaomi-token-plan-sgp", "mimo-v2.6-pro"),
];

/// Use the pinned Pi default when it is present in the effective catalog.
pub(super) fn default_for_provider<'a>(
    provider: &Provider,
    models: &'a [String],
) -> Option<&'a String> {
    PROVIDER_DEFAULTS
        .iter()
        .find(|(id, _)| *id == provider.id())
        .and_then(|(_, preferred)| models.iter().find(|model| model == preferred))
        .or_else(|| models.first())
}

/// Choose from known catalog entries without resolving credential commands
/// or refreshing OAuth on startup. Native Ollama remains the local fallback.
pub(super) async fn automatic_startup_model(args: &Cli) -> Option<ScopedChoice> {
    use super::{model_auth::Credential, providers::Client};

    let profiles = &args.model_profiles;
    let bedrock_ambient = bedrock_environment_configured(|key| {
        std::env::var_os(key).is_some_and(|value| !value.is_empty())
    });
    let mut candidates = Vec::new();
    for provider in profiles
        .providers()
        .filter(|provider| *provider != Provider::Ollama)
    {
        let radius = if provider == Provider::Radius {
            match args.connect_provider(&provider, None) {
                // A model fallback must not fetch catalogs or refresh OAuth.
                // Pi Messages can read its local Radius catalog without probing.
                Ok(Client::PiMessages(client)) => client
                    .with_context_probe(false)
                    .models()
                    .await
                    .unwrap_or_default(),
                _ => Vec::new(),
            }
        } else {
            Vec::new()
        };
        let base_endpoint = profiles.endpoint(&provider);
        let mut models = profiles.model_ids(&provider);
        for id in &radius {
            if !models.contains(id) {
                models.push(id.clone());
            }
        }
        for model in models {
            let endpoint = profiles
                .model_endpoint(&provider, &model)
                .unwrap_or(base_endpoint);
            if args
                .model_keys
                .available_model_ids(&provider, endpoint)
                .is_some_and(|available| !available.contains(&model))
            {
                continue;
            }
            let credential = args.configured_model_credential(&provider, endpoint);
            let usable = match credential {
                Some(Credential::Invalid) => false,
                Some(credential) => credential
                    .literal()
                    .is_none_or(super::model_auth::valid_key),
                None => {
                    (provider == Provider::Bedrock && endpoint.is_empty() && bedrock_ambient)
                        || (provider == Provider::Radius
                            && endpoint == base_endpoint
                            && radius.contains(&model))
                }
            };
            if usable {
                let priority = PROVIDER_DEFAULTS
                    .iter()
                    .position(|(id, preferred)| *id == provider.id() && *preferred == model)
                    .unwrap_or(PROVIDER_DEFAULTS.len());
                candidates.push((
                    priority,
                    ScopedChoice {
                        provider: provider.clone(),
                        model,
                        thinking: None,
                    },
                ));
            }
        }
    }
    candidates.sort_by_key(|(priority, _)| *priority);
    candidates
        .into_iter()
        .map(|(_, choice)| choice)
        .find(|choice| {
            args.connect_provider(&choice.provider, Some(&choice.model))
                .is_ok()
        })
}

pub struct Catalog {
    pub provider: Provider,
    pub client: super::providers::Client,
    pub models: Vec<String>,
    aliases: Vec<(String, String)>,
    probe_details: bool,
    pub complete: bool,
}

impl Catalog {
    pub fn display_name<'a>(&'a self, model: &'a str) -> &'a str {
        self.aliases
            .iter()
            .find(|(id, _)| id == model)
            .map_or(model, |(_, name)| name)
    }

    pub async fn details(
        &self,
        models: &[String],
        profiles: &super::model_profiles::Profiles,
    ) -> std::collections::BTreeMap<String, pi_rs::ollama::ModelDetails> {
        let mut details = if self.probe_details {
            self.client.model_details(models).await
        } else {
            Default::default()
        };
        for model in models {
            if let Some(metadata) = profiles.model(&self.provider, model) {
                metadata.apply(
                    profiles.model_adapter(&self.provider, model),
                    details.entry(model.clone()).or_default(),
                );
            }
        }
        details
    }
}

pub async fn catalog(args: &Cli, provider: Provider, probe: bool) -> Result<Catalog> {
    let client = args.connect_provider(&provider, None)?;
    let client = if provider == Provider::Radius {
        client.with_context_probes(probe)
    } else {
        client
    };
    let mut models = args.model_profiles.model_ids(&provider);
    let mut probe_details = false;
    let bundled = provider.id() == "azure"
        || (provider.id() == "cloudflare-ai-gateway"
            && super::cloudflare_gateway::Route::is_endpoint(args.endpoint_for(&provider)))
        || (provider.id() == "cloudflare-workers-ai"
            && super::model_profiles::cloudflare_workers_chat_account(
                args.endpoint_for(&provider),
            )
            .is_some())
        || super::builtin_chat::find(provider.id())
            .is_some_and(|builtin| builtin.is_default_endpoint(args.endpoint_for(&provider)));
    let mut complete = false;
    if (probe && !bundled) || provider == Provider::Radius {
        match client.models(args.model_profiles.azure_api_version()).await {
            Ok(discovered) => {
                complete = true;
                let mut seen: std::collections::BTreeSet<_> = models.iter().cloned().collect();
                models.extend(discovered.into_iter().filter(|id| seen.insert(id.clone())));
                probe_details = true;
            }
            Err(error) if models.is_empty() => return Err(error),
            Err(_) => {}
        }
    }
    let endpoint = args.endpoint_for(&provider);
    if matches!(
        args.model_credential_for(&provider, endpoint),
        Some(super::model_auth::Credential::Copilot)
    ) {
        // The worker may have refreshed the shared subscription since these arguments were cloned.
        let current = super::model_auth::Keys::load_async().await?;
        if let Some(available) = current.available_model_ids(&provider, endpoint) {
            models.retain(|model| available.contains(model));
        }
    }
    let mut aliases = aliases(&args.model_profiles, &provider, &models);
    let configured: std::collections::HashSet<_> =
        aliases.iter().map(|(id, _)| id.clone()).collect();
    aliases.extend(
        client
            .aliases()
            .into_iter()
            .filter(|(id, _)| !configured.contains(id)),
    );
    Ok(Catalog {
        provider,
        client,
        models,
        aliases,
        probe_details,
        complete,
    })
}

pub async fn catalogs(args: &Cli, automatic: bool) -> Result<Vec<Catalog>> {
    let providers = std::iter::once(args.provider.clone()).chain(
        args.model_profiles
            .providers()
            .filter(|provider| *provider != args.provider),
    );
    let discovered = futures_util::future::join_all(providers.filter_map(|provider| {
        let probe = !automatic || args.allow_endpoint_probes(args.endpoint_for(&provider));
        (probe
            || provider == Provider::Radius
            || !args.model_profiles.model_ids(&provider).is_empty())
        .then_some(catalog(args, provider, probe))
    }))
    .await;
    let mut catalogs = Vec::new();
    let mut first_error = None;
    for entry in discovered {
        match entry {
            Ok(catalog) => catalogs.push(catalog),
            Err(error) => {
                first_error.get_or_insert(error);
            }
        }
    }
    if catalogs.is_empty()
        && (!automatic || !args.offline)
        && let Some(error) = first_error
    {
        return Err(error);
    }
    Ok(catalogs)
}

/// Rebuild a workspace scope from the catalog data already available locally.
pub(super) fn cached_catalogs(args: &Cli) -> Vec<Catalog> {
    let providers = std::iter::once(args.provider.clone()).chain(
        args.model_profiles
            .providers()
            .filter(|provider| *provider != args.provider),
    );
    providers
        .filter_map(|provider| {
            let client = args.connect_provider(&provider, None).ok()?;
            let mut models = args.model_profiles.model_ids(&provider);
            for model in args
                .discovered_models
                .iter()
                .filter(|(id, _)| id == provider.id())
                .map(|(_, model)| model)
                .chain(
                    args.model_scope
                        .iter()
                        .filter(|choice| choice.provider == provider)
                        .map(|choice| &choice.model),
                )
            {
                if !models.contains(model) {
                    models.push(model.clone());
                }
            }
            let endpoint = args.endpoint_for(&provider);
            if matches!(
                args.model_credential_for(&provider, endpoint),
                Some(super::model_auth::Credential::Copilot)
            ) && let Some(available) = args.model_keys.available_model_ids(&provider, endpoint)
            {
                models.retain(|model| available.contains(model));
            }
            if models.is_empty() {
                return None;
            }
            let aliases = aliases(&args.model_profiles, &provider, &models);
            Some(Catalog {
                provider,
                client,
                models,
                aliases,
                probe_details: false,
                complete: false,
            })
        })
        .collect()
}

fn exact_scope_matches<'a>(
    model: &str,
    qualified: Option<&'a Catalog>,
    catalogs: &'a [Catalog],
) -> Vec<(&'a Catalog, &'a str)> {
    if let Some(catalog) = qualified {
        let id = unqualified(model, catalog.provider.id());
        if let Some(matched) = catalog.models.iter().find(|m| m.eq_ignore_ascii_case(id)) {
            return vec![(catalog, matched)];
        }
    }
    catalogs
        .iter()
        .filter_map(|catalog| {
            let matched = catalog
                .models
                .iter()
                .find(|id| id.eq_ignore_ascii_case(model))?;
            Some((catalog, matched.as_str()))
        })
        .collect()
}

fn add_scoped_choice(
    selected: &mut Vec<ScopedChoice>,
    provider: &Provider,
    model: String,
    thinking: Option<String>,
) {
    if !selected
        .iter()
        .any(|choice| &choice.provider == provider && choice.model == model)
    {
        selected.push(ScopedChoice {
            provider: provider.clone(),
            model,
            thinking,
        });
    }
}

pub fn scoped_choices<'a>(
    patterns: impl IntoIterator<Item = &'a str>,
    catalogs: &[Catalog],
) -> (Vec<ScopedChoice>, Vec<String>) {
    let mut selected: Vec<ScopedChoice> = Vec::new();
    let mut warnings = Vec::new();
    for pattern in patterns
        .into_iter()
        .map(str::trim)
        .filter(|pattern| !pattern.is_empty())
    {
        let glob = pattern.contains(['*', '?', '[']);
        let qualified = pattern.split_once('/').and_then(|(prefix, _)| {
            catalogs
                .iter()
                .find(|catalog| prefix.eq_ignore_ascii_case(catalog.provider.id()))
        });
        let suffix = thinking_suffix(pattern);
        // Pi strips the suffix before testing a glob-like pattern for a literal ID.
        let (mut exact, mut thinking) = if glob {
            let (model, level) =
                suffix.map_or((pattern, None), |(base, level)| (base, Some(level)));
            (exact_scope_matches(model, qualified, catalogs), level)
        } else {
            (exact_scope_matches(pattern, qualified, catalogs), None)
        };
        if let Some((model, level)) = suffix.filter(|_| exact.is_empty() && !glob) {
            exact = exact_scope_matches(model, qualified, catalogs);
            thinking = Some(level);
        }
        if !exact.is_empty() {
            if exact.len() > 1 {
                let mut providers: Vec<_> = exact
                    .iter()
                    .map(|(catalog, _)| catalog.provider.id())
                    .collect();
                providers.sort_unstable();
                warnings.push(format!(
                    "Pattern \"{pattern}\" matches several providers: {}. Use provider/id to choose one.",
                    providers.join(", ")
                ));
            }
            for (catalog, model) in exact {
                add_scoped_choice(
                    &mut selected,
                    &catalog.provider,
                    model.to_owned(),
                    thinking.map(str::to_owned),
                );
            }
            continue;
        }
        // Keep native display-name aliases ahead of partial matches.
        let qualified = qualified.or_else(|| {
            if glob {
                return None;
            }
            catalogs.iter().find(|catalog| {
                catalog.aliases.iter().any(|(_, name)| {
                    name.eq_ignore_ascii_case(pattern)
                        || suffix.is_some_and(|(model, _)| name.eq_ignore_ascii_case(model))
                })
            })
        });
        let mut matched = false;
        for catalog in catalogs
            .iter()
            .filter(|catalog| qualified.is_none_or(|named| named.provider == catalog.provider))
        {
            let candidates = search_ids(&catalog.models, &catalog.aliases);
            let resolved = scope([pattern], &candidates, catalog.provider.id());
            if resolved.choices.is_empty() {
                continue;
            }
            matched = true;
            warnings.extend(resolved.warnings);
            for choice in resolved.choices {
                add_scoped_choice(
                    &mut selected,
                    &catalog.provider,
                    canonical(choice.model, &catalog.models, &catalog.aliases),
                    choice.thinking,
                );
            }
            if !glob {
                break;
            }
        }
        if !matched {
            warnings.push(format!("No models match pattern \"{pattern}\""));
        }
    }
    (selected, warnings)
}

fn bedrock_environment_configured(present: impl Fn(&str) -> bool) -> bool {
    [
        "AWS_PROFILE",
        "AWS_BEARER_TOKEN_BEDROCK",
        "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
        "AWS_CONTAINER_CREDENTIALS_FULL_URI",
        "AWS_WEB_IDENTITY_TOKEN_FILE",
    ]
    .into_iter()
    .any(&present)
        || (present("AWS_ACCESS_KEY_ID") && present("AWS_SECRET_ACCESS_KEY"))
}

/// Resolve a known CLI model before an inline key is bound to a provider.
pub fn cli_catalog_model(args: &Cli, value: &str, inline: bool) -> Result<Option<ScopedChoice>> {
    let ambient_bedrock = bedrock_environment_configured(|name| {
        std::env::var_os(name).is_some_and(|value| !value.is_empty())
    });
    resolve_cli_catalog_model(args, value, inline, ambient_bedrock)
}

fn resolve_cli_catalog_model(
    args: &Cli,
    value: &str,
    inline: bool,
    ambient_bedrock: bool,
) -> Result<Option<ScopedChoice>> {
    struct Match {
        choice: ScopedChoice,
        named: bool,
        authenticated: bool,
        bedrock_chain: bool,
    }

    let profiles = &args.model_profiles;
    let value = value.trim();
    let suffix = thinking_suffix(value).map(|(id, level)| (id, Some(level)));
    for (reference, thinking) in std::iter::once((value, None)).chain(suffix) {
        let named = qualified_model(reference, profiles);
        let mut matches = Vec::new();
        for provider in profiles.providers() {
            let models = profiles.model_ids(&provider);
            let explicit = named.as_ref().filter(|(named, _)| &provider == named);
            let known = models
                .iter()
                .find(|model| explicit.is_some_and(|(_, id)| model.eq_ignore_ascii_case(id)))
                .or_else(|| {
                    models
                        .iter()
                        .find(|model| model.eq_ignore_ascii_case(reference))
                });
            // Pi also treats the named provider's own full literal ID as a
            // qualified match. A named partial match can compete with raw IDs.
            let id = known.cloned().or_else(|| {
                let (_, id) = explicit?;
                let names = aliases(profiles, &provider, &models);
                let candidates = search_ids(&models, &names);
                exact(id, &candidates, provider.id())
                    .or_else(|| partial(id, &candidates, provider.id()))
                    .map(|model| canonical(model.clone(), &models, &names))
            });
            let Some(id) = id else {
                continue;
            };
            let endpoint = profiles
                .model_endpoint(&provider, &id)
                .unwrap_or_else(|| profiles.endpoint(&provider));
            let credential = args.configured_model_credential(&provider, endpoint);
            let bedrock_chain = provider == Provider::Bedrock
                && endpoint.is_empty()
                && profiles.model_adapter(&provider, &id) == super::providers::Adapter::Bedrock
                && credential.is_none();
            let authenticated = !inline
                && match credential {
                    Some(credential) => {
                        !matches!(credential, super::model_auth::Credential::Invalid)
                    }
                    None => provider == Provider::Ollama || (bedrock_chain && ambient_bedrock),
                };
            matches.push(Match {
                choice: ScopedChoice {
                    provider,
                    model: id,
                    thinking: thinking.map(str::to_owned),
                },
                named: explicit.is_some(),
                authenticated,
                bedrock_chain,
            });
        }
        if matches.is_empty() {
            continue;
        }
        let usable_count = matches.iter().filter(|model| model.authenticated).count();
        if matches.len() == 1 {
            return Ok(matches.pop().map(|model| model.choice));
        }
        if let Some(model) = matches
            .iter()
            .find(|model| model.named && (model.authenticated || usable_count == 0))
            .or_else(|| {
                matches
                    .iter()
                    .find(|model| model.authenticated && usable_count == 1)
            })
        {
            return Ok(Some(model.choice.clone()));
        }
        // The native default may use the SDK chain even when no startup
        // credential is visible. It cannot overrule evidence of usable auth.
        if !inline
            && usable_count == 0
            && args
                .settings
                .string("defaultProvider")
                .is_some_and(|id| id.eq_ignore_ascii_case(Provider::Bedrock.id()))
            && let Some(model) = matches.iter().find(|model| model.bedrock_chain)
        {
            return Ok(Some(model.choice.clone()));
        }
        let mut names: Vec<_> = matches
            .iter()
            .map(|model| model.choice.qualified())
            .collect();
        names.sort_unstable();
        let hint = if inline {
            "--api-key does not choose a provider."
        } else if usable_count == 0 {
            "No matching provider is authenticated."
        } else {
            "More than one matching provider is authenticated."
        };
        return Err(Error::Config(format!(
            "Model \"{value}\" is ambiguous across providers: {}. {hint} Use --provider or provider/model.",
            names.join(", ")
        )));
    }
    Ok(None)
}

pub fn split_with_profiles(
    value: &str,
    available: &[String],
    provider: &Provider,
    profiles: &super::model_profiles::Profiles,
) -> Result<Choice> {
    let mut ids = available.to_vec();
    for id in profiles.model_ids(provider) {
        if !ids.contains(&id) {
            ids.push(id);
        }
    }
    let aliases = aliases(profiles, provider, &ids);
    let mut selected = split(value, &search_ids(&ids, &aliases), provider.id())?;
    selected.model = canonical(selected.model, &ids, &aliases);
    Ok(selected)
}

fn aliases(
    profiles: &super::model_profiles::Profiles,
    provider: &Provider,
    ids: &[String],
) -> Vec<(String, String)> {
    ids.iter()
        .filter_map(|id| {
            let name = profiles.model(provider, id)?.name?;
            (name != *id).then(|| (id.clone(), name))
        })
        .collect()
}

fn search_ids(ids: &[String], aliases: &[(String, String)]) -> Vec<String> {
    ids.iter()
        .chain(aliases.iter().map(|(_, alias)| alias))
        .cloned()
        .collect()
}

fn canonical(model: String, ids: &[String], aliases: &[(String, String)]) -> String {
    if ids.contains(&model) {
        return model;
    }
    aliases
        .iter()
        .find_map(|(id, alias)| (*alias == model).then(|| id.clone()))
        .unwrap_or(model)
}

pub fn next_model(args: &Cli, available: &[ScopedChoice], reverse: bool) -> Option<ScopedChoice> {
    let count = available.len();
    if count <= 1 {
        return None;
    }
    let current = available
        .iter()
        .position(|model| model.provider == args.provider && model.model == args.model)
        .unwrap_or(0);
    let next = if reverse {
        (current + count - 1) % count
    } else {
        (current + 1) % count
    };
    Some(available[next].clone())
}
fn unqualified<'a>(value: &'a str, provider: &str) -> &'a str {
    let value = value.trim();
    value
        .split_once('/')
        .filter(|(prefix, _)| prefix.eq_ignore_ascii_case(provider))
        .map_or(value, |(_, model)| model.trim())
}

fn exact<'a>(value: &str, available: &'a [String], provider: &str) -> Option<&'a String> {
    available
        .iter()
        .find(|model| model.eq_ignore_ascii_case(unqualified(value, provider)))
        .or_else(|| {
            available
                .iter()
                .find(|model| model.eq_ignore_ascii_case(value.trim()))
        })
}

fn thinking_suffix(value: &str) -> Option<(&str, &str)> {
    let (model, thinking) = value.rsplit_once(':')?;
    (!model.is_empty()
        && matches!(
            thinking,
            "off" | "minimal" | "low" | "medium" | "high" | "xhigh" | "max"
        ))
    .then_some((model, thinking))
}

fn is_alias(model: &str) -> bool {
    !model.rsplit_once('-').is_some_and(|(_, suffix)| {
        suffix.len() == 8 && suffix.bytes().all(|byte| byte.is_ascii_digit())
    })
}

fn partial<'a>(value: &str, available: &'a [String], provider: &str) -> Option<&'a String> {
    let query = unqualified(value, provider).to_lowercase();
    if query.is_empty() {
        return None;
    }
    available
        .iter()
        .filter_map(|model| {
            let lower = model.to_lowercase();
            lower
                .contains(&query)
                .then_some(((is_alias(model), lower), model))
        })
        .max_by(|(left, _), (right, _)| left.cmp(right))
        .map(|(_, model)| model)
}

fn catalog_choice(
    value: &str,
    available: &[String],
    lenient: bool,
    provider: &str,
) -> Option<(Choice, Option<String>)> {
    let mut text = value.trim();
    let mut thinking = None;
    let mut warning = None;
    loop {
        if let Some(model) =
            exact(text, available, provider).or_else(|| partial(text, available, provider))
        {
            return Some((
                Choice {
                    model: model.clone(),
                    thinking: if warning.is_some() { None } else { thinking },
                },
                warning,
            ));
        }
        if let Some((base, level)) = thinking_suffix(text) {
            thinking.get_or_insert_with(|| level.to_owned());
            text = base;
        } else if lenient {
            let (base, suffix) = text.rsplit_once(':')?;
            if base.is_empty() {
                return None;
            }
            warning.get_or_insert_with(|| format!("Invalid thinking level \"{suffix}\" in pattern \"{value}\". Using default instead."));
            text = base;
        } else {
            return None;
        }
    }
}

pub fn split(value: &str, available: &[String], provider: &str) -> Result<Choice> {
    let named = unqualified(value, provider);
    if named.is_empty() {
        return Err(Error::Config("model ID is required".into()));
    }
    if let Some((choice, _)) = catalog_choice(value, available, false, provider) {
        return Ok(choice);
    }
    let (model, thinking) = thinking_suffix(named).map_or((named, None), |(model, level)| {
        (model, Some(level.to_owned()))
    });
    Ok(Choice {
        model: model.into(),
        thinking,
    })
}

#[derive(Default)]
pub struct Scope {
    pub choices: Vec<Choice>,
    pub warnings: Vec<String>,
}

pub fn scope<'a>(
    patterns: impl IntoIterator<Item = &'a str>,
    available: &[String],
    provider: &str,
) -> Scope {
    let mut scope = Scope::default();
    for pattern in patterns
        .into_iter()
        .map(str::trim)
        .filter(|pattern| !pattern.is_empty())
    {
        let choices = if pattern.contains(['*', '?', '[']) {
            let (pattern, thinking) = thinking_suffix(pattern)
                .map_or((pattern, None), |(base, level)| (base, Some(level)));
            let matches = if let Some(model) = exact(pattern, available, provider) {
                vec![model]
            } else if let Ok(glob) = globset::GlobBuilder::new(pattern)
                .case_insensitive(true)
                .literal_separator(true)
                .backslash_escape(true)
                .build()
            {
                let matcher = glob.compile_matcher();
                available
                    .iter()
                    .filter(|model| {
                        matcher.is_match(model) || matcher.is_match(format!("{provider}/{model}"))
                    })
                    .collect()
            } else {
                Vec::new()
            };
            matches
                .into_iter()
                .map(|model| Choice {
                    model: model.clone(),
                    thinking: thinking.map(str::to_owned),
                })
                .collect()
        } else if let Some((choice, warning)) = catalog_choice(pattern, available, true, provider) {
            scope.warnings.extend(warning);
            vec![choice]
        } else {
            Vec::new()
        };
        if choices.is_empty() {
            scope
                .warnings
                .push(format!("No models match pattern \"{pattern}\""));
        }
        for choice in choices {
            if scope
                .choices
                .iter()
                .all(|existing| existing.model != choice.model)
            {
                scope.choices.push(choice);
            }
        }
    }
    scope
}
pub fn fuzzy(query: &str, value: &str) -> bool {
    let value = value.to_lowercase();
    let mut characters = value.chars();
    query
        .to_lowercase()
        .chars()
        .all(|wanted| characters.any(|character| character == wanted))
}

#[cfg(test)]
mod tests {
    use super::{Choice, Scope};

    #[tokio::test]
    async fn unqueried_dynamic_catalog_is_not_complete() {
        use clap::Parser;
        let args = super::Cli::parse_from(["pi"]);
        let catalog = super::catalog(&args, super::Provider::Ollama, false)
            .await
            .unwrap();
        assert!(!catalog.complete);
    }

    #[test]
    fn copilot_cached_catalog_filters_new_models_by_subscription() {
        use clap::Parser;
        use serde_json::json;
        let mut args = super::Cli::parse_from(["pi"]);
        let provider = super::Provider::named("github-copilot").unwrap();
        args.provider = provider.clone();
        args.endpoint = args.model_profiles.endpoint(&provider).into();
        args.model = "gpt-6.2-sol".into();
        args.model_keys = crate::model_auth::Keys::parse(&json!({
            "github-copilot": {
                "type": "oauth", "access": "fixture", "refresh": "other-fixture",
                "expires": 4_000_000_000_000_u64,
                "availableModelIds": ["gemini-4-neon", "gpt-6.2-sol", "unknown"]
            }
        }));
        let selected = super::cached_catalogs(&args)
            .into_iter()
            .find(|catalog| catalog.provider == provider)
            .unwrap();
        assert_eq!(selected.models, ["gemini-4-neon", "gpt-6.2-sol"]);
    }

    #[test]
    fn scope_keeps_provider_identity_and_pattern_order() {
        use super::{Catalog, Provider, scoped_choices};
        let catalogs = [
            Catalog {
                provider: Provider::Ollama,
                client: Provider::Ollama
                    .connect(
                        Provider::Ollama.adapter(),
                        Provider::Ollama.default_endpoint(),
                        None,
                        false,
                    )
                    .unwrap(),
                models: [
                    "same",
                    "exact-extra",
                    "literal[1m]",
                    "literal1",
                    "anthropic/same",
                    "alias-extra",
                ]
                .map(str::to_owned)
                .to_vec(),
                aliases: Vec::new(),
                probe_details: false,
                complete: false,
            },
            Catalog {
                provider: Provider::Anthropic,
                client: Provider::Anthropic
                    .connect(
                        Provider::Anthropic.adapter(),
                        Provider::Anthropic.default_endpoint(),
                        Some("fixture".into()),
                        false,
                    )
                    .unwrap(),
                models: ["same", "exact", "literalm", "literal2"]
                    .map(str::to_owned)
                    .to_vec(),
                aliases: vec![("exact".into(), "alias".into())],
                probe_details: false,
                complete: false,
            },
        ];
        let (choices, warnings) = scoped_choices(
            [
                "anthropic/same:high",
                "ollama/same:off",
                "exact",
                "ollama/exact-*",
            ],
            &catalogs,
        );
        assert!(warnings.is_empty());
        assert_eq!(
            choices
                .iter()
                .map(super::ScopedChoice::pattern)
                .collect::<Vec<_>>(),
            [
                "anthropic/same:high",
                "ollama/same:off",
                "anthropic/exact",
                "ollama/exact-extra"
            ]
        );

        for (pattern, expected) in [
            ("SAME", ["ollama/same", "anthropic/same"]),
            ("same:high", ["ollama/same:high", "anthropic/same:high"]),
        ] {
            let (choices, warnings) = scoped_choices([pattern, "anthropic/same:off"], &catalogs);
            assert_eq!(
                choices
                    .iter()
                    .map(super::ScopedChoice::pattern)
                    .collect::<Vec<_>>(),
                expected
            );
            assert_eq!(
                warnings,
                [format!(
                    "Pattern \"{pattern}\" matches several providers: anthropic, ollama. Use provider/id to choose one."
                )]
            );
        }

        let (choices, warnings) = scoped_choices(
            ["literal[1m]:high", "literal[12]", "alias", "anthropic/same"],
            &catalogs,
        );
        assert!(warnings.is_empty());
        assert_eq!(
            choices
                .iter()
                .map(super::ScopedChoice::pattern)
                .collect::<Vec<_>>(),
            [
                "ollama/literal[1m]:high",
                "ollama/literal1",
                "anthropic/literal2",
                "anthropic/exact",
                "anthropic/same"
            ]
        );
    }

    #[test]
    fn cli_model_reference_prefers_the_only_authenticated_matching_gateway() {
        use clap::Parser;
        use serde_json::json;

        let mut args = super::Cli::parse_from(["pi"]);
        args.model_profiles = super::super::model_profiles::Profiles::parse(&json!({
            "providers":{
                "direct": {"api":"anthropic-messages","baseUrl":"https://direct.example/v1",
                    "models":[{"id":"other"},{"id":"named"},{"id":"direct/own"},
                        {"id":"part-long"},{"id":"actual-id","name":"fast"},{"id":"fast-pro"}]},
                "gateway-one": {"api":"openai-completions","baseUrl":"https://one.example/v1",
                    "models":[{"id":"direct/model"},{"id":"direct/named"},{"id":"model:high"},{"id":"direct/own"}]},
                "gateway-two": {"api":"openai-completions","baseUrl":"https://two.example/v1",
                    "models":[{"id":"direct/model"},{"id":"direct/named"},{"id":"direct/part"}]}
            }
        }))
        .unwrap();
        args.model_keys = super::super::model_auth::Keys::parse(&json!({
            "gateway-two":{"type":"api_key","key":"fixture","endpoint":"https://two.example/v1/chat/completions"}
        }));

        for (value, expected) in [
            ("DIRECT/model", "gateway-two/direct/model"),
            ("direct/model:high", "gateway-two/direct/model:high"),
            ("direct/named", "gateway-two/direct/named"),
            ("model:high", "gateway-one/model:high"),
        ] {
            let selected = super::cli_catalog_model(&args, value, false)
                .unwrap()
                .unwrap();
            assert_eq!(selected.pattern(), expected);
        }
        let inline = super::cli_catalog_model(&args, "direct/model", true)
            .err()
            .expect("inline key cannot disambiguate");
        assert!(
            inline
                .to_string()
                .contains("--api-key does not choose a provider.")
        );
        let named = super::cli_catalog_model(&args, "direct/named", true)
            .unwrap()
            .unwrap();
        assert_eq!(named.qualified(), "direct/named");

        let own = super::cli_catalog_model(&args, "direct/own", false)
            .unwrap()
            .unwrap();
        assert_eq!(own.qualified(), "direct/direct/own");
        let partial = super::cli_catalog_model(&args, "direct/part", true)
            .unwrap()
            .unwrap();
        assert_eq!(partial.qualified(), "direct/part-long");

        let display_name = super::cli_catalog_model(&args, "direct/FAST", true)
            .unwrap()
            .unwrap();
        assert_eq!(display_name.qualified(), "direct/actual-id");

        args.model_keys = Default::default();
        let unauthenticated = super::cli_catalog_model(&args, "direct/model", false)
            .err()
            .expect("no authenticated gateway");
        assert!(unauthenticated.to_string().contains(
            "gateway-one/direct/model, gateway-two/direct/model. No matching provider is authenticated."
        ));
        assert!(
            super::cli_catalog_model(&args, "local-only:latest", false)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn cli_counts_ambient_bedrock_auth_when_a_gateway_lists_the_same_model() {
        use clap::Parser;
        use serde_json::json;

        for configured in [
            &["AWS_PROFILE"][..],
            &["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"][..],
            &["AWS_BEARER_TOKEN_BEDROCK"][..],
            &["AWS_CONTAINER_CREDENTIALS_FULL_URI"][..],
            &["AWS_WEB_IDENTITY_TOKEN_FILE"][..],
        ] {
            assert!(super::bedrock_environment_configured(
                |name| configured.contains(&name)
            ));
        }
        assert!(!super::bedrock_environment_configured(
            |name| name == "AWS_ACCESS_KEY_ID"
        ));

        let mut args = super::Cli::parse_from(["pi"]);
        let model = "us.anthropic.claude-opus-4-6-v1";
        args.model_profiles = super::super::model_profiles::Profiles::parse(&json!({
            "providers":{"gateway":{"api":"openai-completions",
                "baseUrl":"https://gateway.example/v1","models":[{"id":model}]}}
        }))
        .unwrap();
        let bedrock = super::resolve_cli_catalog_model(&args, model, false, true)
            .unwrap()
            .unwrap();
        assert_eq!(bedrock.qualified(), format!("amazon-bedrock/{model}"));
        assert!(super::resolve_cli_catalog_model(&args, model, true, true).is_err());
        args.model_keys = super::super::model_auth::Keys::parse(&json!({
            "gateway":{"type":"api_key","key":"fixture",
                "endpoint":"https://gateway.example/v1/chat/completions"}
        }));
        assert!(super::resolve_cli_catalog_model(&args, model, false, true).is_err());
        args.settings = serde_json::from_value(json!({
            "value":{"defaultProvider":"amazon-bedrock"},
            "resource_layers":[],
            "diagnostics":[]
        }))
        .unwrap();
        let gateway = super::resolve_cli_catalog_model(&args, model, false, false)
            .unwrap()
            .unwrap();
        assert_eq!(gateway.qualified(), format!("gateway/{model}"));
        args.model_keys = Default::default();
        let default = super::resolve_cli_catalog_model(&args, model, false, false)
            .unwrap()
            .unwrap();
        assert_eq!(default.qualified(), format!("amazon-bedrock/{model}"));
        args.model_profiles = super::super::model_profiles::Profiles::parse(&json!({
            "providers":{
                "amazon-bedrock": {"models":[{"id":model,"api":"openai-completions",
                    "baseUrl":"https://relay.example/v1"}]},
                "gateway":{"api":"openai-completions",
                    "baseUrl":"https://gateway.example/v1","models":[{"id":model}]}
            }
        }))
        .unwrap();
        assert!(super::resolve_cli_catalog_model(&args, model, false, true).is_err());
    }

    #[test]
    fn cli_counts_a_configured_chatgpt_model_only_once() {
        use clap::Parser;
        use serde_json::json;

        let mut args = super::Cli::parse_from(["pi"]);
        args.model_profiles = super::super::model_profiles::Profiles::parse(&json!({
            "providers": {
                "openai-chatgpt": {
                    "api": "openai-responses",
                    "models": [{"id": "chatgpt-custom"}]
                }
            }
        }))
        .unwrap();
        let selected = super::cli_catalog_model(&args, "chatgpt-custom", false)
            .unwrap()
            .unwrap();
        assert_eq!(selected.qualified(), "openai-chatgpt/chatgpt-custom");
    }

    fn split(value: &str, available: &[String]) -> pi_rs::Result<Choice> {
        super::split(value, available, "ollama")
    }

    fn scope<'a>(patterns: impl IntoIterator<Item = &'a str>, available: &[String]) -> Scope {
        super::scope(patterns, available, "ollama")
    }
    #[test]
    fn cycling_uses_the_first_slot_as_origin_when_current_model_is_missing() {
        use super::{Cli, Provider, ScopedChoice, next_model};
        use clap::Parser;
        let args = Cli::try_parse_from(["pi"]).unwrap();
        let choices = ["first", "second", "third"].map(|model| ScopedChoice {
            provider: Provider::Ollama,
            model: model.into(),
            thinking: None,
        });
        assert!(next_model(&args, &choices[..1], false).is_none());
        assert!(next_model(&args, &choices[..1], true).is_none());
        assert_eq!(next_model(&args, &choices, false).unwrap().model, "second");
        assert_eq!(next_model(&args, &choices, true).unwrap().model, "third");
    }

    #[test]
    fn preserves_model_tags_and_extracts_only_known_thinking_levels() {
        assert_eq!(split("qwen3.5:4b", &[]).unwrap().model, "qwen3.5:4b");
        assert_eq!(
            split("qwen3.5:4b:high", &[]).unwrap(),
            Choice {
                model: "qwen3.5:4b".into(),
                thinking: Some("high".into())
            }
        );
    }
    #[test]
    fn qualified_references_keep_exact_tags_and_unrelated_namespaces() {
        let models = vec![
            "org/qwen:high".into(),
            "org/qwen:4b".into(),
            "ollama/literal:low".into(),
        ];
        assert_eq!(
            split("OLLAMA/org/qwen:high", &models).unwrap(),
            Choice {
                model: "org/qwen:high".into(),
                thinking: None
            }
        );
        assert_eq!(
            split("ollama/org/qwen:4b:high", &models).unwrap(),
            Choice {
                model: "org/qwen:4b".into(),
                thinking: Some("high".into())
            }
        );
        assert_eq!(
            split("ollama/literal:low", &models).unwrap().model,
            "ollama/literal:low"
        );
        assert_eq!(split("other/qwen:4b", &[]).unwrap().model, "other/qwen:4b");
        assert_eq!(scope(["ollama/org/*:medium"], &models).choices.len(), 2);
        assert!(split("ollama/", &models).is_err());
    }

    #[test]
    fn scopes_keep_pattern_order_deduplicate_and_filter_case_insensitively() {
        let models = vec!["alpha:4b".into(), "beta:8b".into(), "beta:4b".into()];
        let choices = scope(["BETA*:high", "alpha:4b", "beta:4"], &models).choices;
        assert_eq!(
            choices
                .iter()
                .map(|choice| choice.model.as_str())
                .collect::<Vec<_>>(),
            vec!["beta:8b", "beta:4b", "alpha:4b"]
        );
        assert_eq!(choices[0].thinking.as_deref(), Some("high"));
        assert_eq!(scope(["missing"], &models).warnings.len(), 1);
        assert_eq!(scope(["alpha:4b", ""], &models).choices.len(), 1);
    }
    #[test]
    fn scope_prefers_aliases_and_reports_unmatched_patterns_and_invalid_thinking() {
        let models = vec![
            "alpha-1".into(),
            "alpha-1-20260101".into(),
            "alpha-2".into(),
            "alpha-3-20260501".into(),
            "org/bracketed[1m]".into(),
            "org/item-1".into(),
            "org/item-2".into(),
            "org/nested/item-2".into(),
        ];
        let scope = scope(
            [
                "missing",
                "alpha:invalid",
                "ollama/org/bracketed[1m]:high",
                "ollama/org/item-[12]:low",
            ],
            &models,
        );
        assert_eq!(
            scope.choices,
            vec![
                Choice {
                    model: "alpha-2".into(),
                    thinking: None
                },
                Choice {
                    model: "org/bracketed[1m]".into(),
                    thinking: Some("high".into())
                },
                Choice {
                    model: "org/item-1".into(),
                    thinking: Some("low".into())
                },
                Choice {
                    model: "org/item-2".into(),
                    thinking: Some("low".into())
                },
            ]
        );
        assert_eq!(
            scope.warnings,
            [
                "No models match pattern \"missing\"",
                "Invalid thinking level \"invalid\" in pattern \"alpha:invalid\". Using default instead.",
            ]
        );
        assert_eq!(split("alpha:medium", &models).unwrap().model, "alpha-2");
    }

    #[test]
    fn restored_sessions_stay_inside_the_requested_scope() {
        use clap::Parser;
        let mut args = super::super::Cli::try_parse_from(["pi"]).unwrap();
        args.model_scope = vec![super::ScopedChoice {
            provider: super::Provider::Ollama,
            model: "beta:4b".into(),
            thinking: Some("high".into()),
        }];
        args.model = "beta:4b".into();
        args.restore_model_selection("alpha:4b".into(), Some("low".into()), None, false);
        assert_eq!(args.model, "beta:4b");
        assert_eq!(args.thinking.as_deref(), Some("high"));
        args.thinking_explicit = true;
        args.thinking = Some("off".into());
        args.restore_model_selection("beta:4b".into(), Some("low".into()), None, false);
        assert_eq!(args.thinking.as_deref(), Some("off"));
    }
}
