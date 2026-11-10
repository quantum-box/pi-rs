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

pub fn qualified_model(value: &str) -> Option<(Provider, &str)> {
    let (provider, model) = value.split_once('/')?;
    Some((Provider::named(provider)?, model))
}

pub struct Catalog {
    pub provider: Provider,
    pub client: super::providers::Client,
    pub models: Vec<String>,
    aliases: Vec<(String, String)>,
    probe_details: bool,
}

impl Catalog {
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
            if let Some(metadata) = profiles.model(self.provider, model) {
                metadata.apply(self.provider, details.entry(model.clone()).or_default());
            }
        }
        details
    }

    fn has_exact(&self, model: &str) -> bool {
        exact(model, &self.models, self.provider.id()).is_some()
            || self
                .aliases
                .iter()
                .any(|(_, name)| name.eq_ignore_ascii_case(unqualified(model, self.provider.id())))
    }
}

pub async fn catalog(args: &Cli, provider: Provider, probe: bool) -> Result<Catalog> {
    let endpoint = args.endpoint_for(provider);
    let client = provider.connect(endpoint, args.model_token_for(provider, endpoint))?;
    let mut models = args.model_profiles.model_ids(provider);
    let mut probe_details = false;
    if probe {
        match client.models().await {
            Ok(discovered) => {
                let mut seen: std::collections::BTreeSet<_> = models.iter().cloned().collect();
                models.extend(discovered.into_iter().filter(|id| seen.insert(id.clone())));
                probe_details = true;
            }
            Err(error) if models.is_empty() => return Err(error),
            Err(_) => {}
        }
    }
    let aliases = aliases(&args.model_profiles, provider, &models);
    Ok(Catalog {
        provider,
        client,
        models,
        aliases,
        probe_details,
    })
}

pub async fn catalogs(args: &Cli, automatic: bool) -> Result<Vec<Catalog>> {
    let providers = std::iter::once(args.provider).chain(
        Provider::ALL
            .into_iter()
            .filter(|provider| *provider != args.provider),
    );
    let discovered = futures_util::future::join_all(providers.filter_map(|provider| {
        let probe = !automatic || args.allow_endpoint_probes(args.endpoint_for(provider));
        (probe || !args.model_profiles.model_ids(provider).is_empty())
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
        let qualified = qualified_model(pattern)
            .map(|(provider, _)| provider)
            .or_else(|| {
                if glob {
                    return None;
                }
                catalogs
                    .iter()
                    .find(|catalog| {
                        catalog.has_exact(pattern)
                            || thinking_suffix(pattern)
                                .is_some_and(|(model, _)| catalog.has_exact(model))
                    })
                    .map(|catalog| catalog.provider)
            });
        let mut matched = false;
        for catalog in catalogs
            .iter()
            .filter(|catalog| qualified.is_none_or(|provider| provider == catalog.provider))
        {
            let candidates = search_ids(&catalog.models, &catalog.aliases);
            let resolved = scope([pattern], &candidates, catalog.provider.id());
            if resolved.choices.is_empty() {
                continue;
            }
            matched = true;
            warnings.extend(resolved.warnings);
            for mut choice in resolved.choices {
                choice.model = canonical(choice.model, &catalog.models, &catalog.aliases);
                if selected.iter().all(|existing| {
                    existing.provider != catalog.provider || existing.model != choice.model
                }) {
                    selected.push(ScopedChoice {
                        provider: catalog.provider,
                        model: choice.model,
                        thinking: choice.thinking,
                    });
                }
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

pub fn split_with_profiles(
    value: &str,
    available: &[String],
    provider: Provider,
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
    provider: Provider,
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
    if count == 0 {
        return None;
    }
    let current = available
        .iter()
        .position(|model| model.provider == args.provider && model.model == args.model);
    let next = match (current, reverse) {
        (Some(index), true) => (index + count - 1) % count,
        (Some(index), false) => (index + 1) % count,
        (None, true) => count - 1,
        (None, false) => 0,
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

    #[test]
    fn scope_keeps_provider_identity_and_pattern_order() {
        use super::{Catalog, Provider, scoped_choices};
        let catalogs = [
            Catalog {
                provider: Provider::Ollama,
                client: Provider::Ollama
                    .connect(Provider::Ollama.default_endpoint(), None)
                    .unwrap(),
                models: vec!["same".into(), "exact-extra".into()],
                aliases: Vec::new(),
                probe_details: false,
            },
            Catalog {
                provider: Provider::Anthropic,
                client: Provider::Anthropic
                    .connect(
                        Provider::Anthropic.default_endpoint(),
                        Some("fixture".into()),
                    )
                    .unwrap(),
                models: vec!["same".into(), "exact".into()],
                aliases: Vec::new(),
                probe_details: false,
            },
        ];
        let (choices, warnings) = scoped_choices(
            [
                "anthropic/same:high",
                "ollama/same:off",
                "exact",
                "ollama/*",
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
    }

    fn split(value: &str, available: &[String]) -> pi_rs::Result<Choice> {
        super::split(value, available, "ollama")
    }

    fn scope<'a>(patterns: impl IntoIterator<Item = &'a str>, available: &[String]) -> Scope {
        super::scope(patterns, available, "ollama")
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
        args.restore_model_selection("alpha:4b".into(), Some("low".into()), None);
        assert_eq!(args.model, "beta:4b");
        assert_eq!(args.thinking.as_deref(), Some("high"));
        args.thinking_explicit = true;
        args.thinking = Some("off".into());
        args.restore_model_selection("beta:4b".into(), Some("low".into()), None);
        assert_eq!(args.thinking.as_deref(), Some("off"));
    }
}
