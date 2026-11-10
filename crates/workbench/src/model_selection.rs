//! Model scopes and thinking suffixes shared by startup and terminal cycling.
use pi_rs::{Error, Result};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Choice {
    pub model: String,
    pub thinking: Option<String>,
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
        args.model_scope = vec![Choice {
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
