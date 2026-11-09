//! Model scopes and thinking suffixes shared by startup and terminal cycling.
use pi_rs::{Error, Result};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Choice {
    pub model: String,
    pub thinking: Option<String>,
}
pub fn split(value: &str) -> Choice {
    if let Some((model, thinking)) = value.rsplit_once(':')
        && !model.is_empty()
        && matches!(
            thinking,
            "off" | "minimal" | "low" | "medium" | "high" | "xhigh" | "max"
        )
    {
        return Choice {
            model: model.into(),
            thinking: Some(thinking.into()),
        };
    }
    Choice {
        model: value.into(),
        thinking: None,
    }
}
pub fn scope(patterns: &str, available: &[String]) -> Result<Vec<Choice>> {
    let mut choices: Vec<Choice> = Vec::new();
    for value in patterns.split(',').map(str::trim) {
        if value.is_empty() {
            return Err(Error::Config(
                "model scope contains an empty pattern".into(),
            ));
        }
        let choice = split(value);
        let exact = available
            .iter()
            .find(|model| model.eq_ignore_ascii_case(&choice.model));
        let matches: Vec<_> = if let Some(model) = exact {
            vec![model]
        } else if choice.model.contains(['*', '?']) {
            let matcher = globset::GlobBuilder::new(&choice.model)
                .case_insensitive(true)
                .build()
                .map_err(|error| Error::Config(error.to_string()))?
                .compile_matcher();
            available
                .iter()
                .filter(|model| matcher.is_match(model))
                .collect()
        } else {
            available
                .iter()
                .filter(|model| fuzzy(&choice.model, model))
                .collect()
        };
        if matches.is_empty() {
            return Err(Error::Config(format!(
                "no available model matches {:?}",
                choice.model
            )));
        }
        for model in matches {
            if choices.iter().all(|choice| choice.model != *model) {
                choices.push(Choice {
                    model: model.clone(),
                    thinking: choice.thinking.clone(),
                });
            }
        }
    }
    Ok(choices)
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
    use super::*;
    #[test]
    fn preserves_model_tags_and_extracts_only_known_thinking_levels() {
        assert_eq!(split("qwen3.5:4b").model, "qwen3.5:4b");
        assert_eq!(
            split("qwen3.5:4b:high"),
            Choice {
                model: "qwen3.5:4b".into(),
                thinking: Some("high".into())
            }
        );
    }
    #[test]
    fn scopes_keep_pattern_order_deduplicate_and_filter_case_insensitively() {
        let models = vec!["alpha:4b".into(), "beta:8b".into(), "beta:4b".into()];
        let choices = scope("BETA*:high,alpha:4b,bt4", &models).unwrap();
        assert_eq!(
            choices
                .iter()
                .map(|choice| choice.model.as_str())
                .collect::<Vec<_>>(),
            vec!["beta:8b", "beta:4b", "alpha:4b"]
        );
        assert_eq!(choices[0].thinking.as_deref(), Some("high"));
        assert!(scope("missing", &models).is_err());
        assert!(scope("alpha:4b,", &models).is_err());
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
        args.restore_model_selection("alpha:4b".into(), Some("low".into()));
        assert_eq!(args.model, "beta:4b");
        assert_eq!(args.thinking.as_deref(), Some("high"));
        args.thinking_explicit = true;
        args.thinking = Some("off".into());
        args.restore_model_selection("beta:4b".into(), Some("low".into()));
        assert_eq!(args.thinking.as_deref(), Some("off"));
    }
}
