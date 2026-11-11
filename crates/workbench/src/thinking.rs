//! Effective thinking controls shared by RPC and the terminal.
use pi_rs::{Error, Result, Session};

use super::Cli;

pub(super) const LEVELS: &[&str] = &["off", "minimal", "low", "medium", "high", "xhigh", "max"];

pub(super) fn reported(args: &Cli) -> Option<&[String]> {
    args.thinking_profile
        .as_ref()
        .filter(|(model, _)| model == &args.model)
        .map(|(_, levels)| levels.as_slice())
}

pub(super) fn available(args: &mut Cli) -> Vec<String> {
    if let Some(levels) = reported(args) {
        return levels.to_vec();
    }
    let adapter = args
        .model_profiles
        .model_adapter(&args.provider, &args.model);
    if let Some(levels) = args
        .model_profiles
        .model(&args.provider, &args.model)
        .and_then(|model| model.levels(adapter))
    {
        args.thinking_profile = Some((args.model.clone(), levels.clone()));
        return levels;
    }
    let fallback = || match adapter {
        super::providers::Adapter::ChatCompletions | super::providers::Adapter::PiMessages => {
            LEVELS.iter().map(|level| (*level).into()).collect()
        }
        super::providers::Adapter::Anthropic
            if matches!(args.provider.id(), "fireworks" | "openrouter")
                && args
                    .model_profiles
                    .model(&args.provider, &args.model)
                    .is_some_and(|model| model.reasoning == Some(true)) =>
        {
            ["off", "low", "medium", "high"].map(str::to_owned).to_vec()
        }
        super::providers::Adapter::Anthropic => vec!["off".into()],
        super::providers::Adapter::Google => pi_rs::google::model_thinking_levels(&args.model),
    };
    if !args.allow_model_probes() {
        return fallback();
    }
    let connection = args.clone();
    let discovered = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .ok()?;
        runtime.block_on(async {
            let client = connection
                .connect_provider(&connection.provider, Some(&connection.model))
                .ok()?;
            client.thinking_levels(&connection.model).await
        })
    })
    .join()
    .ok()
    .flatten();
    if let Some(levels) = discovered {
        args.thinking_profile = Some((args.model.clone(), levels.clone()));
        levels
    } else {
        fallback()
    }
}

pub(super) fn clamp<'a>(requested: &str, levels: &'a [String]) -> &'a str {
    if let Some(level) = levels.iter().find(|level| *level == requested) {
        return level;
    }
    if let Some(index) = LEVELS.iter().position(|level| *level == requested) {
        for candidate in LEVELS[index..].iter().chain(LEVELS[..index].iter().rev()) {
            if let Some(level) = levels.iter().find(|level| level == candidate) {
                return level;
            }
        }
    }
    levels.first().map_or("off", String::as_str)
}

pub(super) fn adjust(args: &mut Cli) {
    if let Some(requested) = args.thinking.clone() {
        let levels = available(args);
        if reported(args).is_some()
            || args
                .model_profiles
                .model_adapter(&args.provider, &args.model)
                != super::providers::Adapter::ChatCompletions
        {
            args.thinking = Some(clamp(&requested, &levels).into());
        }
    }
}

pub(super) fn set(args: &mut Cli, requested: &str) -> Result<()> {
    let levels = available(args);
    if !LEVELS.contains(&requested) && !levels.iter().any(|level| level == requested) {
        return Err(Error::Config("invalid thinking level".into()));
    }
    persist(args, clamp(requested, &levels))
}

pub(super) fn cycle(args: &mut Cli) -> Result<Option<String>> {
    let levels = available(args);
    if levels.iter().all(|level| level == "off") {
        return Ok(None);
    }
    let current = args.thinking.as_deref().unwrap_or("off");
    let index = levels
        .iter()
        .position(|level| level == current)
        .map_or(0, |index| (index + 1) % levels.len());
    let next = levels[index].clone();
    persist(args, &next)?;
    Ok(Some(next))
}

fn persist(args: &mut Cli, level: &str) -> Result<()> {
    Session::open(&args.session)?.select_provider_model(
        args.recorded_provider(),
        &args.model,
        Some(level),
    )?;
    args.thinking = Some(level.into());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clamp_prefers_supported_higher_levels_before_lower_levels() {
        let toggle = vec!["off".into(), "high".into()];
        assert_eq!(clamp("low", &toggle), "high");
        assert_eq!(clamp("max", &toggle), "high");
        assert_eq!(clamp("off", &toggle), "off");
        let required = vec!["low".into(), "medium".into(), "focused".into()];
        assert_eq!(clamp("off", &required), "low");
        assert_eq!(clamp("focused", &required), "focused");
        assert_eq!(clamp("max", &required), "medium");
    }
}
