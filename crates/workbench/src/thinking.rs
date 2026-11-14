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
    let adapter = args
        .model_profiles
        .model_adapter(&args.provider, &args.model);
    let metadata = args.model_profiles.model(&args.provider, &args.model);
    let required = adapter == super::providers::Adapter::Bedrock
        && pi_rs::bedrock::requires_thinking(
            &args.model,
            metadata.as_ref().and_then(|model| model.name.as_deref()),
        );
    let filter_levels = |mut levels: Vec<String>| {
        if required {
            levels.retain(|level| level != "off");
            if levels.is_empty() {
                levels.push("low".into());
            }
        }
        levels
    };
    if let Some(levels) = reported(args) {
        return filter_levels(levels.to_vec());
    }
    let configured = metadata.as_ref().and_then(|model| model.levels(adapter));
    let discover_first = adapter == super::providers::Adapter::Anthropic
        && args.provider.id() == "anthropic"
        && args.allow_model_probes()
        && metadata.as_ref().is_some_and(|model| {
            model.reasoning == Some(true)
                && model.adaptive_thinking().is_none()
                && !model.has_thinking_level_map()
        });
    if let Some(levels) = configured.as_ref().filter(|_| !discover_first) {
        let levels = filter_levels(levels.clone());
        args.thinking_profile = Some((args.model.clone(), levels.clone()));
        return levels;
    }
    let fallback = || match adapter {
        super::providers::Adapter::ChatCompletions | super::providers::Adapter::PiMessages => {
            LEVELS.iter().map(|level| (*level).into()).collect()
        }
        super::providers::Adapter::OpenAiResponses => pi_rs::openai::thinking_levels(&args.model),
        super::providers::Adapter::Anthropic
            if matches!(
                args.provider.id(),
                "fireworks" | "openrouter" | "vercel-ai-gateway"
            ) && args
                .model_profiles
                .model(&args.provider, &args.model)
                .is_some_and(|model| model.reasoning == Some(true)) =>
        {
            ["off", "low", "medium", "high"].map(str::to_owned).to_vec()
        }
        super::providers::Adapter::Anthropic => vec!["off".into()],
        super::providers::Adapter::Bedrock => {
            let metadata = args.model_profiles.model(&args.provider, &args.model);
            if metadata
                .as_ref()
                .is_some_and(|model| model.reasoning == Some(false))
            {
                vec!["off".into()]
            } else {
                pi_rs::bedrock::thinking_levels(
                    &args.model,
                    metadata.as_ref().and_then(|model| model.name.as_deref()),
                )
            }
        }
        super::providers::Adapter::Google => pi_rs::google::model_thinking_levels(&args.model),
    };
    if !args.allow_model_probes() {
        return filter_levels(fallback());
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
        let levels = filter_levels(levels);
        args.thinking_profile = Some((args.model.clone(), levels.clone()));
        levels
    } else {
        filter_levels(configured.unwrap_or_else(fallback))
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
    let reasoning = args
        .model_profiles
        .model(&args.provider, &args.model)
        .and_then(|model| model.reasoning);
    let levels = available(args);
    if levels.is_empty() || (reasoning != Some(true) && levels.iter().all(|level| level == "off")) {
        return Ok(None);
    }
    let current = args.thinking.as_deref().unwrap_or("off");
    let index = levels
        .iter()
        .position(|level| level == current)
        .map_or(0, |index| (index + 1) % levels.len());
    let next = levels[index].clone();
    if args.thinking.as_deref() != Some(next.as_str()) {
        persist(args, &next)?;
    }
    Ok(Some(next))
}

fn persist(args: &mut Cli, level: &str) -> Result<()> {
    let previous_level = args.thinking.as_deref().unwrap_or("off").to_owned();
    Session::open(&args.session)?.select_thinking_level(
        args.recorded_provider(),
        &args.model,
        level,
    )?;
    args.thinking = Some(level.into());
    if previous_level != level {
        args.selection_events
            .push(pi_rs::LifecycleEvent::ThinkingLevelSelect {
                level: level.into(),
                previous_level,
            });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cycle_uses_reasoning_flag_when_only_off_is_available() {
        use clap::Parser;
        let directory = tempfile::tempdir().unwrap();
        let mut args = Cli::try_parse_from(["pi", "--offline"]).unwrap();
        args.session = directory.path().join("session.jsonl");
        args.model_profiles = super::super::model_profiles::Profiles::parse(&serde_json::json!({
            "providers":{"anthropic":{"models":[
                {"id":"off-only", "reasoning":true,
                 "thinkingLevelMap":{"minimal":null,"low":null,"medium":null,"high":null}},
                {"id":"plain", "reasoning":false}
            ]}}
        }))
        .unwrap();
        args.provider = args.model_profiles.provider("anthropic").unwrap();
        args.model = "off-only".into();
        args.thinking = Some("off".into());
        assert_eq!(available(&mut args), ["off"]);
        assert_eq!(cycle(&mut args).unwrap().as_deref(), Some("off"));
        assert!(!args.session.exists());
        assert!(args.selection_events.is_empty());

        args.model = "plain".into();
        assert_eq!(cycle(&mut args).unwrap(), None);
        assert!(!args.session.exists());
    }

    #[test]
    fn declared_reasoning_displays_discovered_anthropic_capabilities() {
        use clap::Parser;
        use std::io::{BufRead, BufReader, Write};
        use std::net::TcpListener;

        for (capabilities, expected, discovered) in [
            (
                serde_json::json!({}),
                vec!["off", "minimal", "low", "medium", "high"],
                false,
            ),
            (
                serde_json::json!({"thinking":{"supported":false}}),
                vec!["off"],
                true,
            ),
            (
                serde_json::json!({"thinking":{"supported":true,"types":{"adaptive":{"supported":true},"disabled":{"supported":true}}},"effort":{"low":{"supported":true},"high":{"supported":true}}}),
                vec!["off", "low", "high"],
                true,
            ),
        ] {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let endpoint = format!("http://{}/v1/messages", listener.local_addr().unwrap());
            let server = std::thread::spawn(move || {
                let (mut stream, _) = listener.accept().unwrap();
                stream
                    .set_read_timeout(Some(std::time::Duration::from_secs(5)))
                    .unwrap();
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                let mut line = String::new();
                loop {
                    line.clear();
                    reader.read_line(&mut line).unwrap();
                    if line == "\r\n" || line.is_empty() {
                        break;
                    }
                }
                let body = serde_json::json!({"id":"fixture-model","capabilities":capabilities})
                    .to_string();
                write!(stream, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
            });
            let mut args = Cli::try_parse_from([
                "pi",
                "--provider",
                "anthropic",
                "--model",
                "fixture-model",
                "--endpoint",
                &endpoint,
                "--api-key",
                "fixture",
            ])
            .unwrap();
            args.model_profiles =
                super::super::model_profiles::Profiles::parse(&serde_json::json!({
                    "providers":{"anthropic":{"models":[{"id":"fixture-model","reasoning":true}]}}
                }))
                .unwrap();
            args.provider = args.model_profiles.provider("anthropic").unwrap();
            assert_eq!(available(&mut args), expected);
            if discovered {
                assert_eq!(reported(&args).unwrap(), expected);
            } else {
                assert!(reported(&args).is_none());
            }
            server.join().unwrap();
        }
    }

    #[test]
    fn configured_anthropic_reasoning_without_map_supports_default_levels() {
        use clap::Parser;
        let directory = tempfile::tempdir().unwrap();
        let mut args = Cli::try_parse_from(["pi", "--offline"]).unwrap();
        args.session = directory.path().join("session.jsonl");
        args.model_profiles = super::super::model_profiles::Profiles::parse(&serde_json::json!({
            "providers": {"anthropic": {"models": [
                {"id":"custom-reasoning", "reasoning":true},
                {"id":"custom-plain", "reasoning":false},
                {"id":"custom-mapped", "reasoning":true,
                 "thinkingLevelMap":{"off":null, "medium":null, "xhigh":"high"}}
            ]}}
        }))
        .unwrap();
        args.provider = args.model_profiles.provider("anthropic").unwrap();
        args.model = "custom-reasoning".into();
        args.thinking = Some("high".into());
        adjust(&mut args);
        assert_eq!(args.thinking.as_deref(), Some("high"));
        assert_eq!(
            available(&mut args),
            ["off", "minimal", "low", "medium", "high"]
        );
        assert_eq!(cycle(&mut args).unwrap().as_deref(), Some("off"));
        assert_eq!(cycle(&mut args).unwrap().as_deref(), Some("minimal"));

        args.model = "custom-plain".into();
        assert_eq!(available(&mut args), ["off"]);
        args.model = "custom-mapped".into();
        assert_eq!(available(&mut args), ["minimal", "low", "high", "xhigh"]);
    }

    #[test]
    fn vercel_reasoning_model_retains_offline_thinking_level() {
        use clap::Parser;
        let mut args = Cli::try_parse_from(["pi", "--offline"]).unwrap();
        args.model_profiles =
            super::super::model_profiles::Profiles::parse(&serde_json::json!({})).unwrap();
        args.provider = args.model_profiles.provider("vercel-ai-gateway").unwrap();
        args.model = "alibaba/qwen-3-14b".into();
        args.thinking = Some("high".into());
        adjust(&mut args);
        assert_eq!(args.thinking.as_deref(), Some("high"));
    }

    #[test]
    fn bedrock_thinking_levels_follow_offline_model_capabilities() {
        use clap::Parser;
        let mut args = Cli::try_parse_from(["pi", "--offline"]).unwrap();
        args.model_profiles =
            super::super::model_profiles::Profiles::parse(&serde_json::json!({})).unwrap();
        args.provider = args.model_profiles.provider("amazon-bedrock").unwrap();
        args.model = "anthropic.claude-sonnet-4-5-20250929-v1:0".into();
        args.thinking = Some("high".into());
        adjust(&mut args);
        assert_eq!(args.thinking.as_deref(), Some("high"));
        assert!(available(&mut args).contains(&"medium".into()));
        args.model = "global.anthropic.claude-opus-4-8".into();
        assert!(available(&mut args).contains(&"xhigh".into()));
        args.model = "amazon.nova-lite-v1:0".into();
        assert_eq!(available(&mut args), vec!["off".to_owned()]);
        args.model_profiles = super::super::model_profiles::Profiles::parse(&serde_json::json!({"providers":{"amazon-bedrock":{"models":[{"id":"opaque", "name":"Claude Fable 5", "reasoning":true, "thinkingLevelMap":{"high":"high"}}]}}})).unwrap();
        args.provider = args.model_profiles.provider("amazon-bedrock").unwrap();
        args.model = "opaque".into();
        let levels = available(&mut args);
        assert!(!levels.contains(&"off".into()));
        assert!(levels.contains(&"low".into()));
        args.thinking = Some("off".into());
        adjust(&mut args);
        assert_ne!(args.thinking.as_deref(), Some("off"));
    }

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
