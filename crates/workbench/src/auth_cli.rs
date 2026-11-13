//! Check or explicitly print credentials using the normal endpoint-bound auth sources.
use std::io::{self, Write};
use std::time::Duration;

use clap::Subcommand;
use pi_rs::{Error, Result};
use serde_json::json;

use super::{Cli, Provider, model_selection, radius_auth};

#[derive(Clone, Subcommand)]
pub(super) enum AuthCommand {
    /// Check whether a provider has usable credentials.
    Check {
        #[arg(long)]
        json: bool,
        #[arg(long)]
        credentials: bool,
        #[arg(long)]
        no_refresh: bool,
    },
    /// Print the configured API key for the selected provider.
    PrintApiKey,
    /// Print the OAuth bearer token for the selected provider.
    PrintBearerToken {
        #[arg(long, value_parser = expiry)]
        min_expiry: Option<Duration>,
    },
}

struct Candidate {
    provider: Provider,
    model: Option<String>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum AuthType {
    ApiKey,
    OAuth,
}

struct Resolved {
    kind: AuthType,
    value: String,
}

fn expiry(input: &str) -> std::result::Result<Duration, String> {
    let input = input.to_ascii_lowercase();
    let split = input.find(|character: char| !character.is_ascii_digit());
    let duration = split.and_then(|index| {
        let amount = input[..index].parse::<u64>().ok()?;
        let scale = match &input[index..] {
            "ms" => 1,
            "s" => 1_000,
            "m" => 60_000,
            "h" => 3_600_000,
            _ => return None,
        };
        amount.checked_mul(scale).map(Duration::from_millis)
    });
    duration.ok_or_else(|| "--min-expiry requires a duration such as 30m or 1h".into())
}

fn candidates(args: &Cli) -> Result<Vec<Candidate>> {
    let selected = args.provider_argument.as_deref().map(str::trim);
    let model = args.model_explicit.then_some(args.model.trim());
    if selected.is_none_or(str::is_empty) && model.is_none_or(str::is_empty) {
        return Err(Error::Config("auth requires --provider or --model".into()));
    }
    if let Some(name) = selected {
        let Some(provider) = args.model_profiles.provider(name) else {
            return Ok(Vec::new());
        };
        let model = model
            .map(|model| {
                model_selection::split_with_profiles(model, &[], &provider, &args.model_profiles)
            })
            .transpose()?
            .map(|choice| choice.model);
        return Ok(vec![Candidate { provider, model }]);
    }
    let model = model.expect("auth selector validation");
    if let Some((provider, _)) = model_selection::qualified_model(model, &args.model_profiles) {
        let choice =
            model_selection::split_with_profiles(model, &[], &provider, &args.model_profiles)?;
        return Ok(vec![Candidate {
            provider,
            model: Some(choice.model),
        }]);
    }
    let mut matches = Vec::new();
    for provider in args.model_profiles.providers() {
        let choice =
            model_selection::split_with_profiles(model, &[], &provider, &args.model_profiles)?;
        if args
            .model_profiles
            .model(&provider, &choice.model)
            .is_some()
        {
            matches.push(Candidate {
                provider,
                model: Some(choice.model),
            });
        }
    }
    Ok(matches)
}

async fn resolve(
    args: &Cli,
    candidate: &Candidate,
    no_refresh: bool,
    minimum: Duration,
    wanted: Option<AuthType>,
) -> Result<Option<Resolved>> {
    let provider = &candidate.provider;
    let model = candidate.model.as_deref();
    let endpoint = model
        .and_then(|id| args.model_profiles.model_endpoint(provider, id))
        .unwrap_or_else(|| args.model_profiles.endpoint(provider));
    if let Some(credential) = args.configured_model_credential(provider, endpoint) {
        let kind = if args.model_keys.oauth(provider, endpoint) {
            AuthType::OAuth
        } else {
            AuthType::ApiKey
        };
        if wanted.is_some_and(|wanted| wanted != kind) {
            return Ok(None);
        }
        let value = if matches!(credential, super::model_auth::Credential::Kimi) {
            super::kimi_auth::access(
                &args.settings,
                (wanted == Some(AuthType::OAuth)).then_some(minimum),
                no_refresh,
            )
            .await?
        } else {
            credential.resolve().await?
        };
        if !super::model_auth::valid_key(&value) {
            return Err(Error::Config("invalid provider credential".into()));
        }
        return Ok(Some(Resolved { kind, value }));
    }
    if provider == &Provider::Radius && wanted != Some(AuthType::ApiKey) {
        let gateway = radius_auth::Gateway::configured_with_settings(&args.settings)?;
        if gateway.model_endpoint().as_str() == endpoint {
            let access = if no_refresh {
                gateway.cached_access().await?
            } else {
                gateway.access(minimum).await?
            };
            return Ok(access.map(|value| Resolved {
                kind: AuthType::OAuth,
                value,
            }));
        }
    }
    Ok(None)
}

fn emit(line: &str) -> Result<()> {
    let mut out = io::stdout().lock();
    writeln!(out, "{line}")?;
    out.flush()?;
    Ok(())
}

pub(super) async fn run(mut args: Cli, command: AuthCommand) -> Result<i32> {
    // Auth commands are dispatched before normal interactive configuration is loaded.
    // Honor existing trust decisions without prompting in this noninteractive command.
    let explicit = if args.no_trust {
        Some(false)
    } else if args.trust || args.project.is_some() {
        Some(true)
    } else {
        None
    };
    args.trusted = super::config::project_trust(&args.workspace, explicit)?.unwrap_or(false);
    args.settings = super::config::Settings::load(&args.workspace, args.trusted)?;
    if args.api_key.is_some() {
        return Err(Error::Config(
            "omit --api-key when resolving configured credentials".into(),
        ));
    }
    let name = args
        .provider_argument
        .as_deref()
        .or_else(|| args.model_explicit.then_some(args.model.as_str()))
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| Error::Config("auth requires --provider or --model".into()))?;
    let configuration = match super::model_profiles::Profiles::load() {
        Ok(profiles) => super::model_auth::Keys::load_async()
            .await
            .map(|keys| (profiles, keys)),
        Err(error) => Err(error),
    };
    match configuration {
        Ok((profiles, keys)) => {
            args.model_profiles = profiles;
            args.model_keys = keys;
        }
        Err(error) => {
            if let AuthCommand::Check { json, .. } = command {
                if json {
                    emit(
                        &json!({"status":"invalid","provider":name,"reason":"invalid_state"})
                            .to_string(),
                    )?;
                } else {
                    emit("invalid")?;
                }
                return Ok(2);
            }
            return Err(error);
        }
    }
    let candidates = candidates(&args)?;
    if let AuthCommand::Check {
        json,
        credentials,
        no_refresh,
    } = command
    {
        let mut status = "not_ready";
        let mut reason = Some("provider_not_found");
        let mut auth_type = None;
        let mut output_credential = None;
        let mut provider = name;
        if let [candidate] = candidates.as_slice() {
            provider = candidate.provider.id();
            match resolve(&args, candidate, no_refresh, Duration::ZERO, None).await {
                Ok(Some(value)) => {
                    status = "ready";
                    reason = None;
                    auth_type = Some(if value.kind == AuthType::OAuth {
                        "oauth"
                    } else {
                        "api_key"
                    });
                    if credentials {
                        output_credential = Some(value.value);
                    }
                }
                Ok(None) => reason = Some("credentials_not_configured"),
                Err(_) => {
                    status = "invalid";
                    reason = Some("invalid_state");
                }
            }
        } else if candidates.len() > 1 {
            status = "invalid";
            reason = Some("invalid_state");
        }
        if json {
            let mut value = json!({"status":status,"provider":provider});
            if let Some(reason) = reason {
                value["reason"] = json!(reason);
            }
            if let Some(kind) = auth_type {
                value["authType"] = json!(kind);
            }
            if let Some(credential) = output_credential {
                value["credentials"] = json!(credential);
            }
            emit(&value.to_string())?;
        } else {
            emit(output_credential.as_deref().unwrap_or(status))?;
        }
        match status {
            "ready" => return Ok(0),
            "not_ready" => return Ok(1),
            _ => return Ok(2),
        }
    }

    let (kind, minimum, description) = match command {
        AuthCommand::PrintApiKey => (AuthType::ApiKey, Duration::ZERO, "API key"),
        AuthCommand::PrintBearerToken { min_expiry } => (
            AuthType::OAuth,
            min_expiry.unwrap_or(Duration::from_secs(30 * 60)),
            "OAuth bearer token",
        ),
        AuthCommand::Check { .. } => unreachable!("check already returned"),
    };
    let mut matches = Vec::new();
    for candidate in &candidates {
        if let Some(credential) = resolve(&args, candidate, false, minimum, Some(kind)).await? {
            matches.push((candidate.provider.id(), credential.value));
        }
    }
    match matches.as_slice() {
        [(_, credential)] => emit(credential).map(|()| 0),
        [] => Err(Error::Config(format!(
            "No usable {description} is configured for {name}"
        ))),
        _ => Err(Error::Config(format!(
            "Multiple configured providers matched ({}). Specify --provider.",
            matches
                .iter()
                .map(|(provider, _)| *provider)
                .collect::<Vec<_>>()
                .join(", ")
        ))),
    }
}
