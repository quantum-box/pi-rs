//! Terminal provider sign-in shared by both CLI entry points.
use std::io::{self, IsTerminal, Write};
use std::path::Path;
use std::time::Duration;

use crossterm::event::{Event, KeyCode, KeyEventKind, KeyModifiers};
use pi_rs::{CancellationToken, Error, Result};
use reqwest::Url;
use tokio::sync::oneshot;

use super::{
    Cli, Provider, anthropic_auth, chatgpt_auth, codex_auth, config, copilot_auth, kimi_auth,
    meta_auth, model_auth, model_profiles, openrouter_auth, radius_auth, vertex_auth, xai_auth,
};

pub(super) struct OAuthOptions<'a> {
    device_code: bool,
    copy_code: bool,
    new_account: bool,
    account: Option<&'a str>,
    open_browser: bool,
    interactive: bool,
}

impl<'a> OAuthOptions<'a> {
    pub(super) fn legacy(
        device_code: bool,
        copy_code: bool,
        new_account: bool,
        account: Option<&'a str>,
    ) -> Self {
        Self {
            device_code,
            copy_code,
            new_account,
            account,
            open_browser: true,
            interactive: false,
        }
    }
}

fn announce_code(url: &Url, sender: oneshot::Sender<String>, is_openrouter: bool) {
    eprintln!("Continue in your browser: {url}");
    eprintln!("Paste the authorization code or redirect URL if the callback does not complete:");
    std::thread::spawn(move || {
        let mut line = String::new();
        if std::io::stdin()
            .read_line(&mut line)
            .is_ok_and(|count| count > 0)
        {
            let code = if is_openrouter {
                openrouter_auth::manual_code(&line)
            } else {
                Some(line)
            };
            if let Some(code) = code {
                let _ = sender.send(code);
            }
        }
    });
}

pub(super) async fn oauth(
    settings: &config::Settings,
    provider: &str,
    options: OAuthOptions<'_>,
) -> Result<()> {
    let announce_device_url = |url: &str, code: &str| {
        eprintln!("Enter code {code} at {url}");
        if options.interactive && options.open_browser {
            let url = url.to_owned();
            tokio::spawn(async move {
                let _ = pi_rs_workbench::oauth_callback::open_browser(url.as_str()).await;
            });
        }
    };
    let announce_device = |url: &Url, code: &str| announce_device_url(url.as_str(), code);
    if provider == "anthropic" {
        if options.device_code {
            return Err(Error::Config(
                "Anthropic uses browser or copy-code authorization".into(),
            ));
        }
        let method = if options.copy_code {
            anthropic_auth::LoginMethod::CopyCode
        } else {
            anthropic_auth::LoginMethod::Browser
        };
        anthropic_auth::login_with_browser(
            settings,
            method,
            meta_auth::LoginControl::default(),
            options.open_browser,
            |url, sender| announce_code(url, sender, false),
        )
        .await
    } else if provider == "openai-chatgpt" {
        if options.device_code {
            return Err(Error::Config("ChatGPT uses browser authorization".into()));
        }
        chatgpt_auth::login_with_browser(
            settings,
            options.new_account,
            options.account,
            options.open_browser,
            |url| eprintln!("Continue with ChatGPT in your browser: {url}"),
        )
        .await
    } else if provider == "openai-codex" {
        let method = if options.device_code {
            codex_auth::Method::Device
        } else {
            codex_auth::Method::Browser
        };
        codex_auth::login(
            settings,
            meta_auth::LoginControl::default(),
            method,
            |notice| match notice {
                codex_auth::Notice::Device { url, code } => announce_device_url(url, &code),
                codex_auth::Notice::Browser(url, sender) => announce_code(&url, sender, false),
            },
        )
        .await
    } else if provider == "github-copilot" {
        copilot_auth::login(
            settings,
            meta_auth::LoginControl::default(),
            announce_device,
        )
        .await
    } else if provider == "xai" {
        xai_auth::login(
            settings,
            meta_auth::LoginControl::default(),
            announce_device,
        )
        .await
    } else if provider == "kimi-coding" {
        kimi_auth::login(settings, announce_device).await
    } else if provider == "meta" {
        meta_auth::login(
            settings,
            meta_auth::LoginControl::default(),
            announce_device,
        )
        .await
    } else if provider == "openrouter" {
        if options.device_code {
            return Err(Error::Config(
                "OpenRouter uses browser authorization".into(),
            ));
        }
        let mut legacy_sender = None;
        openrouter_auth::login_with_browser(settings, options.open_browser, |url, sender| {
            if options.interactive {
                announce_code(url, sender, true);
            } else {
                eprintln!("Continue in your browser: {url}");
                legacy_sender = Some(sender);
            }
        })
        .await
    } else if provider == "radius" {
        let gateway = radius_auth::Gateway::configured_with_settings(settings)?;
        let method = if options.device_code {
            radius_auth::Method::Device
        } else {
            radius_auth::Method::Browser
        };
        gateway
            .login(method, |notice| match notice {
                radius_auth::Notice::Browser(url) => eprintln!("Continue in your browser: {url}"),
                radius_auth::Notice::Device { verification, code } => {
                    announce_device(&verification, &code)
                }
            })
            .await
    } else {
        Err(Error::Config("unsupported account sign-in provider".into()))
    }
}

fn account_provider(id: &str) -> Option<&str> {
    match id {
        "openai" => Some("openai-chatgpt"),
        "anthropic" | "openai-chatgpt" | "openai-codex" | "github-copilot" | "xai"
        | "kimi-coding" | "meta" | "openrouter" | "radius" => Some(id),
        _ => None,
    }
}

pub(super) async fn login(
    args: &Cli,
    reference: &str,
    use_api_key: bool,
    no_browser: bool,
) -> Result<i32> {
    if use_api_key && no_browser {
        return Err(Error::Config(
            "--no-browser only applies to account sign-in; omit it with --api-key.".into(),
        ));
    }
    let profiles = model_profiles::Profiles::load(&model_auth::Keys::load_async().await?)?;
    let Some(provider) = profiles.auth_provider(reference)? else {
        eprintln!("Unknown provider {reference:?}.");
        let mut providers: Vec<_> = profiles
            .providers()
            .map(|provider| provider.id().to_owned())
            .collect();
        providers.sort_unstable();
        eprintln!("Providers: {}", providers.join(", "));
        return Ok(1);
    };
    if !io::stdin().is_terminal() || !io::stdout().is_terminal() {
        return Err(Error::Config(
            "\"pi auth login <provider> [--api-key] [--no-browser]\" requires an interactive terminal".into(),
        ));
    }
    let name = profiles
        .provider_display_name(&provider)
        .chars()
        .filter(|c| !c.is_control())
        .collect::<String>();
    let path = config::agent_dir().join("auth.json");
    if let Some(id) = account_provider(provider.id()).filter(|_| !use_api_key) {
        oauth(
            &args.settings,
            id,
            OAuthOptions {
                device_code: no_browser && matches!(id, "openai-codex" | "radius"),
                copy_code: no_browser && id == "anthropic",
                new_account: false,
                account: None,
                open_browser: !no_browser,
                interactive: true,
            },
        )
        .await?;
        println!(
            "Logged in to {name}. Credentials saved to {}",
            path.display()
        );
        if id != provider.id() {
            println!("Subscription models are available under {id}.");
        }
    } else {
        if matches!(provider.id(), "openai-codex" | "openai-chatgpt") {
            return Err(Error::Config(format!(
                "{name} has no API key login. Omit --api-key to sign in."
            )));
        }
        let workspace = args.workspace.clone();
        let cancel = args.command_cancel.clone();
        let prompt_name = name.clone();
        let endpoint = profiles.endpoint(&provider).to_owned();
        let fixed_cloudflare_endpoint = profiles.has_fixed_cloudflare_endpoint(&provider, None);
        tokio::task::spawn_blocking(move || {
            api_key_login(
                &provider,
                &prompt_name,
                &endpoint,
                &workspace,
                fixed_cloudflare_endpoint,
                &cancel,
            )
        })
        .await
        .map_err(|_| Error::Config("login prompt failed".into()))??;
        println!(
            "Saved API key for {name}. Credentials saved to {}",
            path.display()
        );
    }
    Ok(0)
}

fn check_cancelled(cancel: &CancellationToken) -> Result<()> {
    if cancel.is_cancelled() {
        Err(Error::Cancelled)
    } else {
        Ok(())
    }
}

fn api_key_login(
    provider: &Provider,
    name: &str,
    endpoint: &str,
    workspace: &Path,
    fixed_cloudflare_endpoint: bool,
    cancel: &CancellationToken,
) -> Result<()> {
    let prompt = |label: &str| read_prompt(label, false, cancel);
    let secret = |label: &str| read_prompt(label, true, cancel);
    match provider.id() {
        "amazon-bedrock" => match choose(
            "Select Amazon Bedrock authentication method",
            &[
                "Bearer token",
                "AWS profile",
                "Existing AWS credential chain",
            ],
            cancel,
        )? {
            1 => {
                let profile = prompt("Enter AWS profile name")?;
                check_cancelled(cancel)?;
                return model_auth::save_bedrock_profile(Some(profile.trim()));
            }
            2 => {
                prompt("Configure AWS credentials, then press Enter to continue")?;
                check_cancelled(cancel)?;
                return model_auth::save_bedrock_profile(None);
            }
            _ => {}
        },
        "google-vertex" => {
            let method = choose(
                "Select Google Vertex AI authentication method",
                &[
                    "Google Cloud API key",
                    "Application Default Credentials",
                    "Service account credentials file",
                ],
                cancel,
            )?;
            if method != 0 {
                let project = prompt("Enter Google Cloud project ID")?;
                vertex_auth::validate_identifier(project.trim(), "project ID")?;
                let location = prompt("Enter Google Cloud location")?;
                let credentials = if method == 2 {
                    let input = prompt("Enter service account credentials file path")?;
                    Some(workspace.join(pi_rs_workbench::normalize_path(input.trim())?))
                } else {
                    None
                };
                check_cancelled(cancel)?;
                return vertex_auth::save_login(
                    project.trim(),
                    location.trim(),
                    credentials.as_deref(),
                );
            }
        }
        "cloudflare-workers-ai" | "cloudflare-ai-gateway"
            if !fixed_cloudflare_endpoint && endpoint.is_empty() =>
        {
            let key = secret("Enter Cloudflare API key")?;
            let account = prompt("Enter Cloudflare account ID")?;
            let gateway = (provider.id() == "cloudflare-ai-gateway")
                .then(|| prompt("Enter Cloudflare AI Gateway ID"))
                .transpose()?;
            check_cancelled(cancel)?;
            return model_auth::save_cloudflare_login(
                provider,
                key.trim(),
                account.trim(),
                gateway.as_deref().map(str::trim),
            );
        }
        _ => {}
    }
    if endpoint.is_empty() && provider != &Provider::Bedrock {
        return Err(Error::Config(format!(
            "Configure an endpoint for {name} before login"
        )));
    }
    let key = secret(&format!("Enter {name} API key"))?;
    check_cancelled(cancel)?;
    model_auth::save(provider, endpoint, &key)
}

fn choose(label: &str, choices: &[&str], cancel: &CancellationToken) -> Result<usize> {
    eprintln!("{label}:");
    for (index, choice) in choices.iter().enumerate() {
        eprintln!("  {}. {choice}", index + 1);
    }
    loop {
        let answer = read_prompt(&format!("Select 1-{}", choices.len()), false, cancel)?;
        if let Ok(index) = answer.trim().parse::<usize>()
            && (1..=choices.len()).contains(&index)
        {
            return Ok(index - 1);
        }
        eprintln!("Enter a number from 1 to {}.", choices.len());
    }
}

fn read_prompt(label: &str, secret: bool, cancel: &CancellationToken) -> Result<String> {
    struct RawMode;
    impl Drop for RawMode {
        fn drop(&mut self) {
            let _ = crossterm::terminal::disable_raw_mode();
        }
    }

    crossterm::terminal::enable_raw_mode()?;
    let raw = RawMode;
    let mut out = io::stderr().lock();
    write!(out, "{label}: ")?;
    out.flush()?;
    let mut value = String::new();
    loop {
        check_cancelled(cancel)?;
        if !crossterm::event::poll(Duration::from_millis(50))? {
            continue;
        }
        let Event::Key(key) = crossterm::event::read()? else {
            continue;
        };
        if key.kind == KeyEventKind::Release {
            continue;
        }
        match key.code {
            KeyCode::Enter => break,
            KeyCode::Esc => return Err(Error::Cancelled),
            KeyCode::Char('c' | 'd') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                return Err(Error::Cancelled);
            }
            KeyCode::Backspace => {
                if value.pop().is_some() && !secret {
                    write!(out, "\x08 \x08")?;
                    out.flush()?;
                }
            }
            KeyCode::Char(ch)
                if !ch.is_control()
                    && !key
                        .modifiers
                        .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT) =>
            {
                // Discard the whole attempt instead of authenticating with a truncated value.
                if value.len() + ch.len_utf8() > 4096 {
                    return Err(Error::Config("Login input exceeds 4096 bytes".into()));
                }
                value.push(ch);
                if !secret {
                    write!(out, "{ch}")?;
                    out.flush()?;
                }
            }
            _ => {}
        }
    }
    drop(raw);
    writeln!(out)?;
    Ok(value)
}
