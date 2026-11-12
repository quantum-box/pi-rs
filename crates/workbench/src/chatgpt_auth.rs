//! Local ChatGPT plan sign-in and serialized credential renewal.
use pi_rs::{Error, Result};
use reqwest::Url;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use uuid::Uuid;

const PROVIDER: &str = "openai-chatgpt";
#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct Registration {
    client_id: String,
    subject: String,
    email: Option<String>,
}

#[derive(Default, serde::Serialize, serde::Deserialize)]
struct Registrations {
    active: Option<String>,
    accounts: Vec<Registration>,
}

fn registrations() -> Result<Registrations> {
    let path = super::config::agent_dir().join("chatgpt-registrations.json");
    let value = super::config::read_object(&path)?;
    if !value.as_object().is_some_and(|data| data.is_empty()) {
        return serde_json::from_value(value)
            .map_err(|_| Error::Config("invalid ChatGPT registrations".into()));
    }
    let value =
        super::config::read_object(&super::config::agent_dir().join("chatgpt-registration.json"))?;
    if value.as_object().is_some_and(|data| data.is_empty()) {
        return Ok(Registrations::default());
    }
    let account: Registration = serde_json::from_value(value)
        .map_err(|_| Error::Config("invalid ChatGPT registration".into()))?;
    Ok(Registrations {
        active: Some(account.client_id.clone()),
        accounts: vec![account],
    })
}

pub(super) fn list_accounts() -> Result<Vec<(String, String, bool)>> {
    let store = registrations()?;
    Ok(store
        .accounts
        .iter()
        .enumerate()
        .map(|(index, account)| {
            let label = account
                .email
                .clone()
                .unwrap_or_else(|| format!("ChatGPT account {}", index + 1));
            (
                account.client_id.clone(),
                label,
                store.active.as_deref() == Some(&account.client_id),
            )
        })
        .collect())
}
fn auth_path() -> std::path::PathBuf {
    super::config::agent_dir().join("auth.json")
}
fn sessions_path() -> std::path::PathBuf {
    super::config::agent_dir().join("chatgpt-sessions.json")
}
fn saved_session(id: &str) -> Result<Option<pi_rs::openai::oauth::Credential>> {
    let sessions = super::config::read_object(&sessions_path())?;
    sessions
        .get(id)
        .map(|value| {
            serde_json::from_value(value.clone())
                .map_err(|_| Error::Config("invalid ChatGPT session".into()))
        })
        .transpose()
}
fn clear_session(id: &str) -> Result<()> {
    super::config::update_object(&sessions_path(), |sessions| {
        sessions.remove(id);
        Ok(())
    })?;
    super::config::update_object(&auth_path(), |data| {
        if data
            .get(PROVIDER)
            .and_then(|entry| entry["credential"]["client_id"].as_str())
            == Some(id)
        {
            data.remove(PROVIDER);
        }
        Ok(())
    })
}
fn load() -> Result<Option<pi_rs::openai::oauth::Credential>> {
    let value = super::config::read_object(&auth_path())?;
    value
        .get(PROVIDER)
        .filter(|value| value["type"] == "oauth")
        .map(|value| {
            serde_json::from_value(value["credential"].clone())
                .map_err(|_| Error::Config("invalid ChatGPT credential".into()))
        })
        .transpose()
}
async fn lock() -> Result<std::fs::File> {
    lock_path(super::config::agent_dir().join("chatgpt-refresh.lock")).await
}
async fn lock_path(path: std::path::PathBuf) -> Result<std::fs::File> {
    std::fs::create_dir_all(path.parent().expect("lock parent"))?;
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)?;
    loop {
        match file.try_lock() {
            Ok(()) => return Ok(file),
            Err(std::fs::TryLockError::WouldBlock) => {
                tokio::time::sleep(Duration::from_millis(50)).await
            }
            Err(std::fs::TryLockError::Error(error)) => return Err(Error::Io(error)),
        }
    }
}

fn host_id() -> Result<String> {
    let mut result = String::new();
    super::config::update_object(
        &super::config::agent_dir().join("chatgpt-host.json"),
        |data| {
            let id = data
                .entry("hostId")
                .or_insert_with(|| serde_json::json!(format!("urn:uuid:{}", Uuid::new_v4())));
            result = id
                .as_str()
                .ok_or_else(|| Error::Config("invalid ChatGPT host ID".into()))?
                .into();
            Ok(())
        },
    )?;
    Ok(result)
}
fn save(credential: &pi_rs::openai::oauth::Credential) -> Result<()> {
    let registration = Registration {
        client_id: credential.client_id.clone(),
        subject: credential.subject.clone(),
        email: credential.email.clone(),
    };
    let mut store = registrations()?;
    if let Some(existing) = store
        .accounts
        .iter_mut()
        .find(|account| account.client_id == registration.client_id)
    {
        *existing = registration;
    } else {
        store.accounts.push(registration);
    }
    store.active = Some(credential.client_id.clone());
    super::config::update_object(&sessions_path(), |sessions| {
        sessions.insert(
            credential.client_id.clone(),
            serde_json::to_value(credential)?,
        );
        Ok(())
    })?;
    super::config::update_object(&auth_path(), |data| {
        data.insert(
            PROVIDER.into(),
            serde_json::json!({"type":"oauth","credential":credential}),
        );
        Ok(())
    })?;
    super::config::write_json(
        &super::config::agent_dir().join("chatgpt-registrations.json"),
        &serde_json::to_value(store)?,
    )
}

fn client(settings: &super::config::Settings) -> Result<reqwest::Client> {
    pi_rs::openai::oauth::http_client_with_options(
        &super::http_proxy::configured(settings)?,
        settings.http_idle_timeout(),
    )
}

pub(super) async fn login(
    settings: &super::config::Settings,
    new_account: bool,
    account: Option<&str>,
    announce: impl FnOnce(&Url),
) -> Result<()> {
    let _lock = lock().await?;
    let store = registrations()?;
    let registered = if new_account {
        None
    } else if let Some(account) = account {
        Some(
            store
                .accounts
                .iter()
                .find(|saved| saved.client_id == account)
                .cloned()
                .ok_or_else(|| {
                    Error::Config("unknown ChatGPT account; use --list-accounts".into())
                })?,
        )
    } else {
        store
            .active
            .as_deref()
            .and_then(|id| store.accounts.iter().find(|saved| saved.client_id == id))
            .cloned()
    };
    let mut needs_consent = false;
    if let Some(account) = registered.as_ref()
        && let Some(credential) = saved_session(&account.client_id)?
    {
        if credential.subject != account.subject || credential.client_id != account.client_id {
            return Err(Error::Config("ChatGPT account identity mismatch".into()));
        }
        if !credential.has_plan_access() {
            needs_consent = true;
        } else {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| Error::Config("invalid system clock".into()))?
                .as_secs();
            if credential.expires_at <= now.saturating_add(180) {
                match credential.refresh(&client(settings)?).await {
                    Ok(refreshed) => {
                        save(&refreshed)?;
                        if refreshed.has_plan_access() {
                            return Ok(());
                        }
                        needs_consent = true;
                    }
                    Err(Error::AuthorizationRequired { .. }) => {
                        clear_session(&account.client_id)?;
                    }
                    Err(error) => return Err(error),
                }
            } else {
                return save(&credential);
            }
        }
    }
    let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).await?;
    let callback = Url::parse(&format!(
        "http://127.0.0.1:{}/auth/callback",
        listener.local_addr()?.port()
    ))
    .expect("loopback URL");
    let auth = pi_rs::openai::oauth::Authorization::new(
        callback.clone(),
        host_id()?,
        registered.as_ref().map(|account| account.client_id.clone()),
    )?;
    let auth = if needs_consent {
        auth.request_consent()
    } else {
        auth
    };
    let url = auth.url();
    announce(&url);
    let _ = pi_rs_workbench::oauth_callback::open_browser(url.as_str()).await;
    let redirect = tokio::time::timeout(
        Duration::from_secs(300),
        pi_rs_workbench::oauth_callback::receive(&listener, &callback, auth.state()),
    )
    .await
    .map_err(|_| Error::Config("ChatGPT sign-in timed out".into()))??;
    let result = async {
        let client = client(settings)?;
        let credential = auth.exchange(&client, &redirect.url).await?;
        if registered
            .as_ref()
            .is_some_and(|previous| previous.subject != credential.subject)
        {
            return Err(Error::Config(
                "ChatGPT account changed during sign-in".into(),
            ));
        }
        let allowed = credential.has_plan_access();
        save(&credential)?;
        Ok(allowed)
    }
    .await;
    redirect.reply(result.is_ok()).await;
    if result? {
        Ok(())
    } else {
        Err(Error::Config(
            "ChatGPT sign-in succeeded, but plan usage permission was not granted".into(),
        ))
    }
}

pub(super) async fn access(settings: &super::config::Settings) -> Result<String> {
    let _lock = lock().await?;
    let mut credential =
        load()?.ok_or_else(|| Error::Config("sign in with pi login openai-chatgpt".into()))?;
    if let Some(saved) = saved_session(&credential.client_id)?
        && saved.subject == credential.subject
        && saved.expires_at >= credential.expires_at
    {
        credential = saved;
    }
    if !credential.has_plan_access() {
        return Err(Error::Config(
            "ChatGPT plan permission is not granted".into(),
        ));
    }
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| Error::Config("invalid system clock".into()))?
        .as_secs();
    if credential.expires_at <= now.saturating_add(180) {
        let client = client(settings)?;
        credential = match credential.refresh(&client).await {
            Ok(refreshed) => refreshed,
            Err(error @ Error::AuthorizationRequired { .. }) => {
                clear_session(&credential.client_id)?;
                return Err(error);
            }
            Err(error) => return Err(error),
        };
        save(&credential)?;
    }
    if !credential.has_plan_access() {
        return Err(Error::Config(
            "ChatGPT plan permission is not granted".into(),
        ));
    }
    Ok(credential.access_token)
}

pub(super) async fn logout(settings: &super::config::Settings) -> Result<bool> {
    let _lock = lock().await?;
    let Some(credential) = load()? else {
        return Ok(false);
    };
    let revoked = match client(settings) {
        Ok(client) => credential.revoke(&client).await,
        Err(error) => Err(error),
    };
    clear_session(&credential.client_id)?;
    revoked.map(|()| true).map_err(|_| {
        Error::Config(
            "ChatGPT local credentials removed; remote revocation was not confirmed".into(),
        )
    })
}
