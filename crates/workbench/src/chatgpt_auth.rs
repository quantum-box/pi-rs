//! Local ChatGPT plan sign-in and serialized credential renewal.
use pi_rs::{Error, Result};
use reqwest::Url;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use uuid::Uuid;

const PROVIDER: &str = "openai-chatgpt";
#[derive(serde::Serialize, serde::Deserialize)]
struct Registration {
    client_id: String,
    subject: String,
    email: Option<String>,
}

fn registration() -> Result<Option<Registration>> {
    let value =
        super::config::read_object(&super::config::agent_dir().join("chatgpt-registration.json"))?;
    if value.as_object().is_some_and(|data| data.is_empty()) {
        return Ok(None);
    }
    serde_json::from_value(value)
        .map(Some)
        .map_err(|_| Error::Config("invalid ChatGPT registration".into()))
}
fn auth_path() -> std::path::PathBuf {
    super::config::agent_dir().join("auth.json")
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
    let path = super::config::agent_dir().join("chatgpt-refresh.lock");
    tokio::task::spawn_blocking(move || {
        std::fs::create_dir_all(path.parent().expect("lock parent"))?;
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        file.lock()?;
        Ok::<_, std::io::Error>(file)
    })
    .await
    .map_err(|_| Error::Config("could not lock ChatGPT credentials".into()))?
    .map_err(Error::Io)
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
    super::config::write_json(
        &super::config::agent_dir().join("chatgpt-registration.json"),
        &serde_json::to_value(registration)?,
    )?;
    super::config::update_object(&auth_path(), |data| {
        data.insert(
            PROVIDER.into(),
            serde_json::json!({"type":"oauth","credential":credential}),
        );
        Ok(())
    })
}

fn client(settings: &super::config::Settings) -> Result<reqwest::Client> {
    pi_rs::openai::oauth::http_client_with_options(
        &super::http_proxy::configured(settings)?,
        settings.http_idle_timeout(),
    )
}

pub(super) async fn login(
    settings: &super::config::Settings,
    announce: impl FnOnce(&Url),
) -> Result<()> {
    let _lock = lock().await?;
    let existing = load()?;
    let registered = registration()?;
    let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).await?;
    let callback = Url::parse(&format!(
        "http://127.0.0.1:{}/auth/callback",
        listener.local_addr()?.port()
    ))
    .expect("loopback URL");
    let auth = pi_rs::openai::oauth::Authorization::new(
        callback.clone(),
        host_id()?,
        existing
            .as_ref()
            .map(|credential| credential.client_id.clone())
            .or_else(|| registered.as_ref().map(|account| account.client_id.clone())),
    )?;
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
        if existing
            .as_ref()
            .is_some_and(|previous| previous.subject != credential.subject)
            || registered
                .as_ref()
                .is_some_and(|previous| previous.subject != credential.subject)
        {
            return Err(Error::Config(
                "ChatGPT account changed during sign-in".into(),
            ));
        }
        save(&credential)
    }
    .await;
    redirect.reply(result.is_ok()).await;
    result
}

pub(super) async fn access(settings: &super::config::Settings) -> Result<String> {
    let _lock = lock().await?;
    let mut credential =
        load()?.ok_or_else(|| Error::Config("sign in with pi login openai-chatgpt".into()))?;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| Error::Config("invalid system clock".into()))?
        .as_secs();
    if credential.expires_at <= now.saturating_add(180) {
        let client = client(settings)?;
        credential = credential.refresh(&client).await?;
        save(&credential)?;
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
    super::config::update_object(&auth_path(), |data| {
        data.remove(PROVIDER);
        Ok(())
    })?;
    revoked.map(|()| true).map_err(|_| {
        Error::Config(
            "ChatGPT local credentials removed; remote revocation was not confirmed".into(),
        )
    })
}
