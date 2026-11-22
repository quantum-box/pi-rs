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
    registrations_at(&super::config::agent_dir())
}
fn registrations_at(directory: &std::path::Path) -> Result<Registrations> {
    let path = directory.join("chatgpt-registrations.json");
    let value = super::config::read_object(&path)?;
    if !value.as_object().is_some_and(|data| data.is_empty()) {
        return serde_json::from_value(value)
            .map_err(|_| Error::Config("invalid ChatGPT registrations".into()));
    }
    let value = super::config::read_object(&directory.join("chatgpt-registration.json"))?;
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
fn clear_session_locked(
    directory: &std::path::Path,
    guard: &super::auth_lock::Guard,
    id: &str,
) -> Result<()> {
    super::config::update_object(&directory.join("chatgpt-sessions.json"), |sessions| {
        sessions.remove(id);
        Ok(())
    })?;
    super::config::update_object_locked(&directory.join("auth.json"), Some(guard), |data| {
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
#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct PiCredential {
    access: String,
    refresh: String,
    expires: u64,
    client_id: String,
    scopes: Vec<String>,
}

const PI_EXPIRY_OFFSET: u64 = 180;

pub(super) fn decode_entry(value: &serde_json::Value) -> Result<pi_rs::openai::oauth::Credential> {
    let invalid = || Error::Config("invalid ChatGPT credential".into());
    if let Some(native) = value.get("credential") {
        return serde_json::from_value(native.clone()).map_err(|_| invalid());
    }
    let pi: PiCredential = serde_json::from_value(value.clone()).map_err(|_| invalid())?;
    if ![&pi.client_id, &pi.access, &pi.refresh]
        .into_iter()
        .all(|value| super::model_auth::valid_key(value))
        || pi.expires == 0
    {
        return Err(invalid());
    }
    Ok(pi_rs::openai::oauth::Credential {
        client_id: pi.client_id,
        // Pi retains the issued client ID and token grant, with no linked
        // account profile. This credential never enters the account store.
        subject: String::new(),
        email: None,
        id_token: String::new(),
        access_token: pi.access,
        refresh_token: pi.refresh,
        expires_at: (pi.expires / 1000).saturating_add(PI_EXPIRY_OFFSET),
        scopes: pi.scopes,
    })
}

fn pi_entry(value: &serde_json::Value) -> bool {
    value[PROVIDER]["type"] == "oauth" && value[PROVIDER].get("credential").is_none()
}

fn parse_credential(value: &serde_json::Value) -> Result<Option<pi_rs::openai::oauth::Credential>> {
    value
        .get(PROVIDER)
        .filter(|value| value["type"] == "oauth")
        .map(decode_entry)
        .transpose()
}

#[cfg(test)]
async fn load_at(path: &std::path::Path) -> Result<Option<pi_rs::openai::oauth::Credential>> {
    parse_credential(&super::config::read_auth_object(path).await?)
}

fn save_pi_refresh(
    auth: &std::path::Path,
    guard: &super::auth_lock::Guard,
    credential: &pi_rs::openai::oauth::Credential,
) -> Result<()> {
    super::config::update_object_locked(auth, Some(guard), |data| {
        let entry = data
            .get_mut(PROVIDER)
            .and_then(serde_json::Value::as_object_mut)
            .ok_or_else(|| Error::Config("ChatGPT credentials changed during refresh".into()))?;
        entry.insert("access".into(), serde_json::json!(credential.access_token));
        entry.insert(
            "refresh".into(),
            serde_json::json!(credential.refresh_token),
        );
        entry.insert(
            "expires".into(),
            serde_json::json!(
                credential
                    .expires_at
                    .saturating_sub(PI_EXPIRY_OFFSET)
                    .saturating_mul(1000)
            ),
        );
        entry.insert("scopes".into(), serde_json::json!(credential.scopes));
        Ok(())
    })
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
fn save_locked(
    directory: &std::path::Path,
    guard: &super::auth_lock::Guard,
    credential: &pi_rs::openai::oauth::Credential,
) -> Result<()> {
    let registration = Registration {
        client_id: credential.client_id.clone(),
        subject: credential.subject.clone(),
        email: credential.email.clone(),
    };
    let mut store = registrations_at(directory)?;
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
    super::config::update_object(&directory.join("chatgpt-sessions.json"), |sessions| {
        sessions.insert(
            credential.client_id.clone(),
            serde_json::to_value(credential)?,
        );
        Ok(())
    })?;
    super::config::update_object_locked(&directory.join("auth.json"), Some(guard), |data| {
        data.insert(
            PROVIDER.into(),
            serde_json::json!({"type":"oauth","credential":credential}),
        );
        Ok(())
    })?;
    super::config::write_json(
        &directory.join("chatgpt-registrations.json"),
        &serde_json::to_value(store)?,
    )
}

async fn save(credential: &pi_rs::openai::oauth::Credential) -> Result<()> {
    let directory = super::config::agent_dir();
    let guard = super::auth_lock::acquire_async(&directory.join("auth.json")).await?;
    save_locked(&directory, &guard, credential)
}

// Once token rotation starts, a blocking worker owns both locks through
// persistence, even if the caller or its runtime is cancelled.
async fn refresh_with<F, Fut>(
    directory: std::path::PathBuf,
    mut previous: pi_rs::openai::oauth::Credential,
    expected_active: Option<pi_rs::openai::oauth::Credential>,
    pi_minimum_seconds: Option<u64>,
    chat_lock: std::fs::File,
    exchange: F,
) -> Result<pi_rs::openai::oauth::Credential>
where
    F: FnOnce(pi_rs::openai::oauth::Credential) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Result<pi_rs::openai::oauth::Credential>> + Send + 'static,
{
    let auth = directory.join("auth.json");
    let guard = super::auth_lock::acquire_async(&auth).await?;
    let from_pi = if let Some(expected) = expected_active {
        let data = super::config::read_auth_object_locked(&guard)?;
        let current = parse_credential(&data)?;
        guard.check()?;
        let from_pi = pi_entry(&data);
        let same_format = from_pi == pi_minimum_seconds.is_some();
        let unchanged = current.as_ref().map(serde_json::to_value).transpose()?
            == Some(serde_json::to_value(&expected)?);
        if !same_format || !unchanged {
            let updated = current
                .filter(|current| same_format && from_pi && current.client_id == expected.client_id)
                .ok_or_else(|| {
                    Error::Config("ChatGPT credentials changed during refresh".into())
                })?;
            if !updated.has_plan_access() {
                return Err(Error::Config(
                    "ChatGPT plan permission is not granted".into(),
                ));
            }
            // Pi can refresh while we wait for auth.json.lock. Reuse its
            // rotated token, or refresh from the current value if needed.
            let seconds = pi_minimum_seconds.expect("Pi refresh threshold");
            if updated.expires_at > unix_seconds()?.saturating_add(seconds) {
                return Ok(updated);
            }
            previous = updated;
        }
        from_pi
    } else {
        false
    };
    super::model_auth::await_admitted_oauth_refresh(
        from_pi.then_some("openai-chatgpt"),
        move |admitted| {
            let _chat_lock = chat_lock;
            let barrier = super::process::refresh_barrier()?;
            let guard = super::process::guard_refresh_auth(barrier, guard);
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|_| Error::Config("could not start ChatGPT refresh runtime".into()))?;
            let _ = admitted.send(());
            match runtime.block_on(exchange(previous.clone())) {
                Ok(refreshed) => {
                    if from_pi {
                        save_pi_refresh(&auth, &guard, &refreshed)?;
                    } else {
                        save_locked(&directory, &guard, &refreshed)?;
                    }
                    Ok(refreshed)
                }
                Err(error @ Error::AuthorizationRequired { .. }) if !from_pi => {
                    clear_session_locked(&directory, &guard, &previous.client_id)?;
                    Err(error)
                }
                Err(error) => Err(error),
            }
        },
    )
    .await
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
    login_with_browser(settings, new_account, account, true, announce).await
}

pub(super) async fn login_with_browser(
    settings: &super::config::Settings,
    new_account: bool,
    account: Option<&str>,
    open_browser: bool,
    announce: impl FnOnce(&Url),
) -> Result<()> {
    let mut chat_lock = Some(lock().await?);
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
                let http = client(settings)?;
                let result = refresh_with(
                    super::config::agent_dir(),
                    credential,
                    None,
                    None,
                    chat_lock.take().expect("ChatGPT lock"),
                    move |previous| async move { previous.refresh(&http).await },
                )
                .await;
                match result {
                    Ok(refreshed) if refreshed.has_plan_access() => return Ok(()),
                    Ok(_) => needs_consent = true,
                    Err(Error::AuthorizationRequired { .. }) => (),
                    Err(error) => return Err(error),
                }
                chat_lock = Some(lock().await?);
            } else {
                return save(&credential).await;
            }
        }
    }
    let _chat_lock = chat_lock;
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
    if open_browser {
        let _ = pi_rs_workbench::oauth_callback::open_browser(url.as_str()).await;
    }
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
        save(&credential).await?;
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
    access_options(settings, None, false).await
}

pub(super) async fn access_options(
    settings: &super::config::Settings,
    minimum: Option<Duration>,
    no_refresh: bool,
) -> Result<String> {
    let settings = settings.clone();
    access_at(
        super::config::agent_dir(),
        minimum,
        no_refresh,
        move |previous| async move { previous.refresh(&client(&settings)?).await },
    )
    .await
}

fn unix_seconds() -> Result<u64> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| Error::Config("invalid system clock".into()))?
        .as_secs())
}

async fn access_at<F, Fut>(
    directory: std::path::PathBuf,
    minimum: Option<Duration>,
    no_refresh: bool,
    exchange: F,
) -> Result<String>
where
    F: FnOnce(pi_rs::openai::oauth::Credential) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Result<pi_rs::openai::oauth::Credential>> + Send + 'static,
{
    let chat_lock = if no_refresh {
        None
    } else {
        Some(lock_path(directory.join("chatgpt-refresh.lock")).await?)
    };
    let data = super::config::read_auth_object(&directory.join("auth.json")).await?;
    let active = parse_credential(&data)?
        .ok_or_else(|| Error::Config("sign in with pi login openai-chatgpt".into()))?;
    let mut credential = active.clone();
    if !no_refresh && !pi_entry(&data) {
        let sessions = super::config::read_object(&directory.join("chatgpt-sessions.json"))?;
        let saved = sessions
            .get(&credential.client_id)
            .map(|value| {
                serde_json::from_value::<pi_rs::openai::oauth::Credential>(value.clone())
                    .map_err(|_| Error::Config("invalid ChatGPT session".into()))
            })
            .transpose()?;
        if let Some(saved) = saved
            && saved.subject == credential.subject
            && saved.expires_at >= credential.expires_at
        {
            credential = saved;
        }
    }
    if !credential.has_plan_access() {
        return Err(Error::Config(
            "ChatGPT plan permission is not granted".into(),
        ));
    }
    let Some(chat_lock) = chat_lock else {
        return Ok(credential.access_token);
    };
    let now = unix_seconds()?;
    // Stored expiry is unadjusted; Pi subtracts three minutes before applying
    // its five-minute OAuth refresh window.
    let requested = minimum.unwrap_or_default().max(Duration::from_secs(300));
    let seconds = requested
        .as_secs()
        .saturating_add(u64::from(requested.subsec_nanos() != 0))
        .saturating_add(PI_EXPIRY_OFFSET);
    if credential.expires_at <= now.saturating_add(seconds) {
        credential = refresh_with(
            directory,
            credential,
            Some(active),
            pi_entry(&data).then_some(seconds),
            chat_lock,
            exchange,
        )
        .await?;
        if minimum.is_some() && credential.expires_at <= unix_seconds()?.saturating_add(seconds) {
            return Err(Error::Config(
                "Refreshed ChatGPT credential expires too soon".into(),
            ));
        }
    }
    if !credential.has_plan_access() {
        return Err(Error::Config(
            "ChatGPT plan permission is not granted".into(),
        ));
    }
    Ok(credential.access_token)
}

#[derive(Clone, Default)]
pub(super) struct LogoutControl(std::sync::Arc<std::sync::atomic::AtomicU8>);
impl LogoutControl {
    // The same atomic transition arbitrates UI cancellation and revocation.
    pub(super) fn cancel_before_commit(&self) -> bool {
        self.0
            .compare_exchange(
                0,
                2,
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
            )
            .is_ok()
    }
}

pub(super) async fn logout(settings: &super::config::Settings) -> Result<bool> {
    logout_controlled(settings, LogoutControl::default()).await
}

pub(super) async fn logout_controlled(
    settings: &super::config::Settings,
    control: LogoutControl,
) -> Result<bool> {
    let chat_lock = lock().await?;
    let http = client(settings);
    logout_with(
        super::config::agent_dir(),
        chat_lock,
        control,
        move |credential| async move {
            match http {
                Ok(client) => credential.revoke(&client).await,
                Err(error) => Err(error),
            }
        },
    )
    .await
}

// State 0 is cancellable, 1 has committed to revocation, 2 is cancelled.
struct LogoutCancellation(std::sync::Arc<std::sync::atomic::AtomicU8>);
impl Drop for LogoutCancellation {
    fn drop(&mut self) {
        let _ = self.0.compare_exchange(
            0,
            2,
            std::sync::atomic::Ordering::SeqCst,
            std::sync::atomic::Ordering::SeqCst,
        );
    }
}

// Cancellation can interrupt lock acquisition. Once remote revocation begins,
// finish local removal under both locks, even if the caller is cancelled.
pub(super) async fn logout_with<F, Fut>(
    directory: std::path::PathBuf,
    chat_lock: std::fs::File,
    control: LogoutControl,
    revoke: F,
) -> Result<bool>
where
    F: FnOnce(pi_rs::openai::oauth::Credential) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Result<()>> + Send + 'static,
{
    logout_with_cleanup_hook(directory, chat_lock, control, revoke, || {}).await
}

// The hook lets tests observe the boundary immediately before local cleanup.
pub(super) async fn logout_with_cleanup_hook<F, Fut, H>(
    directory: std::path::PathBuf,
    chat_lock: std::fs::File,
    control: LogoutControl,
    revoke: F,
    before_cleanup: H,
) -> Result<bool>
where
    F: FnOnce(pi_rs::openai::oauth::Credential) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Result<()>> + Send + 'static,
    H: FnOnce() + Send + 'static,
{
    let auth = directory.join("auth.json");
    let guard = super::auth_lock::acquire_async(&auth).await?;
    let data = super::config::read_auth_object_locked(&guard)?;
    let (credential, client_id) = match parse_credential(&data) {
        Ok(Some(credential)) => {
            let client_id = (!pi_entry(&data)).then(|| credential.client_id.clone());
            (Some(credential), client_id)
        }
        Ok(None) => return Ok(false),
        // The unusable entry cannot be revoked. Its native session can still
        // be cleared when the client ID is intact.
        Err(_) => (
            None,
            data[PROVIDER]["credential"]["client_id"]
                .as_str()
                .map(str::to_owned),
        ),
    };
    guard.check()?;
    let state = control.0;
    let _cancellation = LogoutCancellation(state.clone());
    tokio::task::spawn_blocking(move || {
        use std::sync::atomic::Ordering;
        let _chat_lock = chat_lock;
        let Some(barrier) =
            super::process::refresh_barrier_cancellable(|| state.load(Ordering::SeqCst) == 2)?
        else {
            return Err(Error::Cancelled);
        };
        let guard = super::process::guard_refresh_auth(barrier, guard);
        if state
            .compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Err(Error::Cancelled);
        }
        let revoked = match credential {
            Some(credential) => tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(Error::Io)
                .and_then(|runtime| runtime.block_on(revoke(credential))),
            None => Ok(()),
        };
        before_cleanup();
        if let Some(id) = client_id {
            clear_session_locked(&directory, &guard, &id)?;
        } else {
            super::config::update_object_locked(&auth, Some(&guard), |data| {
                data.remove(PROVIDER);
                Ok(())
            })?;
        }
        revoked.map(|()| true).map_err(|_| {
            Error::Config(
                "ChatGPT local credentials removed; remote revocation was not confirmed".into(),
            )
        })
    })
    .await
    .map_err(|_| Error::Config("ChatGPT logout task failed".into()))?
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn access_options_preserve_rotation_and_honor_no_refresh() {
        let dir = tempfile::tempdir().unwrap();
        fixture(dir.path());
        let value = access_at(dir.path().into(), None, true, |_| async {
            panic!("no-refresh must not contact the token service")
        })
        .await
        .unwrap();
        assert_eq!(value, "old-access");

        let error = access_at(
            dir.path().into(),
            Some(Duration::from_secs(1800)),
            false,
            |mut value| async move {
                value.access_token = "new-access".into();
                value.refresh_token = "rotated".into();
                value.expires_at = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs()
                    + 600;
                Ok(value)
            },
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("expires too soon"));
        let stored = load_at(&dir.path().join("auth.json"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.refresh_token, "rotated");
        assert_eq!(stored.access_token, "new-access");
    }
    #[tokio::test]
    async fn pi_credential_refresh_preserves_flat_storage_and_rotation() {
        let dir = tempfile::tempdir().unwrap();
        let auth = dir.path().join("auth.json");
        let before = serde_json::json!({
            PROVIDER: {"type":"oauth","access":"pi-access","refresh":"pi-refresh","expires":1000,
                "clientId":"pi-client","scopes":["chatgpt.tokens.use.direct"],"extra":"keep"},
            "other": {"type":"api_key","key":"unrelated"},
        });
        let encoded = serde_json::to_vec(&before).unwrap();
        std::fs::write(&auth, &encoded).unwrap();
        assert_eq!(
            access_at(dir.path().into(), None, true, |_| async {
                panic!("the read-only check must not refresh");
            })
            .await
            .unwrap(),
            "pi-access"
        );
        assert_eq!(std::fs::read(&auth).unwrap(), encoded);

        let future_expiry = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 3600;
        let token = access_at(
            dir.path().into(),
            None,
            false,
            move |mut credential| async move {
                assert_eq!(credential.client_id, "pi-client");
                assert_eq!(credential.refresh_token, "pi-refresh");
                assert_eq!(credential.expires_at, 1 + PI_EXPIRY_OFFSET);
                credential.refresh_token = "rotated-pi-refresh".into();
                credential.access_token = "new-pi-access".into();
                credential.expires_at = future_expiry;
                Ok(credential)
            },
        )
        .await
        .unwrap();
        assert_eq!(token, "new-pi-access");
        let current = super::super::config::read_object(&auth).unwrap();
        assert_eq!(current[PROVIDER]["access"], "new-pi-access");
        assert_eq!(current[PROVIDER]["refresh"], "rotated-pi-refresh");
        assert_eq!(
            current[PROVIDER]["expires"],
            (future_expiry - PI_EXPIRY_OFFSET) * 1000
        );
        assert_eq!(current[PROVIDER]["extra"], "keep");
        assert_eq!(current["other"], before["other"]);
        assert!(!dir.path().join("chatgpt-registrations.json").exists());
        assert!(!dir.path().join("chatgpt-sessions.json").exists());
    }

    #[tokio::test]
    async fn refresh_reuses_a_rotated_pi_credential_for_the_same_issued_client() {
        let dir = tempfile::tempdir().unwrap();
        let auth = dir.path().join("auth.json");
        let mut data = serde_json::json!({
            PROVIDER: {"type":"oauth","access":"old-access","refresh":"old-refresh","expires":1000,
                "clientId":"issued-client","scopes":["chatgpt.tokens.use.direct"]}
        });
        let previous = parse_credential(&data).unwrap().unwrap();
        data[PROVIDER]["access"] = serde_json::json!("new-access");
        data[PROVIDER]["refresh"] = serde_json::json!("new-refresh");
        data[PROVIDER]["expires"] = serde_json::json!((unix_seconds().unwrap() + 3600) * 1000);
        std::fs::write(&auth, data.to_string()).unwrap();
        let chat_lock = lock_path(dir.path().join("chatgpt-refresh.lock"))
            .await
            .unwrap();
        let current = refresh_with(
            dir.path().into(),
            previous.clone(),
            Some(previous),
            Some(300 + PI_EXPIRY_OFFSET),
            chat_lock,
            |_| async { panic!("Pi already rotated the usable credential") },
        )
        .await
        .unwrap();
        assert_eq!(current.access_token, "new-access");
        assert_eq!(current.refresh_token, "new-refresh");
    }

    fn fixture(directory: &std::path::Path) -> pi_rs::openai::oauth::Credential {
        let value = pi_rs::openai::oauth::Credential {
            client_id: "client".into(),
            subject: "subject".into(),
            email: None,
            id_token: "id".into(),
            access_token: "old-access".into(),
            refresh_token: "old-refresh".into(),
            expires_at: 1,
            scopes: vec!["chatgpt.tokens.use.direct".into()],
        };
        std::fs::write(directory.join("auth.json"), serde_json::json!({"openai-chatgpt":{"type":"oauth","credential":value},"kimi-coding":{"type":"api_key","key":"keep"}}).to_string()).unwrap();
        value
    }
    fn logout_fixture(directory: &std::path::Path) {
        let credential = fixture(directory);
        std::fs::write(
            directory.join("chatgpt-sessions.json"),
            serde_json::json!({"client":credential,"other":{"keep":true}}).to_string(),
        )
        .unwrap();
    }

    #[tokio::test]
    async fn logout_cancellation_before_shared_lock_does_not_revoke_or_remove() {
        let dir = tempfile::tempdir().unwrap();
        logout_fixture(dir.path());
        let auth = dir.path().join("auth.json");
        let guard = super::super::auth_lock::acquire_async(&auth).await.unwrap();
        let chat_lock = lock_path(dir.path().join("chatgpt-refresh.lock"))
            .await
            .unwrap();
        let (started_tx, mut started_rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(logout_with(
            dir.path().to_owned(),
            chat_lock,
            LogoutControl::default(),
            move |_| async move {
                let _ = started_tx.send(());
                Ok(())
            },
        ));
        assert!(
            tokio::time::timeout(Duration::from_millis(150), &mut started_rx)
                .await
                .is_err()
        );
        task.abort();
        let _ = task.await;
        drop(guard);
        assert!(started_rx.await.is_err());
        let auth = super::super::config::read_object(&auth).unwrap();
        assert_eq!(
            auth["openai-chatgpt"]["credential"]["refresh_token"],
            "old-refresh"
        );
        let sessions =
            super::super::config::read_object(&dir.path().join("chatgpt-sessions.json")).unwrap();
        assert!(sessions.get("client").is_some());
        let _lock = tokio::time::timeout(
            Duration::from_secs(2),
            lock_path(dir.path().join("chatgpt-refresh.lock")),
        )
        .await
        .unwrap()
        .unwrap();
    }

    #[tokio::test]
    async fn logout_cancellation_while_waiting_for_commit_barrier_does_not_revoke() {
        let dir = tempfile::tempdir().unwrap();
        logout_fixture(dir.path());
        let barrier = super::super::process::refresh_barrier().unwrap();
        let chat_lock = lock_path(dir.path().join("chatgpt-refresh.lock"))
            .await
            .unwrap();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(logout_with(
            dir.path().to_owned(),
            chat_lock,
            LogoutControl::default(),
            move |_| async move {
                let _ = started_tx.send(());
                Ok(())
            },
        ));
        tokio::time::sleep(Duration::from_millis(150)).await;
        task.abort();
        let _ = task.await;
        let guard = tokio::time::timeout(
            Duration::from_secs(2),
            super::super::auth_lock::acquire_async(&dir.path().join("auth.json")),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(started_rx.await.is_err());
        let auth =
            super::super::config::read_object_unlocked(&dir.path().join("auth.json")).unwrap();
        assert_eq!(
            auth["openai-chatgpt"]["credential"]["refresh_token"],
            "old-refresh"
        );
        drop(guard);
        drop(barrier);
    }

    #[test]
    fn cancelled_logout_finishes_local_cleanup_after_success_or_failure() {
        for success in [true, false] {
            let dir = tempfile::tempdir().unwrap();
            logout_fixture(dir.path());
            let directory = dir.path().to_owned();
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let (started_tx, started_rx) = tokio::sync::oneshot::channel();
            let (release_tx, release_rx) = tokio::sync::oneshot::channel();
            runtime.block_on(async {
                let chat_lock = lock_path(directory.join("chatgpt-refresh.lock"))
                    .await
                    .unwrap();
                let task = tokio::spawn(logout_with(
                    directory.clone(),
                    chat_lock,
                    LogoutControl::default(),
                    move |_| async move {
                        started_tx.send(()).unwrap();
                        release_rx.await.unwrap();
                        if success {
                            Ok(())
                        } else {
                            Err(Error::Config("fixture revocation failed".into()))
                        }
                    },
                ));
                started_rx.await.unwrap();
                assert!(
                    tokio::time::timeout(
                        Duration::from_millis(100),
                        super::super::auth_lock::acquire_async(&directory.join("auth.json"))
                    )
                    .await
                    .is_err()
                );
                task.abort();
                std::thread::spawn(move || {
                    std::thread::sleep(Duration::from_millis(200));
                    release_tx.send(()).unwrap();
                });
            });
            let start = std::time::Instant::now();
            drop(runtime);
            assert!(start.elapsed() >= Duration::from_millis(100));
            let auth = super::super::config::read_object(&directory.join("auth.json")).unwrap();
            assert!(auth.get("openai-chatgpt").is_none());
            assert_eq!(auth["kimi-coding"]["key"], "keep");
            let sessions =
                super::super::config::read_object(&directory.join("chatgpt-sessions.json"))
                    .unwrap();
            assert!(sessions.get("client").is_none());
            assert_eq!(sessions["other"]["keep"], true);
        }
    }

    #[tokio::test]
    async fn failed_revocation_reports_failure_after_local_removal() {
        let dir = tempfile::tempdir().unwrap();
        logout_fixture(dir.path());
        let lock = lock_path(dir.path().join("chatgpt-refresh.lock"))
            .await
            .unwrap();
        let error = logout_with(
            dir.path().to_owned(),
            lock,
            LogoutControl::default(),
            |_| async { Err(Error::Config("fixture failure".into())) },
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("local credentials removed"));
        assert!(
            super::super::config::read_object(&dir.path().join("auth.json"))
                .unwrap()
                .get("openai-chatgpt")
                .is_none()
        );
    }

    #[tokio::test]
    async fn reads_and_pre_refresh_lock_wait_are_cancellable() {
        let dir = tempfile::tempdir().unwrap();
        let previous = fixture(dir.path());
        let auth = dir.path().join("auth.json");
        let shared = super::super::auth_lock::acquire_async(&auth).await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(150), load_at(&auth))
                .await
                .is_err()
        );
        let chat_lock = lock_path(dir.path().join("chatgpt-refresh.lock"))
            .await
            .unwrap();
        let (entered_tx, mut entered_rx) = tokio::sync::oneshot::channel();
        let path = dir.path().to_owned();
        let task = tokio::spawn(refresh_with(
            path,
            previous.clone(),
            Some(previous),
            None,
            chat_lock,
            move |value| async move {
                let _ = entered_tx.send(());
                Ok(value)
            },
        ));
        assert!(
            tokio::time::timeout(Duration::from_millis(150), &mut entered_rx)
                .await
                .is_err()
        );
        task.abort();
        let _ = task.await;
        drop(shared);
        let loaded = load_at(&auth).await.unwrap().unwrap();
        assert_eq!(loaded.refresh_token, "old-refresh");
        assert!(entered_rx.await.is_err());
        let _reacquired = tokio::time::timeout(
            Duration::from_secs(2),
            lock_path(dir.path().join("chatgpt-refresh.lock")),
        )
        .await
        .unwrap()
        .unwrap();
    }
    #[tokio::test]
    async fn changed_active_account_is_not_refreshed_or_overwritten() {
        let dir = tempfile::tempdir().unwrap();
        let previous = fixture(dir.path());
        let auth = dir.path().join("auth.json");
        let shared = super::super::auth_lock::acquire_async(&auth).await.unwrap();
        let chat_lock = lock_path(dir.path().join("chatgpt-refresh.lock"))
            .await
            .unwrap();
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(refresh_with(
            dir.path().to_owned(),
            previous.clone(),
            Some(previous),
            None,
            chat_lock,
            move |value| async move {
                let _ = entered_tx.send(());
                Ok(value)
            },
        ));
        tokio::time::sleep(Duration::from_millis(100)).await;
        let mut changed = super::super::config::read_object_unlocked(&auth).unwrap();
        changed["openai-chatgpt"]["credential"]["access_token"] = serde_json::json!("different");
        std::fs::write(&auth, changed.to_string()).unwrap();
        drop(shared);
        assert!(task.await.unwrap().is_err());
        assert!(entered_rx.await.is_err());
        assert_eq!(
            super::super::config::read_object(&auth).unwrap()["openai-chatgpt"]["credential"]["access_token"],
            "different"
        );
    }

    #[test]
    fn cancelled_refresh_survives_runtime_shutdown_and_persists_rotation() {
        let dir = tempfile::tempdir().unwrap();
        let previous = fixture(dir.path());
        let directory = dir.path().to_owned();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        runtime.block_on(async {
            let chat_lock = lock_path(directory.join("chatgpt-refresh.lock"))
                .await
                .unwrap();
            let task = tokio::spawn(refresh_with(
                directory.clone(),
                previous.clone(),
                Some(previous),
                None,
                chat_lock,
                move |mut value| async move {
                    started_tx.send(()).unwrap();
                    reply_rx.await.unwrap();
                    value.access_token = "new-access".into();
                    value.refresh_token = "rotated".into();
                    value.expires_at = 4_000_000_000;
                    Ok(value)
                },
            ));
            started_rx.await.unwrap();
            task.abort();
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(200));
                reply_tx.send(()).unwrap();
            });
        });
        let start = std::time::Instant::now();
        drop(runtime);
        assert!(start.elapsed() >= Duration::from_millis(100));
        let auth = super::super::config::read_object(&directory.join("auth.json")).unwrap();
        assert_eq!(
            auth["openai-chatgpt"]["credential"]["refresh_token"],
            "rotated"
        );
        assert_eq!(auth["kimi-coding"]["key"], "keep");
        let sessions =
            super::super::config::read_object(&directory.join("chatgpt-sessions.json")).unwrap();
        assert_eq!(sessions["client"]["refresh_token"], "rotated");
    }
}
