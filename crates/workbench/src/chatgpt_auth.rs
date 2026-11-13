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
fn parse_credential(value: &serde_json::Value) -> Result<Option<pi_rs::openai::oauth::Credential>> {
    value
        .get(PROVIDER)
        .filter(|value| value["type"] == "oauth")
        .map(|value| {
            serde_json::from_value(value["credential"].clone())
                .map_err(|_| Error::Config("invalid ChatGPT credential".into()))
        })
        .transpose()
}
async fn load() -> Result<Option<pi_rs::openai::oauth::Credential>> {
    load_at(&auth_path()).await
}
async fn load_at(path: &std::path::Path) -> Result<Option<pi_rs::openai::oauth::Credential>> {
    parse_credential(&super::config::read_auth_object(path).await?)
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
    previous: pi_rs::openai::oauth::Credential,
    expected_active: Option<pi_rs::openai::oauth::Credential>,
    chat_lock: std::fs::File,
    exchange: F,
) -> Result<pi_rs::openai::oauth::Credential>
where
    F: FnOnce(pi_rs::openai::oauth::Credential) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Result<pi_rs::openai::oauth::Credential>> + Send + 'static,
{
    let auth = directory.join("auth.json");
    let guard = super::auth_lock::acquire_async(&auth).await?;
    if let Some(expected) = expected_active {
        let current = parse_credential(&super::config::read_object_unlocked(&auth)?)?;
        guard.check()?;
        if current.map(serde_json::to_value).transpose()? != Some(serde_json::to_value(expected)?) {
            return Err(Error::Config(
                "ChatGPT credentials changed during refresh".into(),
            ));
        }
    }
    tokio::task::spawn_blocking(move || {
        let _chat_lock = chat_lock;
        let _barrier = super::process::refresh_barrier()?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|_| Error::Config("could not start ChatGPT refresh runtime".into()))?;
        match runtime.block_on(exchange(previous.clone())) {
            Ok(refreshed) => {
                save_locked(&directory, &guard, &refreshed)?;
                Ok(refreshed)
            }
            Err(error @ Error::AuthorizationRequired { .. }) => {
                clear_session_locked(&directory, &guard, &previous.client_id)?;
                Err(error)
            }
            Err(error) => Err(error),
        }
    })
    .await
    .map_err(|_| Error::Config("ChatGPT refresh task failed".into()))?
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
    let chat_lock = lock().await?;
    let active = load()
        .await?
        .ok_or_else(|| Error::Config("sign in with pi login openai-chatgpt".into()))?;
    let mut credential = active.clone();
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
        let http = client(settings)?;
        credential = refresh_with(
            super::config::agent_dir(),
            credential,
            Some(active),
            chat_lock,
            move |previous| async move { previous.refresh(&http).await },
        )
        .await?;
    }
    if !credential.has_plan_access() {
        return Err(Error::Config(
            "ChatGPT plan permission is not granted".into(),
        ));
    }
    Ok(credential.access_token)
}

pub(super) async fn logout(settings: &super::config::Settings) -> Result<bool> {
    let chat_lock = lock().await?;
    let http = client(settings);
    logout_with(
        super::config::agent_dir(),
        chat_lock,
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
async fn logout_with<F, Fut>(
    directory: std::path::PathBuf,
    chat_lock: std::fs::File,
    revoke: F,
) -> Result<bool>
where
    F: FnOnce(pi_rs::openai::oauth::Credential) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Result<()>> + Send + 'static,
{
    let auth = directory.join("auth.json");
    let guard = super::auth_lock::acquire_async(&auth).await?;
    let credential = parse_credential(&super::config::read_object_unlocked(&auth)?)?;
    guard.check()?;
    let Some(credential) = credential else {
        return Ok(false);
    };
    let state = std::sync::Arc::new(std::sync::atomic::AtomicU8::new(0));
    let _cancellation = LogoutCancellation(state.clone());
    tokio::task::spawn_blocking(move || {
        use std::sync::atomic::Ordering;
        let _chat_lock = chat_lock;
        let Some(_barrier) =
            super::process::refresh_barrier_cancellable(|| state.load(Ordering::SeqCst) == 2)?
        else {
            return Err(Error::Cancelled);
        };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build();
        if state
            .compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Err(Error::Cancelled);
        }
        let revoked = match runtime {
            Ok(runtime) => runtime.block_on(revoke(credential.clone())),
            Err(error) => Err(Error::Io(error)),
        };
        clear_session_locked(&directory, &guard, &credential.client_id)?;
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
        let error = logout_with(dir.path().to_owned(), lock, |_| async {
            Err(Error::Config("fixture failure".into()))
        })
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
