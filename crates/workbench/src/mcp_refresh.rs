//! Keep an MCP OAuth rotation alive after its caller is cancelled.
use rmcp::transport::auth::AuthError;
use std::sync::{
    Arc, Condvar, LazyLock, Mutex,
    atomic::{AtomicBool, AtomicU8, Ordering},
};
use std::time::Duration;

// pending, committed, cancelled
struct Transaction {
    state: AtomicU8,
    dropped: AtomicBool,
}
tokio::task_local! { static TRANSACTION: Arc<Transaction>; }
static GATE: LazyLock<(Mutex<(bool, usize)>, Condvar)> =
    LazyLock::new(|| (Mutex::new((false, 0)), Condvar::new()));
static RUNTIME: LazyLock<tokio::runtime::Handle> = LazyLock::new(|| {
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    std::thread::Builder::new()
        .name("pi-mcp-oauth".into())
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("MCP OAuth runtime");
            tx.send(runtime.handle().clone())
                .expect("MCP OAuth runtime receiver");
            runtime.block_on(std::future::pending::<()>());
        })
        .expect("MCP OAuth thread");
    rx.recv().expect("MCP OAuth runtime startup")
});

struct CancelOnDrop(Arc<Transaction>);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.dropped.store(true, Ordering::SeqCst);
        let _ = self
            .0
            .state
            .compare_exchange(0, 2, Ordering::SeqCst, Ordering::SeqCst);
    }
}
async fn cancelled(tx: &Transaction) {
    loop {
        if tx.state.load(Ordering::SeqCst) == 2 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

pub(crate) async fn run<T: Send + 'static>(
    future: impl std::future::Future<Output = T> + Send + 'static,
) -> Result<T, ()> {
    let tx = Arc::new(Transaction {
        state: AtomicU8::new(0),
        dropped: AtomicBool::new(false),
    });
    let _cancel = CancelOnDrop(tx.clone());
    RUNTIME
        .spawn(TRANSACTION.scope(tx.clone(), async move {
            tokio::select! { result = future => Some(result), _ = cancelled(&tx) => None }
        }))
        .await
        .map_err(|_| ())?
        .ok_or(())
}

struct InFlight(Arc<Transaction>);
impl Drop for InFlight {
    fn drop(&mut self) {
        let (lock, wake) = &*GATE;
        let mut gate = lock.lock().unwrap_or_else(|e| e.into_inner());
        self.0.state.store(0, Ordering::SeqCst);
        if self.0.dropped.load(Ordering::SeqCst) {
            let _ = self
                .0
                .state
                .compare_exchange(0, 2, Ordering::SeqCst, Ordering::SeqCst);
        }
        gate.1 -= 1;
        wake.notify_all();
    }
}

pub(crate) struct Guard {
    _file: Arc<std::fs::File>,
    _worker: Option<std::fs::File>,
    _in_flight: Option<InFlight>,
}

pub(crate) async fn guard(file: Arc<std::fs::File>) -> Result<Guard, AuthError> {
    let Ok(tx) = TRANSACTION.try_with(Arc::clone) else {
        return Ok(Guard {
            _file: file,
            _worker: None,
            _in_flight: None,
        });
    };
    let worker = std::env::var_os("PI_RS_WORKER_REFRESH_BARRIER")
        .map(|path| {
            std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(path)
        })
        .transpose()
        .map_err(|_| error())?;
    if let Some(worker) = &worker {
        loop {
            if tx.state.load(Ordering::SeqCst) == 2 {
                return Err(error());
            }
            match worker.try_lock() {
                Ok(()) => break,
                Err(std::fs::TryLockError::WouldBlock) => {
                    tokio::time::sleep(Duration::from_millis(25)).await
                }
                Err(std::fs::TryLockError::Error(_)) => return Err(error()),
            }
        }
    }
    let mut gate = GATE.0.lock().unwrap_or_else(|e| e.into_inner());
    if gate.0
        || tx
            .state
            .compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
    {
        return Err(error());
    }
    gate.1 += 1;
    Ok(Guard {
        _file: file,
        _worker: worker,
        _in_flight: Some(InFlight(tx)),
    })
}
fn error() -> AuthError {
    AuthError::CredentialStoreError("MCP refresh cancelled or unavailable".into())
}

/// Prevent new rotations and wait for committed exchanges to be saved.
pub fn finish_refreshes() {
    let (lock, wake) = &*GATE;
    let mut gate = lock.lock().unwrap_or_else(|e| e.into_inner());
    gate.0 = true;
    while gate.1 != 0 {
        gate = wake.wait(gate).unwrap_or_else(|e| e.into_inner());
    }
}
