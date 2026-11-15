//! Standalone, coalescing delivery of host-supplied wake values.
//!
//! This module does not calculate a wake or subscribe to scheduler changes.
//! The integrating host must publish an initial value and every relevant change,
//! and call `kick` when a failed delivery should be offered again.

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex};

/// When the host next needs a running process. `at` is Unix milliseconds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Wake {
    pub active: bool,
    pub at: Option<i64>,
}

/// A callback rejection or panic. Failures are not retried until another kick
/// or publication; a panic is contained so later attempts remain possible.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DeliveryError {
    Rejected(String),
    Panicked,
}

type Callback = dyn Fn(Wake) -> Result<(), String> + Send + Sync;
type ErrorCallback = dyn Fn(DeliveryError) + Send + Sync;

struct State {
    current: Wake,
    delivered: Option<Wake>,
    pending: bool,
    running: bool,
    closed: bool,
}
struct Inner {
    state: Mutex<State>,
    callback: Box<Callback>,
    on_error: Box<ErrorCallback>,
    runtime: tokio::runtime::Handle,
}

/// Cloneable handle to a single serial callback delivery lane.
///
/// Construction queues the initial value on the supplied Tokio runtime. Calls
/// are coalesced until the spawned task runs; the runtime must remain running and
/// callbacks must not block. No lock
/// is held during callbacks, so they may publish, kick, or shut down reentrantly.
/// Shutdown cancels queued work; a callback already claimed by the delivery task
/// may start or finish after shutdown returns.
#[derive(Clone)]
pub struct WakeDelivery(Arc<Inner>);

impl WakeDelivery {
    pub fn new(
        runtime: tokio::runtime::Handle,
        initial: Wake,
        callback: impl Fn(Wake) -> Result<(), String> + Send + Sync + 'static,
        on_error: impl Fn(DeliveryError) + Send + Sync + 'static,
    ) -> Self {
        let this = Self(Arc::new(Inner {
            state: Mutex::new(State {
                current: initial,
                delivered: None,
                pending: false,
                running: false,
                closed: false,
            }),
            callback: Box::new(callback),
            on_error: Box::new(on_error),
            runtime,
        }));
        this.kick();
        this
    }

    /// Replace the latest value and schedule a delivery pass, even if equal.
    /// An equal value is suppressed only if its previous delivery succeeded.
    pub fn publish(&self, wake: Wake) {
        self.schedule(Some(wake));
    }

    /// Schedule another pass using the latest published value.
    pub fn kick(&self) {
        self.schedule(None);
    }

    fn schedule(&self, wake: Option<Wake>) {
        let mut state = self.0.state.lock().unwrap();
        if state.closed {
            return;
        }
        if let Some(wake) = wake {
            state.current = wake;
        }
        state.pending = true;
        if state.running {
            return;
        }
        state.running = true;
        let inner = self.0.clone();
        self.0.runtime.spawn(async move { deliver(inner) });
    }

    /// Stop future delivery without waiting for a callback already in progress.
    /// Safe to call from either callback, and idempotent.
    pub fn shutdown(&self) {
        let mut state = self.0.state.lock().unwrap();
        state.closed = true;
        state.pending = false;
    }
}

fn deliver(inner: Arc<Inner>) {
    loop {
        let wake = {
            let mut state = inner.state.lock().unwrap();
            if state.closed || !state.pending {
                state.running = false;
                return;
            }
            state.pending = false;
            let wake = state.current;
            if state.delivered == Some(wake) {
                continue;
            }
            wake
        };
        let result = catch_unwind(AssertUnwindSafe(|| (inner.callback)(wake)));
        match result {
            Ok(Ok(())) => inner.state.lock().unwrap().delivered = Some(wake),
            failure => {
                let error = match failure {
                    Ok(Err(error)) => DeliveryError::Rejected(error),
                    Err(_) => DeliveryError::Panicked,
                    Ok(Ok(())) => unreachable!(),
                };
                // An error observer is host code too; its panic cannot wedge the lane.
                let _ = catch_unwind(AssertUnwindSafe(|| (inner.on_error)(error)));
            }
        }
    }
}
