//! Owns Radius refreshes across cancellation of standalone share preparation.
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::FutureExt as _;
use pi_rs::{Error, Result};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use super::radius_auth::{Gateway, RefreshAdmission};

type RefreshTask = (Arc<RefreshAdmission>, JoinHandle<()>);

struct State {
    sealed: bool,
    tasks: Vec<RefreshTask>,
}

#[derive(Clone)]
pub(super) struct Broker {
    state: Arc<Mutex<State>>,
    failure: Arc<Mutex<Option<String>>>,
}

impl Broker {
    pub(super) fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(State {
                sealed: false,
                tasks: Vec::new(),
            })),
            failure: Default::default(),
        }
    }

    pub(super) async fn access(
        &self,
        gateway: Gateway,
        min_validity: Duration,
    ) -> Result<Option<String>> {
        let request = Request(Arc::new(RefreshAdmission::default()));
        let (reply, result) = oneshot::channel();
        {
            let mut state = self.state.lock().expect("Radius share refresh state");
            if state.sealed {
                return Err(Error::Cancelled);
            }
            state
                .tasks
                .retain_mut(|(_, task)| match task.now_or_never() {
                    None => true,
                    Some(result) => {
                        if result.is_err() {
                            self.record_failure("Radius share refresh task failed");
                        }
                        false
                    }
                });
            let failure = self.failure.clone();
            let admission = request.0.clone();
            let task = tokio::spawn(async move {
                let result = gateway.access_with_admission(min_validity, admission).await;
                if let Err(error) = &result
                    && !matches!(error, Error::Cancelled)
                {
                    failure
                        .lock()
                        .expect("Radius share refresh failure")
                        .get_or_insert_with(|| error.to_string());
                }
                let _ = reply.send(result);
            });
            state.tasks.push((request.0.clone(), task));
        }
        result
            .await
            .map_err(|_| Error::Config("Radius share refresh task failed".into()))?
    }

    pub(super) fn seal(&self) {
        let mut state = self.state.lock().expect("Radius share refresh state");
        state.sealed = true;
        for (admission, _) in &state.tasks {
            admission.cancel();
        }
    }

    // A closed caller cancels queued refreshes, while this owner awaits an
    // admitted exchange through its final write.
    pub(super) async fn finish(self) -> Result<()> {
        self.seal();
        let tasks =
            std::mem::take(&mut self.state.lock().expect("Radius share refresh state").tasks);
        for (_, task) in tasks {
            if task.await.is_err() {
                self.record_failure("Radius share refresh task failed");
            }
        }
        let failure = self
            .failure
            .lock()
            .expect("Radius share refresh failure")
            .take();
        match failure {
            Some(message) => Err(Error::Config(message)),
            None => Ok(()),
        }
    }

    fn record_failure(&self, message: &str) {
        self.failure
            .lock()
            .expect("Radius share refresh failure")
            .get_or_insert_with(|| message.into());
    }
}

struct Request(Arc<RefreshAdmission>);

impl Drop for Request {
    fn drop(&mut self) {
        self.0.cancel();
    }
}
