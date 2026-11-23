//! Own native subprocesses until their parent has joined them.
use std::process::ExitStatus;
use std::sync::{Arc, Mutex};

use pi_rs::{CancellationToken, Error, Result};
use process_wrap::tokio::{ChildWrapper, CommandWrap};

#[derive(Clone, Default)]
pub struct Children {
    state: Arc<Mutex<State>>,
}

#[derive(Default)]
struct State {
    sealed: bool,
    children: Vec<Process>,
}

pub(crate) struct Process {
    child: Arc<tokio::sync::Mutex<Option<Box<dyn ChildWrapper>>>>,
    cancel: CancellationToken,
}

pub fn owns_kill_target(child: &dyn ChildWrapper) -> bool {
    // On Windows the Job Object still owns descendants after the leader exits.
    // On Unix a process group ID is only safe while the leader is unreaped.
    cfg!(windows) || child.id().is_some()
}

impl Children {
    pub fn adopt(&self, mut child: Box<dyn ChildWrapper>) {
        let mut state = self.state.lock().expect("owned subprocesses");
        if state.sealed {
            let _ = child.start_kill();
        } else {
            state.children.push(Process::new(child));
        }
    }

    pub(crate) fn spawn<T>(
        &self,
        mut command: CommandWrap,
        take_pipes: impl FnOnce(&mut dyn ChildWrapper) -> T,
    ) -> Result<(Process, T)> {
        let mut state = self.state.lock().expect("owned subprocesses");
        if state.sealed {
            return Err(Error::Cancelled);
        }
        let mut child = command.spawn()?;
        let pipes = take_pipes(child.as_mut());
        let process = Process::new(child);
        state.children.push(Process {
            child: process.child.clone(),
            cancel: process.cancel.clone(),
        });
        Ok((process, pipes))
    }

    pub async fn finish(self) -> Result<()> {
        let children = {
            let mut state = self.state.lock().expect("owned subprocesses");
            state.sealed = true;
            std::mem::take(&mut state.children)
        };
        for process in &children {
            process.cancel.cancel();
        }
        let results =
            futures_util::future::join_all(children.into_iter().map(Process::terminate)).await;
        results
            .into_iter()
            .find_map(|result| result.err())
            .map_or(Ok(()), |error| {
                Err(Error::Config(format!(
                    "Could not reap subprocess after Radius share preparation: {error}"
                )))
            })
    }
}

impl Process {
    fn new(child: Box<dyn ChildWrapper>) -> Self {
        Self {
            child: Arc::new(tokio::sync::Mutex::new(Some(child))),
            cancel: CancellationToken::new(),
        }
    }

    pub(crate) async fn wait(&self) -> Result<ExitStatus> {
        let mut owned = self.child.lock().await;
        let child = owned.as_mut().ok_or(Error::Cancelled)?;
        let status = tokio::select! {
            biased;
            _ = self.cancel.cancelled() => return Err(Error::Cancelled),
            status = child.wait() => status?,
        };
        owned.take();
        Ok(status)
    }

    async fn terminate(self) -> std::io::Result<()> {
        let Some(mut child) = self.child.lock().await.take() else {
            return Ok(());
        };
        // Signal while the leader's PID is still owned, even if it exited.
        if owns_kill_target(child.as_ref())
            && let Err(error) = child.start_kill()
            && child.try_wait()?.is_none()
        {
            return Err(error);
        }
        child.wait().await.map(|_| ())
    }
}
