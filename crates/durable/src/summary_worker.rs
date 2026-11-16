//! Private, explicitly scheduled summary execution. The guard spans recovery,
//! model invocation and settlement; no summary is applied to a conversation.
use std::{sync::Arc, time::Duration};

use durable_store::owner::OwnedStore;
use pi_rs_core::{CancellationToken, Model};
use uuid::Uuid;

use crate::summary_store::{self, Snapshot, State, SummaryStore};

#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error("summary storage: {0}")]
    Store(#[from] summary_store::Error),
    #[error("summary model: {0}")]
    Model(#[from] pi_rs_core::Error),
    #[error("summary worker interrupted")]
    Interrupted,
    #[error("standalone cleanup outcome uncertain; reconcile before retry")]
    CleanupUncertain,
    #[error("summary provider identity is missing or does not match")]
    Identity,
    #[error("summary placement: {0}")]
    Placement(String),
    #[error("summary hook: {0}")]
    Hook(String),
}

fn snapshot(store: &SummaryStore<'_>, id: Uuid) -> Result<Snapshot, Error> {
    Ok(store.get(id)?.ok_or(summary_store::Error::NotFound)?)
}

/// Drive one operation until terminal or an error. Only the scheduler owns the
/// task and its cancellation token. A failed attempt is retried only by a
/// subsequent explicit scheduling call, with a fresh request identity.
pub(crate) async fn run(
    owner: Arc<OwnedStore>,
    model: Arc<dyn Model>,
    id: Uuid,
    cancel: CancellationToken,
    provider: summary_store::ProviderIdentity,
    hook_harness: Option<&crate::runtime::Harness>,
) -> Result<(), Error> {
    run_for_cleanup(owner, model, id, cancel, provider, hook_harness, None).await
}

pub(crate) async fn run_for_cleanup(
    owner: Arc<OwnedStore>,
    model: Arc<dyn Model>,
    id: Uuid,
    cancel: CancellationToken,
    provider: summary_store::ProviderIdentity,
    hook_harness: Option<&crate::runtime::Harness>,
    cleanup_attempt: Option<Uuid>,
) -> Result<(), Error> {
    let _guard = tokio::select! {
        biased;
        _ = cancel.cancelled() => return Err(Error::Interrupted),
        guard = owner.summary_guard(id) => guard,
    };
    let store = SummaryStore::new(owner.store());
    let executor = Uuid::new_v4();
    let mut observed_active = false;
    loop {
        if cancel.is_cancelled() {
            return Err(Error::Interrupted);
        }
        let snap = snapshot(&store, id)?;
        let marker = owner
            .store()
            .get(&crate::runtime::owned_cleanup_key(
                snap.operation.session_id(),
            ))
            .map_err(summary_store::Error::from)?;
        match (marker, cleanup_attempt) {
            (None, None) => (),
            (Some(marker), Some(attempt))
                if serde_json::from_slice::<Uuid>(&marker.value).ok() == Some(attempt) =>
            {
                ()
            }
            _ => return Err(Error::CleanupUncertain),
        }
        if !provider.valid() || snap.operation.provider() != Some(&provider) {
            return Err(Error::Identity);
        }
        if !matches!(snap.operation.state(), State::Aborting { .. }) {
            observed_active = true;
        }
        match snap.operation.state() {
            State::Completed { .. }
            | State::PlacementPending { .. }
            | State::Placed { .. }
            | State::Stale { .. }
            | State::Aborted
            | State::Declined => return Ok(()),
            State::HookPending => {
                let checkpoint = snap
                    .operation
                    .hook()
                    .ok_or(summary_store::Error::InvalidRecord)?;
                let harness = hook_harness
                    .ok_or_else(|| Error::Hook("hook runtime is unavailable".into()))?;
                let decision = harness
                    .run_independent_before_compact(
                        id,
                        snap.operation.session_id(),
                        snap.operation.preparation(),
                        checkpoint,
                        &cancel,
                    )
                    .await
                    .map_err(|error| match error {
                        crate::runtime::Error::Interrupted => Error::Interrupted,
                        other => Error::Hook(other.to_string()),
                    })?;
                store.finish_hook(id, snap.version, decision)?;
            }
            State::Reserved { owner: old, .. } | State::Running { owner: old, .. } => {
                store.recover(id, snap.version, *old)?;
            }
            State::Ready => {
                if settle_one(&store, &*model, id, &snap, &cancel).await? {
                    continue;
                }
                store.reserve(id, snap.version, executor, Uuid::new_v4())?;
                let snap = snapshot(&store, id)?;
                if cancel.is_cancelled() {
                    return Err(Error::Interrupted);
                }
                store.start(id, snap.version, executor)?;
                let snap = snapshot(&store, id)?;
                let State::Running { request_id, .. } = snap.operation.state() else {
                    return Err(summary_store::Error::Conflict.into());
                };
                let generation = crate::model::collect_generation(
                    model.stream_for_summary(
                        *request_id,
                        snap.operation.session_id(),
                        snap.operation.preparation().request().clone(),
                    ),
                    &cancel,
                    |_| {},
                )
                .await
                .map_err(|error| {
                    if cancel.is_cancelled() {
                        Error::Interrupted
                    } else {
                        Error::Model(error)
                    }
                })?;
                store.complete(id, snap.version, executor, generation)?;
            }
            State::Accounting { owner: old, .. } if *old != executor => {
                store.recover_accounting(id, snap.version, *old, executor)?;
            }
            State::Accounting { .. } => {
                if !settle_one(&store, &*model, id, &snap, &cancel).await? {
                    store.publish(id, snap.version, executor)?;
                }
            }
            State::Aborting { owner: old, .. } => {
                // The executor that observed the abort must return before a
                // separately joined cleanup worker finishes the transition.
                if observed_active {
                    return Ok(());
                }
                if !settle_one(&store, &*model, id, &snap, &cancel).await? {
                    store.finish_abort(id, snap.version, *old)?;
                }
            }
        }
    }
}

// The provider must implement idempotence by request ID. The durable report
// remains identical on replay after an acknowledged but unrecorded settlement.
async fn settle_one(
    store: &SummaryStore<'_>,
    model: &dyn Model,
    id: Uuid,
    snap: &Snapshot,
    cancel: &CancellationToken,
) -> Result<bool, Error> {
    let accounting = match snap.operation.state() {
        State::Accounting {
            request_id,
            generation,
            ..
        } => Some((*request_id, generation.as_ref())),
        State::Aborting {
            accounting: Some(pending),
            ..
        } => Some((pending.request_id, pending.generation.as_ref())),
        _ => None,
    };
    let Some(attempt) = snap.operation.attempts().iter().find(|attempt| {
        !attempt.settled
            && (attempt.unconfirmed || accounting.is_some_and(|(id, _)| id == attempt.request_id))
    }) else {
        return Ok(false);
    };
    let request = attempt.request_id;
    let report = snap.operation.settlement_report(request)?;
    let settlement = async {
        match accounting.filter(|(id, _)| *id == request && !attempt.unconfirmed) {
            Some((_, generation)) => {
                model
                    .record_usage_with_metadata(request, &report, &generation.metadata)
                    .await
            }
            None => model.record_usage(request, &report).await,
        }
    };
    tokio::select! {
        biased;
        _ = cancel.cancelled() => return Err(Error::Interrupted),
        result = tokio::time::timeout(Duration::from_secs(120), settlement) => {
            result.map_err(|_| Error::Interrupted)??;
        }
    }
    store.record_settlement(id, snap.version, request)?;
    Ok(true)
}
