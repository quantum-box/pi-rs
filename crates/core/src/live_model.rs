//! A host can prepare a replacement route while a model request is active.
use std::sync::{Arc, Mutex};

use crate::{Limits, Model, Notification, Result, Session};

/// A fully configured model to adopt at the next request boundary.
pub struct LiveModelSelection {
    pub adapter: Arc<dyn Model>,
    pub provider: Option<String>,
    pub request_provider: String,
    pub model: String,
    pub thinking: Option<String>,
    pub limits: Limits,
}

/// Shared with the host while the agent owns the session journal.
#[derive(Clone, Default)]
pub struct LiveModelControl(Arc<Mutex<Option<Arc<LiveModelSelection>>>>);

impl LiveModelControl {
    /// Replace any selection that has not reached a request boundary yet.
    pub fn select(&self, selection: Arc<LiveModelSelection>) {
        *self.0.lock().expect("live model control") = Some(selection);
    }

    pub(crate) fn selection(&self) -> Option<Arc<LiveModelSelection>> {
        self.0.lock().expect("live model control").clone()
    }
}

impl Session {
    /// Persist a prepared selection while retaining the exclusive journal lock.
    pub fn apply_live_model_selection(
        &mut self,
        selection: &LiveModelSelection,
        observe: &mut dyn FnMut(Notification<'_>),
    ) -> Result<()> {
        let existing = self.model_selection();
        let same_model = existing
            .as_ref()
            .is_some_and(|(model, _)| model == &selection.model)
            && self.model_provider() == selection.provider.as_deref();
        let same_thinking = existing.as_ref().and_then(|(_, level)| level.as_deref())
            == selection.thinking.as_deref();
        let before = self.records().len();
        if !(same_model && same_thinking) {
            if same_model && let Some(thinking) = selection.thinking.as_deref() {
                self.select_thinking_level(
                    selection.provider.as_deref(),
                    &selection.model,
                    thinking,
                )?;
            } else {
                self.select_provider_model(
                    selection.provider.as_deref(),
                    &selection.model,
                    selection.thinking.as_deref(),
                )?;
            }
        }
        self.record_request_identity(&selection.request_provider, &selection.model)?;
        for record in &self.records()[before..] {
            observe(Notification::Committed(record));
        }
        Ok(())
    }
}
