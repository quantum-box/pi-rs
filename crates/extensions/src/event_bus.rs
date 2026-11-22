//! Application-defined events shared by an embedded host and its extensions.
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, Weak};

use pi_rs_core::ExtensionResult;
use serde_json::Value;

type Handler = dyn Fn(&Value) -> ExtensionResult<()> + Send + Sync;
type Channels = BTreeMap<String, Vec<Arc<Handler>>>;

/// A host-owned, in-process message bus. Clones share the same listeners.
/// The host chooses which extension instances receive a clone.
#[derive(Clone, Default)]
pub struct ExtensionEventBus {
    channels: Arc<Mutex<Channels>>,
}

/// Keeps one listener registered. Dropping it removes that listener while
/// preserving any other listeners registered on the channel.
#[must_use = "keep the subscription alive for as long as it should receive events"]
pub struct EventSubscription {
    channels: Weak<Mutex<Channels>>,
    channel: String,
    handler: Weak<Handler>,
}

impl Drop for EventSubscription {
    fn drop(&mut self) {
        let (Some(channels), Some(handler)) = (self.channels.upgrade(), self.handler.upgrade())
        else {
            return;
        };
        let mut channels = channels.lock().expect("extension event bus");
        if let Some(listeners) = channels.get_mut(&self.channel) {
            listeners.retain(|listener| !Arc::ptr_eq(listener, &handler));
            if listeners.is_empty() {
                channels.remove(&self.channel);
            }
        }
    }
}

impl ExtensionEventBus {
    pub fn on(
        &self,
        channel: impl Into<String>,
        handler: impl Fn(&Value) -> ExtensionResult<()> + Send + Sync + 'static,
    ) -> EventSubscription {
        let channel = channel.into();
        let handler: Arc<Handler> = Arc::new(handler);
        let subscription = EventSubscription {
            channels: Arc::downgrade(&self.channels),
            channel: channel.clone(),
            handler: Arc::downgrade(&handler),
        };
        self.channels
            .lock()
            .expect("extension event bus")
            .entry(channel)
            .or_default()
            .push(handler);
        subscription
    }

    /// Deliver one JSON value in listener registration order. Listeners are
    /// called on the emitting thread, outside the bus lock. Changes to
    /// subscriptions affect future emits. Failures are returned to the caller
    /// and do not stop delivery to later listeners.
    pub fn emit(&self, channel: &str, data: &Value) -> Vec<String> {
        let listeners = self
            .channels
            .lock()
            .expect("extension event bus")
            .get(channel)
            .cloned()
            .unwrap_or_default();
        let mut errors = Vec::new();
        for listener in listeners {
            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| listener(data))) {
                Ok(Ok(())) => {}
                Ok(Err(error)) => errors.push(error),
                Err(_) => errors.push("extension event handler panicked".into()),
            }
        }
        errors
    }

    /// Remove every listener. Callers can also drop individual subscriptions
    /// to replace extension instances while retaining host listeners.
    pub fn clear(&self) {
        let listeners = std::mem::take(&mut *self.channels.lock().expect("extension event bus"));
        drop(listeners);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn event_subscriptions_preserve_order_and_isolate_reloads_and_errors() {
        let bus = ExtensionEventBus::default();
        let calls = Arc::new(Mutex::new(Vec::new()));
        let host = {
            let calls = calls.clone();
            bus.on("message", move |data| {
                calls.lock().unwrap().push(("host", data.clone()));
                Ok(())
            })
        };
        {
            let calls = calls.clone();
            let _extension = bus.on("message", move |data| {
                calls.lock().unwrap().push(("old extension", data.clone()));
                Err("observer failed".into())
            });
            assert_eq!(bus.emit("message", &json!(1)), ["observer failed"]);
        }
        let current = {
            let calls = calls.clone();
            bus.on("message", move |data| {
                calls
                    .lock()
                    .unwrap()
                    .push(("current extension", data.clone()));
                Ok(())
            })
        };
        assert!(bus.emit("message", &json!(2)).is_empty());
        assert!(bus.emit("other", &json!(3)).is_empty());
        drop(current);
        assert!(bus.emit("message", &json!(4)).is_empty());
        bus.clear();
        assert!(bus.emit("message", &json!(5)).is_empty());
        drop(host);
        assert_eq!(
            *calls.lock().unwrap(),
            [
                ("host", json!(1)),
                ("old extension", json!(1)),
                ("host", json!(2)),
                ("current extension", json!(2)),
                ("host", json!(4)),
            ],
        );
    }

    #[test]
    fn nested_emits_and_subscription_changes_do_not_lock_the_bus() {
        let bus = ExtensionEventBus::default();
        let child_hits = Arc::new(Mutex::new(0));
        let _child = {
            let child_hits = child_hits.clone();
            bus.on("child", move |_| {
                *child_hits.lock().unwrap() += 1;
                Ok(())
            })
        };
        let _parent = {
            let bus_for_listener = bus.clone();
            bus.on("parent", move |data| {
                assert!(bus_for_listener.emit("child", data).is_empty());
                bus_for_listener.clear();
                Ok(())
            })
        };
        let _later = {
            let child_hits = child_hits.clone();
            bus.on("parent", move |_| {
                *child_hits.lock().unwrap() += 1;
                Ok(())
            })
        };
        assert!(bus.emit("parent", &Value::Null).is_empty());
        assert!(bus.emit("parent", &Value::Null).is_empty());
        assert_eq!(*child_hits.lock().unwrap(), 2);
    }
}
