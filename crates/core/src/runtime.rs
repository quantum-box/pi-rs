//! Platform boundary for clocks and timers used by the agent runtime.
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub(crate) mod time {
    pub use tokio::time::{Instant, sleep, timeout, timeout_at};
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub(crate) mod time {
    use futures_util::future::{Either, select};
    use std::{cell::RefCell, collections::HashMap, future::Future, ops::Add, time::Duration};
    use wasm_bindgen::{JsCast, prelude::*};

    #[wasm_bindgen]
    extern "C" {
        #[wasm_bindgen(js_name = setTimeout)]
        fn set_timeout(callback: &js_sys::Function, millis: i32) -> i32;
        #[wasm_bindgen(js_name = clearTimeout)]
        fn clear_timeout(id: i32);
    }

    thread_local! {
        static TIMERS: RefCell<HashMap<i32, Closure<dyn FnMut()>>> = RefCell::new(HashMap::new());
    }

    struct Timer(i32);
    impl Drop for Timer {
        fn drop(&mut self) {
            clear_timeout(self.0);
            TIMERS.with(|timers| {
                timers.borrow_mut().remove(&self.0);
            });
        }
    }

    fn start_timer(millis: i32) -> (Timer, tokio::sync::oneshot::Receiver<()>) {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let mut sender = Some(sender);
        let callback = Closure::<dyn FnMut()>::new(move || {
            if let Some(sender) = sender.take() {
                let _ = sender.send(());
            }
        });
        let id = set_timeout(callback.as_ref().unchecked_ref(), millis);
        TIMERS.with(|timers| {
            timers.borrow_mut().insert(id, callback);
        });
        (Timer(id), receiver)
    }

    /// Uses the host's monotonic performance clock when available.
    #[derive(Clone, Copy)]
    pub struct Instant(f64);
    impl Instant {
        pub fn now() -> Self {
            let global = js_sys::global();
            let millis = js_sys::Reflect::get(&global, &JsValue::from_str("performance"))
                .ok()
                .and_then(|performance| {
                    let now = js_sys::Reflect::get(&performance, &JsValue::from_str("now")).ok()?;
                    now.dyn_into::<js_sys::Function>()
                        .ok()?
                        .call0(&performance)
                        .ok()?
                        .as_f64()
                })
                .unwrap_or_else(js_sys::Date::now);
            Self(millis)
        }
        pub fn elapsed(self) -> Duration {
            Duration::from_secs_f64(((Self::now().0 - self.0).max(0.0)) / 1000.0)
        }
    }
    impl Add<Duration> for Instant {
        type Output = Self;
        fn add(self, duration: Duration) -> Self {
            Self(self.0 + duration.as_secs_f64() * 1000.0)
        }
    }

    #[derive(Debug)]
    pub struct Elapsed;

    pub async fn sleep(duration: Duration) {
        let mut remaining = duration;
        loop {
            let slice = remaining.min(Duration::from_millis(i32::MAX as u64));
            let millis = slice
                .as_millis()
                .saturating_add(u128::from(slice.subsec_nanos() % 1_000_000 != 0))
                as i32;
            let (timer, receiver) = start_timer(millis);
            let _ = receiver.await;
            drop(timer);
            remaining = remaining.saturating_sub(slice);
            if remaining.is_zero() {
                return;
            }
        }
    }

    pub async fn timeout<F: Future>(duration: Duration, future: F) -> Result<F::Output, Elapsed> {
        futures_util::pin_mut!(future);
        let timer = sleep(duration);
        futures_util::pin_mut!(timer);
        match select(future, timer).await {
            Either::Left((value, _)) => Ok(value),
            Either::Right(_) => Err(Elapsed),
        }
    }

    pub async fn timeout_at<F: Future>(deadline: Instant, future: F) -> Result<F::Output, Elapsed> {
        let millis = (deadline.0 - Instant::now().0).max(0.0);
        timeout(Duration::from_secs_f64(millis / 1000.0), future).await
    }
}
