use pi_rs_durable::wake_delivery::{DeliveryError, Wake, WakeDelivery};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

fn wake(active: bool, at: Option<i64>) -> Wake {
    Wake { active, at }
}
async fn flush() {
    tokio::task::yield_now().await;
}

#[tokio::test]
async fn coalesces_and_suppresses_only_successfully_delivered_values() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let output = seen.clone();
    let delivery = WakeDelivery::new(
        tokio::runtime::Handle::current(),
        wake(false, None),
        move |w| {
            output.lock().unwrap().push(w);
            Ok(())
        },
        |_| {},
    );
    delivery.publish(wake(true, Some(7)));
    delivery.publish(wake(false, Some(9)));
    flush().await;
    delivery.kick();
    delivery.publish(wake(false, Some(9)));
    flush().await;
    delivery.publish(wake(false, None));
    flush().await;
    assert_eq!(
        *seen.lock().unwrap(),
        vec![wake(false, Some(9)), wake(false, None)]
    );
}

#[tokio::test]
async fn failed_delivery_retries_on_kick_or_publication_without_spinning() {
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let errors = Arc::new(Mutex::new(Vec::new()));
    let output = errors.clone();
    let delivery = WakeDelivery::new(
        tokio::runtime::Handle::current(),
        wake(false, None),
        move |_| match count.fetch_add(1, Ordering::SeqCst) {
            0 => Err("failure".into()),
            1 => panic!("callback"),
            _ => Ok(()),
        },
        move |e| output.lock().unwrap().push(e),
    );
    flush().await;
    flush().await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    delivery.kick();
    flush().await;
    delivery.publish(wake(false, None));
    flush().await;
    delivery.kick();
    flush().await;
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    assert_eq!(
        *errors.lock().unwrap(),
        vec![
            DeliveryError::Rejected("failure".into()),
            DeliveryError::Panicked
        ]
    );
}

#[tokio::test]
async fn callback_can_publish_kick_and_shutdown_reentrantly() {
    let handle: Arc<Mutex<Option<WakeDelivery>>> = Arc::new(Mutex::new(None));
    let callback_handle = handle.clone();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let output = seen.clone();
    let delivery = WakeDelivery::new(
        tokio::runtime::Handle::current(),
        wake(false, None),
        move |w| {
            output.lock().unwrap().push(w);
            let handle = callback_handle.lock().unwrap().as_ref().unwrap().clone();
            if !w.active {
                handle.publish(wake(true, Some(5)));
                handle.kick();
            } else {
                handle.publish(wake(false, Some(8)));
                handle.shutdown();
            }
            Ok(())
        },
        |_| {},
    );
    *handle.lock().unwrap() = Some(delivery.clone());
    flush().await;
    delivery.kick();
    delivery.publish(wake(true, None));
    flush().await;
    assert_eq!(
        *seen.lock().unwrap(),
        vec![wake(false, None), wake(true, Some(5))]
    );
}

#[tokio::test]
async fn shutdown_before_dispatch_cancels_initial_delivery() {
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let delivery = WakeDelivery::new(
        tokio::runtime::Handle::current(),
        wake(false, None),
        move |_| {
            count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        },
        |_| {},
    );
    delivery.shutdown();
    delivery.shutdown();
    delivery.kick();
    flush().await;
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[test]
fn shutdown_during_callback_cancels_the_reentrant_followup() {
    use std::sync::mpsc;
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Mutex::new(release_rx);
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let (handle_tx, handle_rx) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let delivery = WakeDelivery::new(
            runtime.handle().clone(),
            wake(false, None),
            move |_| {
                count.fetch_add(1, Ordering::SeqCst);
                started_tx.send(()).unwrap();
                release_rx.lock().unwrap().recv().unwrap();
                Ok(())
            },
            |_| {},
        );
        handle_tx.send(delivery).unwrap();
        runtime.block_on(async {
            tokio::task::yield_now().await;
        });
    });
    let delivery = handle_rx.recv().unwrap();
    started_rx.recv().unwrap();
    delivery.publish(wake(true, None));
    delivery.shutdown();
    release_tx.send(()).unwrap();
    worker.join().unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}
