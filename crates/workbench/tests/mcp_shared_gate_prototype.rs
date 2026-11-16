//! Checks for the unconnected experimental per-account gate.
//! Windows tests use a random name, never the user's fixed production mutex.
#![cfg(any(target_os = "linux", target_os = "macos", windows))]
#![allow(dead_code)]

#[path = "../src/mcp_shared_gate.rs"]
mod mcp_shared_gate;

#[cfg(windows)]
#[tokio::test]
async fn mutex_serializes_tasks_and_cancelled_waiter_exits_while_first_is_held() {
    use mcp_shared_gate::windows_mutex::{WorkerHooks, acquire_named_for_test};
    use std::time::Duration;
    let name = format!("Global\\pi-rs-mcp-test-{}", uuid::Uuid::new_v4());
    let first = tokio::time::timeout(
        Duration::from_secs(10),
        acquire_named_for_test(name.clone(), None),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        !first.abandoned,
        "recovery is not implemented in this prototype"
    );
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (exited_tx, exited_rx) = tokio::sync::oneshot::channel();
    let waiter_name = name.clone();
    let waiting = tokio::spawn(async move {
        acquire_named_for_test(
            waiter_name,
            Some(WorkerHooks {
                wait_started: started_tx,
                exited: exited_tx,
            }),
        )
        .await
    });
    // The hook fires only after WaitForSingleObject returns WAIT_TIMEOUT,
    // establishing that the worker actually waited while first was held.
    tokio::time::timeout(Duration::from_secs(10), started_rx)
        .await
        .unwrap()
        .unwrap();
    assert!(!waiting.is_finished());
    waiting.abort();
    assert!(matches!(waiting.await, Err(error) if error.is_cancelled()));
    // Join notification proves the worker exited, with first still held.
    assert!(
        tokio::time::timeout(Duration::from_secs(10), exited_rx)
            .await
            .unwrap()
            .unwrap()
    );
    drop(first);
    let next = tokio::time::timeout(Duration::from_secs(10), acquire_named_for_test(name, None))
        .await
        .unwrap()
        .unwrap();
    assert!(!next.abandoned, "unexpected abandoned mutex");
}

// This exact test is also invoked as a child by the process test. The child
// never resolves the account profile and only uses the injected random name.
#[cfg(windows)]
#[tokio::test]
async fn windows_process_gate_child() {
    use mcp_shared_gate::windows_mutex::{WorkerHooks, acquire_named_for_test};
    use std::{path::PathBuf, time::Duration};
    let Ok(role) = std::env::var("PI_RS_TEST_GATE_ROLE") else {
        return;
    };
    let root = PathBuf::from(std::env::var_os("PI_RS_TEST_GATE_ROOT").unwrap());
    let name = std::env::var("PI_RS_TEST_GATE_NAME").unwrap();
    assert!(name.starts_with("Global\\pi-rs-mcp-process-test-"));
    let expected_home = root.join(format!("home-{role}"));
    assert_eq!(
        std::env::var_os("PI_RS_HOME"),
        Some(expected_home.clone().into_os_string())
    );
    assert_eq!(
        std::env::var_os("USERPROFILE"),
        Some(expected_home.into_os_string())
    );
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (exited_tx, exited_rx) = tokio::sync::oneshot::channel();
    let hooks = WorkerHooks {
        wait_started: started_tx,
        exited: exited_tx,
    };
    if role == "holder" {
        let guard = tokio::time::timeout(
            Duration::from_secs(20),
            acquire_named_for_test(name, Some(hooks)),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(!guard.abandoned);
        std::fs::write(root.join("holder-ready"), b"held").unwrap();
        tokio::time::timeout(Duration::from_secs(20), async {
            while !root.join("release-holder").exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("parent did not release holder");
        drop(guard);
        assert!(
            tokio::time::timeout(Duration::from_secs(10), exited_rx)
                .await
                .unwrap()
                .unwrap()
        );
        std::fs::write(root.join("holder-exited"), b"done").unwrap();
    } else if role == "waiter" {
        let task = tokio::spawn(async move { acquire_named_for_test(name, Some(hooks)).await });
        // Hook is emitted after an actual WAIT_TIMEOUT in the worker thread.
        tokio::time::timeout(Duration::from_secs(20), started_rx)
            .await
            .unwrap()
            .unwrap();
        std::fs::write(root.join("waiter-ready"), b"blocked").unwrap();
        let guard = tokio::time::timeout(Duration::from_secs(20), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(!guard.abandoned);
        std::fs::write(root.join("waiter-acquired"), b"held").unwrap();
        drop(guard);
        assert!(
            tokio::time::timeout(Duration::from_secs(10), exited_rx)
                .await
                .unwrap()
                .unwrap()
        );
        std::fs::write(root.join("waiter-exited"), b"done").unwrap();
    } else {
        panic!("invalid child role");
    }
}

#[cfg(windows)]
#[tokio::test]
async fn mutex_is_shared_across_processes_with_different_homes() {
    use std::{
        path::{Path, PathBuf},
        process::{Child, Command, Stdio},
        time::Duration,
    };
    // Drop always terminates and reaps children, including on test panic/timeout.
    struct ChildGuard(Child);
    impl Drop for ChildGuard {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    fn spawn_child(root: &Path, name: &str, role: &str) -> ChildGuard {
        let home = root.join(format!("home-{role}"));
        std::fs::create_dir_all(&home).unwrap();
        let log = std::fs::File::create(root.join(format!("{role}.log"))).unwrap();
        let error_log = log.try_clone().unwrap();
        let child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "windows_process_gate_child", "--nocapture"])
            .env("PI_RS_TEST_GATE_ROLE", role)
            .env("PI_RS_TEST_GATE_ROOT", root)
            .env("PI_RS_TEST_GATE_NAME", name)
            .env("PI_RS_HOME", &home)
            .env("PI_CODING_AGENT_DIR", &home)
            .env("HOME", &home)
            .env("USERPROFILE", &home)
            .env("APPDATA", &home)
            .env("LOCALAPPDATA", &home)
            .env("TEMP", &home)
            .env("TMP", &home)
            .stdin(Stdio::null())
            .stdout(Stdio::from(log))
            .stderr(Stdio::from(error_log))
            .spawn()
            .unwrap();
        ChildGuard(child)
    }
    async fn wait_marker(root: &Path, marker: &str, child: &mut ChildGuard) {
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                if root.join(marker).exists() {
                    return;
                }
                if let Some(status) = child.0.try_wait().unwrap() {
                    panic!("child exited before {marker}: {status}");
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {marker}"));
    }
    async fn wait_success(child: &mut ChildGuard) {
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                if let Some(status) = child.0.try_wait().unwrap() {
                    assert!(status.success(), "child failed: {status}");
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("child did not exit");
    }
    let temp = tempfile::tempdir().unwrap();
    let root: PathBuf = temp.path().to_owned();
    let name = format!("Global\\pi-rs-mcp-process-test-{}", uuid::Uuid::new_v4());
    let mut holder = spawn_child(&root, &name, "holder");
    wait_marker(&root, "holder-ready", &mut holder).await;
    let mut waiter = spawn_child(&root, &name, "waiter");
    wait_marker(&root, "waiter-ready", &mut waiter).await;
    assert!(holder.0.try_wait().unwrap().is_none());
    assert!(waiter.0.try_wait().unwrap().is_none());
    assert!(
        !root.join("waiter-acquired").exists(),
        "waiter acquired while holder held lock"
    );
    std::fs::write(root.join("release-holder"), b"release").unwrap();
    wait_marker(&root, "waiter-acquired", &mut waiter).await;
    wait_marker(&root, "waiter-exited", &mut waiter).await;
    wait_marker(&root, "holder-exited", &mut holder).await;
    wait_success(&mut waiter).await;
    wait_success(&mut holder).await;
}
