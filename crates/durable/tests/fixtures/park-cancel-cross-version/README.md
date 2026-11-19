# Parked cancel cross-version stores

These SQLite snapshots were produced by actual unit-test binaries built offline
from these commits, each in an independent detached worktree:

- `a587016388626df5df65294f6d6e09bdf3e41411`: discriminator 8, park checkpoint v1.
- `7f5fd458b2d65b2c1eb06d2055a15cd848ef4090`: discriminator 8, park checkpoint v2.

The production code in each worktree was unchanged. A test-only helper (below)
was appended to its existing `async_task_tool::tests` module. It uses that
revision's own `park_cancel_host` and `wait_park_cancel` fixtures. The helper
was run with `PI_CROSS_VERSION_FIXTURE` set to a temporary directory. For
`parked`, the process was killed after `parked`; for `claimed`, it also had
`PI_CROSS_VERSION_ABORT=1` and `PI_TEST_PARK_CANCEL_AFTER_CLAIM` set and was
killed at `cancel-claimed`; for `receipted`, it had
`PI_CROSS_VERSION_ABORT=1` and `PI_TEST_ABORT_AFTER_RECEIPT` set and was killed
at `abort-receipt`. The environment values for the two pause hooks were the
same temporary directory. Each killed process was followed by SQLite's online
backup API to capture its database and WAL consistently into the checked-in
`.sqlite3` file. The `.task-id` files identify the root task in each snapshot.
No legacy callback or production code was replaced by the helper.

```rust
#[test]
fn actual_old_park_store_fixture() {
    let Ok(root) = std::env::var("PI_CROSS_VERSION_FIXTURE") else { return; };
    let root = std::path::PathBuf::from(root);
    let (host, tool) = park_cancel_host(&root);
    let id = host.start_replay_safe_root(&tool, json!({"token":"prepared"}), 3).unwrap();
    std::fs::write(root.join("task-id"), id.to_string()).unwrap();
    let driver = host.into_driver();
    wait_park_cancel(&driver, id);
    std::fs::write(root.join("parked"), b"yes").unwrap();
    if std::env::var_os("PI_CROSS_VERSION_ABORT").is_some() {
        driver.request_abort(id).unwrap();
    }
    loop { std::hint::black_box(&driver); std::thread::park(); }
}
```

The cross-version test copies each snapshot before opening it. These stores
contain only synthetic test inputs and randomly generated task identifiers.
