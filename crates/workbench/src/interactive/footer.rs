//! Read-only data projected into native footer renders.
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};

use pi_rs::{CancellationToken, FooterRenderData};
use tokio::{io::AsyncReadExt, sync::watch};

use super::Cli;

#[derive(Default)]
pub(super) struct FooterData {
    branch: Option<BranchWatcher>,
    data: FooterRenderData,
    pub(super) catalog_provider_count: Option<usize>,
}

impl FooterData {
    pub(super) fn stop_branch_probe(&mut self) {
        self.branch = None;
        self.data.git_branch = None;
    }

    pub(super) fn refresh_branch(&mut self, workspace: &Path) {
        if self
            .branch
            .as_ref()
            .is_none_or(|watcher| watcher.workspace != workspace)
        {
            self.stop_branch_probe();
            self.branch = Some(BranchWatcher::start(workspace.to_owned()));
        }
        let watcher = self.branch.as_mut().expect("branch watcher started");
        if matches!(watcher.branch.has_changed(), Ok(true)) {
            self.data
                .git_branch
                .clone_from(&watcher.branch.borrow_and_update());
        }
    }

    pub(super) fn update_statuses(&mut self, statuses: &BTreeMap<String, String>) {
        self.data.extension_statuses = statuses
            .iter()
            .map(|(key, text)| {
                // Preserve each status entry while bounding the total wire payload.
                let mut end = text.len().min(1024);
                while !text.is_char_boundary(end) {
                    end -= 1;
                }
                (key.clone(), text[..end].to_owned())
            })
            .collect();
    }

    pub(super) fn current(&self) -> &FooterRenderData {
        &self.data
    }

    pub(super) fn snapshot(&mut self, args: &Cli) -> &FooterRenderData {
        use crate::model_selection::{available_provider_count, cached_catalogs};
        self.data.available_provider_count = if args.model_scope.is_empty() {
            *self
                .catalog_provider_count
                .get_or_insert_with(|| available_provider_count(&cached_catalogs(args)))
        } else {
            args.model_scope
                .iter()
                .map(|choice| choice.provider.id())
                .collect::<BTreeSet<_>>()
                .len()
        };
        &self.data
    }
}

struct BranchWatcher {
    workspace: PathBuf,
    branch: watch::Receiver<Option<String>>,
    cancellation: CancellationToken,
}

impl BranchWatcher {
    fn start(workspace: PathBuf) -> Self {
        let (sender, branch) = watch::channel(None);
        let cancellation = CancellationToken::new();
        let token = cancellation.clone();
        let cwd = workspace.clone();
        // Workbench's terminal polls input synchronously on a single-thread runtime.
        // Run the Git timer and child process on a separate thread even while it is idle.
        std::thread::spawn(move || {
            let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            else {
                return;
            };
            runtime.block_on(async move {
                loop {
                    let resolved = tokio::select! {
                        biased;
                        _ = token.cancelled() => break,
                        branch = read_branch(cwd.clone()) => branch,
                    };
                    sender.send_if_modified(|current| {
                        if *current == resolved {
                            return false;
                        }
                        *current = resolved;
                        true
                    });
                    tokio::select! {
                        biased;
                        _ = token.cancelled() => break,
                        _ = tokio::time::sleep(Duration::from_millis(500)) => {}
                    }
                }
            });
        });
        Self {
            workspace,
            branch,
            cancellation,
        }
    }
}

impl Drop for BranchWatcher {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}

async fn read_branch(workspace: PathBuf) -> Option<String> {
    tokio::time::timeout(Duration::from_millis(500), async move {
        let mut child = tokio::process::Command::new("git")
            .args(["symbolic-ref", "--quiet", "HEAD"])
            // Use the workspace's repository even if the caller exported Git overrides.
            .env_remove("GIT_DIR")
            .env_remove("GIT_WORK_TREE")
            .env_remove("GIT_COMMON_DIR")
            .current_dir(workspace)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .ok()?;
        let mut output = Vec::new();
        child
            .stdout
            .take()?
            .take(4097)
            .read_to_end(&mut output)
            .await
            .ok()?;
        if output.len() > 4096 {
            return None;
        }
        match child.wait().await.ok()?.code()? {
            0 => {
                let reference = std::str::from_utf8(&output)
                    .ok()?
                    .trim_end_matches(['\r', '\n']);
                Some(
                    reference
                        .strip_prefix("refs/heads/")
                        .unwrap_or(reference)
                        .to_owned(),
                )
            }
            1 => Some("detached".into()),
            _ => None,
        }
    })
    .await
    .ok()
    .flatten()
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn dynamic_footer_counts_distinct_scoped_providers_and_bounds_utf8_statuses() {
        let mut args = Cli::parse_from(["pi"]);
        let mut footer = FooterData {
            catalog_provider_count: Some(8),
            ..FooterData::default()
        };
        assert_eq!(footer.snapshot(&args).available_provider_count, 8);
        args.model_scope = [
            (crate::Provider::Ollama, "one"),
            (crate::Provider::Ollama, "two"),
            (crate::Provider::Anthropic, "three"),
        ]
        .into_iter()
        .map(|(provider, model)| crate::model_selection::ScopedChoice {
            provider,
            model: model.into(),
            thinking: None,
        })
        .collect();
        let text = "あ".repeat(1024);
        footer.update_statuses(&BTreeMap::from([("example".into(), text)]));
        let snapshot = footer.snapshot(&args);
        assert_eq!(snapshot.available_provider_count, 2);
        assert_eq!(snapshot.extension_statuses["example"], "あ".repeat(341));
    }

    #[tokio::test]
    async fn dynamic_footer_reads_branch_and_detached_linked_worktree() {
        let repository = tempfile::tempdir().unwrap();
        let worktrees = tempfile::tempdir().unwrap();
        let worktree = worktrees.path().join("linked");
        let git = |arguments: &[&str]| {
            let output = std::process::Command::new("git")
                .current_dir(repository.path())
                .args(arguments)
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
        };
        assert_eq!(read_branch(repository.path().into()).await, None);
        git(&["init", "-q"]);
        git(&["symbolic-ref", "HEAD", "refs/heads/project/one"]);
        assert_eq!(
            read_branch(repository.path().into()).await.as_deref(),
            Some("project/one")
        );
        let mut footer = FooterData::default();
        let wait_for_branch = |footer: &mut FooterData, expected| {
            let deadline = std::time::Instant::now() + Duration::from_secs(3);
            loop {
                footer.refresh_branch(repository.path());
                if footer.data.git_branch.as_deref() == Some(expected) {
                    break;
                }
                assert!(std::time::Instant::now() < deadline);
                // Match the terminal's synchronous polling while no async work is awaited.
                std::thread::sleep(Duration::from_millis(20));
            }
        };
        wait_for_branch(&mut footer, "project/one");
        git(&["symbolic-ref", "HEAD", "refs/heads/project/two"]);
        wait_for_branch(&mut footer, "project/two");
        let cancellation = footer.branch.as_ref().unwrap().cancellation.clone();
        footer.stop_branch_probe();
        assert!(cancellation.is_cancelled());

        git(&[
            "-c",
            "user.name=Fixture",
            "-c",
            "user.email=fixture@example.test",
            "commit",
            "--allow-empty",
            "--no-gpg-sign",
            "-qm",
            "fixture",
        ]);
        git(&[
            "worktree",
            "add",
            "--detach",
            worktree.to_str().unwrap(),
            "HEAD",
        ]);
        assert_eq!(read_branch(worktree).await.as_deref(), Some("detached"));
    }
}
