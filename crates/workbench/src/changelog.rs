//! Versioned release notices for the native terminal.
use std::path::Path;

use pi_rs::Result;
use serde_json::json;

pub(super) const VERSION: &str = env!("CARGO_PKG_VERSION");
const BUNDLED: &str = include_str!("../../../CHANGELOG.md");

#[derive(Clone)]
pub(crate) enum Notice {
    Full(String),
    Condensed(String),
}

type Version = (u64, u64, u64);

// Pi compares the numeric core, including when the saved build has a suffix.
fn version(text: &str) -> Option<Version> {
    static VERSION_CORE: std::sync::LazyLock<regex::Regex> =
        std::sync::LazyLock::new(|| regex::Regex::new(r"^v?([0-9]+)\.([0-9]+)\.([0-9]+)").unwrap());
    let captures = VERSION_CORE.captures(text.trim_start())?;
    Some((
        captures[1].parse().ok()?,
        captures[2].parse().ok()?,
        captures[3].parse().ok()?,
    ))
}

fn entries(markdown: &str) -> impl Iterator<Item = (Version, &str)> {
    let markdown = markdown.strip_prefix('\u{feff}').unwrap_or(markdown);
    let (markdown, skip_preamble) = match markdown.strip_prefix("## ") {
        Some(entries) => (entries, 0),
        None => (markdown, 1),
    };
    markdown
        .split("\n## ")
        .skip(skip_preamble)
        .filter_map(|entry| {
            let heading = entry.lines().next()?;
            Some((version(heading.trim_start_matches('['))?, entry.trim_end()))
        })
}

fn markdown<'a>(entries: impl Iterator<Item = (Version, &'a str)>) -> String {
    entries
        .map(|(_, entry)| format!("## {entry}"))
        .collect::<Vec<_>>()
        .join("\n\n")
}

pub(super) fn full() -> Notice {
    let notes = entries(BUNDLED).collect::<Vec<_>>();
    let notes = markdown(notes.into_iter().rev());
    Notice::Full(if notes.is_empty() {
        "No changelog entries found.".into()
    } else {
        notes
    })
}

pub(super) fn startup(path: &Path, collapsed: bool) -> Result<Option<Notice>> {
    let current = version(VERSION).expect("the package has a numeric version");
    let saved = super::config::read_object(path)?;
    if saved["version"].as_str().and_then(version) >= Some(current) {
        return Ok(None);
    }

    let mut notice = None;
    super::config::update_object(path, |state| {
        let previous = state
            .get("version")
            .and_then(|v| v.as_str())
            .and_then(version);
        if previous >= Some(current) {
            return Ok(());
        }
        state.insert("version".into(), json!(VERSION));
        let Some(previous) = previous else {
            return Ok(());
        };
        let mut newer = entries(BUNDLED)
            .filter(|(entry, _)| previous < *entry && *entry <= current)
            .peekable();
        let Some(&((major, minor, patch), _)) = newer.peek() else {
            return Ok(());
        };
        notice = Some(if collapsed {
            Notice::Condensed(format!("{major}.{minor}.{patch}"))
        } else {
            Notice::Full(markdown(newer))
        });
        Ok(())
    })?;
    Ok(notice)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn startup_changelog_records_baseline_and_shows_an_update_only_once() {
        let home = tempfile::tempdir().unwrap();
        let path = home.path().join("pi-rs-changelog.json");
        assert!(startup(&path, false).unwrap().is_none());
        assert_eq!(
            super::super::config::read_object(&path).unwrap()["version"],
            VERSION
        );

        for collapsed in [false, true] {
            std::fs::write(&path, r#"{"version":"v0.0.0-development.1"}"#).unwrap();
            match (collapsed, startup(&path, collapsed).unwrap().unwrap()) {
                (false, Notice::Full(notes)) => assert!(notes.contains("Added native MCP")),
                (true, Notice::Condensed(label)) => {
                    assert_eq!(
                        version(&label),
                        entries(BUNDLED).next().map(|(version, _)| version)
                    );
                }
                _ => panic!("unexpected changelog display"),
            }
            assert!(startup(&path, collapsed).unwrap().is_none());
        }
        for saved in ["invalid", "99.0.0"] {
            std::fs::write(&path, json!({"version": saved}).to_string()).unwrap();
            assert!(startup(&path, false).unwrap().is_none());
            let expected = if saved == "invalid" { VERSION } else { saved };
            assert_eq!(
                super::super::config::read_object(&path).unwrap()["version"],
                expected
            );
        }
    }

    #[test]
    fn changelog_reads_a_version_at_the_start_of_the_file() {
        for prefix in ["", "\u{feff}"] {
            let notes =
                format!("{prefix}## [1.10.0]\nFirst release\n\n## 1.9.0\nEarlier release\n");
            let rendered = markdown(entries(&notes));
            assert_eq!(
                rendered,
                "## [1.10.0]\nFirst release\n\n## 1.9.0\nEarlier release"
            );
        }
    }

    #[test]
    fn changelog_selects_numbered_entries_in_file_order() {
        let notes = "# History\n\n## [1.10.0] announcement\nNew\n### Features\nDetails\n\n## Unreleased\nDraft\n\n## 1.9.0\nOld\n";
        assert_eq!(
            markdown(entries(notes).filter(|(entry, _)| *entry > version("1.9.0-beta").unwrap())),
            "## [1.10.0] announcement\nNew\n### Features\nDetails"
        );
    }
}
