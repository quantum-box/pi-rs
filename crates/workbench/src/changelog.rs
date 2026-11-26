//! Versioned release notices for the native terminal.
use std::collections::HashSet;
use std::path::Path;

use pi_rs::Result;
use serde_json::json;

use super::packages::ExtensionPackage;

pub(super) const VERSION: &str = env!("CARGO_PKG_VERSION");
const BUNDLED: &str = include_str!("../../../CHANGELOG.md");

#[derive(Clone)]
pub(crate) enum Notice {
    Full(String),
    Condensed(String),
    ExtensionsFull(String),
    ExtensionsCondensed(Vec<String>),
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

fn saved_package_version(state: &serde_json::Value, key: &str) -> Option<Version> {
    state["packages"][key].as_str().and_then(version)
}

pub(super) fn startup_extensions(
    path: &Path,
    packages: &[ExtensionPackage],
    collapsed: bool,
) -> Result<Option<Notice>> {
    if packages.is_empty() {
        return Ok(None);
    }
    let saved = super::config::read_object(path)?;
    let mut seen = HashSet::new();
    let pending = packages
        .iter()
        .filter_map(|package| {
            let current = version(&package.version)?;
            if saved_package_version(&saved, &package.key) >= Some(current)
                || !seen.insert(&package.key)
            {
                return None;
            }
            let (name, notes) = package.changelog()?;
            Some((package, current, name, notes))
        })
        .collect::<Vec<_>>();
    if pending.is_empty() {
        return Ok(None);
    }

    let mut updates = Vec::new();
    super::config::update_object(path, |state| {
        let versions = state.entry("packages").or_insert_with(|| json!({}));
        if !versions.is_object() {
            *versions = json!({});
        }
        for (package, current, name, notes) in pending {
            let previous = versions[&package.key].as_str().and_then(version);
            if previous >= Some(current) {
                continue;
            }
            if let Some(previous) = previous {
                let mut newer = entries(&notes)
                    .filter(|(entry, _)| previous < *entry && *entry <= current)
                    .peekable();
                if newer.peek().is_none() {
                    continue;
                }
                let update = if collapsed {
                    let label = package.version.trim();
                    let label = label.strip_prefix('v').unwrap_or(label);
                    format!("Updated {name} to v{label}.")
                } else {
                    let mut escaped = String::new();
                    for ch in name.chars() {
                        if matches!(
                            ch,
                            '\\' | '*' | '_' | '[' | ']' | '`' | '<' | '>' | '&' | '~' | '!'
                        ) {
                            escaped.push('\\');
                        }
                        escaped.push(ch);
                    }
                    newer
                        .map(|(_, entry)| format!("## {escaped} {entry}"))
                        .collect::<Vec<_>>()
                        .join("\n\n")
                };
                updates.push(update);
            }
            versions[&package.key] = json!(package.version);
        }
        Ok(())
    })?;
    Ok(if updates.is_empty() {
        None
    } else if collapsed {
        Some(Notice::ExtensionsCondensed(updates))
    } else {
        Some(Notice::ExtensionsFull(updates.join("\n\n")))
    })
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
    fn extension_updates_use_installed_versions_and_independent_baselines() {
        let home = tempfile::tempdir().unwrap();
        let path = home.path().join("pi-rs-changelog.json");
        std::fs::write(&path, r#"{"version":"0.1.0"}"#).unwrap();
        let root = home.path().join("package");
        std::fs::create_dir(&root).unwrap();
        let mut package = ExtensionPackage {
            key: "extension-a".into(),
            root,
            source: "fixture".into(),
            version: "1.9.0".into(),
        };
        let changelog = package.root.join("CHANGELOG.md");
        std::fs::write(
            &changelog,
            "## Unreleased\nDraft\n## 2.0.0\nFuture\n## [1.10.0]\nNew\n## 1.9.1\nPatch\n## 1.9.0\nOld",
        )
        .unwrap();

        assert!(
            startup_extensions(&path, std::slice::from_ref(&package), false)
                .unwrap()
                .is_none()
        );
        package.version = "1.10.0-beta".into();
        let mut other = package.clone();
        other.key = "extension-b".into();

        let Some(Notice::ExtensionsFull(notes)) =
            startup_extensions(&path, &[package.clone(), package.clone(), other], false).unwrap()
        else {
            panic!("expected extension release notes");
        };
        assert_eq!(notes, "## fixture [1.10.0]\nNew\n\n## fixture 1.9.1\nPatch");
        package.version = "1.10.0".into();
        assert!(
            startup_extensions(&path, std::slice::from_ref(&package), false)
                .unwrap()
                .is_none()
        );

        package.version = "2.0.0".into();
        std::fs::remove_file(&changelog).unwrap();
        assert!(
            startup_extensions(&path, std::slice::from_ref(&package), true)
                .unwrap()
                .is_none()
        );
        std::fs::write(&changelog, "## 2.0.0\nFuture").unwrap();
        let Some(Notice::ExtensionsCondensed(lines)) =
            startup_extensions(&path, std::slice::from_ref(&package), true).unwrap()
        else {
            panic!("expected condensed extension notes");
        };
        assert_eq!(lines, ["Updated fixture to v2.0.0."]);
        package.version = "1.9.0".into();
        assert!(
            startup_extensions(&path, std::slice::from_ref(&package), false)
                .unwrap()
                .is_none()
        );
        let state = super::super::config::read_object(&path).unwrap();
        assert_eq!(
            state,
            json!({"version":"0.1.0","packages":{"extension-a":"2.0.0","extension-b":"1.10.0-beta"}}),
        );
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
