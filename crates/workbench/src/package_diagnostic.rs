//! Read-only candidate discovery for unsupported Pi package resources.
//! These paths are used only to evaluate filters, never loaded or executed.
use std::collections::HashSet;
use std::path::{Path, PathBuf};

use pi_rs::Result;
use serde_json::Value;

use super::{ResourceFilter, parse_manifest_object, read_optional_manifest};

const MAX_ENTRIES: usize = 10_000;
const MAX_DEPTH: usize = 64;
const MAX_IGNORE_BYTES: usize = 4 * 1024 * 1024;
const MAX_EXTGLOB_PATTERN: usize = 1024;
const MAX_EXTGLOB_DEPTH: usize = 16;
const MAX_EXTGLOB_INPUT: usize = 16 * 1024;

// This deliberately supports a bounded subset. In particular negated extglobs,
// classes, braces and globstars require Node-specific semantics; falling back
// is preferable to reporting a partial or incorrectly empty candidate set.
fn extglob_regex(pattern: &str) -> Option<Option<regex::Regex>> {
    let bytes = pattern.as_bytes();
    if !has_extglob(pattern) {
        return Some(None);
    }
    if bytes.len() > MAX_EXTGLOB_PATTERN
        || !pattern.is_ascii()
        || pattern.contains(['[', ']', '{', '}'])
        || pattern.contains("**")
    {
        return None;
    }
    fn sequence(bytes: &[u8], pos: &mut usize, depth: usize) -> Option<String> {
        if depth > MAX_EXTGLOB_DEPTH {
            return None;
        }
        let mut out = String::new();
        while *pos < bytes.len() {
            let ch = bytes[*pos];
            if depth > 0 && matches!(ch, b'|' | b')') {
                break;
            }
            if matches!(ch, b'@' | b'?' | b'*' | b'+' | b'!') && bytes.get(*pos + 1) == Some(&b'(')
            {
                if ch == b'!' {
                    return None;
                }
                *pos += 2;
                let mut alternatives = Vec::new();
                loop {
                    alternatives.push(sequence(bytes, pos, depth + 1)?);
                    match bytes.get(*pos) {
                        Some(b'|') => *pos += 1,
                        Some(b')') => {
                            *pos += 1;
                            break;
                        }
                        _ => return None,
                    }
                }
                let quantifier = match ch {
                    b'@' => "",
                    b'?' => "?",
                    b'*' => "*",
                    b'+' => "+",
                    _ => return None,
                };
                out.push_str(&format!("(?:{}){quantifier}", alternatives.join("|")));
            } else {
                // Bare parentheses inside an extglob are parsed differently by
                // Node's matcher; do not infer a partial interpretation.
                if depth > 0 && ch == b'(' {
                    return None;
                }
                *pos += 1;
                match ch {
                    b'*' => out.push_str("[^/]*"),
                    b'?' => out.push_str("[^/]"),
                    b'/' if depth > 0 => return None,
                    _ => out.push_str(&regex::escape(&(ch as char).to_string())),
                }
            }
        }
        Some(out)
    }
    let mut pos = 0;
    let expression = sequence(bytes, &mut pos, 0)?;
    if pos != bytes.len() {
        return None;
    }
    let regex = regex::RegexBuilder::new(&format!("\\A{expression}\\z"))
        .size_limit(1024 * 1024)
        .build()
        .ok()?;
    Some(Some(regex))
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Kind {
    Extension,
    Skill,
    Prompt,
}
impl Kind {
    fn name(self) -> &'static str {
        match self {
            Self::Extension => "extensions",
            Self::Skill => "skills",
            Self::Prompt => "prompts",
        }
    }
}

// None means that the bounded scan could not establish the candidate set.
// The caller retains its existing native behavior in that case.
pub(super) fn candidates(root: &Path, kind: Kind) -> Result<Option<Vec<PathBuf>>> {
    let Some(bytes) = read_optional_manifest(root, "package.json")? else {
        return Ok(None);
    };
    let value = parse_manifest_object(&root.join("package.json"), &bytes, true)?;
    let entries = value
        .get("pi")
        .and_then(|pi| pi.get(kind.name()))
        .and_then(Value::as_array)
        .map(|entries| {
            entries
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let entries = if entries.is_empty() {
        vec![kind.name().to_owned()]
    } else {
        entries
    };
    let mut scanner = Scanner {
        count: 0,
        ignore_bytes: 0,
        active: HashSet::new(),
    };
    let mut paths = Vec::new();
    for entry in entries
        .iter()
        .filter(|entry| !entry.starts_with(['!', '+', '-']))
    {
        let sources = if entry.contains(['*', '?']) {
            let Some(matches) = scanner.glob(root, entry) else {
                return Ok(None);
            };
            matches
        } else {
            let joined = root.join(entry);
            vec![super::normalize_filter_home(
                Path::new(""),
                &joined.to_string_lossy(),
            )]
        };
        for source in sources {
            if !scanner.source(&source, kind, &mut paths) {
                return Ok(None);
            }
        }
    }
    let overrides: Vec<_> = entries
        .into_iter()
        .filter(|entry| entry.starts_with(['!', '+', '-']))
        .collect();
    if !overrides.is_empty() {
        // The shared override matcher does not implement extglobs. Exact +
        // and - overrides are literal paths, but ! is a pattern in Pi.
        if overrides
            .iter()
            .filter_map(|entry| entry.strip_prefix('!'))
            .any(has_extglob)
        {
            return Ok(None);
        }
        // Pi evaluates overrides against candidates rather than eagerly
        // validating patterns when the candidate set is empty.
        let filter = ResourceFilter {
            patterns: Some(overrides),
            autoload: true,
        };
        let enabled = filter.apply(&paths, root, &vec![false; paths.len()])?;
        paths = paths
            .into_iter()
            .zip(enabled)
            .filter_map(|(path, enabled)| enabled.then_some(path))
            .collect();
    }
    let mut seen = HashSet::new();
    paths.retain(|path| seen.insert(path.clone()));
    Ok(Some(paths))
}

fn has_extglob(pattern: &str) -> bool {
    pattern
        .as_bytes()
        .windows(2)
        .any(|w| matches!(w[0], b'@' | b'?' | b'*' | b'+' | b'!') && w[1] == b'(')
}

struct Scanner {
    count: usize,
    ignore_bytes: usize,
    active: HashSet<PathBuf>,
}
impl Scanner {
    fn tick(&mut self) -> bool {
        self.count += 1;
        self.count <= MAX_ENTRIES
    }
    fn glob(&mut self, root: &Path, pattern: &str) -> Option<Vec<PathBuf>> {
        // Keep discovery within the package and avoid walking through symlinked trees.
        let pattern = pattern.replace('\\', "/");
        let path = Path::new(&pattern);
        if path.is_absolute()
            || path
                .components()
                .any(|component| matches!(component, std::path::Component::ParentDir))
        {
            return None;
        }
        let directory_only = pattern.ends_with('/') || pattern.rsplit('/').next() == Some(".");
        let normalized = pattern
            .split('/')
            .filter(|part| !part.is_empty() && *part != ".")
            .collect::<Vec<_>>()
            .join("/");
        let mut prefix = root.to_path_buf();
        for component in normalized.split('/') {
            if component.contains(['*', '?', '[', '{']) || has_extglob(component) {
                break;
            }
            prefix.push(component);
            if std::fs::symlink_metadata(&prefix)
                .is_ok_and(|metadata| metadata.file_type().is_symlink())
            {
                return None;
            }
        }
        let extglob = extglob_regex(&normalized)?;
        let matcher = if extglob.is_none() {
            Some(
                globset::GlobBuilder::new(&normalized)
                    .literal_separator(true)
                    .backslash_escape(false)
                    .build()
                    .ok()?
                    .compile_matcher(),
            )
        } else {
            None
        };
        let mut matches = Vec::new();
        let mut matched_bytes = 0usize;
        let mut symlink_checks = 0usize;
        for entry in ignore::WalkBuilder::new(root)
            .hidden(true)
            .parents(false)
            .ignore(false)
            .git_ignore(false)
            .git_global(false)
            .git_exclude(false)
            .follow_links(false)
            .max_depth(Some(MAX_DEPTH))
            .build()
        {
            let entry = entry.ok()?;
            if !self.tick() {
                return None;
            }
            if entry.depth() == MAX_DEPTH && entry.file_type().is_some_and(|kind| kind.is_dir()) {
                return None;
            }
            // A fixed component after a wildcard may resolve to a symlinked
            // directory in Node's glob walker. We do not follow such trees;
            // avoid treating an incomplete scan as an empty result.
            if normalized.contains('/')
                && entry.file_type().is_some_and(|kind| kind.is_symlink())
                && entry.path().is_dir()
                && (normalized.contains("**") || entry.depth() < normalized.split('/').count())
            {
                symlink_checks += 1;
                if symlink_checks > MAX_DEPTH {
                    return None;
                }
                let prefix = normalized
                    .split('/')
                    .take(entry.depth())
                    .collect::<Vec<_>>()
                    .join("/");
                let relative = entry.path().strip_prefix(root).ok()?;
                let reachable = if normalized.contains("**") {
                    true
                } else if let Some(regex) = extglob_regex(&prefix)? {
                    let input = relative.to_str()?;
                    if !input.is_ascii() {
                        return None;
                    }
                    regex.is_match(&input.replace('\\', "/"))
                } else {
                    globset::GlobBuilder::new(&prefix)
                        .literal_separator(true)
                        .backslash_escape(false)
                        .build()
                        .ok()?
                        .compile_matcher()
                        .is_match(relative)
                };
                if reachable {
                    return None;
                }
            }
            let relative = entry.path().strip_prefix(root).ok()?;
            let matched = if let Some(regex) = &extglob {
                let input = relative.to_str()?;
                // Node matches UTF-16 code units, while Rust regex matches Unicode
                // scalars; use the safe fallback for non-ASCII paths.
                if !input.is_ascii() || input.len() > MAX_EXTGLOB_INPUT {
                    return None;
                }
                matched_bytes = matched_bytes.checked_add(input.len())?;
                if matched_bytes > MAX_IGNORE_BYTES {
                    return None;
                }
                regex.is_match(&input.replace('\\', "/"))
            } else {
                matcher.as_ref()?.is_match(relative)
            };
            if matched && (!directory_only || entry.file_type().is_some_and(|kind| kind.is_dir())) {
                matches.push(entry.into_path());
            }
        }
        matches.sort();
        Some(matches)
    }
    fn source(&mut self, source: &Path, kind: Kind, output: &mut Vec<PathBuf>) -> bool {
        if !self.tick() {
            return false;
        }
        let Ok(metadata) = std::fs::metadata(source) else {
            return true;
        };
        if metadata.is_file() {
            output.push(source.to_path_buf());
            return true;
        }
        if !metadata.is_dir() {
            return true;
        }
        self.directory(source, source, kind, &[], 0, output)
    }
    fn directory(
        &mut self,
        dir: &Path,
        root: &Path,
        kind: Kind,
        inherited: &[String],
        depth: usize,
        output: &mut Vec<PathBuf>,
    ) -> bool {
        if depth >= MAX_DEPTH || !self.tick() {
            return false;
        }
        let Ok(canonical) = dir.canonicalize() else {
            return true;
        };
        if !self.active.insert(canonical.clone()) {
            return false;
        }
        let result = self.directory_inner(dir, root, kind, inherited, depth, output);
        self.active.remove(&canonical);
        result
    }
    fn directory_inner(
        &mut self,
        dir: &Path,
        root: &Path,
        kind: Kind,
        inherited: &[String],
        depth: usize,
        output: &mut Vec<PathBuf>,
    ) -> bool {
        if kind == Kind::Extension {
            match extension_entry(dir, &mut self.count) {
                None => return false,
                Some(Some(paths)) => {
                    output.extend(paths);
                    return true;
                }
                Some(None) => {}
            }
        }
        let mut rules = inherited.to_vec();
        let prefix = dir
            .strip_prefix(root)
            .unwrap_or(Path::new(""))
            .to_string_lossy()
            .replace('\\', "/");
        for name in [".gitignore", ".ignore", ".fdignore"] {
            match read_optional_manifest(dir, name) {
                Ok(Some(bytes)) => {
                    self.ignore_bytes = self.ignore_bytes.saturating_add(bytes.len());
                    if self.ignore_bytes > MAX_IGNORE_BYTES {
                        return false;
                    }
                    let Ok(text) = std::str::from_utf8(&bytes) else {
                        return false;
                    };
                    for line in text.lines() {
                        if let Some(rule) = prefix_ignore(line, &prefix) {
                            rules.push(rule);
                            if rules.len() > MAX_ENTRIES {
                                return false;
                            }
                        }
                    }
                }
                Ok(None) => {}
                Err(_) => return false,
            }
        }
        let mut builder = ignore::gitignore::GitignoreBuilder::new(root);
        for rule in &rules {
            if builder.add_line(None, rule).is_err() {
                return false;
            }
        }
        let Ok(matcher) = builder.build() else {
            return false;
        };
        let Ok(entries) = std::fs::read_dir(dir) else {
            return true;
        };
        let mut collected = Vec::new();
        for entry in entries {
            if !self.tick() {
                return false;
            }
            if let Ok(entry) = entry {
                collected.push(entry);
            }
        }
        let mut entries = collected;
        if kind == Kind::Skill
            && let Some(entry) = entries.iter().find(|entry| entry.file_name() == "SKILL.md")
        {
            let path = entry.path();
            if path.is_file()
                && !matcher
                    .matched_path_or_any_parents(&path, false)
                    .is_ignore()
            {
                output.push(path);
                return self.tick();
            }
        }
        for entry in entries.drain(..) {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.starts_with('.') || name == "node_modules" {
                continue;
            }
            let path = entry.path();
            let Ok(metadata) = std::fs::metadata(&path) else {
                continue;
            };
            let is_dir = metadata.is_dir();
            if matcher
                .matched_path_or_any_parents(&path, is_dir)
                .is_ignore()
            {
                continue;
            }
            if is_dir {
                if kind == Kind::Extension {
                    match extension_entry(&path, &mut self.count) {
                        None => return false,
                        Some(Some(paths)) => {
                            output.extend(paths);
                        }
                        Some(None) => {}
                    }
                } else if !self.directory(&path, root, kind, &rules, depth + 1, output) {
                    return false;
                }
            } else if metadata.is_file()
                && match kind {
                    Kind::Extension => name.ends_with(".ts") || name.ends_with(".js"),
                    Kind::Skill => depth == 0 && name.ends_with(".md"),
                    Kind::Prompt => name.ends_with(".md"),
                }
            {
                output.push(path);
            }
        }
        true
    }
}

fn prefix_ignore(line: &str, prefix: &str) -> Option<String> {
    let trimmed = line.trim();
    if trimmed.is_empty() || trimmed.starts_with('#') {
        return None;
    }
    let (negated, line) = if let Some(line) = line.strip_prefix('!') {
        (true, line)
    } else if let Some(line) = line.strip_prefix(r"\!") {
        (false, line)
    } else {
        (false, line)
    };
    let line = line.strip_prefix('/').unwrap_or(line);
    let path = if prefix.is_empty() {
        line.to_owned()
    } else {
        format!("{prefix}/{line}")
    };
    Some(if negated { format!("!{path}") } else { path })
}

// Outer None means a special or unreadable package manifest prevented a safe scan.
fn extension_entry(dir: &Path, count: &mut usize) -> Option<Option<Vec<PathBuf>>> {
    let bytes = read_optional_manifest(dir, "package.json").ok()?;
    if let Some(bytes) = bytes
        && let Ok(value) = parse_manifest_object(&dir.join("package.json"), &bytes, true)
        && let Some(entries) = value
            .get("pi")
            .and_then(|pi| pi.get("extensions"))
            .and_then(Value::as_array)
    {
        let mut paths = Vec::new();
        for entry in entries {
            *count += 1;
            if *count > MAX_ENTRIES {
                return None;
            }
            if let Some(entry) = entry.as_str() {
                let path = super::normalize_filter_path(&dir.join(entry));
                if path.exists() {
                    paths.push(path);
                }
            }
        }
        if !paths.is_empty() {
            return Some(Some(paths));
        }
    }
    for name in ["index.ts", "index.js"] {
        *count += 1;
        if *count > MAX_ENTRIES {
            return None;
        }
        let path = dir.join(name);
        if path.exists() {
            return Some(Some(vec![path]));
        }
    }
    Some(None)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extglob_matches_node_when_available() {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir(root.path().join("p")).unwrap();
        for name in [
            "a.md", "b.md", "ab.md", "aba.md", "c.md", "a.txt", "x.md.txt",
        ] {
            std::fs::write(root.path().join("p").join(name), "fixture").unwrap();
        }
        let patterns = [
            "p/*.@(md|txt)",
            "p/?(a|b)*.md",
            "p/*(a|b)*.md",
            "p/+(a|b)*.md",
            "p/@(a|@(b|c))*.md",
            "p/*.+(md|txt)",
            "p/*.?(md|txt)",
            "p/*.*(md|txt)",
            "p/@(a|b)*.@(md|txt)",
            "p/@(a*|b)*.md",
            "p/@(a|+(b|c))*.md",
            "p/*/@(a|b)*.md",
            "@(p|q)/*.@(md|txt)",
            "p/*.@(md|)",
            "p/@(a|b)?(b|c)*.md",
        ];
        let output = std::process::Command::new("node")
            .args(["-e", "const f=require('fs'); if(!f.globSync)process.exit(77); const p=JSON.parse(process.argv[1]); process.stdout.write(JSON.stringify(p.map(x=>f.globSync(x,{cwd:process.cwd()}).sort())))", &serde_json::to_string(&patterns).unwrap()])
            .current_dir(root.path())
            .output();
        let Ok(output) = output else { return };
        if output.status.code() == Some(77) {
            return;
        }
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let expected: Vec<Vec<String>> = serde_json::from_slice(&output.stdout).unwrap();
        for (pattern, expected) in patterns.into_iter().zip(expected) {
            let mut scanner = Scanner {
                count: 0,
                ignore_bytes: 0,
                active: HashSet::new(),
            };
            let actual = scanner.glob(root.path(), pattern).unwrap();
            let expected: Vec<_> = expected
                .into_iter()
                .map(|path| root.path().join(path))
                .collect();
            assert_eq!(actual, expected, "{pattern}");
        }
    }

    #[test]
    #[cfg(unix)]
    fn extglob_symlink_prefix_falls_back() {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir(root.path().join("real")).unwrap();
        std::fs::write(root.path().join("real/a.md"), "prompt").unwrap();
        std::os::unix::fs::symlink(root.path().join("real"), root.path().join("link")).unwrap();
        std::fs::write(
            root.path().join("package.json"),
            serde_json::json!({"pi":{"prompts":["link/*.@(md|txt)"]}}).to_string(),
        )
        .unwrap();
        assert!(candidates(root.path(), Kind::Prompt).unwrap().is_none());
        std::fs::create_dir(root.path().join("parent")).unwrap();
        std::os::unix::fs::symlink(root.path().join("real"), root.path().join("parent/link"))
            .unwrap();
        std::fs::write(
            root.path().join("package.json"),
            serde_json::json!({"pi":{"prompts":["*/link/*.@(md|txt)"]}}).to_string(),
        )
        .unwrap();
        assert!(candidates(root.path(), Kind::Prompt).unwrap().is_none());
    }

    #[cfg(unix)]
    #[test]
    fn unrelated_symlink_does_not_hide_extglob_candidates() {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir(root.path().join("prompts")).unwrap();
        std::fs::write(root.path().join("prompts/a.md"), "prompt").unwrap();
        std::os::unix::fs::symlink(root.path().join("prompts"), root.path().join("unrelated"))
            .unwrap();
        std::fs::write(
            root.path().join("package.json"),
            serde_json::json!({"pi":{"prompts":["prompts/*.@(md|txt)"]}}).to_string(),
        )
        .unwrap();
        assert_eq!(
            candidates(root.path(), Kind::Prompt).unwrap().unwrap(),
            vec![root.path().join("prompts/a.md")]
        );
    }

    #[test]
    fn extglob_node_fixtures_and_manifest_dispatch() {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir(root.path().join("p")).unwrap();
        for name in [
            "a.md",
            "b.md",
            "ab.md",
            "aba.md",
            "c.md",
            "a.txt",
            ".hidden.md",
        ] {
            std::fs::write(root.path().join("p").join(name), "fixture").unwrap();
        }
        // Expected sets from Node globSync, followed by Pi's hidden-path filter.
        for (pattern, expected) in [
            (
                "p/*.@(md|txt)",
                vec!["a.md", "a.txt", "ab.md", "aba.md", "b.md", "c.md"],
            ),
            (
                "p/?(a|b)*.md",
                vec!["a.md", "ab.md", "aba.md", "b.md", "c.md"],
            ),
            (
                "p/*(a|b)*.md",
                vec!["a.md", "ab.md", "aba.md", "b.md", "c.md"],
            ),
            ("p/+(a|b)*.md", vec!["a.md", "ab.md", "aba.md", "b.md"]),
            (
                "p/@(a|@(b|c))*.md",
                vec!["a.md", "ab.md", "aba.md", "b.md", "c.md"],
            ),
        ] {
            std::fs::write(
                root.path().join("package.json"),
                serde_json::json!({"pi":{"prompts":[pattern]}}).to_string(),
            )
            .unwrap();
            let actual = candidates(root.path(), Kind::Prompt).unwrap().unwrap();
            let expected: Vec<_> = expected
                .into_iter()
                .map(|name| root.path().join("p").join(name))
                .collect();
            assert_eq!(actual, expected, "{pattern}");
        }
        // Pi does not treat @() alone as a glob, even though Node globSync does.
        std::fs::write(
            root.path().join("package.json"),
            serde_json::json!({"pi":{"prompts":["p/@(a|b).md"]}}).to_string(),
        )
        .unwrap();
        assert!(
            candidates(root.path(), Kind::Prompt)
                .unwrap()
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn unsupported_extglobs_fall_back_without_partial_results() {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir(root.path().join("p")).unwrap();
        std::fs::write(root.path().join("p/a.md"), "fixture").unwrap();
        for pattern in [
            "p/*.!(md|txt)",
            "p/@(a|!(b))*.md",
            "p/@(a|b/*.md)",
            "p/@(a|b*.md",
            "p/**/@(a|b)*",
            "p/@(a|b)*.[mt]d",
            "p/@(a|b)*.{md,txt}",
        ] {
            std::fs::write(
                root.path().join("package.json"),
                serde_json::json!({"pi":{"prompts":["p/a.md", pattern]}}).to_string(),
            )
            .unwrap();
            assert!(
                candidates(root.path(), Kind::Prompt).unwrap().is_none(),
                "{pattern}"
            );
        }
        std::fs::write(
            root.path().join("package.json"),
            serde_json::json!({"pi":{"prompts":["p/a.md", "!p/*.@(md|txt)"]}}).to_string(),
        )
        .unwrap();
        assert!(candidates(root.path(), Kind::Prompt).unwrap().is_none());
        assert!(extglob_regex(&format!("{}*.md", "@(a|".repeat(MAX_EXTGLOB_DEPTH + 1))).is_none());
        assert!(extglob_regex(&format!("{}@(a|b)*", "a".repeat(MAX_EXTGLOB_PATTERN))).is_none());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn extglob_non_utf8_paths_fall_back() {
        use std::os::unix::ffi::OsStringExt;
        let root = tempfile::tempdir().unwrap();
        std::fs::write(
            root.path()
                .join(std::ffi::OsString::from_vec(vec![b'x', 0xff])),
            "fixture",
        )
        .unwrap();
        std::fs::write(
            root.path().join("package.json"),
            serde_json::json!({"pi":{"prompts":["*.@(md|txt)"]}}).to_string(),
        )
        .unwrap();
        assert!(candidates(root.path(), Kind::Prompt).unwrap().is_none());
    }

    #[test]
    fn manifest_globs_resolve_relative_dot_components() {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir(root.path().join("prompts")).unwrap();
        std::fs::write(root.path().join("prompts/demo.md"), "prompt").unwrap();
        for pattern in ["./prompts/*.md", "prompts/./*.md", "./prompts//*.md"] {
            std::fs::write(
                root.path().join("package.json"),
                serde_json::json!({"pi":{"prompts":[pattern]}}).to_string(),
            )
            .unwrap();
            assert_eq!(
                candidates(root.path(), Kind::Prompt).unwrap().unwrap(),
                vec![root.path().join("prompts/demo.md")]
            );
        }
    }

    #[test]
    fn trailing_directory_globs_exclude_files() {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(root.path().join("prompts/sub")).unwrap();
        std::fs::write(root.path().join("prompts/file.md"), "prompt").unwrap();
        #[cfg(unix)]
        let outside = tempfile::tempdir().unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink(outside.path(), root.path().join("prompts/link")).unwrap();
        for pattern in ["prompts/*/", "prompts/*/."] {
            let mut scanner = Scanner {
                count: 0,
                ignore_bytes: 0,
                active: HashSet::new(),
            };
            assert_eq!(
                scanner.glob(root.path(), pattern).unwrap(),
                vec![root.path().join("prompts/sub")]
            );
        }
    }

    #[test]
    fn nested_extension_manifest_paths_are_normalized_before_overrides() {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir(root.path().join("extensions")).unwrap();
        std::fs::write(root.path().join("entry.ts"), "extension").unwrap();
        std::fs::write(
            root.path().join("extensions/package.json"),
            serde_json::json!({"pi":{"extensions":["../entry.ts"]}}).to_string(),
        )
        .unwrap();
        std::fs::write(
            root.path().join("package.json"),
            serde_json::json!({"pi":{"extensions":["extensions","-entry.ts"]}}).to_string(),
        )
        .unwrap();
        assert!(
            candidates(root.path(), Kind::Extension)
                .unwrap()
                .unwrap()
                .is_empty()
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn nested_extension_manifest_preserves_non_utf8_directories() {
        use std::os::unix::ffi::OsStringExt;
        let root = tempfile::tempdir().unwrap();
        let extensions = root.path().join("extensions");
        let nested = extensions.join(std::ffi::OsString::from_vec(vec![b'x', 0xff]));
        std::fs::create_dir_all(&nested).unwrap();
        std::fs::write(extensions.join("entry.ts"), "extension").unwrap();
        std::fs::write(
            nested.join("package.json"),
            serde_json::json!({"pi":{"extensions":["../entry.ts"]}}).to_string(),
        )
        .unwrap();
        std::fs::write(
            root.path().join("package.json"),
            serde_json::json!({"pi":{"extensions":["extensions","-extensions/entry.ts"]}})
                .to_string(),
        )
        .unwrap();
        assert!(
            candidates(root.path(), Kind::Extension)
                .unwrap()
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            extension_entry(&nested, &mut 0).unwrap().unwrap(),
            vec![extensions.join("entry.ts")]
        );
    }

    #[test]
    fn candidates_follow_pi_manifest_without_loading_resources() {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(root.path().join("skills/demo/nested")).unwrap();
        std::fs::create_dir_all(root.path().join("prompts/nested")).unwrap();
        std::fs::write(root.path().join("entry.ts"), "not executable").unwrap();
        std::fs::write(root.path().join("skills/demo/SKILL.md"), "skill").unwrap();
        std::fs::write(
            root.path().join("skills/demo/nested/SKILL.md"),
            "hidden by parent",
        )
        .unwrap();
        std::fs::write(root.path().join("prompts/visible.md"), "prompt").unwrap();
        std::fs::write(root.path().join("prompts/ignored.md"), "prompt").unwrap();
        std::fs::write(root.path().join("prompts/.ignore"), "ignored.md\n").unwrap();
        std::fs::write(root.path().join("prompts/nested/deep.md"), "prompt").unwrap();
        std::fs::write(root.path().join("prompts/.hidden.md"), "hidden").unwrap();
        std::fs::write(
            root.path().join("package.json"),
            serde_json::json!({"pi": {
                "extensions":["entry.ts"], "skills":["skills"], "prompts":["prompts"]
            }})
            .to_string(),
        )
        .unwrap();
        assert_eq!(
            candidates(root.path(), Kind::Extension).unwrap().unwrap(),
            vec![root.path().join("entry.ts")]
        );
        assert_eq!(
            candidates(root.path(), Kind::Skill).unwrap().unwrap(),
            vec![root.path().join("skills/demo/SKILL.md")]
        );
        let mut prompts = candidates(root.path(), Kind::Prompt).unwrap().unwrap();
        prompts.sort();
        let mut expected = vec![
            root.path().join("prompts/visible.md"),
            root.path().join("prompts/nested/deep.md"),
        ];
        expected.sort();
        assert_eq!(prompts, expected);
    }

    #[test]
    fn explicit_hidden_paths_and_manifest_overrides_are_respected() {
        let root = tempfile::tempdir().unwrap();
        for name in ["a.md", "b.md", ".hidden.md"] {
            std::fs::write(root.path().join(name), "fixture").unwrap();
        }
        std::fs::write(
            root.path().join("package.json"),
            serde_json::json!({"pi": {
                "prompts":["*.md", ".hidden.md", "!a.md", "+a.md", "-b.md"]
            }})
            .to_string(),
        )
        .unwrap();
        let mut paths = candidates(root.path(), Kind::Prompt).unwrap().unwrap();
        paths.sort();
        let mut expected = vec![root.path().join("a.md"), root.path().join(".hidden.md")];
        expected.sort();
        assert_eq!(paths, expected);
    }

    #[test]
    fn depth_limit_falls_back_without_a_partial_candidate_set() {
        let root = tempfile::tempdir().unwrap();
        let mut path = root.path().join("prompts");
        for _ in 0..MAX_DEPTH {
            path.push("d");
        }
        std::fs::create_dir_all(&path).unwrap();
        std::fs::write(path.join("deep.md"), "fixture").unwrap();
        std::fs::write(
            root.path().join("package.json"),
            serde_json::json!({"pi":{"prompts":["prompts"]}}).to_string(),
        )
        .unwrap();
        assert!(candidates(root.path(), Kind::Prompt).unwrap().is_none());
    }

    #[cfg(unix)]
    #[test]
    fn special_ignore_files_do_not_block_discovery() {
        use std::os::unix::ffi::OsStrExt;
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir(root.path().join("prompts")).unwrap();
        let fifo = root.path().join("prompts/.ignore");
        let name = std::ffi::CString::new(fifo.as_os_str().as_bytes()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(name.as_ptr(), 0o600) }, 0);
        std::fs::write(
            root.path().join("package.json"),
            serde_json::json!({"pi":{"prompts":["prompts"]}}).to_string(),
        )
        .unwrap();
        assert!(candidates(root.path(), Kind::Prompt).unwrap().is_none());
    }

    #[test]
    fn missing_paths_also_consume_the_discovery_budget() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(
            root.path().join("package.json"),
            serde_json::json!({"pi":{"extensions":["missing-a", "missing-b"]}}).to_string(),
        )
        .unwrap();
        let mut count = MAX_ENTRIES - 1;
        assert!(extension_entry(root.path(), &mut count).is_none());
        let mut scanner = Scanner {
            count: MAX_ENTRIES,
            ignore_bytes: 0,
            active: HashSet::new(),
        };
        assert!(!scanner.source(&root.path().join("missing"), Kind::Prompt, &mut Vec::new()));
    }

    #[cfg(unix)]
    #[test]
    fn symlink_cycles_fall_back_without_incomplete_candidates() {
        use std::os::unix::fs::symlink;
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir(root.path().join("prompts")).unwrap();
        symlink(
            root.path().join("prompts"),
            root.path().join("prompts/cycle"),
        )
        .unwrap();
        std::fs::write(
            root.path().join("package.json"),
            serde_json::json!({"pi":{"prompts":["prompts"]}}).to_string(),
        )
        .unwrap();
        assert!(candidates(root.path(), Kind::Prompt).unwrap().is_none());
    }
}
