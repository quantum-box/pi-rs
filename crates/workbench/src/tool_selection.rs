//! Startup tool selection shared by the CLI and persistent terminal runtime.
use pi_rs::{Error, Result};
use std::collections::BTreeSet;

use super::Cli;

const BUILTINS: &[&str] = &[
    "read",
    "bash",
    "powershell",
    "edit",
    "write",
    "grep",
    "find",
    "ls",
];

pub struct Selection {
    active: BTreeSet<String>,
    allow: Option<Vec<regex::Regex>>,
    exclude: Vec<regex::Regex>,
    removed: BTreeSet<String>,
    no_tools: bool,
    no_builtin: bool,
    mcp_selected: bool,
}
impl Selection {
    pub fn new(args: &Cli) -> Result<Self> {
        let defaults = args.settings.value().get("defaultTools");
        let mut active: BTreeSet<String> = if args.no_tools || args.no_builtin_tools {
            BTreeSet::new()
        } else if let Some(defaults) = defaults {
            let entries: Vec<String> = serde_json::from_value(defaults.clone())?;
            let mut active = standard();
            if entries.iter().any(|entry| entry.starts_with(['+', '-'])) {
                modify(&mut active, &entries)?;
            } else {
                active = entries.into_iter().collect();
            }
            active
        } else if args.coding_tools {
            BUILTINS.iter().map(|name| (*name).into()).collect()
        } else {
            standard()
        };
        let mut removed = BTreeSet::new();
        if let Some(defaults) = defaults.and_then(serde_json::Value::as_array) {
            for entry in defaults.iter().filter_map(serde_json::Value::as_str) {
                if let Some(name) = entry.strip_prefix('-')
                    && !active.contains(name)
                {
                    removed.insert(name.to_owned());
                }
            }
        }
        let entries = args.tools.as_ref().map(|value| parse_entries(value));
        let allow = if let Some(entries) = &entries {
            if entries.iter().any(|entry| entry.starts_with(['+', '-'])) {
                modify(&mut active, entries)?;
                for entry in entries {
                    if let Some(name) = entry.strip_prefix('-') {
                        removed.insert(name.into());
                    }
                    if let Some(name) = entry.strip_prefix('+') {
                        removed.remove(name);
                    }
                }
                None
            } else {
                Some(patterns(entries)?)
            }
        } else {
            None
        };
        Ok(Self {
            active,
            allow,
            removed,
            exclude: patterns(
                &args
                    .exclude_tools
                    .as_ref()
                    .map(|value| parse_entries(value))
                    .unwrap_or_default(),
            )?,
            no_tools: args.no_tools,
            no_builtin: args.no_builtin_tools,
            mcp_selected: entries
                .as_ref()
                .is_some_and(|entries| entries.iter().any(|entry| entry.starts_with("mcp__"))),
        })
    }
    pub fn enable_automatic(&mut self, name: &str) {
        if !self.no_tools && self.allow.is_none() {
            self.active.insert(name.into());
        }
    }
    pub fn declares_mcp(&self) -> bool {
        self.allow.is_none() || self.mcp_selected
    }
    pub fn callable(&self, name: &str) -> bool {
        if self.exclude.iter().any(|pattern| pattern.is_match(name)) {
            return false;
        }
        if name.starts_with("mcp__") && self.mcp_selected {
            return self
                .allow
                .as_ref()
                .is_none_or(|allow| allow.iter().any(|pattern| pattern.is_match(name)));
        }
        true
    }
    pub fn includes(&self, name: &str) -> bool {
        if self.exclude.iter().any(|pattern| pattern.is_match(name)) || self.removed.contains(name)
        {
            return false;
        }
        if let Some(allow) = &self.allow {
            return (name.starts_with("mcp__") && !self.mcp_selected)
                || allow.iter().any(|pattern| pattern.is_match(name));
        }
        if self.active.contains(name) {
            return true;
        }
        if self.no_tools {
            return false;
        }
        if self.no_builtin && matches!(name, "read_text" | "count_lines" | "lookup_order") {
            return false;
        }
        if BUILTINS.contains(&name) || matches!(name, "tool_search" | "codemode") {
            return false;
        }
        true
    }
}
fn standard() -> BTreeSet<String> {
    [
        "read",
        if cfg!(windows) { "powershell" } else { "bash" },
        "edit",
        "write",
    ]
    .map(str::to_owned)
    .into()
}
fn parse_entries(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(str::to_owned)
        .collect()
}
fn modify(active: &mut BTreeSet<String>, entries: &[String]) -> Result<()> {
    for entry in entries {
        let Some(name) = entry
            .get(1..)
            .filter(|name| !name.is_empty() && !name.contains('*'))
        else {
            return Err(Error::Config(
                "tool modifiers require an exact nonempty name".into(),
            ));
        };
        match entry.as_bytes()[0] {
            b'+' => {
                active.insert(name.into());
            }
            b'-' => {
                active.remove(name);
            }
            _ => {
                return Err(Error::Config(
                    "tool names cannot be mixed with +name or -name entries".into(),
                ));
            }
        }
    }
    Ok(())
}
fn patterns(entries: &[String]) -> Result<Vec<regex::Regex>> {
    entries
        .iter()
        .map(|entry| {
            regex::Regex::new(&format!("^{}$", regex::escape(entry).replace("\\*", ".*")))
                .map_err(|error| Error::Config(error.to_string()))
        })
        .collect()
}
#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    #[test]
    fn startup_selection_supports_defaults_modifiers_and_mcp_patterns() {
        let args =
            Cli::try_parse_from(["pi", "--no-tools", "--tools", "read,mcp__files__*"]).unwrap();
        let selection = Selection::new(&args).unwrap();
        assert!(selection.includes("read"));
        assert!(selection.includes("mcp__files__read"));
        assert!(!selection.includes("mcp__other__write"));
        assert!(!selection.includes("bash"));
        let args = Cli::try_parse_from(["pi", "--tools=+grep,-bash", "--exclude-tools", "mcp__*"])
            .unwrap();
        let selection = Selection::new(&args).unwrap();
        assert!(selection.includes("grep"));
        assert!(selection.includes("read"));
        assert!(!selection.includes("bash"));
        assert!(!selection.includes("mcp__files__read"));
        let args = Cli::try_parse_from(["pi", "--tools=read,+grep"]).unwrap();
        assert!(Selection::new(&args).is_err());
    }
}
