//! Pi-style tool-set factories for embedded hosts.
use std::{path::Path, sync::Arc};

use pi_rs::{Error, Extension, Result, Tool};

use crate::coding::CodingTools;

/// A selected set of model-facing tools, ready to register as an extension.
pub struct CodingToolSet {
    id: &'static str,
    tools: Vec<Arc<dyn Tool>>,
}

impl Extension for CodingToolSet {
    fn id(&self) -> &str {
        self.id
    }
    fn tools(&self) -> Vec<Arc<dyn Tool>> {
        self.tools.clone()
    }
}

fn select(source: &CodingTools, id: &'static str, names: &[&str]) -> Result<CodingToolSet> {
    let tools = source.tools();
    let selected = names
        .iter()
        .map(|name| {
            tools
                .iter()
                .find(|tool| tool.spec().name == *name)
                .cloned()
                .ok_or_else(|| {
                    Error::Config(format!(
                        "coding tool {name:?} is unavailable in this profile"
                    ))
                })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(CodingToolSet {
        id,
        tools: selected,
    })
}

/// Pi's default coding tools, using the unrestricted local profile for `cwd`.
pub fn create_coding_tools(cwd: &Path) -> Result<CodingToolSet> {
    coding_tools_from(&CodingTools::local(cwd)?)
}

/// Pi's read-only tools, using the unrestricted local profile for `cwd`.
pub fn create_read_only_tools(cwd: &Path) -> Result<CodingToolSet> {
    read_only_tools_from(&CodingTools::local(cwd)?)
}

/// Select the coding set from a host-configured profile, retaining its options.
pub fn coding_tools_from(source: &CodingTools) -> Result<CodingToolSet> {
    select(
        source,
        "pi-coding-tools",
        &["read", "bash", "edit", "write"],
    )
}

/// Select the read-only set from a host-configured profile, retaining its options.
pub fn read_only_tools_from(source: &CodingTools) -> Result<CodingToolSet> {
    select(
        source,
        "pi-read-only-tools",
        &["read", "grep", "find", "ls"],
    )
}
