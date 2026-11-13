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
/// The shell runs with the host process's permissions and is not sandboxed.
pub fn create_coding_tools(cwd: &Path) -> Result<CodingToolSet> {
    coding_tools_from(&CodingTools::local(cwd)?)
}

/// Pi's read-only tools, using the unrestricted local profile for `cwd`.
pub fn create_read_only_tools(cwd: &Path) -> Result<CodingToolSet> {
    read_only_tools_from(&CodingTools::local(cwd)?)
}

/// Select the coding set from a host-configured profile, retaining its options.
/// Scoped file tools do not restrict the shell, which is not sandboxed.
/// Custom file operations require the shell-free factory or explicit local-shell opt-in.
pub fn coding_tools_from(source: &CodingTools) -> Result<CodingToolSet> {
    if source.has_custom_read_operations()
        || source.has_custom_write_operations()
        || source.has_custom_edit_operations()
    {
        return Err(Error::Config("custom file operations require injected_file_tools_from or explicit local-shell opt-in".into()));
    }
    select_coding_with_shell(source)
}

fn require_shared_file_backend(source: &CodingTools) -> Result<()> {
    if !source.file_operations_share_backend() {
        return Err(Error::Config(
            "custom read, write and edit operations must all share the same backend Arc".into(),
        ));
    }
    Ok(())
}

/// Select injected read, edit and write only, without a local shell.
/// All three operations must share one backend Arc; the host is responsible for
/// ensuring that backend enforces the intended access policy.
pub fn injected_file_tools_from(source: &CodingTools) -> Result<CodingToolSet> {
    require_shared_file_backend(source)?;
    select(source, "pi-injected-file-tools", &["read", "edit", "write"])
}

/// Select all coding tools with injected file operations and an explicitly
/// accepted local shell. The shell is not sandboxed and runs on the host, not
/// on the injected backend. All file operations must share one backend Arc.
pub fn coding_tools_from_with_local_shell(source: &CodingTools) -> Result<CodingToolSet> {
    require_shared_file_backend(source)?;
    select_coding_with_shell(source)
}

fn select_coding_with_shell(source: &CodingTools) -> Result<CodingToolSet> {
    let tools = source.tools();
    let shell = if tools.iter().any(|tool| tool.spec().name == "bash") {
        "bash"
    } else {
        "powershell"
    };
    select(source, "pi-coding-tools", &["read", shell, "edit", "write"])
}

/// Select read-only local tools from a configured profile.
/// Custom read operations are rejected until search tools support the same backend.
pub fn read_only_tools_from(source: &CodingTools) -> Result<CodingToolSet> {
    if source.has_custom_read_operations() {
        return Err(Error::Config(
            "read-only tool sets cannot combine custom read operations with local search tools"
                .into(),
        ));
    }
    select(
        source,
        "pi-read-only-tools",
        &["read", "grep", "find", "ls"],
    )
}
