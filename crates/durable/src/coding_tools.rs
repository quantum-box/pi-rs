//! Opt-in host adapters for the workbench's local coding tools.
//!
//! These registrations use the workbench implementations directly. They do not
//! provide a sandbox, exactly-once effects, or filesystem isolation. In
//! particular, shell commands can access resources outside the workspace.

use std::path::Path;

use pi_rs_core::Extension;
use pi_rs_workbench::coding::CodingTools;

use crate::{recovery::ReplayPolicy, runtime::RegisteredTool};

/// Register read, write, edit and bash using the local Pi workbench profile.
///
/// The workspace sets the working directory, not an access boundary. All
/// tools are unsafe to replay by default: even reads may observe changed files
/// or special files, while mutations and commands can have external effects.
/// Hosts must persist the intent and result through the durable runtime and
/// decide how to reconcile interrupted calls.
///
/// On Windows, the local profile also exposes bash; its availability depends
/// on the configured system shell. PowerShell is intentionally not registered.
pub fn local(workspace: &Path) -> pi_rs_core::Result<Vec<RegisteredTool>> {
    let coding = CodingTools::local(workspace)?;
    Ok(coding
        .tools()
        .into_iter()
        .filter(|tool| {
            matches!(
                tool.spec().name.as_str(),
                "read" | "write" | "edit" | "bash"
            )
        })
        .map(|tool| RegisteredTool {
            tool,
            replay: ReplayPolicy::Unsafe,
        })
        .collect())
}
