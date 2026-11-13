//! Pi-style tool-set factories for embedded hosts.
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use pi_rs::{Error, Extension, Result, Tool};

use crate::coding::{
    BashOperations, CodingTools, EditOperations, EditToolOptions, ReadOperations,
    RemotePathOptions, ShellConfig, ShellOutputStore, WriteOperations,
    create_bash_tool_with_shell_outputs, create_edit_tool_with_path_options,
    create_read_tool_with_options_and_shell_outputs, create_write_tool_with_options,
};

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

/// Configuration for a fully injected remote coding set.
#[derive(Default)]
pub struct RemoteCodingToolOptions {
    /// Backend home for `~`; absent means home-relative file paths are rejected.
    pub paths: RemotePathOptions,
    /// Atomic edit policy for the backend.
    pub edit: EditToolOptions,
    /// Shell configuration, including any host-supplied spawn hook.
    pub shell: ShellConfig,
}

/// Create a remote coding set without accessing or canonicalizing the host cwd.
/// The same backend allocation supplies all operations, including its remote
/// shell. The backend must enforce its own access policy and route all operations
/// to the same environment. A spawn hook can alter shell routing and is trusted.
pub fn create_remote_coding_tools<T>(
    cwd: &Path,
    backend: Arc<T>,
    options: RemoteCodingToolOptions,
) -> Result<CodingToolSet>
where
    T: ReadOperations + WriteOperations + EditOperations + BashOperations + 'static,
{
    if !cwd.is_absolute() {
        return Err(Error::Config(
            "remote working directory must be absolute".into(),
        ));
    }
    if backend.is_local() {
        return Err(Error::Config(
            "remote coding tools require a non-local shell backend".into(),
        ));
    }
    let outputs = ShellOutputStore::default();
    let read = create_read_tool_with_options_and_shell_outputs(
        cwd,
        backend.clone(),
        options.paths.clone(),
        outputs.clone(),
    )?;
    let shell = create_bash_tool_with_shell_outputs(cwd, options.shell, backend.clone(), outputs)?;
    let edit = create_edit_tool_with_path_options(
        cwd,
        backend.clone(),
        options.edit,
        options.paths.clone(),
    )?;
    let write = create_write_tool_with_options(cwd, backend, options.paths)?;
    Ok(CodingToolSet {
        id: "pi-coding-tools",
        tools: vec![read, shell, edit, write],
    })
}

/// Select the coding set from a host-configured profile, retaining its options.
/// Scoped local file tools do not restrict the local shell, which is not sandboxed.
/// Custom operations must all share one non-local backend Arc. The host must
/// ensure that backend consistently enforces its access policy.
pub fn coding_tools_from(source: &CodingTools) -> Result<CodingToolSet> {
    let custom_files = source.has_custom_read_operations()
        || source.has_custom_write_operations()
        || source.has_custom_edit_operations();
    if (custom_files || source.has_custom_bash_operations())
        && !source.shell_and_file_operations_share_remote_backend()
    {
        return Err(Error::Config("coding tools require read, write, edit and non-local bash operations sharing the same backend Arc; use the shell-free set or explicitly opt into a local shell".into()));
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
    if source.has_custom_bash_operations() {
        return Err(Error::Config("local-shell opt-in requires the default host-local shell, not injected bash operations".into()));
    }
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

/// Build all read-only tools from the same remote backend allocation and path namespace.
/// The backend enforces access control and implements search semantics. `remote_home`
/// must be absolute when set; without it, home-relative paths are rejected.
pub fn create_remote_read_only_tools<B>(
    cwd: &Path,
    backend: Arc<B>,
    remote_home: Option<PathBuf>,
) -> Result<CodingToolSet>
where
    B: crate::coding::ReadOperations
        + pi_rs_remote_find::FindOperations
        + pi_rs_remote_grep::GrepOperations
        + pi_rs_remote_ls::LsOperations
        + 'static,
{
    let tools = vec![
        crate::coding::create_read_tool_with_options(
            cwd,
            backend.clone(),
            crate::coding::RemotePathOptions {
                remote_home: remote_home.clone(),
            },
        )?,
        pi_rs_remote_grep::create_grep_tool_with_options(
            cwd,
            backend.clone(),
            pi_rs_remote_grep::GrepToolOptions {
                remote_home: remote_home.clone(),
            },
        )?,
        pi_rs_remote_find::create_find_tool_with_options(
            cwd,
            backend.clone(),
            pi_rs_remote_find::FindToolOptions {
                remote_home: remote_home.clone(),
            },
        )?,
        pi_rs_remote_ls::create_ls_tool_with_options(
            cwd,
            backend,
            pi_rs_remote_ls::LsToolOptions { remote_home },
        )?,
    ];
    Ok(CodingToolSet {
        id: "pi-remote-read-only-tools",
        tools,
    })
}
