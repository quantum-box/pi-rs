//! Pi's multi-character flags, translated before Clap handles native arguments.
use std::collections::BTreeMap;
use std::ffi::OsString;

use clap::{Arg, ArgMatches, Command, CommandFactory, FromArgMatches};
use pi_rs::ExtensionFlagValue;

pub(super) type ExtensionFlags = BTreeMap<String, ExtensionFlagValue>;

pub(super) fn parse(
    args: impl IntoIterator<Item = OsString>,
) -> Result<(ArgMatches, ExtensionFlags), clap::Error> {
    parse_ordered(args, true).map(|(matches, flags, _)| (matches, flags))
}

pub(super) fn parse_ordered(
    args: impl IntoIterator<Item = OsString>,
    defer_missing_pi_options: bool,
) -> Result<(ArgMatches, ExtensionFlags, Vec<String>), clap::Error> {
    let command = command();
    let normalized = normalize(&command, args, false, defer_missing_pi_options);
    Ok((
        command.try_get_matches_from(normalized.args)?,
        normalized.flags,
        normalized.flag_order,
    ))
}

fn command() -> Command {
    let mut command = super::Cli::command();
    // Pi assigns these options as it scans argv, so later values replace
    // earlier ones. Keep repeatable resource arguments and native-only flags
    // on their existing Clap actions.
    for name in [
        "provider",
        "model",
        "models",
        "list-models",
        "api-key",
        "session-dir",
        "name",
        "use-theme",
        "system-prompt",
        "tools",
        "exclude-tools",
        "mode",
        "tui-mode",
        "session",
        "session-id",
        "fork",
    ] {
        let id = command
            .get_arguments()
            .find(|arg| arg.get_long() == Some(name))
            .map(|arg| arg.get_id().clone());
        if let Some(id) = id {
            command = command.mut_arg(id.clone(), |arg| arg.overrides_with(id));
        }
    }
    command = command.mut_arg("tools", |arg| arg.value_parser(validate_tool_list));
    command.build();
    command
}

fn validate_tool_list(value: &str) -> Result<String, String> {
    let entries: Vec<_> = value
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect();
    let modifiers: Vec<_> = entries
        .iter()
        .filter(|s| s.starts_with(['+', '-']))
        .collect();
    if !modifiers.is_empty() && modifiers.len() != entries.len() {
        return Err("tool names cannot be mixed with +name or -name entries".into());
    }
    if let Some(pattern) = modifiers.iter().find(|s| s.contains('*')) {
        return Err(format!(
            "+name and -name entries take exact tool names, not patterns: {pattern}"
        ));
    }
    Ok(value.into())
}

struct NormalizedArguments {
    args: Vec<OsString>,
    flags: ExtensionFlags,
    flag_order: Vec<String>,
    native_subcommand: bool,
    literal_export_output: Option<usize>,
}

fn normalize(
    root: &Command,
    args: impl IntoIterator<Item = OsString>,
    metadata_ownership: bool,
    defer_missing_pi_options: bool,
) -> NormalizedArguments {
    let mut input = args.into_iter().peekable();
    let mut output: Vec<_> = input.next().into_iter().collect();
    let mut prompt = Vec::new();
    let mut flags = BTreeMap::new();
    let mut flag_order = Vec::new();
    let mut command = root;
    let mut remaining = 0;
    let mut hyphen_values = false;
    let mut trust_flag_index = None;
    let mut selected_subcommand = false;
    let mut value_owner: Option<usize> = None;
    let mut export_values = false;
    let mut export_waiting_output = false;
    let mut export_insert_index = None;
    let mut literal_export_output = None;
    while let Some(arg) = input.next() {
        let option = arg.as_encoded_bytes().starts_with(b"-") && arg != "-";
        if metadata_ownership
            && remaining > 0
            && option
            && hyphen_values
            && let Some(index) = value_owner
        {
            if output[index].as_encoded_bytes().starts_with(b"--") {
                output[index].push("=");
            }
            output[index].push(arg);
            remaining -= 1;
            value_owner = None;
            if export_values {
                export_waiting_output = remaining > 0;
                export_values = remaining > 0;
                if export_waiting_output {
                    export_insert_index = Some(output.len());
                }
            }
            if remaining > 0 {
                hyphen_values = false;
            }
            continue;
        }
        if arg == "--" {
            if metadata_ownership && export_waiting_output {
                let destination = prompt
                    .iter()
                    .position(|value: &OsString| !value.as_encoded_bytes().starts_with(b"@"))
                    .map(|first| prompt.remove(first))
                    .or_else(|| input.find(|value| !value.as_encoded_bytes().starts_with(b"@")));
                if let Some(destination) = destination {
                    let index = export_insert_index.unwrap_or(output.len());
                    if destination.as_encoded_bytes().starts_with(b"-") {
                        literal_export_output = Some(index);
                    }
                    output.insert(index, destination);
                    if let Some(trust_index) = &mut trust_flag_index
                        && *trust_index >= index
                    {
                        *trust_index += 1;
                    }
                }
                // Pi uses only the first positional argument as the export
                // destination. Later tokens after the delimiter are literals.
                break;
            }
            output.push(arg);
            output.append(&mut prompt);
            output.extend(input);
            break;
        }
        if remaining > 0 && (!option || hyphen_values) {
            if metadata_ownership
                && export_values
                && remaining == 1
                && arg.as_encoded_bytes().starts_with(b"@")
            {
                prompt.push(arg);
                remaining = 0;
                export_values = false;
                export_waiting_output = true;
                value_owner = None;
                continue;
            }
            if metadata_ownership
                && export_values
                && remaining == 1
                && let Some(first) = prompt
                    .iter()
                    .position(|value: &OsString| !value.as_encoded_bytes().starts_with(b"@"))
            {
                let destination = prompt.remove(first);
                if destination.as_encoded_bytes().starts_with(b"-") {
                    literal_export_output = Some(output.len());
                }
                output.push(destination);
                prompt.push(arg);
                remaining = 0;
                export_values = false;
                export_waiting_output = false;
                value_owner = None;
                continue;
            }
            let consuming_export = export_values;
            remaining -= 1;
            if export_values {
                export_waiting_output = remaining > 0;
                export_values = remaining > 0;
            }
            if metadata_ownership && value_owner.take().is_some() && remaining > 0 {
                hyphen_values = false;
            }
            output.push(arg);
            if consuming_export && export_waiting_output {
                export_insert_index = Some(output.len());
            }
            continue;
        }
        remaining = 0;
        export_values = false;
        if !option {
            if metadata_ownership && export_waiting_output {
                if arg.as_encoded_bytes().starts_with(b"@") {
                    prompt.push(arg);
                    continue;
                }
                let index = export_insert_index.unwrap_or(output.len());
                let destination = if let Some(first) = prompt
                    .iter()
                    .position(|value: &OsString| !value.as_encoded_bytes().starts_with(b"@"))
                {
                    let earlier = prompt.remove(first);
                    prompt.push(arg);
                    earlier
                } else {
                    arg
                };
                if destination.as_encoded_bytes().starts_with(b"-") {
                    literal_export_output = Some(index);
                }
                output.insert(index, destination);
                if let Some(trust_index) = &mut trust_flag_index
                    && *trust_index >= index
                {
                    *trust_index += 1;
                }
                export_waiting_output = false;
                export_values = false;
                continue;
            }
            if prompt.is_empty()
                && let Some(subcommand) = command.find_subcommand(&arg)
            {
                command = subcommand;
                selected_subcommand = true;
            } else if std::ptr::eq(command, root) {
                prompt.push(arg);
                continue;
            }
            output.push(arg);
            continue;
        }
        let Some(value) = arg.to_str() else {
            let attached_export =
                metadata_ownership && arg.as_encoded_bytes().starts_with(b"--export=");
            output.push(arg);
            if attached_export {
                export_values = true;
                export_waiting_output = true;
                export_insert_index = Some(output.len());
            }
            continue;
        };
        if std::ptr::eq(command, root) && value == "-xt" && input.peek().is_none() {
            output.push(arg);
            continue;
        }
        let canonical = match value {
            "-v" => "--version",
            "-nt" => "--no-tools",
            "-nbt" => "--no-builtin-tools",
            "-xt" => "--exclude-tools",
            "-ne" => "--no-extensions",
            "-ns" => "--no-skills",
            "-np" => "--no-prompt-templates",
            "-nc" => "--no-context-files",
            "-na" => "--no-trust",
            _ => value,
        };
        if let Some(long) = canonical.strip_prefix("--") {
            let (name, attached) = long
                .split_once('=')
                .map_or((long, None), |(name, value)| (name, Some(value)));
            let missing_pi_value = defer_missing_pi_options
                && std::ptr::eq(command, root)
                && value == canonical
                && canonical != "--name"
                && attached.is_none()
                && pi_unconditional_value(canonical)
                && input.peek().is_none();
            if (missing_pi_value || (long_arg(command, name).is_none() && flag_name(name)))
                && accepts_extension_flags(command, root)
            {
                let value = if let Some(value) = attached {
                    ExtensionFlagValue::String(value.into())
                } else if input.peek().is_some_and(|next| {
                    !next.as_encoded_bytes().starts_with(b"-")
                        && !next.as_encoded_bytes().starts_with(b"@")
                        && (!prompt.is_empty() || command.find_subcommand(next).is_none())
                        && next.to_str().is_some()
                }) {
                    ExtensionFlagValue::String(
                        input
                            .next()
                            .and_then(|next| next.into_string().ok())
                            .expect("validated flag value"),
                    )
                } else {
                    ExtensionFlagValue::Boolean(true)
                };
                if flags.insert(name.into(), value).is_none() {
                    flag_order.push(name.into());
                }
                continue;
            }
        }
        if std::ptr::eq(command, root)
            && canonical == "--list-models"
            && !input.peek().is_some_and(|next| list_models_search(next))
        {
            output.push("--list-models=".into());
            continue;
        }
        (remaining, hyphen_values) = option_values(command, canonical);
        if metadata_ownership && (canonical == "--export" || canonical.starts_with("--export=")) {
            export_values = remaining > 0;
            export_waiting_output = canonical.starts_with("--export=");
        }
        if metadata_ownership && pi_unconditional_value(canonical) && remaining > 0 {
            hyphen_values = true;
            value_owner = Some(output.len());
        } else {
            value_owner = None;
        }
        let print = matches!(canonical, "--print" | "-p");
        if matches!(
            canonical,
            "-a" | "--approve" | "--trust" | "--no-approve" | "--no-trust"
        ) {
            if let Some(index) = trust_flag_index {
                output.remove(index);
                if let Some(export_index) = &mut export_insert_index
                    && *export_index > index
                {
                    *export_index -= 1;
                }
                if let Some(literal_index) = &mut literal_export_output
                    && *literal_index > index
                {
                    *literal_index -= 1;
                }
            }
            trust_flag_index = Some(output.len());
        }
        let attached_export = metadata_ownership && canonical.starts_with("--export=");
        output.push(if canonical == value {
            arg
        } else {
            canonical.into()
        });
        if attached_export {
            export_insert_index = Some(output.len());
        }
        if std::ptr::eq(command, root)
            && print
            && input.peek().is_some_and(|next| {
                let bytes = next.as_encoded_bytes();
                ((!bytes.starts_with(b"@") && !bytes.starts_with(b"-"))
                    || bytes.starts_with(b"---"))
                    && next != "extension"
            })
        {
            prompt.extend(input.next());
        }
    }
    if metadata_ownership
        && export_waiting_output
        && let Some(first) = prompt
            .iter()
            .position(|value: &OsString| !value.as_encoded_bytes().starts_with(b"@"))
    {
        let index = export_insert_index.unwrap_or(output.len());
        let destination = prompt.remove(first);
        if destination.as_encoded_bytes().starts_with(b"-") {
            literal_export_output = Some(index);
        }
        output.insert(index, destination);
    }
    if !prompt.is_empty() {
        output.push("--".into());
        output.extend(prompt);
    }
    NormalizedArguments {
        args: output,
        flags,
        flag_order,
        native_subcommand: selected_subcommand,
        literal_export_output,
    }
}

fn list_models_search(value: &std::ffi::OsStr) -> bool {
    let bytes = value.as_encoded_bytes();
    !bytes.starts_with(b"-") && !bytes.starts_with(b"@")
}

fn pi_unconditional_value(option: &str) -> bool {
    matches!(
        option,
        "--provider"
            | "--model"
            | "--api-key"
            | "--system-prompt"
            | "--append-system-prompt"
            | "--name"
            | "-n"
            | "--session"
            | "--session-id"
            | "--fork"
            | "--session-dir"
            | "--models"
            | "--tools"
            | "-t"
            | "--exclude-tools"
            | "--thinking"
            | "--export"
            | "--extension"
            | "-e"
            | "--skill"
            | "--prompt-template"
            | "--theme"
    )
}

fn accepts_extension_flags(command: &Command, root: &Command) -> bool {
    std::ptr::eq(command, root)
        || matches!(
            command.get_name(),
            "run" | "resume" | "interactive" | "serve" | "compact" | "review"
        )
}

fn flag_name(name: &str) -> bool {
    name.len() <= 128
        && name.as_bytes().first().is_some_and(u8::is_ascii_alphabetic)
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-_".contains(&byte))
}

fn long_arg<'a>(command: &'a Command, name: &str) -> Option<&'a Arg> {
    command.get_arguments().find(|arg| {
        arg.get_long() == Some(name)
            || arg
                .get_all_aliases()
                .is_some_and(|aliases| aliases.contains(&name))
    })
}

pub(super) fn reserved_flag_names() -> std::collections::BTreeSet<String> {
    let mut command = super::Cli::command();
    command.build();
    command
        .get_arguments()
        .flat_map(|arg| {
            arg.get_long()
                .into_iter()
                .chain(arg.get_all_aliases().unwrap_or_default())
        })
        .map(str::to_owned)
        .collect()
}

pub(super) fn extension_help(argv: &[OsString]) -> Option<String> {
    let args: Vec<_> = argv
        .iter()
        .filter(|argument| *argument != "--help" && *argument != "-h")
        .cloned()
        .collect();
    extension_help_from_args(&args)
}

pub(super) fn extension_help_from_args(argv: &[OsString]) -> Option<String> {
    let (matches, _) = parse(argv.iter().cloned()).ok()?;
    let mut args = super::Cli::from_arg_matches(&matches).ok()?;
    if args.command.is_some() {
        return None;
    }
    let explicit_trust = if args.no_trust {
        Some(false)
    } else if args.trust || args.project.is_some() {
        Some(true)
    } else {
        None
    };
    args.trusted = super::config::project_trust(&args.workspace, explicit_trust)
        .ok()?
        .unwrap_or(false);
    args.settings = super::config::Settings::load(&args.workspace, args.trusted).ok()?;
    let mut flags = BTreeMap::new();
    for flag in discovered_extension_flags(&args).ok()? {
        let usage = match flag.default {
            ExtensionFlagValue::Boolean(_) => format!("--{}", flag.name),
            ExtensionFlagValue::String(_) => format!("--{} <value>", flag.name),
        };
        let description = flag.description.unwrap_or_default();
        let description: String = description
            .chars()
            .take(200)
            .map(|character| {
                if character.is_control() {
                    ' '
                } else {
                    character
                }
            })
            .collect();
        flags.entry(usage).or_insert(description);
    }
    if flags.is_empty() {
        return None;
    }
    let mut help = String::from("\nExtension CLI flags:\n");
    for (usage, description) in flags {
        use std::fmt::Write as _;
        let _ = writeln!(help, "  {usage:<25} {description}");
    }
    Some(help)
}

pub(super) fn discovered_extension_flags(
    args: &super::Cli,
) -> pi_rs::Result<Vec<pi_rs::ExtensionFlag>> {
    use pi_rs::Extension;

    let resources = super::resources::Resources::load(args)?;
    let reserved = reserved_flag_names();
    let mut flags = Vec::new();
    for path in resources.extensions {
        let Ok(extension) = pi_rs_extensions::ExecutableExtension::load(&path) else {
            continue;
        };
        if args.disable_extension.iter().any(|id| id == extension.id()) {
            continue;
        }
        for mut flag in extension.cli_flags() {
            if !reserved.contains(&flag.name) {
                flag.description
                    .get_or_insert_with(|| format!("Registered by {}", extension.id()));
                flags.push(flag);
            }
        }
    }
    Ok(flags)
}

/// Export paths are resolved independently of Clap's multi-value `--export`.
/// Keeping them typed prevents a later input from becoming an output path.
#[derive(Debug)]
pub(super) struct ExportPlan {
    pub input: std::path::PathBuf,
    pub inputs: Vec<std::path::PathBuf>,
    pub output: Option<std::path::PathBuf>,
    pub validation_args: Vec<OsString>,
}

/// Project root export arguments onto native validation arguments while assigning
/// each original token exactly once. Native subcommands retain their own parser.
pub(super) fn export_plan(args: &[OsString]) -> Option<ExportPlan> {
    let root = command();
    let mut validation = vec![args.first()?.clone()];
    let mut input = None;
    let mut inputs = Vec::new();
    let mut output = None;
    let mut saw_export = false;
    let mut saw_message = false;
    let mut i = 1;
    while i < args.len() {
        let arg = &args[i];
        let bytes = arg.as_encoded_bytes();
        if arg == "--" {
            for value in &args[i + 1..] {
                if !value.as_encoded_bytes().starts_with(b"@") && output.is_none() {
                    output = Some(value.clone().into());
                }
            }
            break;
        }
        if arg == "--export" {
            i += 1;
            if i == args.len() {
                validation.push(arg.clone());
                break;
            }
            saw_export = true;
            let path = std::path::PathBuf::from(&args[i]);
            inputs.push(path.clone());
            input = Some(path);
            i += 1;
            continue;
        }
        if let Some(value) = bytes.strip_prefix(b"--export=") {
            saw_export = true;
            // Removing an ASCII prefix preserves an OsStr encoding boundary.
            let value = unsafe { std::ffi::OsStr::from_encoded_bytes_unchecked(value) };
            let path = std::path::PathBuf::from(value);
            inputs.push(path.clone());
            input = Some(path);
            i += 1;
            continue;
        }
        if !bytes.starts_with(b"-") || arg == "-" {
            if !saw_export
                && !saw_message
                && !bytes.starts_with(b"@")
                && root.find_subcommand(arg).is_some()
            {
                return None;
            }
            if !bytes.starts_with(b"@") {
                saw_message = true;
                if output.is_none() {
                    output = Some(arg.clone().into());
                }
            }
            i += 1;
            continue;
        }
        let Some(text) = arg.to_str() else {
            validation.push(arg.clone());
            i += 1;
            continue;
        };
        validation.push(arg.clone());
        if text == "--print" || text == "-p" {
            if let Some(next) = args.get(i + 1) {
                let value = next.as_encoded_bytes();
                if !value.starts_with(b"@")
                    && (!value.starts_with(b"-") || value.starts_with(b"---"))
                    && next != "extension"
                {
                    saw_message = true;
                    if output.is_none() {
                        output = Some(next.clone().into());
                    }
                    i += 1;
                }
            }
        } else {
            let canonical = match text {
                "-nt" => "--no-tools",
                "-nbt" => "--no-builtin-tools",
                "-xt" => "--exclude-tools",
                "-ne" => "--no-extensions",
                "-ns" => "--no-skills",
                "-np" => "--no-prompt-templates",
                "-nc" => "--no-context-files",
                "-na" => "--no-trust",
                _ => text,
            };
            let (count, allow_hyphen) = option_values(&root, canonical);
            let unknown_long = text.strip_prefix("--").is_some_and(|name| {
                let name = name.split('=').next().unwrap_or(name);
                long_arg(&root, name).is_none() && flag_name(name) && !text.contains('=')
            });
            let count = if unknown_long { 1 } else { count.min(1) };
            for _ in 0..count {
                let Some(next) = args.get(i + 1) else { break };
                let next_bytes = next.as_encoded_bytes();
                if canonical == "--list-models" && !list_models_search(next) {
                    validation.last_mut().expect("option").push("=");
                    break;
                }
                let unconditional = pi_unconditional_value(canonical);
                if !unconditional
                    && (next == "--"
                        || (next_bytes.starts_with(b"-") && !allow_hyphen)
                        || (unknown_long
                            && (next_bytes.starts_with(b"@")
                                || next.to_str().is_none()
                                || (!saw_message && root.find_subcommand(next).is_some()))))
                {
                    break;
                }
                // Preserve value ownership when projection removes preceding
                // messages, which can otherwise make a custom flag's value look
                // like a native subcommand during the second normalization.
                if unknown_long || (unconditional && next_bytes.starts_with(b"-")) {
                    let last = validation.last_mut().expect("option");
                    if text.starts_with("--") {
                        last.push("=");
                    }
                    last.push(next);
                } else {
                    validation.push(next.clone());
                }
                i += 1;
            }
        }
        i += 1;
    }
    if !saw_export {
        return None;
    }
    // Empty input cannot be safely confused with an earlier export occurrence.
    let input = input.unwrap_or_default();
    Some(ExportPlan {
        input,
        inputs,
        output,
        validation_args: validation,
    })
}

/// Root metadata flags and diagnostics that may stop CLI startup.
pub(super) struct Metadata {
    pub(super) version: bool,
    pub(super) short_help: bool,
    pub(super) rpc_has_attachments: bool,
    pub(super) export: bool,
    pub(super) literal_export: Option<Vec<std::path::PathBuf>>,
    pub(super) args_without_help: Vec<OsString>,
    pub(super) diagnostics: Vec<(bool, String)>,
}

pub(super) fn metadata(args: &[OsString]) -> Option<Metadata> {
    let command = command();
    let normalized = normalize(&command, args.iter().cloned(), true, false);
    if normalized.native_subcommand {
        return None;
    }
    let args = normalized.args;
    let literal_export_output = normalized.literal_export_output;
    let mut result = Metadata {
        version: false,
        short_help: false,
        rpc_has_attachments: false,
        export: false,
        literal_export: literal_export_output.and_then(|index| {
            let input = args.get(index.checked_sub(1)?)?;
            let input = input
                .as_encoded_bytes()
                .strip_prefix(b"--export=")
                .map_or_else(
                    || std::path::PathBuf::from(input),
                    |value| {
                        // The removed prefix is ASCII, so this is a boundary in
                        // the original OsStr's self-synchronizing encoding.
                        std::path::PathBuf::from(unsafe {
                            std::ffi::OsStr::from_encoded_bytes_unchecked(value)
                        })
                    },
                );
            Some(vec![input, std::path::PathBuf::from(args.get(index)?)])
        }),
        args_without_help: Vec::new(),
        diagnostics: Vec::new(),
    };
    let mut help = false;
    let mut rpc_mode = false;
    let mut help_edits = BTreeMap::new();
    let mut iter = args.iter().skip(1).peekable();
    while let Some(arg_os) = iter.next() {
        if arg_os == "--" {
            // Normalization puts root positional arguments after this delimiter.
            result.rpc_has_attachments =
                rpc_mode && iter.any(|argument| argument.as_encoded_bytes().starts_with(b"@"));
            break;
        }
        let index = args
            .iter()
            .position(|a| std::ptr::eq(a, arg_os))
            .expect("argument belongs to normalized argv");
        if literal_export_output == Some(index) {
            help_edits.insert(index, None);
            continue;
        }
        let Some(arg) = arg_os.to_str() else {
            if arg_os.as_encoded_bytes().starts_with(b"--export=") {
                result.export = true;
            }
            continue;
        };
        let (option, attached) = arg
            .split_once('=')
            .map_or((arg, None), |(a, b)| (a, Some(b)));
        match option {
            "--help" | "-h" if attached.is_none() => {
                if !help {
                    result.short_help = option == "-h";
                }
                help = true;
                help_edits.insert(index, None);
            }
            "--version" | "-V" if attached.is_none() => result.version = true,
            "--mode" | "--tui-mode" | "--use-theme" | "--name" | "-n" => {
                let next = attached.or_else(|| {
                    iter.peek().and_then(|s| s.to_str()).filter(|s| {
                        *s != "--" && (matches!(option, "--name" | "-n") || !s.starts_with('-'))
                    })
                });
                if attached.is_none() && next.is_some() {
                    iter.next();
                }
                let message = match (option, next) {
                    ("--mode", None) => Some("--mode requires text, json, or rpc".into()),
                    ("--mode", Some(value)) if !["text", "json", "rpc"].contains(&value) => Some(
                        format!("Invalid mode \"{value}\". Valid values: text, json, rpc"),
                    ),
                    ("--mode", Some(value)) => {
                        rpc_mode = value == "rpc";
                        None
                    }
                    ("--tui-mode", None) => {
                        Some("--tui-mode requires regular or fullscreen".into())
                    }
                    ("--tui-mode", Some(value)) if !["regular", "fullscreen"].contains(&value) => {
                        Some(format!(
                            "Invalid TUI mode \"{value}\". Valid values: regular, fullscreen"
                        ))
                    }
                    ("--use-theme", None) => Some("--use-theme requires a theme name".into()),
                    ("--name" | "-n", None) => Some("--name requires a value".into()),
                    _ => None,
                };
                if let Some(message) = message {
                    result.diagnostics.push((true, message));
                }
            }
            "--tools" | "-t" | "--thinking" => {
                let value = attached.or_else(|| {
                    if iter.peek().is_some_and(|next| next != &"--") {
                        iter.next().and_then(|s| s.to_str())
                    } else {
                        None
                    }
                });
                if value.is_none() && option == "-t" {
                    result.diagnostics.push((true, "Unknown option: -t".into()));
                }
                if let Some(value) = value {
                    if option == "--thinking" && !super::thinking::LEVELS.contains(&value) {
                        result.diagnostics.push((
                            false,
                            format!(
                                "Invalid thinking level \"{value}\". Valid values: {}",
                                super::thinking::LEVELS.join(", ")
                            ),
                        ));
                    } else if option != "--thinking"
                        && let Err(error) = validate_tool_list(value)
                    {
                        result
                            .diagnostics
                            .push((true, format!("{option}: {error}")));
                    }
                }
            }
            _ if arg.starts_with('-') && arg != "-" => {
                if option == "--export" {
                    result.export = attached.is_some()
                        || iter
                            .peek()
                            .is_some_and(|next| !next.as_encoded_bytes().starts_with(b"-"));
                }
                let mut consumed_value = false;
                if !arg.starts_with("--") {
                    let mut without_help = String::from("-");
                    let mut removed_help = false;
                    for (offset, short) in arg[1..].char_indices() {
                        if short == 'h' {
                            if !help {
                                result.short_help = true;
                            }
                            help = true;
                            removed_help = true;
                            continue;
                        }
                        without_help.push(short);
                        if short == 'V' {
                            result.version = true;
                            continue;
                        }
                        let known = command.get_arguments().find(|a| {
                            a.get_short() == Some(short)
                                || a.get_all_short_aliases()
                                    .is_some_and(|aliases| aliases.contains(&short))
                        });
                        match known {
                            Some(_)
                                if arg == "-e" && iter.peek().is_none_or(|next| *next == "--") =>
                            {
                                result.diagnostics.push((true, "Unknown option: -e".into()));
                            }
                            Some(a) if a.get_action().takes_values() => {
                                let remainder = &arg[1 + offset + short.len_utf8()..];
                                without_help.push_str(remainder);
                                if short == 't' {
                                    let value = if remainder.is_empty() {
                                        consumed_value = true;
                                        if iter.peek().is_some_and(|next| next != &"--") {
                                            iter.next().and_then(|s| s.to_str())
                                        } else {
                                            None
                                        }
                                    } else {
                                        Some(remainder.strip_prefix('=').unwrap_or(remainder))
                                    };
                                    if let Some(value) = value
                                        && let Err(error) = validate_tool_list(value)
                                    {
                                        result.diagnostics.push((true, format!("-t: {error}")));
                                    }
                                }
                                break;
                            }
                            Some(_) => {}
                            None => {
                                result
                                    .diagnostics
                                    .push((true, format!("Unknown option: {arg}")));
                                break;
                            }
                        }
                    }
                    if removed_help {
                        help_edits
                            .insert(index, (without_help.len() > 1).then(|| without_help.into()));
                    }
                }
                let (count, hyphen_values) = option_values(&command, arg);
                for _ in 0..if consumed_value { 0 } else { count } {
                    if iter.peek().is_some_and(|next| {
                        next != &"--"
                            && (hyphen_values || !next.as_encoded_bytes().starts_with(b"-"))
                    }) {
                        iter.next();
                    } else {
                        break;
                    }
                }
            }
            _ => {}
        }
    }
    result.args_without_help = args
        .into_iter()
        .enumerate()
        .filter_map(|(index, arg)| help_edits.remove(&index).unwrap_or(Some(arg)))
        .collect();
    (help || result.version || result.diagnostics.iter().any(|(error, _)| *error)).then_some(result)
}

pub(super) fn unknown_extension_options(names: &[&str]) -> String {
    let label = if names.len() == 1 {
        "option"
    } else {
        "options"
    };
    let options = names
        .iter()
        .map(|name| format!("--{name}"))
        .collect::<Vec<_>>()
        .join(", ");
    format!("Unknown {label}: {options}")
}

fn option_values(command: &Command, option: &str) -> (usize, bool) {
    let argument = if let Some(long) = option.strip_prefix("--") {
        let (name, attached) = long
            .split_once('=')
            .map_or((long, false), |(name, _)| (name, true));
        long_arg(command, name).map(|arg| (arg, attached))
    } else {
        let short = &option[1..];
        short.char_indices().find_map(|(offset, name)| {
            command
                .get_arguments()
                .find(|arg| {
                    arg.get_action().takes_values()
                        && (arg.get_short() == Some(name)
                            || arg
                                .get_all_short_aliases()
                                .is_some_and(|aliases| aliases.contains(&name)))
                })
                .map(|arg| (arg, offset + name.len_utf8() < short.len()))
        })
    };
    argument.map_or((0, false), |(arg, attached)| {
        let count = if arg.is_require_equals_set() {
            0
        } else {
            arg.get_num_args().map_or(0, |range| range.max_values())
        };
        (
            count.saturating_sub(usize::from(attached)),
            arg.is_allow_hyphen_values_set(),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn export_projection_preserves_consumed_custom_flag_value() {
        for value in ["run", "mcp", "export"] {
            let args = [
                "pi",
                "report.html",
                "--custom",
                value,
                "--export",
                "session.jsonl",
            ]
            .map(OsString::from);
            let plan = export_plan(&args).unwrap();
            let (_, flags) = parse(plan.validation_args).unwrap();
            assert_eq!(
                flags.get("custom"),
                Some(&ExtensionFlagValue::String(value.into()))
            );
            assert_eq!(
                plan.output.unwrap(),
                std::path::PathBuf::from("report.html")
            );
        }
        let args =
            ["pi", "report.html", "--custom", "--export", "session.jsonl"].map(OsString::from);
        let plan = export_plan(&args).unwrap();
        let (_, flags) = parse(plan.validation_args).unwrap();
        assert_eq!(
            flags.get("custom"),
            Some(&ExtensionFlagValue::Boolean(true))
        );
    }

    #[cfg(unix)]
    #[test]
    fn export_plan_preserves_non_utf8_input_and_output_bytes() {
        use std::os::unix::ffi::OsStringExt;
        let input = OsString::from_vec(b"input-\xff".to_vec());
        let output = OsString::from_vec(b"output-\xfe".to_vec());
        let mut attached = OsString::from("--export=");
        attached.push(&input);
        let args = vec![
            "pi".into(),
            "--export".into(),
            "old".into(),
            attached,
            "--help".into(),
            output.clone(),
        ];
        let plan = export_plan(&args).unwrap();
        assert_eq!(plan.input.as_os_str(), input);
        assert_eq!(plan.output.unwrap().as_os_str(), output);
        let args = vec![
            "pi".into(),
            "--custom".into(),
            output.clone(),
            "--export".into(),
            input.clone(),
        ];
        let plan = export_plan(&args).unwrap();
        assert_eq!(plan.output.unwrap().as_os_str(), output);
        let (_, flags) = parse(plan.validation_args).unwrap();
        assert_eq!(
            flags.get("custom"),
            Some(&ExtensionFlagValue::Boolean(true))
        );
    }

    #[test]
    fn metadata_respects_option_values_delimiters_and_native_commands() {
        let scan = |args: &[&str]| metadata(&args.iter().map(OsString::from).collect::<Vec<_>>());
        assert!(scan(&["pi", "--thinking", "--help"]).is_none());
        assert!(scan(&["pi", "--", "--version"]).is_none());
        assert!(scan(&["pi", "--native-flag", "--", "--help"]).is_none());
        assert!(scan(&["pi", "mcp", "--help"]).is_none());
        assert!(scan(&["pi", "--help", "run"]).is_none());

        let result = scan(&[
            "pi",
            "--help",
            "--mode",
            "invalid",
            "--mode",
            "json",
            "--thinking",
            "bad",
            "-z",
        ])
        .unwrap();
        assert_eq!(result.diagnostics.len(), 3);
        assert!(result.diagnostics[0].1.contains("Invalid mode"));
        assert!(!result.diagnostics[1].0);
        assert!(result.diagnostics[2].1.contains("Unknown option"));
        assert!(scan(&["pi", "-ph"]).unwrap().short_help);
        assert!(scan(&["pi", "-pV"]).unwrap().version);
        let stripped = scan(&["pi", "--thinking", "--help", "-ph"]).unwrap();
        assert_eq!(
            stripped.args_without_help,
            ["pi", "--thinking=--help", "-p"].map(OsString::from)
        );
        let named = scan(&[
            "pi",
            "--no-trust",
            "--extension",
            "fixture",
            "--name",
            "--help",
            "--help",
        ])
        .unwrap();
        let (matches, _) = parse(named.args_without_help.clone()).unwrap();
        assert!(
            super::super::Cli::from_arg_matches(&matches).is_ok(),
            "{:?}",
            named.args_without_help
        );
        let export = scan(&["pi", "--export", "input.jsonl", "--help", "--", "--version"]).unwrap();
        assert_eq!(
            export.args_without_help,
            ["pi", "--export", "input.jsonl"].map(OsString::from)
        );
        assert_eq!(
            export.literal_export.unwrap(),
            ["input.jsonl", "--version"].map(std::path::PathBuf::from)
        );
    }
    fn cli(args: &[&str]) -> super::super::Cli {
        let (matches, _) = parse(args.iter().map(OsString::from)).unwrap();
        super::super::Cli::from_arg_matches(&matches).unwrap()
    }

    #[test]
    fn pi_flags_use_the_existing_native_controls() {
        let args = cli(&[
            "pi", "-nt", "-nbt", "-xt", "bash", "-ne", "-ns", "-np", "-nc", "-na",
        ]);
        assert!(args.no_tools && args.no_builtin_tools && args.no_extensions);
        assert!(
            args.no_skills && args.no_prompt_templates && args.no_context_files && args.no_trust
        );
        assert_eq!(args.exclude_tools.as_deref(), Some("bash"));
        assert!(cli(&["pi", "-a"]).trust);
        assert!(cli(&["pi", "--approve"]).trust);
        assert!(cli(&["pi", "--no-approve"]).no_trust);
        assert!(cli(&["pi", "inspect", "-na"]).no_trust);
    }

    #[test]
    fn last_project_approval_flag_wins() {
        for positive in ["-a", "--approve", "--trust"] {
            for negative in ["-na", "--no-approve", "--no-trust"] {
                let denied = cli(&["pi", positive, negative]);
                assert!(!denied.trust && denied.no_trust);
                let allowed = cli(&["pi", negative, positive]);
                assert!(allowed.trust && !allowed.no_trust);
            }
        }
        let args = cli(&["pi", "--approve", "--", "--no-approve"]);
        assert!(args.trust && !args.no_trust);
        assert_eq!(args.prompt, ["--no-approve"]);
    }

    #[test]
    fn repeated_pi_scalar_options_keep_the_last_value() {
        let args = cli(&[
            "pi",
            "--provider",
            "first",
            "--provider",
            "last",
            "--model",
            "first-model",
            "--model",
            "last-model",
            "--api-key",
            "first-key",
            "--api-key",
            "last-key",
            "--models",
            "first",
            "--models",
            "second",
            "--session-dir",
            "first-dir",
            "--session-dir",
            "last-dir",
            "--use-theme",
            "first-theme",
            "--use-theme",
            "last-theme",
            "--system-prompt",
            "first-prompt",
            "--system-prompt",
            "last-prompt",
            "-n",
            "first",
            "--name",
            "last",
            "-xt",
            "read",
            "--exclude-tools",
            "bash",
            "--skill",
            "one",
            "--skill",
            "two",
            "--",
            "--model",
            "literal",
        ]);
        assert_eq!(args.provider_argument.as_deref(), Some("last"));
        assert_eq!(args.model, "last-model");
        assert_eq!(args.api_key.as_deref(), Some("last-key"));
        assert_eq!(args.models.as_deref(), Some("second"));
        assert_eq!(
            args.session_dir.as_deref(),
            Some(std::path::Path::new("last-dir"))
        );
        assert_eq!(args.use_theme.as_deref(), Some("last-theme"));
        assert_eq!(
            args.system_prompt.as_deref(),
            Some(std::path::Path::new("last-prompt"))
        );
        assert_eq!(args.name.as_deref(), Some("last"));
        assert_eq!(args.exclude_tools.as_deref(), Some("bash"));
        assert_eq!(args.skill, ["one", "two"]);
        assert_eq!(args.prompt, ["--model", "literal"]);
    }

    #[test]
    fn repeated_modes_and_session_references_keep_the_last_value() {
        let args = cli(&[
            "pi",
            "--mode",
            "text",
            "--mode",
            "json",
            "--tui-mode",
            "regular",
            "--tui-mode",
            "fullscreen",
            "--session",
            "first",
            "--session",
            "last",
            "--",
            "--mode",
            "rpc",
        ]);
        assert!(matches!(args.mode, Some(super::super::OutputMode::Json)));
        assert!(matches!(
            args.tui_mode,
            Some(super::super::TuiMode::Fullscreen)
        ));
        assert_eq!(args.session, std::path::Path::new("last"));
        assert_eq!(args.prompt, ["--mode", "rpc"]);
        let args = cli(&["pi", "--session-id", "first", "--session-id", "last"]);
        assert_eq!(args.session_id.as_deref(), Some("last"));
        let args = cli(&["pi", "--fork", "first", "--fork", "last"]);
        assert_eq!(
            args.fork_session.as_deref(),
            Some(std::path::Path::new("last"))
        );
        for args in [
            &["pi", "--mode", "bogus", "--mode", "json"][..],
            &["pi", "--tui-mode", "bogus", "--tui-mode", "regular"][..],
            &["pi", "--session-id", "invalid/id", "--session-id", "valid"][..],
            &[
                "pi",
                "--session",
                "one",
                "--session",
                "two",
                "--session-id",
                "id",
            ][..],
        ] {
            assert!(parse(args.iter().map(OsString::from)).is_err(), "{args:?}");
        }
    }

    #[test]
    fn list_models_uses_the_last_optional_search_and_leaves_attachments() {
        for (tokens, search, prompt) in [
            (
                &["--list-models", "first", "--list-models", "last"][..],
                "last",
                &[][..],
            ),
            (
                &["--list-models", "first", "--list-models", "@file"][..],
                "",
                &["@file"][..],
            ),
            (
                &["--list-models", "-", "@file"][..],
                "",
                &["-", "@file"][..],
            ),
            (&["--list-models=@literal"][..], "@literal", &[][..]),
        ] {
            let argv = ["pi"]
                .into_iter()
                .chain(tokens.iter().copied())
                .collect::<Vec<_>>();
            let args = cli(&argv);
            assert_eq!(args.list_models.as_deref(), Some(search), "{argv:?}");
            assert_eq!(args.prompt, prompt, "{argv:?}");
        }
    }

    #[test]
    fn export_projection_preserves_an_unfiltered_model_listing() {
        let argv = [
            "pi",
            "--list-models",
            "@file",
            "search",
            "--export",
            "input",
            "output",
        ]
        .map(OsString::from);
        let plan = export_plan(&argv).unwrap();
        assert_eq!(plan.output.unwrap(), std::path::Path::new("search"));
        let (matches, _) = parse(plan.validation_args).unwrap();
        let args = super::super::Cli::from_arg_matches(&matches).unwrap();
        assert_eq!(args.list_models.as_deref(), Some(""));
    }

    #[test]
    fn repeated_tool_selections_keep_the_last_value() {
        for arguments in [
            &["pi", "--tools", "read", "--tools", "bash"][..],
            &["pi", "-t", "read", "--tools", "bash"][..],
            &["pi", "--tools=read", "-t", "bash"][..],
        ] {
            let args = cli(arguments);
            assert_eq!(args.tools.as_deref(), Some("bash"));
        }
        for invalid in ["read,+bash", "+b*"] {
            assert!(parse(["pi", "--tools", invalid, "-t", "bash"].map(OsString::from)).is_err());
        }
    }

    #[test]
    fn resume_does_not_consume_a_following_message() {
        for flag in ["--resume", "-r"] {
            let args = cli(&["pi", flag, "hello", "--no-mcp"]);
            assert_eq!(
                args.resume_session.as_deref(),
                Some(std::ffi::OsStr::new(""))
            );
            assert_eq!(args.prompt, ["hello"]);
            assert!(args.no_mcp);
        }
        let args = cli(&["pi", "--resume=old.jsonl", "hello"]);
        assert_eq!(
            args.resume_session.as_deref(),
            Some(std::ffi::OsStr::new("old.jsonl"))
        );
        assert_eq!(args.prompt, ["hello"]);
    }

    #[test]
    fn alias_like_values_and_prompt_tokens_remain_literal() {
        let args = cli(&["pi", "-pt", "-nt", "-ns", "prompt", "--", "-na"]);
        assert_eq!(args.tools.as_deref(), Some("-nt"));
        assert!(!args.no_tools && !args.no_trust);
        assert!(args.no_skills && args.print);
        assert_eq!(args.prompt, ["prompt", "-na"]);
        let args = cli(&["pi", "--tools=-nt", "--", "-ns"]);
        assert_eq!(args.tools.as_deref(), Some("-nt"));
        assert_eq!(args.prompt, ["-ns"]);
        assert!(!args.no_skills);
    }

    #[test]
    fn package_update_and_global_extension_flags_do_not_share_an_argument() {
        use super::super::{Command, packages::PackageCommand};

        let args = cli(&["pi", "-e", "local-extension", "update", "--extensions"]);
        assert_eq!(args.extensions, ["local-extension"]);
        assert!(matches!(
            args.command,
            Some(Command::Package(PackageCommand::Update {
                source: None,
                update_extensions: true,
            }))
        ));
    }

    #[test]
    fn print_flag_preserves_an_explicit_extension_subcommand() {
        let args = cli(&["pi", "--print", "extension", "example", "run"]);
        assert!(args.print);
        assert!(
            matches!(args.command, Some(super::super::Command::Extension { extension_id, command, .. }) if extension_id == "example" && command == "run")
        );
    }

    #[test]
    fn extension_flags_are_separate_from_native_options_and_prompt_text() {
        let (matches, flags) = parse(
            [
                "pi",
                "--native-plan",
                "--native-target",
                "example",
                "-ns",
                "--print",
                "draft",
                "--",
                "--leave-literal",
            ]
            .into_iter()
            .map(OsString::from),
        )
        .unwrap();
        let args = super::super::Cli::from_arg_matches(&matches).unwrap();
        assert!(args.no_skills && args.print);
        assert_eq!(args.prompt, ["draft", "--leave-literal"]);
        assert_eq!(flags["native-plan"], ExtensionFlagValue::Boolean(true));
        assert_eq!(
            flags["native-target"],
            ExtensionFlagValue::String("example".into())
        );

        let (_, flags) = parse(
            ["pi", "--native-target=--literal", "serve"]
                .into_iter()
                .map(OsString::from),
        )
        .unwrap();
        assert_eq!(
            flags["native-target"],
            ExtensionFlagValue::String("--literal".into())
        );
        assert!(
            parse(
                ["pi", "auth", "check", "--unknown"]
                    .into_iter()
                    .map(OsString::from)
            )
            .is_err()
        );
    }

    #[test]
    fn flags_can_follow_prompt_tokens_before_the_delimiter() {
        let (matches, flags) = parse(
            [
                "pi",
                "first",
                "-ns",
                "second",
                "--native-target",
                "run",
                "--print",
                "---third",
                "--",
                "--no-tools",
            ]
            .into_iter()
            .map(OsString::from),
        )
        .unwrap();
        let args = super::super::Cli::from_arg_matches(&matches).unwrap();
        assert!(args.no_skills && args.print && !args.no_tools);
        assert_eq!(args.prompt, ["first", "second", "---third", "--no-tools"]);
        assert_eq!(
            flags["native-target"],
            ExtensionFlagValue::String("run".into())
        );
        assert_eq!(cli(&["pi", "-p", "inspect"]).prompt, ["inspect"]);
    }

    #[test]
    fn both_version_flags_print_the_version() {
        for flag in ["-v", "-V", "--version"] {
            let result = parse(["pi", flag].into_iter().map(OsString::from));
            assert!(
                matches!(result, Err(error) if error.kind() == clap::error::ErrorKind::DisplayVersion)
            );
        }
    }

    #[test]
    fn repeated_scalar_options_use_the_last_value_and_lists_accumulate() {
        let args = cli(&[
            "pi",
            "--provider",
            "first",
            "--provider",
            "second",
            "--model",
            "old",
            "--model=new",
            "--mode",
            "text",
            "--mode",
            "json",
            "--name",
            "old name",
            "-n",
            "new name",
            "--append-system-prompt",
            "one",
            "--append-system-prompt",
            "two",
            "--skill",
            "first-skill",
            "--skill",
            "second-skill",
            "hello",
        ]);
        assert_eq!(args.provider_argument.as_deref(), Some("second"));
        assert_eq!(args.model, "new");
        assert!(matches!(args.mode, Some(super::super::OutputMode::Json)));
        assert_eq!(args.name.as_deref(), Some("new name"));
        assert_eq!(args.append_system_prompt, ["one", "two"]);
        assert_eq!(args.skill, ["first-skill", "second-skill"]);
        assert_eq!(args.prompt, ["hello"]);
    }
}
