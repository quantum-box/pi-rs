//! Pi's multi-character flags, translated before Clap handles native arguments.
use std::collections::BTreeMap;
use std::ffi::OsString;

use clap::{Arg, ArgMatches, Command, CommandFactory, FromArgMatches};
use pi_rs::ExtensionFlagValue;

pub(super) type ExtensionFlags = BTreeMap<String, ExtensionFlagValue>;

pub(super) fn parse(
    args: impl IntoIterator<Item = OsString>,
) -> Result<(ArgMatches, ExtensionFlags), clap::Error> {
    let mut command = super::Cli::command();
    command.build();
    let (args, flags) = normalize(&command, args);
    Ok((command.try_get_matches_from(args)?, flags))
}

fn normalize(
    root: &Command,
    args: impl IntoIterator<Item = OsString>,
) -> (Vec<OsString>, ExtensionFlags) {
    let mut input = args.into_iter().peekable();
    let mut output: Vec<_> = input.next().into_iter().collect();
    let mut prompt = Vec::new();
    let mut flags = BTreeMap::new();
    let mut command = root;
    let mut remaining = 0;
    let mut hyphen_values = false;
    let mut trust_flag_index = None;
    while let Some(arg) = input.next() {
        if arg == "--" {
            output.push(arg);
            output.append(&mut prompt);
            output.extend(input);
            break;
        }
        let option = arg.as_encoded_bytes().starts_with(b"-") && arg != "-";
        if remaining > 0 && (!option || hyphen_values) {
            remaining -= 1;
            output.push(arg);
            continue;
        }
        remaining = 0;
        if !option {
            if prompt.is_empty()
                && let Some(subcommand) = command.find_subcommand(&arg)
            {
                command = subcommand;
            } else if std::ptr::eq(command, root) {
                prompt.push(arg);
                continue;
            }
            output.push(arg);
            continue;
        }
        let Some(value) = arg.to_str() else {
            output.push(arg);
            continue;
        };
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
            if long_arg(command, name).is_none()
                && flag_name(name)
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
                flags.insert(name.into(), value);
                continue;
            }
        }
        (remaining, hyphen_values) = option_values(command, canonical);
        let print = matches!(canonical, "--print" | "-p");
        if matches!(
            canonical,
            "-a" | "--approve" | "--trust" | "--no-approve" | "--no-trust"
        ) {
            if let Some(index) = trust_flag_index {
                output.remove(index);
            }
            trust_flag_index = Some(output.len());
        }
        output.push(if canonical == value {
            arg
        } else {
            canonical.into()
        });
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
    if !prompt.is_empty() {
        output.push("--".into());
        output.extend(prompt);
    }
    (output, flags)
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
    use pi_rs::Extension;

    let (matches, _) = parse(
        argv.iter()
            .filter(|argument| *argument != "--help" && *argument != "-h")
            .cloned(),
    )
    .ok()?;
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
    let resources = super::resources::Resources::load(&args).ok()?;
    let mut flags = BTreeMap::new();
    let reserved = reserved_flag_names();
    for path in resources.extensions {
        let Ok(extension) = pi_rs_extensions::ExecutableExtension::load(&path) else {
            continue;
        };
        if args.disable_extension.iter().any(|id| id == extension.id()) {
            continue;
        }
        for flag in extension.cli_flags() {
            if reserved.contains(&flag.name) {
                continue;
            }
            let usage = match flag.default {
                ExtensionFlagValue::Boolean(_) => format!("--{}", flag.name),
                ExtensionFlagValue::String(_) => format!("--{} <value>", flag.name),
            };
            let description = flag
                .description
                .unwrap_or_else(|| format!("Registered by {}", extension.id()));
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
}
