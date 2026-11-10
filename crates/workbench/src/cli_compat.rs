//! Pi's multi-character flags, translated before Clap handles native arguments.
use std::collections::BTreeMap;
use std::ffi::OsString;

use clap::{Arg, ArgMatches, Command, CommandFactory};
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
    let mut flags = BTreeMap::new();
    let mut command = root;
    let mut remaining = 0;
    let mut hyphen_values = false;
    while let Some(arg) = input.next() {
        if arg == "--" {
            output.push(arg);
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
            if let Some(subcommand) = command.find_subcommand(&arg) {
                command = subcommand;
            } else if std::ptr::eq(command, root) {
                // The first free positional starts Clap's trailing prompt.
                output.push(arg);
                output.extend(input);
                break;
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
                        && command.find_subcommand(next).is_none()
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
        output.push(if canonical == value {
            arg
        } else {
            canonical.into()
        });
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
        let count = arg.get_num_args().map_or(0, |range| range.max_values());
        (
            count.saturating_sub(usize::from(attached)),
            arg.is_allow_hyphen_values_set(),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::FromArgMatches;

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
    fn alias_like_values_and_prompt_tokens_remain_literal() {
        let args = cli(&["pi", "-pt", "-nt", "-ns", "prompt", "-na"]);
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
    fn both_version_flags_print_the_version() {
        for flag in ["-v", "-V", "--version"] {
            let result = parse(["pi", flag].into_iter().map(OsString::from));
            assert!(
                matches!(result, Err(error) if error.kind() == clap::error::ErrorKind::DisplayVersion)
            );
        }
    }
}
