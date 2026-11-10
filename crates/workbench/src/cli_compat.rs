//! Pi's multi-character flags, translated before Clap handles native arguments.
use std::ffi::OsString;

use clap::{ArgMatches, Command, CommandFactory};

pub(super) fn parse(args: impl IntoIterator<Item = OsString>) -> Result<ArgMatches, clap::Error> {
    let mut command = super::Cli::command();
    command.build();
    let mut args: Vec<_> = args.into_iter().collect();
    normalize(&command, &mut args);
    command.try_get_matches_from(args)
}

fn normalize(root: &Command, args: &mut [OsString]) {
    let mut command = root;
    let mut remaining = 0;
    let mut hyphen_values = false;
    for arg in args.iter_mut().skip(1) {
        if arg == "--" {
            break;
        }
        let option = arg.as_encoded_bytes().starts_with(b"-") && arg != "-";
        if remaining > 0 && (!option || hyphen_values) {
            remaining -= 1;
            continue;
        }
        remaining = 0;
        if !option {
            if let Some(subcommand) = command.find_subcommand(&*arg) {
                command = subcommand;
            } else if std::ptr::eq(command, root) {
                // The first free positional starts Clap's trailing prompt.
                break;
            }
            continue;
        }
        let Some(value) = arg.to_str() else { continue };
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
        (remaining, hyphen_values) = option_values(command, canonical);
        if canonical != value {
            *arg = canonical.into();
        }
    }
}

fn option_values(command: &Command, option: &str) -> (usize, bool) {
    let argument = if let Some(long) = option.strip_prefix("--") {
        let (name, attached) = long
            .split_once('=')
            .map_or((long, false), |(name, _)| (name, true));
        command
            .get_arguments()
            .find(|arg| {
                arg.get_long() == Some(name)
                    || arg
                        .get_all_aliases()
                        .is_some_and(|aliases| aliases.contains(&name))
            })
            .map(|arg| (arg, attached))
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
        let matches = parse(args.iter().map(OsString::from)).unwrap();
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
    fn both_version_flags_print_the_version() {
        for flag in ["-v", "-V", "--version"] {
            let result = parse(["pi", flag].into_iter().map(OsString::from));
            assert!(
                matches!(result, Err(error) if error.kind() == clap::error::ErrorKind::DisplayVersion)
            );
        }
    }
}
