//! Extract Pi's process-wide profile option before any configuration is read.
use std::ffi::OsString;
use std::path::{Component, Path, PathBuf};

pub(super) fn take(argv: Vec<OsString>) -> Result<(Vec<OsString>, Option<PathBuf>), String> {
    let mut input = argv.into_iter();
    let mut rest = Vec::new();
    if let Some(program) = input.next() {
        rest.push(program);
    }
    let mut selected = None;
    while let Some(arg) = input.next() {
        if arg == "--" {
            rest.push(arg);
            rest.extend(input);
            break;
        }
        if arg == "--agent-dir" {
            let value = input
                .next()
                .filter(|value| !value.is_empty())
                .ok_or("--agent-dir requires a directory")?;
            selected = Some(resolve(Path::new(&value)).map_err(|_| "cannot resolve --agent-dir")?);
        } else if arg
            .to_str()
            .is_some_and(|value| value.starts_with("--agent-dir="))
        {
            return Err("--agent-dir requires a separate directory argument".into());
        } else {
            rest.push(arg);
        }
    }
    Ok((rest, selected))
}

pub(super) fn resolve(path: &Path) -> std::io::Result<PathBuf> {
    let expanded = if let Some(value) = path.to_str() {
        pi_rs_workbench::normalize_path(value).map_err(|error| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, error.to_string())
        })?
    } else {
        path.to_owned()
    };
    let absolute = std::path::absolute(expanded)?;
    let mut normalized = PathBuf::new();
    for component in absolute.components() {
        match component {
            Component::ParentDir => {
                normalized.pop();
            }
            Component::CurDir => {}
            other => normalized.push(other.as_os_str()),
        }
    }
    Ok(normalized)
}

pub(super) fn absolute_literal(path: &Path) -> std::io::Result<PathBuf> {
    std::path::absolute(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn takes_last_profile_before_delimiter() {
        let args = [
            "pi",
            "--agent-dir",
            "first",
            "auth",
            "--agent-dir",
            "a/../last",
            "--",
            "--agent-dir",
            "message",
        ]
        .map(OsString::from)
        .to_vec();
        let (args, profile) = take(args).unwrap();
        assert_eq!(
            args,
            ["pi", "auth", "--", "--agent-dir", "message"].map(OsString::from)
        );
        assert_eq!(profile, Some(resolve(Path::new("last")).unwrap()));
        assert!(take(["pi", "--agent-dir"].map(OsString::from).to_vec()).is_err());
        assert!(take(["pi", "--agent-dir", ""].map(OsString::from).to_vec()).is_err());
        assert!(take(["pi", "--agent-dir=other"].map(OsString::from).to_vec()).is_err());
    }
}
