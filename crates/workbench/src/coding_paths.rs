//! Path input normalization used by Pi's local file tools.
use std::path::{Component, Path, PathBuf};

use pi_rs::{Error, Result};
use unicode_normalization::UnicodeNormalization;

pub(super) fn normalize(value: &str) -> Result<PathBuf> {
    #[cfg(windows)]
    let home = std::env::var_os("USERPROFILE").or_else(|| std::env::var_os("HOME"));
    #[cfg(not(windows))]
    let home = std::env::var_os("HOME").or_else(|| std::env::var_os("USERPROFILE"));
    input(value, home.as_deref().map(Path::new))
}

fn input(value: &str, home: Option<&Path>) -> Result<PathBuf> {
    let value: String = value
        .chars()
        .map(|ch| match ch {
            '\u{a0}' | '\u{2000}'..='\u{200a}' | '\u{202f}' | '\u{205f}' | '\u{3000}' => ' ',
            ch => ch,
        })
        .collect();
    let value = value.strip_prefix('@').unwrap_or(&value);
    #[cfg(windows)]
    let windows_value = windows_shell_path(value);
    #[cfg(windows)]
    let value = windows_value.as_str();
    if value == "~" || value.starts_with("~/") || (cfg!(windows) && value.starts_with("~\\")) {
        let home = home.ok_or_else(|| Error::Config("home directory is unavailable".into()))?;
        return Ok(if value == "~" {
            home.to_path_buf()
        } else {
            home.join(&value[2..])
        });
    }
    if value.starts_with("file://") {
        return reqwest::Url::parse(value)
            .map_err(|error| Error::Config(error.to_string()))?
            .to_file_path()
            .map_err(|_| Error::Config("invalid file URL".into()));
    }
    Ok(PathBuf::from(value))
}

pub(super) fn resolve(root: &Path, input: &Path) -> PathBuf {
    let absolute = if input.is_absolute() {
        input.to_path_buf()
    } else {
        root.join(input)
    };
    let mut resolved = PathBuf::new();
    for component in absolute.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                resolved.pop();
            }
            component => resolved.push(component.as_os_str()),
        }
    }
    resolved
}

pub(super) fn read_variant(path: PathBuf) -> PathBuf {
    if path.exists() {
        return path;
    }
    let value = path.to_string_lossy();
    static AM_PM: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    let am_pm = AM_PM.get_or_init(|| regex::Regex::new(r" (?i:(AM|PM))\.").expect("AM/PM pattern"));
    let decomposed: String = value.nfd().collect();
    for candidate in [
        am_pm.replace_all(&value, "\u{202f}$1.").into_owned(),
        decomposed.clone(),
        value.replace('\'', "\u{2019}"),
        decomposed.replace('\'', "\u{2019}"),
    ] {
        let candidate = PathBuf::from(candidate);
        if candidate.exists() {
            return candidate;
        }
    }
    path
}

#[cfg(any(windows, test))]
fn windows_shell_path(value: &str) -> String {
    if !value.starts_with('/') || value.starts_with("//") || value.contains('\\') {
        return value.into();
    }
    let drive = value
        .strip_prefix("/mnt/")
        .or_else(|| value.strip_prefix("/cygdrive/"))
        .unwrap_or(&value[1..]);
    let bytes = drive.as_bytes();
    if bytes.first().is_some_and(u8::is_ascii_alphabetic)
        && (bytes.len() == 1 || bytes.get(1) == Some(&b'/'))
    {
        return format!(
            "{}:\\{}",
            char::from(bytes[0]).to_ascii_uppercase(),
            drive.get(2..).unwrap_or("").replace('/', "\\")
        );
    }
    value.into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_mentions_spaces_home_and_file_urls_without_trimming() {
        let home = Path::new("/home/example");
        assert_eq!(
            input("@some\u{202f}file", Some(home)).unwrap(),
            Path::new("some file")
        );
        assert_eq!(
            input("@@literal", Some(home)).unwrap(),
            Path::new("@literal")
        );
        assert_eq!(
            input(" spaced ", Some(home)).unwrap(),
            Path::new(" spaced ")
        );
        assert_eq!(input("~", Some(home)).unwrap(), home);
        assert_eq!(input("~/notes", Some(home)).unwrap(), home.join("notes"));
        assert_eq!(
            input("~someone/notes", Some(home)).unwrap(),
            Path::new("~someone/notes")
        );
        let directory = tempfile::tempdir().unwrap();
        let file = directory.path().join("space name");
        let url = reqwest::Url::from_file_path(&file).unwrap();
        assert_eq!(input(url.as_str(), None).unwrap(), file);
        assert!(input("~/notes", None).is_err());
        assert!(input("file://%invalid", None).is_err());
    }

    #[test]
    fn resolves_dot_segments_before_filesystem_lookup() {
        let directory = tempfile::tempdir().unwrap();
        assert_eq!(
            resolve(directory.path(), Path::new("a/../b/./c")),
            directory.path().join("b/c")
        );
    }

    #[test]
    fn windows_shell_paths_preserve_unc_and_non_drive_paths() {
        for (input, expected) in [
            ("/c/Users/name", "C:\\Users\\name"),
            ("/mnt/d/file", "D:\\file"),
            ("/cygdrive/e/folder/file", "E:\\folder\\file"),
            ("/c", "C:\\"),
            ("//host/share", "//host/share"),
            ("/usr/share", "/usr/share"),
            ("/c/mixed\\path", "/c/mixed\\path"),
        ] {
            assert_eq!(windows_shell_path(input), expected);
        }
    }

    #[test]
    fn reader_fallbacks_preserve_exact_matches_and_find_screenshot_names() {
        let directory = tempfile::tempdir().unwrap();
        let exact = directory.path().join("Capture d'ecran.png");
        let curly = directory.path().join("Capture d’ecran.png");
        std::fs::write(&exact, "exact").unwrap();
        std::fs::write(&curly, "curly").unwrap();
        assert_eq!(read_variant(exact.clone()), exact);
        std::fs::remove_file(&exact).unwrap();
        assert_eq!(read_variant(exact), curly);
        let screenshot = directory.path().join("Screenshot 10.00.00\u{202f}PM.png");
        std::fs::write(&screenshot, "screenshot").unwrap();
        assert_eq!(
            read_variant(directory.path().join("Screenshot 10.00.00 PM.png")),
            screenshot
        );
        let decomposed = directory.path().join("Capture d’e\u{301}cran.png");
        std::fs::write(&decomposed, "decomposed").unwrap();
        let result = read_variant(directory.path().join("Capture d'écran.png"));
        assert_eq!(std::fs::read_to_string(result).unwrap(), "decomposed");
    }
}
