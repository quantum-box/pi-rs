//! Path input normalization used by Pi's local file tools.
use std::path::{Component, Path, PathBuf};

use pi_rs::{Error, Result};
use unicode_normalization::UnicodeNormalization;

pub(super) fn normalize(value: &str) -> Result<PathBuf> {
    input(value, home().as_deref().map(Path::new))
}

/// Normalize a filesystem path without rewriting its spaces or stripping a leading `@`.
pub fn normalize_plain(value: &str) -> Result<PathBuf> {
    plain_input(value, home().as_deref().map(Path::new))
}

fn home() -> Option<std::ffi::OsString> {
    #[cfg(windows)]
    let home = std::env::var_os("USERPROFILE").or_else(|| std::env::var_os("HOME"));
    #[cfg(not(windows))]
    let home = std::env::var_os("HOME").or_else(|| std::env::var_os("USERPROFILE"));
    home
}

pub(super) fn input(value: &str, home: Option<&Path>) -> Result<PathBuf> {
    input_with_policy(value, home, false)
}

pub(super) fn remote_input(value: &str, home: Option<&Path>) -> Result<PathBuf> {
    input_with_policy(value, home, true)
}

fn input_with_policy(value: &str, home: Option<&Path>, remote: bool) -> Result<PathBuf> {
    let value: String = value
        .chars()
        .map(|ch| match ch {
            '\u{a0}' | '\u{2000}'..='\u{200a}' | '\u{202f}' | '\u{205f}' | '\u{3000}' => ' ',
            ch => ch,
        })
        .collect();
    plain_input_with_policy(value.strip_prefix('@').unwrap_or(&value), home, remote)
}

fn plain_input(value: &str, home: Option<&Path>) -> Result<PathBuf> {
    plain_input_with_policy(value, home, false)
}

fn plain_input_with_policy(value: &str, home: Option<&Path>, remote: bool) -> Result<PathBuf> {
    #[cfg(windows)]
    let windows_value = windows_shell_path(value);
    #[cfg(windows)]
    let value = windows_value.as_str();
    if value == "~" || value.starts_with("~/") || (cfg!(windows) && value.starts_with("~\\")) {
        let home = home.ok_or_else(|| Error::Config("home directory is unavailable".into()))?;
        return Ok(if value == "~" {
            home.to_path_buf()
        } else {
            let tail = Path::new(&value[2..]);
            if remote
                && (tail.has_root()
                    || matches!(tail.components().next(), Some(Component::Prefix(_))))
            {
                return Err(Error::Config("invalid backend home-relative path".into()));
            }
            home.join(tail)
        });
    }
    if value.starts_with("file://") {
        let url = reqwest::Url::parse(value).map_err(|error| Error::Config(error.to_string()))?;
        validate_file_url_path(url.path(), cfg!(windows))?;
        return url
            .to_file_path()
            .map_err(|_| Error::Config("invalid file URL".into()));
    }
    Ok(PathBuf::from(value))
}

// Node's fileURLToPath rejects encoded separators before decoding, and its
// decodeURIComponent rejects malformed escapes and invalid UTF-8. Checking the
// URL path rather than the whole URL leaves query and fragment semantics intact.
fn validate_file_url_path(path: &str, windows: bool) -> Result<()> {
    let input = path.as_bytes();
    let escape = |index: usize| -> Option<u8> {
        let digit = |byte: u8| match byte {
            b'0'..=b'9' => Some(byte - b'0'),
            b'a'..=b'f' => Some(byte - b'a' + 10),
            b'A'..=b'F' => Some(byte - b'A' + 10),
            _ => None,
        };
        Some(digit(*input.get(index + 1)?)? * 16 + digit(*input.get(index + 2)?)?)
    };
    for index in 0..input.len() {
        if input[index] == b'%'
            && let Some(byte) = escape(index)
            && (byte == b'/' || (windows && byte == b'\\'))
        {
            let message = if windows {
                "File URL path must not include encoded \\ or / characters"
            } else {
                "File URL path must not include encoded / characters"
            };
            return Err(Error::Config(message.into()));
        }
    }
    let mut decoded = Vec::with_capacity(input.len());
    let mut index = 0;
    while index < input.len() {
        if input[index] == b'%' {
            decoded.push(escape(index).ok_or_else(|| Error::Config("URI malformed".into()))?);
            index += 3;
        } else {
            decoded.push(input[index]);
            index += 1;
        }
    }
    std::str::from_utf8(&decoded).map_err(|_| Error::Config("URI malformed".into()))?;
    Ok(())
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

pub(super) fn resolve_remote(root: &Path, input: &Path) -> Result<PathBuf> {
    if !input.is_absolute() && matches!(input.components().next(), Some(Component::Prefix(_))) {
        return Err(Error::Config(
            "drive-relative backend path is not supported".into(),
        ));
    }
    let path = resolve(root, input);
    if !path.is_absolute() {
        return Err(Error::Config("backend path must be absolute".into()));
    }
    Ok(path)
}

pub(super) fn read_candidates(path: &Path) -> Vec<PathBuf> {
    let value = path.to_string_lossy();
    static AM_PM: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    let am_pm = AM_PM.get_or_init(|| regex::Regex::new(r" (?i:(AM|PM))\.").expect("AM/PM pattern"));
    let decomposed: String = value.nfd().collect();
    let mut candidates = vec![path.to_owned()];
    for candidate in [
        am_pm.replace_all(&value, "\u{202f}$1.").into_owned(),
        decomposed.clone(),
        value.replace('\'', "\u{2019}"),
        decomposed.replace('\'', "\u{2019}"),
    ] {
        let candidate = PathBuf::from(candidate);
        if !candidates.contains(&candidate) {
            candidates.push(candidate);
        }
    }
    candidates
}

pub(super) fn read_variant(path: PathBuf) -> PathBuf {
    read_candidates(&path)
        .into_iter()
        .find(|candidate| candidate.exists())
        .unwrap_or(path)
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
    fn file_url_validation_matches_node_separator_and_utf8_rules() {
        let message = |path: &str, windows: bool| match validate_file_url_path(path, windows) {
            Err(Error::Config(message)) => message,
            result => panic!("expected invalid URL path, got {result:?}"),
        };
        for path in ["/tmp/a%2Fb", "/tmp/a%2fb", "/tmp/%zz%2F"] {
            assert_eq!(
                message(path, false),
                "File URL path must not include encoded / characters"
            );
            assert_eq!(
                message(path, true),
                "File URL path must not include encoded \\ or / characters"
            );
        }
        assert!(validate_file_url_path("/tmp/a%5Cb", false).is_ok());
        assert_eq!(
            message("/tmp/a%5Cb", true),
            "File URL path must not include encoded \\ or / characters"
        );
        for path in ["/tmp/%zz", "/tmp/%", "/tmp/%FF", "/tmp/%C3%28"] {
            assert_eq!(message(path, false), "URI malformed");
        }
        assert!(validate_file_url_path("/tmp/a%20b/%E6%97%A5", false).is_ok());
        assert!(validate_file_url_path("/tmp/a%252Fb", false).is_ok());
    }

    #[test]
    fn file_urls_reject_encoded_separator_before_filesystem_access() {
        let directory = tempfile::tempdir().unwrap();
        let nested = directory.path().join("folder").join("note.txt");
        std::fs::create_dir_all(nested.parent().unwrap()).unwrap();
        std::fs::write(&nested, "contents").unwrap();
        let file_url = reqwest::Url::from_file_path(&nested).unwrap().to_string();
        let escaped = file_url.replace("folder/note.txt", "folder%2Fnote.txt");
        assert_ne!(escaped, file_url);
        assert!(
            matches!(input(&escaped, None), Err(Error::Config(message)) if message.contains("encoded"))
        );
        assert_eq!(input(&file_url, None).unwrap(), nested);
        assert_eq!(
            input(&format!("{file_url}?ignored=%2F#%FF"), None).unwrap(),
            nested
        );
    }

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
