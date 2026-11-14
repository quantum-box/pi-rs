//! Validated file URL conversion for native local and injected tools.
use std::path::PathBuf;

/// Match Node's path validation before handing a path to a local or remote backend.
pub fn to_file_path(input: &str) -> Result<PathBuf, String> {
    let parsed = url::Url::parse(input);
    #[cfg(windows)]
    let (url, fallback_host) = match parsed {
        Ok(url) => (url, None),
        Err(error) => match malformed_punycode_url(input) {
            Some((url, host)) => (url, Some(host)),
            None => return Err(error.to_string()),
        },
    };
    #[cfg(not(windows))]
    let url = parsed.map_err(|error| error.to_string())?;
    #[cfg(not(windows))]
    if url
        .host_str()
        .is_some_and(|host| !host.is_empty() && host != "localhost")
    {
        return Err("invalid file URL".into());
    }
    validate_path(url.path(), cfg!(windows))?;
    let path = url
        .to_file_path()
        .map_err(|_| "invalid file URL".to_owned())?;
    #[cfg(windows)]
    if let Some(url::Host::Domain(host)) = url.host() {
        let text = path.to_str().ok_or_else(|| "invalid file URL".to_owned())?;
        if let Some(decoded) = fallback_host {
            return Ok(PathBuf::from(replace_unc_host(text, host, &decoded)?));
        }
        return Ok(PathBuf::from(unicode_unc_path(text, host)?));
    }
    Ok(path)
}

// Node accepts some malformed ACE labels literally. Only retry simple ASCII
// authorities, so the regular URL parser still handles all structural syntax.
#[cfg(any(windows, test))]
fn malformed_punycode_url(input: &str) -> Option<(url::Url, String)> {
    let prefix = input.get(..7)?;
    if !prefix.eq_ignore_ascii_case("file://") {
        return None;
    }
    let rest = &input[7..];
    let end = rest.find(['/', '\\', '?', '#']).unwrap_or(rest.len());
    let host = &rest[..end];
    if host.is_empty()
        || !host
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-._".contains(&b))
    {
        return None;
    }
    let host = host.to_ascii_lowercase();
    let mut invalid_ace = false;
    let decoded = host
        .split('.')
        .map(|label| {
            let (decoded, status) = idna::domain_to_unicode(label);
            if status.is_err() {
                if !label.starts_with("xn--") {
                    return None;
                }
                invalid_ace = true;
                Some(label.to_owned())
            } else {
                Some(decoded)
            }
        })
        .collect::<Option<Vec<_>>>()?
        .join(".");
    if !invalid_ace {
        return None;
    }
    let replacement = format!("file://pi-invalid-host.invalid{}", &rest[end..]);
    Some((url::Url::parse(&replacement).ok()?, decoded))
}

/// Node checks encoded separators before decoding the path as UTF-8.
pub fn validate_path(path: &str, windows: bool) -> Result<(), String> {
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
    for (index, &character) in input.iter().enumerate() {
        if character == b'%'
            && let Some(byte) = escape(index)
            && (byte == b'/' || (windows && byte == b'\\'))
        {
            return Err(if windows {
                "File URL path must not include encoded \\ or / characters"
            } else {
                "File URL path must not include encoded / characters"
            }
            .into());
        }
    }
    let mut decoded = Vec::with_capacity(input.len());
    let mut index = 0;
    while index < input.len() {
        if input[index] == b'%' {
            decoded.push(escape(index).ok_or_else(|| "URI malformed".to_owned())?);
            index += 3;
        } else {
            decoded.push(input[index]);
            index += 1;
        }
    }
    std::str::from_utf8(&decoded).map_err(|_| "URI malformed".to_owned())?;
    Ok(())
}

// url::Url produces an ASCII/punycode UNC host; Node converts it back to
// Unicode. Replace only the validated host prefix, leaving the path intact.
#[cfg(any(windows, test))]
fn unicode_unc_path(path: &str, host: &str) -> Result<String, String> {
    let (decoded, status) = idna::domain_to_unicode(host);
    status.map_err(|_| "invalid file URL".to_owned())?;
    replace_unc_host(path, host, &decoded)
}

#[cfg(any(windows, test))]
fn replace_unc_host(path: &str, host: &str, decoded: &str) -> Result<String, String> {
    let prefix = format!("\\\\{host}");
    let suffix = path
        .strip_prefix(&prefix)
        .ok_or_else(|| "invalid file URL".to_owned())?;
    if !suffix.is_empty() && !suffix.starts_with('\\') {
        return Err("invalid file URL".into());
    }
    Ok(format!("\\\\{decoded}{suffix}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn validates_separator_precedence_and_utf8() {
        for path in ["/a%2fb", "/a%zz%2f"] {
            assert!(validate_path(path, false).unwrap_err().contains("encoded"));
        }
        for path in ["/a%zz", "/a%ff", "/a%ed%a0%80"] {
            assert_eq!(validate_path(path, false).unwrap_err(), "URI malformed");
        }
        assert!(validate_path("/a%5cb", true).is_err());
        assert!(validate_path("/a%5cb", false).is_ok());
        assert!(validate_path("/a%252fb/%F0%9F%98%80", true).is_ok());
    }
    #[test]
    fn converts_double_encoded_path_and_ignores_query_and_fragment() {
        let root = std::env::temp_dir();
        let base = url::Url::from_directory_path(&root).unwrap();
        let input = format!("{base}literal%252F%F0%9F%98%80?ignored=%2f#%ff");
        assert_eq!(to_file_path(&input).unwrap(), root.join("literal%2F😀"));
    }
    #[test]
    fn decodes_only_the_unc_host() {
        assert_eq!(
            unicode_unc_path(r"\\xn--n3h\share\xn--n3h", "xn--n3h").unwrap(),
            r"\\☃\share\xn--n3h"
        );
        assert_eq!(
            unicode_unc_path(
                r"\\xn--bcher-kva.example\share\a b",
                "xn--bcher-kva.example"
            )
            .unwrap(),
            r"\\bücher.example\share\a b"
        );
        assert!(unicode_unc_path(r"\\other\share", "xn--n3h").is_err());
    }
    #[test]
    fn malformed_ace_fallback_is_limited_to_safe_authorities() {
        for (input, expected) in [
            ("file://XN--BAD-/share", "xn--bad-"),
            ("file://xn--bad-.xn--n3h/share", "xn--bad-.☃"),
            ("file://xn--n3h.xn--bad-/share", "☃.xn--bad-"),
        ] {
            let (url, host) = malformed_punycode_url(input).unwrap();
            assert_eq!(host, expected);
            assert_eq!(url.path(), "/share");
        }
        for input in [
            "file://user@xn--bad-/share",
            "file://xn--bad-:99/share",
            "file://xn--bad-%2f/share",
            "file://xn--n3h/share",
            "https://xn--bad-/share",
        ] {
            assert!(malformed_punycode_url(input).is_none(), "{input}");
        }
    }
    #[cfg(windows)]
    #[test]
    fn windows_file_urls_keep_unicode_unc_and_drive_paths() {
        assert_eq!(
            to_file_path("file://xn--n3h/share/a%20b").unwrap(),
            PathBuf::from(r"\\☃\share\a b")
        );
        assert_eq!(
            to_file_path("file:///C:/hello").unwrap(),
            PathBuf::from(r"C:\hello")
        );
        assert!(to_file_path("file://xn--n3h/share/a%2fb").is_err());
        assert_eq!(
            to_file_path("file://xn--bad-.xn--n3h/share/a%20b").unwrap(),
            PathBuf::from(r"\\xn--bad-.☃\share\a b")
        );
        assert!(to_file_path("file://xn--bad-/share/a%2fb").is_err());
    }
}
