//! Protected identities for effective model configuration.
use hmac::{Hmac, Mac};
use serde::Serialize;
use sha2::Sha256;

/// Returns None if a protected persistent key cannot be established.
pub fn protected_model_fingerprint<T: Serialize>(value: &T) -> Option<String> {
    let key = protected_key()?;
    fingerprint_with_key(&key, value)
}

fn fingerprint_with_key<T: Serialize>(key: &[u8; 32], value: &T) -> Option<String> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).ok()?;
    mac.update(&serde_json::to_vec(value).ok()?);
    Some(format!("{:x}", mac.finalize().into_bytes()))
}

#[cfg(unix)]
fn protected_key() -> Option<[u8; 32]> {
    load_or_create_key(&key_path())
}

#[cfg(windows)]
fn protected_key() -> Option<[u8; 32]> {
    load_or_create_windows_key(&key_path())
}

#[cfg(not(any(unix, windows)))]
fn protected_key() -> Option<[u8; 32]> {
    // Do not persist a key without a verified owner-only storage mechanism.
    None
}

#[cfg(all(any(unix, windows), not(test)))]
fn key_path() -> std::path::PathBuf {
    std::env::var_os("PI_RS_HOME")
        .or_else(|| std::env::var_os("PI_CODING_AGENT_DIR"))
        .map(std::path::PathBuf::from)
        .or_else(|| {
            std::env::var_os("HOME")
                .or_else(|| std::env::var_os("USERPROFILE"))
                .map(|home| std::path::PathBuf::from(home).join(".pi-rs"))
        })
        .unwrap_or_else(|| std::env::temp_dir().join("pi-rs"))
        .join("context-fingerprint.key")
}

#[cfg(all(any(unix, windows), test))]
fn key_path() -> std::path::PathBuf {
    use std::sync::OnceLock;
    static DIRECTORY: OnceLock<tempfile::TempDir> = OnceLock::new();
    DIRECTORY
        .get_or_init(|| tempfile::tempdir().unwrap())
        .path()
        .join("context-fingerprint.key")
}

#[cfg(unix)]
fn load_or_create_key(path: &std::path::Path) -> Option<[u8; 32]> {
    use std::io::{Read, Write};
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
    let parent = path.parent()?;
    std::fs::create_dir_all(parent).ok()?;
    let open_existing = || {
        std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(path)
    };
    let mut file = match open_existing() {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let mut file = match std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .custom_flags(libc::O_NOFOLLOW)
                .open(path)
            {
                Ok(file) => file,
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    open_existing().ok()?
                }
                Err(_) => return None,
            };
            if file.metadata().ok()?.len() == 0 {
                let mut key = [0_u8; 32];
                key[..16].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
                key[16..].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
                file.write_all(&key).ok()?;
                file.sync_all().ok()?;
                file = open_existing().ok()?;
            }
            file
        }
        Err(_) => return None,
    };
    let metadata = file.metadata().ok()?;
    // SAFETY: geteuid takes no arguments and has no memory-safety preconditions.
    let effective_uid = unsafe { libc::geteuid() };
    if !metadata.is_file()
        || metadata.uid() != effective_uid
        || metadata.nlink() != 1
        || metadata.mode() & 0o077 != 0
        || metadata.len() != 32
    {
        return None;
    }
    let mut key = [0_u8; 32];
    file.read_exact(&mut key).ok()?;
    Some(key)
}

#[cfg(windows)]
fn dpapi_transform(input: &[u8], protect: bool) -> Option<Vec<u8>> {
    use windows_sys::Win32::Foundation::LocalFree;
    use windows_sys::Win32::Security::Cryptography::{
        CRYPT_INTEGER_BLOB, CRYPTPROTECT_UI_FORBIDDEN, CryptProtectData, CryptUnprotectData,
    };
    let input = CRYPT_INTEGER_BLOB {
        cbData: u32::try_from(input.len()).ok()?,
        pbData: input.as_ptr() as *mut u8,
    };
    let mut output = CRYPT_INTEGER_BLOB::default();
    // SAFETY: input points to live bytes and output is a valid out parameter;
    // optional pointers are null, and Windows allocates output for LocalFree.
    let ok = unsafe {
        if protect {
            CryptProtectData(
                &input,
                std::ptr::null(),
                std::ptr::null(),
                std::ptr::null(),
                std::ptr::null(),
                CRYPTPROTECT_UI_FORBIDDEN,
                &mut output,
            )
        } else {
            CryptUnprotectData(
                &input,
                std::ptr::null_mut(),
                std::ptr::null(),
                std::ptr::null(),
                std::ptr::null(),
                CRYPTPROTECT_UI_FORBIDDEN,
                &mut output,
            )
        }
    };
    if ok == 0 || output.pbData.is_null() {
        return None;
    }
    // SAFETY: successful DPAPI calls return cbData initialized bytes.
    let bytes =
        unsafe { std::slice::from_raw_parts(output.pbData, output.cbData as usize).to_vec() };
    // SAFETY: the output buffer is writable and must be released with LocalFree.
    unsafe {
        for index in 0..output.cbData as usize {
            std::ptr::write_volatile(output.pbData.add(index), 0);
        }
        LocalFree(output.pbData.cast());
    }
    Some(bytes)
}

#[cfg(windows)]
fn load_or_create_windows_key(path: &std::path::Path) -> Option<[u8; 32]> {
    use std::io::{Read, Write};
    std::fs::create_dir_all(path.parent()?).ok()?;
    let read_key = || {
        let file = std::fs::File::open(path).ok()?;
        if !file.metadata().ok()?.is_file() {
            return None;
        }
        let mut encrypted = Vec::new();
        file.take(65537).read_to_end(&mut encrypted).ok()?;
        if encrypted.len() > 65536 {
            return None;
        }
        dpapi_transform(&encrypted, false)?.try_into().ok()
    };
    if path.exists() {
        return read_key();
    }
    let mut key = [0_u8; 32];
    key[..16].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
    key[16..].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
    let encrypted = dpapi_transform(&key, true)?;
    match std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
    {
        Ok(mut file) => {
            file.write_all(&encrypted).ok()?;
            file.sync_all().ok()?;
            Some(key)
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => read_key(),
        Err(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fingerprints_are_keyed_and_do_not_expose_values() {
        let first = fingerprint_with_key(&[1; 32], &("x-routing", "low-entropy-secret")).unwrap();
        let repeated =
            fingerprint_with_key(&[1; 32], &("x-routing", "low-entropy-secret")).unwrap();
        let other_key =
            fingerprint_with_key(&[2; 32], &("x-routing", "low-entropy-secret")).unwrap();
        let other_value = fingerprint_with_key(&[1; 32], &("x-routing", "other")).unwrap();
        assert_eq!(first, repeated);
        assert_ne!(first, other_key);
        assert_ne!(first, other_value);
        assert!(!first.contains("low-entropy-secret"));
    }

    #[cfg(windows)]
    #[test]
    fn dpapi_key_persists_without_plaintext_and_rejects_tampering() {
        if let Some(path) = std::env::var_os("PI_RS_DPAPI_TEST_CHILD") {
            let key = load_or_create_windows_key(std::path::Path::new(&path)).unwrap();
            assert_eq!(
                fingerprint_with_key(&key, &"fixture").unwrap(),
                std::env::var("PI_RS_DPAPI_TEST_EXPECTED").unwrap()
            );
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("key");
        let key = load_or_create_windows_key(&path).unwrap();
        assert_eq!(load_or_create_windows_key(&path), Some(key));
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "context_fingerprint::tests::dpapi_key_persists_without_plaintext_and_rejects_tampering"])
            .env("PI_RS_DPAPI_TEST_CHILD", &path)
            .env("PI_RS_DPAPI_TEST_EXPECTED", fingerprint_with_key(&key, &"fixture").unwrap())
            .status().unwrap();
        assert!(status.success());
        let first = protected_model_fingerprint(&("authorization", "pin"));
        assert!(first.is_some());
        assert_eq!(
            first,
            protected_model_fingerprint(&("authorization", "pin"))
        );
        let encrypted = std::fs::read(&path).unwrap();
        assert!(!encrypted.windows(key.len()).any(|window| window == key));
        std::fs::write(&path, b"tampered").unwrap();
        assert_eq!(load_or_create_windows_key(&path), None);
    }

    #[cfg(unix)]
    #[test]
    fn key_persists_and_rejects_insecure_storage() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("key");
        let key = load_or_create_key(&path).unwrap();
        assert_eq!(load_or_create_key(&path), Some(key));
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert_eq!(load_or_create_key(&path), None);
        let link = dir.path().join("link");
        std::os::unix::fs::symlink(&path, &link).unwrap();
        assert_eq!(load_or_create_key(&link), None);
    }
}
