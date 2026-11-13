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
    std::fs::create_dir_all(path.parent()?).ok()?;
    let mut lock_path = path.as_os_str().to_os_string();
    lock_path.push(".lock");
    let lock = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(lock_path)
        .ok()?;
    // SAFETY: geteuid takes no arguments and has no memory-safety preconditions.
    let effective_uid = unsafe { libc::geteuid() };
    let secure = |metadata: &std::fs::Metadata| {
        metadata.is_file()
            && metadata.uid() == effective_uid
            && metadata.nlink() == 1
            && metadata.mode() & 0o077 == 0
    };
    if !secure(&lock.metadata().ok()?) {
        return None;
    }
    lock.lock().ok()?;
    let open_existing = |path: &std::path::Path| {
        std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(path)
    };
    let mut published_path = path.as_os_str().to_os_string();
    published_path.push(".v2");
    let published_path = std::path::PathBuf::from(published_path);
    let read_key = |mut file: std::fs::File| {
        let metadata = file.metadata().ok()?;
        if !secure(&metadata) || metadata.len() != 32 {
            return None;
        }
        let mut key = [0_u8; 32];
        file.read_exact(&mut key).ok()?;
        Some(key)
    };
    match open_existing(&published_path) {
        Ok(file) => return read_key(file),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
        Err(_) => return None,
    }
    let legacy = match open_existing(path) {
        Ok(file) => {
            let metadata = file.metadata().ok()?;
            if !secure(&metadata) || metadata.len() > 32 {
                return None;
            }
            if metadata.len() == 32 {
                Some(read_key(file)?)
            } else {
                None // An old, incomplete key cannot have produced a fingerprint.
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(_) => return None,
    };
    let key = legacy.unwrap_or_else(|| {
        let mut key = [0_u8; 32];
        key[..16].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
        key[16..].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
        key
    });
    let mut temp_path = path.as_os_str().to_os_string();
    temp_path.push(format!(".{}.tmp", uuid::Uuid::new_v4()));
    let temp_path = std::path::PathBuf::from(temp_path);
    struct RemoveTemp(std::path::PathBuf);
    impl Drop for RemoveTemp {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }
    let _cleanup = RemoveTemp(temp_path.clone());
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(&temp_path)
        .ok()?;
    file.write_all(&key).ok()?;
    file.sync_all().ok()?;
    drop(file);
    std::fs::rename(temp_path, published_path).ok()?;
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
    // All readers and creators hold the same persistent lock. The OS releases
    // it on process exit, including crashes; never unlink the lock file.
    let mut lock_path = path.as_os_str().to_os_string();
    lock_path.push(".lock");
    let lock = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(lock_path)
        .ok()?;
    lock.lock().ok()?;
    let read_key = |file: std::fs::File| {
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
    // Never overwrite a published key: a DPAPI error may mean this process
    // lacks access to another user's key, rather than a damaged file.
    let mut published_path = path.as_os_str().to_os_string();
    published_path.push(".v2");
    let published_path = std::path::PathBuf::from(published_path);
    match std::fs::File::open(&published_path) {
        Ok(file) => return read_key(file),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
        Err(_) => return None,
    }
    // Only an empty legacy file is provably incomplete without interpreting
    // DPAPI's opaque format. Other decryption and I/O failures fail closed.
    let legacy = match std::fs::File::open(path) {
        Ok(file) => {
            let metadata = file.metadata().ok()?;
            if !metadata.is_file() {
                return None;
            }
            if metadata.len() == 0 {
                None
            } else {
                Some(read_key(file)?)
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(_) => return None,
    };
    let key = legacy.unwrap_or_else(|| {
        let mut key = [0_u8; 32];
        key[..16].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
        key[16..].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
        key
    });
    let encrypted = dpapi_transform(&key, true)?;
    // Do not replace an unreadable legacy file if DPAPI is unavailable.
    if dpapi_transform(&encrypted, false)?.as_slice() != key {
        return None;
    }
    let mut temp_path = path.as_os_str().to_os_string();
    temp_path.push(format!(".{}.tmp", uuid::Uuid::new_v4()));
    let temp_path = std::path::PathBuf::from(temp_path);
    struct RemoveTemp(std::path::PathBuf);
    impl Drop for RemoveTemp {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }
    let _cleanup = RemoveTemp(temp_path.clone());
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp_path)
        .ok()?;
    file.write_all(&encrypted).ok()?;
    file.sync_all().ok()?;
    drop(file);
    std::fs::rename(&temp_path, &published_path).ok()?;
    Some(key)
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
        let published_path = path.with_extension("v2");
        let encrypted = std::fs::read(&published_path).unwrap();
        assert!(!encrypted.windows(key.len()).any(|window| window == key));
        std::fs::write(&published_path, b"tampered").unwrap();
        assert_eq!(load_or_create_windows_key(&path), None);
        assert_eq!(std::fs::read(&published_path).unwrap(), b"tampered");
    }

    #[cfg(windows)]
    #[test]
    fn windows_key_concurrent_creators_recover_incomplete_files() {
        if let Some(path) = std::env::var_os("PI_RS_DPAPI_RACE_PATH") {
            let key = load_or_create_windows_key(std::path::Path::new(&path)).unwrap();
            std::fs::write(
                std::env::var_os("PI_RS_DPAPI_RACE_RESULT").unwrap(),
                fingerprint_with_key(&key, &"race").unwrap(),
            )
            .unwrap();
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("key");
        let encrypted = dpapi_transform(&[7; 32], true).unwrap();
        {
            let length = 0;
            let _ = std::fs::remove_file(path.with_extension("v2"));
            std::fs::write(&path, &encrypted[..length]).unwrap();
            let mut children = Vec::new();
            for index in 0..8 {
                let result = dir.path().join(format!("result-{index}"));
                let child = std::process::Command::new(std::env::current_exe().unwrap())
                    .args([
                        "--exact",
                        "context_fingerprint::tests::windows_key_concurrent_creators_recover_incomplete_files",
                    ])
                    .env("PI_RS_DPAPI_RACE_PATH", &path)
                    .env("PI_RS_DPAPI_RACE_RESULT", &result)
                    .spawn()
                    .unwrap();
                children.push((child, result));
            }
            let mut results = Vec::new();
            for (mut child, result) in children {
                assert!(child.wait().unwrap().success());
                results.push(std::fs::read_to_string(result).unwrap());
            }
            assert!(results.iter().all(|result| result == &results[0]));
            let key = load_or_create_windows_key(&path).unwrap();
            assert_eq!(fingerprint_with_key(&key, &"race").unwrap(), results[0]);
            assert_eq!(std::fs::read(&path).unwrap(), encrypted[..length]);
        }
    }

    #[cfg(windows)]
    #[test]
    fn windows_key_migrates_readable_legacy_key() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("key");
        let key = [42; 32];
        std::fs::write(&path, dpapi_transform(&key, true).unwrap()).unwrap();
        assert_eq!(load_or_create_windows_key(&path), Some(key));
        std::fs::remove_file(path).unwrap();
        assert_eq!(
            load_or_create_windows_key(&dir.path().join("key")),
            Some(key)
        );
    }

    #[cfg(windows)]
    #[test]
    fn windows_key_fails_closed_for_unreadable_legacy_key() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("key");
        let encrypted = dpapi_transform(&[7; 32], true).unwrap();
        for length in [1, encrypted.len() / 2, encrypted.len() - 1] {
            std::fs::write(&path, &encrypted[..length]).unwrap();
            assert_eq!(load_or_create_windows_key(&path), None);
            assert!(!path.with_extension("v2").exists());
        }
        // A structurally complete blob which DPAPI cannot authenticate models
        // a key protected by another Windows user or a damaged key.
        let mut inaccessible = encrypted;
        let last = inaccessible.len() - 1;
        inaccessible[last] ^= 0xff;
        std::fs::write(&path, &inaccessible).unwrap();
        assert_eq!(load_or_create_windows_key(&path), None);
        assert!(!path.with_extension("v2").exists());
        assert_eq!(std::fs::read(path).unwrap(), inaccessible);
    }

    #[cfg(unix)]
    #[test]
    fn key_persists_and_rejects_insecure_storage() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("key");
        let key = load_or_create_key(&path).unwrap();
        assert_eq!(load_or_create_key(&path), Some(key));
        let published = path.with_extension("v2");
        assert_eq!(
            std::fs::metadata(&published).unwrap().permissions().mode() & 0o777,
            0o600
        );
        std::fs::set_permissions(&published, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert_eq!(load_or_create_key(&path), None);
        let link = dir.path().join("link");
        std::os::unix::fs::symlink(&published, &link).unwrap();
        assert_eq!(load_or_create_key(&link), None);
    }

    #[cfg(unix)]
    #[test]
    fn unix_key_migrates_and_recovers_concurrently() {
        use std::os::unix::fs::PermissionsExt;
        if let Some(path) = std::env::var_os("PI_RS_UNIX_KEY_RACE_PATH") {
            let key = load_or_create_key(std::path::Path::new(&path)).unwrap();
            std::fs::write(
                std::env::var_os("PI_RS_UNIX_KEY_RACE_RESULT").unwrap(),
                fingerprint_with_key(&key, &"race").unwrap(),
            )
            .unwrap();
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("key");
        let original = [42; 32];
        std::fs::write(&path, original).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        assert_eq!(load_or_create_key(&path), Some(original));
        std::fs::remove_file(path.with_extension("v2")).unwrap();
        for length in [0, 1, 16, 31] {
            std::fs::write(&path, &original[..length]).unwrap();
            let mut children = Vec::new();
            for index in 0..8 {
                let result = dir.path().join(format!("result-{index}"));
                let child = std::process::Command::new(std::env::current_exe().unwrap())
                    .args([
                        "--exact",
                        "context_fingerprint::tests::unix_key_migrates_and_recovers_concurrently",
                    ])
                    .env("PI_RS_UNIX_KEY_RACE_PATH", &path)
                    .env("PI_RS_UNIX_KEY_RACE_RESULT", &result)
                    .spawn()
                    .unwrap();
                children.push((child, result));
            }
            let mut results = Vec::new();
            for (mut child, result) in children {
                assert!(child.wait().unwrap().success());
                results.push(std::fs::read_to_string(result).unwrap());
            }
            assert!(results.iter().all(|result| result == &results[0]));
            let key = load_or_create_key(&path).unwrap();
            assert_eq!(fingerprint_with_key(&key, &"race").unwrap(), results[0]);
            assert_eq!(std::fs::read(&path).unwrap(), original[..length]);
            std::fs::remove_file(path.with_extension("v2")).unwrap();
        }
    }
}
