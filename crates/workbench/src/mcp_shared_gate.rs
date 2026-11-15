//! Per-account MCP configuration gate, independent of process environment.
//!
//! The returned file is unlocked. Callers must acquire it with `try_lock` in a
//! cancellable/deadline-aware loop, and retain the handle until the operation
//! finishes. Never unlink, rename, or replace the gate while it may be in use.
use std::{
    fs::File,
    io,
    path::{Path, PathBuf},
};

/// Open the stable, per-OS-account gate without consulting HOME or PI_RS_HOME.
pub(crate) fn open() -> io::Result<File> {
    let profile = account_profile()?;
    open_in_profile(&profile)
}

#[cfg(unix)]
fn account_profile() -> io::Result<PathBuf> {
    use std::{
        ffi::{CStr, OsString},
        os::unix::ffi::OsStringExt,
    };
    let mut size = 1024usize;
    loop {
        let mut buffer = vec![0u8; size];
        let mut entry = std::mem::MaybeUninit::<libc::passwd>::uninit();
        let mut result = std::ptr::null_mut();
        // The returned passwd and pw_dir are valid while buffer is alive.
        let status = unsafe {
            libc::getpwuid_r(
                libc::geteuid(),
                entry.as_mut_ptr(),
                buffer.as_mut_ptr().cast(),
                buffer.len(),
                &mut result,
            )
        };
        if status == libc::ERANGE && size < 1024 * 1024 {
            size *= 2;
            continue;
        }
        if status != 0 {
            return Err(io::Error::from_raw_os_error(status));
        }
        if result.is_null() || unsafe { (*result).pw_dir.is_null() } {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "account profile unavailable",
            ));
        }
        let bytes = unsafe { CStr::from_ptr((*result).pw_dir) }
            .to_bytes()
            .to_vec();
        let path = PathBuf::from(OsString::from_vec(bytes));
        if !path.is_absolute() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "account profile must be absolute",
            ));
        }
        return Ok(path);
    }
}

#[cfg(windows)]
fn account_profile() -> io::Result<PathBuf> {
    use std::{ffi::OsString, os::windows::ffi::OsStringExt, ptr::null_mut};
    use windows_sys::Win32::{
        Foundation::{CloseHandle, ERROR_INSUFFICIENT_BUFFER},
        Security::TOKEN_QUERY,
        System::Threading::{GetCurrentProcess, OpenProcessToken},
        UI::Shell::GetUserProfileDirectoryW,
    };
    let mut token = null_mut();
    if unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) } == 0 {
        return Err(io::Error::last_os_error());
    }
    struct Token(windows_sys::Win32::Foundation::HANDLE);
    impl Drop for Token {
        fn drop(&mut self) {
            unsafe {
                CloseHandle(self.0);
            }
        }
    }
    let token = Token(token);
    let mut len = 0u32;
    if unsafe { GetUserProfileDirectoryW(token.0, null_mut(), &mut len) } != 0
        || io::Error::last_os_error().raw_os_error() != Some(ERROR_INSUFFICIENT_BUFFER as i32)
        || len == 0
        || len > 32768
    {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            "account profile unavailable",
        ));
    }
    let mut buffer = vec![0u16; len as usize];
    if unsafe { GetUserProfileDirectoryW(token.0, buffer.as_mut_ptr(), &mut len) } == 0 {
        return Err(io::Error::last_os_error());
    }
    let end = buffer
        .iter()
        .position(|&c| c == 0)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid profile path"))?;
    let path = PathBuf::from(OsString::from_wide(&buffer[..end]));
    if !path.is_absolute() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "account profile must be absolute",
        ));
    }
    Ok(path)
}

#[cfg(not(any(unix, windows)))]
fn account_profile() -> io::Result<PathBuf> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "account profile unavailable",
    ))
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn open_in_profile(profile: &Path) -> io::Result<File> {
    use std::{
        ffi::CString,
        os::{
            fd::{AsRawFd, FromRawFd},
            unix::{ffi::OsStrExt, fs::MetadataExt},
        },
    };
    fn invalid(message: &'static str) -> io::Error {
        io::Error::new(io::ErrorKind::PermissionDenied, message)
    }
    fn openat(dir: i32, name: &std::ffi::CStr, flags: i32, mode: u32) -> io::Result<File> {
        let fd = unsafe {
            libc::openat(
                dir,
                name.as_ptr(),
                flags | libc::O_CLOEXEC | libc::O_NOFOLLOW,
                mode as libc::c_uint,
            )
        };
        if fd < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(unsafe { File::from_raw_fd(fd) })
        }
    }
    if !profile.is_absolute() {
        return Err(invalid("profile must be absolute"));
    }
    let path = CString::new(profile.as_os_str().as_bytes())
        .map_err(|_| invalid("invalid profile path"))?;
    // The account database may legitimately name a symlinked home. The opened
    // directory must belong to this effective user and not be writable by others.
    let fd = unsafe {
        libc::open(
            path.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC,
        )
    };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    let root = unsafe { File::from_raw_fd(fd) };
    let meta = root.metadata()?;
    if meta.uid() != unsafe { libc::geteuid() } || meta.mode() & 0o022 != 0 {
        return Err(invalid("unsafe account profile ownership or permissions"));
    }
    check_no_acl(&root)?;
    let dirname = c".pi-rs-shared";
    let rc = unsafe { libc::mkdirat(root.as_raw_fd(), dirname.as_ptr(), 0o700) };
    if rc != 0 && io::Error::last_os_error().kind() != io::ErrorKind::AlreadyExists {
        return Err(io::Error::last_os_error());
    }
    let dir = openat(
        root.as_raw_fd(),
        dirname,
        libc::O_RDONLY | libc::O_DIRECTORY,
        0,
    )?;
    let meta = dir.metadata()?;
    if !meta.is_dir() || meta.uid() != unsafe { libc::geteuid() } || meta.mode() & 0o7777 != 0o700 {
        return Err(invalid("unsafe shared gate directory"));
    }
    check_no_acl(&dir)?;
    let file = openat(
        dir.as_raw_fd(),
        c"mcp-config-gate.lock",
        libc::O_RDWR | libc::O_CREAT | libc::O_NONBLOCK,
        0o600,
    )?;
    let meta = file.metadata()?;
    if !meta.is_file()
        || meta.uid() != unsafe { libc::geteuid() }
        || meta.mode() & 0o7777 != 0o600
        || meta.nlink() != 1
    {
        return Err(invalid("unsafe shared gate file"));
    }
    check_no_acl(&file)?;
    Ok(file)
}

#[cfg(target_os = "linux")]
fn check_no_acl(file: &File) -> io::Result<()> {
    use std::os::fd::AsRawFd;
    for name in [c"system.posix_acl_access", c"system.posix_acl_default"] {
        let result =
            unsafe { libc::fgetxattr(file.as_raw_fd(), name.as_ptr(), std::ptr::null_mut(), 0) };
        if result >= 0 {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "shared gate has an ACL",
            ));
        }
        let error = io::Error::last_os_error();
        if error.raw_os_error() != Some(libc::ENODATA) {
            return Err(error);
        }
    }
    Ok(())
}

#[cfg(target_os = "macos")]
fn check_no_acl(file: &File) -> io::Result<()> {
    use std::{ffi::c_void, os::fd::AsRawFd};
    unsafe extern "C" {
        fn acl_get_fd_np(fd: i32, kind: i32) -> *mut c_void;
        fn acl_free(acl: *mut c_void) -> i32;
    }
    // ACL_TYPE_EXTENDED, from macOS <sys/acl.h>.
    let acl = unsafe { acl_get_fd_np(file.as_raw_fd(), 0x00000100) };
    if !acl.is_null() {
        unsafe {
            acl_free(acl);
        }
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "shared gate has an ACL",
        ));
    }
    let error = io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::ENOENT) {
        Ok(())
    } else {
        Err(error)
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn open_in_profile(_profile: &Path) -> io::Result<File> {
    // Windows must retain non-delete-sharing handles to the profile and gate
    // directory for as long as the gate file is in use. Returning only File
    // cannot retain those handles; checking a path and then dropping its parent
    // handles would allow a rename/replacement to split the lock namespace.
    // Also required: validate owner SID, protected DACL, reparse attributes,
    // link count and file identity through opened handles, without following
    // attacker-controlled components. Fail closed until that API exists.
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "secure shared gate is not implemented on this platform",
    ))
}

#[cfg(all(test, windows))]
mod windows_tests {
    use super::*;

    #[test]
    fn unsupported_gate_does_not_create_a_directory_or_use_account_profile() {
        // The profile is deliberately a nonexistent path under the build tree.
        // This test never asks the OS for the actual account profile.
        let profile = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join(format!("mcp-windows-no-create-{}", std::process::id()));
        assert!(!profile.exists());
        let error = open_in_profile(&profile).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::Unsupported);
        assert!(!profile.exists());
    }
}

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
mod tests {
    use super::*;
    use std::os::unix::fs::{PermissionsExt, symlink};
    struct TestDir(PathBuf);
    impl TestDir {
        fn new() -> Self {
            // Explicit test-only root; never resolve or touch the account profile.
            let root =
                PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/mcp-shared-gate-tests");
            std::fs::create_dir_all(&root).unwrap();
            for n in 0..1000 {
                let path = root.join(format!("{}-{n}", std::process::id()));
                match std::fs::create_dir(&path) {
                    Ok(()) => {
                        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700))
                            .unwrap();
                        return Self(path);
                    }
                    Err(e) if e.kind() == io::ErrorKind::AlreadyExists => (),
                    Err(e) => panic!("{e}"),
                }
            }
            panic!("cannot create test directory")
        }
    }
    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    #[test]
    fn gate_handles_contend_and_reopen() {
        let root = TestDir::new();
        let first = open_in_profile(&root.0).unwrap();
        let second = open_in_profile(&root.0).unwrap();
        first.try_lock().unwrap();
        assert!(matches!(
            second.try_lock(),
            Err(std::fs::TryLockError::WouldBlock)
        ));
        drop(first);
        second.try_lock().unwrap();
    }
    #[test]
    fn reject_directory_symlink_and_unsafe_permissions() {
        let root = TestDir::new();
        symlink(&root.0, root.0.join(".pi-rs-shared")).unwrap();
        assert!(open_in_profile(&root.0).is_err());
        std::fs::remove_file(root.0.join(".pi-rs-shared")).unwrap();
        std::fs::create_dir(root.0.join(".pi-rs-shared")).unwrap();
        std::fs::set_permissions(
            root.0.join(".pi-rs-shared"),
            std::fs::Permissions::from_mode(0o755),
        )
        .unwrap();
        assert!(open_in_profile(&root.0).is_err());
    }
    #[test]
    fn reject_symlink_and_hardlink_gate() {
        let root = TestDir::new();
        drop(open_in_profile(&root.0).unwrap());
        let gate = root.0.join(".pi-rs-shared/mcp-config-gate.lock");
        std::fs::hard_link(&gate, root.0.join("hardlink")).unwrap();
        assert!(open_in_profile(&root.0).is_err());
        std::fs::remove_file(&gate).unwrap();
        symlink(root.0.join("hardlink"), &gate).unwrap();
        assert!(open_in_profile(&root.0).is_err());
    }
}
