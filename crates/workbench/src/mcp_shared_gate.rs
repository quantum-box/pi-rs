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

/// Experimental Windows mutex gate. It is deliberately not wired to `open`.
/// A same-SID low-integrity process can hold the gate and delay higher-IL
/// processes; the DACL is not a boundary between integrity levels of one user.
/// AppContainer and restricted tokens may still fail additional access checks.
/// Windows behavior has not been tested. Name squatting and durable crash
/// recovery remain unresolved. Inspect `abandoned` and recover before use.
#[cfg(windows)]
pub(crate) mod windows_mutex {
    use std::{
        ffi::c_void,
        io,
        ptr::null_mut,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
            mpsc,
        },
        time::Duration,
    };
    use windows_sys::Win32::{
        Foundation::{
            CloseHandle, HANDLE, LocalFree, WAIT_ABANDONED, WAIT_FAILED, WAIT_OBJECT_0,
            WAIT_TIMEOUT,
        },
        Security::Authorization::{
            ConvertSidToStringSidW, ConvertStringSecurityDescriptorToSecurityDescriptorW,
            ConvertStringSidToSidW, GetSecurityInfo, SDDL_REVISION_1, SE_KERNEL_OBJECT,
        },
        Security::{
            ACCESS_ALLOWED_ACE, ACL, DACL_SECURITY_INFORMATION, EqualSid, GetAce,
            GetSecurityDescriptorControl, GetSecurityDescriptorDacl, GetSecurityDescriptorOwner,
            GetSecurityDescriptorSacl, GetTokenInformation, LABEL_SECURITY_INFORMATION,
            OWNER_SECURITY_INFORMATION, PSECURITY_DESCRIPTOR, PSID, SE_DACL_PROTECTED,
            SECURITY_ATTRIBUTES, SYSTEM_MANDATORY_LABEL_ACE, TOKEN_QUERY, TOKEN_USER, TokenUser,
        },
        System::Threading::{
            CreateMutexExW, GetCurrentProcess, MUTEX_MODIFY_STATE, OpenProcessToken, ReleaseMutex,
            SYNCHRONIZATION_READ_CONTROL, SYNCHRONIZATION_SYNCHRONIZE, WaitForSingleObject,
        },
    };

    const RIGHTS: u32 =
        SYNCHRONIZATION_SYNCHRONIZE | SYNCHRONIZATION_READ_CONTROL | MUTEX_MODIFY_STATE;
    struct Handle(HANDLE);
    impl Drop for Handle {
        fn drop(&mut self) {
            unsafe {
                CloseHandle(self.0);
            }
        }
    }
    struct Local(*mut c_void);
    impl Drop for Local {
        fn drop(&mut self) {
            unsafe {
                LocalFree(self.0);
            }
        }
    }
    fn denied(msg: &'static str) -> io::Error {
        io::Error::new(io::ErrorKind::PermissionDenied, msg)
    }
    fn wide(s: &str) -> Vec<u16> {
        s.encode_utf16().chain(std::iter::once(0)).collect()
    }

    // Own the aligned TokenUser buffer while its SID pointer is in use.
    struct User {
        _buffer: Vec<usize>,
        sid: PSID,
        text: String,
    }
    fn user() -> io::Result<User> {
        let mut token = null_mut();
        if unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) } == 0 {
            return Err(io::Error::last_os_error());
        }
        let token = Handle(token);
        let mut len = 0u32;
        unsafe {
            GetTokenInformation(token.0, TokenUser, null_mut(), 0, &mut len);
        }
        if len == 0 || len > 1024 * 1024 {
            return Err(denied("invalid token user size"));
        }
        let mut buffer = vec![0usize; (len as usize).div_ceil(std::mem::size_of::<usize>())];
        if unsafe {
            GetTokenInformation(
                token.0,
                TokenUser,
                buffer.as_mut_ptr().cast(),
                len,
                &mut len,
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        let sid = unsafe { (*(buffer.as_ptr().cast::<TOKEN_USER>())).User.Sid };
        if sid.is_null() {
            return Err(denied("missing token SID"));
        }
        let mut text = null_mut();
        if unsafe { ConvertSidToStringSidW(sid, &mut text) } == 0 {
            return Err(io::Error::last_os_error());
        }
        let allocated = Local(text.cast());
        let mut n = 0usize;
        while n < 256 && unsafe { *text.add(n) } != 0 {
            n += 1;
        }
        if n == 256 {
            return Err(denied("invalid SID string"));
        }
        let text = String::from_utf16(unsafe { std::slice::from_raw_parts(text, n) })
            .map_err(|_| denied("invalid SID string"))?;
        drop(allocated);
        Ok(User {
            _buffer: buffer,
            sid,
            text,
        })
    }

    fn descriptor(user: &User) -> io::Result<Local> {
        // One explicit ACE, no inheritance, granting only requested rights.
        let sddl = wide(&format!(
            "O:{}D:P(A;;0x{:08x};;;{})S:(ML;;NW;;;LW)",
            user.text, RIGHTS, user.text
        ));
        let mut sd: PSECURITY_DESCRIPTOR = null_mut();
        if unsafe {
            ConvertStringSecurityDescriptorToSecurityDescriptorW(
                sddl.as_ptr(),
                SDDL_REVISION_1,
                &mut sd,
                null_mut(),
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        Ok(Local(sd))
    }

    fn validate(handle: HANDLE, user: &User) -> io::Result<()> {
        let mut sd: PSECURITY_DESCRIPTOR = null_mut();
        let status = unsafe {
            GetSecurityInfo(
                handle,
                SE_KERNEL_OBJECT,
                OWNER_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION | LABEL_SECURITY_INFORMATION,
                null_mut(),
                null_mut(),
                null_mut(),
                null_mut(),
                &mut sd,
            )
        };
        if status != 0 {
            return Err(io::Error::from_raw_os_error(status as i32));
        }
        let sd = Local(sd);
        let mut owner: PSID = null_mut();
        let mut defaulted = 0;
        if unsafe { GetSecurityDescriptorOwner(sd.0, &mut owner, &mut defaulted) } == 0
            || owner.is_null()
            || unsafe { EqualSid(owner, user.sid) } == 0
        {
            return Err(denied("mutex owner does not match user"));
        }
        let mut control = 0u16;
        let mut revision = 0u32;
        if unsafe { GetSecurityDescriptorControl(sd.0, &mut control, &mut revision) } == 0
            || control & SE_DACL_PROTECTED == 0
        {
            return Err(denied("mutex DACL is not protected"));
        }
        let mut present = 0;
        let mut dacl: *mut ACL = null_mut();
        if unsafe { GetSecurityDescriptorDacl(sd.0, &mut present, &mut dacl, &mut defaulted) } == 0
            || present == 0
            || dacl.is_null()
            || unsafe { (*dacl).AceCount } != 1
        {
            return Err(denied("unexpected mutex DACL"));
        }
        let mut ace: *mut c_void = null_mut();
        if unsafe { GetAce(dacl, 0, &mut ace) } == 0 || ace.is_null() {
            return Err(denied("invalid mutex ACE"));
        }
        let ace = ace.cast::<ACCESS_ALLOWED_ACE>();
        // ACE type zero is ACCESS_ALLOWED_ACE_TYPE. Reject inherited, callback,
        // deny, multiple, or otherwise unfamiliar ACEs rather than interpreting them.
        if unsafe { (*ace).Header.AceType } != 0
            || unsafe { (*ace).Header.AceFlags } != 0
            || unsafe { (*ace).Mask } != RIGHTS
            || unsafe { (*ace).Header.AceSize } < 12
        {
            return Err(denied("unexpected mutex ACE"));
        }
        let ace_sid = unsafe { std::ptr::addr_of_mut!((*ace).SidStart).cast::<u8>() };
        // SID has an eight-byte header followed by at most 15 subauthorities.
        // Check its claimed extent before passing it to EqualSid.
        if unsafe { (*ace).Header.AceSize } < 16 {
            return Err(denied("truncated mutex SID"));
        }
        let count = unsafe { *ace_sid.add(1) } as usize;
        let available = unsafe { (*ace).Header.AceSize } as usize - 8;
        if count > 15 || 8 + count * 4 > available {
            return Err(denied("invalid mutex SID extent"));
        }
        if unsafe { EqualSid(ace_sid.cast(), user.sid) } == 0 {
            return Err(denied("mutex ACE SID does not match user"));
        }
        validate_low_label(sd.0)?;
        Ok(())
    }

    // Require exactly one low integrity label with NO_WRITE_UP. Missing or
    // higher labels are rejected even when this process can open the object:
    // another same-user process at low integrity must be able to use it.
    fn validate_low_label(sd: PSECURITY_DESCRIPTOR) -> io::Result<()> {
        let mut present = 0;
        let mut defaulted = 0;
        let mut sacl: *mut ACL = null_mut();
        if unsafe { GetSecurityDescriptorSacl(sd, &mut present, &mut sacl, &mut defaulted) } == 0
            || present == 0
            || sacl.is_null()
            || unsafe { (*sacl).AceCount } != 1
        {
            return Err(denied("mutex integrity label is missing or unexpected"));
        }
        let mut ptr: *mut c_void = null_mut();
        if unsafe { GetAce(sacl, 0, &mut ptr) } == 0 || ptr.is_null() {
            return Err(denied("invalid mutex integrity label"));
        }
        let ace = ptr.cast::<SYSTEM_MANDATORY_LABEL_ACE>();
        // SYSTEM_MANDATORY_LABEL_ACE_TYPE = 0x11; NO_WRITE_UP = 1.
        if unsafe { (*ace).Header.AceType } != 0x11
            || unsafe { (*ace).Header.AceFlags } != 0
            || unsafe { (*ace).Header.AceSize } != 20
            || unsafe { (*ace).Mask } != 1
        {
            return Err(denied("unexpected mutex integrity label"));
        }
        let mut low: PSID = null_mut();
        if unsafe { ConvertStringSidToSidW(wide("S-1-16-4096").as_ptr(), &mut low) } == 0 {
            return Err(io::Error::last_os_error());
        }
        let low = Local(low);
        let label_sid = unsafe { std::ptr::addr_of_mut!((*ace).SidStart).cast::<u8>() };
        if unsafe { *label_sid.add(1) } != 1 || unsafe { EqualSid(label_sid.cast(), low.0) } == 0 {
            return Err(denied("mutex integrity level is not low"));
        }
        Ok(())
    }

    #[cfg(test)]
    mod integrity_tests {
        use super::*;

        fn parse(sddl: &str) -> Local {
            let mut sd: PSECURITY_DESCRIPTOR = null_mut();
            assert_ne!(
                unsafe {
                    ConvertStringSecurityDescriptorToSecurityDescriptorW(
                        wide(sddl).as_ptr(),
                        SDDL_REVISION_1,
                        &mut sd,
                        null_mut(),
                    )
                },
                0
            );
            Local(sd)
        }

        #[test]
        fn only_low_no_write_up_label_is_accepted() {
            for (sddl, accepted) in [
                ("S:(ML;;NW;;;ME)", false),
                ("S:(ML;;NW;;;HI)", false),
                ("S:(ML;;NW;;;LW)", true),
                ("S:(ML;;NR;;;LW)", false),
                ("D:(A;;GA;;;SY)", false),
            ] {
                let descriptor = parse(sddl);
                assert_eq!(validate_low_label(descriptor.0).is_ok(), accepted, "{sddl}");
            }
        }
    }

    fn open_mutex() -> io::Result<Handle> {
        let user = user()?;
        let sd = descriptor(&user)?;
        let attrs = SECURITY_ATTRIBUTES {
            nLength: std::mem::size_of::<SECURITY_ATTRIBUTES>() as u32,
            lpSecurityDescriptor: sd.0,
            bInheritHandle: 0,
        };
        let name = wide(&format!("Global\\pi-rs-mcp-config-v1-{}", user.text));
        let mutex = unsafe { CreateMutexExW(&attrs, name.as_ptr(), 0, RIGHTS) };
        if mutex.is_null() {
            return Err(io::Error::last_os_error());
        }
        let mutex = Handle(mutex);
        // Validate new and already-existing objects; supplied attributes do not
        // replace the security descriptor of an existing named mutex.
        validate(mutex.0, &user)?;
        Ok(mutex)
    }

    /// Owns an open mutex on a dedicated thread; dropping requests release.
    pub(crate) struct Guard {
        release: Option<mpsc::Sender<bool>>,
        pub(crate) abandoned: bool,
        recovered: bool,
    }
    impl Guard {
        /// Call only after persistent state has been checked and repaired.
        pub(crate) fn mark_recovered(&mut self) {
            self.recovered = true;
        }
    }
    impl Drop for Guard {
        fn drop(&mut self) {
            if let Some(release) = self.release.take() {
                let _ = release.send(self.recovered);
            }
        }
    }
    struct Cancel(Arc<AtomicBool>);
    impl Drop for Cancel {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }

    /// Cancellation-safe before ownership: dropping this future stops the
    /// waiting worker. After acquisition, retain Guard until all writes finish.
    pub(crate) async fn acquire() -> io::Result<Guard> {
        let cancelled = Arc::new(AtomicBool::new(false));
        let _cancel = Cancel(cancelled.clone());
        let (result_tx, result_rx) = mpsc::sync_channel::<io::Result<bool>>(1);
        let (release_tx, release_rx) = mpsc::channel::<bool>();
        std::thread::Builder::new()
            .name("pi-mcp-gate".into())
            .spawn(move || {
                let result = (|| -> io::Result<()> {
                    let mutex = open_mutex()?;
                    loop {
                        if cancelled.load(Ordering::Acquire) {
                            return Ok(());
                        }
                        let wait = unsafe { WaitForSingleObject(mutex.0, 50) };
                        match wait {
                            WAIT_TIMEOUT => continue,
                            WAIT_OBJECT_0 | WAIT_ABANDONED => {
                                // result conveys whether recovery is required. The owning
                                // thread releases even if the async receiver is dropped.
                                let _ = result_tx.send(Ok(wait == WAIT_ABANDONED));
                                let recovered = release_rx.recv().unwrap_or(false);
                                // Preserve abandonment for existing handles when a caller
                                // disappears before recovery. If this is the last handle,
                                // Windows destroys the object and the indication is lost;
                                // persistent crash recovery needs a separate protocol.
                                if wait == WAIT_ABANDONED && !recovered {
                                    return Ok(());
                                }
                                if unsafe { ReleaseMutex(mutex.0) } == 0 {
                                    return Err(io::Error::last_os_error());
                                }
                                return Ok(());
                            }
                            WAIT_FAILED => return Err(io::Error::last_os_error()),
                            _ => return Err(denied("unexpected mutex wait result")),
                        }
                    }
                })();
                if let Err(error) = result {
                    let _ = result_tx.send(Err(error));
                }
            })?;
        loop {
            match result_rx.try_recv() {
                Ok(Ok(abandoned)) => {
                    return Ok(Guard {
                        release: Some(release_tx),
                        abandoned,
                        recovered: !abandoned,
                    });
                }
                Ok(Err(error)) => return Err(error),
                Err(mpsc::TryRecvError::Disconnected) => {
                    return Err(io::Error::other("mutex worker exited"));
                }
                Err(mpsc::TryRecvError::Empty) => {
                    tokio::time::sleep(Duration::from_millis(10)).await
                }
            }
        }
    }
}
