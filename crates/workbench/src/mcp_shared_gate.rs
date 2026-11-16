//! Per-account MCP configuration gate, independent of process environment.
//!
//! Hold the returned guard until the protected operation finishes. Never
//! unlink, rename, or replace the underlying gate while it may be in use.
#[cfg(any(target_os = "linux", target_os = "macos", test))]
use std::path::Path;
use std::{fs::File, io, path::PathBuf};

/// Open the stable, per-OS-account gate without consulting HOME or PI_RS_HOME.
#[cfg(any(target_os = "linux", target_os = "macos"))]
pub(crate) fn open() -> io::Result<File> {
    let profile = account_profile()?;
    open_in_profile(&profile)
}

/// WIP: serializes participating processes only. No crash recovery is wired in.
/// Older/non-cooperating readers and OAuth transactions remain non-atomic.
pub(crate) struct AccountGuard {
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    file: File,
    #[cfg(windows)]
    _mutex: windows_mutex::Guard,
}

impl AccountGuard {
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    pub(crate) fn aliases_file(&self, other: &File) -> io::Result<bool> {
        Ok(same_file::Handle::from_file(self.file.try_clone()?)?
            == same_file::Handle::from_file(other.try_clone()?)?)
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    pub(crate) fn aliases_file(&self, _other: &File) -> io::Result<bool> {
        Ok(false)
    }
}

/// Holds the account gate and applicable legacy configuration locks.
pub struct ConfigGuard {
    _account: AccountGuard,
    _targets: Vec<std::fs::File>,
}

/// Acquire the account gate and sorted legacy configuration locks.
///
/// If provided, the credential lock must already be held and remain held for
/// the operation. Read-only mode may skip an unavailable legacy lock. Older
/// readers and login transactions are not atomic with participating clients.
pub async fn acquire_config_guard(
    paths: &[PathBuf],
    credential: Option<&std::fs::File>,
    readonly: bool,
) -> pi_rs::Result<ConfigGuard> {
    let account = acquire(credential).await?;
    account_guard_with(account, paths, credential, readonly).await
}

/// Read a synchronous snapshot while the configuration guard is held.
pub async fn with_config_guard<T>(
    gate: impl std::future::Future<Output = pi_rs::Result<ConfigGuard>>,
    read: impl FnOnce() -> pi_rs::Result<T>,
) -> pi_rs::Result<T> {
    let _guard = gate.await?;
    read()
}

async fn account_guard_with(
    account: AccountGuard,
    paths: &[PathBuf],
    credential: Option<&std::fs::File>,
    readonly: bool,
) -> pi_rs::Result<ConfigGuard> {
    let credential_id = credential
        .map(|file| same_file::Handle::from_file(file.try_clone()?))
        .transpose()?;
    let mut lock_paths: Vec<_> = paths.iter().map(|p| p.with_extension("lock")).collect();
    lock_paths.sort();
    let mut opened = Vec::new();
    for path in lock_paths {
        let result = if readonly {
            let writable = std::fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(&path);
            match writable {
                Err(ref error) if weak_read_lock_error(&path, error) => {
                    std::fs::File::open(&path).map_err(pi_rs::Error::from)
                }
                result => result.map_err(pi_rs::Error::from),
            }
        } else {
            open_mcp_lock(&path)
        };
        let lock = match result {
            Ok(lock) => lock,
            Err(pi_rs::Error::Io(error)) if readonly && weak_read_lock_error(&path, &error) => {
                continue;
            }
            Err(error) => return Err(error),
        };
        let id = same_file::Handle::from_file(lock.try_clone()?)?;
        if account.aliases_file(&lock)? || credential_id.as_ref() == Some(&id) {
            return Err(pi_rs::Error::Config(
                "MCP locks refer to the same file".into(),
            ));
        }
        if opened.iter().any(|(other, _, _)| other == &id) {
            continue;
        }
        let order = lock_identity(&lock)?;
        opened.push((id, order, lock));
    }
    // Open and validate every handle before waiting. The account gate serializes
    // new participants; stable physical ordering also avoids alias path inversions.
    opened.sort_by_key(|(_, order, _)| *order);
    let mut targets = Vec::with_capacity(opened.len());
    for (_, _, lock) in opened {
        lock_mcp_file(&lock).await?;
        targets.push(lock);
    }
    Ok(ConfigGuard {
        _account: account,
        _targets: targets,
    })
}

fn weak_read_lock_error(path: &std::path::Path, error: &std::io::Error) -> bool {
    if error.kind() == std::io::ErrorKind::PermissionDenied {
        return true;
    }
    #[cfg(unix)]
    if error.raw_os_error() == Some(libc::EROFS) {
        return true;
    }
    if error.kind() == std::io::ErrorKind::NotFound {
        return path.parent().is_some_and(|parent| matches!(std::fs::metadata(parent), Err(e) if e.kind() == std::io::ErrorKind::NotFound));
    }
    false
}

#[cfg(unix)]
fn lock_identity(file: &std::fs::File) -> pi_rs::Result<(u64, u64)> {
    use std::os::unix::fs::MetadataExt;
    let metadata = file.metadata()?;
    Ok((metadata.dev(), metadata.ino()))
}
#[cfg(windows)]
fn lock_identity(file: &std::fs::File) -> pi_rs::Result<(u64, u64)> {
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Storage::FileSystem::{
        BY_HANDLE_FILE_INFORMATION, GetFileInformationByHandle,
    };
    let mut info = std::mem::MaybeUninit::<BY_HANDLE_FILE_INFORMATION>::uninit();
    if unsafe { GetFileInformationByHandle(file.as_raw_handle().cast(), info.as_mut_ptr()) } == 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    let info = unsafe { info.assume_init() };
    Ok((
        u64::from(info.dwVolumeSerialNumber),
        (u64::from(info.nFileIndexHigh) << 32) | u64::from(info.nFileIndexLow),
    ))
}
#[cfg(not(any(unix, windows)))]
fn lock_identity(_file: &std::fs::File) -> pi_rs::Result<(u64, u64)> {
    Err(pi_rs::Error::Config(
        "MCP lock identity is unavailable".into(),
    ))
}

fn open_mcp_lock(path: &std::path::Path) -> pi_rs::Result<std::fs::File> {
    std::fs::create_dir_all(path.parent().unwrap_or(std::path::Path::new(".")))?;
    Ok(std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)?)
}

async fn lock_mcp_file(file: &std::fs::File) -> pi_rs::Result<()> {
    loop {
        match file.try_lock() {
            Ok(()) => return Ok(()),
            Err(std::fs::TryLockError::WouldBlock) => {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            Err(std::fs::TryLockError::Error(error)) => return Err(error.into()),
        }
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
pub(crate) async fn acquire(credential: Option<&File>) -> io::Result<AccountGuard> {
    let file = open_off_runtime(open).await?;
    acquire_checked(file, credential).await
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
async fn open_off_runtime<F>(open_file: F) -> io::Result<File>
where
    F: FnOnce() -> io::Result<File> + Send + 'static,
{
    static SLOTS: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(16);
    open_off_runtime_with_timeout(&SLOTS, open_file, std::time::Duration::from_secs(10)).await
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
async fn open_off_runtime_with_timeout<F>(
    slots: &'static tokio::sync::Semaphore,
    open_file: F,
    timeout: std::time::Duration,
) -> io::Result<File>
where
    F: FnOnce() -> io::Result<File> + Send + 'static,
{
    tokio::time::timeout(timeout, open_off_runtime_with_slots(slots, open_file))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "MCP account lookup timed out"))?
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
async fn open_off_runtime_with_slots<F>(
    slots: &'static tokio::sync::Semaphore,
    open_file: F,
) -> io::Result<File>
where
    F: FnOnce() -> io::Result<File> + Send + 'static,
{
    // NSS can block indefinitely. A detached thread avoids blocking runtime
    // shutdown; it opens the file but never acquires its lock. Cancellation can
    // still leave that lookup running and it may create the gate file later.
    // Bound leaked workers if the account service never responds. A permit
    // stays occupied after cancellation until its lookup actually ends.
    let slot = slots
        .acquire()
        .await
        .map_err(|_| io::Error::other("MCP account lookup slots unavailable"))?;
    let (send, receive) = tokio::sync::oneshot::channel();
    std::thread::Builder::new()
        .name("mcp-account-lookup".into())
        .spawn(move || {
            let _slot = slot;
            let _ = send.send(open_file());
        })?;
    receive
        .await
        .map_err(|_| io::Error::other("MCP account lookup failed"))?
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
async fn acquire_checked(file: File, credential: Option<&File>) -> io::Result<AccountGuard> {
    if let Some(credential) = credential
        && same_file::Handle::from_file(file.try_clone()?)?
            == same_file::Handle::from_file(credential.try_clone()?)?
    {
        return Err(io::Error::other("MCP account gate aliases credential lock"));
    }
    acquire_file(file).await
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
async fn acquire_file(file: File) -> io::Result<AccountGuard> {
    loop {
        match file.try_lock() {
            Ok(()) => return Ok(AccountGuard { file }),
            Err(std::fs::TryLockError::WouldBlock) => {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await
            }
            Err(std::fs::TryLockError::Error(error)) => return Err(error),
        }
    }
}

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
pub(crate) async fn acquire_for_test(
    profile: &Path,
    credential: Option<&File>,
) -> io::Result<AccountGuard> {
    acquire_checked(open_in_profile(profile)?, credential).await
}

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
pub(crate) async fn acquire_config_guard_for_test(
    profile: &Path,
    paths: &[PathBuf],
    credential: Option<&File>,
) -> pi_rs::Result<ConfigGuard> {
    let account = acquire_for_test(profile, credential).await?;
    account_guard_with(account, paths, credential, false).await
}

#[cfg(windows)]
pub(crate) async fn acquire(_credential: Option<&File>) -> io::Result<AccountGuard> {
    let guard = windows_mutex::acquire().await?;
    if guard.abandoned {
        return Err(io::Error::other(
            "abandoned MCP gate requires recovery; recovery is not implemented",
        ));
    }
    Ok(AccountGuard { _mutex: guard })
}

#[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
pub(crate) async fn acquire(_credential: Option<&File>) -> io::Result<AccountGuard> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "shared MCP gate unavailable",
    ))
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
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
    check_no_acl(&root).map_err(|e| io::Error::new(e.kind(), format!("account profile: {e}")))?;
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
    check_no_acl(&dir).map_err(|e| io::Error::new(e.kind(), format!("gate directory: {e}")))?;
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
    check_no_acl(&file).map_err(|e| io::Error::new(e.kind(), format!("gate file: {e}")))?;
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
        fn filesec_init() -> *mut c_void;
        fn filesec_free(fsec: *mut c_void);
        fn filesec_query_property(fsec: *mut c_void, property: i32, valid: *mut i32) -> i32;
        fn filesec_get_property(fsec: *mut c_void, property: i32, value: *mut c_void) -> i32;
        #[cfg_attr(target_arch = "x86_64", link_name = "fstatx_np$INODE64")]
        fn fstatx_np(fd: i32, stat: *mut libc::stat, fsec: *mut c_void) -> i32;
        fn acl_free(acl: *mut c_void) -> i32;
        fn acl_valid(acl: *mut c_void) -> i32;
        fn acl_get_entry(acl: *mut c_void, id: i32, entry: *mut *mut c_void) -> i32;
        fn acl_get_tag_type(entry: *mut c_void, tag: *mut i32) -> i32;
        fn acl_size(acl: *mut c_void) -> isize;
        fn acl_copy_ext(buf: *mut c_void, acl: *mut c_void, size: isize) -> isize;
    }
    struct FileSec(*mut c_void);
    impl Drop for FileSec {
        fn drop(&mut self) {
            unsafe { filesec_free(self.0) };
        }
    }
    let fsec = unsafe { filesec_init() };
    if fsec.is_null() {
        return Err(io::Error::last_os_error());
    }
    let fsec = FileSec(fsec);
    let present = macos_acl_property_present(
        || {
            let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
            if unsafe { fstatx_np(file.as_raw_fd(), stat.as_mut_ptr(), fsec.0) } != 0 {
                Err(io::Error::last_os_error())
            } else {
                Ok(())
            }
        },
        |property| {
            let mut present = 0;
            if unsafe { filesec_query_property(fsec.0, property, &mut present) } != 0 {
                Err(io::Error::last_os_error())
            } else {
                Ok(present != 0)
            }
        },
    )?;
    if !present {
        // WIP limitation: Darwin Libc may also omit the property for an empty
        // ACL, hiding its flags. Do not claim that unknown/deferred flags are
        // fully rejected until raw ACL absence can be distinguished safely.
        return Ok(());
    }
    // FILESEC_ACL = 5. A successful property retrieval returns an owned copy.
    let acl = macos_acl_property_value(|| {
        let mut acl: *mut c_void = std::ptr::null_mut();
        if unsafe { filesec_get_property(fsec.0, 5, (&mut acl as *mut *mut c_void).cast()) } != 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(acl)
        }
    })?;
    struct Acl(*mut c_void);
    impl Drop for Acl {
        fn drop(&mut self) {
            unsafe { acl_free(self.0) };
        }
    }
    let acl = Acl(acl);
    if unsafe { acl_valid(acl.0) } != 0 {
        return Err(io::Error::last_os_error());
    }
    // Darwin's public kauth_filesec external layout and Libc acl_copy_ext
    // encode the entry count and flags in big endian. This is a Darwin-specific
    // format, not the opaque acl_t layout or a portable ACL representation.
    let size = unsafe { acl_size(acl.0) };
    let size = macos_acl_size_result(size, io::Error::last_os_error)?;
    if !(44..=44 + 128 * 24).contains(&size) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid shared gate ACL size",
        ));
    }
    // The C implementation writes a kauth_filesec struct, so provide a
    // suitably aligned allocation rather than relying on Vec<u8> alignment.
    let mut storage = vec![0u64; (size as usize).div_ceil(8)];
    let written = unsafe { acl_copy_ext(storage.as_mut_ptr().cast(), acl.0, size) };
    macos_acl_copy_result(written, size, io::Error::last_os_error)?;
    let encoded =
        unsafe { std::slice::from_raw_parts(storage.as_ptr().cast::<u8>(), size as usize) };
    check_macos_acl_encoding(encoded, |id| {
        let mut entry = std::ptr::null_mut();
        // The exported count is authoritative for this private snapshot. Any
        // failure, including EINVAL at a valid index, is an error, not EOF.
        if unsafe { acl_get_entry(acl.0, id, &mut entry) } != 0 {
            return Err(io::Error::last_os_error());
        }
        if entry.is_null() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid ACL entry",
            ));
        }
        let mut tag = 0;
        if unsafe { acl_get_tag_type(entry, &mut tag) } != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(tag)
    })
}

#[cfg(target_os = "macos")]
fn macos_acl_property_present(
    stat: impl FnOnce() -> io::Result<()>,
    mut query: impl FnMut(i32) -> io::Result<bool>,
) -> io::Result<bool> {
    stat()?;
    // Darwin statx1 may return success after an ACL-buffer realloc failure,
    // without populating any properties. Never interpret that as no ACL.
    for property in [1, 2, 4] {
        // FILESEC_OWNER, FILESEC_GROUP, FILESEC_MODE
        if !query(property)? {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "incomplete shared gate security snapshot",
            ));
        }
    }
    query(5) // FILESEC_ACL
}

#[cfg(target_os = "macos")]
fn macos_acl_property_value(
    get: impl FnOnce() -> io::Result<*mut std::ffi::c_void>,
) -> io::Result<*mut std::ffi::c_void> {
    let acl = get()?;
    if acl.is_null() || acl as usize == 1 {
        // _FILESEC_REMOVE_ACL
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid shared gate ACL property",
        ));
    }
    Ok(acl)
}

#[cfg(target_os = "macos")]
fn macos_acl_size_result(size: isize, error: impl FnOnce() -> io::Error) -> io::Result<isize> {
    if size < 0 { Err(error()) } else { Ok(size) }
}

#[cfg(target_os = "macos")]
fn macos_acl_copy_result(
    written: isize,
    expected: isize,
    error: impl FnOnce() -> io::Error,
) -> io::Result<()> {
    if written < 0 {
        Err(error())
    } else if written != expected {
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid shared gate ACL export length",
        ))
    } else {
        Ok(())
    }
}

#[cfg(target_os = "macos")]
fn check_macos_acl_encoding(
    encoded: &[u8],
    mut get_tag: impl FnMut(i32) -> io::Result<i32>,
) -> io::Result<()> {
    fn word(bytes: &[u8]) -> u32 {
        u32::from_be_bytes(bytes.try_into().expect("fixed ACL field"))
    }
    let invalid = || {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid shared gate ACL encoding",
        )
    };
    if encoded.len() < 44 || word(&encoded[..4]) != 0x012c_c16d {
        return Err(invalid());
    }
    let count = word(&encoded[36..40]) as usize;
    if count > 128 || encoded.len() != 44 + count * 24 {
        return Err(invalid());
    }
    // Only KAUTH_ACL_NO_INHERIT is understood and safe. In particular,
    // KAUTH_ACL_DEFER_INHERIT and all private/unknown flags fail closed.
    if word(&encoded[40..44]) & !(1 << 17) != 0 {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "unsafe shared gate ACL flags",
        ));
    }
    for i in 0..count {
        let flags = word(&encoded[44 + i * 24 + 16..44 + i * 24 + 20]);
        let rights = word(&encoded[44 + i * 24 + 20..44 + i * 24 + 24]);
        // KAUTH_ACE_DENY plus the five documented inheritance flags only.
        // Rights: Darwin ACL bits 1..13, synchronize, and the documented
        // KAUTH generic bits. Unknown rights remain fail-closed.
        if flags & 0xf != 2
            || flags & !(0xf | 0x1f0) != 0
            || rights & !(0x3ffe | (1 << 20) | (0xf << 21)) != 0
            || get_tag(i as i32)? != 2
        {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "shared gate ACL contains an allow or unknown entry",
            ));
        }
    }
    Ok(())
}

#[cfg(all(test, windows))]
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

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
mod tests {
    use super::*;
    use std::os::unix::fs::{PermissionsExt, symlink};

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_gate_accepts_deny_only_and_rejects_allow_at_each_layer() {
        fn chmod(path: &Path, args: &[&str]) {
            let output = std::process::Command::new("/bin/chmod")
                .args(args)
                .arg(path)
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "chmod {}: {}",
                path.display(),
                String::from_utf8_lossy(&output.stderr)
            );
        }
        for layer in ["profile", "directory", "file"] {
            let root = tempfile::tempdir().unwrap();
            let profile = root.path().join("profile");
            std::fs::create_dir(&profile).unwrap();
            std::fs::set_permissions(&profile, std::fs::Permissions::from_mode(0o700)).unwrap();
            drop(open_in_profile(&profile).expect("ACL-free baseline"));
            let target = match layer {
                "profile" => profile.clone(),
                "directory" => profile.join(".pi-rs-shared"),
                _ => profile.join(".pi-rs-shared/mcp-config-gate.lock"),
            };
            // Deny a nonessential right so the current user can still open
            // the object and remove the fixture ACL during cleanup.
            chmod(&target, &["+a", "everyone deny readextattr"]);
            assert!(
                open_in_profile(&profile).is_ok(),
                "deny-only rejected at {layer}"
            );
            // A mixed ACL ensures the iterator cannot stop after the first deny.
            chmod(&target, &["+a", "everyone allow read"]);
            let error = open_in_profile(&profile).expect_err("allow must be rejected");
            assert_eq!(
                error.kind(),
                io::ErrorKind::PermissionDenied,
                "{layer}: {error}"
            );
            assert!(error.to_string().contains("ACL"), "{layer}: {error}");
            let expected = match layer {
                "profile" => "account profile:",
                "directory" => "gate directory:",
                _ => "gate file:",
            };
            assert!(error.to_string().starts_with(expected), "{layer}: {error}");
            chmod(&target, &["-N"]);
            assert!(open_in_profile(&profile).is_ok(), "ACL removal at {layer}");
        }
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_acl_encoding_rejects_unknown_flags_and_index_errors() {
        fn encoding(global: u32, entries: &[u32]) -> Vec<u8> {
            let mut bytes = vec![0; 44 + 24 * entries.len()];
            bytes[..4].copy_from_slice(&0x012c_c16du32.to_be_bytes());
            bytes[36..40].copy_from_slice(&(entries.len() as u32).to_be_bytes());
            bytes[40..44].copy_from_slice(&global.to_be_bytes());
            for (i, flags) in entries.iter().enumerate() {
                bytes[60 + i * 24..64 + i * 24].copy_from_slice(&flags.to_be_bytes());
            }
            bytes
        }
        assert!(check_macos_acl_encoding(&encoding(0, &[]), |_| panic!("no entries")).is_ok());
        assert!(check_macos_acl_encoding(&encoding(1 << 17, &[2 | (1 << 4)]), |_| Ok(2)).is_ok());
        for flags in [1 << 16, 1, 1 << 31] {
            assert!(check_macos_acl_encoding(&encoding(flags, &[]), |_| Ok(2)).is_err());
        }
        for flags in [1, 3, 2 | (1 << 9), 2 | (1 << 31)] {
            assert!(check_macos_acl_encoding(&encoding(0, &[flags]), |_| Ok(2)).is_err());
        }
        assert!(check_macos_acl_encoding(&encoding(0, &[2]), |_| Ok(99)).is_err());
        let error = check_macos_acl_encoding(&encoding(0, &[2]), |_| {
            Err(io::Error::from_raw_os_error(libc::EINVAL))
        })
        .unwrap_err();
        assert_eq!(error.raw_os_error(), Some(libc::EINVAL));
        let mut truncated = encoding(0, &[2]);
        truncated.pop();
        assert!(check_macos_acl_encoding(&truncated, |_| Ok(2)).is_err());
        let mut unknown_right = encoding(0, &[2]);
        unknown_right[64..68].copy_from_slice(&(1u32 << 31).to_be_bytes());
        assert!(check_macos_acl_encoding(&unknown_right, |_| Ok(2)).is_err());
        let mut known_rights = encoding(0, &[2]);
        known_rights[64..68].copy_from_slice(&((1u32 << 13) | (1 << 20)).to_be_bytes());
        assert!(check_macos_acl_encoding(&known_rights, |_| Ok(2)).is_ok());
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_acl_errors_are_distinct_from_absent_property() {
        let noent = || io::Error::from_raw_os_error(libc::ENOENT);
        let error = macos_acl_property_present(
            || Err(noent()),
            |_| panic!("must not query after failed stat"),
        )
        .unwrap_err();
        assert_eq!(error.raw_os_error(), Some(libc::ENOENT));
        assert!(!macos_acl_property_present(|| Ok(()), |property| Ok(property != 5)).unwrap());
        for missing in [1, 2, 4] {
            assert_eq!(
                macos_acl_property_present(
                    || Ok(()),
                    |property| Ok(property != missing && property != 5)
                )
                .unwrap_err()
                .kind(),
                io::ErrorKind::InvalidData
            );
        }
        assert_eq!(
            macos_acl_property_present(|| Ok(()), |_| Err(noent()))
                .unwrap_err()
                .raw_os_error(),
            Some(libc::ENOENT)
        );
        assert_eq!(
            macos_acl_property_value(|| Err(noent()))
                .unwrap_err()
                .raw_os_error(),
            Some(libc::ENOENT)
        );
        assert_eq!(
            macos_acl_property_value(|| Ok(std::ptr::null_mut()))
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidData
        );
        assert_eq!(
            macos_acl_property_value(|| Ok(std::ptr::without_provenance_mut::<std::ffi::c_void>(
                1
            )))
            .unwrap_err()
            .kind(),
            io::ErrorKind::InvalidData
        );
        let mut object = 0u8;
        let ptr = (&mut object as *mut u8).cast();
        assert_eq!(macos_acl_property_value(|| Ok(ptr)).unwrap(), ptr);
        assert_eq!(
            macos_acl_size_result(-1, noent).unwrap_err().raw_os_error(),
            Some(libc::ENOENT)
        );
        assert_eq!(
            macos_acl_copy_result(-1, 44, noent)
                .unwrap_err()
                .raw_os_error(),
            Some(libc::ENOENT)
        );
        assert_eq!(
            macos_acl_copy_result(43, 44, noent).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }

    async fn config_guard_in_profile(profile: &Path, config: &Path) -> pi_rs::Result<ConfigGuard> {
        let account = acquire_for_test(profile, None).await?;
        account_guard_with(account, &[config.to_path_buf()], None, true).await
    }

    #[tokio::test]
    async fn snapshot_uses_actual_config_guard_and_releases_it() {
        let root = tempfile::tempdir().unwrap();
        let profile = root.path().join("profile");
        std::fs::create_dir(&profile).unwrap();
        std::fs::set_permissions(&profile, std::fs::Permissions::from_mode(0o700)).unwrap();
        let config = root.path().join("mcp.json");
        std::fs::write(&config, "intermediate").unwrap();
        let held = config_guard_in_profile(&profile, &config).await.unwrap();
        let mut snapshot = Box::pin(with_config_guard(
            config_guard_in_profile(&profile, &config),
            || {
                let account_contender = open_in_profile(&profile)?;
                let config_contender = File::open(config.with_extension("lock"))?;
                assert!(matches!(
                    account_contender.try_lock(),
                    Err(std::fs::TryLockError::WouldBlock)
                ));
                assert!(matches!(
                    config_contender.try_lock(),
                    Err(std::fs::TryLockError::WouldBlock)
                ));
                Ok(std::fs::read_to_string(&config)?)
            },
        ));
        // The injected opener and first try_lock run in this poll. Pending is
        // observed while another real ConfigGuard owns the account gate.
        assert!(futures_util::poll!(snapshot.as_mut()).is_pending());
        std::fs::write(&config, "final").unwrap();
        drop(held);
        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_secs(5), snapshot)
                .await
                .unwrap()
                .unwrap(),
            "final"
        );
        let next = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            config_guard_in_profile(&profile, &config),
        )
        .await
        .unwrap()
        .unwrap();
        drop(next);
        let result = with_config_guard(config_guard_in_profile(&profile, &config), || {
            Err::<(), _>(pi_rs::Error::Config("fixture".into()))
        })
        .await;
        assert!(result.is_err());
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            config_guard_in_profile(&profile, &config),
        )
        .await
        .unwrap()
        .unwrap();
    }

    #[tokio::test]
    async fn snapshot_cancel_and_timeout_do_not_run_after_guard_release() {
        use std::sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        };

        let root = tempfile::tempdir().unwrap();
        let profile = root.path().join("profile");
        std::fs::create_dir(&profile).unwrap();
        std::fs::set_permissions(&profile, std::fs::Permissions::from_mode(0o700)).unwrap();
        let config = root.path().join("mcp.json");
        let held = config_guard_in_profile(&profile, &config).await.unwrap();
        let ran = Arc::new(AtomicBool::new(false));
        let mut cancelled = Box::pin(with_config_guard(
            config_guard_in_profile(&profile, &config),
            || {
                ran.store(true, Ordering::SeqCst);
                Ok(())
            },
        ));
        assert!(futures_util::poll!(cancelled.as_mut()).is_pending());
        drop(cancelled);

        let mut timed = Box::pin(with_config_guard(
            config_guard_in_profile(&profile, &config),
            || {
                ran.store(true, Ordering::SeqCst);
                Ok(())
            },
        ));
        assert!(futures_util::poll!(timed.as_mut()).is_pending());
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), timed)
                .await
                .is_err()
        );
        let task_profile = profile.clone();
        let task_config = config.clone();
        let observed = ran.clone();
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let mut snapshot = Box::pin(with_config_guard(
                config_guard_in_profile(&task_profile, &task_config),
                || {
                    observed.store(true, Ordering::SeqCst);
                    Ok(())
                },
            ));
            assert!(futures_util::poll!(snapshot.as_mut()).is_pending());
            let _ = ready_tx.send(());
            snapshot.await
        });
        tokio::time::timeout(std::time::Duration::from_secs(5), ready_rx)
            .await
            .unwrap()
            .unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        drop(held);
        let next = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            config_guard_in_profile(&profile, &config),
        )
        .await
        .unwrap()
        .unwrap();
        drop(next);
        tokio::task::yield_now().await;
        assert!(!ran.load(Ordering::SeqCst));
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[tokio::test]
    async fn injected_account_gate_serializes_distinct_homes_and_cancels_waiter() {
        let root = tempfile::tempdir().unwrap();
        let profile = root.path().join("profile");
        std::fs::create_dir(&profile).unwrap();
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&profile, std::fs::Permissions::from_mode(0o700)).unwrap();
        let first = acquire_for_test(&profile, None).await.unwrap();
        let project = root.path().join("project/mcp.json");
        let guard = account_guard_with(
            first,
            &[root.path().join("home-a/mcp.json"), project.clone()],
            None,
            false,
        )
        .await
        .unwrap();
        let waiting = tokio::time::timeout(
            std::time::Duration::from_millis(150),
            acquire_for_test(&profile, None),
        )
        .await;
        assert!(waiting.is_err());
        drop(guard);
        let second = acquire_for_test(&profile, None).await.unwrap();
        let _guard = account_guard_with(
            second,
            &[root.path().join("home-b/mcp.json"), project],
            None,
            false,
        )
        .await
        .unwrap();
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[tokio::test]
    async fn injected_gate_acquires_duplicate_legacy_locks_once() {
        use std::os::unix::fs::PermissionsExt;
        let root = tempfile::tempdir().unwrap();
        let profile = root.path().join("profile");
        std::fs::create_dir(&profile).unwrap();
        std::fs::set_permissions(&profile, std::fs::Permissions::from_mode(0o700)).unwrap();
        let first = root.path().join("first/mcp.json");
        let second = root.path().join("second/mcp.json");
        let _held = open_mcp_lock(&first.with_extension("lock")).unwrap();
        std::fs::create_dir_all(second.parent().unwrap()).unwrap();
        std::fs::hard_link(first.with_extension("lock"), second.with_extension("lock")).unwrap();
        let account = acquire_for_test(&profile, None).await.unwrap();
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            account_guard_with(account, &[first, second], None, false),
        )
        .await;
        assert!(result.expect("duplicate must not deadlock").is_ok());
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[tokio::test]
    async fn injected_gate_rejects_credential_lock_alias_before_waiting() {
        use std::os::unix::fs::PermissionsExt;
        let root = tempfile::tempdir().unwrap();
        let profile = root.path().join("profile");
        std::fs::create_dir(&profile).unwrap();
        std::fs::set_permissions(&profile, std::fs::Permissions::from_mode(0o700)).unwrap();
        let credential_path = root.path().join("credentials.lock");
        let credential = open_mcp_lock(&credential_path).unwrap();
        credential.lock().unwrap();
        let config = root.path().join("mcp.json");
        std::fs::hard_link(&credential_path, config.with_extension("lock")).unwrap();
        let account = acquire_for_test(&profile, Some(&credential)).await.unwrap();
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            account_guard_with(account, &[config], Some(&credential), false),
        )
        .await;
        assert!(result.expect("credential alias must not deadlock").is_err());
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[tokio::test]
    async fn injected_gate_rejects_account_lock_alias_before_waiting() {
        use std::os::unix::fs::PermissionsExt;
        let root = tempfile::tempdir().unwrap();
        let profile = root.path().join("profile");
        std::fs::create_dir(&profile).unwrap();
        std::fs::set_permissions(&profile, std::fs::Permissions::from_mode(0o700)).unwrap();
        let account = acquire_for_test(&profile, None).await.unwrap();
        let config = root.path().join("mcp.json");
        std::fs::hard_link(
            profile.join(".pi-rs-shared/mcp-config-gate.lock"),
            config.with_extension("lock"),
        )
        .unwrap();
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            account_guard_with(account, &[config], None, false),
        )
        .await;
        assert!(result.expect("account alias must not deadlock").is_err());
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[tokio::test]
    async fn injected_read_guard_locks_existing_readonly_file() {
        use std::os::unix::fs::PermissionsExt;
        let root = tempfile::tempdir().unwrap();
        let profile = root.path().join("profile");
        std::fs::create_dir(&profile).unwrap();
        std::fs::set_permissions(&profile, std::fs::Permissions::from_mode(0o700)).unwrap();
        let config = root.path().join("mcp.json");
        let lock_path = config.with_extension("lock");
        std::fs::File::create(&lock_path).unwrap();
        std::fs::set_permissions(&lock_path, std::fs::Permissions::from_mode(0o444)).unwrap();
        let account = acquire_for_test(&profile, None).await.unwrap();
        let guard = account_guard_with(account, &[config], None, true)
            .await
            .unwrap();
        let contender = std::fs::File::open(&lock_path).unwrap();
        assert!(matches!(
            contender.try_lock(),
            Err(std::fs::TryLockError::WouldBlock)
        ));
        drop(guard);
        contender.try_lock().unwrap();
    }

    #[test]
    fn readonly_fallback_distinguishes_missing_parent_and_contention() {
        let root = tempfile::tempdir().unwrap();
        let missing = root.path().join("missing/mcp.lock");
        assert!(weak_read_lock_error(
            &missing,
            &std::io::Error::from(std::io::ErrorKind::NotFound)
        ));
        assert!(!weak_read_lock_error(
            &missing,
            &std::io::Error::from(std::io::ErrorKind::WouldBlock)
        ));
        assert!(!weak_read_lock_error(
            &root.path().join("mcp.lock"),
            &std::io::Error::from(std::io::ErrorKind::NotFound)
        ));
        #[cfg(unix)]
        assert!(weak_read_lock_error(
            &missing,
            &std::io::Error::from_raw_os_error(libc::EROFS)
        ));
    }

    #[tokio::test]
    async fn blocked_account_lookup_can_be_cancelled() {
        static SLOTS: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(1);
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("gate");
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let (finished_tx, finished_rx) = tokio::sync::oneshot::channel();
        let mut lookup = Box::pin(open_off_runtime_with_slots(&SLOTS, move || {
            let _ = started_tx.send(());
            let _ = release_rx.recv();
            let result = File::create(path);
            let _ = finished_tx.send(());
            result
        }));
        tokio::select! {
            _ = started_rx => (),
            _ = &mut lookup => panic!("lookup unexpectedly completed"),
        }
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), &mut lookup)
                .await
                .is_err()
        );
        drop(lookup);
        assert!(SLOTS.try_acquire().is_err());
        let waiting = open_off_runtime_with_timeout(
            &SLOTS,
            || panic!("no slot was available"),
            std::time::Duration::from_millis(100),
        )
        .await;
        assert_eq!(waiting.unwrap_err().kind(), io::ErrorKind::TimedOut);
        release_tx.send(()).unwrap();
        finished_rx.await.unwrap();
        let _permit = tokio::time::timeout(std::time::Duration::from_secs(1), SLOTS.acquire())
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn blocked_account_lookup_times_out_without_releasing_worker_slot() {
        static SLOTS: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(1);
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("gate");
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let result = open_off_runtime_with_timeout(
            &SLOTS,
            move || {
                let _ = started_tx.send(());
                let _ = release_rx.recv();
                File::create(path)
            },
            std::time::Duration::from_millis(100),
        )
        .await;
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::TimedOut);
        started_rx.await.unwrap();
        assert!(SLOTS.try_acquire().is_err());
        release_tx.send(()).unwrap();
        let _permit = tokio::time::timeout(std::time::Duration::from_secs(1), SLOTS.acquire())
            .await
            .unwrap()
            .unwrap();
    }

    struct ChildGuard(std::process::Child);
    impl Drop for ChildGuard {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
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
        const CHILD: &str = "PI_RS_TEST_GATE_HANDLE_CHILD";
        if let Some(root) = std::env::var_os(CHILD) {
            let root = PathBuf::from(root);
            let first = open_in_profile(&root).unwrap();
            let second = open_in_profile(&root).unwrap();
            first.try_lock().unwrap();
            assert!(matches!(
                second.try_lock(),
                Err(std::fs::TryLockError::WouldBlock)
            ));
            drop(first);
            let until = std::time::Instant::now() + std::time::Duration::from_secs(2);
            loop {
                match second.try_lock() {
                    Ok(()) => {
                        std::fs::write(root.join("handle-test-complete"), b"ok").unwrap();
                        return;
                    }
                    Err(std::fs::TryLockError::WouldBlock) => {
                        assert!(std::time::Instant::now() < until, "gate was not released");
                        std::thread::sleep(std::time::Duration::from_millis(10));
                    }
                    Err(error) => panic!("gate lock failed: {error:?}"),
                }
            }
        }
        let root = TestDir::new();
        let mut child = ChildGuard(
            std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "mcp_shared_gate::tests::gate_handles_contend_and_reopen",
                ])
                .env(CHILD, &root.0)
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .spawn()
                .unwrap(),
        );
        let until = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            if let Some(status) = child.0.try_wait().unwrap() {
                assert!(status.success(), "gate child failed: {status}");
                assert!(root.0.join("handle-test-complete").exists());
                break;
            }
            assert!(std::time::Instant::now() < until, "gate child did not exit");
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }
    #[test]
    fn account_profile_does_not_follow_agent_or_home_environment() {
        use std::os::unix::ffi::OsStrExt;
        const CHILD: &str = "PI_RS_TEST_SHARED_GATE_PROFILE_CHILD";
        const OUTPUT: &str = "PI_RS_TEST_SHARED_GATE_PROFILE_OUTPUT";
        if std::env::var_os(CHILD).is_some() {
            let profile = account_profile().unwrap();
            std::fs::write(
                std::env::var_os(OUTPUT).unwrap(),
                profile.as_os_str().as_bytes(),
            )
            .unwrap();
            return;
        }
        let root = TestDir::new();
        for index in 0..2 {
            let mut child = ChildGuard(std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "mcp_shared_gate::tests::account_profile_does_not_follow_agent_or_home_environment"])
                .env(CHILD, "1")
                .env(OUTPUT, root.0.join(format!("profile-{index}")))
                .env("HOME", root.0.join(format!("home-{index}")))
                .env("PI_RS_HOME", root.0.join(format!("agent-{index}")))
                .env("PI_CODING_AGENT_DIR", root.0.join(format!("coding-agent-{index}")))
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .spawn().unwrap());
            let until = std::time::Instant::now() + std::time::Duration::from_secs(10);
            loop {
                if let Some(status) = child.0.try_wait().unwrap() {
                    assert!(status.success(), "profile child failed: {status}");
                    break;
                }
                assert!(
                    std::time::Instant::now() < until,
                    "profile child did not exit"
                );
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
        }
        let first = std::fs::read(root.0.join("profile-0")).unwrap();
        let second = std::fs::read(root.0.join("profile-1")).unwrap();
        assert!(!first.is_empty());
        assert_eq!(first, second);
    }

    #[test]
    fn gate_contends_between_processes_with_different_agent_directories() {
        use std::time::{Duration, Instant};
        const CHILD: &str = "PI_RS_TEST_SHARED_GATE_PROCESS_CHILD";
        if let Some(root) = std::env::var_os(CHILD) {
            let root = PathBuf::from(root);
            let gate = open_in_profile(&root).unwrap();
            gate.lock().unwrap();
            std::fs::write(root.join("ready"), b"ready").unwrap();
            let until = Instant::now() + Duration::from_secs(15);
            while !root.join("release").exists() {
                assert!(Instant::now() < until, "parent did not release child");
                std::thread::sleep(Duration::from_millis(10));
            }
            return;
        }
        let root = TestDir::new();
        let mut child = ChildGuard(std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "mcp_shared_gate::tests::gate_contends_between_processes_with_different_agent_directories"])
            .env(CHILD, &root.0)
            .env("HOME", root.0.join("other-home"))
            .env("PI_RS_HOME", root.0.join("other-agent"))
            .env("PI_CODING_AGENT_DIR", root.0.join("other-coding-agent"))
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn().unwrap());
        let until = Instant::now() + Duration::from_secs(10);
        while !root.0.join("ready").exists() {
            assert!(
                child.0.try_wait().unwrap().is_none(),
                "child exited before acquiring gate"
            );
            assert!(Instant::now() < until, "child did not acquire gate");
            std::thread::sleep(Duration::from_millis(10));
        }
        let gate = open_in_profile(&root.0).unwrap();
        assert!(matches!(
            gate.try_lock(),
            Err(std::fs::TryLockError::WouldBlock)
        ));
        std::fs::write(root.0.join("release"), b"release").unwrap();
        let until = Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(status) = child.0.try_wait().unwrap() {
                assert!(status.success(), "child failed after release");
                break;
            }
            assert!(Instant::now() < until, "child did not exit");
            std::thread::sleep(Duration::from_millis(10));
        }
        gate.try_lock().unwrap();
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

    fn open_mutex(test_name: Option<&str>) -> io::Result<Handle> {
        let user = user()?;
        let sd = descriptor(&user)?;
        let attrs = SECURITY_ATTRIBUTES {
            nLength: std::mem::size_of::<SECURITY_ATTRIBUTES>() as u32,
            lpSecurityDescriptor: sd.0,
            bInheritHandle: 0,
        };
        let name = wide(
            &test_name
                .map(str::to_owned)
                .unwrap_or_else(|| format!("Global\\pi-rs-mcp-config-v1-{}", user.text)),
        );
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
        acquire_inner(
            None,
            #[cfg(test)]
            None,
        )
        .await
    }

    /// Hooks are available only in tests. `wait_started` fires after an actual
    /// timed-out OS wait, while `exited` fires only after joining the worker.
    #[cfg(test)]
    pub(crate) struct WorkerHooks {
        pub(crate) wait_started: tokio::sync::oneshot::Sender<()>,
        pub(crate) exited: tokio::sync::oneshot::Sender<bool>,
    }

    #[cfg(test)]
    pub(crate) async fn acquire_named_for_test(
        name: String,
        hooks: Option<WorkerHooks>,
    ) -> io::Result<Guard> {
        acquire_inner(Some(name), hooks).await
    }

    async fn acquire_inner(
        test_name: Option<String>,
        #[cfg(test)] hooks: Option<WorkerHooks>,
    ) -> io::Result<Guard> {
        let cancelled = Arc::new(AtomicBool::new(false));
        let _cancel = Cancel(cancelled.clone());
        let (result_tx, result_rx) = mpsc::sync_channel::<io::Result<bool>>(1);
        let (release_tx, release_rx) = mpsc::channel::<bool>();
        #[cfg(test)]
        let (mut started, exited) = match hooks {
            Some(hooks) => (Some(hooks.wait_started), Some(hooks.exited)),
            None => (None, None),
        };
        let worker = std::thread::Builder::new()
            .name("pi-mcp-gate".into())
            .spawn(move || {
                let result = (|| -> io::Result<()> {
                    let mutex = open_mutex(test_name.as_deref())?;
                    loop {
                        if cancelled.load(Ordering::Acquire) {
                            return Ok(());
                        }
                        let wait = unsafe { WaitForSingleObject(mutex.0, 50) };
                        match wait {
                            WAIT_TIMEOUT => {
                                #[cfg(test)]
                                if let Some(started) = started.take() {
                                    let _ = started.send(());
                                }
                                continue;
                            }
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
        #[cfg(test)]
        if let Some(exited) = exited {
            std::thread::Builder::new()
                .name("pi-mcp-test-join".into())
                .spawn(move || {
                    let _ = exited.send(worker.join().is_ok());
                })?;
        }
        #[cfg(not(test))]
        drop(worker);
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
}

#[cfg(all(test, windows))]
mod windows_tests {
    use super::*;

    async fn config_guard_named(
        name: String,
        config: &Path,
        hooks: Option<windows_mutex::WorkerHooks>,
    ) -> pi_rs::Result<ConfigGuard> {
        let mutex = windows_mutex::acquire_named_for_test(name, hooks).await?;
        if mutex.abandoned {
            return Err(io::Error::other("abandoned MCP gate requires recovery").into());
        }
        account_guard_with(
            AccountGuard { _mutex: mutex },
            &[config.to_path_buf()],
            None,
            true,
        )
        .await
    }

    #[tokio::test]
    async fn snapshot_cancel_releases_named_mutex_worker() {
        use std::sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        };
        use std::time::Duration;

        let root = tempfile::tempdir().unwrap();
        let config = root.path().join("mcp.json");
        let name = format!("Global\\pi-rs-mcp-snapshot-test-{}", uuid::Uuid::new_v4());
        let held = tokio::time::timeout(
            Duration::from_secs(10),
            config_guard_named(name.clone(), &config, None),
        )
        .await
        .unwrap()
        .unwrap();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (exited_tx, exited_rx) = tokio::sync::oneshot::channel();
        let ran = Arc::new(AtomicBool::new(false));
        let observed = ran.clone();
        let waiter_name = name.clone();
        let waiter_config = config.clone();
        let task = tokio::spawn(async move {
            with_config_guard(
                config_guard_named(
                    waiter_name,
                    &waiter_config,
                    Some(windows_mutex::WorkerHooks {
                        wait_started: started_tx,
                        exited: exited_tx,
                    }),
                ),
                || {
                    observed.store(true, Ordering::SeqCst);
                    Ok(())
                },
            )
            .await
        });
        tokio::time::timeout(Duration::from_secs(10), started_rx)
            .await
            .unwrap()
            .unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert!(
            tokio::time::timeout(Duration::from_secs(10), exited_rx)
                .await
                .unwrap()
                .unwrap()
        );
        drop(held);
        let next = tokio::time::timeout(
            Duration::from_secs(10),
            config_guard_named(name, &config, None),
        )
        .await
        .unwrap()
        .unwrap();
        drop(next);
        assert!(!ran.load(Ordering::SeqCst));
    }

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
