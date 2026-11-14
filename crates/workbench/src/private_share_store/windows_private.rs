//! Windows DPAPI primitives for a future private share store.
//!
//! Encryption alone does not provide generation invalidation, safe directory
//! ownership, or reparse-point protection. These functions are not a store
//! and must not be used as a replacement for those checks.
use std::io;
use windows_sys::Win32::Foundation::LocalFree;
use windows_sys::Win32::Security::Cryptography::{
    CRYPT_INTEGER_BLOB, CRYPTPROTECT_UI_FORBIDDEN, CryptProtectData, CryptUnprotectData,
};

const MAX_PROTECTED_BYTES: usize = 16 * 1024 * 1024;

fn transform(input: &[u8], protect: bool) -> io::Result<Vec<u8>> {
    if input.len() > MAX_PROTECTED_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "share data exceeds DPAPI limit",
        ));
    }
    let blob = CRYPT_INTEGER_BLOB {
        cbData: u32::try_from(input.len()).map_err(|_| io::ErrorKind::InvalidData)?,
        pbData: input.as_ptr() as *mut u8,
    };
    let mut output = CRYPT_INTEGER_BLOB::default();
    // SAFETY: the input points to initialized memory, the output is a valid
    // out parameter, and all optional parameters are null.
    let success = unsafe {
        if protect {
            CryptProtectData(
                &blob,
                std::ptr::null(),
                std::ptr::null(),
                std::ptr::null(),
                std::ptr::null(),
                CRYPTPROTECT_UI_FORBIDDEN,
                &mut output,
            )
        } else {
            CryptUnprotectData(
                &blob,
                std::ptr::null_mut(),
                std::ptr::null(),
                std::ptr::null(),
                std::ptr::null_mut(),
                CRYPTPROTECT_UI_FORBIDDEN,
                &mut output,
            )
        }
    };
    if success == 0 {
        return Err(io::Error::last_os_error());
    }
    if output.pbData.is_null() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "DPAPI returned no buffer",
        ));
    }
    let length = output.cbData as usize;
    // SAFETY: successful DPAPI calls allocate `length` initialized bytes.
    let bytes = unsafe { std::slice::from_raw_parts(output.pbData, length) };
    let result = if length > MAX_PROTECTED_BYTES {
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "DPAPI output exceeds limit",
        ))
    } else {
        Ok(bytes.to_vec())
    };
    // SAFETY: DPAPI owns this writable buffer until LocalFree; volatile writes
    // reduce retention of the plaintext in that allocation.
    unsafe {
        for index in 0..length {
            std::ptr::write_volatile(output.pbData.add(index), 0);
        }
        LocalFree(output.pbData.cast());
    }
    result
}

/// Protect bytes for the current Windows user without displaying UI.
pub fn protect(input: &[u8]) -> io::Result<Vec<u8>> {
    transform(input, true)
}

/// Reject data that DPAPI cannot authenticate for the current user.
pub fn unprotect(input: &[u8]) -> io::Result<Vec<u8>> {
    transform(input, false)
}

/// A verified directory handle with no-delete-sharing handles to each
/// ancestor. File creation and generation locking still require separate
/// verification before this can serve as a complete private store.
pub struct VerifiedDirectory {
    _handle: std::fs::File,
    _ancestors: Vec<std::fs::File>,
    path: std::path::PathBuf,
}

struct LocalDescriptor(*mut core::ffi::c_void);
impl Drop for LocalDescriptor {
    fn drop(&mut self) {
        // SAFETY: GetSecurityInfo and SDDL conversion allocate with LocalAlloc.
        unsafe {
            LocalFree(self.0.cast());
        }
    }
}

struct Token(windows_sys::Win32::Foundation::HANDLE);
impl Drop for Token {
    fn drop(&mut self) {
        // SAFETY: this is an owned handle from OpenProcessToken.
        unsafe {
            windows_sys::Win32::Foundation::CloseHandle(self.0);
        }
    }
}

fn private_directory_error(detail: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::PermissionDenied, detail)
}

fn current_user_token_info() -> io::Result<Vec<usize>> {
    use windows_sys::Win32::Security::{GetTokenInformation, TOKEN_QUERY, TOKEN_USER, TokenUser};
    use windows_sys::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};
    let mut token = std::ptr::null_mut();
    // SAFETY: the current process pseudo-handle and output pointer are valid.
    if unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) } == 0 {
        return Err(io::Error::last_os_error());
    }
    let token = Token(token);
    let mut needed = 0;
    // SAFETY: a null buffer requests the required size.
    unsafe {
        GetTokenInformation(token.0, TokenUser, std::ptr::null_mut(), 0, &mut needed);
    }
    if needed < std::mem::size_of::<TOKEN_USER>() as u32 || needed > 1024 * 1024 {
        return Err(private_directory_error("token user size"));
    }
    let mut info = vec![0usize; (needed as usize).div_ceil(std::mem::size_of::<usize>())];
    // SAFETY: info is aligned and has at least `needed` writable bytes.
    if unsafe {
        GetTokenInformation(
            token.0,
            TokenUser,
            info.as_mut_ptr().cast(),
            needed,
            &mut needed,
        )
    } == 0
    {
        return Err(io::Error::last_os_error());
    }
    Ok(info)
}

fn owner_descriptor(inheritable: bool) -> io::Result<LocalDescriptor> {
    use windows_sys::Win32::Security::Authorization::{
        ConvertSidToStringSidW, ConvertStringSecurityDescriptorToSecurityDescriptorW,
        SDDL_REVISION_1,
    };
    use windows_sys::Win32::Security::{IsValidSid, TOKEN_USER};
    let token_info = current_user_token_info()?;
    // SAFETY: current_user_token_info returned a populated TOKEN_USER.
    let user = unsafe { (*(token_info.as_ptr().cast::<TOKEN_USER>())).User.Sid };
    if user.is_null() || unsafe { IsValidSid(user) } == 0 {
        return Err(private_directory_error("invalid token user SID"));
    }
    let mut sid_string = std::ptr::null_mut();
    // SAFETY: user points into token_info and the output pointer is valid.
    if unsafe { ConvertSidToStringSidW(user, &mut sid_string) } == 0 {
        return Err(io::Error::last_os_error());
    }
    let sid_allocation = LocalDescriptor(sid_string.cast());
    let mut length = 0usize;
    // SID strings are NUL-terminated ASCII; bound the scan defensively.
    while length < 1024 && unsafe { *sid_string.add(length) } != 0 {
        length += 1;
    }
    if length == 1024 {
        return Err(private_directory_error("invalid token SID string"));
    }
    // SAFETY: ConvertSidToStringSidW returned length initialized UTF-16 units.
    let sid = String::from_utf16(unsafe { std::slice::from_raw_parts(sid_string, length) })
        .map_err(|_| private_directory_error("invalid token SID string"))?;
    let inheritance = if inheritable { "OICI" } else { "" };
    let sddl: Vec<u16> =
        format!("O:{sid}D:P(A;{inheritance};FA;;;{sid})(A;{inheritance};FA;;;SY)\0")
            .encode_utf16()
            .collect();
    drop(sid_allocation);
    let mut descriptor = std::ptr::null_mut();
    // SAFETY: valid NUL-terminated SDDL and writable out pointer.
    if unsafe {
        ConvertStringSecurityDescriptorToSecurityDescriptorW(
            sddl.as_ptr(),
            SDDL_REVISION_1,
            &mut descriptor,
            std::ptr::null_mut(),
        )
    } == 0
    {
        return Err(io::Error::last_os_error());
    }
    Ok(LocalDescriptor(descriptor))
}

fn verify_private_acl(file: &std::fs::File, directory: bool) -> io::Result<()> {
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Security::Authorization::{GetSecurityInfo, SE_FILE_OBJECT};
    use windows_sys::Win32::Security::{
        ACCESS_ALLOWED_ACE, ACL, ACL_SIZE_INFORMATION, AclSizeInformation, CONTAINER_INHERIT_ACE,
        DACL_SECURITY_INFORMATION, EqualSid, GetAce, GetAclInformation,
        GetSecurityDescriptorControl, INHERITED_ACE, IsValidSid, IsWellKnownSid,
        OBJECT_INHERIT_ACE, OWNER_SECURITY_INFORMATION, PSID, SE_DACL_PROTECTED, TOKEN_USER,
        WinCreatorOwnerRightsSid, WinLocalSystemSid,
    };
    let token_info = current_user_token_info()?;
    // SAFETY: current_user_token_info returned a populated TOKEN_USER.
    let user = unsafe { (*(token_info.as_ptr().cast::<TOKEN_USER>())).User.Sid };
    let mut owner: PSID = std::ptr::null_mut();
    let mut dacl: *mut ACL = std::ptr::null_mut();
    let mut descriptor = std::ptr::null_mut();
    // SAFETY: file is a live handle and output pointers are valid.
    let status = unsafe {
        GetSecurityInfo(
            file.as_raw_handle().cast(),
            SE_FILE_OBJECT,
            OWNER_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION,
            &mut owner,
            std::ptr::null_mut(),
            &mut dacl,
            std::ptr::null_mut(),
            &mut descriptor,
        )
    };
    if status != 0 {
        return Err(io::Error::from_raw_os_error(status as i32));
    }
    let descriptor = LocalDescriptor(descriptor);
    if owner.is_null() || dacl.is_null() || user.is_null() {
        return Err(private_directory_error("missing owner or dacl"));
    }
    // SAFETY: SIDs are held by the descriptor and token buffer.
    if unsafe { IsValidSid(owner) == 0 || IsValidSid(user) == 0 || EqualSid(owner, user) == 0 } {
        return Err(private_directory_error("owner differs from token user"));
    }
    let mut control = 0u16;
    let mut revision = 0;
    // SAFETY: descriptor is valid and outputs are writable.
    if unsafe { GetSecurityDescriptorControl(descriptor.0, &mut control, &mut revision) } == 0 {
        return Err(io::Error::last_os_error());
    }
    if control & SE_DACL_PROTECTED == 0 {
        return Err(private_directory_error("dacl is not protected"));
    }
    let mut info = ACL_SIZE_INFORMATION::default();
    // SAFETY: dacl is provided by GetSecurityInfo and info is writable.
    if unsafe {
        GetAclInformation(
            dacl,
            (&mut info as *mut ACL_SIZE_INFORMATION).cast(),
            std::mem::size_of::<ACL_SIZE_INFORMATION>() as u32,
            AclSizeInformation,
        )
    } == 0
    {
        return Err(io::Error::last_os_error());
    }
    if info.AceCount == 0 || info.AceCount > 64 {
        return Err(private_directory_error("unexpected ace count"));
    }
    let mut has_owner = false;
    for index in 0..info.AceCount {
        let mut entry: *mut core::ffi::c_void = std::ptr::null_mut();
        // SAFETY: index is bounded by the ACL's reported AceCount.
        if unsafe { GetAce(dacl, index, &mut entry) } == 0 || entry.is_null() {
            return Err(private_directory_error("cannot obtain ace"));
        }
        let ace = entry.cast::<ACCESS_ALLOWED_ACE>();
        // SAFETY: the ACL is returned by Windows; check the header before SID access.
        let header = unsafe { (*ace).Header };
        let required = (OBJECT_INHERIT_ACE | CONTAINER_INHERIT_ACE) as u8;
        if header.AceType != 0
            || header.AceSize < std::mem::size_of::<ACCESS_ALLOWED_ACE>() as u16
            || (directory && header.AceFlags & required != required)
            || header.AceFlags & INHERITED_ACE as u8 != 0
        {
            return Err(private_directory_error("unsupported ace flags or type"));
        }
        // SAFETY: ACCESS_ALLOWED_ACE stores an inline SID at SidStart.
        let sid = unsafe { std::ptr::addr_of_mut!((*ace).SidStart).cast() };
        // SAFETY: SID is within a Windows-supplied ACL; validity checked first.
        if unsafe { IsValidSid(sid) } == 0 {
            return Err(private_directory_error("invalid ace sid"));
        }
        let is_owner = unsafe {
            EqualSid(sid, user) != 0 || IsWellKnownSid(sid, WinCreatorOwnerRightsSid) != 0
        };
        let is_system = unsafe { IsWellKnownSid(sid, WinLocalSystemSid) != 0 };
        if !is_owner && !is_system {
            return Err(private_directory_error("unexpected ace principal"));
        }
        has_owner |= is_owner;
    }
    if !has_owner {
        return Err(private_directory_error("no owner ace"));
    }
    Ok(())
}

fn verify_private_file(file: &std::fs::File) -> io::Result<()> {
    use std::os::windows::fs::MetadataExt;
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Storage::FileSystem::{
        BY_HANDLE_FILE_INFORMATION, FILE_ATTRIBUTE_REPARSE_POINT, GetFileInformationByHandle,
    };
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
        return Err(private_directory_error("reparse or non-regular file"));
    }
    let mut information = BY_HANDLE_FILE_INFORMATION::default();
    // SAFETY: file is live and information is a valid writable structure.
    if unsafe { GetFileInformationByHandle(file.as_raw_handle().cast(), &mut information) } == 0 {
        return Err(io::Error::last_os_error());
    }
    if information.nNumberOfLinks != 1 {
        return Err(private_directory_error("hard-linked cache file"));
    }
    verify_private_acl(file, false)
}

impl VerifiedDirectory {
    /// Create a directory with a protected inheritable DACL for its owner and
    /// LocalSystem, then validate it. The caller must independently secure and
    /// pin the parent; this function is not a complete private store.
    pub fn create(path: &std::path::Path) -> io::Result<Self> {
        use std::os::windows::ffi::{OsStrExt, OsStringExt};
        use windows_sys::Win32::Security::SECURITY_ATTRIBUTES;
        use windows_sys::Win32::Storage::FileSystem::CreateDirectoryW;
        let descriptor = owner_descriptor(true)?;
        let attributes = SECURITY_ATTRIBUTES {
            nLength: std::mem::size_of::<SECURITY_ATTRIBUTES>() as u32,
            lpSecurityDescriptor: descriptor.0,
            bInheritHandle: 0,
        };
        let path: Vec<u16> = path.as_os_str().encode_wide().chain(Some(0)).collect();
        // SAFETY: path and security attributes remain valid for the call.
        if unsafe { CreateDirectoryW(path.as_ptr(), &attributes) } == 0 {
            return Err(io::Error::last_os_error());
        }
        // Opening from the same UTF-16 path avoids lossy conversion.
        let path = std::ffi::OsString::from_wide(&path[..path.len() - 1]);
        Self::open(std::path::Path::new(&path))
    }

    /// Reject reparse points, inherited/unprotected DACLs, unexpected allow
    /// principals, and directories not owned by the current token's user.
    /// Ancestor handles deny delete-sharing to keep the path anchored while
    /// the returned value is alive.
    pub fn open(path: &std::path::Path) -> io::Result<Self> {
        use std::os::windows::fs::{MetadataExt, OpenOptionsExt};
        use windows_sys::Win32::Storage::FileSystem::{
            FILE_ATTRIBUTE_REPARSE_POINT, FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT,
            FILE_SHARE_READ, FILE_SHARE_WRITE,
        };
        if !path.is_absolute()
            || path.components().any(|component| {
                matches!(
                    component,
                    std::path::Component::ParentDir | std::path::Component::CurDir
                )
            })
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "share cache path must be absolute and normalized",
            ));
        }
        let open_directory = |path: &std::path::Path| -> io::Result<std::fs::File> {
            let file = std::fs::OpenOptions::new()
                .read(true)
                .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
                .custom_flags(FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT)
                .open(path)?;
            let metadata = file.metadata()?;
            if !metadata.is_dir() || metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
            {
                return Err(private_directory_error("reparse or non-directory"));
            }
            Ok(file)
        };
        let mut ancestor_paths: Vec<_> = path.ancestors().skip(1).collect();
        ancestor_paths.reverse();
        let mut ancestors = Vec::with_capacity(ancestor_paths.len());
        for ancestor in ancestor_paths {
            ancestors.push(open_directory(ancestor)?);
        }
        let file = open_directory(path)?;
        verify_private_acl(&file, true)?;
        Ok(Self {
            _handle: file,
            _ancestors: ancestors,
            path: path.to_owned(),
        })
    }

    fn child_path(&self, name: &str) -> io::Result<std::path::PathBuf> {
        let stem = name.split('.').next().unwrap_or("");
        let reserved = [
            "CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7",
            "COM8", "COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
        ];
        if name.is_empty()
            || name.len() > 128
            || name.ends_with('.')
            || !name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
            || reserved
                .iter()
                .any(|entry| stem.eq_ignore_ascii_case(entry))
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid share cache filename",
            ));
        }
        Ok(self.path.join(name))
    }

    /// Create an owner-only regular file without following a reparse point.
    pub fn create_private_file(&self, name: &str) -> io::Result<std::fs::File> {
        verify_private_acl(&self._handle, true)?;
        use std::os::windows::ffi::OsStrExt;
        use std::os::windows::io::FromRawHandle;
        use windows_sys::Win32::Foundation::{GENERIC_READ, GENERIC_WRITE, INVALID_HANDLE_VALUE};
        use windows_sys::Win32::Security::SECURITY_ATTRIBUTES;
        use windows_sys::Win32::Storage::FileSystem::{
            CREATE_NEW, CreateFileW, FILE_ATTRIBUTE_NORMAL, FILE_FLAG_OPEN_REPARSE_POINT,
            FILE_SHARE_READ, FILE_SHARE_WRITE,
        };
        let path = self.child_path(name)?;
        let wide: Vec<u16> = path.as_os_str().encode_wide().chain(Some(0)).collect();
        let descriptor = owner_descriptor(false)?;
        let attributes = SECURITY_ATTRIBUTES {
            nLength: std::mem::size_of::<SECURITY_ATTRIBUTES>() as u32,
            lpSecurityDescriptor: descriptor.0,
            bInheritHandle: 0,
        };
        // SAFETY: path and attributes are valid; CREATE_NEW refuses an existing link.
        let handle = unsafe {
            CreateFileW(
                wide.as_ptr(),
                GENERIC_READ | GENERIC_WRITE,
                FILE_SHARE_READ | FILE_SHARE_WRITE,
                &attributes,
                CREATE_NEW,
                FILE_ATTRIBUTE_NORMAL | FILE_FLAG_OPEN_REPARSE_POINT,
                std::ptr::null_mut(),
            )
        };
        if handle == INVALID_HANDLE_VALUE {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: successful CreateFileW returns a new owned handle.
        let file = unsafe { std::fs::File::from_raw_handle(handle.cast()) };
        if let Err(error) = verify_private_file(&file) {
            drop(file);
            let _ = std::fs::remove_file(path);
            return Err(error);
        }
        Ok(file)
    }

    pub(super) fn replace_private_file(&self, source: &str, target: &str) -> io::Result<()> {
        use std::os::windows::ffi::OsStrExt;
        use windows_sys::Win32::Storage::FileSystem::{
            MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH, MoveFileExW,
        };
        verify_private_acl(&self._handle, true)?;
        let source: Vec<u16> = self
            .child_path(source)?
            .as_os_str()
            .encode_wide()
            .chain(Some(0))
            .collect();
        let target: Vec<u16> = self
            .child_path(target)?
            .as_os_str()
            .encode_wide()
            .chain(Some(0))
            .collect();
        // SAFETY: both paths are NUL-terminated children of the pinned directory.
        if unsafe {
            MoveFileExW(
                source.as_ptr(),
                target.as_ptr(),
                MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    pub(super) fn remove_private_file(&self, name: &str) -> io::Result<()> {
        verify_private_acl(&self._handle, true)?;
        match std::fs::remove_file(self.child_path(name)?) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error),
        }
    }

    /// Open an existing private file without following reparse points.
    pub fn open_private_file(&self, name: &str) -> io::Result<std::fs::File> {
        verify_private_acl(&self._handle, true)?;
        use std::os::windows::fs::OpenOptionsExt;
        use windows_sys::Win32::Storage::FileSystem::{
            FILE_FLAG_OPEN_REPARSE_POINT, FILE_SHARE_READ, FILE_SHARE_WRITE,
        };
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
            .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT)
            .open(self.child_path(name)?)?;
        verify_private_file(&file)?;
        Ok(file)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestDirectory(std::path::PathBuf);
    impl TestDirectory {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!(
                "pi-share-acl-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            std::fs::create_dir(&path).unwrap();
            Self(path)
        }
    }
    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn create_world_accessible(path: &std::path::Path) {
        use std::os::windows::ffi::OsStrExt;
        use windows_sys::Win32::Security::Authorization::{
            ConvertStringSecurityDescriptorToSecurityDescriptorW, SDDL_REVISION_1,
        };
        use windows_sys::Win32::Security::SECURITY_ATTRIBUTES;
        use windows_sys::Win32::Storage::FileSystem::CreateDirectoryW;
        let sddl: Vec<u16> = "D:P(A;OICI;FA;;;WD)\0".encode_utf16().collect();
        let mut descriptor = std::ptr::null_mut();
        // SAFETY: valid SDDL and writable output.
        assert_ne!(
            unsafe {
                ConvertStringSecurityDescriptorToSecurityDescriptorW(
                    sddl.as_ptr(),
                    SDDL_REVISION_1,
                    &mut descriptor,
                    std::ptr::null_mut(),
                )
            },
            0
        );
        let descriptor = LocalDescriptor(descriptor);
        let attributes = SECURITY_ATTRIBUTES {
            nLength: std::mem::size_of::<SECURITY_ATTRIBUTES>() as u32,
            lpSecurityDescriptor: descriptor.0,
            bInheritHandle: 0,
        };
        let path: Vec<u16> = path.as_os_str().encode_wide().chain(Some(0)).collect();
        // SAFETY: path and attributes remain live during the call.
        assert_ne!(unsafe { CreateDirectoryW(path.as_ptr(), &attributes) }, 0);
    }

    #[test]
    fn private_directory_accepts_owner_only_and_rejects_world_access() {
        let root = TestDirectory::new();
        let private = root.0.join("private");
        let directory = VerifiedDirectory::create(&private).unwrap();
        let second = VerifiedDirectory::open(&private).unwrap();
        drop(second);
        drop(directory);
        let public = root.0.join("public");
        create_world_accessible(&public);
        assert!(VerifiedDirectory::open(&public).is_err());
    }

    #[test]
    fn private_files_round_trip_and_reject_hard_links_and_path_escape() {
        use std::io::{Read, Write};
        let root = TestDirectory::new();
        let directory = VerifiedDirectory::create(&root.0.join("private")).unwrap();
        let mut file = directory.create_private_file("snapshot.dat").unwrap();
        file.write_all(b"private").unwrap();
        drop(file);
        let mut reopened = directory.open_private_file("snapshot.dat").unwrap();
        let mut contents = Vec::new();
        reopened.read_to_end(&mut contents).unwrap();
        assert_eq!(contents, b"private");
        drop(reopened);
        for name in ["../escape", "a\\b", "NUL", "trailing.", "x:stream"] {
            assert!(
                directory.create_private_file(name).is_err(),
                "accepted {name}"
            );
        }
        let original = root.0.join("private/snapshot.dat");
        let link = root.0.join("alias.dat");
        std::fs::hard_link(&original, &link).unwrap();
        assert!(directory.open_private_file("snapshot.dat").is_err());
        std::fs::remove_file(link).unwrap();
        drop(directory);
    }

    #[test]
    fn private_directory_pins_parent_against_rename() {
        let root = TestDirectory::new();
        let parent = root.0.join("parent");
        std::fs::create_dir(&parent).unwrap();
        let directory = VerifiedDirectory::create(&parent.join("private")).unwrap();
        let renamed = root.0.join("renamed");
        assert!(std::fs::rename(&parent, &renamed).is_err());
        drop(directory);
        std::fs::rename(&parent, &renamed).unwrap();
    }

    #[test]
    fn private_directory_rejects_reparse_ancestor() {
        let root = TestDirectory::new();
        let parent = root.0.join("real-parent");
        std::fs::create_dir(&parent).unwrap();
        let private = VerifiedDirectory::create(&parent.join("private")).unwrap();
        let junction = root.0.join("parent-junction");
        let status = std::process::Command::new("cmd")
            .arg("/C")
            .arg("mklink")
            .arg("/J")
            .arg(&junction)
            .arg(&parent)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .unwrap();
        assert!(status.success(), "could not create junction fixture");
        assert!(VerifiedDirectory::open(&junction.join("private")).is_err());
        std::fs::remove_dir(junction).unwrap();
        drop(private);
    }

    #[test]
    fn private_directory_rejects_junction() {
        let root = TestDirectory::new();
        let private = root.0.join("private");
        let verified = VerifiedDirectory::create(&private).unwrap();
        let junction = root.0.join("junction");
        let status = std::process::Command::new("cmd")
            .arg("/C")
            .arg("mklink")
            .arg("/J")
            .arg(&junction)
            .arg(&private)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .unwrap();
        assert!(status.success(), "could not create junction fixture");
        assert!(VerifiedDirectory::open(&junction).is_err());
        std::fs::remove_dir(junction).unwrap();
        drop(verified);
    }

    #[test]
    fn round_trip_and_reject_tampering() {
        let plaintext = b"private prompt and tool schema";
        let mut ciphertext = protect(plaintext).unwrap();
        assert!(
            !ciphertext
                .windows(plaintext.len())
                .any(|chunk| chunk == plaintext)
        );
        assert_eq!(unprotect(&ciphertext).unwrap(), plaintext);
        let index = ciphertext.len() / 2;
        ciphertext[index] ^= 1;
        assert!(unprotect(&ciphertext).is_err());
    }

    #[test]
    fn reject_oversized_input_before_calling_windows() {
        assert!(protect(&vec![0; MAX_PROTECTED_BYTES + 1]).is_err());
        assert!(unprotect(&vec![0; MAX_PROTECTED_BYTES + 1]).is_err());
    }
}
