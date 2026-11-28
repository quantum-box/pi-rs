use std::{fs::File, io, os::windows::io::AsRawHandle};
use windows_sys::Win32::Storage::FileSystem::{
    FILE_ID_INFO, FileIdInfo, GetFileInformationByHandleEx,
};

#[derive(PartialEq, Eq)]
pub(crate) struct FileIdentity {
    volume: u64,
    id: [u8; 16],
}

impl FileIdentity {
    pub(crate) fn from_file(file: &File) -> io::Result<Self> {
        let mut info = FILE_ID_INFO::default();
        // The borrowed handle is valid and info is a writable buffer of the specified size.
        let ok = unsafe {
            GetFileInformationByHandleEx(
                file.as_raw_handle() as _,
                FileIdInfo,
                std::ptr::addr_of_mut!(info).cast(),
                std::mem::size_of::<FILE_ID_INFO>() as u32,
            )
        };
        if ok == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Self {
            volume: info.VolumeSerialNumber,
            id: info.FileId.Identifier,
        })
    }
}
