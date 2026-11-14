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

#[cfg(test)]
mod tests {
    use super::*;

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
