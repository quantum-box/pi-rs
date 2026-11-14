//! Pi's content-only image detection for CLI attachments.
use std::io::Read;
use std::path::Path;

use pi_rs::{Error, Result};

const SNIFF_BYTES: u64 = 4100;
const PNG: &[u8] = b"\x89PNG\r\n\x1a\n";

pub(super) fn is_image_file(path: &Path) -> Result<bool> {
    if !path.is_file() {
        return Err(Error::Config("expected a regular file".into()));
    }
    let mut prefix = Vec::new();
    std::fs::File::open(path)?
        .take(SNIFF_BYTES)
        .read_to_end(&mut prefix)?;
    Ok(is_image(&prefix))
}

fn is_image(bytes: &[u8]) -> bool {
    if bytes.starts_with(&[0xff, 0xd8, 0xff]) {
        return bytes.get(3) != Some(&0xf7);
    }
    if bytes.starts_with(PNG) {
        if bytes.len() < 16 || bytes[8..12] != [0, 0, 0, 13] || &bytes[12..16] != b"IHDR" {
            return false;
        }
        let mut offset = 8usize;
        while offset + 8 <= bytes.len() {
            let length = u32::from_be_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
            let kind = &bytes[offset + 4..offset + 8];
            if kind == b"acTL" {
                return false;
            }
            if kind == b"IDAT" {
                return true;
            }
            let Some(next) = offset
                .checked_add(length)
                .and_then(|end| end.checked_add(12))
            else {
                break;
            };
            if next > bytes.len() {
                break;
            }
            offset = next;
        }
        return true;
    }
    if bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a") {
        return true;
    }
    if bytes.starts_with(b"RIFF") && bytes.get(8..12) == Some(b"WEBP") {
        return true;
    }
    bytes.starts_with(b"BM") && is_bmp(bytes)
}

fn is_bmp(bytes: &[u8]) -> bool {
    if bytes.len() < 26 {
        return false;
    }
    let u32_at = |i| u32::from_le_bytes(bytes[i..i + 4].try_into().unwrap());
    let size = u32_at(2);
    let offset = u32_at(10);
    let dib = u32_at(14);
    if (size != 0 && size < 26)
        || u64::from(offset) < 14 + u64::from(dib)
        || (size != 0 && offset >= size)
    {
        return false;
    }
    let start = if dib == 12 {
        22
    } else if (40..=124).contains(&dib) && bytes.len() >= 30 {
        26
    } else {
        return false;
    };
    let planes = u16::from_le_bytes(bytes[start..start + 2].try_into().unwrap());
    let bits = u16::from_le_bytes(bytes[start + 2..start + 4].try_into().unwrap());
    planes == 1 && [1, 4, 8, 16, 24, 32].contains(&bits)
}

#[cfg(test)]
mod tests {
    use super::is_image;

    fn bmp(dib: u32, planes: u16, bits: u16) -> Vec<u8> {
        let mut bytes = vec![0; 30];
        bytes[..2].copy_from_slice(b"BM");
        bytes[10..14].copy_from_slice(&(14 + dib).to_le_bytes());
        bytes[14..18].copy_from_slice(&dib.to_le_bytes());
        let start = if dib == 12 { 22 } else { 26 };
        bytes[start..start + 2].copy_from_slice(&planes.to_le_bytes());
        bytes[start + 2..start + 4].copy_from_slice(&bits.to_le_bytes());
        bytes
    }

    #[test]
    fn reference_signatures_and_bmp_boundaries() {
        for bytes in [
            &b"\xff\xd8\xff"[..],
            b"GIF87a",
            b"GIF89a",
            b"RIFF0000WEBP",
            bmp(12, 1, 24).as_slice(),
            bmp(40, 1, 32).as_slice(),
        ] {
            assert!(is_image(bytes), "{bytes:?}");
        }
        for bytes in [
            &b"\xff\xd8\xff\xf7"[..],
            b"\xff\xd8",
            b"GIF89",
            b"RIFF0000WEB",
            b"II*\0",
            b"BM",
            bmp(40, 2, 32).as_slice(),
            bmp(40, 1, 2).as_slice(),
            bmp(39, 1, 24).as_slice(),
        ] {
            assert!(!is_image(bytes), "{bytes:?}");
        }
        let mut invalid = bmp(40, 1, 24);
        invalid[2..6].copy_from_slice(&54u32.to_le_bytes());
        assert!(!is_image(&invalid)); // pixel offset equals declared size
        invalid[2..6].copy_from_slice(&53u32.to_le_bytes());
        assert!(!is_image(&invalid));
    }
}
