//! Clipboard reads happen only after an explicit paste action.
use base64::Engine;
use pi_rs::{Error, ImageContent, Result};
use pi_rs_workbench::image_profile::{self, ImageResizeProfile};

#[derive(Debug)]
pub enum Content {
    Text(String),
    Files(Vec<std::path::PathBuf>),
    Image(ImageContent),
}
pub fn read(auto_resize: bool, profile: Option<ImageResizeProfile>) -> Result<Content> {
    let mut clipboard = arboard::Clipboard::new();
    if let Ok(clipboard) = &mut clipboard {
        if let Ok(paths) = clipboard.get().file_list()
            && !paths.is_empty()
        {
            file_text(&paths, false)?;
            return Ok(Content::Files(paths));
        }
        if let Ok(image) = clipboard.get_image() {
            return prepare(image, auto_resize, profile).map(Content::Image);
        }
    }

    #[cfg(target_os = "linux")]
    let wsl = crate::platform::is_wsl();
    #[cfg(target_os = "linux")]
    if wsl
        && let Some(image) = super::clipboard_wsl::read_image()
            .and_then(|bytes| prepare_wsl_png(&bytes, auto_resize, profile).ok())
    {
        return Ok(Content::Image(image));
    }

    let native_text = clipboard
        .map_err(|error| Error::Config(format!("clipboard unavailable: {error}")))
        .and_then(|mut clipboard| {
            clipboard.get_text().map_err(|error| {
                Error::Config(format!(
                    "clipboard contains no readable files, image or text: {error}"
                ))
            })
        });
    #[cfg(target_os = "linux")]
    if wsl
        && native_text.as_ref().map_or(true, String::is_empty)
        && let Some(text) = super::clipboard_wsl::read_text()
    {
        return Ok(Content::Text(text));
    }
    native_text.map(Content::Text)
}

#[cfg(any(target_os = "linux", all(unix, test)))]
pub(super) fn prepare_wsl_png(
    bytes: &[u8],
    auto_resize: bool,
    profile: Option<ImageResizeProfile>,
) -> Result<ImageContent> {
    use image::ImageDecoder;

    let mut reader =
        image::ImageReader::with_format(std::io::Cursor::new(bytes), image::ImageFormat::Png);
    let mut limits = image::Limits::default();
    limits.max_alloc = Some(IMAGE_PIXEL_LIMIT as u64);
    reader.limits(limits);
    let map_error = |error| Error::Config(format!("cannot decode clipboard PNG: {error}"));
    let decoder = reader.into_decoder().map_err(map_error)?;
    let (width, height) = decoder.dimensions();
    validate_pixel_size(width as usize, height as usize)?;
    if decoder.total_bytes() > IMAGE_PIXEL_LIMIT as u64 {
        return Err(Error::Config(
            "clipboard image dimensions exceed the image limit".into(),
        ));
    }
    let pixels = image::DynamicImage::from_decoder(decoder).map_err(map_error)?;
    prepare_pixels(pixels, auto_resize, profile)
}

pub fn file_text(paths: &[std::path::PathBuf], shell: bool) -> Result<String> {
    let mut formatted = Vec::with_capacity(paths.len());
    let mut bytes = 0usize;
    for path in paths {
        let path = path
            .to_str()
            .ok_or_else(|| Error::Config("clipboard path is not UTF-8".into()))?;
        if path.chars().any(char::is_control) {
            return Err(Error::Config(
                "clipboard path contains control characters".into(),
            ));
        }
        let path = if shell {
            if cfg!(windows) {
                format!("'{}'", path.replace('\'', "''"))
            } else {
                shell_words::quote(path).into_owned()
            }
        } else {
            path.to_owned()
        };
        bytes = bytes.saturating_add(path.len()).saturating_add(1);
        if bytes > 1024 * 1024 {
            return Err(Error::Config("clipboard file list exceeds 1 MiB".into()));
        }
        formatted.push(path);
    }
    Ok(formatted.join(if shell { " " } else { "\n" }))
}

fn prepare(
    image: arboard::ImageData<'_>,
    auto_resize: bool,
    profile: Option<ImageResizeProfile>,
) -> Result<ImageContent> {
    if validate_pixel_size(image.width, image.height)? != image.bytes.len() {
        return Err(Error::Config(
            "clipboard image dimensions exceed the image limit".into(),
        ));
    }
    let width = u32::try_from(image.width)
        .map_err(|_| Error::Config("clipboard image is too wide".into()))?;
    let height = u32::try_from(image.height)
        .map_err(|_| Error::Config("clipboard image is too tall".into()))?;
    let pixels = image::RgbaImage::from_raw(width, height, image.bytes.into_owned())
        .ok_or_else(|| Error::Config("invalid clipboard pixels".into()))?;
    prepare_pixels(
        image::DynamicImage::ImageRgba8(pixels),
        auto_resize,
        profile,
    )
}

const IMAGE_PIXEL_LIMIT: usize = 128 * 1024 * 1024;

fn validate_pixel_size(width: usize, height: usize) -> Result<usize> {
    width
        .checked_mul(height)
        .and_then(|size| size.checked_mul(4))
        .filter(|size| (1..=IMAGE_PIXEL_LIMIT).contains(size))
        .ok_or_else(|| Error::Config("clipboard image dimensions exceed the image limit".into()))
}

fn prepare_pixels(
    pixels: image::DynamicImage,
    auto_resize: bool,
    profile: Option<ImageResizeProfile>,
) -> Result<ImageContent> {
    if auto_resize && let Some(profile) = profile {
        let prepared = image_profile::prepare_pixels(pixels, profile)?;
        return Ok(ImageContent {
            data: base64::engine::general_purpose::STANDARD.encode(prepared.bytes),
            mime_type: prepared.mime_type.into(),
        });
    }
    let prepared = pi_rs_workbench::coding::prepare_image_with_resize(pixels, auto_resize)?;
    Ok(ImageContent {
        data: base64::engine::general_purpose::STANDARD.encode(prepared.png),
        mime_type: "image/png".into(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::borrow::Cow;
    #[test]
    fn clipboard_file_paths_are_bounded_and_shell_quoted() {
        let paths = vec!["/tmp/one file".into(), "/tmp/two'file".into()];
        assert_eq!(
            file_text(&paths, false).unwrap(),
            "/tmp/one file\n/tmp/two'file"
        );
        let quoted = file_text(&paths, true).unwrap();
        if cfg!(windows) {
            assert_eq!(quoted, "'/tmp/one file' '/tmp/two''file'");
        } else {
            assert_eq!(
                shell_words::split(&quoted).unwrap(),
                vec!["/tmp/one file", "/tmp/two'file"]
            );
        }
        assert!(file_text(&["/tmp/line\ncommand".into()], false).is_err());
        assert!(file_text(&["x".repeat(1024 * 1024).into()], true).is_err());
    }

    #[test]
    fn clipboard_pixels_preserve_alpha_and_validate_dimensions() {
        let image = prepare(
            arboard::ImageData {
                width: 1,
                height: 1,
                bytes: Cow::Borrowed(&[255, 0, 0, 80]),
            },
            true,
            None,
        )
        .unwrap();
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(image.data)
            .unwrap();
        let decoded = image::load_from_memory(&bytes).unwrap().to_rgba8();
        assert_eq!(decoded.get_pixel(0, 0).0, [255, 0, 0, 80]);
        assert!(
            prepare(
                arboard::ImageData {
                    width: usize::MAX,
                    height: 2,
                    bytes: Cow::Borrowed(&[])
                },
                true,
                None,
            )
            .is_err()
        );
        assert!(
            prepare(
                arboard::ImageData {
                    width: 2,
                    height: 1,
                    bytes: Cow::Borrowed(&[0; 4])
                },
                true,
                None,
            )
            .is_err()
        );
    }
}
