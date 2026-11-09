//! Clipboard reads happen only after an explicit paste action.
use base64::Engine;
use pi_rs::{Error, ImageContent, Result};

#[derive(Debug)]
pub enum Content {
    Text(String),
    Image(ImageContent),
}
pub fn read() -> Result<Content> {
    let mut clipboard = arboard::Clipboard::new()
        .map_err(|error| Error::Config(format!("clipboard unavailable: {error}")))?;
    match clipboard.get_image() {
        Ok(image) => Ok(Content::Image(prepare(image)?)),
        Err(_) => clipboard.get_text().map(Content::Text).map_err(|error| {
            Error::Config(format!(
                "clipboard contains no readable image or text: {error}"
            ))
        }),
    }
}
fn prepare(image: arboard::ImageData<'_>) -> Result<ImageContent> {
    let size = image
        .width
        .checked_mul(image.height)
        .and_then(|size| size.checked_mul(4));
    if size.is_none_or(|size| size == 0 || size > 128 * 1024 * 1024 || size != image.bytes.len()) {
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
    let prepared = pi_rs_workbench::coding::prepare_image(image::DynamicImage::ImageRgba8(pixels))?;
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
    fn clipboard_pixels_preserve_alpha_and_validate_dimensions() {
        let image = prepare(arboard::ImageData {
            width: 1,
            height: 1,
            bytes: Cow::Borrowed(&[255, 0, 0, 80]),
        })
        .unwrap();
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(image.data)
            .unwrap();
        let decoded = image::load_from_memory(&bytes).unwrap().to_rgba8();
        assert_eq!(decoded.get_pixel(0, 0).0, [255, 0, 0, 80]);
        assert!(
            prepare(arboard::ImageData {
                width: usize::MAX,
                height: 2,
                bytes: Cow::Borrowed(&[])
            })
            .is_err()
        );
        assert!(
            prepare(arboard::ImageData {
                width: 2,
                height: 1,
                bytes: Cow::Borrowed(&[0; 4])
            })
            .is_err()
        );
    }
}
