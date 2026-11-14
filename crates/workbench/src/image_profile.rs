//! Model-specific preparation of new image inputs.
use std::io::{BufRead, Cursor, Read, Seek};

use base64::Engine;
use image::{
    DynamicImage, GenericImageView, ImageDecoder, ImageFormat, ImageReader,
    codecs::jpeg::JpegEncoder,
};
use pi_rs::{Error, ImageContent, Result};
use serde::{Deserialize, Serialize};

pub const MAX_IMAGE_SOURCE_BYTES: usize = 20 * 1024 * 1024;
const MAX_ENCODED_IMAGE_BYTES: usize = 8 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ImageResizeProfile {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_width: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_height: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_bytes: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub jpeg_quality: Option<u8>,
}

impl ImageResizeProfile {
    pub fn overlay(self, newer: Self) -> Self {
        Self {
            max_width: newer.max_width.or(self.max_width),
            max_height: newer.max_height.or(self.max_height),
            max_bytes: newer.max_bytes.or(self.max_bytes),
            jpeg_quality: newer.jpeg_quality.or(self.jpeg_quality),
        }
    }

    pub fn valid(self) -> bool {
        self.max_width != Some(0)
            && self.max_height != Some(0)
            && self.max_bytes != Some(0)
            && self
                .jpeg_quality
                .is_none_or(|quality| (1..=100).contains(&quality))
    }
}

pub struct ModelImage {
    pub bytes: Vec<u8>,
    pub mime_type: &'static str,
    pub source: (u32, u32),
    pub output: (u32, u32),
}

pub fn normalize<R: BufRead + Seek>(
    reader: ImageReader<R>,
    profile: ImageResizeProfile,
) -> Result<ModelImage> {
    normalize_cancellable(reader, profile, None)
}

pub(super) fn check_cancelled(token: Option<&pi_rs::CancellationToken>) -> Result<()> {
    if token.is_some_and(|token| token.is_cancelled()) {
        Err(Error::Cancelled)
    } else {
        Ok(())
    }
}

pub(super) fn normalize_cancellable<R: BufRead + Seek>(
    reader: ImageReader<R>,
    profile: ImageResizeProfile,
    token: Option<&pi_rs::CancellationToken>,
) -> Result<ModelImage> {
    check_cancelled(token)?;
    let format = reader
        .format()
        .ok_or_else(|| Error::Config("unknown image format".into()))?;
    let mut bytes = Vec::new();
    reader
        .into_inner()
        .take((MAX_IMAGE_SOURCE_BYTES + 1) as u64)
        .read_to_end(&mut bytes)?;
    check_cancelled(token)?;
    if bytes.len() > MAX_IMAGE_SOURCE_BYTES {
        return Err(Error::Config("image exceeds 20 MiB".into()));
    }
    normalize_encoded(bytes, format, profile, token)
}

pub(super) fn convert_to_png_cancellable<R: BufRead + Seek>(
    reader: ImageReader<R>,
    max_bytes: usize,
    token: Option<&pi_rs::CancellationToken>,
) -> Result<Vec<u8>> {
    check_cancelled(token)?;
    let format = reader
        .format()
        .ok_or_else(|| Error::Config("unknown image format".into()))?;
    let mut bytes = Vec::new();
    reader
        .into_inner()
        .take((max_bytes + 1) as u64)
        .read_to_end(&mut bytes)?;
    check_cancelled(token)?;
    if bytes.len() > max_bytes {
        return Err(Error::Config(if max_bytes == MAX_IMAGE_SOURCE_BYTES {
            "image exceeds 20 MiB".into()
        } else {
            "image exceeds the 6 MiB original image limit".into()
        }));
    }
    let mut reader = ImageReader::with_format(Cursor::new(bytes), format);
    let mut limits = image::Limits::default();
    limits.max_alloc = Some(128 * 1024 * 1024);
    reader.limits(limits);
    let image = reader.decode().map_err(image_error)?;
    check_cancelled(token)?;
    let mut png = Vec::new();
    image
        .write_to(&mut Cursor::new(&mut png), ImageFormat::Png)
        .map_err(image_error)?;
    check_cancelled(token)?;
    Ok(png)
}

pub fn normalize_content(
    content: ImageContent,
    profile: ImageResizeProfile,
) -> Result<ImageContent> {
    if content.data.len() > MAX_ENCODED_IMAGE_BYTES {
        return Err(Error::Config(
            "image exceeds the encoded input limit".into(),
        ));
    }
    let encoding = base64::engine::general_purpose::STANDARD;
    let bytes = encoding
        .decode(content.data)
        .map_err(|_| Error::Config("invalid image data".into()))?;
    let format = image::guess_format(&bytes).map_err(image_error)?;
    let prepared = normalize_encoded(bytes, format, profile, None)?;
    Ok(ImageContent {
        data: encoding.encode(prepared.bytes),
        mime_type: prepared.mime_type.into(),
    })
}

fn normalize_encoded(
    bytes: Vec<u8>,
    format: ImageFormat,
    profile: ImageResizeProfile,
    token: Option<&pi_rs::CancellationToken>,
) -> Result<ModelImage> {
    check_cancelled(token)?;
    let mut reader = ImageReader::with_format(Cursor::new(&bytes), format);
    let mut limits = image::Limits::default();
    limits.max_alloc = Some(128 * 1024 * 1024);
    reader.limits(limits);
    let mut decoder = reader.into_decoder().map_err(image_error)?;
    let orientation = decoder.orientation().map_err(image_error)?;
    check_cancelled(token)?;
    let mut image = DynamicImage::from_decoder(decoder).map_err(image_error)?;
    check_cancelled(token)?;
    image.apply_orientation(orientation);
    check_cancelled(token)?;
    prepare(image, Some((bytes, format.to_mime_type())), profile, token)
}

pub fn prepare_pixels(image: DynamicImage, profile: ImageResizeProfile) -> Result<ModelImage> {
    prepare(image, None, profile, None)
}

fn prepare(
    original: DynamicImage,
    encoded: Option<(Vec<u8>, &'static str)>,
    profile: ImageResizeProfile,
    token: Option<&pi_rs::CancellationToken>,
) -> Result<ModelImage> {
    check_cancelled(token)?;
    if !profile.valid() {
        return Err(Error::Config("invalid model image resize profile".into()));
    }
    let source = (original.width(), original.height());
    let max_width = profile.max_width.unwrap_or(2000);
    let max_height = profile.max_height.unwrap_or(2000);
    let max_bytes = profile
        .max_bytes
        .unwrap_or(9 * 512 * 1024)
        .min(MAX_ENCODED_IMAGE_BYTES + 1);
    let fits = |bytes: &[u8]| bytes.len().div_ceil(3) * 4 < max_bytes;
    if let Some((bytes, mime_type)) = encoded
        && source.0 <= max_width
        && source.1 <= max_height
        && fits(&bytes)
        && matches!(
            mime_type,
            "image/png" | "image/jpeg" | "image/gif" | "image/webp"
        )
    {
        return Ok(ModelImage {
            bytes,
            mime_type,
            source,
            output: source,
        });
    }

    let mut target = (source.0.min(max_width), source.1.min(max_height));
    let qualities = [profile.jpeg_quality.unwrap_or(80), 85, 70, 55, 40];
    loop {
        check_cancelled(token)?;
        let resized = if target == source {
            std::borrow::Cow::Borrowed(&original)
        } else {
            std::borrow::Cow::Owned(original.resize(
                target.0,
                target.1,
                image::imageops::FilterType::Lanczos3,
            ))
        };
        check_cancelled(token)?;
        let output = (resized.width(), resized.height());
        let mut png = Vec::new();
        resized
            .write_to(&mut Cursor::new(&mut png), ImageFormat::Png)
            .map_err(image_error)?;
        check_cancelled(token)?;
        let has_alpha =
            resized.color().has_alpha() && resized.pixels().any(|(_, _, pixel)| pixel[3] != 255);
        check_cancelled(token)?;
        if has_alpha {
            if fits(&png) {
                return Ok(ModelImage {
                    bytes: png,
                    mime_type: "image/png",
                    source,
                    output,
                });
            }
        } else {
            let rgb = resized.to_rgb8();
            check_cancelled(token)?;
            for (index, &quality) in qualities.iter().enumerate() {
                check_cancelled(token)?;
                if index > 0 && quality == qualities[0] {
                    continue;
                }
                let mut jpeg = Vec::new();
                JpegEncoder::new_with_quality(&mut jpeg, quality)
                    .encode_image(&rgb)
                    .map_err(image_error)?;
                check_cancelled(token)?;
                if index == 0 && fits(&png) && png.len() <= jpeg.len() {
                    return Ok(ModelImage {
                        bytes: png,
                        mime_type: "image/png",
                        source,
                        output,
                    });
                }
                if fits(&jpeg) {
                    return Ok(ModelImage {
                        bytes: jpeg,
                        mime_type: "image/jpeg",
                        source,
                        output,
                    });
                }
            }
        }
        if output == (1, 1) {
            return Err(Error::Config(
                "image cannot fit the configured model size limit".into(),
            ));
        }
        target = ((output.0 * 3 / 4).max(1), (output.1 * 3 / 4).max(1));
    }
}

fn image_error(error: image::ImageError) -> Error {
    Error::Config(format!("cannot prepare model image: {error}"))
}

#[cfg(test)]
mod cancellation_phase_tests {
    use super::*;
    struct CancelOnEof {
        inner: Cursor<Vec<u8>>,
        token: pi_rs::CancellationToken,
    }
    impl Read for CancelOnEof {
        fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
            let count = self.inner.read(buffer)?;
            if count == 0 {
                self.token.cancel();
            }
            Ok(count)
        }
    }
    impl Seek for CancelOnEof {
        fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
            self.inner.seek(pos)
        }
    }
    #[test]
    fn cancellation_after_fetch_stops_before_decode_and_resize() {
        let token = pi_rs::CancellationToken::new();
        let source = CancelOnEof {
            inner: Cursor::new(vec![1, 2, 3]),
            token: token.clone(),
        };
        let reader = ImageReader::with_format(std::io::BufReader::new(source), ImageFormat::Png);
        assert!(matches!(
            normalize_cancellable(reader, ImageResizeProfile::default(), Some(&token)),
            Err(Error::Cancelled)
        ));
        let image = DynamicImage::new_rgb8(1, 1);
        assert!(matches!(
            prepare(image, None, ImageResizeProfile::default(), Some(&token)),
            Err(Error::Cancelled)
        ));
    }
}
