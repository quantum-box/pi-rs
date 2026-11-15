//! Optional image-rs based processor. CPU-heavy work runs on a blocking worker.
use super::{Dimensions, ImageLimits, ImageProcessor, PreparedImage, Resize};
use base64::Engine;
use image::{
    DynamicImage, ImageDecoder, ImageFormat, ImageReader, codecs::jpeg::JpegEncoder,
    imageops::FilterType, metadata::Orientation,
};
use std::io::Cursor;

const MAX_SOURCE_BYTES: usize = 20 * 1024 * 1024;
const MAX_DECODE_BYTES: u64 = 128 * 1024 * 1024;
const JPEG_QUALITIES: [u8; 4] = [80, 70, 55, 40];

/// Decoder and resizer for PNG, JPEG, GIF, WebP and BMP, using image-rs codecs.
/// Source data is limited to 20 MiB and decoded RGBA dimensions to 128 MiB.
#[derive(Clone, Copy, Debug, Default)]
pub struct BuiltinImageProcessor;

#[async_trait::async_trait]
impl ImageProcessor for BuiltinImageProcessor {
    async fn prepare(
        &self,
        bytes: &[u8],
        mime_type: &str,
        limits: ImageLimits,
    ) -> Result<Option<PreparedImage>, String> {
        if bytes.len() > MAX_SOURCE_BYTES {
            return Ok(None);
        }
        if tokio::runtime::Handle::try_current().is_err() {
            return Ok(prepare(bytes, mime_type, limits));
        }
        let bytes = bytes.to_vec();
        let mime = mime_type.to_owned();
        tokio::task::spawn_blocking(move || prepare(&bytes, &mime, limits))
            .await
            .map_err(|error| format!("image worker failed: {error}"))
    }
}

fn prepare(bytes: &[u8], mime: &str, limits: ImageLimits) -> Option<PreparedImage> {
    if limits.max_width == 0 || limits.max_height == 0 || limits.max_bytes == 0 {
        return None;
    }
    let format = image::guess_format(bytes).ok()?;
    let actual = match format {
        ImageFormat::Png => "image/png",
        ImageFormat::Jpeg => "image/jpeg",
        ImageFormat::Gif => "image/gif",
        ImageFormat::WebP => "image/webp",
        ImageFormat::Bmp => "image/bmp",
        _ => return None,
    };
    if actual != mime {
        return None;
    }
    let mut reader = ImageReader::with_format(Cursor::new(bytes), format);
    let mut decoder_limits = image::Limits::default();
    decoder_limits.max_alloc = Some(MAX_DECODE_BYTES);
    reader.limits(decoder_limits);
    let mut decoder = reader.into_decoder().ok()?;
    let (width, height) = decoder.dimensions();
    if width == 0
        || height == 0
        || u64::from(width)
            .checked_mul(u64::from(height))?
            .checked_mul(4)?
            > MAX_DECODE_BYTES
    {
        return None;
    }
    let orientation = match format {
        ImageFormat::WebP => decoder
            .exif_metadata()
            .ok()
            .flatten()
            .and_then(|exif| {
                let tiff = exif.strip_prefix(b"Exif\0\0").unwrap_or(&exif);
                Orientation::from_exif_chunk(tiff)
            })
            .unwrap_or(Orientation::NoTransforms),
        ImageFormat::Jpeg => decoder.orientation().unwrap_or(Orientation::NoTransforms),
        _ => Orientation::NoTransforms,
    };
    let mut image = DynamicImage::from_decoder(decoder).ok()?;
    if orientation == Orientation::NoTransforms
        && actual != "image/bmp"
        && width <= limits.max_width
        && height <= limits.max_height
        && encoded_length(bytes.len())? <= limits.max_bytes
    {
        return Some(PreparedImage {
            data: base64::engine::general_purpose::STANDARD.encode(bytes),
            mime_type: actual.into(),
            resized: None,
            converted_from: None,
        });
    }
    image.apply_orientation(orientation);
    let from = Dimensions {
        width: image.width(),
        height: image.height(),
    };
    let scale = 1.0f64
        .min(limits.max_width as f64 / from.width as f64)
        .min(limits.max_height as f64 / from.height as f64);
    let mut to = Dimensions {
        width: ((from.width as f64 * scale).round() as u32).max(1),
        height: ((from.height as f64 * scale).round() as u32).max(1),
    };
    loop {
        let scaled = if to == from {
            image.clone()
        } else {
            image.resize_exact(to.width, to.height, FilterType::Lanczos3)
        };
        if let Some((output, output_mime)) =
            encode(&scaled, actual == "image/jpeg", limits.max_bytes)
        {
            return Some(PreparedImage {
                data: base64::engine::general_purpose::STANDARD.encode(output),
                mime_type: output_mime.into(),
                resized: (to != from).then_some(Resize { from, to }),
                converted_from: (actual != output_mime).then(|| actual.into()),
            });
        }
        if to.width == 1 && to.height == 1 {
            return None;
        }
        to.width = (to.width * 3 / 4).max(1);
        to.height = (to.height * 3 / 4).max(1);
    }
}
fn encoded_length(bytes: usize) -> Option<usize> {
    bytes.checked_add(2)?.checked_div(3)?.checked_mul(4)
}
fn encode(
    image: &DynamicImage,
    jpeg_first: bool,
    max_bytes: usize,
) -> Option<(Vec<u8>, &'static str)> {
    let png = || {
        let mut out = Cursor::new(Vec::new());
        image.write_to(&mut out, ImageFormat::Png).ok()?;
        let bytes = out.into_inner();
        (encoded_length(bytes.len())? <= max_bytes).then_some((bytes, "image/png"))
    };
    let jpeg = || {
        for quality in JPEG_QUALITIES {
            let mut out = Vec::new();
            if JpegEncoder::new_with_quality(&mut out, quality)
                .encode_image(image)
                .is_ok()
                && encoded_length(out.len()).is_some_and(|size| size <= max_bytes)
            {
                return Some((out, "image/jpeg"));
            }
        }
        None
    };
    if jpeg_first {
        jpeg().or_else(png)
    } else {
        png().or_else(jpeg)
    }
}
