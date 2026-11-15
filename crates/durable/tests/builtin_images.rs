#![cfg(feature = "builtin-images")]
use base64::Engine;
use image::{DynamicImage, ImageFormat, Rgba, RgbaImage};
use pi_rs_durable::images::{
    BuiltinImageProcessor, FittedImage, FittedImages, ImageLimits, ImageProcessor, fit_image,
    fit_images,
};
use serde_json::json;
use std::io::Cursor;

fn fixture(width: u32, height: u32, format: ImageFormat) -> Vec<u8> {
    let mut pixels = RgbaImage::new(width, height);
    for (x, y, p) in pixels.enumerate_pixels_mut() {
        *p = Rgba([
            (x * 17 % 256) as u8,
            (y * 31 % 256) as u8,
            ((x + y) * 11 % 256) as u8,
            255,
        ]);
    }
    let mut out = Cursor::new(Vec::new());
    DynamicImage::ImageRgba8(pixels)
        .write_to(&mut out, format)
        .unwrap();
    out.into_inner()
}
fn jpeg_with_orientation(bytes: &[u8], orientation: u16) -> Vec<u8> {
    let mut out = bytes[..2].to_vec();
    let mut exif = b"Exif\0\0II\x2a\0\x08\0\0\0\x01\0\x12\x01\x03\0\x01\0\0\0".to_vec();
    exif.extend_from_slice(&orientation.to_le_bytes());
    exif.extend_from_slice(b"\0\0\0\0\0\0");
    out.extend_from_slice(&[0xff, 0xe1]);
    out.extend_from_slice(&((exif.len() + 2) as u16).to_be_bytes());
    out.extend_from_slice(&exif);
    out.extend_from_slice(&bytes[2..]);
    out
}
fn decoded(image: &PreparedImageAlias) -> DynamicImage {
    image::load_from_memory(
        &base64::engine::general_purpose::STANDARD
            .decode(&image.data)
            .unwrap(),
    )
    .unwrap()
}
type PreparedImageAlias = pi_rs_durable::images::PreparedImage;

#[tokio::test]
async fn upright_inline_formats_are_decoded_and_preserved() {
    let processor = BuiltinImageProcessor;
    for (format, mime) in [
        (ImageFormat::Png, "image/png"),
        (ImageFormat::Jpeg, "image/jpeg"),
        (ImageFormat::Gif, "image/gif"),
        (ImageFormat::WebP, "image/webp"),
    ] {
        let bytes = fixture(8, 6, format);
        let result = processor
            .prepare(&bytes, mime, ImageLimits::default())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            result.data,
            base64::engine::general_purpose::STANDARD.encode(&bytes)
        );
        assert_eq!(result.mime_type, mime);
        assert_eq!(decoded(&result).width(), 8);
        assert!(result.resized.is_none());
    }
    assert!(
        processor
            .prepare(b"bad", "image/png", ImageLimits::default())
            .await
            .unwrap()
            .is_none()
    );
    let png = fixture(8, 6, ImageFormat::Png);
    assert!(
        processor
            .prepare(&png[..24], "image/png", ImageLimits::default())
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        processor
            .prepare(&png, "image/jpeg", ImageLimits::default())
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn resizes_converts_and_fits_encoded_byte_limit() {
    let p = BuiltinImageProcessor;
    let png = fixture(400, 100, ImageFormat::Png);
    let limits = ImageLimits {
        max_width: 200,
        ..ImageLimits::default()
    };
    let result = p.prepare(&png, "image/png", limits).await.unwrap().unwrap();
    assert_eq!(
        (decoded(&result).width(), decoded(&result).height()),
        (200, 50)
    );
    let resize = result.resized.unwrap();
    assert_eq!(
        (
            resize.from.width,
            resize.from.height,
            resize.to.width,
            resize.to.height
        ),
        (400, 100, 200, 50)
    );
    let bmp = fixture(4, 4, ImageFormat::Bmp);
    let result = p
        .prepare(&bmp, "image/bmp", ImageLimits::default())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.mime_type, "image/png");
    assert_eq!(result.converted_from.as_deref(), Some("image/bmp"));
    let limit = ImageLimits {
        max_bytes: 3000,
        ..ImageLimits::default()
    };
    let result = p
        .prepare(&fixture(80, 80, ImageFormat::Png), "image/png", limit)
        .await
        .unwrap()
        .unwrap();
    assert!(result.data.len() <= limit.max_bytes);
    assert!(
        p.prepare(
            &png,
            "image/png",
            ImageLimits {
                max_bytes: 1,
                ..ImageLimits::default()
            }
        )
        .await
        .unwrap()
        .is_none()
    );
}

#[tokio::test]
async fn orientation_and_ordered_fit_are_applied_before_returning() {
    let p = BuiltinImageProcessor;
    let jpeg = jpeg_with_orientation(&fixture(40, 30, ImageFormat::Jpeg), 6);
    let result = p
        .prepare(&jpeg, "image/jpeg", ImageLimits::default())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (decoded(&result).width(), decoded(&result).height()),
        (30, 40)
    );
    assert_ne!(
        result.data,
        base64::engine::general_purpose::STANDARD.encode(&jpeg)
    );
    let jpeg_base64 = base64::engine::general_purpose::STANDARD.encode(&jpeg);
    let input = vec![
        json!({"type":"text","text":"before"}),
        json!({"type":"image","data":jpeg_base64,"mimeType":"image/png"}),
        json!({"type":"text","text":"after"}),
    ];
    let FittedImages::Fitted { content, .. } = fit_images(&input, ImageLimits::default(), &p)
        .await
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(content[0], input[0]);
    assert_eq!(content[1]["mimeType"], "image/jpeg");
    assert_eq!(content[2], input[2]);
    assert!(matches!(
        fit_image(b"garbage", "image/png", ImageLimits::default(), &p).await,
        FittedImage::Omitted { .. }
    ));
}

#[test]
fn model_descriptors_resolve_individual_overrides_from_snapshot() {
    use pi_rs_durable::images::ImageLimitsCatalog;
    let mut catalog = ImageLimitsCatalog::default();
    catalog
        .insert(
            "provider/model",
            &json!({"inputLimits":{"images":{"resize":{"maxWidth":100,"maxBytes":12345}}}}),
        )
        .unwrap();
    assert_eq!(
        catalog.limits("provider/model"),
        ImageLimits {
            max_width: 100,
            max_height: 2000,
            max_bytes: 12345
        }
    );
    assert_eq!(catalog.limits("other"), ImageLimits::default());
    assert!(
        catalog
            .insert(
                "bad",
                &json!({"inputLimits":{"images":{"resize":{"maxHeight":0}}}})
            )
            .is_err()
    );
    let resolve = catalog.resolver();
    assert_eq!(
        resolve(&json!({"config":{"model":"provider/model"}})).max_width,
        100
    );
}

#[tokio::test]
async fn all_jpeg_orientations_are_decoded_and_reoriented() {
    let original = fixture(8, 6, ImageFormat::Jpeg);
    for orientation in 1..=8 {
        let input = jpeg_with_orientation(&original, orientation);
        let result = BuiltinImageProcessor
            .prepare(&input, "image/jpeg", ImageLimits::default())
            .await
            .unwrap()
            .unwrap();
        let actual = decoded(&result);
        let expected = if orientation >= 5 { (6, 8) } else { (8, 6) };
        assert_eq!(
            (actual.width(), actual.height()),
            expected,
            "orientation {orientation}"
        );
        if orientation == 1 {
            assert_eq!(
                result.data,
                base64::engine::general_purpose::STANDARD.encode(&input)
            );
        } else {
            assert_ne!(
                result.data,
                base64::engine::general_purpose::STANDARD.encode(&input)
            );
        }
    }
}

#[test]
fn processor_also_works_without_a_tokio_runtime() {
    use futures_util::FutureExt;
    let png = fixture(4, 4, ImageFormat::Png);
    let result = BuiltinImageProcessor
        .prepare(&png, "image/png", ImageLimits::default())
        .now_or_never()
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(result.mime_type, "image/png");
}

#[tokio::test]
async fn rejects_inputs_outside_processor_resource_limits() {
    let png = fixture(4, 4, ImageFormat::Png);
    assert!(
        BuiltinImageProcessor
            .prepare(
                &png,
                "image/png",
                ImageLimits {
                    max_width: 0,
                    ..ImageLimits::default()
                }
            )
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        BuiltinImageProcessor
            .prepare(
                &vec![0; 20 * 1024 * 1024 + 1],
                "image/png",
                ImageLimits::default()
            )
            .await
            .unwrap()
            .is_none()
    );
}

fn exif_tiff(orientation: u16, little: bool) -> Vec<u8> {
    let mut out = vec![0u8; 26];
    out[0..2].copy_from_slice(if little { b"II" } else { b"MM" });
    let u16_bytes = |n: u16| {
        if little {
            n.to_le_bytes()
        } else {
            n.to_be_bytes()
        }
    };
    let u32_bytes = |n: u32| {
        if little {
            n.to_le_bytes()
        } else {
            n.to_be_bytes()
        }
    };
    out[2..4].copy_from_slice(&u16_bytes(42));
    out[4..8].copy_from_slice(&u32_bytes(8));
    out[8..10].copy_from_slice(&u16_bytes(1));
    out[10..12].copy_from_slice(&u16_bytes(0x112));
    out[12..14].copy_from_slice(&u16_bytes(3));
    out[14..18].copy_from_slice(&u32_bytes(1));
    out[18..20].copy_from_slice(&u16_bytes(orientation));
    out
}
fn webp_with_exif(
    bytes: &[u8],
    width: u32,
    height: u32,
    prefix: bool,
    orientation: u16,
) -> Vec<u8> {
    fn chunk(out: &mut Vec<u8>, name: &[u8; 4], data: &[u8]) {
        out.extend_from_slice(name);
        out.extend_from_slice(&(data.len() as u32).to_le_bytes());
        out.extend_from_slice(data);
        if !data.len().is_multiple_of(2) {
            out.push(0);
        }
    }
    let mut body = b"WEBP".to_vec();
    let w = (width - 1).to_le_bytes();
    let h = (height - 1).to_le_bytes();
    chunk(
        &mut body,
        b"VP8X",
        &[0x18, 0, 0, 0, w[0], w[1], w[2], h[0], h[1], h[2]],
    );
    body.extend_from_slice(&bytes[12..]);
    let mut exif = Vec::new();
    if prefix {
        exif.extend_from_slice(b"Exif\0\0");
    }
    exif.extend_from_slice(&exif_tiff(orientation, true));
    chunk(&mut body, b"EXIF", &exif);
    let mut out = b"RIFF".to_vec();
    out.extend_from_slice(&(body.len() as u32).to_le_bytes());
    out.extend_from_slice(&body);
    out
}
#[tokio::test]
async fn webp_orientation_accepts_bare_and_prefixed_exif() {
    let webp = fixture(40, 30, ImageFormat::WebP);
    for prefixed in [false, true] {
        let input = webp_with_exif(&webp, 40, 30, prefixed, 6);
        let prepared = BuiltinImageProcessor
            .prepare(&input, "image/webp", ImageLimits::default())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            (decoded(&prepared).width(), decoded(&prepared).height()),
            (30, 40),
            "prefixed={prefixed}"
        );
        assert_ne!(
            prepared.data,
            base64::engine::general_purpose::STANDARD.encode(&input)
        );
    }
}

#[tokio::test]
async fn jpeg_orientation_moves_all_colored_blocks_correctly() {
    let colors = [
        [255, 0, 0],
        [0, 255, 0],
        [0, 0, 255],
        [255, 255, 255],
        [0, 0, 0],
        [255, 255, 0],
    ];
    let mut pixels = RgbaImage::new(48, 32);
    for (x, y, p) in pixels.enumerate_pixels_mut() {
        let c = colors[((y / 16) * 3 + x / 16) as usize];
        *p = Rgba([c[0], c[1], c[2], 255]);
    }
    let mut out = Cursor::new(Vec::new());
    DynamicImage::ImageRgba8(pixels)
        .write_to(&mut out, ImageFormat::Jpeg)
        .unwrap();
    let jpeg = out.into_inner();
    let expected: [&[usize]; 8] = [
        &[0, 1, 2, 3, 4, 5],
        &[2, 1, 0, 5, 4, 3],
        &[5, 4, 3, 2, 1, 0],
        &[3, 4, 5, 0, 1, 2],
        &[0, 3, 1, 4, 2, 5],
        &[3, 0, 4, 1, 5, 2],
        &[5, 2, 4, 1, 3, 0],
        &[2, 5, 1, 4, 0, 3],
    ];
    for orientation in 1..=8 {
        let input = jpeg_with_orientation(&jpeg, orientation);
        let result = BuiltinImageProcessor
            .prepare(&input, "image/jpeg", ImageLimits::default())
            .await
            .unwrap()
            .unwrap();
        let image = decoded(&result).to_rgb8();
        let cols = if orientation < 5 { 3 } else { 2 };
        for (i, expected_index) in expected[(orientation - 1) as usize].iter().enumerate() {
            let pixel = image.get_pixel((i as u32 % cols) * 16 + 8, (i as u32 / cols) * 16 + 8);
            let seen = pixel.0.map(|v| if v > 127 { 255 } else { 0 });
            assert_eq!(
                seen, colors[*expected_index],
                "orientation {orientation} tile {i}"
            );
        }
    }
}

#[tokio::test]
async fn exact_base64_limit_and_extreme_aspect_are_respected() {
    let p = BuiltinImageProcessor;
    let png = fixture(16, 12, ImageFormat::Png);
    let size = base64::engine::general_purpose::STANDARD.encode(&png).len();
    let exact = p
        .prepare(
            &png,
            "image/png",
            ImageLimits {
                max_bytes: size,
                ..ImageLimits::default()
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        exact.data,
        base64::engine::general_purpose::STANDARD.encode(&png)
    );
    let below = p
        .prepare(
            &png,
            "image/png",
            ImageLimits {
                max_bytes: size - 1,
                ..ImageLimits::default()
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert!(below.data.len() < size);
    let wide = fixture(6000, 2, ImageFormat::Png);
    let result = p
        .prepare(&wide, "image/png", ImageLimits::default())
        .await
        .unwrap()
        .unwrap();
    let resized = result.resized.unwrap();
    assert_eq!(
        (
            resized.from.width,
            resized.from.height,
            resized.to.width,
            resized.to.height
        ),
        (6000, 2, 2000, 1)
    );
}

#[tokio::test]
async fn over_byte_limit_tries_jpeg_before_reducing_dimensions() {
    let (width, height) = (256, 256);
    let mut pixels = RgbaImage::new(width, height);
    let mut seed: u32 = 0x12345678;
    for pixel in pixels.pixels_mut() {
        let mut channels = [0u8; 4];
        for value in channels.iter_mut().take(3) {
            seed ^= seed << 13;
            seed ^= seed >> 17;
            seed ^= seed << 5;
            *value = seed as u8;
        }
        channels[3] = 255;
        *pixel = Rgba(channels);
    }
    let mut out = Cursor::new(Vec::new());
    DynamicImage::ImageRgba8(pixels)
        .write_to(&mut out, ImageFormat::Png)
        .unwrap();
    let source = out.into_inner();
    let limits = ImageLimits {
        max_bytes: 100_000,
        ..ImageLimits::default()
    };
    assert!(
        base64::engine::general_purpose::STANDARD
            .encode(&source)
            .len()
            > limits.max_bytes
    );
    let result = BuiltinImageProcessor
        .prepare(&source, "image/png", limits)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.mime_type, "image/jpeg");
    assert_eq!(
        (decoded(&result).width(), decoded(&result).height()),
        (width, height)
    );
    assert_eq!(result.converted_from.as_deref(), Some("image/png"));
    assert!(result.resized.is_none());
    assert!(result.data.len() <= limits.max_bytes);
    let tight = ImageLimits {
        max_bytes: 4000,
        ..ImageLimits::default()
    };
    let reduced = BuiltinImageProcessor
        .prepare(&source, "image/png", tight)
        .await
        .unwrap()
        .unwrap();
    assert!(reduced.data.len() <= tight.max_bytes);
    assert!(
        reduced
            .resized
            .is_some_and(|size| size.to.width < width && size.to.height < height)
    );
}

fn png_with_dimensions(bytes: &[u8], width: u32, height: u32) -> Vec<u8> {
    let mut out = bytes.to_vec();
    out[16..20].copy_from_slice(&width.to_be_bytes());
    out[20..24].copy_from_slice(&height.to_be_bytes());
    let mut crc = 0xffff_ffffu32;
    for byte in &out[12..29] {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0xedb8_8320 & 0u32.wrapping_sub(crc & 1));
        }
    }
    out[29..33].copy_from_slice(&(!crc).to_be_bytes());
    out
}
#[tokio::test]
async fn oversized_encoded_and_declared_decoded_inputs_are_omitted() {
    let png = fixture(1, 1, ImageFormat::Png);
    let mut large = png.clone();
    large.resize(20 * 1024 * 1024 + 1, 0);
    assert!(image::load_from_memory(&large).is_ok());
    assert!(
        BuiltinImageProcessor
            .prepare(&large, "image/png", ImageLimits::default())
            .await
            .unwrap()
            .is_none()
    );
    let decompression_bomb = png_with_dimensions(&png, 10_000, 4_000);
    let decoder =
        image::ImageReader::with_format(Cursor::new(&decompression_bomb), ImageFormat::Png)
            .into_decoder()
            .unwrap();
    assert_eq!(image::ImageDecoder::dimensions(&decoder), (10_000, 4_000));
    assert!(
        BuiltinImageProcessor
            .prepare(&decompression_bomb, "image/png", ImageLimits::default())
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn jpeg_big_endian_exif_after_app0_is_applied() {
    let jpeg = fixture(40, 30, ImageFormat::Jpeg);
    let mut input = jpeg[..2].to_vec();
    input.extend_from_slice(&[
        0xff, 0xe0, 0, 16, b'J', b'F', b'I', b'F', 0, 1, 1, 0, 0, 1, 0, 1, 0, 0,
    ]);
    let mut exif = b"Exif\0\0".to_vec();
    exif.extend_from_slice(&exif_tiff(8, false));
    input.extend_from_slice(&[0xff, 0xe1]);
    input.extend_from_slice(&((exif.len() + 2) as u16).to_be_bytes());
    input.extend_from_slice(&exif);
    input.extend_from_slice(&jpeg[2..]);
    let image = BuiltinImageProcessor
        .prepare(&input, "image/jpeg", ImageLimits::default())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (decoded(&image).width(), decoded(&image).height()),
        (30, 40)
    );
}

#[tokio::test]
async fn png_resize_preserves_alpha_and_color_in_uniform_regions() {
    let mut pixels = RgbaImage::new(128, 64);
    for (x, _, pixel) in pixels.enumerate_pixels_mut() {
        *pixel = if x < 64 {
            Rgba([255, 0, 0, 255])
        } else {
            Rgba([0, 0, 255, 64])
        };
    }
    let mut out = Cursor::new(Vec::new());
    DynamicImage::ImageRgba8(pixels)
        .write_to(&mut out, ImageFormat::Png)
        .unwrap();
    let result = BuiltinImageProcessor
        .prepare(
            &out.into_inner(),
            "image/png",
            ImageLimits {
                max_width: 64,
                ..ImageLimits::default()
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.mime_type, "image/png");
    let image = decoded(&result).to_rgba8();
    assert_eq!((image.width(), image.height()), (64, 32));
    assert_eq!(*image.get_pixel(12, 16), Rgba([255, 0, 0, 255]));
    assert_eq!(*image.get_pixel(52, 16), Rgba([0, 0, 255, 64]));
}

#[tokio::test]
async fn malformed_webp_orientation_does_not_change_valid_image() {
    let webp = fixture(40, 30, ImageFormat::WebP);
    let input = webp_with_exif(&webp, 40, 30, true, 9);
    let result = BuiltinImageProcessor
        .prepare(&input, "image/webp", ImageLimits::default())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        result.data,
        base64::engine::general_purpose::STANDARD.encode(&input)
    );
}

#[tokio::test]
async fn transformed_grayscale_and_sixteen_bit_png_use_rgba8_like_photon() {
    let fixtures = [
        DynamicImage::ImageLuma8(image::GrayImage::from_pixel(8, 6, image::Luma([128]))),
        DynamicImage::ImageRgba16(image::ImageBuffer::from_pixel(
            8,
            6,
            image::Rgba([0xffffu16, 0x8000, 0, 0xffff]),
        )),
    ];
    for image in fixtures {
        let mut bytes = Cursor::new(Vec::new());
        image.write_to(&mut bytes, ImageFormat::Png).unwrap();
        let bytes = bytes.into_inner();
        let preserved = BuiltinImageProcessor
            .prepare(&bytes, "image/png", ImageLimits::default())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            preserved.data,
            base64::engine::general_purpose::STANDARD.encode(&bytes)
        );
        let result = BuiltinImageProcessor
            .prepare(
                &bytes,
                "image/png",
                ImageLimits {
                    max_width: 4,
                    ..ImageLimits::default()
                },
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result.mime_type, "image/png");
        let data = base64::engine::general_purpose::STANDARD
            .decode(&result.data)
            .unwrap();
        let decoder = image::ImageReader::with_format(Cursor::new(data), ImageFormat::Png)
            .into_decoder()
            .unwrap();
        assert_eq!(
            image::ImageDecoder::color_type(&decoder),
            image::ColorType::Rgba8
        );
    }
}

#[tokio::test]
async fn resized_formats_follow_upstream_encoding_preference() {
    for (format, mime, expected) in [
        (ImageFormat::Png, "image/png", "image/png"),
        (ImageFormat::Jpeg, "image/jpeg", "image/jpeg"),
        (ImageFormat::Gif, "image/gif", "image/png"),
        (ImageFormat::WebP, "image/webp", "image/png"),
        (ImageFormat::Bmp, "image/bmp", "image/png"),
    ] {
        let input = fixture(8, 6, format);
        let result = BuiltinImageProcessor
            .prepare(
                &input,
                mime,
                ImageLimits {
                    max_width: 4,
                    ..ImageLimits::default()
                },
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result.mime_type, expected, "{mime}");
        assert_eq!(
            (decoded(&result).width(), decoded(&result).height()),
            (4, 3)
        );
        assert_eq!(
            result.converted_from.as_deref(),
            (mime != expected).then_some(mime)
        );
    }
    let jpeg = fixture(8, 6, ImageFormat::Jpeg);
    let result = BuiltinImageProcessor
        .prepare(
            &jpeg,
            "image/jpeg",
            ImageLimits {
                max_width: 4,
                max_bytes: 200,
                ..ImageLimits::default()
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.mime_type, "image/png");
    assert_eq!(result.converted_from.as_deref(), Some("image/jpeg"));
    assert!(result.data.len() <= 200);
}

#[tokio::test]
async fn jpeg_qualities_are_attempted_in_upstream_order() {
    let mut pixels = RgbaImage::new(128, 128);
    let mut seed = 0x3210_abcd_u32;
    for pixel in pixels.pixels_mut() {
        let mut channels = [0u8; 4];
        for channel in channels.iter_mut().take(3) {
            seed ^= seed << 13;
            seed ^= seed >> 17;
            seed ^= seed << 5;
            *channel = seed as u8;
        }
        channels[3] = 255;
        *pixel = Rgba(channels);
    }
    let image = DynamicImage::ImageRgba8(pixels);
    let mut png = Cursor::new(Vec::new());
    image.write_to(&mut png, ImageFormat::Png).unwrap();
    let png = png.into_inner();
    let mut candidates = Vec::new();
    for quality in [80, 70, 55, 40] {
        let mut bytes = Vec::new();
        image::codecs::jpeg::JpegEncoder::new_with_quality(&mut bytes, quality)
            .encode_image(&image)
            .unwrap();
        candidates.push(base64::engine::general_purpose::STANDARD.encode(bytes));
    }
    for i in 0..candidates.len() {
        let limit = candidates[i].len();
        assert!(base64::engine::general_purpose::STANDARD.encode(&png).len() > limit);
        if i > 0 {
            assert!(candidates[i - 1].len() > limit);
        }
        let result = BuiltinImageProcessor
            .prepare(
                &png,
                "image/png",
                ImageLimits {
                    max_bytes: limit,
                    ..ImageLimits::default()
                },
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result.mime_type, "image/jpeg");
        assert_eq!(result.data, candidates[i], "quality index {i}");
        assert!(result.resized.is_none());
    }
}

#[tokio::test]
async fn byte_budget_retries_at_three_quarters_of_previous_size() {
    use image::imageops::FilterType;
    let mut pixels = RgbaImage::new(128, 128);
    let mut seed = 0x4a1b_2c3d_u32;
    for pixel in pixels.pixels_mut() {
        let mut channels = [0u8; 4];
        for channel in channels.iter_mut().take(3) {
            seed ^= seed << 13;
            seed ^= seed >> 17;
            seed ^= seed << 5;
            *channel = seed as u8;
        }
        channels[3] = 255;
        *pixel = Rgba(channels);
    }
    let original = DynamicImage::ImageRgba8(pixels);
    let mut input = Cursor::new(Vec::new());
    original.write_to(&mut input, ImageFormat::Png).unwrap();
    let scaled = original.resize_exact(96, 96, FilterType::Lanczos3);
    let jpeg = |image: &DynamicImage, quality| {
        let mut bytes = Vec::new();
        image::codecs::jpeg::JpegEncoder::new_with_quality(&mut bytes, quality)
            .encode_image(image)
            .unwrap();
        base64::engine::general_purpose::STANDARD.encode(bytes)
    };
    let expected = jpeg(&scaled, 40);
    let limit = expected.len();
    assert!(jpeg(&original, 40).len() > limit);
    assert!(jpeg(&scaled, 55).len() > limit);
    let result = BuiltinImageProcessor
        .prepare(
            &input.into_inner(),
            "image/png",
            ImageLimits {
                max_bytes: limit,
                ..ImageLimits::default()
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.data, expected);
    assert_eq!(result.mime_type, "image/jpeg");
    let resized = result.resized.unwrap();
    assert_eq!(
        (
            resized.from.width,
            resized.from.height,
            resized.to.width,
            resized.to.height
        ),
        (128, 128, 96, 96)
    );
}

#[tokio::test]
async fn animated_gif_is_preserved_when_unchanged_and_resized_from_first_frame() {
    use image::AnimationDecoder;
    let mut bytes = Vec::new();
    {
        let mut encoder = image::codecs::gif::GifEncoder::new(&mut bytes);
        for color in [Rgba([255, 0, 0, 255]), Rgba([0, 255, 0, 255])] {
            encoder
                .encode_frame(image::Frame::new(RgbaImage::from_pixel(8, 6, color)))
                .unwrap();
        }
    }
    let preserved = BuiltinImageProcessor
        .prepare(&bytes, "image/gif", ImageLimits::default())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        preserved.data,
        base64::engine::general_purpose::STANDARD.encode(&bytes)
    );
    let decoder = image::codecs::gif::GifDecoder::new(Cursor::new(&bytes)).unwrap();
    assert_eq!(decoder.into_frames().collect_frames().unwrap().len(), 2);
    let changed = BuiltinImageProcessor
        .prepare(
            &bytes,
            "image/gif",
            ImageLimits {
                max_width: 4,
                ..ImageLimits::default()
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(changed.mime_type, "image/png");
    let result = decoded(&changed).to_rgba8();
    assert_eq!((result.width(), result.height()), (4, 3));
    assert_eq!(*result.get_pixel(2, 1), Rgba([255, 0, 0, 255]));
}
