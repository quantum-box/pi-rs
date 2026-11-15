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
