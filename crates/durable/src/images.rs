//! Opt-in preparation of host-supplied images before transcript admission.
//!
//! A host supplies the processor; this module does not decode or resize images.
use base64::{
    Engine, alphabet,
    engine::{DecodePaddingMode, GeneralPurpose, GeneralPurposeConfig},
};
use serde_json::{Value, json};

/// Model image limits; `max_bytes` measures the length of base64 data.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ImageLimits {
    pub max_width: u32,
    pub max_height: u32,
    pub max_bytes: usize,
}

impl Default for ImageLimits {
    fn default() -> Self {
        Self {
            max_width: 2000,
            max_height: 2000,
            max_bytes: 4_718_592,
        }
    }
}

/// Optional per-model overrides; missing fields use the upstream defaults.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ImageLimitOverrides {
    pub max_width: Option<u32>,
    pub max_height: Option<u32>,
    pub max_bytes: Option<usize>,
}

impl ImageLimitOverrides {
    pub fn resolve(self) -> ImageLimits {
        let defaults = ImageLimits::default();
        ImageLimits {
            max_width: self.max_width.unwrap_or(defaults.max_width),
            max_height: self.max_height.unwrap_or(defaults.max_height),
            max_bytes: self.max_bytes.unwrap_or(defaults.max_bytes),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Dimensions {
    pub width: u32,
    pub height: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Resize {
    pub from: Dimensions,
    pub to: Dimensions,
}

/// Processor output, including optional conversion and coordinate information.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreparedImage {
    pub data: String,
    pub mime_type: String,
    pub resized: Option<Resize>,
    pub converted_from: Option<String>,
}

/// A host-provided image decoder and processor. `Ok(None)` means it cannot prepare the image.
#[async_trait::async_trait]
pub trait ImageProcessor: Send + Sync {
    async fn prepare(
        &self,
        bytes: &[u8],
        mime_type: &str,
        limits: ImageLimits,
    ) -> Result<Option<PreparedImage>, String>;
}

#[derive(Clone, Debug, PartialEq)]
pub enum FittedImage {
    Fitted { image: Value, notes: Vec<String> },
    Omitted { note: String },
}

#[derive(Clone, Debug, PartialEq)]
pub enum FittedImages {
    Unchanged,
    Fitted {
        content: Vec<Value>,
        notes: Vec<String>,
    },
}

fn omitted(mime_type: &str, reason: Option<&str>) -> FittedImage {
    let reason = reason
        .map(|reason| format!(": {reason}"))
        .unwrap_or_default();
    FittedImage::Omitted {
        note: format!(
            "An image ({mime_type}) cannot be prepared for the model and was left out{reason}."
        ),
    }
}

/// Prepare bytes before admission; detected image types take precedence over the declaration.
pub async fn fit_image(
    bytes: &[u8],
    mime_type: &str,
    limits: ImageLimits,
    processor: &dyn ImageProcessor,
) -> FittedImage {
    let detected = detect_image_mime_type(bytes).unwrap_or(mime_type);
    let prepared = match processor.prepare(bytes, detected, limits).await {
        Ok(Some(image)) => image,
        Ok(None) => return omitted(mime_type, None),
        Err(error) => return omitted(mime_type, Some(&error)),
    };
    let mut notes = Vec::new();
    if let Some(from) = &prepared.converted_from {
        notes.push(format!("Converted from {from} to {}.", prepared.mime_type));
    }
    if let Some(resized) = prepared.resized {
        let scale = resized.from.width as f64 / resized.to.width as f64;
        notes.push(format!("Resized from {}x{} to {}x{}. Multiply coordinates by {:.2} to map them to the original.", resized.from.width, resized.from.height, resized.to.width, resized.to.height, scale));
    }
    FittedImage::Fitted {
        image: json!({"type":"image", "data":prepared.data, "mimeType":prepared.mime_type}),
        notes,
    }
}

/// Fit ordered text/image blocks, preserving text and unchanged images verbatim.
/// Malformed block shapes return an error without returning a partial result.
pub async fn fit_images(
    content: &[Value],
    limits: ImageLimits,
    processor: &dyn ImageProcessor,
) -> Result<FittedImages, String> {
    if crate::content::UserContent::from_yield(Value::Array(content.to_vec()), false).is_none() {
        return Err("invalid user content blocks".into());
    }
    let mut fitted = Vec::with_capacity(content.len());
    let mut notes = Vec::new();
    let mut changed = false;
    for item in content {
        if item["type"] != "image" {
            fitted.push(item.clone());
            continue;
        }
        let data = item["data"].as_str().unwrap();
        let mime = item["mimeType"].as_str().unwrap();
        let bytes: Vec<u8> = data
            .bytes()
            .filter(|b| !matches!(b, b'\t' | b'\n' | 0x0c | b'\r' | b' '))
            .collect();
        let decoder = GeneralPurpose::new(
            &alphabet::STANDARD,
            GeneralPurposeConfig::new()
                .with_decode_padding_mode(DecodePaddingMode::Indifferent)
                .with_decode_allow_trailing_bits(true),
        );
        let result = match decoder.decode(bytes) {
            Ok(bytes) => fit_image(&bytes, mime, limits, processor).await,
            Err(error) => omitted(mime, Some(&error.to_string())),
        };
        match result {
            FittedImage::Omitted { note } => {
                changed = true;
                notes.push(note);
            }
            FittedImage::Fitted {
                image,
                notes: image_notes,
            } => {
                if image["data"] == item["data"] && image["mimeType"] == item["mimeType"] {
                    fitted.push(item.clone());
                } else {
                    changed = true;
                    fitted.push(image);
                    notes.extend(image_notes);
                }
            }
        }
    }
    Ok(if changed {
        FittedImages::Fitted {
            content: fitted,
            notes,
        }
    } else {
        FittedImages::Unchanged
    })
}

/// Match upstream's supported-format sniffing, including its animated PNG exclusion.
pub fn detect_image_mime_type(bytes: &[u8]) -> Option<&'static str> {
    if bytes.starts_with(&[0xff, 0xd8, 0xff]) {
        return (bytes.get(3) != Some(&0xf7)).then_some("image/jpeg");
    }
    if bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
        if bytes.len() < 16 || bytes[8..16] != *b"\0\0\0\rIHDR" {
            return None;
        }
        let mut offset: usize = 8;
        while let Some(header) = bytes.get(offset..offset.saturating_add(8)) {
            let length = u32::from_be_bytes(header[..4].try_into().unwrap()) as usize;
            match &header[4..8] {
                b"acTL" => return None,
                b"IDAT" => break,
                _ => {}
            }
            let Some(next) = offset.checked_add(12).and_then(|n| n.checked_add(length)) else {
                break;
            };
            if next > bytes.len() {
                break;
            }
            offset = next;
        }
        return Some("image/png");
    }
    if bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a") {
        return Some("image/gif");
    }
    if bytes.starts_with(b"RIFF") && bytes.get(8..12) == Some(b"WEBP") {
        return Some("image/webp");
    }
    if bytes.starts_with(b"BM") && bytes.len() >= 26 {
        let le32 = |at| u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap());
        let file = le32(2);
        let pixels = le32(10);
        let dib = le32(14);
        if (file != 0 && file < 26)
            || u64::from(pixels) < 14 + u64::from(dib)
            || (file != 0 && pixels >= file)
        {
            return None;
        }
        let pos = if dib == 12 {
            22
        } else if (40..=124).contains(&dib) && bytes.len() >= 30 {
            26
        } else {
            return None;
        };
        let planes = u16::from_le_bytes(bytes[pos..pos + 2].try_into().unwrap());
        let bits = u16::from_le_bytes(bytes[pos + 2..pos + 4].try_into().unwrap());
        if planes == 1 && [1, 4, 8, 16, 24, 32].contains(&bits) {
            return Some("image/bmp");
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    struct Processor {
        calls: Mutex<Vec<String>>,
        output: Result<Option<PreparedImage>, String>,
    }
    #[async_trait::async_trait]
    impl ImageProcessor for Processor {
        async fn prepare(
            &self,
            _: &[u8],
            mime: &str,
            _: ImageLimits,
        ) -> Result<Option<PreparedImage>, String> {
            self.calls.lock().unwrap().push(mime.into());
            self.output.clone()
        }
    }
    fn processor(output: Result<Option<PreparedImage>, String>) -> Processor {
        Processor {
            calls: Mutex::new(Vec::new()),
            output,
        }
    }
    fn prepared(data: &str, mime: &str) -> PreparedImage {
        PreparedImage {
            data: data.into(),
            mime_type: mime.into(),
            resized: None,
            converted_from: None,
        }
    }

    #[tokio::test]
    async fn fits_ordered_blocks_and_detects_bytes_before_processing() {
        let mut result = prepared("bmV3", "image/jpeg");
        result.converted_from = Some("image/bmp".into());
        result.resized = Some(Resize {
            from: Dimensions {
                width: 400,
                height: 100,
            },
            to: Dimensions {
                width: 200,
                height: 50,
            },
        });
        let p = processor(Ok(Some(result)));
        let input = vec![
            json!({"type":"text","text":"before","signature":"kept"}),
            json!({"type":"image","data":"/9j/AA==","mimeType":"image/png"}),
            json!({"type":"text","text":"after"}),
        ];
        let fitted = fit_images(&input, ImageLimits::default(), &p)
            .await
            .unwrap();
        assert_eq!(*p.calls.lock().unwrap(), vec!["image/jpeg"]);
        assert_eq!(fitted, FittedImages::Fitted {
            content: vec![input[0].clone(), json!({"type":"image","data":"bmV3","mimeType":"image/jpeg"}), input[2].clone()],
            notes: vec!["Converted from image/bmp to image/jpeg.".into(), "Resized from 400x100 to 200x50. Multiply coordinates by 2.00 to map them to the original.".into()],
        });
    }

    #[tokio::test]
    async fn unchanged_and_omissions_match_upstream() {
        let image = json!({"type":"image","data":"aGk=","mimeType":"image/png","detail":"high"});
        let p = processor(Ok(Some(prepared("aGk=", "image/png"))));
        assert_eq!(
            fit_images(&[image.clone()], ImageLimits::default(), &p)
                .await
                .unwrap(),
            FittedImages::Unchanged
        );
        assert_eq!(
            fit_images(
                &[json!({"type":"text","text":"a"})],
                ImageLimits::default(),
                &p
            )
            .await
            .unwrap(),
            FittedImages::Unchanged
        );
        assert_eq!(p.calls.lock().unwrap().len(), 1);
        let p = processor(Err("worker exited".into()));
        assert_eq!(fit_image(b"hi", "image/png", ImageLimits::default(), &p).await, FittedImage::Omitted { note: "An image (image/png) cannot be prepared for the model and was left out: worker exited.".into() });
        let p = processor(Ok(None));
        let input = vec![
            json!({"type":"text","text":"a"}),
            image,
            json!({"type":"image","data":"%%%","mimeType":"image/png"}),
        ];
        let FittedImages::Fitted { content, notes } =
            fit_images(&input, ImageLimits::default(), &p)
                .await
                .unwrap()
        else {
            panic!()
        };
        assert_eq!(content, vec![input[0].clone()]);
        assert_eq!(notes.len(), 2);
        assert_eq!(
            notes[0],
            "An image (image/png) cannot be prepared for the model and was left out."
        );
        assert!(notes[1].starts_with(
            "An image (image/png) cannot be prepared for the model and was left out: "
        ));
        assert_eq!(p.calls.lock().unwrap().len(), 1);
        assert!(
            fit_images(&[json!({"type":"image"})], ImageLimits::default(), &p)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn forwards_decoded_bytes_and_chosen_limits() {
        struct Check;
        #[async_trait::async_trait]
        impl ImageProcessor for Check {
            async fn prepare(
                &self,
                bytes: &[u8],
                mime: &str,
                limits: ImageLimits,
            ) -> Result<Option<PreparedImage>, String> {
                assert_eq!(bytes, b"hi");
                assert_eq!(mime, "image/png");
                assert_eq!(
                    limits,
                    ImageLimits {
                        max_width: 123,
                        max_height: 456,
                        max_bytes: 789
                    }
                );
                Ok(Some(prepared("aGk=", "image/png")))
            }
        }
        let limits = ImageLimits {
            max_width: 123,
            max_height: 456,
            max_bytes: 789,
        };
        let input = vec![json!({"type":"image", "data":"aGk", "mimeType":"image/png"})];
        let FittedImages::Fitted { content, notes } =
            fit_images(&input, limits, &Check).await.unwrap()
        else {
            panic!()
        };
        assert_eq!(content[0]["data"], "aGk=");
        assert!(notes.is_empty());
    }

    #[test]
    fn defaults_overrides_and_signatures() {
        assert_eq!(
            ImageLimitOverrides {
                max_width: Some(100),
                ..Default::default()
            }
            .resolve(),
            ImageLimits {
                max_width: 100,
                max_height: 2000,
                max_bytes: 4_718_592
            }
        );
        assert_eq!(detect_image_mime_type(b"\xff\xd8\xff\xf7"), None);
        assert_eq!(detect_image_mime_type(b"GIF89a"), Some("image/gif"));
        assert_eq!(detect_image_mime_type(b"RIFF0000WEBP"), Some("image/webp"));
        let mut png = b"\x89PNG\r\n\x1a\n\0\0\0\rIHDR".to_vec();
        png.extend_from_slice(&[0; 17]);
        png.extend_from_slice(b"\0\0\0\x08acTL");
        assert_eq!(detect_image_mime_type(&png), None);
    }
}
