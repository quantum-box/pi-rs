//! Explicit @file inputs for the initial CLI prompt.
use std::path::Path;

use pi_rs::{Error, ImageContent, Result};
use pi_rs_workbench::image_profile::ImageResizeProfile;

pub fn expand(
    arguments: &[String],
    workspace: &Path,
    auto_resize: bool,
    profile: Option<ImageResizeProfile>,
) -> Result<(String, Vec<ImageContent>)> {
    let tools = pi_rs_workbench::coding::CodingTools::local(workspace)?
        .with_image_resize(auto_resize)
        .with_image_profile(profile);
    let mut files = String::new();
    let mut messages = Vec::new();
    let mut images = Vec::new();
    for argument in arguments {
        let Some(path) = argument.strip_prefix('@').filter(|path| !path.is_empty()) else {
            messages.push(argument.as_str());
            continue;
        };
        let path = tools.resolve_read_path(Path::new(path))?;
        let metadata = path.metadata()?;
        if metadata.is_file() && metadata.len() == 0 {
            continue;
        }
        let (content, attached) = tools.read_attachment_lossy(&path)?;
        let name = path
            .to_string_lossy()
            .replace('&', "&amp;")
            .replace('"', "&quot;")
            .replace('<', "&lt;")
            .replace('>', "&gt;");
        if attached.is_empty() {
            let content = content.strip_prefix('\u{feff}').unwrap_or(&content);
            files.push_str(&format!("<file name=\"{name}\">\n{content}\n</file>\n"));
        } else {
            let hints = if auto_resize { &content } else { "" };
            files.push_str(&format!("<file name=\"{name}\">{hints}</file>\n"));
        }
        images.extend(attached);
        if files
            .len()
            .saturating_add(images.iter().map(|image| image.data.len()).sum::<usize>())
            > 8 * 1024 * 1024
        {
            return Err(Error::Config(
                "attachments exceed the 8 MiB input limit".into(),
            ));
        }
    }
    files.push_str(&messages.join(" "));
    Ok((files, images))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_files_are_skipped_and_text_attachment_boms_are_removed() {
        let directory = tempfile::tempdir().unwrap();
        std::fs::write(directory.path().join("empty.md"), "").unwrap();
        std::fs::write(directory.path().join("note.md"), "\u{feff}text").unwrap();
        let (text, images) = expand(
            &["@empty.md".into(), "@note.md".into(), "prompt".into()],
            directory.path(),
            true,
            None,
        )
        .unwrap();
        assert!(images.is_empty());
        assert!(!text.contains("empty.md"));
        assert!(text.ends_with("note.md\">\ntext\n</file>\nprompt"));
    }
    #[test]
    fn malformed_utf8_text_attachments_use_replacement_characters() {
        let directory = tempfile::tempdir().unwrap();
        std::fs::write(
            directory.path().join("broken.txt"),
            b"\xef\xbb\xbfhello \xffworld\xe2\x82",
        )
        .unwrap();
        let (text, images) = expand(
            &["@broken.txt".into(), "summarize".into()],
            directory.path(),
            true,
            None,
        )
        .unwrap();
        assert!(images.is_empty());
        assert!(text.ends_with("broken.txt\">\nhello \u{fffd}world\u{fffd}\n</file>\nsummarize"));
    }

    #[test]
    fn corrupt_detected_image_retains_image_omission() {
        let directory = tempfile::tempdir().unwrap();
        std::fs::write(
            directory.path().join("broken.png"),
            b"\x89PNG\r\n\x1a\n\0\0\0\x0dIHDR",
        )
        .unwrap();
        let (text, images) = expand(&["@broken.png".into()], directory.path(), true, None).unwrap();
        assert!(images.is_empty());
        assert!(text.contains("[Image omitted:"), "{text}");
        assert!(!text.contains('\u{fffd}'));
    }

    #[test]
    fn short_and_animated_png_are_decoded_as_text() {
        let directory = tempfile::tempdir().unwrap();
        let short = b"\x89PNG\r\n\x1a\ncorrupt";
        std::fs::write(directory.path().join("short.png"), short).unwrap();
        let mut animated = b"\x89PNG\r\n\x1a\n\0\0\0\x0dIHDR".to_vec();
        animated.extend_from_slice(&[0; 17]); // IHDR data and CRC
        animated.extend_from_slice(b"\0\0\0\x08acTL");
        std::fs::write(directory.path().join("animated.png"), &animated).unwrap();
        for (name, bytes) in [
            ("short.png", short.as_slice()),
            ("animated.png", animated.as_slice()),
        ] {
            let (text, images) =
                expand(&[format!("@{name}")], directory.path(), true, None).unwrap();
            assert!(images.is_empty());
            assert!(
                text.contains(String::from_utf8_lossy(bytes).as_ref()),
                "{text:?}"
            );
            assert!(!text.contains("[Image omitted:"));
        }
    }

    #[test]
    fn image_extensions_do_not_make_text_or_partial_png_an_image() {
        let directory = tempfile::tempdir().unwrap();
        let signature = b"\x89PNG\r\n\x1a\n";
        for size in 1..signature.len() {
            let name = format!("partial-{size}.png");
            let bytes = &signature[..size];
            std::fs::write(directory.path().join(&name), bytes).unwrap();
            let (text, images) =
                expand(&[format!("@{name}")], directory.path(), true, None).unwrap();
            assert!(images.is_empty());
            assert!(
                text.contains(String::from_utf8_lossy(bytes).as_ref()),
                "{text:?}"
            );
            assert!(!text.contains("[Image omitted:"));
        }
        for name in ["note.png", "note.jpg", "note.webp"] {
            std::fs::write(directory.path().join(name), "ordinary text").unwrap();
            let (text, images) =
                expand(&[format!("@{name}")], directory.path(), true, None).unwrap();
            assert!(images.is_empty());
            assert!(text.contains("ordinary text"));
            assert!(!text.contains("[Image omitted:"));
        }
    }

    #[test]
    fn malformed_text_still_obeys_aggregate_input_limit() {
        let directory = tempfile::tempdir().unwrap();
        let mut bytes = vec![b'a'; 8 * 1024 * 1024];
        bytes.push(0xff);
        std::fs::write(directory.path().join("large.txt"), bytes).unwrap();
        let error = expand(&["@large.txt".into()], directory.path(), true, None).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("attachments exceed the 8 MiB input limit")
        );
    }

    #[test]
    fn attachments_use_reader_filename_fallbacks_and_file_urls() {
        let directory = tempfile::tempdir().unwrap();
        let file = directory.path().join("Capture d’ecran.md");
        std::fs::write(&file, "fallback attachment").unwrap();
        let (text, images) = expand(
            &["@Capture d'ecran.md".into()],
            directory.path(),
            true,
            None,
        )
        .unwrap();
        assert!(text.contains("fallback attachment"));
        assert!(images.is_empty());
        let url = reqwest::Url::from_file_path(&file).unwrap();
        let (text, _) = expand(&[format!("@{url}")], directory.path(), true, None).unwrap();
        assert!(text.contains("fallback attachment"));
    }

    #[test]
    fn explicit_text_and_image_files_share_the_normal_attachment_reader() {
        let directory = tempfile::tempdir().unwrap();
        std::fs::write(directory.path().join("note.md"), "attachment marker\n").unwrap();
        image::RgbaImage::from_pixel(2, 2, image::Rgba([255, 0, 0, 255]))
            .save(directory.path().join("image.png"))
            .unwrap();
        let (text, images) = expand(
            &["@note.md".into(), "@image.png".into(), "describe".into()],
            directory.path(),
            true,
            None,
        )
        .unwrap();
        assert!(text.contains("attachment marker\n"));
        assert!(text.ends_with("describe"));
        assert_eq!(images.len(), 1);
        images[0].validate().unwrap();
        assert!(expand(&["@missing".into()], directory.path(), true, None).is_err());
    }

    #[test]
    fn original_mode_preserves_jpeg_bytes_and_resize_mode_caps_dimensions() {
        use base64::Engine as _;

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("photo.jpg");
        image::RgbImage::from_pixel(2400, 12, image::Rgb([20, 100, 220]))
            .save(&path)
            .unwrap();
        let input = ["@photo.jpg".into()];
        let (reference, original) = expand(&input, directory.path(), false, None).unwrap();
        assert!(reference.ends_with("photo.jpg\"></file>\n"));
        assert_eq!(original[0].mime_type, "image/jpeg");
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(&original[0].data)
            .unwrap();
        assert_eq!(bytes, std::fs::read(&path).unwrap());

        let (_, resized) = expand(&input, directory.path(), true, None).unwrap();
        assert_eq!(resized[0].mime_type, "image/png");
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(&resized[0].data)
            .unwrap();
        assert_eq!(image::load_from_memory(&bytes).unwrap().width(), 2000);
    }
}
