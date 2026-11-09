//! Explicit @file inputs for the initial CLI prompt.
use std::path::Path;

use pi_rs::{Error, ImageContent, Result};

pub fn expand(arguments: &[String], workspace: &Path) -> Result<(String, Vec<ImageContent>)> {
    let tools = pi_rs_workbench::coding::CodingTools::local(workspace)?;
    let mut files = String::new();
    let mut messages = Vec::new();
    let mut images = Vec::new();
    for argument in arguments {
        let Some(path) = argument.strip_prefix('@').filter(|path| !path.is_empty()) else {
            messages.push(argument.as_str());
            continue;
        };
        let path = tools.resolve_read_path(Path::new(path))?;
        let (content, attached) = tools.read_attachment(&path)?;
        let name = path
            .to_string_lossy()
            .replace('&', "&amp;")
            .replace('"', "&quot;")
            .replace('<', "&lt;")
            .replace('>', "&gt;");
        files.push_str(&format!("<file name=\"{name}\">\n{content}\n</file>\n"));
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
    fn attachments_use_reader_filename_fallbacks_and_file_urls() {
        let directory = tempfile::tempdir().unwrap();
        let file = directory.path().join("Capture d’ecran.md");
        std::fs::write(&file, "fallback attachment").unwrap();
        let (text, images) = expand(&["@Capture d'ecran.md".into()], directory.path()).unwrap();
        assert!(text.contains("fallback attachment"));
        assert!(images.is_empty());
        let url = reqwest::Url::from_file_path(&file).unwrap();
        let (text, _) = expand(&[format!("@{url}")], directory.path()).unwrap();
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
        )
        .unwrap();
        assert!(text.contains("attachment marker\n"));
        assert!(text.ends_with("describe"));
        assert_eq!(images.len(), 1);
        images[0].validate().unwrap();
        assert!(expand(&["@missing".into()], directory.path()).is_err());
    }
}
