//! Explicit @file inputs for the initial CLI prompt.
use std::path::{Path, PathBuf};

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
        let path = if let Some(relative) = path.strip_prefix("~/") {
            let home = std::env::var_os("HOME")
                .or_else(|| std::env::var_os("USERPROFILE"))
                .ok_or_else(|| Error::Config("home directory is unavailable".into()))?;
            PathBuf::from(home).join(relative)
        } else {
            PathBuf::from(path)
        };
        let path = if path.is_absolute() {
            path
        } else {
            workspace.join(path)
        }
        .canonicalize()?;
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
