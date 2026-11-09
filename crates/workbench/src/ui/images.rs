//! Terminal image placements for the Kitty and iTerm2 protocols used by Pi.
use std::collections::BTreeSet;
use std::io::{Cursor, Write};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, LazyLock, Mutex};

use base64::Engine;
use crossterm::{
    cursor::{MoveToColumn, MoveUp, RestorePosition, SavePosition},
    queue,
};

use super::render::Line;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Protocol {
    Kitty,
    Iterm2,
}
fn detect(get: impl Fn(&str) -> Option<String>) -> Option<Protocol> {
    match get("PI_IMAGE_PROTOCOL").as_deref() {
        Some("kitty") => return Some(Protocol::Kitty),
        Some("iterm2") => return Some(Protocol::Iterm2),
        Some("none" | "0") => return None,
        _ => {}
    }
    let term = get("TERM").unwrap_or_default().to_lowercase();
    if get("TMUX").is_some() || term.starts_with("tmux") || term.starts_with("screen") {
        return None;
    }
    let program = get("TERM_PROGRAM").unwrap_or_default().to_lowercase();
    if matches!(
        program.as_str(),
        "kitty" | "ghostty" | "wezterm" | "warpterminal" | "herdr" | "rex"
    ) || term.contains("kitty")
        || term.contains("ghostty")
        || [
            "KITTY_WINDOW_ID",
            "GHOSTTY_RESOURCES_DIR",
            "WEZTERM_PANE",
            "WARP_SESSION_ID",
            "WARP_TERMINAL_SESSION_UUID",
        ]
        .iter()
        .any(|key| get(key).is_some())
        || (program == "orca" && get("ORCA_IMAGE_PROTOCOL").as_deref() == Some("kitty"))
    {
        return Some(Protocol::Kitty);
    }
    (program == "iterm.app" || get("ITERM_SESSION_ID").is_some()).then_some(Protocol::Iterm2)
}
static NEXT_ID: LazyLock<AtomicU32> = LazyLock::new(|| {
    AtomicU32::new(
        u32::from_le_bytes(uuid::Uuid::new_v4().as_bytes()[..4].try_into().unwrap()).max(1),
    )
});
#[derive(Debug)]
pub struct InlineImage {
    id: u32,
    data: String,
    bytes: usize,
    width: u32,
    height: u32,
}
impl InlineImage {
    pub fn decode(content: &pi_rs::ImageContent) -> Option<Arc<Self>> {
        content.validate().ok()?;
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(&content.data)
            .ok()?;
        let format = image::guess_format(&bytes).ok()?;
        let reader = image::ImageReader::with_format(Cursor::new(bytes), format);
        let prepared = pi_rs_workbench::coding::normalize_image(reader).ok()?;
        let id = loop {
            let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
            if id != 0 {
                break id;
            }
        };
        Some(Arc::new(Self {
            id,
            data: base64::engine::general_purpose::STANDARD.encode(&prepared.png),
            bytes: prepared.png.len(),
            width: prepared.width,
            height: prepared.height,
        }))
    }
    pub fn lines(self: &Arc<Self>, width: usize) -> Vec<Line> {
        let maximum_columns = width.saturating_sub(2).clamp(1, 60);
        let maximum_rows = crossterm::terminal::size()
            .map(|(_, rows)| usize::from(rows).saturating_sub(8))
            .unwrap_or(20)
            .max(1)
            .min(maximum_columns.div_ceil(2).max(1));
        let (columns, rows) = dimensions(self.width, self.height, maximum_columns, maximum_rows);
        (0..rows)
            .map(|offset| Line {
                image: Some(Placement {
                    image: self.clone(),
                    columns,
                    rows,
                    offset,
                }),
                ..Line::default()
            })
            .collect()
    }
}
fn dimensions(width: u32, height: u32, max_columns: usize, max_rows: usize) -> (usize, usize) {
    let width = u64::from(width.max(1));
    let height = u64::from(height.max(1));
    let max_width = (max_columns.max(1) as u64) * 9;
    let max_height = (max_rows.max(1) as u64) * 18;
    let scale = (max_width as f64 / width as f64)
        .min(max_height as f64 / height as f64)
        .min(1.0);
    let columns = ((width as f64 * scale / 9.0).ceil() as usize).clamp(1, max_columns.max(1));
    let rows = ((height as f64 * scale / 18.0).ceil() as usize).clamp(1, max_rows.max(1));
    (columns, rows)
}
#[derive(Clone, Debug)]
pub struct Placement {
    image: Arc<InlineImage>,
    columns: usize,
    rows: usize,
    offset: usize,
}
#[derive(Clone, Default)]
pub struct Cleanup(Arc<Mutex<BTreeSet<u32>>>);
impl Cleanup {
    pub fn clear(&self) {
        let mut ids = self.0.lock().expect("image registry");
        if ids.is_empty() {
            return;
        }
        let mut out = std::io::stdout().lock();
        for id in ids.iter() {
            let _ = write!(out, "\x1b_Ga=d,d=I,i={id},q=2\x1b\\");
        }
        let _ = out.flush();
        ids.clear();
    }
}
pub struct Renderer {
    protocol: Option<Protocol>,
    uploaded: BTreeSet<u32>,
    placed: BTreeSet<u32>,
    cleanup: Cleanup,
}
impl Default for Renderer {
    fn default() -> Self {
        Self {
            protocol: detect(|key| std::env::var(key).ok()),
            uploaded: BTreeSet::new(),
            placed: BTreeSet::new(),
            cleanup: Cleanup::default(),
        }
    }
}
impl Renderer {
    pub fn enabled(&self) -> bool {
        self.protocol.is_some()
    }
    pub fn cleanup(&self) -> Cleanup {
        self.cleanup.clone()
    }
    pub fn reset(&mut self) {
        self.cleanup.clear();
        self.uploaded.clear();
        self.placed.clear();
    }
    pub fn clear_placements(&mut self, out: &mut impl Write) -> std::io::Result<()> {
        if self.protocol == Some(Protocol::Kitty) {
            for id in &self.placed {
                write!(out, "\x1b_Ga=d,d=i,i={id},q=2\x1b\\")?;
            }
        }
        self.placed.clear();
        Ok(())
    }
    pub fn paint(
        &mut self,
        line: &Line,
        out: &mut impl Write,
        row: usize,
        available_rows: usize,
    ) -> std::io::Result<()> {
        let Some(placement) = &line.image else {
            return Ok(());
        };
        let Some(protocol) = self.protocol else {
            return Ok(());
        };
        let image = &placement.image;
        if available_rows == 0 || self.placed.contains(&image.id) {
            return Ok(());
        }
        if protocol == Protocol::Iterm2
            && (placement.offset + 1 != placement.rows || row.saturating_add(1) < placement.rows)
        {
            return Ok(());
        }
        queue!(out, SavePosition)?;
        if protocol == Protocol::Iterm2 && placement.rows > 1 {
            queue!(out, MoveUp((placement.rows - 1) as u16))?;
        }
        queue!(out, MoveToColumn(0))?;
        match protocol {
            Protocol::Kitty => {
                if !self.uploaded.contains(&image.id) {
                    write_kitty_data(out, image)?;
                    self.uploaded.insert(image.id);
                    self.cleanup
                        .0
                        .lock()
                        .expect("image registry")
                        .insert(image.id);
                }
                let visible_rows = available_rows.min(placement.rows - placement.offset);
                let source_start =
                    (u64::from(image.height) * placement.offset as u64) / placement.rows as u64;
                let source_end = (u64::from(image.height)
                    * (placement.offset + visible_rows) as u64)
                    .div_ceil(placement.rows as u64);
                let source_height = source_end
                    .min(u64::from(image.height))
                    .saturating_sub(source_start)
                    .max(1);
                write!(
                    out,
                    "\x1b_Ga=p,i={},p=1,c={},r={},y={},h={},C=1,q=2;\x1b\\",
                    image.id, placement.columns, visible_rows, source_start, source_height
                )?;
                self.placed.insert(image.id);
            }
            Protocol::Iterm2 => {
                write!(
                    out,
                    "\x1b]1337;File=inline=1;size={};width={};height={};preserveAspectRatio=1:{}\x07",
                    image.bytes, placement.columns, placement.rows, image.data
                )?;
                self.placed.insert(image.id);
            }
        }
        queue!(out, RestorePosition)
    }
}
fn write_kitty_data(out: &mut impl Write, image: &InlineImage) -> std::io::Result<()> {
    let chunks = image.data.as_bytes().chunks(4096);
    let count = chunks.len();
    for (index, chunk) in chunks.enumerate() {
        if index == 0 {
            write!(
                out,
                "\x1b_Ga=t,f=100,i={},q=2,m={};",
                image.id,
                usize::from(index + 1 < count)
            )?;
        } else {
            write!(out, "\x1b_Gm={};", usize::from(index + 1 < count))?;
        }
        out.write_all(chunk)?;
        out.write_all(b"\x1b\\")?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use image::ImageFormat;
    fn fixture() -> Arc<InlineImage> {
        let image = image::DynamicImage::ImageRgba8(image::RgbaImage::from_pixel(
            18,
            36,
            image::Rgba([255, 0, 0, 120]),
        ));
        let mut bytes = Vec::new();
        image
            .write_to(&mut Cursor::new(&mut bytes), ImageFormat::Png)
            .unwrap();
        InlineImage::decode(&pi_rs::ImageContent {
            data: base64::engine::general_purpose::STANDARD.encode(bytes),
            mime_type: "image/png".into(),
        })
        .unwrap()
    }
    fn renderer(protocol: Protocol) -> Renderer {
        Renderer {
            protocol: Some(protocol),
            uploaded: BTreeSet::new(),
            placed: BTreeSet::new(),
            cleanup: Cleanup::default(),
        }
    }
    #[test]
    fn capability_detection_is_conservative_and_accepts_explicit_override() {
        let from = |entries: &[(&str, &str)]| {
            detect(|key| {
                entries
                    .iter()
                    .find(|(name, _)| *name == key)
                    .map(|(_, value)| value.to_string())
            })
        };
        assert_eq!(from(&[("TERM_PROGRAM", "ghostty")]), Some(Protocol::Kitty));
        assert_eq!(
            from(&[("ITERM_SESSION_ID", "fixture")]),
            Some(Protocol::Iterm2)
        );
        assert_eq!(from(&[("TMUX", "1"), ("KITTY_WINDOW_ID", "1")]), None);
        assert_eq!(
            from(&[("TMUX", "1"), ("PI_IMAGE_PROTOCOL", "kitty")]),
            Some(Protocol::Kitty)
        );
        assert_eq!(from(&[("TERM_PROGRAM", "unknown")]), None);
    }
    #[test]
    fn sizing_and_transcoding_preserve_bounds_and_alpha() {
        assert_eq!(dimensions(2, 2, 60, 20), (1, 1));
        assert_eq!(dimensions(600, 1200, 60, 20), (20, 20));
        let image = fixture();
        assert_eq!((image.width, image.height), (18, 36));
        let decoded = image::load_from_memory(
            &base64::engine::general_purpose::STANDARD
                .decode(&image.data)
                .unwrap(),
        )
        .unwrap()
        .to_rgba8();
        assert_eq!(decoded.get_pixel(0, 0).0[3], 120);
    }
    #[test]
    fn kitty_uploads_once_and_crops_the_visible_source_rows() {
        let mut renderer = renderer(Protocol::Kitty);
        let line = Line {
            image: Some(Placement {
                image: fixture(),
                columns: 2,
                rows: 2,
                offset: 1,
            }),
            ..Default::default()
        };
        let mut output = Vec::new();
        renderer.paint(&line, &mut output, 0, 1).unwrap();
        let first = String::from_utf8(output).unwrap();
        assert!(first.contains("a=t,f=100"));
        assert!(first.contains("a=p,"));
        assert!(first.contains("r=1,y=18,h=18"));
        let mut duplicate = Vec::new();
        renderer.paint(&line, &mut duplicate, 1, 1).unwrap();
        assert!(duplicate.is_empty());
        let mut next = Vec::new();
        renderer.clear_placements(&mut next).unwrap();
        renderer.paint(&line, &mut next, 2, 2).unwrap();
        let next = String::from_utf8(next).unwrap();
        assert!(next.contains("a=d,d=i"));
        assert!(!next.contains("a=t,f=100"));
        assert!(next.contains("C=1,q=2"));
    }
    #[test]
    fn iterm_uses_inline_data_and_restores_the_cursor() {
        let mut renderer = renderer(Protocol::Iterm2);
        let line = Line {
            image: Some(Placement {
                image: fixture(),
                columns: 2,
                rows: 1,
                offset: 0,
            }),
            ..Default::default()
        };
        let mut output = Vec::new();
        renderer.paint(&line, &mut output, 0, 1).unwrap();
        let output = String::from_utf8(output).unwrap();
        assert!(output.contains("1337;File=inline=1;"));
        assert!(output.contains("width=2;height=1;preserveAspectRatio=1:"));
        assert!(!output.contains("[0A"));
    }
    #[test]
    fn kitty_payload_chunks_terminate_the_transfer() {
        let image = InlineImage {
            id: 7,
            data: "A".repeat(9000),
            bytes: 6750,
            width: 90,
            height: 90,
        };
        let mut output = Vec::new();
        write_kitty_data(&mut output, &image).unwrap();
        let output = String::from_utf8(output).unwrap();
        assert_eq!(output.matches("\x1b_G").count(), 3);
        assert!(output.starts_with("\x1b_Ga=t,f=100,i=7,q=2,m=1;"));
        assert!(output.contains("\x1b_Gm=0;"));
        assert!(output.ends_with("\x1b\\"));
    }
}
