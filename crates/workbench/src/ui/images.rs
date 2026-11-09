//! Terminal image placements for the Kitty and iTerm2 protocols used by Pi.
use std::collections::{BTreeMap, BTreeSet};
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
        let cell = cell_dimensions();
        let maximum_rows = ((maximum_columns as f64 * cell.0) / cell.1).ceil().max(1.0) as usize;
        let optimize = detect(|key| std::env::var(key).ok()) == Some(Protocol::Kitty);
        let (columns, rows) = dimensions(
            self.width,
            self.height,
            maximum_columns,
            maximum_rows,
            cell,
            optimize,
        );
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
#[cfg(any(unix, windows))]
static REPORTED_CELL: AtomicU32 = AtomicU32::new(0);

pub fn query_cell_size() -> std::io::Result<()> {
    #[cfg(any(unix, windows))]
    {
        REPORTED_CELL.store(0, Ordering::Relaxed);
        let mut out = std::io::stdout().lock();
        out.write_all(b"\x1b[16t")?;
        out.flush()?;
    }
    Ok(())
}

#[cfg(any(unix, windows))]
pub fn set_cell_dimensions(width: u16, height: u16) -> bool {
    if width == 0 || height == 0 || width > 1024 || height > 1024 {
        return false;
    }
    let dimensions = (u32::from(width) << 16) | u32::from(height);
    REPORTED_CELL.swap(dimensions, Ordering::Relaxed) != dimensions
}

pub fn cell_dimensions() -> (f64, f64) {
    #[cfg(any(unix, windows))]
    {
        let reported = REPORTED_CELL.load(Ordering::Relaxed);
        if reported != 0 {
            return (
                f64::from((reported >> 16) as u16),
                f64::from(reported as u16),
            );
        }
    }
    crossterm::terminal::window_size()
        .ok()
        .and_then(|size| reported_cell_dimensions(&size))
        .unwrap_or((9.0, 18.0))
}

fn reported_cell_dimensions(size: &crossterm::terminal::WindowSize) -> Option<(f64, f64)> {
    if size.columns == 0 || size.rows == 0 {
        return None;
    }
    let width = f64::from(size.width) / f64::from(size.columns);
    let height = f64::from(size.height) / f64::from(size.rows);
    ((1.0..=1024.0).contains(&width) && (1.0..=1024.0).contains(&height)).then_some((width, height))
}

fn dimensions(
    width: u32,
    height: u32,
    max_columns: usize,
    max_rows: usize,
    cell: (f64, f64),
    optimize: bool,
) -> (usize, usize) {
    let width = f64::from(width.max(1));
    let height = f64::from(height.max(1));
    let max_columns = max_columns.max(1);
    let max_rows = max_rows.max(1);
    let width_scale = max_columns as f64 * cell.0 / width;
    let height_scale = max_rows as f64 * cell.1 / height;
    let scale = width_scale.min(height_scale);
    let mut columns = (width * scale / cell.0).ceil().max(1.0) as usize;
    let mut rows = (height * scale / cell.1).ceil().max(1.0) as usize;
    columns = columns.min(max_columns);
    rows = rows.min(max_rows);
    if optimize {
        if width_scale <= height_scale {
            rows = less_distorted(rows, columns as f64 * cell.0 * height / (width * cell.1));
        } else {
            columns = less_distorted(columns, rows as f64 * cell.1 * width / (height * cell.0));
        }
    }
    (columns, rows)
}

fn less_distorted(upper: usize, ideal: f64) -> usize {
    if upper <= 1 {
        return upper;
    }
    let lower = upper - 1;
    let upper_error = (upper as f64 / ideal).max(ideal / upper as f64);
    let lower_error = (lower as f64 / ideal).max(ideal / lower as f64);
    if lower_error < upper_error {
        lower
    } else {
        upper
    }
}

#[derive(Clone, Debug)]
pub struct Placement {
    image: Arc<InlineImage>,
    columns: usize,
    rows: usize,
    offset: usize,
}
/// Count the contiguous visible portion of one image, excluding following text or widgets.
pub fn visible_rows(lines: &[Line]) -> usize {
    let Some(first) = lines.first().and_then(|line| line.image.as_ref()) else {
        return 0;
    };
    lines
        .iter()
        .enumerate()
        .take_while(|(index, line)| {
            line.image.as_ref().is_some_and(|image| {
                image.image.id == first.image.id && image.offset == first.offset + index
            })
        })
        .count()
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
struct CroppedImage {
    data: String,
    bytes: usize,
}

pub struct Renderer {
    protocol: Option<Protocol>,
    uploaded: BTreeSet<u32>,
    placed: BTreeSet<(u32, u32)>,
    cleanup: Cleanup,
    crops: BTreeMap<(u32, u32, u32), Arc<CroppedImage>>,
}
impl Default for Renderer {
    fn default() -> Self {
        Self {
            protocol: detect(|key| std::env::var(key).ok()),
            uploaded: BTreeSet::new(),
            placed: BTreeSet::new(),
            cleanup: Cleanup::default(),
            crops: BTreeMap::new(),
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
        self.crops.clear();
    }
    pub fn clear_placements(&mut self, out: &mut impl Write) -> std::io::Result<()> {
        self.clear_region(out, None)
    }
    pub fn clear_active(&mut self, out: &mut impl Write) -> std::io::Result<()> {
        self.clear_region(out, Some(2))
    }
    fn clear_region(&mut self, out: &mut impl Write, region: Option<u32>) -> std::io::Result<()> {
        let placements: Vec<_> = self
            .placed
            .iter()
            .copied()
            .filter(|(_, placement)| region.is_none_or(|region| region == *placement))
            .collect();
        for (id, placement) in placements {
            if self.protocol == Some(Protocol::Kitty) {
                write!(out, "\x1b_Ga=d,d=i,i={id},p={placement},q=2\x1b\\")?;
            }
            self.placed.remove(&(id, placement));
        }
        Ok(())
    }
    pub fn paint(
        &mut self,
        line: &Line,
        out: &mut impl Write,
        row: usize,
        available_rows: usize,
    ) -> std::io::Result<()> {
        self.paint_region(line, out, row, available_rows, 1)
    }
    pub fn paint_active(
        &mut self,
        line: &Line,
        out: &mut impl Write,
        row: usize,
        available_rows: usize,
    ) -> std::io::Result<()> {
        self.paint_region(line, out, row, available_rows, 2)
    }
    fn paint_region(
        &mut self,
        line: &Line,
        out: &mut impl Write,
        row: usize,
        available_rows: usize,
        placement_id: u32,
    ) -> std::io::Result<()> {
        let Some(placement) = &line.image else {
            return Ok(());
        };
        let Some(protocol) = self.protocol else {
            return Ok(());
        };
        let image = &placement.image;
        if available_rows == 0 || self.placed.contains(&(image.id, placement_id)) {
            return Ok(());
        }
        let iterm_rows = if row == usize::MAX {
            if protocol == Protocol::Iterm2 && placement.offset + 1 != placement.rows {
                return Ok(());
            }
            placement.rows
        } else {
            if protocol == Protocol::Iterm2 && available_rows != 1 {
                return Ok(());
            }
            (placement.offset + 1).min(row + 1)
        };
        queue!(out, SavePosition)?;
        if protocol == Protocol::Iterm2 && iterm_rows > 1 {
            queue!(out, MoveUp((iterm_rows - 1) as u16))?;
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
                    "\x1b_Ga=p,i={},p={},c={},r={},y={},h={},C=1,q=2;\x1b\\",
                    image.id,
                    placement_id,
                    placement.columns,
                    visible_rows,
                    source_start,
                    source_height
                )?;
                self.placed.insert((image.id, placement_id));
            }
            Protocol::Iterm2 => {
                let hidden_rows = placement.offset + 1 - iterm_rows;
                let source_start =
                    (u64::from(image.height) * hidden_rows as u64) / placement.rows as u64;
                let source_end = (u64::from(image.height) * (hidden_rows + iterm_rows) as u64)
                    .div_ceil(placement.rows as u64)
                    .min(u64::from(image.height));
                if source_start == 0 && source_end == u64::from(image.height) {
                    write_iterm(out, &image.data, image.bytes, placement.columns, iterm_rows)?;
                } else {
                    let crop = self.cropped_image(
                        image,
                        source_start as u32,
                        (source_end - source_start).max(1) as u32,
                    )?;
                    write_iterm(out, &crop.data, crop.bytes, placement.columns, iterm_rows)?;
                }
                self.placed.insert((image.id, placement_id));
            }
        }
        queue!(out, RestorePosition)
    }

    fn cropped_image(
        &mut self,
        image: &InlineImage,
        top: u32,
        height: u32,
    ) -> std::io::Result<Arc<CroppedImage>> {
        let key = (image.id, top, height);
        if let Some(crop) = self.crops.get(&key) {
            return Ok(crop.clone());
        }
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(&image.data)
            .map_err(std::io::Error::other)?;
        let image = image::load_from_memory(&bytes).map_err(std::io::Error::other)?;
        let cropped = image.crop_imm(0, top, image.width(), height);
        let mut png = Vec::new();
        cropped
            .write_to(&mut Cursor::new(&mut png), image::ImageFormat::Png)
            .map_err(std::io::Error::other)?;
        let crop = Arc::new(CroppedImage {
            data: base64::engine::general_purpose::STANDARD.encode(&png),
            bytes: png.len(),
        });
        const CACHE_BYTES: usize = 4 * 1024 * 1024;
        if crop.data.len() <= CACHE_BYTES {
            let cached: usize = self.crops.values().map(|crop| crop.data.len()).sum();
            if cached + crop.data.len() > CACHE_BYTES {
                self.crops.clear();
            }
            self.crops.insert(key, crop.clone());
        }
        Ok(crop)
    }
}
fn write_iterm(
    out: &mut impl Write,
    data: &str,
    bytes: usize,
    columns: usize,
    rows: usize,
) -> std::io::Result<()> {
    write!(
        out,
        "\x1b]1337;File=inline=1;size={bytes};width={columns};height={rows};preserveAspectRatio=1:{data}\x07"
    )
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
        let mut pixels = image::RgbaImage::from_pixel(18, 36, image::Rgba([255, 0, 0, 120]));
        for y in 18..36 {
            for x in 0..18 {
                pixels.put_pixel(x, y, image::Rgba([0, 0, 255, 120]));
            }
        }
        let image = image::DynamicImage::ImageRgba8(pixels);
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
            crops: BTreeMap::new(),
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
        assert_eq!(dimensions(2, 2, 60, 20, (9.0, 18.0), false), (40, 20));
        assert_eq!(dimensions(600, 1200, 60, 20, (9.0, 18.0), false), (20, 20));
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
    fn image_dimensions_match_the_pinned_reference() {
        for (width, height, columns, rows, cell, ordinary, optimized) in [
            (2, 2, 60, 20, (9.0, 18.0), (40, 20), (40, 20)),
            (600, 1200, 60, 20, (9.0, 18.0), (20, 20), (20, 20)),
            (201, 101, 60, 30, (9.0, 18.0), (60, 16), (60, 15)),
            (321, 200, 58, 28, (9.5, 19.75), (58, 18), (58, 17)),
            (100, 333, 41, 19, (7.0, 17.0), (14, 19), (14, 19)),
            (888, 151, 58, 25, (11.0, 21.0), (58, 6), (58, 5)),
            (1, 1000, 60, 30, (8.0, 16.0), (1, 30), (1, 30)),
            (1000, 1, 60, 30, (8.0, 16.0), (60, 1), (60, 1)),
        ] {
            assert_eq!(
                dimensions(width, height, columns, rows, cell, false),
                ordinary
            );
            assert_eq!(
                dimensions(width, height, columns, rows, cell, true),
                optimized
            );
        }
    }

    #[test]
    fn terminal_pixel_metrics_keep_fractional_cells_and_reject_missing_values() {
        let size = crossterm::terminal::WindowSize {
            columns: 100,
            rows: 40,
            width: 950,
            height: 790,
        };
        assert_eq!(reported_cell_dimensions(&size), Some((9.5, 19.75)));
        for size in [
            crossterm::terminal::WindowSize { width: 0, ..size },
            crossterm::terminal::WindowSize { height: 0, ..size },
            crossterm::terminal::WindowSize { columns: 0, ..size },
            crossterm::terminal::WindowSize { rows: 0, ..size },
            crossterm::terminal::WindowSize { width: 1, ..size },
        ] {
            assert_eq!(reported_cell_dimensions(&size), None);
        }
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
    fn active_placements_preserve_committed_scrollback_images() {
        let mut renderer = renderer(Protocol::Kitty);
        let image = fixture();
        let line = Line {
            image: Some(Placement {
                image: image.clone(),
                columns: 2,
                rows: 2,
                offset: 0,
            }),
            ..Default::default()
        };
        let mut output = Vec::new();
        renderer.paint(&line, &mut output, usize::MAX, 2).unwrap();
        renderer.paint_active(&line, &mut output, 0, 2).unwrap();
        let output = String::from_utf8(output).unwrap();
        assert!(output.contains(&format!("i={},p=1", image.id)));
        assert!(output.contains(&format!("i={},p=2", image.id)));
        assert_eq!(output.matches("a=t,f=100").count(), 1);
        let mut cleared = Vec::new();
        renderer.clear_active(&mut cleared).unwrap();
        assert_eq!(
            String::from_utf8(cleared).unwrap(),
            format!("\x1b_Ga=d,d=i,i={},p=2,q=2\x1b\\", image.id)
        );
        let mut unchanged = Vec::new();
        renderer
            .paint(&line, &mut unchanged, usize::MAX, 2)
            .unwrap();
        assert!(unchanged.is_empty());
        renderer.clear_placements(&mut unchanged).unwrap();
        assert!(
            String::from_utf8(unchanged)
                .unwrap()
                .contains(&format!("i={},p=1", image.id))
        );
    }

    #[test]
    fn visible_image_rows_stop_before_widgets_and_offset_gaps() {
        let image = fixture();
        let line = |offset| Line {
            image: Some(Placement {
                image: image.clone(),
                columns: 2,
                rows: 3,
                offset,
            }),
            ..Default::default()
        };
        assert_eq!(visible_rows(&[line(1), line(2), Line::default()]), 2);
        assert_eq!(visible_rows(&[line(0), line(2)]), 1);
        assert_eq!(visible_rows(&[Line::default(), line(0)]), 0);
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
    fn iterm_crops_visible_rows_and_reuses_encoded_pixels() {
        let mut renderer = renderer(Protocol::Iterm2);
        let image = fixture();
        for (offset, expected) in [(0, [255, 0, 0, 120]), (1, [0, 0, 255, 120])] {
            let line = Line {
                image: Some(Placement {
                    image: image.clone(),
                    columns: 2,
                    rows: 2,
                    offset,
                }),
                ..Default::default()
            };
            let mut output = Vec::new();
            renderer.paint_active(&line, &mut output, 0, 1).unwrap();
            let output = String::from_utf8(output).unwrap();
            let encoded = output
                .split("preserveAspectRatio=1:")
                .nth(1)
                .unwrap()
                .split('\x07')
                .next()
                .unwrap();
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(encoded)
                .unwrap();
            let pixels = image::load_from_memory(&bytes).unwrap().to_rgba8();
            assert_eq!(pixels.dimensions(), (18, 18));
            assert_eq!(pixels.get_pixel(0, 0).0, expected);
            assert!(output.contains("width=2;height=1;"));
            renderer.clear_active(&mut Vec::new()).unwrap();
        }
        let first = renderer.cropped_image(&image, 0, 18).unwrap();
        let second = renderer.cropped_image(&image, 0, 18).unwrap();
        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(renderer.crops.len(), 2);
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
