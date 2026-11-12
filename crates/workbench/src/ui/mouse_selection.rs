//! Mouse selection over a rendered transcript, using terminal cells and UTF-8 boundaries.
use std::ops::Range;
use std::time::{Duration, Instant};

use unicode_segmentation::UnicodeSegmentation;
use unicode_width::UnicodeWidthStr;

use super::render::Line;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Point {
    row: usize,
    byte: usize,
}

#[derive(Clone, Copy)]
enum Granularity {
    Glyph,
    Word,
    Line,
}

struct Gesture {
    anchor: Point,
    focus: Point,
    granularity: Granularity,
    dragging: bool,
    moved: bool,
}

#[derive(Default)]
pub struct MouseSelection {
    rows: Vec<String>,
    screen: Range<usize>,
    top: usize,
    gesture: Option<Gesture>,
    last_click: Option<(Point, Instant, u8)>,
}

impl MouseSelection {
    pub fn update(&mut self, lines: &[Line], top: usize, screen: Range<usize>) {
        self.top = top;
        self.screen = screen.start
            ..screen
                .end
                .min(screen.start + lines.len().saturating_sub(top));
        let selected = self.gesture.as_ref().map(|gesture| {
            gesture.anchor.row.min(gesture.focus.row)..=gesture.anchor.row.max(gesture.focus.row)
        });
        let mut changed = selected
            .as_ref()
            .is_some_and(|range| *range.end() >= lines.len());
        self.rows.resize_with(lines.len(), String::new);
        for (index, (line, row)) in lines.iter().zip(&mut self.rows).enumerate() {
            if row
                .bytes()
                .ne(line.spans.iter().flat_map(|span| span.text.bytes()))
            {
                changed |= selected
                    .as_ref()
                    .is_some_and(|range| range.contains(&index));
                row.clear();
                row.extend(line.spans.iter().map(|span| span.text.as_str()));
            }
        }
        if changed {
            self.clear();
        }
    }

    pub fn clear(&mut self) {
        self.gesture = None;
        self.last_click = None;
    }

    pub fn focus_lost(&mut self) {
        if self
            .gesture
            .as_ref()
            .is_some_and(|gesture| gesture.dragging)
        {
            self.clear();
        } else {
            self.last_click = None;
        }
    }

    pub fn relative_row(&self, row: u16) -> usize {
        usize::from(row).saturating_sub(self.screen.start)
    }

    pub fn contains_row(&self, row: u16) -> bool {
        self.screen.contains(&usize::from(row))
    }

    fn point(&self, column: u16, screen_row: u16) -> Option<Point> {
        if self.rows.is_empty() || self.screen.is_empty() {
            return None;
        }
        let screen_row = usize::from(screen_row).clamp(self.screen.start, self.screen.end - 1);
        let row = (self.top + screen_row - self.screen.start).min(self.rows.len() - 1);
        let line = &self.rows[row];
        let mut cell = 0;
        let mut byte = line.len();
        for (offset, glyph) in line.grapheme_indices(true) {
            cell += glyph.width();
            if cell > usize::from(column) {
                byte = offset;
                break;
            }
        }
        Some(Point { row, byte })
    }

    pub fn press(&mut self, column: u16, row: u16, now: Instant) -> bool {
        if !self.contains_row(row) {
            self.clear();
            return false;
        }
        let Some(point) = self.point(column, row) else {
            return false;
        };
        let count = self
            .last_click
            .filter(|(previous, time, _)| {
                *previous == point && now.duration_since(*time) < Duration::from_millis(400)
            })
            .map_or(1, |(_, _, count)| count % 3 + 1);
        self.last_click = Some((point, now, count));
        let granularity = match count {
            2 => Granularity::Word,
            3 => Granularity::Line,
            _ => Granularity::Glyph,
        };
        self.gesture = Some(Gesture {
            anchor: point,
            focus: point,
            granularity,
            dragging: true,
            moved: false,
        });
        count == 1
    }

    pub fn drag(&mut self, column: u16, row: u16) {
        if let Some(point) = self.point(column, row)
            && let Some(gesture) = &mut self.gesture
            && gesture.dragging
        {
            gesture.moved |= point != gesture.anchor;
            gesture.focus = point;
            if gesture.moved {
                self.last_click = None;
            }
        }
    }

    pub fn release(&mut self, column: u16, row: u16) -> bool {
        self.drag(column, row);
        if let Some(gesture) = &mut self.gesture
            && gesture.dragging
        {
            gesture.dragging = false;
            return gesture.moved || !matches!(gesture.granularity, Granularity::Glyph);
        }
        false
    }

    fn bounds(&self) -> Option<Range<Point>> {
        let gesture = self.gesture.as_ref()?;
        if gesture.anchor == gesture.focus && matches!(gesture.granularity, Granularity::Glyph) {
            return None;
        }
        let start = gesture.anchor.min(gesture.focus);
        let end = gesture.anchor.max(gesture.focus);
        let start_byte = self.range(start, gesture.granularity)?.start;
        let end_byte = self.range(end, gesture.granularity)?.end;
        Some(
            Point {
                byte: start_byte,
                ..start
            }..Point {
                byte: end_byte,
                ..end
            },
        )
    }

    fn range(&self, point: Point, granularity: Granularity) -> Option<Range<usize>> {
        let line = self.rows.get(point.row)?;
        let byte = point.byte.min(line.len());
        if matches!(granularity, Granularity::Line) {
            return Some(0..line.len());
        }
        if matches!(granularity, Granularity::Word)
            && let Some((start, word)) = line
                .unicode_word_indices()
                .find(|(start, word)| (*start..*start + word.len()).contains(&byte))
        {
            return Some(start..start + word.len());
        }
        Some(
            line.grapheme_indices(true)
                .find_map(|(start, glyph)| {
                    let range = start..start + glyph.len();
                    range.contains(&byte).then_some(range)
                })
                .unwrap_or(line.len()..line.len()),
        )
    }

    fn bytes(&self, range: &Range<Point>, row: usize) -> Range<usize> {
        let length = self.rows[row].len();
        let start = if row == range.start.row {
            range.start.byte
        } else {
            0
        };
        let end = if row == range.end.row {
            range.end.byte
        } else {
            length
        };
        start..end
    }

    pub fn text(&self) -> Option<String> {
        let range = self.bounds()?;
        let mut text = String::new();
        for row in range.start.row..=range.end.row {
            if row != range.start.row {
                text.push('\n');
            }
            text.push_str(self.rows[row][self.bytes(&range, row)].trim_end());
        }
        (!text.is_empty()).then_some(text)
    }

    pub fn highlight(&self, lines: &mut [Line], visible: Range<usize>) {
        if let Some(range) = self.bounds() {
            let start = range.start.row.max(visible.start);
            let end = (range.end.row + 1).min(visible.end);
            for (row, line) in lines.iter_mut().enumerate().take(end).skip(start) {
                line.style_bytes(self.bytes(&range, row), |span| span.inverse = true);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossterm::style::Color;

    #[test]
    fn copies_wide_glyphs_and_multirow_selection_in_either_direction() {
        let lines = [
            Line::plain("a東京", Color::Reset),
            Line::plain("plain text ", Color::Reset),
        ];
        let mut selection = MouseSelection::default();
        selection.update(&lines, 0, 1..3);
        let now = Instant::now();
        selection.press(3, 2, now);
        assert!(selection.release(2, 1));
        assert_eq!(selection.text().as_deref(), Some("東京\nplai"));
        selection.press(7, 2, now);
        assert!(!selection.release(7, 2));
        selection.press(7, 2, now + Duration::from_millis(100));
        assert!(selection.release(7, 2));
        assert_eq!(selection.text().as_deref(), Some("text"));
        selection.press(7, 2, now + Duration::from_millis(200));
        assert!(selection.release(7, 2));
        assert_eq!(selection.text().as_deref(), Some("plain text"));
    }
}
