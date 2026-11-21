//! Collect OSC color replies into the snapshots Pi applies at query boundaries.
use std::time::{Duration, Instant};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct ColorSnapshot {
    pub foreground: Option<[u8; 3]>,
    pub background: Option<[u8; 3]>,
    pub palette: Option<[[u8; 3]; 16]>,
}

pub(crate) struct ColorBatch {
    started: Instant,
    foreground: Option<[u8; 3]>,
    background: Option<[u8; 3]>,
    palette: [Option<[u8; 3]>; 16],
    replied: u32,
    timed_out: bool,
    completed: bool,
}

impl ColorBatch {
    pub(crate) fn new(started: Instant) -> Self {
        Self {
            started,
            foreground: None,
            background: None,
            palette: [None; 16],
            replied: 0,
            timed_out: false,
            completed: false,
        }
    }

    fn snapshot(&self) -> ColorSnapshot {
        let palette = self
            .palette
            .iter()
            .copied()
            .collect::<Option<Vec<_>>>()
            .map(|colors| colors.try_into().expect("sixteen palette colors"));
        ColorSnapshot {
            foreground: self.foreground,
            background: self.background,
            palette,
        }
    }

    /// Records a valid color or invalid response, and completes after every target replies.
    pub(crate) fn record(&mut self, target: u8, rgb: Option<[u8; 3]>) -> Option<ColorSnapshot> {
        if self.completed || target >= 18 || self.replied & (1 << target) != 0 {
            return None;
        }
        self.replied |= 1 << target;
        match target {
            0..=15 => self.palette[target as usize] = rgb,
            16 => self.foreground = rgb,
            17 => self.background = rgb,
            _ => unreachable!(),
        }
        if self.replied == (1 << 18) - 1 {
            self.finish()
        } else {
            None
        }
    }

    /// Deliver the partial result once after 100 ms, retaining the batch for late completion.
    pub(crate) fn timeout(&mut self, now: Instant) -> Option<ColorSnapshot> {
        if self.completed
            || self.timed_out
            || now.saturating_duration_since(self.started) < Duration::from_millis(100)
        {
            return None;
        }
        self.timed_out = true;
        Some(self.snapshot())
    }

    /// Complete at DA1 (or on the final target) and deliver at most once.
    pub(crate) fn finish(&mut self) -> Option<ColorSnapshot> {
        if self.completed {
            return None;
        }
        self.completed = true;
        Some(self.snapshot())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partial_timeout_then_late_boundary_delivers_cumulative_snapshot() {
        let start = Instant::now();
        let mut batch = ColorBatch::new(start);
        assert_eq!(batch.record(17, Some([255, 255, 255])), None);
        assert_eq!(batch.timeout(start + Duration::from_millis(99)), None);
        let partial = batch.timeout(start + Duration::from_millis(100)).unwrap();
        assert_eq!(partial.background, Some([255, 255, 255]));
        assert_eq!(partial.foreground, None);
        assert_eq!(partial.palette, None);
        assert_eq!(batch.record(16, Some([0, 0, 0])), None);
        assert_eq!(batch.record(16, Some([1, 1, 1])), None);
        let late = batch.finish().unwrap();
        assert_eq!(late.background, partial.background);
        assert_eq!(late.foreground, Some([0, 0, 0]));
        assert!(batch.finish().is_none());
        assert!(batch.timeout(start + Duration::from_secs(1)).is_none());
    }

    #[test]
    fn invalid_target_completes_batch_without_replacing_existing_values() {
        for invalid_first in [true, false] {
            for after_timeout in [true, false] {
                let start = Instant::now();
                let mut batch = ColorBatch::new(start);
                if invalid_first {
                    assert!(batch.record(2, None).is_none());
                }
                for target in 0..18 {
                    if target == 2 {
                        continue;
                    }
                    let value = batch.record(target, Some([target, 1, 2]));
                    if after_timeout && target == 8 {
                        assert!(batch.timeout(start + Duration::from_millis(100)).is_some());
                    }
                    if invalid_first && target == 17 {
                        let snapshot = value.unwrap();
                        assert_eq!(snapshot.palette, None);
                        assert_eq!(snapshot.foreground, Some([16, 1, 2]));
                        assert_eq!(snapshot.background, Some([17, 1, 2]));
                    } else {
                        assert!(value.is_none());
                    }
                }
                if !invalid_first {
                    let snapshot = batch.record(2, None).unwrap();
                    assert_eq!(snapshot.palette, None);
                    assert_eq!(snapshot.foreground, Some([16, 1, 2]));
                    assert_eq!(snapshot.background, Some([17, 1, 2]));
                }
                assert!(batch.record(2, Some([2, 3, 4])).is_none());
                assert!(batch.finish().is_none());
            }
        }
    }

    #[test]
    fn palette_is_reported_only_when_complete_and_completion_is_once() {
        let start = Instant::now();
        let mut batch = ColorBatch::new(start);
        for index in 0..15 {
            assert_eq!(batch.record(index, Some([index, 1, 2])), None);
        }
        assert_eq!(
            batch
                .timeout(start + Duration::from_millis(100))
                .unwrap()
                .palette,
            None
        );
        assert_eq!(batch.record(15, Some([15, 1, 2])), None);
        assert_eq!(batch.record(16, Some([1, 2, 3])), None);
        let complete = batch.record(17, Some([4, 5, 6])).unwrap();
        assert_eq!(complete.palette.unwrap()[15], [15, 1, 2]);
        assert!(batch.finish().is_none());
        assert!(batch.record(17, Some([9, 9, 9])).is_none());
    }
}
