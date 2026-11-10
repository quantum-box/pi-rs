//! Navigation state for the fullscreen transcript.
use std::ops::Range;

#[derive(Default)]
pub struct Viewport {
    // None follows the end when new transcript lines arrive.
    top: Option<usize>,
    max: usize,
    height: usize,
    prompts: Vec<usize>,
}

impl Viewport {
    pub fn update(&mut self, length: usize, height: usize, prompts: Vec<usize>) {
        self.max = length.saturating_sub(height);
        self.height = height;
        self.prompts = prompts;
        if let Some(top) = &mut self.top {
            *top = (*top).min(self.max);
        }
    }

    pub fn top(&self) -> usize {
        self.top.unwrap_or(self.max)
    }

    pub fn reveal(&mut self, rows: Range<usize>) {
        let top = self.top();
        let target = if rows.start < top || rows.end > top.saturating_add(self.height) {
            rows.start.saturating_sub(self.height / 3)
        } else {
            top
        };
        self.top = Some(target.min(self.max));
    }

    pub fn follow(&mut self) {
        self.top = None;
    }

    pub fn navigate(&mut self, action: &str) -> bool {
        let top = self.top();
        let page = self.height.saturating_sub(4).max(1);
        let half = (self.height / 2).max(1);
        let target = match action {
            "tui.altScreen.pageUp" => top.saturating_sub(page),
            "tui.altScreen.pageDown" => top.saturating_add(page),
            "tui.altScreen.halfPageUp" => top.saturating_sub(half),
            "tui.altScreen.halfPageDown" => top.saturating_add(half),
            "tui.altScreen.lineUp" => top.saturating_sub(1),
            "tui.altScreen.lineDown" => top.saturating_add(1),
            "tui.altScreen.previousPrompt" => {
                let index = self.prompts.partition_point(|row| *row < top);
                index.checked_sub(1).map_or(top, |i| self.prompts[i])
            }
            "tui.altScreen.nextPrompt" => {
                let index = self.prompts.partition_point(|row| *row <= top);
                self.prompts.get(index).copied().unwrap_or(top)
            }
            "tui.altScreen.top" => 0,
            "tui.altScreen.bottom" => self.max,
            _ => return false,
        };
        if target >= self.max && action != "tui.altScreen.top" {
            self.follow();
        } else {
            self.top = Some(target);
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pages_recover_from_top_and_semantic_jumps_use_prompt_starts() {
        let mut viewport = Viewport::default();
        viewport.update(100, 20, vec![5, 25, 65, 95]);
        assert_eq!(viewport.top(), 80);
        viewport.navigate("tui.altScreen.previousPrompt");
        assert_eq!(viewport.top(), 65);
        viewport.navigate("tui.altScreen.previousPrompt");
        assert_eq!(viewport.top(), 25);
        viewport.navigate("tui.altScreen.nextPrompt");
        assert_eq!(viewport.top(), 65);
        viewport.navigate("tui.altScreen.top");
        viewport.navigate("tui.altScreen.pageDown");
        assert_eq!(viewport.top(), 16);
        viewport.reveal(60..62);
        assert_eq!(viewport.top(), 54);
        viewport.update(101, 20, vec![]);
        assert_eq!(viewport.top(), 54);
        viewport.follow();
        assert_eq!(viewport.top(), 81);
    }
}
