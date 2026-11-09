//! Bounded shell output with a recoverable full transcript.
use std::io::Write;
use std::path::PathBuf;

const DISPLAY_BYTES: usize = 50 * 1024;
const DISPLAY_LINES: usize = 2000;
const SCRIPT_BYTES: usize = 1024 * 1024;

#[derive(Default)]
pub(super) struct Accumulator {
    head: Vec<u8>,
    tail: Vec<u8>,
    bytes: u64,
    newlines: u64,
    last_byte: Option<u8>,
    file: Option<tempfile::NamedTempFile>,
}

pub(super) struct Output {
    pub display: String,
    pub script: String,
    pub script_truncated: bool,
    pub path: Option<PathBuf>,
}

impl Accumulator {
    pub fn append(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        if bytes.is_empty() {
            return Ok(());
        }
        self.bytes += bytes.len() as u64;
        self.newlines += bytes.iter().filter(|byte| **byte == b'\n').count() as u64;
        self.last_byte = bytes.last().copied();
        if self.file.is_none()
            && (self.bytes > DISPLAY_BYTES as u64 || self.lines() > DISPLAY_LINES as u64)
        {
            let mut file = tempfile::Builder::new()
                .prefix("pi-bash-")
                .suffix(".log")
                .tempfile()?;
            file.write_all(&self.head)?;
            self.file = Some(file);
        }
        if let Some(file) = &mut self.file {
            file.write_all(bytes)?;
        }
        let head_remaining = SCRIPT_BYTES.saturating_sub(self.head.len());
        self.head
            .extend_from_slice(&bytes[..bytes.len().min(head_remaining)]);
        self.tail.extend_from_slice(bytes);
        if self.tail.len() > SCRIPT_BYTES {
            self.tail.drain(..self.tail.len() - SCRIPT_BYTES);
        }
        Ok(())
    }

    fn lines(&self) -> u64 {
        self.newlines + u64::from(self.last_byte.is_some_and(|byte| byte != b'\n'))
    }

    pub fn finish(mut self) -> std::io::Result<Output> {
        let display = if self.file.is_none() {
            String::from_utf8_lossy(&self.head).into_owned()
        } else {
            let tail = decode_tail(&self.tail);
            let mut selected = Vec::new();
            let mut size = 0;
            for line in tail.split_terminator('\n').rev().take(DISPLAY_LINES) {
                let next = line.len() + usize::from(!selected.is_empty());
                if size + next > DISPLAY_BYTES {
                    if selected.is_empty() {
                        let mut start = line.len().saturating_sub(DISPLAY_BYTES);
                        while !line.is_char_boundary(start) {
                            start += 1;
                        }
                        selected.push(&line[start..]);
                    }
                    break;
                }
                selected.push(line);
                size += next;
            }
            selected.reverse();
            selected.join("\n")
        };
        let script_truncated = self.bytes > SCRIPT_BYTES as u64;
        let script = if script_truncated {
            let half = SCRIPT_BYTES / 2;
            let head = decode_head(&self.head[..half]);
            let tail = decode_tail(&self.tail[self.tail.len() - half..]);
            format!(
                "{head}\n\n[... {} bytes omitted ...]\n\n{tail}",
                self.bytes - SCRIPT_BYTES as u64
            )
        } else {
            String::from_utf8_lossy(&self.head).into_owned()
        };
        let path = if let Some(mut file) = self.file.take() {
            file.flush()?;
            let (_, path) = file.keep().map_err(|error| error.error)?;
            Some(path)
        } else {
            None
        };
        let display = match &path {
            Some(path) => {
                let shown = display.split_terminator('\n').count();
                let start = self.lines().saturating_sub(shown as u64) + 1;
                format!(
                    "{display}\n\n[Showing lines {start}-{} of {} (2000 line / 50 KiB limit). Full output: {}]",
                    self.lines(),
                    self.lines(),
                    path.display()
                )
            }
            None => display,
        };
        Ok(Output {
            display,
            script,
            script_truncated,
            path,
        })
    }
}

fn decode_tail(bytes: &[u8]) -> String {
    let start = bytes
        .iter()
        .position(|byte| byte & 0xc0 != 0x80)
        .unwrap_or(bytes.len());
    String::from_utf8_lossy(&bytes[start..]).into_owned()
}
fn decode_head(bytes: &[u8]) -> String {
    match std::str::from_utf8(bytes) {
        Ok(text) => text.to_owned(),
        Err(error) if error.error_len().is_none() => {
            String::from_utf8_lossy(&bytes[..error.valid_up_to()]).into_owned()
        }
        Err(_) => String::from_utf8_lossy(bytes).into_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn small_output_retains_bytes_without_a_file() {
        let mut output = Accumulator::default();
        output.append("日本語\n".as_bytes()).unwrap();
        let output = output.finish().unwrap();
        assert_eq!(output.display, "日本語\n");
        assert_eq!(output.script, output.display);
        assert!(!output.script_truncated);
        assert!(output.path.is_none());
    }
    #[test]
    fn line_limit_keeps_tail_and_publishes_complete_file() {
        let input = (1..=2100)
            .map(|line| format!("{line}\n"))
            .collect::<String>();
        let mut output = Accumulator::default();
        for chunk in input.as_bytes().chunks(97) {
            output.append(chunk).unwrap();
        }
        let output = output.finish().unwrap();
        assert!(output.display.starts_with("101\n102\n"));
        assert!(output.display.contains("lines 101-2100 of 2100"));
        assert_eq!(output.script, input);
        assert!(!output.script_truncated);
        let path = output.path.unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), input);
        std::fs::remove_file(path).unwrap();
    }
    #[test]
    fn large_multibyte_output_has_bounded_views_and_complete_file() {
        let input = format!("head\n{}\ntail\n", "界".repeat(SCRIPT_BYTES));
        let mut output = Accumulator::default();
        for chunk in input.as_bytes().chunks(8192) {
            output.append(chunk).unwrap();
        }
        let output = output.finish().unwrap();
        assert!(output.script.starts_with("head\n"));
        assert!(output.script.ends_with("\ntail\n"));
        assert!(!output.script.contains('\u{fffd}'));
        assert!(output.script.len() < SCRIPT_BYTES + 100);
        assert!(output.script_truncated);
        assert!(output.display.starts_with("tail"));
        let path = output.path.unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), input);
        std::fs::remove_file(path).unwrap();
    }
}
