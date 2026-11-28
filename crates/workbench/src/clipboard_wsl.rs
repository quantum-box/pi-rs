//! Bounded Windows clipboard interop for WSL. The bridge is invoked only on an
//! explicit clipboard action, after the corresponding native clipboard lookup.

#[cfg(target_os = "linux")]
use std::io::IsTerminal;
use std::io::Write;
use std::process::Stdio;
use std::time::Duration;

use base64::{Engine, engine::general_purpose::STANDARD};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;
use tokio::time::timeout;

const PATH_LIMIT: usize = 4096;
const TEXT_LIMIT: usize = 1024 * 1024;
const PNG_LIMIT: usize = 50 * 1024 * 1024;
const OSC52_LIMIT: usize = 100_000;
const PATH_TIMEOUT: Duration = Duration::from_secs(1);
const CLIPBOARD_TIMEOUT: Duration = Duration::from_secs(5);

const READ_IMAGE: &str = r#"
Add-Type -AssemblyName System.Windows.Forms
Add-Type -AssemblyName System.Drawing
$image = [System.Windows.Forms.Clipboard]::GetImage()
if ($null -eq $image) { exit 0 }
try {
    if ([long]$image.Width * $image.Height * 4 -gt 128MB) { exit 1 }
    $stream = [System.IO.MemoryStream]::new()
    try {
        $image.Save($stream, [System.Drawing.Imaging.ImageFormat]::Png)
        if ($stream.Length -gt 50MB) { exit 1 }
        $stream.WriteTo([Console]::OpenStandardOutput())
    } finally { $stream.Dispose() }
} finally { $image.Dispose() }
"#;

const READ_TEXT: &str = r#"
Add-Type -AssemblyName System.Windows.Forms
$text = [System.Windows.Forms.Clipboard]::GetText()
$utf8 = [System.Text.Encoding]::UTF8
if ($utf8.GetByteCount($text) -gt 1MB) { exit 1 }
$bytes = $utf8.GetBytes($text)
[Console]::OpenStandardOutput().Write($bytes, 0, $bytes.Length)
"#;

struct Bridge<'a> {
    powershell: &'a str,
    wslpath: &'a str,
}

#[cfg(target_os = "linux")]
const WINDOWS: Bridge<'static> = Bridge {
    powershell: "powershell.exe",
    wslpath: "wslpath",
};

impl Bridge<'_> {
    async fn powershell(&self, script: &str, output_limit: usize) -> Option<Vec<u8>> {
        let script = format!("$ErrorActionPreference = 'Stop'; {script}");
        run(
            self.powershell,
            &["-NoProfile", "-NonInteractive", "-Sta", "-Command", &script],
            output_limit,
            CLIPBOARD_TIMEOUT,
        )
        .await
    }

    async fn read_image(&self) -> Option<Vec<u8>> {
        self.powershell(READ_IMAGE, PNG_LIMIT)
            .await
            .filter(|bytes| !bytes.is_empty())
    }

    async fn read_text(&self) -> Option<String> {
        let bytes = self.powershell(READ_TEXT, TEXT_LIMIT).await?;
        String::from_utf8(bytes)
            .ok()
            .filter(|text| !text.is_empty())
    }

    async fn write_text(&self, text: &str) -> bool {
        self.write_text_file(text).await.is_some()
    }

    async fn write_text_file(&self, text: &str) -> Option<()> {
        if text.len() > TEXT_LIMIT {
            return None;
        }
        // Keep the path short for interop. The file has private permissions and
        // is removed on failure, success, timeout or future cancellation.
        let mut file = tempfile::Builder::new()
            .prefix("pi-wsl-clip-")
            .suffix(".txt")
            .tempfile_in("/tmp")
            .ok()?;
        file.write_all(text.as_bytes()).ok()?;

        let path = run(
            self.wslpath,
            &["-w", file.path().to_str()?],
            PATH_LIMIT,
            PATH_TIMEOUT,
        )
        .await?;
        let path = String::from_utf8(path).ok()?;
        let path = path.strip_suffix('\n').unwrap_or(&path);
        let path = path.strip_suffix('\r').unwrap_or(path);
        if path.is_empty() || path.chars().any(char::is_control) {
            return None;
        }
        // Pass the translated path as base64 so it cannot become script syntax.
        // The file avoids the console code page that corrupts piped UTF-8.
        let path = STANDARD.encode(path);
        let script = format!(
            "$path = [System.Text.Encoding]::UTF8.GetString([Convert]::FromBase64String('{path}')); \
             Set-Clipboard -Value ([System.IO.File]::ReadAllText($path, [System.Text.Encoding]::UTF8))"
        );
        self.powershell(&script, 0).await?;
        Some(())
    }
}

fn command(program: &str, arguments: &[&str]) -> Command {
    let mut command = Command::new(program);
    // A deeply nested WSL cwd can exceed the Windows interop path limit.
    command
        .args(arguments)
        .current_dir("/")
        .process_group(0)
        .stderr(Stdio::null())
        .kill_on_drop(true);
    command
}

async fn run(
    program: &str,
    arguments: &[&str],
    output_limit: usize,
    deadline: Duration,
) -> Option<Vec<u8>> {
    timeout(deadline, async {
        let mut child = command(program, arguments)
            .stdin(Stdio::null())
            .stdout(if output_limit == 0 {
                Stdio::null()
            } else {
                Stdio::piped()
            })
            .spawn()
            .ok()?;
        let mut output = Vec::new();
        if let Some(stdout) = child.stdout.take() {
            stdout
                .take(output_limit as u64 + 1)
                .read_to_end(&mut output)
                .await
                .ok()?;
            if output.len() > output_limit {
                return None;
            }
        }
        child.wait().await.ok()?.success().then_some(output)
    })
    .await
    .ok()
    .flatten()
}

#[cfg(target_os = "linux")]
fn runtime() -> std::io::Result<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
}

#[cfg(target_os = "linux")]
pub(super) fn read_image() -> Option<Vec<u8>> {
    runtime().ok()?.block_on(WINDOWS.read_image())
}

#[cfg(target_os = "linux")]
pub(super) fn read_text() -> Option<String> {
    runtime().ok()?.block_on(WINDOWS.read_text())
}

#[cfg(target_os = "linux")]
pub(super) fn copy_text_sync(text: &str, candidates: &[(&str, &[&str])]) -> pi_rs::Result<()> {
    // Sync callers can already be inside the main Tokio runtime.
    std::thread::scope(|scope| {
        scope
            .spawn(|| runtime()?.block_on(copy_text(text, candidates)))
            .join()
            .map_err(|_| pi_rs::Error::Config("clipboard worker stopped".into()))?
    })
}

#[cfg(target_os = "linux")]
pub(super) async fn copy_text(text: &str, candidates: &[(&str, &[&str])]) -> pi_rs::Result<()> {
    let has_env = |name| std::env::var_os(name).is_some_and(|value| !value.is_empty());
    let remote = ["SSH_CONNECTION", "SSH_CLIENT", "MOSH_CONNECTION"]
        .into_iter()
        .any(has_env)
        || std::env::var_os("PI_CLIPBOARD_OSC52").is_some_and(|value| value == "1");
    if copy_native(text, candidates).await {
        if remote {
            emit_osc52(text);
        }
        return Ok(());
    }

    let windows_terminal = has_env("WT_SESSION");
    let headless = !["DISPLAY", "WAYLAND_DISPLAY", "TERMUX_VERSION"]
        .into_iter()
        .any(has_env);
    if copy_fallback(
        &WINDOWS,
        text,
        windows_terminal,
        headless,
        remote,
        emit_osc52,
    )
    .await
    {
        Ok(())
    } else {
        Err(pi_rs::Error::Config(
            "no working WSL clipboard fallback found".into(),
        ))
    }
}

async fn copy_native(text: &str, candidates: &[(&str, &[&str])]) -> bool {
    for (program, arguments) in candidates {
        if write_native(program, arguments, text).await {
            return true;
        }
    }
    false
}

async fn write_native(program: &str, arguments: &[&str], text: &str) -> bool {
    timeout(CLIPBOARD_TIMEOUT, async {
        let mut child = command(program, arguments)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .spawn()?;
        let mut stdin = child.stdin.take().expect("piped clipboard");
        let written = stdin.write_all(text.as_bytes()).await;
        drop(stdin);
        let status = child.wait().await?;
        written?;
        Ok::<_, std::io::Error>(status.success())
    })
    .await
    .is_ok_and(|result| result.unwrap_or(false))
}

async fn copy_fallback(
    bridge: &Bridge<'_>,
    text: &str,
    windows_terminal: bool,
    headless: bool,
    remote: bool,
    mut osc52: impl FnMut(&str) -> bool,
) -> bool {
    // A Windows Terminal write is cheaper than the Windows process round trip.
    if windows_terminal && osc52(text) {
        return true;
    }
    let copied = bridge.write_text(text).await;
    let emitted = (remote || (!windows_terminal && !copied && headless)) && osc52(text);
    copied || emitted
}

fn osc52_sequence(text: &str) -> Option<String> {
    if text.len() > OSC52_LIMIT / 4 * 3 {
        return None;
    }
    Some(format!("\x1b]52;c;{}\x07", STANDARD.encode(text)))
}

#[cfg(target_os = "linux")]
fn emit_osc52(text: &str) -> bool {
    let stdout = std::io::stdout();
    if !stdout.is_terminal() {
        return false;
    }
    osc52_sequence(text).is_some_and(|sequence| {
        let mut stdout = stdout.lock();
        stdout.write_all(sequence.as_bytes()).is_ok() && stdout.flush().is_ok()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    use std::path::Path;

    fn fake_command(dir: &Path, name: &str, script: &str) -> String {
        let path = dir.join(name);
        std::fs::write(&path, format!("#!/bin/sh\nset -eu\n{script}")).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
        path.into_os_string().into_string().unwrap()
    }

    fn shell_path(path: &Path) -> String {
        shell_words::quote(path.to_str().unwrap()).into_owned()
    }

    #[tokio::test]
    async fn windows_image_and_text_use_binary_stdout() {
        let dir = tempfile::tempdir().unwrap();
        let png = dir.path().join("image.png");
        image::RgbaImage::from_pixel(2, 1, image::Rgba([6, 20, 120, 90]))
            .save(&png)
            .unwrap();
        let text = dir.path().join("text");
        let clipboard_text = "日本語 ’🎉\r\nkept whitespace \n";
        std::fs::write(&text, clipboard_text).unwrap();
        let ps = fake_command(
            dir.path(),
            "powershell.exe",
            &format!(
                r#"test "$PWD" = /
test "$1 $2 $3 $4" = "-NoProfile -NonInteractive -Sta -Command"
case "$5" in
    *'::GetImage()'*) cat {} ;;
    *'::GetText()'*) cat {} ;;
    *) exit 1 ;;
esac"#,
                shell_path(&png),
                shell_path(&text)
            ),
        );
        let bridge = Bridge {
            powershell: &ps,
            wslpath: "unused",
        };
        let bytes = bridge.read_image().await.unwrap();
        let profile = pi_rs_workbench::image_profile::ImageResizeProfile {
            max_width: Some(1),
            ..Default::default()
        };
        let resized = crate::clipboard::prepare_wsl_png(&bytes, true, Some(profile)).unwrap();
        let resized = STANDARD.decode(resized.data).unwrap();
        assert_eq!(image::load_from_memory(&resized).unwrap().width(), 1);
        let content = crate::clipboard::prepare_wsl_png(&bytes, false, Some(profile)).unwrap();
        assert_eq!(content.mime_type, "image/png");
        let bytes = STANDARD.decode(content.data).unwrap();
        let decoded = image::load_from_memory(&bytes).unwrap().to_rgba8();
        assert_eq!(decoded.dimensions(), (2, 1));
        assert_eq!(decoded.get_pixel(0, 0).0, [6, 20, 120, 90]);
        assert_eq!(bridge.read_text().await.as_deref(), Some(clipboard_text));

        std::fs::write(&png, b"not a PNG").unwrap();
        let bytes = bridge.read_image().await.unwrap();
        assert!(crate::clipboard::prepare_wsl_png(&bytes, false, None).is_err());
        std::fs::write(&png, []).unwrap();
        assert!(bridge.read_image().await.is_none());
        std::fs::write(&text, [0xff]).unwrap();
        assert!(bridge.read_text().await.is_none());
    }

    #[tokio::test]
    async fn unicode_copy_uses_private_file_and_removes_it() {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("source");
        let copied = dir.path().join("copied");
        let windows_path = r"C:\users\pi's ’clipboard.txt";
        let wslpath = fake_command(
            dir.path(),
            "wslpath",
            &format!(
                "test \"$1\" = -w\nprintf '%s' \"$2\" > {}\nprintf '%s\\r\\n' {}",
                shell_path(&source),
                shell_words::quote(windows_path),
            ),
        );
        let ps_script = format!(
            r#"case "$5" in *'{}'*'ReadAllText'*'UTF8'*) ;; *) exit 1 ;; esac
cat "$(cat {})" > {}"#,
            STANDARD.encode(windows_path),
            shell_path(&source),
            shell_path(&copied),
        );
        let ps = fake_command(dir.path(), "powershell.exe", &ps_script);
        let bridge = Bridge {
            powershell: &ps,
            wslpath: &wslpath,
        };
        let text = "日本語 🎉\n'quoted' \"text\"\n";
        assert!(bridge.write_text(text).await);
        assert_eq!(std::fs::read_to_string(copied).unwrap(), text);
        let source_path = std::fs::read_to_string(&source).unwrap();
        assert!(!Path::new(&source_path).exists());
        std::fs::remove_file(&source).unwrap();
        assert!(!bridge.write_text(&"x".repeat(TEXT_LIMIT + 1)).await);
        assert!(!source.exists(), "oversized input must skip interop");

        fake_command(dir.path(), "powershell.exe", "exit 1");
        assert!(!bridge.write_text(text).await);
        assert!(!Path::new(&std::fs::read_to_string(source).unwrap()).exists());
    }

    #[tokio::test]
    async fn cancelled_copy_removes_the_private_input_file() {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("source");
        let ready = dir.path().join("ready");
        let wslpath = fake_command(
            dir.path(),
            "wslpath",
            &format!(
                "printf '%s' \"$2\" > {}
printf 'fixture\\n'",
                shell_path(&source)
            ),
        );
        let ps = fake_command(
            dir.path(),
            "powershell.exe",
            &format!(
                ": > {}
exec sleep 30",
                shell_path(&ready)
            ),
        );
        let bridge = Bridge {
            powershell: &ps,
            wslpath: &wslpath,
        };
        let mut copy = Box::pin(bridge.write_text("private"));
        tokio::select! {
            _ = &mut copy => panic!("copy ended before cancellation"),
            result = timeout(Duration::from_secs(3), async {
                while !ready.exists() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }) => result.unwrap(),
        }
        let path = std::fs::read_to_string(source).unwrap();
        let permissions = std::fs::metadata(&path).unwrap().permissions();
        assert_eq!(permissions.mode() & 0o777, 0o600);
        drop(copy);
        assert!(!Path::new(&path).exists());
    }

    #[tokio::test]
    async fn native_copy_stops_after_first_success() {
        let dir = tempfile::tempdir().unwrap();
        let fail = fake_command(dir.path(), "failed-writer", "exit 1");
        let copied = dir.path().join("copied");
        let writer = fake_command(
            dir.path(),
            "writer",
            &format!("cat > {}", shell_path(&copied)),
        );
        let text = "日本語 🎉
";
        assert!(copy_native(text, &[(&fail, &[]), (&writer, &[]), (&fail, &[])]).await);
        assert_eq!(std::fs::read_to_string(copied).unwrap(), text);
    }

    #[tokio::test]
    async fn command_checks_exit_status_output_size_and_deadline() {
        let deadline = Duration::from_millis(250);
        assert_eq!(
            run("/bin/sh", &["-c", "printf 1234"], 4, deadline).await,
            Some(b"1234".to_vec())
        );
        assert!(
            run("/bin/sh", &["-c", "printf 12345"], 4, deadline)
                .await
                .is_none()
        );
        assert!(
            run("/bin/sh", &["-c", "printf 1234; exit 1"], 4, deadline)
                .await
                .is_none()
        );
        for script in ["exec sleep 5", "exec 1>&-; exec sleep 5"] {
            let start = std::time::Instant::now();
            assert!(run("/bin/sh", &["-c", script], 4, deadline).await.is_none());
            assert!(start.elapsed() < Duration::from_secs(4));
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn wsl_entrypoints_with_fake_interop() {
        let dir = tempfile::tempdir().unwrap();
        let png = dir.path().join("clipboard.png");
        image::RgbaImage::from_pixel(1, 1, image::Rgba([0, 0, 0, 80]))
            .save(&png)
            .unwrap();
        fake_command(
            dir.path(),
            "powershell.exe",
            r#"case "$5" in
    *'::GetImage()'*) if [ -f "$FIXTURE_PNG" ]; then /bin/cat "$FIXTURE_PNG"; fi ;;
    *'::GetText()'*) printf '%s' '日本語 🎉' ;;
    *'Set-Clipboard'*) /bin/cat "$FIXTURE_PNG.source" > "$FIXTURE_PNG.copied" ;;
    *) exit 1 ;;
esac"#,
        );
        fake_command(
            dir.path(),
            "wslpath",
            r#"/bin/cp "$2" "$FIXTURE_PNG.source"
printf '%s\n' 'C:\test\clip.txt'"#,
        );
        let result = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "clipboard_wsl::tests::wsl_entrypoint_in_child"])
            .env_clear()
            .env("PATH", dir.path())
            .env("FIXTURE_PNG", png)
            .env("WSLENV", "fixture")
            .output()
            .unwrap();
        assert!(
            result.status.success(),
            "{}
{}",
            String::from_utf8_lossy(&result.stdout),
            String::from_utf8_lossy(&result.stderr)
        );
        assert!(String::from_utf8_lossy(&result.stdout).contains("1 passed"));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn wsl_entrypoint_in_child() {
        let Some(png) = std::env::var_os("FIXTURE_PNG") else {
            return;
        };
        use crate::clipboard::{Content, read};
        let image = read(false, None).unwrap();
        assert!(matches!(image, Content::Image(image) if image.mime_type == "image/png"));
        std::fs::remove_file(&png).unwrap();
        let text = "日本語 🎉";
        assert!(matches!(read(false, None).unwrap(), Content::Text(value) if value == text));

        // Exercise the sync entry point from an existing Tokio runtime as the TUI does,
        // then the async entry point used by device-code copy.
        let copied = std::path::Path::new(&png).with_extension("png.copied");
        runtime().unwrap().block_on(async {
            crate::sessions::copy_text(text).unwrap();
            assert_eq!(std::fs::read_to_string(&copied).unwrap(), text);
            crate::sessions::copy_text_async("another 🖼").await.unwrap();
            assert_eq!(std::fs::read_to_string(&copied).unwrap(), "another 🖼");
        });
    }

    #[tokio::test]
    async fn terminal_copy_prefers_osc52_and_headless_can_retry_with_osc52() {
        let missing = Bridge {
            powershell: "/no-such-pi-clipboard-command",
            wslpath: "/no-such-pi-clipboard-command",
        };
        let mut calls = Vec::new();
        for (windows_terminal, headless, remote, expected) in [
            (true, true, false, true),
            (false, true, false, true),
            (false, false, true, true),
            (false, false, false, false),
        ] {
            calls.clear();
            assert_eq!(
                copy_fallback(
                    &missing,
                    "🎉",
                    windows_terminal,
                    headless,
                    remote,
                    |text| {
                        calls.push(osc52_sequence(text).unwrap());
                        true
                    }
                )
                .await,
                expected
            );
            assert_eq!(calls.len(), usize::from(expected));
            if expected {
                assert_eq!(calls[0], "\x1b]52;c;8J+OiQ==\x07");
            }
        }
        assert!(osc52_sequence(&"x".repeat(75_000)).is_some());
        assert!(osc52_sequence(&"x".repeat(75_001)).is_none());
    }
}
