//! Host platform detection shared by the terminal and clipboard.

pub(crate) fn is_wsl() -> bool {
    cfg!(target_os = "linux")
        && detect_wsl(
            |name| std::env::var_os(name).is_some_and(|value| !value.is_empty()),
            || std::fs::read_to_string("/proc/version").ok(),
        )
}

fn detect_wsl(
    has_env: impl Fn(&str) -> bool,
    read_version: impl FnOnce() -> Option<String>,
) -> bool {
    ["WSL_DISTRO_NAME", "WSL_INTEROP", "WSLENV"]
        .into_iter()
        .any(has_env)
        || read_version().is_some_and(|version| {
            let version = version.to_ascii_lowercase();
            version.contains("microsoft") || version.contains("wsl")
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wsl_detection_uses_environment_or_kernel_release() {
        for marker in ["WSL_DISTRO_NAME", "WSL_INTEROP", "WSLENV"] {
            assert!(detect_wsl(
                |name| name == marker,
                || panic!("kernel fallback")
            ));
        }
        for release in ["Linux version 4.4.0-Microsoft", "Linux version 6.6.87-WSL2"] {
            assert!(detect_wsl(|_| false, || Some(release.into())));
        }
        assert!(!detect_wsl(|_| false, || Some("Linux version 6.12".into())));
        assert!(!detect_wsl(|_| false, || None));
    }
}
