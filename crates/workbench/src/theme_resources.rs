//! Host-granted snapshots of file-based Pi themes.
use std::collections::{BTreeMap, HashSet};
use std::path::{Component, Path, PathBuf};

use crossterm::style::Color;
use pi_rs::{Error, Result, UiThemeColor, UiThemeInfo, UiThemePalette};

const MAX_FILES: usize = 64;
const MAX_DIRECTORY_ENTRIES: usize = 4096;
const MAX_TOTAL_BYTES: usize = 4 * 1024 * 1024;

/// Exact absolute files the host authorizes this load to read. Paths cannot
/// contain parent components. Symlinks are followed;
/// a grant also authorizes the target and is not a filesystem sandbox.
#[derive(Clone, Debug, Default)]
pub struct ThemeReadGrants {
    pub files: Vec<PathBuf>,
}

/// An ordered, independent snapshot. Loading does not select or apply a theme.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ThemeSnapshot {
    themes: Vec<ThemeResource>,
}

/// A resolved file theme. Colors are terminal-independent RGB, ANSI index, or reset values.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ThemeResource {
    name: String,
    path: PathBuf,
    colors: BTreeMap<String, Color>,
}

impl ThemeSnapshot {
    pub fn themes(&self) -> &[ThemeResource] {
        &self.themes
    }
}

impl ThemeResource {
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn path(&self) -> &std::path::Path {
        &self.path
    }
    pub fn colors(&self) -> &BTreeMap<String, Color> {
        &self.colors
    }
}

impl ThemeReadGrants {
    /// Read exact file grants first, then direct `.json` files in each granted
    /// directory. Directory order follows the caller's list and entries are
    /// sorted by path. Hidden files and symlinks to regular files are included.
    /// Directory grants must be absolute without parent components and include
    /// symlink targets. Reads are synchronous and any failure rejects the snapshot.
    pub fn load_with_directories(&self, directories: &[PathBuf]) -> Result<ThemeSnapshot> {
        if directories.is_empty() {
            return self.load();
        }
        if directories.len() > MAX_FILES || self.files.len() > MAX_FILES {
            return Err(Error::Config("too many theme paths".into()));
        }
        for directory in directories {
            validate_path(directory, "directory")?;
        }
        for file in &self.files {
            validate_path(file, "file")?;
        }

        let mut files = self.files.clone();
        let mut examined = 0;
        for directory in directories {
            let mut entries = Vec::new();
            for entry in std::fs::read_dir(directory)? {
                examined += 1;
                if examined > MAX_DIRECTORY_ENTRIES {
                    return Err(Error::Config("too many theme directory entries".into()));
                }
                let entry = entry?;
                if !entry.file_name().as_encoded_bytes().ends_with(b".json") {
                    continue;
                }
                let kind = entry.file_type()?;
                let path = entry.path();
                let is_file = if kind.is_symlink() {
                    match std::fs::metadata(&path) {
                        Ok(metadata) => metadata.is_file(),
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
                        Err(error) => return Err(error.into()),
                    }
                } else {
                    kind.is_file()
                };
                if is_file {
                    if files.len() + entries.len() >= MAX_FILES {
                        return Err(Error::Config("too many theme files".into()));
                    }
                    entries.push(path);
                }
            }
            entries.sort();
            files.extend(entries);
        }
        Self { files }.load()
    }

    /// Read only these files, in order. Any failure rejects the entire snapshot.
    /// Reads are synchronous and should run off a UI thread.
    pub fn load(&self) -> Result<ThemeSnapshot> {
        if self.files.len() > MAX_FILES {
            return Err(Error::Config("too many theme files".into()));
        }
        let mut total = 0usize;
        let mut names = HashSet::new();
        let mut themes = Vec::with_capacity(self.files.len());
        for path in &self.files {
            validate_path(path, "file")?;
            let text = read_theme_file(path)?.ok_or_else(|| {
                Error::Config(format!("theme file is missing: {}", path.display()))
            })?;
            total = total
                .checked_add(text.len())
                .ok_or_else(|| Error::Config("theme files exceed size limit".into()))?;
            if total > MAX_TOTAL_BYTES {
                return Err(Error::Config("theme files exceed size limit".into()));
            }
            let parsed = crate::theme_parser::parse_theme(&text)?;
            if !names.insert(parsed.name.clone()) {
                return Err(Error::Config(format!(
                    "duplicate theme name: {}",
                    parsed.name
                )));
            }
            themes.push(ThemeResource {
                name: parsed.name,
                path: path.clone(),
                colors: parsed.colors,
            });
        }
        Ok(ThemeSnapshot { themes })
    }
}

/// Read one bounded, regular theme file using the native loader policy.
#[doc(hidden)]
pub fn read_theme_file(path: &Path) -> Result<Option<String>> {
    let text = crate::prompt_resources::read_optional_resource(path, "theme file")?;
    if text
        .as_deref()
        .is_some_and(|text| text.starts_with('\u{feff}'))
    {
        return Err(Error::Config(
            "theme file contains more than one UTF-8 BOM".into(),
        ));
    }
    Ok(text)
}

/// The worker's catalog and fixed palettes share one ordered resource snapshot.
#[doc(hidden)]
#[derive(Default)]
pub struct ExtensionThemeSnapshot {
    catalog: Vec<UiThemeInfo>,
    palettes: BTreeMap<String, UiThemePalette>,
}

impl ExtensionThemeSnapshot {
    pub fn catalog(&self) -> &[UiThemeInfo] {
        &self.catalog
    }

    pub fn palette(&self, name: &str) -> Option<&UiThemePalette> {
        self.palettes.get(name)
    }
}

/// Resolve the worker's already resolved theme files. The generated system
/// palette is supplied by the terminal, which owns the live terminal colors.
#[doc(hidden)]
pub fn extension_theme_snapshot(paths: &[PathBuf]) -> Result<ExtensionThemeSnapshot> {
    if paths.len() > MAX_DIRECTORY_ENTRIES {
        return Err(Error::Config("too many resolved theme files".into()));
    }
    let mut bytes = 0usize;
    let mut palettes = BTreeMap::new();
    let mut catalog = Vec::new();
    for source in [
        include_str!("../assets/themes/dark.json"),
        include_str!("../assets/themes/light.json"),
    ] {
        let theme = crate::theme_parser::parse_theme(source)?;
        let palette = ui_theme_palette(theme.name, None, &theme.colors, Default::default());
        catalog.push(UiThemeInfo {
            name: palette.name.clone(),
            path: None,
        });
        palettes.insert(palette.name.clone(), palette);
    }
    for path in paths {
        let Ok(Some(text)) = read_theme_file(path) else {
            continue;
        };
        bytes += text.len();
        if bytes > MAX_TOTAL_BYTES {
            return Err(Error::Config(
                "theme catalog exceeds its read size limit".into(),
            ));
        }
        let Ok(theme) = crate::theme_parser::parse_theme(&text) else {
            continue;
        };
        if let std::collections::btree_map::Entry::Vacant(entry) =
            palettes.entry(theme.name.clone())
        {
            let path = path.to_str().map(PathBuf::from);
            catalog.push(UiThemeInfo {
                name: theme.name.clone(),
                path: path.clone(),
            });
            entry.insert(ui_theme_palette(
                theme.name,
                path,
                &theme.colors,
                Default::default(),
            ));
        }
    }
    let locale = crate::locale::select(|name| std::env::var(name).ok());
    let collator = crate::locale::collator(&locale).ok();
    catalog.sort_by(|left, right| {
        collator.as_ref().map_or_else(
            || left.name.cmp(&right.name),
            |collator| collator.compare(&left.name, &right.name),
        )
    });
    catalog.insert(
        0,
        UiThemeInfo {
            name: "system".into(),
            path: None,
        },
    );
    Ok(ExtensionThemeSnapshot { catalog, palettes })
}

#[doc(hidden)]
pub fn ui_theme_palette(
    name: String,
    path: Option<PathBuf>,
    colors: &BTreeMap<String, Color>,
    dim: std::collections::BTreeSet<String>,
) -> UiThemePalette {
    UiThemePalette {
        name,
        path,
        colors: colors
            .iter()
            .map(|(role, color)| (role.clone(), ui_theme_color(*color)))
            .collect(),
        dim,
    }
}

fn ui_theme_color(color: Color) -> UiThemeColor {
    let index = match color {
        Color::Rgb { r, g, b } => return UiThemeColor::Rgb { r, g, b },
        Color::Reset => return UiThemeColor::Reset,
        Color::AnsiValue(index) => index,
        Color::Black => 0,
        Color::DarkRed => 1,
        Color::DarkGreen => 2,
        Color::DarkYellow => 3,
        Color::DarkBlue => 4,
        Color::DarkMagenta => 5,
        Color::DarkCyan => 6,
        Color::Grey => 7,
        Color::DarkGrey => 8,
        Color::Red => 9,
        Color::Green => 10,
        Color::Yellow => 11,
        Color::Blue => 12,
        Color::Magenta => 13,
        Color::Cyan => 14,
        Color::White => 15,
    };
    UiThemeColor::Indexed { index }
}

fn validate_path(path: &Path, kind: &str) -> Result<()> {
    if !path.is_absolute() || path.components().any(|part| part == Component::ParentDir) {
        return Err(Error::Config(format!(
            "theme {kind} path must be absolute without parent components"
        )));
    }
    Ok(())
}
