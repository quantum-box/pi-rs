use crossterm::style::Color;
use pi_rs_workbench::theme_resources::ThemeReadGrants;
use serde_json::{Value, json};
use std::path::PathBuf;

fn theme(name: &str) -> Value {
    let mut value: Value =
        serde_json::from_str(include_str!("../assets/themes/dark.json")).unwrap();
    value["name"] = json!(name);
    value
}

fn grants(files: Vec<PathBuf>) -> ThemeReadGrants {
    ThemeReadGrants { files }
}

#[test]
fn snapshot_resolves_variables_in_order_and_is_independent() {
    let dir = tempfile::tempdir().unwrap();
    let a = dir.path().join("a.json");
    let b = dir.path().join("b.json");
    let mut first = theme("one");
    first["vars"]["hostColor"] = json!("#0af");
    first["colors"]["accent"] = json!("hostColor");
    std::fs::write(&a, format!("\u{feff}{first}")).unwrap();
    std::fs::write(&b, theme("two").to_string()).unwrap();
    let snapshot = grants(vec![a.clone(), b.clone()]).load().unwrap();
    assert_eq!(
        snapshot
            .themes()
            .iter()
            .map(|t| t.name())
            .collect::<Vec<_>>(),
        ["one", "two"]
    );
    assert_eq!(snapshot.themes()[0].path(), a);
    assert_eq!(
        snapshot.themes()[0].colors()["accent"],
        Color::Rgb {
            r: 0,
            g: 170,
            b: 255
        }
    );
    std::fs::remove_file(&a).unwrap();
    assert!(grants(vec![a, b]).load().is_err());
    assert_eq!(snapshot.themes()[0].name(), "one");
}

#[test]
fn only_granted_files_are_read() {
    let dir = tempfile::tempdir().unwrap();
    let selected = dir.path().join("selected.json");
    let ungranted = dir.path().join("broken.json");
    std::fs::write(&selected, theme("selected").to_string()).unwrap();
    std::fs::write(&ungranted, "not json").unwrap();
    let snapshot = grants(vec![selected]).load().unwrap();
    assert_eq!(snapshot.themes().len(), 1);
    assert_eq!(snapshot.themes()[0].name(), "selected");
}

#[test]
fn failures_reject_the_whole_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let a = dir.path().join("a.json");
    let b = dir.path().join("b.json");
    std::fs::write(&a, theme("duplicate").to_string()).unwrap();
    std::fs::write(&b, theme("duplicate").to_string()).unwrap();
    assert!(grants(vec![a.clone(), b.clone()]).load().is_err());
    std::fs::write(&b, "{").unwrap();
    assert!(grants(vec![a.clone(), b.clone()]).load().is_err());
    std::fs::write(&b, [0xff]).unwrap();
    assert!(grants(vec![a.clone(), b.clone()]).load().is_err());
    std::fs::write(&b, vec![b' '; 256 * 1024 + 1]).unwrap();
    assert!(grants(vec![a.clone(), b]).load().is_err());
    assert!(grants(vec![a; 65]).load().is_err());
    assert!(grants(vec![dir.path().to_owned()]).load().is_err());
    assert!(grants(vec![]).load().unwrap().themes().is_empty());
    for path in [
        PathBuf::from("relative.json"),
        dir.path().join("../theme.json"),
    ] {
        assert!(
            grants(vec![path])
                .load()
                .unwrap_err()
                .to_string()
                .contains("theme file path must be absolute without parent components")
        );
    }
}

#[test]
fn excessive_variable_chains_and_cycles_fail() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("theme.json");
    let mut value = theme("depth");
    for i in 0..130 {
        value["vars"][format!("v{i}")] = json!(format!("v{}", i + 1));
    }
    value["vars"]["v130"] = json!("#fff");
    value["colors"]["accent"] = json!("v0");
    std::fs::write(&path, value.to_string()).unwrap();
    assert!(grants(vec![path.clone()]).load().is_err());
    let mut value = theme("cycle");
    value["vars"]["a"] = json!("b");
    value["vars"]["b"] = json!("a");
    value["colors"]["accent"] = json!("a");
    std::fs::write(&path, value.to_string()).unwrap();
    assert!(grants(vec![path]).load().is_err());
}

#[test]
fn total_size_limit_applies_across_files() {
    let dir = tempfile::tempdir().unwrap();
    let mut paths = Vec::new();
    for index in 0..17 {
        let path = dir.path().join(format!("{index}.json"));
        let mut body = theme(&format!("theme-{index}")).to_string();
        body.push_str(&" ".repeat(250_000 - body.len()));
        std::fs::write(&path, body).unwrap();
        paths.push(path);
    }
    assert!(grants(paths[..16].to_vec()).load().is_ok());
    assert!(grants(paths).load().is_err());
}

#[cfg(unix)]
#[test]
fn grants_follow_symlinks_and_reject_fifos() {
    use std::os::unix::{ffi::OsStrExt, fs::symlink};
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("target.json");
    let link = dir.path().join("link.json");
    std::fs::write(&file, theme("linked").to_string()).unwrap();
    symlink(&file, &link).unwrap();
    assert_eq!(
        grants(vec![link.clone()]).load().unwrap().themes()[0].path(),
        link
    );
    let fifo = dir.path().join("fifo");
    let c_path = std::ffi::CString::new(fifo.as_os_str().as_bytes()).unwrap();
    assert_eq!(unsafe { libc::mkfifo(c_path.as_ptr(), 0o600) }, 0);
    assert!(grants(vec![fifo]).load().is_err());
}

#[cfg(windows)]
#[test]
fn windows_ambiguous_theme_paths_are_rejected() {
    for path in [r"C:theme.json", r"\theme.json"] {
        assert!(
            grants(vec![path.into()])
                .load()
                .unwrap_err()
                .to_string()
                .contains("theme file path must be absolute without parent components")
        );
    }
}
