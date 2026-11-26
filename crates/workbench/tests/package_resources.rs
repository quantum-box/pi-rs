use std::fs;
use std::path::{Path, PathBuf};

use pi_rs_workbench::package_resources::{
    PackageResourcePaths, ResourcePathOrigin, ResourcePathSelection,
};
use pi_rs_workbench::resource_factory::{ResourceFactory, ResourceReadGrants};
use serde_json::json;

fn write(root: &Path, path: &str, content: &str) -> PathBuf {
    let path = root.join(path);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(&path, content).unwrap();
    path
}

const THEME: &str = include_str!("../assets/themes/dark.json");

fn package_selection(path: &Path, root: &Path) -> ResourcePathSelection {
    ResourcePathSelection {
        path: path.to_owned(),
        origin: ResourcePathOrigin::Package {
            grant_root: root.to_owned(),
        },
    }
}

fn direct_selection(path: &Path) -> ResourcePathSelection {
    ResourcePathSelection {
        path: path.to_owned(),
        origin: ResourcePathOrigin::DirectFile,
    }
}

#[test]
fn snapshots_merge_grants_and_honor_native_manifest_filters() {
    let temp = tempfile::tempdir().unwrap();
    let package = temp.path().join("package");
    let manifest = write(
        &package,
        "pi-package.json",
        &json!({"pi": {
            "skills": ["guide/*"],
            "prompts": ["snippets/**/*.md", "!**/excluded.md", "+.saved/rescued.md", "-snippets/deleted.md"],
            "themes": ["custom/dark.json"]
        }})
        .to_string(),
    );
    let skill = write(
        &package,
        "guide/example/SKILL.md",
        "---\nname: example\ndescription: Example instructions\n---\n",
    );
    let theme = write(&package, "custom/dark.json", THEME);
    write(&package, "themes/ignored.json", "malformed");
    let excluded = write(&package, "snippets/deep/excluded.md", "excluded");
    write(&package, "snippets/deleted.md", "deleted");
    write(&package, "snippets/.hidden.md", "hidden");
    let kept = write(&package, "snippets/deep/kept.md", "kept");
    let rescued = write(&package, ".saved/rescued.md", "rescued");
    let host = write(temp.path(), "standalone/host.md", "from the host");
    let fallback = write(temp.path(), "other/prompts/other.md", "another package");
    let roots = [package, temp.path().join("other")];
    let mut grants = ResourceReadGrants::default();
    grants.templates.files = vec![host.clone(), kept.clone(), excluded.clone()];

    let factory = ResourceFactory::default();
    let (snapshot, provenance) = factory
        .load_with_package_provenance(&grants, &[], &roots)
        .unwrap();
    assert_eq!(
        snapshot,
        factory.load_with_packages(&grants, &[], &roots).unwrap()
    );
    assert_eq!(
        provenance.prompts,
        [
            direct_selection(&host),
            direct_selection(&kept),
            direct_selection(&excluded),
            package_selection(&rescued, &roots[0]),
            package_selection(&fallback, &roots[1]),
        ]
    );
    assert_eq!(provenance.skills, [package_selection(&skill, &roots[0])]);
    assert_eq!(provenance.themes, [package_selection(&theme, &roots[0])]);
    assert!(provenance.extension_candidates.is_empty());
    assert_eq!(
        snapshot
            .resources
            .templates
            .iter()
            .map(|t| &t.path)
            .collect::<Vec<_>>(),
        [&host, &kept, &excluded, &rescued, &fallback]
    );
    assert_eq!(snapshot.resources.skills.skills[0].path, skill);
    assert_eq!(snapshot.resources.themes.themes()[0].path(), theme);
    assert!(snapshot.extension_candidates.is_empty());

    fs::write(&manifest, r#"{"pi":{"prompts":[]}}"#).unwrap();
    let reload = factory.load_with_packages(&grants, &[], &roots).unwrap();
    assert_eq!(reload.resources.templates.len(), 4);
    assert!(reload.resources.themes.themes().is_empty());
    fs::write(manifest, "{").unwrap();
    assert!(factory.load_with_packages(&grants, &[], &roots).is_err());
    assert_eq!(snapshot.resources.templates.len(), 5);
    assert_eq!(snapshot.resources.themes.themes().len(), 1);
}

#[test]
fn package_provenance_uses_first_selecting_grant_and_preserves_discovery_order() {
    let temp = tempfile::tempdir().unwrap();
    let outer = temp.path();
    let inner = outer.join("nested");
    write(
        outer,
        "pi-package.json",
        r#"{"pi":{"prompts":["nested/prompts","prompts","!inner-only.md","!never.md"]}}"#,
    );
    write(
        &inner,
        "pi-package.json",
        r#"{"pi":{"prompts":["prompts","!never.md"]}}"#,
    );
    let z = write(&inner, "prompts/z.md", "z");
    let a = write(&inner, "prompts/a.md", "a");
    let inner_only = write(&inner, "prompts/inner-only.md", "inner only");
    write(&inner, "prompts/never.md", "excluded by both manifests");
    let outer_only = write(outer, "prompts/outer.md", "outer only");

    let roots = [outer.to_owned(), inner.clone(), outer.to_owned()];
    let provenance = PackageResourcePaths::discover_with_provenance(&roots).unwrap();
    assert_eq!(
        provenance.prompts,
        [
            package_selection(&a, outer),
            package_selection(&z, outer),
            package_selection(&outer_only, outer),
            package_selection(&inner_only, &inner),
        ]
    );
    assert_eq!(
        PackageResourcePaths::discover(&roots).unwrap().prompts,
        [
            a.as_path(),
            z.as_path(),
            outer_only.as_path(),
            inner_only.as_path()
        ]
    );
    let reversed =
        PackageResourcePaths::discover_with_provenance(&[inner.clone(), outer.into()]).unwrap();
    assert_eq!(
        reversed.prompts,
        [
            package_selection(&a, &inner),
            package_selection(&inner_only, &inner),
            package_selection(&z, &inner),
            package_selection(&outer_only, outer),
        ]
    );
}

#[test]
fn override_only_lists_keep_default_candidates_and_exclude_basenames_and_directories() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    let manifest = write(root, "pi-package.json", "{}");
    let kept = write(root, "prompts/kept.md", "kept");
    let excluded = write(root, "prompts/nested/draft.md", "excluded by basename");
    let other = write(root, "prompts/nested/other.md", "excluded by directory");
    let rescued = write(root, ".saved/rescued.md", "forced");
    let discover = |patterns: &[&str]| {
        fs::write(&manifest, json!({"pi": {"prompts": patterns}}).to_string()).unwrap();
        PackageResourcePaths::discover(&[root.to_owned()])
            .unwrap()
            .prompts
    };

    assert_eq!(
        discover(&["+.saved/rescued.md"]),
        [rescued.clone(), kept.clone(), excluded, other.clone()]
    );
    assert_eq!(discover(&["prompts", "!draft.md"]), [kept.clone(), other]);
    assert_eq!(
        discover(&["!prompts/nested", "+.saved/rescued.md"]),
        [rescued, kept]
    );
}

#[cfg(any(unix, windows))]
#[test]
fn extension_discovery_uses_native_candidates_and_stem_overrides() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    let manifest = root.join("pi-package.json");
    let executable = |name: &str| {
        let path = write(root, name, "metadata-only fixture");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).unwrap();
        }
        path
    };
    let helper = executable("extensions/helper.exe");
    let runner = executable("extensions/runner.exe");
    let nested = executable("extensions/nested/tool.exe");
    let elsewhere = executable("tools/worker.exe");
    #[cfg(windows)]
    executable("extensions/no-executable-suffix");

    let candidates = || {
        PackageResourcePaths::discover(&[root.to_owned()])
            .unwrap()
            .extension_candidates
    };
    assert_eq!(candidates(), [helper.clone(), runner.clone()]);

    let discover = |patterns: &[&str]| {
        fs::write(&manifest, json!({"pi":{"extensions":patterns}}).to_string()).unwrap();
        candidates()
    };
    assert_eq!(discover(&["!helper"]), std::slice::from_ref(&runner));
    assert_eq!(
        discover(&["!helper", "+helper"]),
        [helper.clone(), runner.clone()]
    );
    assert_eq!(
        discover(&["!helper", "+helper", "-helper"]),
        std::slice::from_ref(&runner)
    );
    assert!(discover(&["+tools/worker.exe", "!*.exe"]).is_empty());
    assert_eq!(
        discover(&[
            "extensions/*.exe",
            "!helper",
            "extensions/nested/tool.exe",
            "tools/worker.exe"
        ]),
        [nested, runner.clone(), elsewhere]
    );
    assert_eq!(
        discover(&["extensions/@(helper|runner).exe"]),
        [helper.clone(), runner]
    );
    #[cfg(unix)]
    {
        let helper = executable("extensions/helper");
        let runner = executable("extensions/runner");
        assert_eq!(discover(&["extensions/@(helper|runner)"]), [helper, runner]);
    }
    assert_eq!(discover(&["helper"]), {
        #[cfg(unix)]
        {
            [root.join("extensions/helper"), helper].to_vec()
        }
        #[cfg(windows)]
        {
            vec![helper]
        }
    });
    assert!(discover(&[]).is_empty());
}

#[test]
fn skill_patterns_match_containing_directories_and_exact_overrides() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    let manifest = root.join("pi-package.json");
    let public = write(root, "skills/public/SKILL.md", "");
    let internal = write(root, "skills/internal-guide/SKILL.md", "");
    let outside = write(root, "docs/additional/SKILL.md", "");
    fs::create_dir(root.join("public")).unwrap();
    let discover = |patterns: &[&str]| {
        fs::write(&manifest, json!({"pi":{"skills":patterns}}).to_string()).unwrap();
        PackageResourcePaths::discover(&[root.to_owned()])
            .unwrap()
            .skills
    };

    assert_eq!(
        discover(&["skills", "!skills/internal-*"]),
        std::slice::from_ref(&public)
    );
    assert_eq!(
        discover(&["skills/*", "!internal-*"]),
        std::slice::from_ref(&public)
    );
    assert_eq!(discover(&["public"]), std::slice::from_ref(&public));
    let nested = write(root, "skills/public-guide/internal/SKILL.md", "");
    assert!(discover(&["skills/public-*"]).is_empty());
    assert_eq!(
        discover(&["skills/public-guide"]),
        std::slice::from_ref(&nested)
    );
    fs::remove_file(nested).unwrap();
    assert_eq!(
        discover(&["!internal-*"]),
        [outside.clone(), public.clone()]
    );
    assert_eq!(
        discover(&["skills", "!internal-*", "+skills/internal-guide"]),
        [internal, public.clone()]
    );
    assert_eq!(
        discover(&[
            "skills",
            "!internal-*",
            "+skills/internal-guide",
            "-skills/internal-guide",
            "+docs/additional"
        ]),
        [outside, public]
    );
}

#[test]
fn package_wide_matching_preserves_the_sdk_depth_limit() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    let depth = std::iter::repeat_n("d", 65).collect::<Vec<_>>().join("/");
    write(root, &format!("other/{depth}/SKILL.md"), "");
    let skill = write(root, "skills/chosen/SKILL.md", "");
    let manifest = write(
        root,
        "pi-package.json",
        r#"{"pi":{"skills":["skills/chosen"]}}"#,
    );
    let discover = || PackageResourcePaths::discover(&[root.to_owned()]);
    assert_eq!(discover().unwrap().skills, [skill]);
    fs::write(manifest, r#"{"pi":{"skills":["**/SKILL.md"]}}"#).unwrap();
    assert!(
        discover()
            .unwrap_err()
            .to_string()
            .contains("package directory scan limit exceeded")
    );
}

#[test]
fn validates_package_grants_and_manifest_paths() {
    let temp = tempfile::tempdir().unwrap();
    let package = temp.path().join("package");
    let outside = write(temp.path(), "outside.md", "outside the grant");
    let manifest = write(
        &package,
        "Cargo.toml",
        "[package.metadata.pi]\nprompts = []\n",
    );
    for path in [
        "../outside.md".to_owned(),
        r"+..\outside.md".to_owned(),
        outside.display().to_string(),
        "file:///outside.md".to_owned(),
        r"C:outside.md".to_owned(),
    ] {
        fs::write(
            &manifest,
            format!("[package.metadata.pi]\nprompts = [{}]\n", json!(path)),
        )
        .unwrap();
        let error = PackageResourcePaths::discover(std::slice::from_ref(&package)).unwrap_err();
        assert!(error.to_string().contains("must be relative"), "{error}");
    }
    for path in [
        PathBuf::from("relative"),
        package.join("../package"),
        outside,
    ] {
        assert!(PackageResourcePaths::discover(&[path]).is_err());
    }
}

#[test]
fn uses_cargo_metadata_and_keeps_pi_package_json_theme_compatibility() {
    let temp = tempfile::tempdir().unwrap();
    let package = temp.path();
    let cargo = write(
        package,
        "Cargo.toml",
        r#"[package.metadata.pi]
prompts = ["listed.md"]
"#,
    );
    let template = write(package, "listed.md", "from Cargo metadata");
    let theme = write(package, "custom/dark.json", THEME);
    write(
        package,
        "package.json",
        r#"{"pi":{"prompts":["listed.md"],"themes":["custom/dark.json"]}}"#,
    );
    let discover = || PackageResourcePaths::discover(&[package.into()]).unwrap();
    assert_eq!(discover().prompts, [template]);
    assert!(discover().themes.is_empty());
    fs::remove_file(cargo).unwrap();
    let paths = discover();
    assert!(paths.prompts.is_empty());
    assert_eq!(paths.themes, [theme]);
}

#[cfg(unix)]
#[tokio::test]
async fn package_read_never_describes_executables_until_explicitly_authorized() {
    use pi_rs_extensions::{NativeExtensionLoader, NativeExtensionSource};
    use std::os::unix::fs::{PermissionsExt, symlink};

    let temp = tempfile::tempdir().unwrap();
    let package = temp.path().join("package");
    let marker = temp.path().join("described");
    let executable = write(
        &package,
        "extensions/fixture",
        &format!(
            r#"#!/bin/sh
IFS= read -r request
touch '{}'
printf '%s\n' '{{"version":1,"response":{{"Ok":{{"id":"fixture","dependencies":[],"commands":["check"],"tools":[]}}}}}}'
"#,
            marker.display()
        ),
    );
    fs::set_permissions(&executable, fs::Permissions::from_mode(0o755)).unwrap();
    write(&package, "extensions/not-executable", "plain text");
    let unrelated = write(temp.path(), "ungranted/extensions/unrelated", "plain text");
    fs::set_permissions(unrelated, fs::Permissions::from_mode(0o755)).unwrap();
    let linked = write(temp.path(), "elsewhere/linked.md", "linked template");
    fs::create_dir(package.join("prompts")).unwrap();
    symlink(linked, package.join("prompts/linked.md")).unwrap();
    symlink(
        temp.path().join("elsewhere"),
        package.join("prompts/folder"),
    )
    .unwrap();

    let granted_root = temp.path().join("package-alias");
    symlink(&package, &granted_root).unwrap();
    let executable = granted_root.join("extensions/fixture");
    let (snapshot, provenance) = ResourceFactory::default()
        .load_with_package_provenance(
            &ResourceReadGrants::default(),
            &[],
            std::slice::from_ref(&granted_root),
        )
        .unwrap();
    assert_eq!(
        provenance.extension_candidates,
        [package_selection(&executable, &granted_root)]
    );
    assert_eq!(
        provenance.prompts,
        [package_selection(
            &granted_root.join("prompts/linked.md"),
            &granted_root
        )]
    );
    assert_eq!(snapshot.extension_candidates, [executable]);
    assert_eq!(snapshot.resources.templates.len(), 1);
    assert_eq!(snapshot.resources.templates[0].body, "linked template");
    assert!(!marker.exists());

    let registry = NativeExtensionLoader {
        sources: vec![NativeExtensionSource::Executable {
            path: snapshot.extension_candidates[0].clone(),
            flags: Default::default(),
        }],
        ..Default::default()
    }
    .load()
    .await
    .unwrap();
    assert!(marker.is_file());
    assert_eq!(registry.registered_commands()[0].extension_id, "fixture");
}
