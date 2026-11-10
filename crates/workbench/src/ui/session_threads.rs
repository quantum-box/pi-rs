//! Parent-session grouping, ordered by the newest activity in each subtree.
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

pub struct Row {
    pub index: usize,
    pub prefix: String,
}
fn key(path: &Path) -> PathBuf {
    path.canonicalize().unwrap_or_else(|_| path.to_path_buf())
}

/// Input paths are in recent-first order. Missing parents become independent roots.
pub fn layout(paths: &[PathBuf], parent_paths: &[Option<PathBuf>]) -> Vec<Row> {
    let by_path: BTreeMap<_, _> = paths
        .iter()
        .enumerate()
        .map(|(index, path)| (key(path), index))
        .collect();
    let mut parents: Vec<_> = parent_paths
        .iter()
        .map(|parent| {
            parent
                .as_ref()
                .and_then(|path| by_path.get(&key(path)))
                .copied()
        })
        .collect();
    for start in 0..parents.len() {
        let mut seen = BTreeSet::new();
        let mut current = Some(start);
        while let Some(index) = current {
            if !seen.insert(index) {
                parents[start] = None;
                break;
            }
            current = parents[index];
        }
    }
    let mut children = vec![Vec::new(); paths.len()];
    let mut roots = Vec::new();
    let mut recent: Vec<_> = (0..paths.len()).collect();
    for (index, parent) in parents.iter().enumerate() {
        if let Some(parent) = parent {
            children[*parent].push(index);
        } else {
            roots.push(index);
        }
        let mut ancestor = *parent;
        while let Some(parent) = ancestor {
            recent[parent] = recent[parent].min(index);
            ancestor = parents[parent];
        }
    }
    roots.sort_by_key(|index| recent[*index]);
    for children in &mut children {
        children.sort_by_key(|index| recent[*index]);
    }
    let mut pending: Vec<_> = roots
        .iter()
        .enumerate()
        .rev()
        .map(|(position, index)| (*index, Vec::<bool>::new(), position + 1 == roots.len()))
        .collect();
    let mut rows = Vec::with_capacity(paths.len());
    while let Some((index, ancestors, last)) = pending.pop() {
        let mut prefix = ancestors
            .iter()
            .map(|continues| if *continues { "│  " } else { "   " })
            .collect::<String>();
        if !ancestors.is_empty() {
            prefix.push_str(if last { "└─ " } else { "├─ " });
        }
        rows.push(Row { index, prefix });
        for (position, child) in children[index].iter().enumerate().rev() {
            let mut continues = ancestors.clone();
            continues.push(!ancestors.is_empty() && !last);
            pending.push((*child, continues, position + 1 == children[index].len()));
        }
    }
    rows
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn newest_descendant_orders_roots_and_children() {
        let paths = ["grandchild", "other", "child", "root"].map(PathBuf::from);
        let parents =
            [Some("child"), None, Some("root"), None].map(|parent| parent.map(PathBuf::from));
        let rows = layout(&paths, &parents);
        assert_eq!(
            rows.iter().map(|row| row.index).collect::<Vec<_>>(),
            vec![3, 2, 0, 1]
        );
        assert_eq!(rows[0].prefix, "");
        assert_eq!(rows[1].prefix, "   └─ ");
        assert_eq!(rows[2].prefix, "      └─ ");
    }
    #[test]
    fn missing_parents_and_cycles_do_not_hide_sessions() {
        let paths = ["a", "b", "orphan"].map(PathBuf::from);
        let parents =
            [Some("b"), Some("a"), Some("missing")].map(|parent| parent.map(PathBuf::from));
        let rows = layout(&paths, &parents);
        assert_eq!(rows.len(), 3);
        assert_eq!(
            rows.iter().map(|row| row.index).collect::<BTreeSet<_>>(),
            BTreeSet::from([0, 1, 2])
        );
    }
}
