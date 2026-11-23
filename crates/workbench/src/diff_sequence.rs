//! Sequence alignment from jsdiff 8.0.4 by Kevin Decker and contributors.
//! See ui/diff/JSDIFF-LICENSE.txt.
use std::{collections::HashMap, rc::Rc};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Tag {
    Equal,
    Delete,
    Insert,
}
#[derive(Clone)]
struct Component {
    tag: Tag,
    count: usize,
    prev: Option<Rc<Component>>,
}
#[derive(Clone)]
struct Path {
    old: isize,
    last: Option<Rc<Component>>,
}

fn add(path: &Path, tag: Tag, count: usize, old_inc: isize) -> Path {
    let (count, prev) = match &path.last {
        Some(last) if last.tag == tag => (count + last.count, last.prev.clone()),
        _ => (count, path.last.clone()),
    };
    Path {
        old: path.old + old_inc,
        last: Some(Rc::new(Component { tag, count, prev })),
    }
}
fn common<T>(
    path: &mut Path,
    diagonal: isize,
    old: &[T],
    new: &[T],
    equal: &impl Fn(&T, &T) -> bool,
    keep_going: &mut impl FnMut() -> bool,
) -> Option<isize> {
    let mut n = path.old - diagonal;
    let mut count = 0;
    while (path.old + 1) < old.len() as isize
        && n + 1 < new.len() as isize
        && equal(&old[(path.old + 1) as usize], &new[(n + 1) as usize])
    {
        if !keep_going() {
            return None;
        }
        path.old += 1;
        n += 1;
        count += 1;
    }
    if count > 0 {
        *path = add(path, Tag::Equal, count, 0);
    }
    Some(n)
}
fn components(path: Path) -> Vec<(Tag, usize)> {
    let mut comps = Vec::new();
    let mut node = path.last;
    while let Some(c) = node {
        comps.push((c.tag, c.count));
        node = c.prev.clone();
    }
    comps.reverse();
    comps
}

/// Choose the same alignment as jsdiff for the supplied tokens.
pub fn align<T>(
    old: &[T],
    new: &[T],
    equal: impl Fn(&T, &T) -> bool,
    max_distance: usize,
    mut keep_going: impl FnMut() -> bool,
) -> Option<Vec<(Tag, usize)>> {
    let mut best: HashMap<isize, Path> = HashMap::new();
    let mut initial = Path {
        old: -1,
        last: None,
    };
    let n = common(&mut initial, 0, old, new, &equal, &mut keep_going)?;
    if initial.old + 1 >= old.len() as isize && n + 1 >= new.len() as isize {
        return Some(components(initial));
    }
    best.insert(0, initial);
    let (mut min, mut max) = (isize::MIN, isize::MAX);
    for length in 1..=max_distance.min(old.len() + new.len()) {
        let length = length as isize;
        let mut diagonal = (-length).max(min);
        let end = length.min(max);
        while diagonal <= end {
            if !keep_going() {
                return None;
            }
            let remove = best.remove(&(diagonal - 1));
            let add_path = best.get(&(diagonal + 1)).cloned();
            let can_add = add_path.as_ref().is_some_and(|p| {
                let pos = p.old - diagonal;
                pos >= 0 && pos < new.len() as isize
            });
            let can_remove = remove
                .as_ref()
                .is_some_and(|p| p.old + 1 < old.len() as isize);
            if !can_add && !can_remove {
                best.remove(&diagonal);
                diagonal += 2;
                continue;
            }
            let mut path = if !can_remove
                || (can_add && remove.as_ref().unwrap().old < add_path.as_ref().unwrap().old)
            {
                add(add_path.as_ref().unwrap(), Tag::Insert, 1, 0)
            } else {
                add(remove.as_ref().unwrap(), Tag::Delete, 1, 1)
            };
            let n = common(&mut path, diagonal, old, new, &equal, &mut keep_going)?;
            if path.old + 1 >= old.len() as isize && n + 1 >= new.len() as isize {
                return Some(components(path));
            }
            if path.old + 1 >= old.len() as isize {
                max = max.min(diagonal - 1);
            }
            if n + 1 >= new.len() as isize {
                min = min.max(diagonal + 1);
            }
            best.insert(diagonal, path);
            diagonal += 2;
        }
    }
    None
}
