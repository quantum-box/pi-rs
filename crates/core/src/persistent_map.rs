//! Small persistent ordered map. Updates copy only the AVL search path.
use std::borrow::Borrow;
use std::cmp::Ordering;
use std::sync::Arc;

#[derive(Clone)]
struct Node<K, V> {
    key: K,
    value: V,
    left: Option<Arc<Node<K, V>>>,
    right: Option<Arc<Node<K, V>>>,
    height: u8,
}

pub(crate) struct PersistentMap<K, V> {
    root: Option<Arc<Node<K, V>>>,
}

impl<K, V> Clone for PersistentMap<K, V> {
    fn clone(&self) -> Self {
        Self {
            root: self.root.clone(),
        }
    }
}
impl<K, V> Default for PersistentMap<K, V> {
    fn default() -> Self {
        Self { root: None }
    }
}

fn height<K, V>(node: &Option<Arc<Node<K, V>>>) -> u8 {
    node.as_ref().map_or(0, |node| node.height)
}
fn fix_height<K: Clone, V: Clone>(node: &mut Arc<Node<K, V>>) {
    let node = Arc::make_mut(node);
    node.height = 1 + height(&node.left).max(height(&node.right));
}
fn rotate_left<K: Clone, V: Clone>(link: &mut Option<Arc<Node<K, V>>>) {
    let mut root = link.take().expect("AVL root");
    let mut right = Arc::make_mut(&mut root)
        .right
        .take()
        .expect("AVL right child");
    Arc::make_mut(&mut root).right = Arc::make_mut(&mut right).left.take();
    fix_height(&mut root);
    Arc::make_mut(&mut right).left = Some(root);
    fix_height(&mut right);
    *link = Some(right);
}
fn rotate_right<K: Clone, V: Clone>(link: &mut Option<Arc<Node<K, V>>>) {
    let mut root = link.take().expect("AVL root");
    let mut left = Arc::make_mut(&mut root)
        .left
        .take()
        .expect("AVL left child");
    Arc::make_mut(&mut root).left = Arc::make_mut(&mut left).right.take();
    fix_height(&mut root);
    Arc::make_mut(&mut left).right = Some(root);
    fix_height(&mut left);
    *link = Some(left);
}
fn rebalance<K: Clone, V: Clone>(link: &mut Option<Arc<Node<K, V>>>) {
    let Some(root) = link.as_mut() else { return };
    fix_height(root);
    let difference = i16::from(height(&root.left)) - i16::from(height(&root.right));
    if difference > 1 {
        let left = root.left.as_ref().expect("AVL left child");
        if height(&left.right) > height(&left.left) {
            rotate_left(&mut Arc::make_mut(root).left);
        }
        rotate_right(link);
    } else if difference < -1 {
        let right = root.right.as_ref().expect("AVL right child");
        if height(&right.left) > height(&right.right) {
            rotate_right(&mut Arc::make_mut(root).right);
        }
        rotate_left(link);
    }
}

impl<K: Ord + Clone, V: Clone> PersistentMap<K, V> {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn get<Q: Ord + ?Sized>(&self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
    {
        let mut node = self.root.as_deref();
        while let Some(current) = node {
            node = match key.cmp(current.key.borrow()) {
                Ordering::Less => current.left.as_deref(),
                Ordering::Greater => current.right.as_deref(),
                Ordering::Equal => return Some(&current.value),
            };
        }
        None
    }
    pub(crate) fn contains_key<Q: Ord + ?Sized>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
    {
        self.get(key).is_some()
    }
    pub(crate) fn insert(&mut self, key: K, value: V) -> Option<V> {
        fn put<K: Ord + Clone, V: Clone>(
            link: &mut Option<Arc<Node<K, V>>>,
            key: K,
            value: V,
        ) -> Option<V> {
            let Some(root) = link.as_mut() else {
                *link = Some(Arc::new(Node {
                    key,
                    value,
                    left: None,
                    right: None,
                    height: 1,
                }));
                return None;
            };
            let node = Arc::make_mut(root);
            let old = match key.cmp(&node.key) {
                Ordering::Less => put(&mut node.left, key, value),
                Ordering::Greater => put(&mut node.right, key, value),
                Ordering::Equal => return Some(std::mem::replace(&mut node.value, value)),
            };
            rebalance(link);
            old
        }
        put(&mut self.root, key, value)
    }
    pub(crate) fn remove<Q: Ord + ?Sized>(&mut self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
    {
        fn take_min<K: Ord + Clone, V: Clone>(link: &mut Option<Arc<Node<K, V>>>) -> (K, V) {
            if link.as_ref().expect("nonempty AVL").left.is_none() {
                let node = link.take().expect("nonempty AVL");
                *link = node.right.clone();
                return (node.key.clone(), node.value.clone());
            }
            let result = take_min(&mut Arc::make_mut(link.as_mut().expect("nonempty AVL")).left);
            rebalance(link);
            result
        }
        fn del<K: Ord + Clone + Borrow<Q>, V: Clone, Q: Ord + ?Sized>(
            link: &mut Option<Arc<Node<K, V>>>,
            key: &Q,
        ) -> Option<V> {
            let root = link.as_mut()?;
            let direction = key.cmp(root.key.borrow());
            let old = match direction {
                Ordering::Less => del(&mut Arc::make_mut(root).left, key),
                Ordering::Greater => del(&mut Arc::make_mut(root).right, key),
                Ordering::Equal => {
                    let node = Arc::make_mut(root);
                    let old = node.value.clone();
                    if node.left.is_none() {
                        *link = node.right.clone();
                        return Some(old);
                    }
                    if node.right.is_none() {
                        *link = node.left.clone();
                        return Some(old);
                    }
                    let (key, value) = take_min(&mut node.right);
                    node.key = key;
                    node.value = value;
                    Some(old)
                }
            };
            if old.is_some() {
                rebalance(link);
            }
            old
        }
        del(&mut self.root, key)
    }
    pub(crate) fn iter(&self) -> Iter<'_, K, V> {
        Iter {
            stack: Vec::new(),
            next: self.root.as_deref(),
        }
    }
}

impl<K: Ord + Clone, V: Clone> Extend<(K, V)> for PersistentMap<K, V> {
    fn extend<T: IntoIterator<Item = (K, V)>>(&mut self, iter: T) {
        for (key, value) in iter {
            self.insert(key, value);
        }
    }
}

pub(crate) struct Iter<'a, K, V> {
    stack: Vec<&'a Node<K, V>>,
    next: Option<&'a Node<K, V>>,
}
impl<'a, K, V> Iterator for Iter<'a, K, V> {
    type Item = (&'a K, &'a V);
    fn next(&mut self) -> Option<Self::Item> {
        while let Some(node) = self.next.take() {
            self.stack.push(node);
            self.next = node.left.as_deref();
        }
        let node = self.stack.pop()?;
        self.next = node.right.as_deref();
        Some((&node.key, &node.value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeMap, HashSet};

    fn verify(node: &Option<Arc<Node<u32, u32>>>, low: Option<u32>, high: Option<u32>) -> u8 {
        let Some(node) = node else { return 0 };
        assert!(low.is_none_or(|bound| node.key > bound));
        assert!(high.is_none_or(|bound| node.key < bound));
        let left = verify(&node.left, low, Some(node.key));
        let right = verify(&node.right, Some(node.key), high);
        assert!((i16::from(left) - i16::from(right)).abs() <= 1);
        assert_eq!(node.height, 1 + left.max(right));
        node.height
    }

    #[test]
    fn mixed_updates_match_btree_and_preserve_snapshots() {
        let mut actual = PersistentMap::new();
        let mut expected = BTreeMap::new();
        let mut seed = 17_u64;
        for step in 0..12_000_u32 {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            let key = ((seed >> 32) % 700) as u32;
            let before = (step % 211 == 0).then(|| (actual.clone(), expected.clone()));
            if seed & 3 == 0 {
                assert_eq!(actual.remove(&key), expected.remove(&key));
            } else {
                assert_eq!(actual.insert(key, step), expected.insert(key, step));
            }
            assert_eq!(actual.get(&key), expected.get(&key));
            if let Some((snapshot, previous)) = before {
                assert_eq!(
                    snapshot.iter().map(|(k, v)| (*k, *v)).collect::<Vec<_>>(),
                    previous.into_iter().collect::<Vec<_>>()
                );
            }
            if step % 211 == 0 {
                verify(&actual.root, None, None);
                assert_eq!(
                    actual.iter().map(|(k, v)| (*k, *v)).collect::<Vec<_>>(),
                    expected.iter().map(|(k, v)| (*k, *v)).collect::<Vec<_>>()
                );
            }
        }
        verify(&actual.root, None, None);
    }

    #[test]
    fn sorted_updates_remain_balanced_and_share_untouched_nodes() {
        let mut base = PersistentMap::new();
        for key in 0..20_000_u32 {
            base.insert(key, key);
        }
        let tree_height = verify(&base.root, None, None);
        assert!(tree_height <= 30, "AVL height was {tree_height}");
        let mut branch = base.clone();
        branch.insert(20_000, 9);
        branch.remove(&10_000);
        assert_eq!(base.get(&20_000), None);
        assert_eq!(base.get(&10_000), Some(&10_000));
        assert_eq!(branch.get(&10_000), None);
        verify(&branch.root, None, None);
        fn pointers(node: &Option<Arc<Node<u32, u32>>>, seen: &mut HashSet<usize>) {
            if let Some(node) = node {
                seen.insert(Arc::as_ptr(node) as usize);
                pointers(&node.left, seen);
                pointers(&node.right, seen);
            }
        }
        let mut before = HashSet::new();
        let mut after = HashSet::new();
        pointers(&base.root, &mut before);
        pointers(&branch.root, &mut after);
        assert!(before.intersection(&after).count() > 19_900);
        for key in 0..500_u32 {
            let mut sibling = base.clone();
            sibling.insert(key, u32::MAX);
            sibling.remove(&(19_999 - key));
            assert_eq!(base.get(&key), Some(&key));
            assert_eq!(base.get(&(19_999 - key)), Some(&(19_999 - key)));
        }
    }

    #[test]
    fn borrowed_string_lookup_and_removal() {
        let mut map = PersistentMap::new();
        map.insert(Arc::<str>::from("alpha"), 1);
        map.insert(Arc::<str>::from("beta"), 2);
        let snapshot = map.clone();
        assert_eq!(map.remove("alpha"), Some(1));
        assert_eq!(map.get("alpha"), None);
        assert_eq!(snapshot.get("alpha"), Some(&1));
    }
}
