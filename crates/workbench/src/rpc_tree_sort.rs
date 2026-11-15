// V8 TimSort comparison and merge order, specialized for tree timestamps.
// Derived from V8's array-sort.tq (Node 22/24); V8 credits the Python
// Software Foundation's TimSort. See ../../licenses/v8-array-sort.txt and
// ../../licenses/python.txt for notices and license terms.
// https://chromium.googlesource.com/v8/v8/+/e309b2d995fe23edf4bf8f92c4fe05914a08aef2/third_party/v8/builtins/array-sort.tq

type Item = (Option<i64>, usize);
fn lt(a: Item, b: Item) -> bool {
    matches!((a.0,b.0),(Some(x),Some(y)) if x<y)
}
// Find the insertion boundary using V8’s directional exponential search.
fn gallop(a: &[Item], key: Item, hint: usize, right: bool) -> usize {
    let n = a.len();
    let mut last: isize = 0;
    let mut off: isize = 1;
    let hint = hint as isize;
    let test = |v: Item| if right { lt(key, v) } else { !lt(v, key) };
    if !test(a[hint as usize]) {
        let max = n as isize - hint;
        while off < max && !test(a[(hint + off) as usize]) {
            last = off;
            off = (off * 2 + 1).min(max)
        }
        off = off.min(max);
        last += hint;
        off += hint;
    } else {
        let max = hint + 1;
        while off < max && test(a[(hint - off) as usize]) {
            last = off;
            off = (off * 2 + 1).min(max)
        }
        off = off.min(max);
        let tmp = last;
        last = hint - off;
        off = hint - tmp;
    }
    last += 1;
    while last < off {
        let mid = last + (off - last) / 2;
        if test(a[mid as usize]) {
            off = mid
        } else {
            last = mid + 1
        }
    }
    off as usize
}
// Merge forward when the left run is smaller. The comparison sequence,
// including transitions into and out of galloping, is observable for NaN.
fn merge_low(a: &[Item], b: &[Item], mg: &mut usize) -> Vec<Item> {
    let (mut i, mut j) = (0, 0);
    let mut o = Vec::with_capacity(a.len() + b.len());
    o.push(b[j]);
    j += 1;
    loop {
        if j == b.len() {
            o.extend_from_slice(&a[i..]);
            break;
        }
        if a.len() - i == 1 {
            o.extend_from_slice(&b[j..]);
            o.push(a[i]);
            break;
        }
        let (mut wa, mut wb) = (0usize, 0usize);
        loop {
            if lt(b[j], a[i]) {
                o.push(b[j]);
                j += 1;
                wb += 1;
                wa = 0;
                if j == b.len() || wb >= *mg {
                    break;
                }
            } else {
                o.push(a[i]);
                i += 1;
                wa += 1;
                wb = 0;
                if a.len() - i == 1 || wa >= *mg {
                    break;
                }
            }
        }
        if j == b.len() || a.len() - i == 1 {
            continue;
        }
        *mg += 1;
        let mut first = true;
        while wa >= 7 || wb >= 7 || first {
            first = false;
            *mg = (*mg).saturating_sub(1).max(1);
            wa = gallop(&a[i..], b[j], 0, true);
            o.extend_from_slice(&a[i..i + wa]);
            i += wa;
            if a.len() - i <= 1 {
                break;
            }
            o.push(b[j]);
            j += 1;
            if j == b.len() {
                break;
            }
            wb = gallop(&b[j..], a[i], 0, false);
            o.extend_from_slice(&b[j..j + wb]);
            j += wb;
            if j == b.len() {
                break;
            }
            o.push(a[i]);
            i += 1;
            if a.len() - i == 1 {
                break;
            }
        }
        if i == a.len() {
            o.extend_from_slice(&b[j..]);
            break;
        }
        if j == b.len() || a.len() - i == 1 {
            continue;
        }
        *mg += 1;
    }
    o
}
// Merge backward when the right run is smaller.
fn merge_high(a: &[Item], b: &[Item], mg: &mut usize) -> Vec<Item> {
    let (mut i, mut j) = (a.len(), b.len());
    let mut o = Vec::with_capacity(a.len() + b.len());
    i -= 1;
    o.push(a[i]);
    loop {
        if i == 0 {
            o.extend(b[..j].iter().rev().copied());
            break;
        }
        if j == 1 {
            o.extend(a[..i].iter().rev().copied());
            o.push(b[0]);
            break;
        }
        let (mut wa, mut wb) = (0usize, 0usize);
        loop {
            if lt(b[j - 1], a[i - 1]) {
                i -= 1;
                o.push(a[i]);
                wa += 1;
                wb = 0;
                if i == 0 || wa >= *mg {
                    break;
                }
            } else {
                j -= 1;
                o.push(b[j]);
                wb += 1;
                wa = 0;
                if j == 1 || wb >= *mg {
                    break;
                }
            }
        }
        if i == 0 || j == 1 {
            continue;
        }
        *mg += 1;
        let mut first = true;
        while wa >= 7 || wb >= 7 || first {
            first = false;
            *mg = (*mg).saturating_sub(1).max(1);
            let k = gallop(&a[..i], b[j - 1], i - 1, true);
            wa = i - k;
            o.extend(a[k..i].iter().rev().copied());
            i = k;
            if i == 0 {
                break;
            }
            j -= 1;
            o.push(b[j]);
            if j == 1 {
                break;
            }
            let k = gallop(&b[..j], a[i - 1], j - 1, false);
            wb = j - k;
            o.extend(b[k..j].iter().rev().copied());
            j = k;
            if j <= 1 {
                break;
            }
            i -= 1;
            o.push(a[i]);
            if i == 0 {
                break;
            }
        }
        if j == 0 {
            o.extend(a[..i].iter().rev().copied());
            break;
        }
        if i == 0 || j == 1 {
            continue;
        }
        *mg += 1;
    }
    o.reverse();
    o
}
// Trim already positioned items using V8’s gallop boundaries.
fn merge_at(v: &mut [Item], runs: &mut Vec<(usize, usize)>, k: usize, mg: &mut usize) {
    let (a, na) = runs[k];
    let (b, nb) = runs[k + 1];
    assert_eq!(a + na, b);
    runs[k] = (a, na + nb);
    runs.remove(k + 1);
    let trim_a = gallop(&v[a..b], v[b], 0, true);
    let a = a + trim_a;
    let na = na - trim_a;
    if na == 0 {
        return;
    }
    let nb = gallop(&v[b..b + nb], v[a + na - 1], nb - 1, false);
    if nb == 0 {
        return;
    }
    let x = v[a..b].to_vec();
    let y = v[b..b + nb].to_vec();
    let out = if na <= nb {
        merge_low(&x, &y, mg)
    } else {
        merge_high(&x, &y, mg)
    };
    assert_eq!(out.len(), na + nb);
    v[a..b + nb].copy_from_slice(&out);
}
pub(super) fn sort_tree_mixed(v: &mut [Item]) {
    let n = v.len();
    if n < 2 {
        return;
    }
    let (mut min, mut rem) = (n, 0);
    while min >= 64 {
        rem |= min & 1;
        min >>= 1
    }
    min += rem;
    let mut runs = Vec::new();
    let mut mg = 7;
    let mut s = 0;
    while s < n {
        let mut e = s + 1;
        if e < n {
            let desc = lt(v[e], v[s]);
            e += 1;
            while e < n
                && (if desc {
                    lt(v[e], v[e - 1])
                } else {
                    !lt(v[e], v[e - 1])
                })
            {
                e += 1
            }
            if desc {
                v[s..e].reverse()
            }
        }
        let target = (s + min).min(n);
        while e < target {
            let item = v[e];
            let (mut lo, mut hi) = (s, e);
            while lo < hi {
                let m = lo + (hi - lo) / 2;
                if lt(item, v[m]) { hi = m } else { lo = m + 1 }
            }
            v[lo..=e].rotate_right(1);
            e += 1
        }
        runs.push((s, e - s));
        s = e;
        while runs.len() > 1 {
            let mut k = runs.len() - 2;
            let bad = |i: usize, r: &[(usize, usize)]| i >= 2 && r[i - 2].1 <= r[i - 1].1 + r[i].1;
            if bad(k + 1, &runs) || bad(k, &runs) {
                if k > 0 && runs[k - 1].1 < runs[k + 1].1 {
                    k -= 1
                }
                merge_at(v, &mut runs, k, &mut mg)
            } else if runs[k].1 <= runs[k + 1].1 {
                merge_at(v, &mut runs, k, &mut mg)
            } else {
                break;
            }
        }
    }
    while runs.len() > 1 {
        let mut k = runs.len() - 2;
        if k > 0 && runs[k - 1].1 < runs[k + 1].1 {
            k -= 1
        }
        merge_at(v, &mut runs, k, &mut mg)
    }
}

#[cfg(test)]
mod tests {
    use super::sort_tree_mixed;

    #[test]
    fn mixed_timestamp_order_matches_node_22_and_24() {
        // Generated with Node 22.23.4 and 24.22.1: comparator returns NaN
        // if either timestamp is invalid, and otherwise subtracts timestamps.
        let fixtures: serde_json::Value =
            serde_json::from_str(include_str!("rpc_tree_sort_fixtures.json")).unwrap();
        for fixture in fixtures.as_array().unwrap() {
            let mut entries: Vec<_> = fixture["input"]
                .as_array()
                .unwrap()
                .iter()
                .enumerate()
                .map(|(index, value)| (value.as_i64(), index))
                .collect();
            sort_tree_mixed(&mut entries);
            let actual: Vec<_> = entries.iter().map(|item| item.1).collect();
            let expected: Vec<_> = fixture["expected"]
                .as_array()
                .unwrap()
                .iter()
                .map(|value| value.as_u64().unwrap() as usize)
                .collect();
            assert_eq!(actual, expected, "fixture size {}", entries.len());
        }
    }
}
