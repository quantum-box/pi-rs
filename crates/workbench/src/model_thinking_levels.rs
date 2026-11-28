use std::collections::BTreeMap;

pub(super) const LEVELS: &[&str] = &["off", "minimal", "low", "medium", "high", "xhigh", "max"];

pub(super) fn mapped_levels(map: &BTreeMap<String, Option<String>>) -> Vec<String> {
    LEVELS
        .iter()
        .copied()
        .filter(|level| {
            map.get(*level)
                .map_or(!matches!(*level, "xhigh" | "max"), Option::is_some)
        })
        .map(str::to_owned)
        .collect()
}
