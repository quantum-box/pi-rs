//! Existing downstream exhaustive matches must keep compiling.
use pi_rs_core::CompactionReason;

#[test]
fn existing_compaction_reason_remains_source_compatible() {
    let describe = |reason: CompactionReason| match reason {
        CompactionReason::Manual => "manual",
        CompactionReason::Threshold => "threshold",
    };
    assert_eq!(describe(CompactionReason::Manual), "manual");
}
