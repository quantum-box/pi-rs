//! Source compatibility with downstream extensions built against main.
use pi_rs_core::Event;

#[test]
fn existing_compaction_event_and_reason_remain_source_compatible() {
    let event = Event::Compacted {
        summary: "summary".into(),
        system_prompt: None,
        retain_from: None,
        tokens_before: None,
        states: Default::default(),
        usage: None,
        usage_metadata: None,
    };
    assert!(matches!(event, Event::Compacted { .. }));
}
