use pi_rs_core::{Event, Record, Session};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use uuid::Uuid;

fn record(id: u64, parent: Option<u64>, event: Event) -> Record {
    Record {
        id,
        parent,
        event,
        version: 2,
        timestamp: None,
        run_id: None,
    }
}
fn source(entry: Value) -> Event {
    Event::SourceEntry {
        format: "pi".into(),
        entry,
    }
}
fn created() -> Record {
    record(
        1,
        None,
        Event::Created {
            session_id: Uuid::new_v4(),
        },
    )
}
fn user(text: &str) -> Event {
    Event::User {
        content: text.into(),
        input_id: None,
    }
}
fn session(records: Vec<Record>, ids: &[u64]) -> Session {
    let metadata: BTreeMap<_, _> = ids
        .iter()
        .map(|id| (*id, json!({"pi_rs_import_conversion_count_v1":1})))
        .collect();
    Session::from_records_with_metadata(records, metadata).unwrap()
}
fn model(name: &str, thinking: Option<&str>) -> Event {
    Event::ModelSelected {
        model: name.into(),
        provider: Some("p".into()),
        thinking: thinking.map(str::to_owned),
    }
}
#[test]
fn partial_conversion_branch_is_rejected_by_batch_and_fork() {
    let s = session(
        vec![
            created(),
            record(2, Some(1), source(json!({"type":"session","version":3}))),
            record(
                3,
                Some(2),
                source(
                    json!({"type":"model_change","id":"a","parentId":null,"provider":"p","modelId":"alpha"}),
                ),
            ),
            record(4, Some(3), model("alpha", None)),
            record(5, Some(4), Event::Checkout { target: 3 }),
            record(
                6,
                Some(3),
                source(
                    json!({"type":"thinking_level_change","id":"b","parentId":"a","thinkingLevel":"high"}),
                ),
            ),
            record(7, Some(6), model("alpha", Some("high"))),
        ],
        &[3, 6],
    );
    assert!(!s.branch().iter().any(|r| r.id == 4));
    let validation = s.validated_pi_conversion_counts();
    eprintln!("partial branch batch validation: {validation:?}");
    assert!(validation.is_err());
    let complete_fork = s.fork_session(4);
    eprintln!(
        "complete conversion fork: {:?}",
        complete_fork.as_ref().map(|_| "accepted")
    );
    assert!(complete_fork.is_ok());
    let fork = s.fork_session(s.head());
    eprintln!("partial branch fork: {:?}", fork.as_ref().err());
    assert!(fork.is_err());
}
#[test]
fn parent_id_validation_preserves_missing_and_rejects_self_or_wrong_type() {
    for parent in [
        json!("a"),
        json!("missing"),
        json!("b"),
        json!(42),
        Value::Null,
    ] {
        let s = session(
            vec![
                created(),
                record(2, Some(1), source(json!({"type":"session","version":3}))),
                record(
                    3,
                    Some(2),
                    source(
                        json!({"type":"message","id":"a","parentId":null,"message":{"role":"user","content":"one"}}),
                    ),
                ),
                record(4, Some(3), user("one")),
                record(
                    5,
                    Some(4),
                    source(
                        json!({"type":"message","id":"b","parentId":parent,"message":{"role":"user","content":"two"}}),
                    ),
                ),
                record(6, Some(5), user("two")),
            ],
            &[3, 5],
        );
        let result = s.validated_pi_conversion_counts();
        eprintln!("parentId={parent}: {result:?}");
        let expected = parent == json!("a") || parent == json!("missing");
        assert_eq!(result.is_ok(), expected);
        assert_eq!(stream_validate(&s).is_ok(), expected);
    }
}
#[test]
fn genuine_missing_parent_root_and_v1_implicit_parent_are_accepted() {
    let orphan = session(
        vec![
            created(),
            record(2, Some(1), source(json!({"type":"session","version":3}))),
            record(
                3,
                Some(2),
                source(
                    json!({"type":"message","id":"orphan","parentId":"external-missing","message":{"role":"user","content":"hello"}}),
                ),
            ),
            record(4, Some(3), user("hello")),
        ],
        &[3],
    );
    eprintln!(
        "genuine orphan: {:?}",
        orphan.validated_pi_conversion_counts()
    );
    assert!(orphan.validated_pi_conversion_counts().is_ok());
    let v1 = session(
        vec![
            created(),
            record(2, Some(1), source(json!({"type":"session","version":1}))),
            record(
                3,
                Some(2),
                source(json!({"type":"message","message":{"role":"user","content":"one"}})),
            ),
            record(4, Some(3), user("one")),
            record(
                5,
                Some(4),
                source(json!({"type":"message","message":{"role":"user","content":"two"}})),
            ),
            record(6, Some(5), user("two")),
        ],
        &[3, 5],
    );
    eprintln!(
        "v1 implicit ancestry: {:?}",
        v1.validated_pi_conversion_counts()
    );
    assert!(v1.validated_pi_conversion_counts().is_ok());
}
#[test]
fn actual_fork_may_remove_a_source_parent_label() {
    let s = session(
        vec![
            created(),
            record(2, Some(1), source(json!({"type":"session","version":3}))),
            record(
                3,
                Some(2),
                source(
                    json!({"type":"message","id":"a","parentId":null,"message":{"role":"user","content":"one"}}),
                ),
            ),
            record(4, Some(3), user("one")),
            record(
                5,
                Some(4),
                source(
                    json!({"type":"label","id":"label","parentId":"a","targetId":"a","label":"tag"}),
                ),
            ),
            record(
                6,
                Some(5),
                Event::Label {
                    target: 4,
                    label: Some("tag".into()),
                },
            ),
            record(
                7,
                Some(6),
                source(
                    json!({"type":"message","id":"b","parentId":"label","message":{"role":"user","content":"two"}}),
                ),
            ),
            record(8, Some(7), user("two")),
        ],
        &[3, 5, 7],
    );
    assert!(s.validated_pi_conversion_counts().is_ok());
    let fork = s.fork_session(s.head()).unwrap();
    let sources: Vec<_> = fork
        .records()
        .iter()
        .filter_map(|r| {
            if let Event::SourceEntry { format, entry } = &r.event {
                (format == "pi").then_some(entry)
            } else {
                None
            }
        })
        .collect();
    let b = sources.iter().find(|e| e["id"] == "b").unwrap();
    eprintln!(
        "fork source parentId={}, retained source IDs={:?}",
        b["parentId"],
        sources.iter().map(|e| &e["id"]).collect::<Vec<_>>()
    );
    assert_eq!(b["parentId"], "label");
    assert!(!sources.iter().any(|e| e["id"] == "label"));
    let result = fork.validated_pi_conversion_counts();
    eprintln!("fork validation: {result:?}");
    assert!(result.is_ok());
}

#[test]
fn mismatched_native_conversion_is_rejected() {
    let s = session(
        vec![
            created(),
            record(2, Some(1), source(json!({"type":"session","version":3}))),
            record(
                3,
                Some(2),
                source(
                    json!({"type":"message","id":"a","parentId":null,"message":{"role":"user","content":"original"}}),
                ),
            ),
            record(4, Some(3), user("forged")),
        ],
        &[3],
    );
    let result = s.validated_pi_conversion_counts();
    eprintln!("modified native payload: {result:?}");
    assert!(result.is_err());
}

fn stream_validate(s: &Session) -> pi_rs_core::Result<()> {
    let metadata = s
        .records()
        .iter()
        .filter_map(|r| s.record_metadata(r.id).cloned().map(|m| (r.id, m)))
        .collect();
    let mut stream = pi_rs_core::pi_import::StreamVerifier::new();
    assert_eq!(stream.advance(s.records(), &metadata, true)?, None);
    Ok(())
}

#[test]
fn existing_sibling_and_forward_parents_are_rejected() {
    for parent in ["a", "later"] {
        let s = session(
            vec![
                created(),
                record(2, Some(1), source(json!({"type":"session","version":3}))),
                record(
                    3,
                    Some(2),
                    source(
                        json!({"type":"message","id":"a","parentId":null,"message":{"role":"user","content":"a"}}),
                    ),
                ),
                record(4, Some(3), user("a")),
                record(5, Some(4), Event::Checkout { target: 2 }),
                record(
                    6,
                    Some(2),
                    source(
                        json!({"type":"message","id":"b","parentId":null,"message":{"role":"user","content":"b"}}),
                    ),
                ),
                record(7, Some(6), user("b")),
                record(
                    8,
                    Some(7),
                    source(
                        json!({"type":"message","id":"c","parentId":parent,"message":{"role":"user","content":"c"}}),
                    ),
                ),
                record(9, Some(8), user("c")),
                record(
                    10,
                    Some(9),
                    source(
                        json!({"type":"message","id":"later","parentId":"c","message":{"role":"user","content":"later"}}),
                    ),
                ),
                record(11, Some(10), user("later")),
            ],
            &[3, 6, 8, 10],
        );
        assert!(s.validated_pi_conversion_counts().is_err(), "{parent}");
        assert!(stream_validate(&s).is_err(), "{parent}");
    }
}

#[test]
fn stream_and_batch_agree_on_partial_and_complete_ancestry() {
    for target in [3, 4] {
        let s = session(
            vec![
                created(),
                record(2, Some(1), source(json!({"type":"session","version":3}))),
                record(
                    3,
                    Some(2),
                    source(
                        json!({"type":"model_change","id":"a","parentId":null,"provider":"p","modelId":"alpha"}),
                    ),
                ),
                record(4, Some(3), model("alpha", None)),
                record(5, Some(4), Event::Checkout { target }),
                record(
                    6,
                    Some(target),
                    source(
                        json!({"type":"thinking_level_change","id":"b","parentId":"a","thinkingLevel":"high"}),
                    ),
                ),
                record(7, Some(6), model("alpha", Some("high"))),
            ],
            &[3, 6],
        );
        assert_eq!(s.validated_pi_conversion_counts().is_ok(), target == 4);
        assert_eq!(stream_validate(&s).is_ok(), target == 4);
    }
}

#[test]
fn stream_and_batch_accept_fork_orphan_and_legacy() {
    let cases = [
        session(
            vec![
                created(),
                record(2, Some(1), source(json!({"type":"session","version":3}))),
                record(
                    3,
                    Some(2),
                    source(
                        json!({"type":"message","id":"x","parentId":"missing","message":{"role":"user","content":"x"}}),
                    ),
                ),
                record(4, Some(3), user("x")),
            ],
            &[3],
        ),
        session(
            vec![
                created(),
                record(2, Some(1), source(json!({"type":"session","version":1}))),
                record(
                    3,
                    Some(2),
                    source(json!({"type":"message","message":{"role":"user","content":"x"}})),
                ),
                record(4, Some(3), user("x")),
            ],
            &[3],
        ),
    ];
    for s in cases {
        assert!(s.validated_pi_conversion_counts().is_ok());
        assert!(stream_validate(&s).is_ok());
    }
}

#[test]
fn multi_event_conversion_rejects_branch_from_each_interior() {
    for target in [3, 4, 5] {
        let request = Uuid::new_v4();
        let s = session(
            vec![
                created(),
                record(2, Some(1), source(json!({"type":"session","version":3}))),
                record(
                    3,
                    Some(2),
                    source(
                        json!({"type":"message","id":"a","parentId":null,"message":{"role":"assistant","content":"answer"}}),
                    ),
                ),
                record(
                    4,
                    Some(3),
                    Event::ModelFinished {
                        request_id: request,
                        metadata: Default::default(),
                        usage: pi_rs_core::UsageReport::Unconfirmed,
                        failure: None,
                        partial: String::new(),
                        reply: None,
                    },
                ),
                record(
                    5,
                    Some(4),
                    Event::Assistant {
                        content: "answer".into(),
                        invocations: vec![],
                        usage: None,
                        request_id: Some(request),
                    },
                ),
                record(6, Some(5), Event::Checkout { target }),
                record(
                    7,
                    Some(target),
                    source(
                        json!({"type":"message","id":"b","parentId":"a","message":{"role":"user","content":"next"}}),
                    ),
                ),
                record(8, Some(7), user("next")),
            ],
            &[7],
        );
        let metadata = [
            (3, json!({"pi_rs_import_conversion_count_v1":2})),
            (7, json!({"pi_rs_import_conversion_count_v1":1})),
        ]
        .into_iter()
        .collect();
        let s = Session::from_records_with_metadata(s.records().to_vec(), metadata).unwrap();
        assert_eq!(
            s.validated_pi_conversion_counts().is_ok(),
            target == 5,
            "target {target}"
        );
        assert_eq!(stream_validate(&s).is_ok(), target == 5, "target {target}");
    }
}

#[test]
fn zero_event_source_is_a_complete_parent() {
    let s = Session::from_records_with_metadata(vec![
        created(),record(2,Some(1),source(json!({"type":"session","version":3}))),
        record(3,Some(2),source(json!({"type":"thinking_level_change","id":"a","parentId":null,"thinkingLevel":"high"}))),
        record(4,Some(3),source(json!({"type":"message","id":"b","parentId":"a","message":{"role":"user","content":"next"}}))),
        record(5,Some(4),user("next")),
    ],[(3,json!({"pi_rs_import_conversion_count_v1":0})),(4,json!({"pi_rs_import_conversion_count_v1":1}))].into_iter().collect()).unwrap();
    assert!(s.validated_pi_conversion_counts().is_ok());
    assert!(stream_validate(&s).is_ok());
}

#[test]
fn incremental_stream_rejects_forward_reference_when_source_arrives() {
    let s = session(
        vec![
            created(),
            record(2, Some(1), source(json!({"type":"session","version":3}))),
            record(
                3,
                Some(2),
                source(
                    json!({"type":"message","id":"a","parentId":"later","message":{"role":"user","content":"a"}}),
                ),
            ),
            record(4, Some(3), user("a")),
            record(
                5,
                Some(4),
                source(
                    json!({"type":"message","id":"later","parentId":"a","message":{"role":"user","content":"later"}}),
                ),
            ),
            record(6, Some(5), user("later")),
        ],
        &[3, 5],
    );
    let metadata = s
        .records()
        .iter()
        .filter_map(|r| s.record_metadata(r.id).cloned().map(|m| (r.id, m)))
        .collect();
    let mut verifier = pi_rs_core::pi_import::StreamVerifier::new();
    assert_eq!(
        verifier
            .advance(&s.records()[..4], &metadata, false)
            .unwrap(),
        None
    );
    assert!(verifier.advance(s.records(), &metadata, true).is_err());
    assert!(s.validated_pi_conversion_counts().is_err());
}

#[test]
fn null_or_missing_parent_requires_physical_root_for_v2_and_v3() {
    for version in [2, 3] {
        for has_null in [false, true] {
            let mut entry =
                json!({"type":"message","id":"b","message":{"role":"user","content":"two"}});
            if has_null {
                entry["parentId"] = Value::Null;
            }
            for child in [false, true] {
                let parent = if child { 4 } else { 2 };
                let s = session(
                    vec![
                        created(),
                        record(
                            2,
                            Some(1),
                            source(json!({"type":"session","version":version})),
                        ),
                        record(
                            3,
                            Some(2),
                            source(
                                json!({"type":"message","id":"a","parentId":null,"message":{"role":"user","content":"one"}}),
                            ),
                        ),
                        record(4, Some(3), user("one")),
                        record(5, Some(4), Event::Checkout { target: parent }),
                        record(6, Some(parent), source(entry.clone())),
                        record(7, Some(6), user("two")),
                    ],
                    &[3, 6],
                );
                assert_eq!(
                    s.validated_pi_conversion_counts().is_ok(),
                    !child,
                    "batch version={version} null={has_null} child={child}"
                );
                assert_eq!(
                    stream_validate(&s).is_ok(),
                    !child,
                    "stream version={version} null={has_null} child={child}"
                );
                assert_eq!(
                    s.fork_session(s.head()).is_ok(),
                    !child,
                    "fork version={version} null={has_null} child={child}"
                );
            }
        }
    }
}

#[test]
fn stream_new_and_default_handle_journal_partial_and_invalid_sources() {
    for make in [
        pi_rs_core::pi_import::StreamVerifier::new,
        pi_rs_core::pi_import::StreamVerifier::default,
    ] {
        let s = session(
            vec![
                created(),
                record(2, Some(1), source(json!({"type":"session","version":3}))),
                record(
                    3,
                    Some(2),
                    source(
                        json!({"type":"message","id":"a","parentId":null,"message":{"role":"user","content":"one"}}),
                    ),
                ),
                record(4, Some(3), user("one")),
                record(
                    5,
                    Some(4),
                    source(
                        json!({"type":"message","id":"b","parentId":"a","message":{"role":"user","content":"two"}}),
                    ),
                ),
                record(6, Some(5), user("two")),
            ],
            &[3, 5],
        );
        let metadata = s
            .records()
            .iter()
            .filter_map(|r| s.record_metadata(r.id).cloned().map(|m| (r.id, m)))
            .collect();
        let mut verifier = make();
        assert_eq!(
            verifier
                .advance(&s.records()[..3], &metadata, false)
                .unwrap(),
            Some(4)
        );
        assert!(verifier.counts().is_empty());
        assert_eq!(
            verifier.advance(s.records(), &metadata, true).unwrap(),
            None
        );
        assert_eq!(verifier.counts(), &[(3, 1), (5, 1)].into_iter().collect());
        let mut bad_records = s.records().to_vec();
        if let Event::SourceEntry { entry, .. } = &mut bad_records[4].event {
            entry["parentId"] = json!(42);
        }
        let mut verifier = make();
        let error = verifier.advance(&bad_records, &metadata, true).unwrap_err();
        assert!(error.to_string().contains("invalid imported Pi parent ID"));
    }
}

#[test]
fn stream_rejects_invalid_record_identity_without_panicking() {
    for make in [
        pi_rs_core::pi_import::StreamVerifier::new,
        pi_rs_core::pi_import::StreamVerifier::default,
    ] {
        for (id, parent) in [
            (2, Some(0)),
            (2, Some(2)),
            (2, Some(3)),
            (2, Some(u64::MAX)),
            (3, Some(1)),
            (0, Some(1)),
        ] {
            let records = vec![created(), record(id, parent, user("bad"))];
            let result = make().advance(&records, &Default::default(), true);
            assert!(result.is_err(), "id={id}, parent={parent:?}: {result:?}");
        }
        let mut verifier = make();
        assert_eq!(
            verifier
                .advance(&[created()], &Default::default(), false)
                .unwrap(),
            None
        );
        let records = vec![created(), record(2, Some(u64::MAX), user("bad"))];
        assert!(
            verifier
                .advance(&records, &Default::default(), false)
                .is_err()
        );
    }
}

#[test]
fn stream_rejects_invalid_identity_inside_staged_conversion() {
    for make in [
        pi_rs_core::pi_import::StreamVerifier::new,
        pi_rs_core::pi_import::StreamVerifier::default,
    ] {
        let mut verifier = make();
        let mut records = vec![
            created(),
            record(
                2,
                Some(1),
                source(
                    json!({"type":"message","id":"a","parentId":null,"message":{"role":"user","content":"one"}}),
                ),
            ),
        ];
        let metadata = [(2, json!({"pi_rs_import_conversion_count_v1":1}))]
            .into_iter()
            .collect();
        assert_eq!(
            verifier.advance(&records, &metadata, false).unwrap(),
            Some(3)
        );
        assert!(verifier.advance(&records[..1], &metadata, false).is_err());
        records.push(record(99, Some(2), user("one")));
        assert!(verifier.advance(&records, &metadata, false).is_err());
        assert!(verifier.counts().is_empty());
    }
}
