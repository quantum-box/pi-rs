use pi_rs_tool_arguments::{StreamingJson, parse_tool_arguments};
use serde_json::json;

#[test]
fn incremental_snapshots_are_independent_and_final_arguments_are_validated() {
    let mut parser = StreamingJson::default();
    let first = parser.append("{\"path\":\"日本");
    let second = parser.append("語\",\"limit\":2}");
    assert_eq!(first, json!({"path":"日本"}));
    assert_eq!(second, json!({"path":"日本語","limit":2}));
    assert_eq!(
        parse_tool_arguments("{\"path\":\"日本語\",\"limit\":2}").unwrap(),
        second
    );
    assert!(parse_tool_arguments("{\"path\":\"日本").is_err());
}
