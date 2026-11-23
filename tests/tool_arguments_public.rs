use pi_rs::{StreamingJson, parse_tool_arguments};
use serde_json::json;

#[test]
fn root_exports_share_the_portable_tool_argument_contract() {
    let mut preview = StreamingJson::default();
    assert_eq!(preview.append("{\"path\":\"ab"), json!({"path":"ab"}));
    assert_eq!(preview.append("c\"}"), json!({"path":"abc"}));
    assert_eq!(
        parse_tool_arguments("{\"path\":\"abc\"}").unwrap(),
        json!({"path":"abc"})
    );
    assert!(parse_tool_arguments("{\"path\":\"ab").is_err());
}
