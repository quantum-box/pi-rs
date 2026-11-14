use serde_json::Value;

#[test]
fn direct_minimax_catalog_withholds_unverified_m27_routes() {
    let catalog: Value =
        serde_json::from_str(include_str!("../assets/builtin-chat-models.json")).unwrap();
    for provider in ["minimax", "minimax-cn"] {
        let models = catalog[provider].as_array().unwrap();
        let contains = |id: &str| models.iter().any(|model| model["id"] == id);
        assert!(contains("MiniMax-M3"));
        assert!(contains("MiniMax-M3.1-Flash"));
        assert!(!contains("MiniMax-M2.7"));
        assert!(!contains("MiniMax-M2.7-highspeed"));
    }
}
