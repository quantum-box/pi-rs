use std::collections::BTreeMap;

use pi_rs_core::{Message, ToolDeclaration};
pub(super) use pi_rs_core::{
    ToolTranscriptMode as Mode, ToolTranscriptPlan as Plan, plan_tool_transcript as plan,
};
use serde_json::{Value, json};
pub(super) struct WireProjection {
    pub(super) top_level: Vec<Value>,
    pub(super) by_message: BTreeMap<usize, AnchoredItems>,
    top_level_grammar: BTreeMap<String, Option<String>>,
}

impl WireProjection {
    /// Resolve the raw-input property against the tool declaration in effect
    /// where the call appears in the outgoing transcript.
    pub(super) fn grammar_calls<'a>(
        &self,
        messages: &'a [Message],
    ) -> BTreeMap<(&'a str, &'a str), String> {
        let mut active = self.top_level_grammar.clone();
        let mut properties = BTreeMap::new();
        for (index, message) in messages.iter().enumerate() {
            if let Some(update) = self.by_message.get(&index) {
                active.extend(update.grammar.clone());
            }
            if let Message::Assistant { calls, .. } = message {
                for call in calls {
                    if let Some(Some(property)) = active.get(&call.name) {
                        properties.insert((call.id.as_str(), call.name.as_str()), property.clone());
                    }
                }
            }
        }
        properties
    }
}

#[derive(Default)]
pub(super) struct AnchoredItems {
    pub(super) before: Vec<Value>,
    pub(super) removal_notices: Vec<String>,
    grammar: BTreeMap<String, Option<String>>,
}

/// Render tool declarations separately from message content so the selected
/// Responses transport can insert them immediately before their system message.
pub(super) fn wire_projection(
    plan: Plan<'_>,
    mode: Mode,
    instruction_role: &str,
    leading_system: bool,
    mut encode: impl FnMut(&ToolDeclaration) -> (Value, Option<String>),
) -> WireProjection {
    let mut top_level_grammar = BTreeMap::new();
    let top_level = plan
        .top_level
        .into_iter()
        .map(|tool| {
            let (wire, property) = encode(tool);
            top_level_grammar.insert(tool.spec.name.clone(), property);
            wire
        })
        .collect();
    let mut by_message = BTreeMap::<usize, AnchoredItems>::new();
    let mut declared = false;
    let mut unavailable: Vec<&str> = Vec::new();
    for checkpoint in plan.anchored {
        let mut items = AnchoredItems::default();
        let names: Vec<&str> = checkpoint
            .added
            .iter()
            .map(|tool| tool.spec.name.as_str())
            .collect();
        unavailable.retain(|name| !names.contains(name));
        if !names.is_empty() {
            let mut tools: Vec<_> = checkpoint
                .added
                .iter()
                .map(|tool| {
                    let (wire, property) = encode(tool);
                    items.grammar.insert(tool.spec.name.clone(), property);
                    wire
                })
                .collect();
            match mode {
                Mode::TopLevel => unreachable!("top-level tools have no checkpoints"),
                Mode::Changes => {
                    if declared {
                        let mut notice = "This is an incremental tools update. Previously declared tools remain available unless explicitly marked unavailable. A tool declared again here is available again, and its latest definition replaces the earlier one.".to_owned();
                        if !unavailable.is_empty() {
                            notice.push_str(&format!(
                                "\nStill unavailable: {}.",
                                unavailable.join(", ")
                            ));
                        }
                        items
                            .before
                            .push(json!({"role":instruction_role,"content":notice}));
                    }
                    declared = true;
                    items
                        .before
                        .push(json!({"type":"additional_tools","role":"developer","tools":tools}));
                }
                Mode::Additions => {
                    let message = checkpoint
                        .message_index
                        .saturating_sub(usize::from(leading_system));
                    let call_id = format!(
                        "pi_tool_load_{}",
                        short_hash(&format!("system:{message}:{}", names.join(",")))
                    );
                    for tool in &mut tools {
                        tool["defer_loading"] = json!(true);
                    }
                    items.before.push(json!({
                        "type":"tool_search_call","call_id":call_id,"execution":"client",
                        "status":"completed","arguments":{"query":names.join(" "),"limit":names.len()}
                    }));
                    items.before.push(json!({
                        "type":"tool_search_output","call_id":call_id,"execution":"client",
                        "status":"completed","tools":tools
                    }));
                }
            }
        }
        if mode == Mode::Changes {
            let removed: Vec<&str> = checkpoint
                .removed
                .iter()
                .map(String::as_str)
                .filter(|name| !names.contains(name))
                .collect();
            if !removed.is_empty() {
                let mut notice =
                    "The following tools are no longer available. Do not call them:".to_owned();
                for name in removed {
                    notice.push_str("\n- ");
                    notice.push_str(name);
                    if !unavailable.contains(&name) {
                        unavailable.push(name);
                    }
                }
                items.removal_notices.push(notice);
            }
        }
        if !items.before.is_empty() || !items.removal_notices.is_empty() {
            let anchored = by_message.entry(checkpoint.message_index).or_default();
            // A tool batch can move multiple checkpoints to the same wire position.
            if !items.before.is_empty() && !anchored.removal_notices.is_empty() {
                let notices = std::mem::take(&mut anchored.removal_notices);
                anchored.before.push(json!({
                    "role":instruction_role,
                    "content":notices.join("\n\n")
                }));
            }
            anchored.before.extend(items.before);
            anchored.removal_notices.extend(items.removal_notices);
            anchored.grammar.extend(items.grammar);
        }
    }
    WireProjection {
        top_level,
        by_message,
        top_level_grammar,
    }
}

fn short_hash(value: &str) -> String {
    let mut high = 0x41c6ce57_u32;
    let mut low = 0xdeadbeef_u32;
    for unit in value.encode_utf16() {
        low = (low ^ u32::from(unit)).wrapping_mul(2654435761);
        high = (high ^ u32::from(unit)).wrapping_mul(1597334677);
    }
    low = (low ^ (low >> 16)).wrapping_mul(2246822507)
        ^ (high ^ (high >> 13)).wrapping_mul(3266489909);
    high = (high ^ (high >> 16)).wrapping_mul(2246822507)
        ^ (low ^ (low >> 13)).wrapping_mul(3266489909);
    format!("{}{}", base36(high), base36(low))
}

fn base36(mut value: u32) -> String {
    let mut digits = Vec::new();
    loop {
        let digit = value % 36;
        digits.push(char::from_digit(digit, 36).expect("digit is below 36"));
        value /= 36;
        if value == 0 {
            return digits.into_iter().rev().collect();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pi_rs_core::ToolTranscriptChange as Checkpoint;

    fn tool(name: &str, revision: &str) -> ToolDeclaration {
        ToolDeclaration {
            spec: pi_rs_core::ToolSpec {
                name: name.into(),
                description: revision.into(),
                parameters: serde_json::json!({"type": "object"}),
            },
            constrained_sampling: None,
        }
    }

    #[test]
    fn search_folds_removals_and_redefinitions_and_anchors_later_additions() {
        let checkpoints = [
            Checkpoint {
                message_index: 0,
                added: vec![tool("updated", "original"), tool("removed", "original")],
                removed: vec![],
            },
            Checkpoint {
                message_index: 2,
                added: vec![tool("introduced", "original")],
                removed: vec!["removed".into()],
            },
            Checkpoint {
                message_index: 4,
                added: vec![tool("updated", "replacement")],
                removed: vec![],
            },
            Checkpoint {
                message_index: 6,
                added: vec![tool("incremental", "original")],
                removed: vec![],
            },
        ];
        let active = [
            tool("updated", "replacement"),
            tool("introduced", "original"),
            tool("incremental", "original"),
        ];

        let search = plan(
            Mode::from_capabilities(true, false, true),
            &checkpoints,
            &active,
        );
        assert_eq!(search.top_level, [&active[0], &active[1]]);
        assert_eq!(search.anchored.len(), 1);
        assert_eq!(search.anchored[0].message_index, 6);

        let changes = plan(
            Mode::from_capabilities(true, true, true),
            &checkpoints,
            &active,
        );
        assert!(changes.top_level.is_empty());
        assert_eq!(changes.anchored.len(), checkpoints.len());

        let fallback = plan(
            Mode::from_capabilities(false, true, false),
            &checkpoints,
            &active,
        );
        assert_eq!(fallback.top_level, active.iter().collect::<Vec<_>>());
        assert!(fallback.anchored.is_empty());
    }

    #[test]
    fn responses_keeps_removals_before_redeclarations_at_a_shared_anchor() {
        let restored = tool("read", "restored");
        let checkpoints = [
            Checkpoint {
                message_index: 0,
                added: vec![tool("read", "original")],
                removed: vec![],
            },
            Checkpoint {
                message_index: 2,
                added: vec![],
                removed: vec!["read".into()],
            },
            Checkpoint {
                message_index: 2,
                added: vec![restored.clone()],
                removed: vec![],
            },
        ];
        let request = pi_rs_core::ModelRequest {
            model: "fixture".into(),
            messages: vec![
                Message::System {
                    content: "base".into(),
                },
                Message::User {
                    content: "continue".into(),
                },
                Message::System {
                    content: "checkpoint".into(),
                },
            ],
            tools: vec![restored.spec],
            max_output_tokens: 80,
        };
        let client =
            super::super::OpenAiResponses::new_api("http://localhost/v1", "fixture".into())
                .unwrap()
                .with_mid_conversation_system_policy(true, BTreeMap::new())
                .with_transcript_tools_policy((true, false), BTreeMap::new())
                .with_tool_transcript(&request, &checkpoints);
        let payload = client
            .transcript_payload(&request, &super::super::ResponsesTools::default(), "system")
            .unwrap()
            .unwrap();
        let input = payload["input"].as_array().unwrap();
        assert_eq!(input.len(), 7, "{input:?}");
        assert_eq!(
            input[3]["content"],
            "The following tools are no longer available. Do not call them:\n- read"
        );
        assert!(
            !input[4]["content"]
                .as_str()
                .unwrap()
                .contains("Still unavailable"),
            "{input:?}"
        );
        assert_eq!(input[5]["type"], "additional_tools");
        assert_eq!(input[5]["tools"][0]["description"], "restored");
        assert_eq!(input[6]["content"], "checkpoint");
    }

    #[test]
    fn wire_uses_additional_tools_or_deferred_search_items_at_the_anchor() {
        let checkpoints = [
            Checkpoint {
                message_index: 0,
                added: vec![tool("initial", "original"), tool("removed", "original")],
                removed: vec![],
            },
            Checkpoint {
                message_index: 2,
                added: vec![tool("added", "original")],
                removed: vec!["removed".into()],
            },
            Checkpoint {
                message_index: 4,
                added: vec![tool("last", "original")],
                removed: vec![],
            },
        ];
        let active = [
            tool("initial", "original"),
            tool("added", "original"),
            tool("last", "original"),
        ];
        let encode =
            |tool: &ToolDeclaration| (super::super::unconstrained_wire_tool(&tool.spec), None);
        let changes = wire_projection(
            plan(Mode::Changes, &checkpoints, &active),
            Mode::Changes,
            "developer",
            true,
            encode,
        );
        assert!(changes.top_level.is_empty());
        assert_eq!(changes.by_message[&0].before[0]["type"], "additional_tools");
        assert_eq!(
            changes.by_message[&2].removal_notices[0],
            "The following tools are no longer available. Do not call them:\n- removed"
        );
        assert!(
            changes.by_message[&4].before[0]["content"]
                .as_str()
                .unwrap()
                .ends_with("Still unavailable: removed.")
        );
        let search = wire_projection(
            plan(Mode::Additions, &checkpoints, &active),
            Mode::Additions,
            "developer",
            true,
            encode,
        );
        assert_eq!(
            search
                .top_level
                .iter()
                .map(|tool| tool["name"].as_str().unwrap())
                .collect::<Vec<_>>(),
            ["initial", "added"]
        );
        assert_eq!(search.by_message.len(), 1);
        let added = &search.by_message[&4].before;
        assert_eq!(added[0]["type"], "tool_search_call");
        assert_eq!(added[0]["arguments"], json!({"query":"last","limit":1}));
        assert_eq!(added[0]["call_id"], "pi_tool_load_1s99ayp14uubvf");
        assert_eq!(added[0]["call_id"], added[1]["call_id"]);
        assert_eq!(added[1]["tools"][0]["defer_loading"], true);
    }

    #[test]
    fn responses_encoder_inserts_tool_frames_before_their_system_message() {
        use pi_rs_core::{Message, ModelApi, ModelRequest};

        let first = tool("initial", "original");
        let second = tool("added", "original");
        let request = ModelRequest {
            model: "fixture".into(),
            messages: vec![
                Message::System {
                    content: "initial instructions".into(),
                },
                Message::User {
                    content: "prompt".into(),
                },
                Message::System {
                    content: "updated instructions".into(),
                },
            ],
            tools: vec![first.spec.clone(), second.spec.clone()],
            max_output_tokens: 32,
        };
        let checkpoints = [
            Checkpoint {
                message_index: 0,
                added: vec![first, tool("removed", "original")],
                removed: vec![],
            },
            Checkpoint {
                message_index: 2,
                added: vec![second],
                removed: vec!["removed".into()],
            },
        ];
        let projection = wire_projection(
            plan(Mode::Changes, &checkpoints, &[]),
            Mode::Changes,
            "developer",
            true,
            |tool| (super::super::unconstrained_wire_tool(&tool.spec), None),
        );
        let body = super::super::responses_payload(
            &request,
            "openai",
            "",
            "developer",
            ModelApi::OpenaiResponses,
            super::super::ResponsesProjection {
                supports_mid_conversation_system: true,
                tools: Some(projection),
                ..Default::default()
            },
        );
        assert!(body.get("tools").is_none());
        let items = body["input"].as_array().unwrap();
        assert_eq!(items[0]["tools"][0]["name"], "initial");
        assert_eq!(items[1]["content"], "initial instructions");
        assert_eq!(items[2]["content"], "prompt");
        assert_eq!(items[4]["tools"][0]["name"], "added");
        assert_eq!(
            items[5]["content"],
            "updated instructions\n\nThe following tools are no longer available. Do not call them:\n- removed"
        );
    }

    #[test]
    fn historical_grammar_keeps_its_argument_property_when_the_tool_changes() {
        use pi_rs_core::{Model, ModelRequest, ToolCall, ToolConstraint};
        let declaration = |property: &str| {
            let mut tool = tool("read", "read a file");
            tool.spec.parameters = json!({
                "type": "object",
                "properties": {(property): {"type": "string"}},
                "required": [property],
            });
            tool.constrained_sampling = Some(ToolConstraint::Grammar {
                variants: pi_rs_core::ToolGrammarVariants {
                    openai_regex: Some(".+".into()),
                    ..Default::default()
                },
            });
            tool
        };
        let old = declaration("path");
        let new = declaration("query");
        let request = ModelRequest {
            model: "fixture".into(),
            messages: vec![
                Message::System {
                    content: "initial".into(),
                },
                Message::Assistant {
                    content: String::new(),
                    calls: vec![ToolCall {
                        name: "read".into(),
                        id: "first".into(),
                        arguments: json!({"path":"old.json"}).to_string(),
                    }],
                    replay: None,
                },
                Message::Tool {
                    call_id: "first".into(),
                    content: "old result".into(),
                    is_structured: Some(false),
                    is_error: false,
                },
                Message::Assistant {
                    content: String::new(),
                    calls: vec![ToolCall {
                        name: "read".into(),
                        id: "second".into(),
                        arguments: json!({"query":"new.json"}).to_string(),
                    }],
                    replay: None,
                },
            ],
            tools: vec![new.spec.clone()],
            max_output_tokens: 80,
        };
        let checkpoints = [
            Checkpoint {
                message_index: 0,
                added: vec![old],
                removed: vec![],
            },
            Checkpoint {
                message_index: 3,
                added: vec![new],
                removed: vec![],
            },
        ];
        let model = super::super::OpenAiResponses::new_api("http://localhost/v1", "fixture".into())
            .unwrap()
            .with_grammar_tool_support(true)
            .with_mid_conversation_system_policy(true, BTreeMap::new())
            .with_transcript_tools_policy((true, false), BTreeMap::new())
            .with_tool_transcript(&request, &checkpoints);
        let projection = model
            .tool_projection(&request, &super::super::ResponsesTools::default(), "system")
            .unwrap();
        let properties = projection.grammar_calls(&request.messages);
        assert_eq!(properties[&("first", "read")], "path");
        assert_eq!(properties[&("second", "read")], "query");
        let body = model
            .transcript_payload(&request, &super::super::ResponsesTools::default(), "system")
            .unwrap()
            .unwrap();
        let input = body["input"].as_array().unwrap();
        let tools: Vec<_> = input
            .iter()
            .filter(|item| item["type"] == "additional_tools")
            .collect();
        assert_eq!(tools.len(), 2);
        assert!(
            tools
                .iter()
                .all(|item| item["tools"][0]["type"] == "custom")
        );
        let calls: Vec<_> = input
            .iter()
            .filter(|item| item["type"] == "custom_tool_call")
            .map(|item| item["input"].as_str().unwrap())
            .collect();
        assert_eq!(calls, ["old.json", "new.json"]);
        assert_eq!(
            model.estimate_request_tokens(&request),
            8 + pi_rs_core::estimate_replay_tokens(input),
        );
        assert!(
            input
                .iter()
                .any(|item| item["type"] == "custom_tool_call_output")
        );
    }

    #[test]
    fn azure_tool_transcript_replays_only_from_the_active_deployment() {
        use crate::{OpenAiResponses, ResponsesTools};
        use pi_rs_core::{ModelRequest, ProviderReplay, ResolvedModelIdentity};

        let azure = OpenAiResponses::new_azure(
            "http://localhost/openai/v1/responses",
            "fixture".into(),
            "azure-openai",
        )
        .unwrap()
        .with_mid_conversation_system_policy(true, BTreeMap::new())
        .with_transcript_tools_policy((true, false), BTreeMap::new());
        let route = |deployment: &str| {
            azure.clone().with_azure_routes(
                "preview",
                &BTreeMap::from([("fixture".into(), deployment.into())]),
                None,
            )
        };
        let declaration = tool("read", "initial");
        let original = route("original");
        let request = ModelRequest {
            model: "fixture".into(),
            messages: vec![Message::Assistant {
                content: "portable answer".into(),
                calls: vec![],
                replay: Some(Box::new(ProviderReplay {
                    identity: ResolvedModelIdentity {
                        provider: original.replay_provider("fixture"),
                        model: "fixture".into(),
                    },
                    blocks: vec![json!({"type":"reasoning","encrypted_content":"original-replay"})],
                })),
            }],
            tools: vec![declaration.spec.clone()],
            max_output_tokens: 64,
        };
        let checkpoints = [Checkpoint {
            message_index: 0,
            added: vec![declaration],
            removed: vec![],
        }];
        for (client, matches) in [(original, true), (route("other"), false)] {
            let body = client
                .with_tool_transcript(&request, &checkpoints)
                .transcript_payload(&request, &ResponsesTools::default(), "system")
                .unwrap()
                .unwrap();
            assert_eq!(body.to_string().contains("original-replay"), matches);
            assert_eq!(body.to_string().contains("portable answer"), !matches);
        }
    }

    #[test]
    fn historical_required_schema_stays_unconstrained_on_an_implicit_strict_route() {
        use pi_rs_core::{ModelRequest, StrictToolPolicy, ToolConstraint};
        let mut previous = tool("read", "historical read");
        previous.spec.parameters = json!({"type":"object","additionalProperties":true});
        previous.constrained_sampling = Some(ToolConstraint::JsonSchema {
            strict: StrictToolPolicy::Require,
        });
        let request = ModelRequest {
            model: "fixture".into(),
            max_output_tokens: 64,
            messages: vec![Message::User {
                content: "question".into(),
            }],
            tools: vec![],
        };
        let checkpoints = [Checkpoint {
            message_index: 0,
            added: vec![previous],
            removed: vec![],
        }];
        let mut model = super::super::OpenAiResponses::new_api(
            "https://api.openai.com/v1/responses",
            "fixture".into(),
        )
        .unwrap()
        .with_strict_tool_support(false)
        .with_mid_conversation_system_policy(true, BTreeMap::new())
        .with_transcript_tools_policy((true, false), BTreeMap::new())
        .with_tool_transcript(&request, &checkpoints);
        model.codex_account = Some("fixture".into());
        let body = model
            .transcript_payload(&request, &super::super::ResponsesTools::default(), "system")
            .unwrap()
            .unwrap();
        assert_eq!(body["input"][0]["tools"][0]["strict"], false);
    }

    async fn capture_native_tool_requests(
        additional_tools: bool,
        system_updates: bool,
    ) -> Vec<Value> {
        use crate::OpenAiResponses;
        use pi_rs_core::{
            Agent, CancellationToken, Extension, Limits, MemoryStore, Outcome, Registry, Session,
            Tool, ToolContext, ToolSpec,
        };
        use std::sync::Arc;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        struct FixtureTool(&'static str);
        #[async_trait::async_trait]
        impl Tool for FixtureTool {
            fn spec(&self) -> ToolSpec {
                tool(self.0, "fixture").spec
            }
            fn constrained_sampling(&self) -> Option<pi_rs_core::ToolConstraint> {
                (self.0 == "write").then_some(pi_rs_core::ToolConstraint::JsonSchema {
                    strict: pi_rs_core::StrictToolPolicy::Require,
                })
            }
            async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
                Outcome::Succeeded(String::new())
            }
        }
        struct FixtureTools;
        impl Extension for FixtureTools {
            fn id(&self) -> &str {
                "fixture-tools"
            }
            fn tools(&self) -> Vec<Arc<dyn Tool>> {
                vec![
                    Arc::new(FixtureTool("read")),
                    Arc::new(FixtureTool("write")),
                ]
            }
        }

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/responses", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let mut captured = Vec::<Value>::new();
            for _ in 0..3 {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut chunks = Vec::new();
                let mut buffer = [0; 4096];
                let offset = loop {
                    let count = socket.read(&mut buffer).await.unwrap();
                    assert!(count > 0);
                    chunks.extend_from_slice(&buffer[..count]);
                    if let Some(offset) = chunks.windows(4).position(|part| part == b"\r\n\r\n") {
                        let offset = offset + 4;
                        let headers = std::str::from_utf8(&chunks[..offset])
                            .unwrap()
                            .to_ascii_lowercase();
                        let length = headers
                            .lines()
                            .find_map(|line| {
                                line.strip_prefix("content-length: ")?.parse::<usize>().ok()
                            })
                            .unwrap();
                        if chunks.len() >= offset + length {
                            break offset;
                        }
                    }
                };
                captured.push(serde_json::from_slice(&chunks[offset..]).unwrap());
                let complete = json!({"type":"response.completed","response":{
                    "status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"done"}]}],
                    "usage":{"input_tokens":3,"output_tokens":1}
                }});
                let events = format!(
                    "data: {}\n\ndata: {complete}\n\n",
                    json!({"type":"response.output_text.delta","delta":"done"})
                );
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{events}",
                    events.len()
                );
                socket.write_all(response.as_bytes()).await.unwrap();
            }
            captured
        });
        let model = OpenAiResponses::new_api(&endpoint, "sk-fixture".into())
            .unwrap()
            .with_developer_role(true)
            .with_mid_conversation_system_policy(true, BTreeMap::new())
            .with_strict_tool_support(true)
            .with_transcript_tools_policy((additional_tools, true), BTreeMap::new());
        let mut registry = Registry::default();
        registry.register(Arc::new(FixtureTools)).unwrap();
        let mut session = Session::with_store(MemoryStore::default()).unwrap();
        let (_tx, mut followups) = tokio::sync::mpsc::channel::<String>(1);

        for (prompt, enabled) in [
            ("first", vec!["read"]),
            ("second", vec!["read", "write"]),
            ("third", vec!["write"]),
        ] {
            registry
                .set_active_tools(enabled.iter().map(|name| (*name).to_owned()))
                .unwrap();
            if system_updates {
                let mut records = session.records().to_vec();
                let metadata = records
                    .iter()
                    .filter_map(|record| {
                        session
                            .record_metadata(record.id)
                            .cloned()
                            .map(|value| (record.id, value))
                    })
                    .collect();
                let previous = records.last().unwrap();
                records.push(pi_rs_core::Record {
                    timestamp: None,
                    run_id: None,
                    version: previous.version,
                    id: previous.id + 1,
                    parent: Some(session.head()),
                    event: pi_rs_core::Event::SystemCheckpoint {
                        content: format!("Instructions for {prompt}"),
                        sections: vec![],
                        tools_added: match prompt {
                            "first" => vec![tool("read", "fixture").spec],
                            "second" => vec![tool("write", "fixture").spec],
                            _ => vec![],
                        },
                        tools_removed: if prompt == "third" {
                            vec!["read".into()]
                        } else {
                            vec![]
                        },
                    },
                });
                session = Session::from_records_with_metadata(records, metadata).unwrap();
            }
            let agent = Agent {
                model: &model,
                registry: &registry,
                model_name: "fixture",
                system_prompt: "instructions",
                limits: Limits::default(),
            };
            assert_eq!(
                agent
                    .run(
                        &mut session,
                        Some(prompt.into()),
                        &mut followups,
                        &CancellationToken::new(),
                        &mut |_| {}
                    )
                    .await
                    .unwrap(),
                "done"
            );
        }

        server.await.unwrap()
    }

    #[tokio::test]
    async fn native_session_sends_tool_additions_and_removals_across_turns() {
        let captured = capture_native_tool_requests(true, false).await;
        assert!(
            captured
                .iter()
                .all(|request| request.get("tools").is_none())
        );
        for request in &captured {
            assert_eq!(request["input"][0]["type"], "additional_tools");
            assert_eq!(request["input"][0]["tools"][0]["name"], "read");
        }
        let additions = captured[2]["input"].as_array().unwrap();
        assert!(
            additions
                .iter()
                .any(|item| item["type"] == "additional_tools"
                    && item["tools"][0]["name"] == "write"
                    && item["tools"][0]["strict"] == true)
        );
        assert_eq!(
            additions.last().unwrap()["content"],
            "The following tools are no longer available. Do not call them:\n- read"
        );
    }

    #[tokio::test]
    async fn native_session_encodes_search_additions_with_strict_schema_and_folds_removals() {
        let captured = capture_native_tool_requests(false, false).await;
        assert_eq!(captured[0]["tools"][0]["name"], "read");
        assert_eq!(captured[1]["tools"][0]["name"], "read");
        let discovered = captured[1]["input"]
            .as_array()
            .unwrap()
            .iter()
            .find(|item| item["type"] == "tool_search_output")
            .unwrap();
        assert_eq!(discovered["tools"][0]["name"], "write");
        assert_eq!(discovered["tools"][0]["strict"], true);
        assert_eq!(discovered["tools"][0]["defer_loading"], true);
        assert_eq!(captured[2]["tools"].as_array().unwrap().len(), 1);
        assert_eq!(captured[2]["tools"][0]["name"], "write");
        assert_eq!(captured[2]["tools"][0]["strict"], true);
        assert!(
            captured[2]["input"]
                .as_array()
                .unwrap()
                .iter()
                .all(|item| item["type"] != "tool_search_output")
        );
    }

    #[tokio::test]
    async fn native_session_anchors_tool_changes_next_to_their_system_updates() {
        let captured = capture_native_tool_requests(true, true).await;
        let input = captured[2]["input"].as_array().unwrap();
        let added_at = input
            .iter()
            .position(|item| {
                item["type"] == "additional_tools" && item["tools"][0]["name"] == "write"
            })
            .unwrap();
        assert_eq!(input[added_at + 1]["content"], "Instructions for second");
        assert_eq!(input[added_at + 2]["content"], "second");
        let removed_at = input
            .iter()
            .position(|item| {
                item["content"].as_str().is_some_and(|text| {
                    text.contains("The following tools are no longer available")
                        && text.contains("- read")
                })
            })
            .unwrap();
        assert!(
            input[removed_at]["content"]
                .as_str()
                .unwrap()
                .starts_with("Instructions for third")
        );
        assert_eq!(input[removed_at + 1]["content"], "third");
    }

    #[test]
    fn previously_removed_name_counts_as_redeclaration() {
        let checkpoints = [
            Checkpoint {
                message_index: 0,
                added: vec![tool("repeat", "old")],
                removed: vec![],
            },
            Checkpoint {
                message_index: 2,
                added: vec![],
                removed: vec!["repeat".into()],
            },
            Checkpoint {
                message_index: 4,
                added: vec![tool("repeat", "new")],
                removed: vec![],
            },
        ];
        let active = [tool("repeat", "new")];
        let search = plan(Mode::Additions, &checkpoints, &active);
        assert_eq!(search.top_level, [&active[0]]);
        assert!(search.anchored.is_empty());
    }
}
