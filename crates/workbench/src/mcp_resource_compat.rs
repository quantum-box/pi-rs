//! Accept resource listings that omit a display name, as upstream Pi does.

use std::future::Future;

use rmcp::{
    RoleClient,
    model::{ClientJsonRpcMessage, ServerJsonRpcMessage, ServerResult},
    transport::Transport,
};
use serde::Deserialize;
use serde_json::Value;

pub(super) struct ResourceCompatibility<T>(pub(super) T);

impl<T: Transport<RoleClient>> Transport<RoleClient> for ResourceCompatibility<T> {
    type Error = T::Error;

    fn send(
        &mut self,
        message: ClientJsonRpcMessage,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'static {
        self.0.send(message)
    }

    async fn receive(&mut self) -> Option<ServerJsonRpcMessage> {
        let mut message = self.0.receive().await?;
        normalize_response(&mut message);
        Some(message)
    }

    async fn close(&mut self) -> Result<(), Self::Error> {
        self.0.close().await
    }
}

fn normalize_response(message: &mut ServerJsonRpcMessage) {
    // rmcp keeps replies without the required name as a CustomResult. Repair
    // them before its usual typed listing, caching, and cancellation handling.
    if let ServerJsonRpcMessage::Response(reply) = message
        && let ServerResult::CustomResult(raw) = &mut reply.result
        && let Some(result) = normalize_listing(&mut raw.0)
    {
        reply.result = result;
    }
}

fn normalize_listing(raw: &mut Value) -> Option<ServerResult> {
    let (collection, uri) = match (raw.get("resources"), raw.get("resourceTemplates")) {
        (Some(_), None) => ("resources", "uri"),
        (None, Some(_)) => ("resourceTemplates", "uriTemplate"),
        _ => return None,
    };
    let mut changed = false;
    for item in raw.get_mut(collection)?.as_array_mut()? {
        if let Some(item) = item.as_object_mut()
            && !item.contains_key("name")
            && let Some(name) = item.get(uri).and_then(Value::as_str)
        {
            item.insert("name".into(), Value::String(name.to_owned()));
            changed = true;
        }
    }
    if !changed {
        return None;
    }
    match collection {
        "resources" => rmcp::model::ListResourcesResult::deserialize(&*raw)
            .ok()
            .map(ServerResult::ListResourcesResult),
        _ => rmcp::model::ListResourceTemplatesResult::deserialize(&*raw)
            .ok()
            .map(ServerResult::ListResourceTemplatesResult),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn recognizes_only_omitted_resource_and_template_names_as_uris() {
        for (collection, uri) in [("resources", "uri"), ("resourceTemplates", "uriTemplate")] {
            let wrap = |rows: Value| {
                serde_json::from_value::<ServerJsonRpcMessage>(json!({
                    "jsonrpc": "2.0", "id": 7,
                    "result": { "resultType": "complete", collection: rows, "nextCursor": "opaque" }
                }))
                .unwrap()
            };

            let mut message =
                wrap(json!([{uri:"file:///first"}, {uri:"file:///second", "name":""}]));
            normalize_response(&mut message);
            let ServerJsonRpcMessage::Response(reply) = message else {
                panic!("expected response");
            };
            match collection {
                "resources" => {
                    assert!(matches!(reply.result, ServerResult::ListResourcesResult(_)))
                }
                _ => assert!(matches!(
                    reply.result,
                    ServerResult::ListResourceTemplatesResult(_)
                )),
            }
            let listing = serde_json::to_value(&reply.result).unwrap();
            assert_eq!(listing[collection][0]["name"], "file:///first");
            assert_eq!(listing[collection][1]["name"], "");
            assert_eq!(listing["nextCursor"], "opaque");

            for invalid in [Value::Null, json!(4)] {
                let mut message =
                    wrap(json!([{uri:"file:///first"}, {uri:"file:///second", "name":invalid}]));
                normalize_response(&mut message);
                assert!(matches!(
                    message,
                    ServerJsonRpcMessage::Response(reply)
                        if matches!(reply.result, ServerResult::CustomResult(_))
                ));
            }
        }
    }
}
