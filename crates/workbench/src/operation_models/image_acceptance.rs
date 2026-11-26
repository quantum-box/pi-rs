use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use clap::Parser;
use pi_rs::{CancellationToken, Extension, ExtensionResult, ToolHost, ToolReply};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;

use super::*;

const WAIT: Duration = Duration::from_secs(10);
const IMAGE: &str =
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jCUcAAAAASUVORK5CYII=";
fn generate(options: Option<&str>) -> String {
    let options = options.map_or_else(String::new, |options| format!(", {options}"));
    format!(
        r#"
const result = await models.generateImages(
    {{provider:"relay", id:"image-fixture", baseUrl:"http://127.0.0.1:1", headers:{{authorization:"wrong"}}}},
    {{input:[{{type:"text",text:"private fixture prompt"}}]}}{options}
);
"#
    )
}

struct ImageHost {
    operations: Operations,
    progress: Mutex<Vec<Value>>,
}

impl ImageHost {
    fn new(address: SocketAddr) -> Arc<Self> {
        let mut args = Cli::parse_from(["pi", "--offline"]);
        args.model_profiles = super::super::model_profiles::Profiles::parse(&json!({
            "providers":{"relay":{"baseUrl":format!("http://{address}/v1"),"apiKey":"fixture-key",
                "models":[{"type":"image","id":"image-fixture","api":"openrouter-images",
                    "input":["text"],"output":["image","text"]}]}}
        }))
        .unwrap();
        let client = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap();
        Arc::new(Self {
            operations: Operations::with_client(args, client),
            progress: Mutex::default(),
        })
    }

    async fn run(self: &Arc<Self>, code: String, cancellation: CancellationToken) -> ToolReply {
        let tool = pi_rs_extensions::codemode::Codemode::default()
            .tools()
            .remove(0);
        timeout(
            WAIT,
            tool.invoke_transaction(
                json!({"code":code}),
                ToolContext {
                    operation_id: uuid::Uuid::new_v4(),
                    cancellation,
                },
                None,
                self.clone(),
            ),
        )
        .await
        .expect("Codemode did not settle")
    }

    fn assert_progress(&self, status: &str) {
        let updates = self.progress.lock().unwrap();
        let calls = updates
            .iter()
            .flat_map(|update| update["details"]["calls"].as_array().unwrap().iter());
        for call in calls {
            assert_eq!(call["name"], "models.generateImages");
            assert_eq!(call["args"], "relay/image-fixture");
        }
        let running = updates
            .iter()
            .map(|update| &update["details"]["calls"][0])
            .find(|call| call["status"] == "running")
            .expect("image call never reported running");
        let settled = &updates.last().unwrap()["details"]["calls"][0];
        assert_eq!(settled["id"], running["id"]);
        assert_eq!(settled["status"], status);
    }
}

#[async_trait::async_trait]
impl ToolHost for ImageHost {
    fn tools(&self) -> Vec<ToolSpec> {
        vec![self.operations.spec()]
    }

    fn supports_progress(&self) -> bool {
        true
    }

    async fn report_progress(&self, snapshot: Value) -> ExtensionResult<()> {
        self.progress.lock().unwrap().push(snapshot);
        Ok(())
    }

    async fn call(&self, name: &str, arguments: Value) -> Outcome {
        self.call_with_cancellation(name, arguments, CancellationToken::new())
            .await
    }

    async fn call_with_cancellation(
        &self,
        name: &str,
        arguments: Value,
        cancellation: CancellationToken,
    ) -> Outcome {
        assert_eq!(name, TOOL);
        self.operations
            .invoke(
                arguments,
                ToolContext {
                    operation_id: uuid::Uuid::new_v4(),
                    cancellation,
                },
            )
            .await
    }
}

async fn receive(listener: &TcpListener, image_config: Option<Value>) -> TcpStream {
    let (mut socket, _) = timeout(WAIT, listener.accept()).await.unwrap().unwrap();
    let mut reader = BufReader::new(&mut socket);
    let mut line = String::new();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line, "POST /v1/chat/completions HTTP/1.1\r\n");
    let mut length = None;
    let mut authorization = None;
    loop {
        line.clear();
        assert_ne!(reader.read_line(&mut line).await.unwrap(), 0);
        if line == "\r\n" {
            break;
        }
        let (name, value) = line.split_once(':').unwrap();
        if name.eq_ignore_ascii_case("content-length") {
            length = Some(value.trim().parse().unwrap());
        }
        if name.eq_ignore_ascii_case("authorization") {
            authorization = Some(value.trim().to_owned());
        }
    }
    let mut body = vec![0; length.unwrap()];
    reader.read_exact(&mut body).await.unwrap();
    assert_eq!(authorization.as_deref(), Some("Bearer fixture-key"));
    let mut expected = json!({
        "model":"image-fixture","stream":false,"modalities":["image","text"],
        "messages":[{"role":"user","content":[{"type":"text","text":"private fixture prompt"}]}]
    });
    if let Some(image_config) = image_config {
        expected["image_config"] = image_config;
    }
    assert_eq!(serde_json::from_slice::<Value>(&body).unwrap(), expected);
    socket
}

async fn fixture() -> (TcpListener, Arc<ImageHost>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let host = ImageHost::new(listener.local_addr().unwrap());
    (listener, host)
}

#[tokio::test]
async fn codemode_openrouter_image_loopback_acceptance() {
    const CHILD: &str = "PI_RS_CODEMODE_IMAGE_ACCEPTANCE_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let home = tempfile::tempdir().unwrap();
        let result = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "operation_models::image_acceptance::codemode_openrouter_image_loopback_acceptance",
            ])
            .env(CHILD, "1")
            .env("PI_RS_HOME", home.path())
            .output()
            .unwrap();
        assert!(
            result.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&result.stdout),
            String::from_utf8_lossy(&result.stderr)
        );
        return;
    }

    let rejected_value = "HTTP/1.1 400 Fixture\r\ncontent-length: 0\r\nconnection: close\r\n\r\n";
    for (options, image_config, response, expected) in [
        (
            None,
            None,
            concat!(
                "HTTP/1.1 429 Fixture\r\nx-should-retry: true\r\n",
                "retry-after-ms: 0\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"
            )
            .into(),
            "HTTP 429",
        ),
        (
            Some(r#"{aspectRatio:" 4:3 "}"#),
            Some(json!({"aspect_ratio":"4:3"})),
            format!(
                "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                RESPONSE_LIMIT + 1
            ),
            "exceeds the size limit",
        ),
        (
            Some(r#"{size:" 1024x1536 "}"#),
            Some(json!({"image_size":"1024x1536"})),
            rejected_value.into(),
            "HTTP 400",
        ),
        (Some("null"), None, rejected_value.into(), "HTTP 400"),
        (
            Some("{size:undefined}"),
            None,
            rejected_value.into(),
            "HTTP 400",
        ),
    ] {
        let (listener, host) = fixture().await;
        let server = tokio::spawn(async move {
            let mut socket = receive(&listener, image_config).await;
            socket.write_all(response.as_bytes()).await.unwrap();
            drop(socket);
            assert!(
                timeout(Duration::from_millis(200), listener.accept())
                    .await
                    .is_err()
            );
        });
        let generate = generate(options);
        let reply = host
            .run(
                format!("{generate} text(result.stopReason + ': ' + result.errorMessage);"),
                Default::default(),
            )
            .await;
        assert!(!reply.outcome.is_error(), "{:?}", reply.outcome);
        let text = reply.outcome.tool_content();
        assert!(text.contains("error:") && text.contains(expected), "{text}");
        host.assert_progress("error");
        server.await.unwrap();
    }

    for abort in [false, true] {
        let (listener, host) = fixture().await;
        let cancellation = CancellationToken::new();
        let cancel = cancellation.clone();
        let server = tokio::spawn(async move {
            let mut socket = receive(&listener, abort.then(|| json!({"image_size":"1K"}))).await;
            socket
                .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 1024\r\n\r\n{")
                .await
                .unwrap();
            if abort {
                cancel.cancel();
            }
            match timeout(WAIT, socket.read(&mut [0])).await.unwrap() {
                Ok(0) => {}
                Err(error) if error.kind() == std::io::ErrorKind::ConnectionReset => {}
                result => panic!("expected the cancelled response to disconnect: {result:?}"),
            }
        });
        let options = if abort {
            ""
        } else {
            "// @options: {\"timeout_ms\":1000}\n"
        };
        let generate = generate(abort.then_some(r#"{size:"1K"}"#));
        let reply = host.run(format!("{options}text('partial text'); store('discard', true); {generate} text('unexpected completion');"), cancellation).await;
        assert!(reply.outcome.is_error(), "{:?}", reply.outcome);
        assert!(reply.state.is_none());
        let text = reply.outcome.tool_content();
        if !abort {
            assert!(
                text.contains("partial text") && text.contains("Script timed out"),
                "{text}"
            );
        }
        assert!(!text.contains("unexpected completion"), "{text}");
        host.assert_progress("cancelled");
        server.await.unwrap();
    }

    let (listener, host) = fixture().await;
    let server = tokio::spawn(async move {
        let mut socket = receive(
            &listener,
            Some(json!({"aspect_ratio":"16:9","image_size":"2K"})),
        )
        .await;
        let body = json!({"id":"loopback-image","choices":[{"message":{"content":"generated description","images":[
            {"image_url":{"url":format!("data:image/png;base64,{IMAGE}")}}
        ]}}],"usage":{"prompt_tokens":2,"completion_tokens":3}}).to_string();
        socket
            .write_all(
                format!(
                    "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                )
                .as_bytes(),
            )
            .await
            .unwrap();
    });
    let generate = generate(Some(r#"{aspectRatio:" 16:9 ", size:"2K"}"#));
    let reply = host.run(format!("// @options: {{\"max_output_tokens\":0}}\n{generate} text(result.output[0].text); image(result.output[1]);"), Default::default()).await;
    let Outcome::Structured { data, .. } = reply.outcome else {
        panic!("expected structured image output")
    };
    assert_eq!(data["isError"], false);
    assert_eq!(data["usage"]["totalTokens"], 5);
    let content = data["content"].as_array().unwrap();
    assert!(
        content
            .iter()
            .any(|item| *item == json!({"type":"image","mimeType":"image/png","data":IMAGE}))
    );
    let preview = content[0]["text"].as_str().unwrap();
    assert!(preview.contains("Warning: truncated output"), "{preview}");
    assert!(!preview.contains("generated description"), "{preview}");
    let transcript = data["full_output_path"].as_str().unwrap();
    assert!(
        std::fs::read_to_string(transcript)
            .unwrap()
            .contains("generated description")
    );
    std::fs::remove_file(transcript).unwrap();
    let label = content
        .iter()
        .filter_map(|item| item["text"].as_str())
        .find_map(|text| text.strip_prefix("Image saved to "))
        .unwrap();
    assert_eq!(
        std::fs::read(label).unwrap(),
        base64::Engine::decode(&base64::engine::general_purpose::STANDARD, IMAGE).unwrap()
    );
    std::fs::remove_file(label).unwrap();
    host.assert_progress("ok");
    server.await.unwrap();
}
