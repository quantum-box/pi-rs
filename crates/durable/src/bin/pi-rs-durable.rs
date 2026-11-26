use std::{
    io::{self, Write},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use clap::Parser;
use pi_rs_core::{
    HostAuthBuilder, HostCredential, HostCredentialKind, Model, ModelRequest, ModelStream,
};
use pi_rs_durable::{
    openai_responses::OpenAiResponsesAdapter,
    progress,
    recovery::UncertainPolicy,
    runtime::{Config, Options, Scheduler, Status},
    worker_activity::WorkerKind,
};
use pi_rs_workbench::model_factory::{ModelClientFactory, NativeModelApi};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use uuid::Uuid;

mod text_rpc;

#[derive(Parser)]
#[command(about = "Run an experimental durable text conversation using OpenAI Responses")]
struct Args {
    /// SQLite database for durable conversations.
    #[arg(long)]
    db: PathBuf,
    /// Exact Responses endpoint. The API key comes from OPENAI_API_KEY.
    #[arg(long)]
    endpoint: String,
    #[arg(
        long,
        required_unless_present = "conversation",
        conflicts_with = "conversation"
    )]
    model: Option<String>,
    /// Enable local read, write, edit and bash in this workspace. Use the same directory on resume.
    #[arg(long, value_name = "WORKSPACE")]
    coding_tools: Option<PathBuf>,
    /// Conversation ID printed when the conversation was created.
    #[arg(long)]
    conversation: Option<Uuid>,
    /// Reuse this ID when retrying the same submission.
    #[arg(long)]
    request_id: Option<String>,
    /// Emit newline-delimited conversation, progress and result objects.
    #[arg(long, conflicts_with = "rpc")]
    json: bool,
    /// Accept RPC commands over stdin and return session events on stdout.
    #[arg(long, conflicts_with_all = ["prompt", "request_id"])]
    rpc: bool,
    #[arg(required_unless_present = "rpc")]
    prompt: Option<String>,
}

struct RoutedModel {
    client: Arc<dyn Model>,
    suffix: String,
    has_image_adapter: bool,
    has_coding_tools: bool,
}

impl RoutedModel {
    fn provider_model<'a>(&self, configured: &'a str) -> pi_rs_core::Result<&'a str> {
        configured
            .strip_suffix(&self.suffix)
            .filter(|name| !name.is_empty())
            .ok_or_else(|| {
                pi_rs_core::Error::Config(
                    "durable conversation uses a different provider route or coding tools workspace".into(),
                )
            })
    }
}

#[async_trait::async_trait]
impl Model for RoutedModel {
    async fn stream(&self, mut request: ModelRequest) -> pi_rs_core::Result<ModelStream> {
        request.model = self.provider_model(&request.model)?.to_owned();
        self.client.stream(request).await
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let key = std::env::var("OPENAI_API_KEY").map_err(|_| "set OPENAI_API_KEY")?;
    let mut credentials = HostAuthBuilder::new();
    credentials.register(
        "openai",
        &args.endpoint,
        HostCredential::new(HostCredentialKind::ApiKey, key.clone())?,
    )?;
    let client = ModelClientFactory::new(credentials.build()).client(
        NativeModelApi::OpenAiResponses,
        "openai",
        &args.endpoint,
    )?;
    // Persist only a route fingerprint in the model identifier. Recovery checks
    // it on every request before forwarding the original model name.
    let mut suffix = format!(
        "|openai-responses:{:x}",
        Sha256::digest(args.endpoint.as_bytes())
    );
    let tools = if let Some(workspace) = &args.coding_tools {
        let workspace = workspace.canonicalize()?;
        if !workspace.is_dir() {
            return Err("coding tools workspace must be a directory".into());
        }
        // Include the execution profile in the saved route so recovery never
        // silently changes the working directory or enables tools.
        suffix.push_str(&format!(
            "|coding-tools-v1:{:x}",
            Sha256::digest(workspace.as_os_str().as_encoded_bytes())
        ));
        pi_rs_durable::coding_tools::local(&workspace)?
    } else {
        Vec::new()
    };
    let adapter = OpenAiResponsesAdapter::new(&args.endpoint, key)
        .ok()
        .map(|adapter| Arc::new(adapter.with_model_route_suffix(suffix.clone())));
    let model = Arc::new(RoutedModel {
        client,
        suffix,
        has_image_adapter: adapter.is_some(),
        has_coding_tools: !tools.is_empty(),
    });
    let options = Options {
        model: model.clone(),
        tools,
        restart_limit: 3,
        max_turns: if model.has_coding_tools { 32 } else { 1 },
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    };
    let mut scheduler = match adapter {
        Some(adapter) => Scheduler::open_paused_with_provider_adapter(&args.db, options, adapter)?,
        None => Scheduler::open_paused(&args.db, options)?,
    };

    let result = run(&mut scheduler, &model, args).await;
    let shutdown = scheduler.shutdown().await;
    result?;
    shutdown?;
    Ok(())
}

async fn run(
    scheduler: &mut Scheduler,
    model: &RoutedModel,
    args: Args,
) -> Result<(), Box<dyn std::error::Error>> {
    let Args {
        model: new_model,
        conversation,
        request_id,
        prompt,
        json: json_output,
        rpc,
        ..
    } = args;
    let id = match conversation {
        Some(id) => {
            model.provider_model(&scheduler.conversation(id)?.config.model)?;
            id
        }
        None => {
            let provider_model = new_model
                .filter(|name| !name.trim().is_empty())
                .ok_or("a model is required")?;
            scheduler.create_conversation(Config {
                model: format!("{provider_model}{}", model.suffix),
                system_prompt: String::new(),
                max_output_tokens: 4096,
            })?
        }
    };
    if rpc {
        return text_rpc::run(scheduler, id, model).await;
    }
    let prompt = prompt.ok_or("a prompt is required")?;
    if json_output {
        emit(&json!({"type": "conversation", "id": id}))?;
    } else {
        eprintln!("Conversation {id}");
    }

    let submission = scheduler.submit(id, request_id, prompt)?;
    if json_output {
        let interrupt = tokio::signal::ctrl_c();
        tokio::pin!(interrupt);
        let mut poll = tokio::time::interval(Duration::from_millis(50));
        poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut last_text = String::new();
        while scheduler
            .worker_activity()
            .snapshot()
            .iter()
            .any(|worker| worker.kind == WorkerKind::Conversation && worker.id == id)
        {
            tokio::select! {
                result = &mut interrupt => {
                    result?;
                    return Err("interrupted".into());
                }
                _ = poll.tick() => {
                    let view = scheduler.view_status(id)?;
                    if view.state.model_request_id(submission).is_some()
                        && let Some(progress::State::Running(partial)) = view.live
                        && partial.text != last_text
                    {
                        last_text = partial.text;
                        emit(&json!({"type": "progress", "text": last_text}))?;
                    }
                }
            }
        }
        scheduler.wait(id).await?;
    } else {
        tokio::select! {
            result = scheduler.wait(id) => result?,
            result = tokio::signal::ctrl_c() => {
                result?;
                return Err("interrupted".into());
            }
        }
    }
    let state = scheduler.conversation(id)?;
    match &state
        .submissions
        .iter()
        .find(|entry| entry.id == submission)
        .ok_or("submission is missing")?
        .status
    {
        Status::Done { answer } if json_output => {
            emit(&json!({"type": "result", "submission": submission, "text": answer}))?
        }
        Status::Done { answer } => writeln!(io::stdout().lock(), "{answer}")?,
        Status::Unanswered { reason } => return Err(reason.clone().into()),
        _ => return Err("submission has no settled result".into()),
    }
    Ok(())
}

fn emit(object: &Value) -> io::Result<()> {
    let mut stdout = io::stdout().lock();
    serde_json::to_writer(&mut stdout, object)?;
    writeln!(stdout)?;
    stdout.flush()
}
