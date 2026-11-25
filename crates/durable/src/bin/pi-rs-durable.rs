use std::{
    io::{self, Write},
    num::NonZeroUsize,
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use clap::Parser;
use pi_rs_core::{
    HostAuthBuilder, HostCredential, HostCredentialKind, Model, ModelRequest, ModelStream,
};
use pi_rs_durable::{
    progress,
    recovery::UncertainPolicy,
    runtime::{Config, Options, Scheduler, Status},
};
use pi_rs_workbench::model_factory::{ModelClientFactory, NativeModelApi};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use uuid::Uuid;

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
    /// Conversation ID printed when the conversation was created.
    #[arg(long)]
    conversation: Option<Uuid>,
    /// Reuse this ID when retrying the same submission.
    #[arg(long)]
    request_id: Option<String>,
    /// Emit newline-delimited conversation, progress and result objects.
    #[arg(long)]
    json: bool,
    prompt: String,
}

struct RoutedModel {
    client: Arc<dyn Model>,
    suffix: String,
}

impl RoutedModel {
    fn provider_model<'a>(&self, configured: &'a str) -> pi_rs_core::Result<&'a str> {
        configured
            .strip_suffix(&self.suffix)
            .filter(|name| !name.is_empty())
            .ok_or_else(|| {
                pi_rs_core::Error::Config(
                    "durable conversation uses a different provider route".into(),
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
        HostCredential::new(HostCredentialKind::ApiKey, key)?,
    )?;
    let client = ModelClientFactory::new(credentials.build()).client(
        NativeModelApi::OpenAiResponses,
        "openai",
        &args.endpoint,
    )?;
    // Persist only a route fingerprint in the model identifier. Recovery checks
    // it on every request before forwarding the original model name.
    let suffix = format!(
        "|openai-responses:{:x}",
        Sha256::digest(args.endpoint.as_bytes())
    );
    let model = Arc::new(RoutedModel { client, suffix });
    let mut scheduler = Scheduler::open_paused(
        &args.db,
        Options {
            model: model.clone(),
            tools: Vec::new(),
            restart_limit: 3,
            max_turns: 1,
            uncertain_policy: UncertainPolicy::RequireReconciliation,
        },
    )?;

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
    if json_output {
        emit(&json!({"type": "conversation", "id": id}))?;
    } else {
        eprintln!("Conversation {id}");
    }

    let mut watch = scheduler.watch_view(id, NonZeroUsize::MIN, Duration::from_millis(50))?;
    let submission = scheduler.submit(id, request_id, prompt)?;
    if json_output {
        let completed = scheduler.wait(id);
        let interrupt = tokio::signal::ctrl_c();
        tokio::pin!(completed, interrupt);
        let mut last_text = String::new();
        loop {
            tokio::select! {
                result = &mut completed => {
                    result?;
                    break;
                }
                result = &mut interrupt => {
                    result?;
                    return Err("interrupted".into());
                }
                view = watch.next() => {
                    if let Some(progress::State::Running(partial)) = view?.and_then(|view| {
                        let active = view.state.submissions.iter().any(|entry| {
                            entry.id == submission && matches!(entry.status, Status::Running)
                        });
                        view.live.filter(|_| active)
                    }) && partial.text != last_text {
                        last_text = partial.text;
                        emit(&json!({"type": "progress", "text": last_text}))?;
                    }
                }
            }
        }
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
