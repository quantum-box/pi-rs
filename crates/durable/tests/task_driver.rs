use async_trait::async_trait;
use futures_util::{StreamExt, stream};
use pi_rs_core::{Error, Model, ModelEvent, ModelRequest, ModelStream};
use pi_rs_durable::{
    recovery::UncertainPolicy,
    runtime::{Config, Options, Scheduler},
    task::{Checkpoint, Task, TaskOwner, TaskState},
    task_scheduler::{Registry, Step},
};
use serde_json::json;
use std::sync::Arc;
use uuid::Uuid;
struct FakeModel;
#[async_trait]
impl Model for FakeModel {
    async fn stream(&self, _: ModelRequest) -> Result<ModelStream, Error> {
        Ok(stream::iter(vec![
            Ok(ModelEvent::Text("ok".into())),
            Ok(ModelEvent::Complete {
                calls: vec![],
                usage: None,
            }),
        ])
        .boxed())
    }
}
fn options() -> Options {
    Options {
        model: Arc::new(FakeModel),
        tools: vec![],
        restart_limit: 5,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::InterruptedResult,
    }
}
fn registry() -> Registry {
    let mut registry = Registry::default();
    registry.register("test", true, |_: &Task| {
        Ok(Step::Succeed(json!("finished")))
    });
    registry
}
#[tokio::test]
async fn generic_tasks_run_under_the_same_owner_and_survive_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let id = Uuid::new_v4();
    {
        let mut s = Scheduler::open(&path, options()).unwrap();
        let conversation = s
            .create_conversation(Config {
                model: "m".into(),
                system_prompt: "".into(),
                max_output_tokens: 100,
            })
            .unwrap();
        let driver = s.start_tasks(registry()).unwrap();
        assert!(s.start_tasks(registry()).is_err());
        let task = Task::new(
            id,
            TaskOwner::Conversation(conversation),
            Checkpoint {
                phase: "start".into(),
                data: json!(null),
            },
            5,
        )
        .unwrap()
        .registered("test".into(), true);
        driver.submit(&task).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if matches!(
                    driver.get(id).unwrap().unwrap().state(),
                    TaskState::Succeeded(_)
                ) {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        driver.shutdown().unwrap();
        s.shutdown().await.unwrap();
    }
    let s = Scheduler::open(&path, options()).unwrap();
    let driver = s.start_tasks(registry()).unwrap();
    assert!(matches!(
        driver.get(id).unwrap().unwrap().state(),
        TaskState::Succeeded(_)
    ));
    driver.shutdown().unwrap();
    s.shutdown().await.unwrap();
}
