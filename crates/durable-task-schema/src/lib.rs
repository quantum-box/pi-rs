//! Single-source persisted task declarations shared by the runtime and store.
//! Consumers provide serde, serde_json and uuid imports at the expansion site.

#[macro_export]
macro_rules! declare_signal_key {
    () => {
        /// Upstream-shaped signal kind and wait key; versions do not affect matching.
        #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
        pub struct SignalKey {
            signal: String,
            key: String,
        }
    };
}

#[macro_export]
macro_rules! declare_task_schema {
    ($signal_key:ty) => {
        fn initial_version() -> u32 {
            1
        }

        fn task_tool_is_false(value: &bool) -> bool {
            !value
        }

        /// Ownership is an immutable identity edge, not an executor lease.
        #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
        pub enum TaskOwner {
            Conversation(Uuid),
            Task(Uuid),
        }

        #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
        pub struct Checkpoint {
            pub phase: String,
            pub data: Value,
        }

        /// Conditions persisted with the task. Deadlines are absolute Unix milliseconds;
        /// `Tasks` waits for all listed tasks to reach any terminal state.
        #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
        pub enum WaitFor {
            DeadlineMillis(i64),
            Signal(String),
            /// Wait for one namespaced signal key, independent of its value version.
            TypedSignal($signal_key),
            /// Typed keys to wake on, optionally bounded by an absolute deadline.
            TypedSleep {
                until_millis: Option<i64>,
                signals: Vec<$signal_key>,
            },
            /// Wake on the first matching signal or when the absolute deadline passes.
            Sleep {
                until_millis: Option<i64>,
                signals: Vec<String>,
            },
            /// Wake when all named tasks are terminal, including unowned tasks.
            Tasks(Vec<Uuid>),
            /// Wake when any named task ends. An empty set is immediately ready.
            TasksAny(Vec<Uuid>),
            /// Wait for all direct children; a failure cancels the other listed subtrees.
            TasksFailFast(Vec<Uuid>),
        }

        /// Durable wake reason for a typed sleep.
        #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
        pub enum TypedWake {
            Signal($signal_key),
            Expired,
            Time,
        }

        #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
        pub enum TaskState {
            Ready,
            Running,
            Waiting(WaitFor),
            Completing(Value),
            CompletingFailed(String),
            Succeeded(Value),
            Failed(String),
            Cancelled,
        }

        impl TaskState {
            // Exhaustive so adding a state requires a shared lifecycle decision.
            fn is_terminal_state(&self) -> bool {
                match self {
                    Self::Succeeded(_) | Self::Failed(_) | Self::Cancelled => true,
                    Self::Ready
                    | Self::Running
                    | Self::Waiting(_)
                    | Self::Completing(_)
                    | Self::CompletingFailed(_) => false,
                }
            }
        }

        #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
        pub struct Task {
            id: Uuid,
            owner: TaskOwner,
            checkpoint: Checkpoint,
            #[serde(default = "initial_version")]
            definition_version: u32,
            #[serde(default)]
            input: Value,
            state: TaskState,
            /// Identity of the current scheduler reservation; legacy and standalone
            /// task records may have none. This is not by itself an executor lease.
            #[serde(default, skip_serializing_if = "Option::is_none")]
            attempt_id: Option<Uuid>,
            /// Last scheduler attempt whose handler returned and whose transition committed.
            /// This is storage evidence only, not proof that detached work has stopped.
            #[serde(default, skip_serializing_if = "Option::is_none")]
            retired_attempt_id: Option<Uuid>,
            /// Consecutive recoveries since the last changed checkpoint.
            restarts: u32,
            restart_limit: u32,
            #[serde(default)]
            delivered_signal: Option<Uuid>,
            #[serde(default)]
            typed_wake: Option<TypedWake>,
            /// Terminal direct children observed by the most recent child wait. Kept
            /// across reservation and replay until the next progress or settlement.
            #[serde(default, skip_serializing_if = "Option::is_none")]
            ended_children: Option<Vec<Uuid>>,
            /// Children named by the wait, retained to refresh ended IDs at reservation.
            #[serde(default, skip_serializing_if = "Option::is_none")]
            wake_children: Option<Vec<Uuid>>,
            /// Registration identity; absent on records created by the standalone API.
            #[serde(default)]
            kind: Option<String>,
            /// Only explicitly replay-safe tasks may run again after an interrupted invocation.
            #[serde(default)]
            replay_safe: bool,
            /// Durable cancellation intent; retained on terminal records.
            #[serde(default)]
            abort_requested: bool,
            /// Conversation-owned background tasks form an ordinary abort boundary.
            #[serde(default)]
            background: bool,
            /// Set only by the TaskTool adapter, never inferred from a task kind.
            #[serde(default, skip_serializing_if = "task_tool_is_false")]
            task_tool: bool,
        }
    };
}
