//! Built-in chat-completions providers from the pinned Pi AI catalog.
use std::sync::Arc;

use serde_json::{Value, json};

use super::{Provider, providers::Adapter};

pub(super) struct Builtin {
    id: &'static str,
    endpoint: &'static str,
    env: &'static str,
    compatibility: fn() -> Value,
}

pub(super) const PROVIDERS: &[Builtin] = &[
    Builtin {
        id: "cerebras",
        endpoint: "https://api.cerebras.ai/v1/chat/completions",
        env: "CEREBRAS_API_KEY",
        compatibility: || json!({"maxTokensField":"max_completion_tokens"}),
    },
    Builtin {
        id: "deepseek",
        endpoint: "https://api.deepseek.com/chat/completions",
        env: "DEEPSEEK_API_KEY",
        compatibility: || {
            json!({
                "thinkingFormat":"deepseek", "requiresReasoningContentOnAssistantMessages":true,
            })
        },
    },
    Builtin {
        id: "fireworks",
        endpoint: "https://api.fireworks.ai/inference/v1/chat/completions",
        env: "FIREWORKS_API_KEY",
        compatibility: standard,
    },
    Builtin {
        id: "groq",
        endpoint: "https://api.groq.com/openai/v1/chat/completions",
        env: "GROQ_API_KEY",
        compatibility: standard,
    },
    Builtin {
        id: "huggingface",
        endpoint: "https://router.huggingface.co/v1/chat/completions",
        env: "HF_TOKEN",
        compatibility: standard,
    },
    Builtin {
        id: "moonshotai",
        endpoint: "https://api.moonshot.ai/v1/chat/completions",
        env: "MOONSHOT_API_KEY",
        compatibility: moonshot,
    },
    Builtin {
        id: "moonshotai-cn",
        endpoint: "https://api.moonshot.cn/v1/chat/completions",
        env: "MOONSHOT_API_KEY",
        compatibility: moonshot,
    },
    Builtin {
        id: "openrouter",
        endpoint: "https://openrouter.ai/api/v1/chat/completions",
        env: "OPENROUTER_API_KEY",
        compatibility: || {
            json!({
                "supportsStore":true, "maxTokensField":"max_completion_tokens", "thinkingFormat":"openrouter",
            })
        },
    },
    Builtin {
        id: "together",
        endpoint: "https://api.together.ai/v1/chat/completions",
        env: "TOGETHER_API_KEY",
        compatibility: || json!({"thinkingFormat":"together", "supportsReasoningEffort":false}),
    },
    Builtin {
        id: "zai",
        endpoint: "https://api.z.ai/api/coding/paas/v4/chat/completions",
        env: "ZAI_API_KEY",
        compatibility: zai,
    },
    Builtin {
        id: "zai-coding-cn",
        endpoint: "https://open.bigmodel.cn/api/coding/paas/v4/chat/completions",
        env: "ZAI_CODING_CN_API_KEY",
        compatibility: zai,
    },
];

fn standard() -> Value {
    json!({"supportsStore":true,"supportsDeveloperRole":true,"maxTokensField":"max_completion_tokens"})
}

fn moonshot() -> Value {
    json!({"supportsReasoningEffort":false})
}

fn zai() -> Value {
    json!({"thinkingFormat":"zai","supportsReasoningEffort":false})
}

pub(super) fn find(name: &str) -> Option<&'static Builtin> {
    PROVIDERS
        .iter()
        .find(|provider| name.eq_ignore_ascii_case(provider.id))
}

impl Builtin {
    pub fn provider(&self) -> Provider {
        Provider::Custom {
            id: Arc::from(self.id),
            adapter: Adapter::ChatCompletions,
        }
    }

    pub fn endpoint(&self) -> &'static str {
        self.endpoint
    }

    pub fn env(&self) -> &'static str {
        self.env
    }

    pub fn compatibility(&self) -> Value {
        (self.compatibility)()
    }
}
