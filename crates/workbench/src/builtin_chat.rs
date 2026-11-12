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
        id: "openai",
        endpoint: "https://api.openai.com/v1/responses",
        env: "OPENAI_API_KEY",
        compatibility: || json!({}),
    },
    Builtin {
        id: "ant-ling",
        endpoint: "https://api.ant-ling.com/v1/chat/completions",
        env: "ANT_LING_API_KEY",
        compatibility: || json!({"supportsStore":false,"supportsDeveloperRole":false,"supportsReasoningEffort":false,"maxTokensField":"max_tokens","thinkingFormat":"ant-ling","supportsStrictMode":true}),
    },
    Builtin {
        id: "baseten",
        endpoint: "https://inference.baseten.co/v1/chat/completions",
        env: "BASETEN_API_KEY",
        compatibility: || json!({}),
    },
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
        id: "kimi-coding",
        endpoint: "https://api.kimi.com/coding/v1/chat/completions",
        env: "KIMI_API_KEY",
        compatibility: || {
            json!({
                "supportsStrictMode":false, "supportsStore":false,
                "supportsDeveloperRole":false, "supportsReasoningEffort":true,
                "maxTokensField":"max_tokens", "thinkingFormat":"openai",
                "requiresReasoningContentOnAssistantMessages":true
            })
        },
    },
    Builtin {
        id: "minimax",
        endpoint: "https://api.minimax.io/anthropic/v1/messages",
        env: "MINIMAX_API_KEY",
        compatibility: || json!({}),
    },
    Builtin {
        id: "minimax-cn",
        endpoint: "https://api.minimaxi.com/anthropic/v1/messages",
        env: "MINIMAX_CN_API_KEY",
        compatibility: || json!({}),
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
        id: "nvidia",
        endpoint: "https://integrate.api.nvidia.com/v1/chat/completions",
        env: "NVIDIA_API_KEY",
        compatibility: || json!({}),
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
        id: "qwen-token-plan",
        endpoint: "https://token-plan.ap-southeast-1.maas.aliyuncs.com/compatible-mode/v1/chat/completions",
        env: "QWEN_TOKEN_PLAN_API_KEY",
        compatibility: qwen_token_plan,
    },
    Builtin {
        id: "qwen-token-plan-cn",
        endpoint: "https://token-plan.cn-beijing.maas.aliyuncs.com/compatible-mode/v1/chat/completions",
        env: "QWEN_TOKEN_PLAN_CN_API_KEY",
        compatibility: qwen_token_plan,
    },
    Builtin {
        id: "qwen-token-plan-individual",
        endpoint: "https://token-plan.ap-southeast-1.maas.aliyuncs.com/compatible-mode/v1/chat/completions",
        env: "QWEN_TOKEN_PLAN_API_KEY",
        compatibility: qwen_token_plan,
    },
    Builtin {
        id: "together",
        endpoint: "https://api.together.ai/v1/chat/completions",
        env: "TOGETHER_API_KEY",
        compatibility: || json!({"thinkingFormat":"together", "supportsReasoningEffort":false}),
    },
    Builtin {
        id: "vercel-ai-gateway",
        endpoint: "https://ai-gateway.vercel.sh/v1/messages",
        env: "AI_GATEWAY_API_KEY",
        compatibility: || json!({}),
    },
    Builtin {
        id: "xai",
        endpoint: "https://api.x.ai/v1/responses",
        env: "XAI_API_KEY",
        compatibility: || json!({}),
    },
    Builtin {
        id: "xiaomi",
        endpoint: "https://api.xiaomimimo.com/v1/chat/completions",
        env: "XIAOMI_API_KEY",
        compatibility: xiaomi,
    },
    Builtin {
        id: "xiaomi-token-plan-cn",
        endpoint: "https://token-plan-cn.xiaomimimo.com/v1/chat/completions",
        env: "XIAOMI_TOKEN_PLAN_CN_API_KEY",
        compatibility: xiaomi,
    },
    Builtin {
        id: "xiaomi-token-plan-ams",
        endpoint: "https://token-plan-ams.xiaomimimo.com/v1/chat/completions",
        env: "XIAOMI_TOKEN_PLAN_AMS_API_KEY",
        compatibility: xiaomi,
    },
    Builtin {
        id: "xiaomi-token-plan-sgp",
        endpoint: "https://token-plan-sgp.xiaomimimo.com/v1/chat/completions",
        env: "XIAOMI_TOKEN_PLAN_SGP_API_KEY",
        compatibility: xiaomi,
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

fn qwen_token_plan() -> Value {
    json!({"thinkingFormat":"qwen", "supportsDeveloperRole":false, "supportsStore":false})
}

fn xiaomi() -> Value {
    json!({"thinkingFormat":"deepseek", "supportsReasoningEffort":false,
        "supportsStore":true,"supportsDeveloperRole":true,"maxTokensField":"max_completion_tokens",
        "requiresReasoningContentOnAssistantMessages":true})
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
            adapter: match self.id {
                "openai" | "xai" => Adapter::OpenAiResponses,
                "minimax" | "minimax-cn" | "vercel-ai-gateway" => Adapter::Anthropic,
                _ => Adapter::ChatCompletions,
            },
        }
    }

    pub fn endpoint(&self) -> &'static str {
        self.endpoint
    }

    pub fn endpoint_for(&self, adapter: Adapter) -> Option<&'static str> {
        match (adapter, self.id) {
            (Adapter::OpenAiResponses, "openai" | "xai") => Some(self.endpoint),
            (Adapter::ChatCompletions, id)
                if !matches!(
                    id,
                    "openai" | "xai" | "minimax" | "minimax-cn" | "vercel-ai-gateway"
                ) =>
            {
                Some(self.endpoint)
            }
            (Adapter::Anthropic, "minimax" | "minimax-cn" | "vercel-ai-gateway") => {
                Some(self.endpoint)
            }
            (Adapter::Anthropic, "fireworks") => {
                Some("https://api.fireworks.ai/inference/v1/messages")
            }
            (Adapter::Anthropic, "openrouter") => Some("https://openrouter.ai/api/v1/messages"),
            _ => None,
        }
    }

    pub fn is_official_endpoint(&self, endpoint: &str) -> bool {
        self.endpoint == endpoint || self.endpoint_for(Adapter::Anthropic) == Some(endpoint)
    }

    pub fn env(&self) -> &'static str {
        self.env
    }

    pub fn compatibility(&self) -> Value {
        (self.compatibility)()
    }
}
