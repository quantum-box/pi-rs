//! Pi-compatible model-specific reasoning and prompt-cache choices.
use pi_rs_core::{CacheRetention, Error, Result};
use serde_json::{Value, json};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReasoningLevel {
    Off,
    Minimal,
    Low,
    Medium,
    High,
    Xhigh,
    Max,
}
impl ReasoningLevel {
    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "off" => Ok(Self::Off),
            "minimal" => Ok(Self::Minimal),
            "low" => Ok(Self::Low),
            "medium" => Ok(Self::Medium),
            "high" => Ok(Self::High),
            "xhigh" => Ok(Self::Xhigh),
            "max" => Ok(Self::Max),
            _ => Err(Error::Config(format!(
                "unsupported Bedrock reasoning level {value:?}"
            ))),
        }
    }
    fn budget(self) -> u32 {
        match self {
            Self::Off => 0,
            Self::Minimal => 1024,
            Self::Low => 2048,
            Self::Medium => 8192,
            Self::High | Self::Xhigh | Self::Max => 16384,
        }
    }
    fn gpt_effort(self) -> &'static str {
        match self {
            Self::Off => "none",
            Self::Minimal | Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
            Self::Xhigh => "xhigh",
            Self::Max => "max",
        }
    }
    fn oss_effort(self) -> &'static str {
        match self {
            Self::Off => "none",
            Self::Minimal | Self::Low => "low",
            Self::Medium => "medium",
            _ => "high",
        }
    }
}

fn candidates(model: &str, name: Option<&str>) -> Vec<String> {
    [Some(model), name]
        .into_iter()
        .flatten()
        .flat_map(|v| {
            let lower = v.to_ascii_lowercase();
            let normalized = lower.replace([' ', '_', '.', ':'], "-");
            [lower, normalized]
        })
        .collect()
}
fn has_any(items: &[String], patterns: &[&str]) -> bool {
    items.iter().any(|s| patterns.iter().any(|p| s.contains(p)))
}
const ADAPTIVE: &[&str] = &[
    "opus-4-6",
    "opus-4-7",
    "opus-4-8",
    "opus-5",
    "sonnet-4-6",
    "sonnet-5",
    "haiku-5",
    "fable-5",
    "mythos-5",
];
const NEW_ADAPTIVE: &[&str] = &[
    "opus-4-7", "opus-4-8", "opus-5", "sonnet-5", "haiku-5", "fable-5", "mythos-5",
];

/// Whether the model cannot guarantee thinking is off during tool use.
pub fn requires_thinking(model: &str, name: Option<&str>) -> bool {
    let items = candidates(model, name);
    has_any(&items, &["claude"])
        && has_any(&items, &["fable-5", "mythos-5", "opus-5-5", "sonnet-5-5"])
}

pub(crate) fn sonnet_55(model: &str, name: Option<&str>) -> bool {
    let items = candidates(model, name);
    has_any(&items, &["claude"]) && has_any(&items, &["sonnet-5-5"])
}

pub fn thinking_levels(model: &str, name: Option<&str>) -> Vec<String> {
    let items = candidates(model, name);
    if !has_any(&items, &["claude", "gpt-"]) {
        return vec!["off".into()];
    }
    let mut levels = vec!["minimal", "low", "medium", "high"];
    if !requires_thinking(model, name) {
        levels.insert(0, "off");
    }
    if has_any(&items, NEW_ADAPTIVE)
        || (has_any(&items, &["gpt-"]) && !has_any(&items, &["gpt-oss"]))
    {
        levels.push("xhigh");
    }
    if has_any(&items, ADAPTIVE) || (has_any(&items, &["gpt-"]) && !has_any(&items, &["gpt-oss"])) {
        levels.push("max");
    }
    levels.into_iter().map(str::to_owned).collect()
}

pub(crate) fn supports_long_cache(model: &str, name: Option<&str>) -> bool {
    let items = candidates(model, name);
    has_any(&items, &["claude"])
        && has_any(
            &items,
            &[
                "opus-4-5",
                "opus-4-6",
                "opus-4-7",
                "opus-4-8",
                "opus-5",
                "sonnet-4-5",
                "sonnet-4-6",
                "sonnet-5",
                "haiku-4-5",
                "haiku-5",
                "fable-5",
            ],
        )
}

pub(crate) fn supports_cache(model: &str, name: Option<&str>, force: bool) -> bool {
    let items = candidates(model, name);
    if !has_any(&items, &["claude"]) {
        return force;
    }
    has_any(
        &items,
        &[
            "fable-5",
            "opus-5",
            "sonnet-5",
            "haiku-5",
            "-4-",
            "claude-3-7-sonnet",
            "claude-3-5-haiku",
        ],
    )
}

pub(crate) fn cache_point(
    retention: CacheRetention,
) -> Result<Option<aws_sdk_bedrockruntime::types::CachePointBlock>> {
    use aws_sdk_bedrockruntime::types::{CachePointBlock, CachePointType, CacheTtl};
    if retention == CacheRetention::None {
        return Ok(None);
    }
    let mut point = CachePointBlock::builder().r#type(CachePointType::Default);
    if retention == CacheRetention::Long {
        point = point.ttl(CacheTtl::OneHour);
    }
    Ok(Some(point.build().map_err(|e| {
        Error::Config(format!("invalid Bedrock cache point: {e}"))
    })?))
}

pub(crate) fn reasoning_fields(
    model: &str,
    name: Option<&str>,
    level: Option<ReasoningLevel>,
    mapped: Option<&str>,
    max_tokens: u32,
    region: Option<&str>,
    tool_free: bool,
) -> Result<Option<Value>> {
    let items = candidates(model, name);
    if has_any(&items, &["claude"]) {
        if level.is_none() && requires_thinking(model, name) {
            return Ok(None);
        }
        let level = level.unwrap_or(ReasoningLevel::Off);
        if level == ReasoningLevel::Off {
            if has_any(&items, &["fable-5", "mythos-5", "opus-5-5"]) {
                return Err(Error::Config("This Bedrock Claude model cannot disable thinking; select a reasoning level or use a different model".into()));
            }
            if has_any(&items, &["sonnet-5-5"]) {
                return if tool_free {
                    Ok(Some(json!({"thinking":{"type":"between_tools"}})))
                } else {
                    Err(Error::Config("Claude Sonnet 5.5 cannot disable thinking between tool calls; select a reasoning level or use a different model".into()))
                };
            }
            if has_any(&items, &["opus-5", "sonnet-5", "haiku-5-5"]) {
                return Ok(Some(json!({"thinking":{"type":"disabled"}})));
            }
            return Ok(None);
        }
        let gov = region.is_some_and(|r| r.to_ascii_lowercase().starts_with("us-gov-"))
            || model.to_ascii_lowercase().starts_with("us-gov.")
            || model.to_ascii_lowercase().starts_with("arn:aws-us-gov:");
        let adaptive = has_any(&items, ADAPTIVE);
        let binding = adaptive && !gov && has_any(&items, NEW_ADAPTIVE);
        let mut thinking = if adaptive {
            json!({"type":"adaptive"})
        } else {
            let budget = level.budget().min(max_tokens.saturating_sub(1024));
            if budget < 1024 {
                return Err(Error::Config("Bedrock Claude thinking requires at least 2048 max output tokens (1024 for thinking and 1024 for the answer)".into()));
            }
            json!({"type":"enabled","budget_tokens":budget})
        };
        if !gov {
            thinking["display"] = json!("summarized");
        }
        if binding {
            thinking["block_binding"] = json!({"prefix_mismatch_behavior":"drop_block"});
        }
        let mut fields = json!({"thinking":thinking});
        if adaptive {
            let effort = if let Some(mapped) = mapped {
                mapped
            } else if level == ReasoningLevel::Xhigh && has_any(&items, NEW_ADAPTIVE) {
                "xhigh"
            } else {
                match level {
                    ReasoningLevel::Off | ReasoningLevel::Minimal | ReasoningLevel::Low => "low",
                    ReasoningLevel::Medium => "medium",
                    ReasoningLevel::Max => "max",
                    _ => "high",
                }
            };
            fields["output_config"] = json!({"effort":effort});
            if binding {
                fields["anthropic_beta"] = json!(["thinking-binding-controls-2026-08-01"]);
            }
        } else {
            fields["anthropic_beta"] = json!(["interleaved-thinking-2025-05-14"]);
        }
        return Ok(Some(fields));
    }
    let Some(level) = level else { return Ok(None) };
    if level == ReasoningLevel::Off && mapped.is_none() {
        return Ok(None);
    }
    if has_any(&items, &["gpt-oss"]) {
        return Ok(Some(
            json!({"reasoning_effort":mapped.unwrap_or(level.oss_effort())}),
        ));
    }
    if has_any(&items, &["gpt-"]) {
        return Ok(Some(
            json!({"reasoning":{"effort":mapped.unwrap_or(level.gpt_effort())}}),
        ));
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn model_support_and_effort_selection_follow_pi_families() {
        assert!(supports_cache(
            "us.anthropic.claude-3-5-haiku-20241022-v1:0",
            None,
            false
        ));
        assert!(supports_cache("opaque", Some("Claude Sonnet 4.5"), false));
        assert!(!supports_cache("amazon.nova-pro-v1:0", None, false));
        assert!(supports_cache("opaque", None, true));
        assert!(!supports_cache("anthropic.claude-3-haiku", None, true));
        assert_eq!(
            reasoning_fields(
                "anthropic.claude-sonnet-4-6",
                None,
                Some(ReasoningLevel::Xhigh),
                None,
                1024,
                None,
                true
            )
            .unwrap(),
            Some(
                json!({"thinking":{"type":"adaptive","display":"summarized"},"output_config":{"effort":"high"}})
            )
        );
        assert_eq!(
            reasoning_fields(
                "opaque",
                Some("GPT-5.6"),
                Some(ReasoningLevel::Minimal),
                None,
                1024,
                None,
                true
            )
            .unwrap(),
            Some(json!({"reasoning":{"effort":"low"}}))
        );
        assert_eq!(
            reasoning_fields(
                "unknown",
                None,
                Some(ReasoningLevel::High),
                None,
                1024,
                None,
                true
            )
            .unwrap(),
            None
        );
        assert_eq!(
            reasoning_fields(
                "anthropic.claude-opus-4-8",
                None,
                None,
                None,
                1024,
                None,
                true
            )
            .unwrap(),
            None
        );
        assert!(ReasoningLevel::parse("nonsense").is_err());
    }
}
