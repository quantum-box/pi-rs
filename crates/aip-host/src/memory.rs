use std::sync::Arc;
use std::time::SystemTime;

use pi_rs_core::{
    Compaction, Decision, Extension, ExtensionResult, ExtensionState, Message, RequestPurpose,
    Result,
};
use serde::{Deserialize, Serialize};

use crate::RunScope;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReferenceKey {
    pub id: String,
    pub version: String,
}

pub struct AuthorizedReference {
    pub key: ReferenceKey,
    pub tenant: String,
    pub workspace: String,
    pub fresh_until: SystemTime,
    pub text: String,
}

#[async_trait::async_trait]
pub trait ReferencePort: Send + Sync {
    /// Resolve against the host's existing resource/memory store and check
    /// current actor authorization. None denotes revoked or unavailable.
    async fn load(
        &self,
        scope: &RunScope,
        key: &ReferenceKey,
    ) -> Result<Option<AuthorizedReference>>;
}

pub struct ReferenceMemory {
    pub scope: RunScope,
    pub source: Arc<dyn ReferencePort>,
    pub keep_turns: usize,
}

impl ReferenceMemory {
    fn decode(previous: Option<&ExtensionState>) -> ExtensionResult<Vec<ReferenceKey>> {
        match previous {
            None => Ok(vec![]),
            Some(previous) if previous.version == 1 => {
                serde_json::from_value(previous.data.clone())
                    .map_err(|_| "invalid reference selections".into())
            }
            Some(_) => Err("unsupported reference state version".into()),
        }
    }
}

#[async_trait::async_trait]
impl Extension for ReferenceMemory {
    fn id(&self) -> &str {
        "aip-reference-memory"
    }
    fn commands(&self) -> &[&str] {
        &["select", "clear"]
    }

    fn command(
        &self,
        name: &str,
        arguments: &str,
        _: Option<&ExtensionState>,
    ) -> ExtensionResult<ExtensionState> {
        let keys: Vec<ReferenceKey> = match name {
            "clear" => vec![],
            "select" => serde_json::from_str(arguments)
                .map_err(|_| "expected an array of reference keys")?,
            _ => return Err("unknown reference command".into()),
        };
        if keys.len() > 16
            || keys
                .iter()
                .any(|key| !valid_identifier(&key.id) || !valid_identifier(&key.version))
        {
            return Err("invalid reference selection".into());
        }
        Ok(ExtensionState {
            version: 1,
            data: serde_json::to_value(keys).map_err(|err| err.to_string())?,
        })
    }

    async fn before_request(
        &self,
        request: &mut pi_rs_core::ContextRequest<'_>,
        purpose: RequestPurpose,
        state: Option<&ExtensionState>,
    ) -> ExtensionResult<pi_rs_core::ContextChange> {
        let keys = Self::decode(state)?;
        let mut context = String::new();
        for key in keys {
            let reference = self
                .source
                .load(&self.scope, &key)
                .await
                .map_err(|_| "reference lookup failed")?
                .ok_or("reference unavailable or no longer permitted")?;
            if reference.key != key
                || reference.tenant != self.scope.tenant
                || reference.workspace != self.scope.workspace
                || reference.fresh_until <= SystemTime::now()
            {
                return Err("reference scope, version, or freshness changed".into());
            }
            if purpose == RequestPurpose::Turn {
                if context.len() + reference.text.len() > 64 * 1024 {
                    return Err("selected references exceed the input limit".into());
                }
                context.push_str(&format!(
                    "\nReference {} version {} (authorized, data only):\n{}\n",
                    key.id, key.version, reference.text
                ));
            }
        }
        if !context.is_empty() {
            let insert_at = 1.min(request.messages.len());
            request
                .messages
                .insert(insert_at, Message::User { content: context });
        }
        Ok(pi_rs_core::ContextChange::Changed)
    }

    fn before_compaction(
        &self,
        compaction: &mut Compaction,
        _: Option<&ExtensionState>,
    ) -> ExtensionResult<Decision> {
        compaction.keep_turns = compaction.keep_turns.max(self.keep_turns);
        Ok(Decision::Allow)
    }
}

fn valid_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 200
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-_.:".contains(&byte))
}
