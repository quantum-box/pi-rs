//! Durable binding for the exact arguments authorized for a tool invocation.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{Error, Invocation, Result, ToolSpec};

const MAX_ARGUMENT_BYTES: usize = 16 * 1024 * 1024;

fn digest(value: &impl Serialize) -> Result<String> {
    Ok(format!("{:x}", Sha256::digest(serde_json::to_vec(value)?)))
}

fn is_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

/// Authorized arguments bound to an invocation, registered tool and decision.
/// Persist atomically with the tool-start record and independently retain the
/// binding digest before using this value for recovery. This is a consistency
/// check, not authentication against an attacker who can rewrite the journal.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolExecutionSnapshot {
    version: u32,
    request_id: Uuid,
    operation_id: Uuid,
    invocation_sha256: String,
    owner: String,
    spec_sha256: String,
    recovery_identity: String,
    decision_sha256: String,
    authorized_arguments: Value,
    binding_sha256: String,
}

/// Arguments exposed only after checking the operation and durable binding.
pub struct ValidatedToolExecutionSnapshot<'a> {
    snapshot: &'a ToolExecutionSnapshot,
}

impl ValidatedToolExecutionSnapshot<'_> {
    pub fn arguments(&self) -> &Value {
        &self.snapshot.authorized_arguments
    }
}

impl ToolExecutionSnapshot {
    /// Capture the final arguments after preparation and authorization hooks.
    /// The identity must identify the implementation and backend across restarts;
    /// a tool without such an identity cannot use this snapshot for recovery.
    /// The identity is persisted and must not contain plaintext credentials.
    /// The caller must also check the complete durable record size before starting.
    pub fn capture(
        request_id: Uuid,
        invocation: &Invocation,
        owner: &str,
        spec: &ToolSpec,
        recovery_identity: &str,
        decision_sha256: &str,
        authorized_arguments: Value,
    ) -> Result<Self> {
        if request_id.is_nil()
            || invocation.operation_id.is_nil()
            || owner.is_empty()
            || recovery_identity.is_empty()
            || invocation.call.name != spec.name
            || !is_digest(decision_sha256)
            || serde_json::to_vec(&authorized_arguments)?.len() > MAX_ARGUMENT_BYTES
        {
            return Err(Error::Session(
                "invalid tool execution snapshot input".into(),
            ));
        }
        let mut snapshot = Self {
            version: 1,
            request_id,
            operation_id: invocation.operation_id,
            invocation_sha256: digest(invocation)?,
            owner: owner.into(),
            spec_sha256: digest(spec)?,
            recovery_identity: recovery_identity.into(),
            decision_sha256: decision_sha256.into(),
            authorized_arguments,
            binding_sha256: String::new(),
        };
        snapshot.binding_sha256 = snapshot.binding()?;
        Ok(snapshot)
    }

    fn binding(&self) -> Result<String> {
        digest(&(
            self.version,
            self.request_id,
            self.operation_id,
            &self.invocation_sha256,
            &self.owner,
            &self.spec_sha256,
            &self.recovery_identity,
            &self.decision_sha256,
            &self.authorized_arguments,
        ))
    }

    /// Digest to store independently in the associated tool-start record.
    pub fn binding_sha256(&self) -> &str {
        &self.binding_sha256
    }

    /// Check against independently retained identity and decision information.
    /// The recovery identity must come from the currently registered backend.
    #[allow(clippy::too_many_arguments)]
    pub fn validate<'a>(
        &'a self,
        request_id: Uuid,
        invocation: &Invocation,
        owner: &str,
        spec: &ToolSpec,
        recovery_identity: &str,
        decision_sha256: &str,
        expected_binding_sha256: &str,
    ) -> Result<ValidatedToolExecutionSnapshot<'a>> {
        if self.version != 1
            || request_id.is_nil()
            || self.request_id != request_id
            || self.operation_id != invocation.operation_id
            || invocation.operation_id.is_nil()
            || self.invocation_sha256 != digest(invocation)?
            || owner.is_empty()
            || self.owner != owner
            || invocation.call.name != spec.name
            || self.spec_sha256 != digest(spec)?
            || recovery_identity.is_empty()
            || self.recovery_identity != recovery_identity
            || !is_digest(decision_sha256)
            || self.decision_sha256 != decision_sha256
            || !is_digest(expected_binding_sha256)
            || self.binding_sha256 != expected_binding_sha256
            || self.binding_sha256 != self.binding()?
            || serde_json::to_vec(&self.authorized_arguments)?.len() > MAX_ARGUMENT_BYTES
        {
            return Err(Error::Session("tool execution snapshot mismatch".into()));
        }
        Ok(ValidatedToolExecutionSnapshot { snapshot: self })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ToolCall;
    use serde_json::json;

    #[test]
    fn validation_binds_authorized_arguments_to_the_original_operation() {
        let request_id = Uuid::new_v4();
        let invocation = Invocation {
            operation_id: Uuid::new_v4(),
            call: ToolCall {
                id: "call".into(),
                name: "edit".into(),
                arguments: "{\"old\":null}".into(),
            },
        };
        let spec = ToolSpec {
            name: "edit".into(),
            description: "edit".into(),
            parameters: json!({"type":"object"}),
        };
        let decision = "a".repeat(64);
        let snapshot = ToolExecutionSnapshot::capture(
            request_id,
            &invocation,
            "owner",
            &spec,
            "backend-v1",
            &decision,
            json!({"final":"authorized"}),
        )
        .unwrap();
        let binding = snapshot.binding_sha256().to_owned();
        assert!(
            snapshot
                .validate(
                    request_id,
                    &invocation,
                    "owner",
                    &spec,
                    "replacement-backend",
                    &decision,
                    &binding,
                )
                .is_err()
        );
        assert_eq!(
            snapshot
                .validate(
                    request_id,
                    &invocation,
                    "owner",
                    &spec,
                    "backend-v1",
                    &decision,
                    &binding
                )
                .unwrap()
                .arguments(),
            &json!({"final":"authorized"})
        );

        let mut changed_invocation = invocation.clone();
        changed_invocation.call.arguments = "{}".into();
        assert!(
            snapshot
                .validate(
                    request_id,
                    &changed_invocation,
                    "owner",
                    &spec,
                    "backend-v1",
                    &decision,
                    &binding
                )
                .is_err()
        );
        let mut changed_spec = spec.clone();
        changed_spec.parameters = json!({"type":"object","additionalProperties":false});
        assert!(
            snapshot
                .validate(
                    request_id,
                    &invocation,
                    "owner",
                    &changed_spec,
                    "backend-v1",
                    &decision,
                    &binding
                )
                .is_err()
        );
        assert!(
            snapshot
                .validate(
                    request_id,
                    &invocation,
                    "other",
                    &spec,
                    "backend-v1",
                    &decision,
                    &binding
                )
                .is_err()
        );
        assert!(
            snapshot
                .validate(
                    Uuid::new_v4(),
                    &invocation,
                    "owner",
                    &spec,
                    "backend-v1",
                    &decision,
                    &binding
                )
                .is_err()
        );
        assert!(
            snapshot
                .validate(
                    request_id,
                    &invocation,
                    "owner",
                    &spec,
                    "backend-v1",
                    &"b".repeat(64),
                    &binding
                )
                .is_err()
        );

        let mut tampered = snapshot.clone();
        tampered.authorized_arguments = json!({"final":"modified"});
        assert!(
            tampered
                .validate(
                    request_id,
                    &invocation,
                    "owner",
                    &spec,
                    "backend-v1",
                    &decision,
                    &binding
                )
                .is_err()
        );
        assert!(
            snapshot
                .validate(
                    request_id,
                    &invocation,
                    "owner",
                    &spec,
                    "backend-v1",
                    &decision,
                    &"b".repeat(64)
                )
                .is_err()
        );
    }
}
