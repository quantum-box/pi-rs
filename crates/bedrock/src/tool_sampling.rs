use aws_sdk_bedrockruntime::types;
use futures_util::StreamExt;
use pi_rs_core::{
    DetailedModelStream, EffectiveToolDeclaration, EffectiveToolSampling, Error, ModelRequest,
    PreparedToolDispatch, Result, STRICT_TOOL_NORMALIZATION_VERSION, ToolConstraint,
    ToolDecisionSnapshot, ToolDeclaration, ToolDispatchContext, ToolSpec,
};
use serde_json::Value;

use crate::{Bedrock, build_error, doc};

pub(crate) struct Prepared {
    client: Bedrock,
    request: ModelRequest,
    reservation: ModelRequest,
    tools: Vec<types::Tool>,
    decisions: Vec<EffectiveToolDeclaration>,
    snapshot: ToolDecisionSnapshot,
}

impl Prepared {
    pub(crate) fn new(
        client: Bedrock,
        request: &ModelRequest,
        declarations: &[ToolDeclaration],
        context: &ToolDispatchContext,
        supports_strict: bool,
    ) -> Result<Self> {
        if request.tools.len() != declarations.len() {
            return Err(Error::Config(
                "tool declarations do not match the effective request".into(),
            ));
        }
        let mut reservation = request.clone();
        let mut tools = Vec::with_capacity(declarations.len());
        let mut decisions = Vec::with_capacity(declarations.len());
        for (reserved, declaration) in reservation.tools.iter_mut().zip(declarations) {
            let spec = &declaration.spec;
            let schema = match declaration.constrained_sampling {
                Some(ToolConstraint::JsonSchema { strict }) => {
                    pi_rs_core::resolve_strict_tool_schema(
                        &spec.name,
                        &spec.parameters,
                        strict,
                        supports_strict,
                        |_, _| false,
                    )
                    .map_err(|error| Error::Config(error.to_string()))?
                }
                _ => None,
            };
            let sampling = if schema.is_some() {
                EffectiveToolSampling::StrictJsonSchema {
                    original_schema: spec.parameters.clone(),
                    normalization_version: STRICT_TOOL_NORMALIZATION_VERSION,
                }
            } else {
                EffectiveToolSampling::Unconstrained
            };
            tools.push(wire_tool(spec, schema.as_ref())?);
            decisions.push(EffectiveToolDeclaration {
                spec: spec.clone(),
                sampling,
            });
            if let Some(schema) = schema {
                reserved.parameters = schema;
            }
        }
        let snapshot = ToolDecisionSnapshot::capture(context, request, declarations, &decisions)
            .map_err(|error| Error::Config(error.to_string()))?;
        Ok(Self {
            client,
            request: request.clone(),
            reservation,
            tools,
            decisions,
            snapshot,
        })
    }
}

#[async_trait::async_trait]
impl PreparedToolDispatch for Prepared {
    fn effective_tools(&self) -> &[EffectiveToolDeclaration] {
        &self.decisions
    }
    fn decision_snapshot(&self) -> &ToolDecisionSnapshot {
        &self.snapshot
    }
    fn dispatch_request(&self) -> &ModelRequest {
        &self.request
    }
    fn reservation_request(&self) -> &ModelRequest {
        &self.reservation
    }
    async fn dispatch(self: Box<Self>) -> Result<DetailedModelStream> {
        Ok(Box::pin(
            self.client
                .stream_with_tools(self.request, Some(self.tools))
                .await?
                .map(|event| {
                    event.map(|event| pi_rs_core::EnrichedModelEvent::Legacy(event).into())
                }),
        ))
    }
}

pub(crate) fn wire_tool(spec: &ToolSpec, strict_schema: Option<&Value>) -> Result<types::Tool> {
    let tool = types::ToolSpecification::builder()
        .name(&spec.name)
        .description(&spec.description)
        .input_schema(types::ToolInputSchema::Json(doc(strict_schema
            .unwrap_or(&spec.parameters)
            .clone())))
        .set_strict(strict_schema.map(|_| true))
        .build()
        .map_err(build_error)?;
    Ok(types::Tool::ToolSpec(tool))
}
