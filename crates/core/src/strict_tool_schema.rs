//! Conversion of tool schemas to the strict subset used by model providers.
use std::collections::BTreeMap;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

/// A tool schema cannot be represented in the provider's strict subset.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct StrictSchemaError(String);

/// Version of the strict-tool argument normalization rules.
pub const STRICT_TOOL_NORMALIZATION_VERSION: u32 = 1;

/// Whether strict tool sampling may fall back to ordinary tool calling.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StrictToolPolicy {
    /// Use strict mode only when the provider and schema support it.
    Prefer,
    /// Fail rather than send an unconstrained tool.
    Require,
}

/// Provider-specific grammar alternatives for constrained tool sampling.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolGrammarVariants {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub openai_lark: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub openai_regex: Option<String>,
}

/// Grammar formats recognized by OpenAI custom tools.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolGrammarSyntax {
    Lark,
    Regex,
}

/// An optional tool constraint; `Disabled` preserves Pi's explicit `false`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ToolConstraint {
    Disabled,
    JsonSchema { strict: StrictToolPolicy },
    Grammar { variants: ToolGrammarVariants },
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ConstraintObject {
    JsonSchema { strict: StrictToolPolicy },
    Grammar { variants: ToolGrammarVariants },
}

impl Serialize for ToolConstraint {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Disabled => serializer.serialize_bool(false),
            Self::JsonSchema { strict } => {
                ConstraintObject::JsonSchema { strict: *strict }.serialize(serializer)
            }
            Self::Grammar { variants } => ConstraintObject::Grammar {
                variants: variants.clone(),
            }
            .serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for ToolConstraint {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = Value::deserialize(deserializer)?;
        if value == Value::Bool(false) {
            return Ok(Self::Disabled);
        }
        let object: ConstraintObject =
            serde_json::from_value(value).map_err(serde::de::Error::custom)?;
        Ok(match object {
            ConstraintObject::JsonSchema { strict } => Self::JsonSchema { strict },
            ConstraintObject::Grammar { variants } => Self::Grammar { variants },
        })
    }
}

/// A tool definition and its optional sampling constraint, in Pi's wire shape.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolDeclaration {
    #[serde(flatten)]
    pub spec: crate::ToolSpec,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_constraint"
    )]
    pub constrained_sampling: Option<ToolConstraint>,
}

/// Pick the OpenAI grammar variant and the argument that carries its raw input.
pub fn resolve_openai_tool_grammar(
    tool: &ToolDeclaration,
) -> Result<Option<(ToolGrammarSyntax, &str, &str)>, StrictSchemaError> {
    let Some(ToolConstraint::Grammar { variants }) = &tool.constrained_sampling else {
        return Ok(None);
    };
    let failure = |reason: &str| {
        StrictSchemaError(format!(
            "tool {:?} cannot use grammar constrained sampling: {reason}",
            tool.spec.name
        ))
    };
    let (syntax, definition) = variants
        .openai_lark
        .as_deref()
        .filter(|definition| !definition.trim().is_empty())
        .map(|definition| (ToolGrammarSyntax::Lark, definition))
        .or_else(|| {
            variants
                .openai_regex
                .as_deref()
                .filter(|definition| !definition.trim().is_empty())
                .map(|definition| (ToolGrammarSyntax::Regex, definition))
        })
        .ok_or_else(|| failure("no supported grammar variant was provided"))?;
    let schema = &tool.spec.parameters;
    if schema["type"] != "object" {
        return Err(failure("the parameter schema must be an object"));
    }
    let property = schema["required"]
        .as_array()
        .and_then(|required| match required.as_slice() {
            [property] => property.as_str(),
            _ => None,
        })
        .ok_or_else(|| failure("the parameter schema must have exactly one required property"))?;
    if schema["properties"][property]["type"] != "string" {
        return Err(failure("the required property must have type string"));
    }
    Ok(Some((syntax, definition, property)))
}

/// Recover raw grammar input only if representing it as a string loses no arguments.
pub fn lossless_openai_grammar_input(arguments: &str, property: &str) -> Option<String> {
    let parsed = crate::parse_tool_arguments(arguments).ok()?;
    let object = parsed.as_object().filter(|object| object.len() == 1)?;
    object.get(property)?.as_str().map(str::to_owned)
}

/// The actual sampling decision for one advertised tool.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum EffectiveToolSampling {
    Unconstrained,
    OpenAiGrammar {
        syntax: ToolGrammarSyntax,
        input_property: String,
    },
    StrictJsonSchema {
        original_schema: Value,
        normalization_version: u32,
    },
}

/// A tool definition bound to the sampling actually used for its request.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EffectiveToolDeclaration {
    pub spec: crate::ToolSpec,
    pub sampling: EffectiveToolSampling,
}

/// Versioned decisions bound to the exact model request and dispatch context.
/// This value must be committed atomically with the request record before it
/// can be used for recovery; serialization alone is not a durable commit.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolDecisionSnapshot {
    version: u32,
    context: crate::ToolDispatchContext,
    request_sha256: String,
    dispatch_sha256: String,
    binding_sha256: String,
    declarations: Vec<ToolDeclaration>,
    effective: Vec<EffectiveToolDeclaration>,
    // Local execution contracts can differ from the tools advertised by a
    // request hook. Provider snapshots leave this field unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    registered_tools: Option<BTreeMap<String, String>>,
}

/// A decision snapshot checked against its request and binding.
pub struct ValidatedToolDecisionSnapshot<'a> {
    snapshot: &'a ToolDecisionSnapshot,
}

impl ValidatedToolDecisionSnapshot<'_> {
    pub fn effective_tools(&self) -> &[EffectiveToolDeclaration] {
        &self.snapshot.effective
    }

    /// Parse a model call and normalize it only when this validated decision
    /// says that strict JSON Schema sampling was actually used. The caller
    /// must independently establish the call's request and branch provenance.
    pub fn normalized_arguments(&self, call: &crate::ToolCall) -> Result<Value, StrictSchemaError> {
        let decision = self
            .snapshot
            .effective
            .iter()
            .find(|entry| entry.spec.name == call.name)
            .ok_or_else(|| StrictSchemaError("tool was not in the decision snapshot".into()))?;
        let arguments = crate::parse_tool_arguments(&call.arguments)
            .map_err(|error| StrictSchemaError(format!("invalid tool arguments: {error}")))?;
        match &decision.sampling {
            EffectiveToolSampling::Unconstrained => Ok(arguments),
            EffectiveToolSampling::OpenAiGrammar { input_property, .. } => {
                if arguments.as_object().is_some_and(|object| {
                    object.len() == 1 && object.get(input_property).is_some_and(Value::is_string)
                }) {
                    Ok(arguments)
                } else {
                    Err(StrictSchemaError(
                        "invalid OpenAI grammar tool input".into(),
                    ))
                }
            }
            EffectiveToolSampling::StrictJsonSchema {
                original_schema,
                normalization_version: STRICT_TOOL_NORMALIZATION_VERSION,
            } => normalize_strict_tool_arguments(original_schema, &arguments),
            EffectiveToolSampling::StrictJsonSchema { .. } => Err(StrictSchemaError(
                "unsupported strict-tool normalization version".into(),
            )),
        }
    }
}

impl ToolDecisionSnapshot {
    /// Validate and capture a decision against its final request.
    pub fn capture(
        context: &crate::ToolDispatchContext,
        request: &crate::ModelRequest,
        declarations: &[ToolDeclaration],
        effective: &[EffectiveToolDeclaration],
    ) -> Result<Self, StrictSchemaError> {
        Self::capture_dispatch(context, request, request, declarations, effective)
    }

    /// Bind both the caller's request and the request after host filtering.
    pub fn capture_dispatch(
        context: &crate::ToolDispatchContext,
        request: &crate::ModelRequest,
        dispatch_request: &crate::ModelRequest,
        declarations: &[ToolDeclaration],
        effective: &[EffectiveToolDeclaration],
    ) -> Result<Self, StrictSchemaError> {
        validate_effective_tool_declarations(request, declarations, effective)?;
        validate_effective_tool_declarations(dispatch_request, declarations, effective)?;
        let request_bytes = serde_json::to_vec(request)
            .map_err(|error| StrictSchemaError(format!("cannot encode model request: {error}")))?;
        let request_sha256 = format!("{:x}", Sha256::digest(request_bytes));
        let dispatch_bytes = serde_json::to_vec(dispatch_request).map_err(|error| {
            StrictSchemaError(format!("cannot encode dispatch request: {error}"))
        })?;
        let dispatch_sha256 = format!("{:x}", Sha256::digest(dispatch_bytes));
        let mut snapshot = Self {
            version: 1,
            context: context.clone(),
            request_sha256,
            dispatch_sha256,
            binding_sha256: String::new(),
            declarations: declarations.to_vec(),
            effective: effective.to_vec(),
            registered_tools: None,
        };
        snapshot.binding_sha256 = snapshot.binding_digest()?;
        Ok(snapshot)
    }

    fn binding_digest(&self) -> Result<String, StrictSchemaError> {
        let provider = (
            self.version,
            &self.context,
            &self.request_sha256,
            &self.dispatch_sha256,
            &self.declarations,
            &self.effective,
        );
        // Keep existing provider snapshot digests and older journal records
        // compatible when they have no separate registration binding.
        match &self.registered_tools {
            Some(tools) => schema_digest(&(provider, tools)),
            None => schema_digest(&provider),
        }
    }

    pub(crate) fn with_registered_tools(
        mut self,
        registry: &crate::Registry,
    ) -> Result<Self, StrictSchemaError> {
        self.registered_tools = Some(
            self.effective
                .iter()
                .map(|entry| {
                    let name = &entry.spec.name;
                    let tool = registry.tools.get(name).ok_or_else(|| {
                        StrictSchemaError(format!("tool {name:?} is not registered"))
                    })?;
                    Ok((name.clone(), schema_digest(&(&tool.owner, &tool.spec))?))
                })
                .collect::<Result<_, StrictSchemaError>>()?,
        );
        self.binding_sha256 = self.binding_digest()?;
        Ok(self)
    }

    pub(crate) fn matches_registered_tool(
        &self,
        owner: &str,
        spec: &crate::ToolSpec,
    ) -> Result<bool, StrictSchemaError> {
        let Some(advertised) = self
            .effective
            .iter()
            .find(|entry| entry.spec.name == spec.name)
        else {
            return Ok(false);
        };
        match &self.registered_tools {
            Some(tools) => Ok(tools.get(&spec.name) == Some(&schema_digest(&(owner, spec))?)),
            None => Ok(&advertised.spec == spec),
        }
    }

    /// Check the request, context and effective decisions before use.
    pub fn validate(
        &self,
        context: &crate::ToolDispatchContext,
        request: &crate::ModelRequest,
        dispatch_request: &crate::ModelRequest,
        declarations: &[ToolDeclaration],
    ) -> Result<ValidatedToolDecisionSnapshot<'_>, StrictSchemaError> {
        if self.version != 1 || &self.context != context || self.declarations != declarations {
            return Err(StrictSchemaError(
                "tool decision snapshot context or version mismatch".into(),
            ));
        }
        let expected = Self::capture_dispatch(
            context,
            request,
            dispatch_request,
            declarations,
            &self.effective,
        )?;
        let binding_sha256 = if self.registered_tools.is_some() {
            self.binding_digest()?
        } else {
            expected.binding_sha256
        };
        if self.request_sha256 != expected.request_sha256
            || self.dispatch_sha256 != expected.dispatch_sha256
            || self.binding_sha256 != binding_sha256
        {
            return Err(StrictSchemaError(
                "tool decision snapshot binding mismatch".into(),
            ));
        }
        Ok(ValidatedToolDecisionSnapshot { snapshot: self })
    }

    /// Verify against digests retained independently in a durable request record.
    /// Passing the snapshot's own digests does not establish its association.
    /// This detects inconsistent records; it does not authenticate a journal.
    pub fn validate_recorded(
        &self,
        context: &crate::ToolDispatchContext,
        request_sha256: &str,
        dispatch_sha256: &str,
        expected_binding_sha256: &str,
    ) -> Result<ValidatedToolDecisionSnapshot<'_>, StrictSchemaError> {
        if self.version != 1
            || &self.context != context
            || self.request_sha256 != request_sha256
            || self.dispatch_sha256 != dispatch_sha256
            || self.binding_sha256 != expected_binding_sha256
        {
            return Err(StrictSchemaError(
                "tool decision snapshot context or request mismatch".into(),
            ));
        }
        let request = crate::ModelRequest {
            model: String::new(),
            messages: vec![],
            tools: self
                .declarations
                .iter()
                .map(|entry| entry.spec.clone())
                .collect(),
            max_output_tokens: 1,
        };
        validate_effective_tool_declarations(&request, &self.declarations, &self.effective)?;
        if self.binding_sha256 != self.binding_digest()? {
            return Err(StrictSchemaError(
                "tool decision snapshot binding mismatch".into(),
            ));
        }
        Ok(ValidatedToolDecisionSnapshot { snapshot: self })
    }

    /// Original dispatch identity captured with this snapshot.
    pub fn context(&self) -> &crate::ToolDispatchContext {
        &self.context
    }

    /// Digest of the exact request used to capture this snapshot.
    pub fn request_sha256(&self) -> &str {
        &self.request_sha256
    }

    /// Digest of the request after host filtering.
    pub fn dispatch_sha256(&self) -> &str {
        &self.dispatch_sha256
    }

    /// Digest to commit independently with the durable request record.
    pub fn binding_sha256(&self) -> &str {
        &self.binding_sha256
    }
}

fn schema_digest(value: &impl Serialize) -> Result<String, StrictSchemaError> {
    let bytes = serde_json::to_vec(value).map_err(|error| {
        StrictSchemaError(format!("cannot encode tool decision binding: {error}"))
    })?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

/// Validate effective decisions against the final request and declarations.
/// This rejects downgraded required constraints, stale schemas and unknown
/// normalization versions before a request is dispatched or recorded.
pub fn validate_effective_tool_declarations(
    request: &crate::ModelRequest,
    declarations: &[ToolDeclaration],
    effective: &[EffectiveToolDeclaration],
) -> Result<(), StrictSchemaError> {
    if request.tools.len() != declarations.len() || declarations.len() != effective.len() {
        return Err(StrictSchemaError(
            "tool declaration count does not match the request".into(),
        ));
    }
    let mut names = std::collections::BTreeSet::new();
    for ((spec, declaration), decision) in request.tools.iter().zip(declarations).zip(effective) {
        if spec != &declaration.spec || spec != &decision.spec || !names.insert(&spec.name) {
            return Err(StrictSchemaError(
                "tool declarations do not match the effective request".into(),
            ));
        }
        match (&declaration.constrained_sampling, &decision.sampling) {
            (
                Some(ToolConstraint::JsonSchema {
                    strict: StrictToolPolicy::Require,
                }),
                EffectiveToolSampling::Unconstrained,
            ) => {
                return Err(StrictSchemaError(format!(
                    "required strict constraint for tool {:?} was not applied",
                    spec.name
                )));
            }
            (
                Some(ToolConstraint::JsonSchema { .. }),
                EffectiveToolSampling::StrictJsonSchema {
                    original_schema,
                    normalization_version,
                },
            ) => {
                if original_schema != &spec.parameters
                    || *normalization_version != STRICT_TOOL_NORMALIZATION_VERSION
                {
                    return Err(StrictSchemaError(format!(
                        "invalid strict normalization decision for tool {:?}",
                        spec.name
                    )));
                }
                null_normalization_plan(original_schema)?;
            }
            (_, EffectiveToolSampling::StrictJsonSchema { .. }) => {
                return Err(StrictSchemaError(format!(
                    "unrequested strict constraint for tool {:?}",
                    spec.name
                )));
            }
            (
                _,
                EffectiveToolSampling::OpenAiGrammar {
                    syntax,
                    input_property,
                },
            ) => {
                if !resolve_openai_tool_grammar(declaration)?.is_some_and(
                    |(format, _, property)| format == *syntax && property == input_property,
                ) {
                    return Err(StrictSchemaError(format!(
                        "invalid grammar decision for tool {:?}",
                        spec.name
                    )));
                }
            }
            (_, EffectiveToolSampling::Unconstrained) => {}
        }
    }
    Ok(())
}

fn deserialize_constraint<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<ToolConstraint>, D::Error> {
    ToolConstraint::deserialize(deserializer).map(Some)
}

/// Resolve a strict schema for a tool, applying its fallback policy.
///
/// Schemas must also support safe argument normalization before they can be
/// advertised as strict. Unsupported normalization falls back or fails.
pub fn resolve_strict_tool_schema(
    name: &str,
    schema: &Value,
    policy: StrictToolPolicy,
    supported: bool,
    unsupported: impl Fn(&str, &Value) -> bool,
) -> Result<Option<Value>, StrictSchemaError> {
    let failure = if supported {
        match strict_tool_schema(schema, unsupported)
            .and_then(|strict| null_normalization_plan(schema).map(|_| strict))
        {
            Ok(schema) => return Ok(Some(schema)),
            Err(error) => error.0,
        }
    } else {
        "strict tools are unsupported".into()
    };
    match policy {
        StrictToolPolicy::Prefer => Ok(None),
        StrictToolPolicy::Require => Err(StrictSchemaError(format!(
            "Tool \"{name}\" requires JSON-schema constrained sampling, but {failure}."
        ))),
    }
}

/// Normalize a copy of strict-tool arguments against their original schema.
///
/// Strict schemas make optional fields required and nullable. A null supplied
/// for an optional field that rejects null is treated as an omitted field. The
/// original arguments remain available for journaling and provider replay.
/// Schemas must pass [`strict_tool_schema`]; dynamic references and nested
/// dialect declarations are also rejected.
pub fn normalize_strict_tool_arguments(
    schema: &Value,
    arguments: &Value,
) -> Result<Value, StrictSchemaError> {
    let plan = null_normalization_plan(schema)?;
    let mut normalized = arguments.clone();
    plan.apply(&mut normalized);
    Ok(normalized)
}

fn null_normalization_plan(schema: &Value) -> Result<NullNormalizationPlan, StrictSchemaError> {
    strict_tool_schema(schema, |key, _| {
        matches!(key, "$dynamicRef" | "$recursiveRef")
    })?;
    reject_nested_dialects(schema, true)?;
    NullNormalizationPlan::build(schema, schema.get("$schema"))
}

fn reject_nested_dialects(schema: &Value, root: bool) -> Result<(), StrictSchemaError> {
    if !root && schema.get("$schema").is_some() {
        return Err(StrictSchemaError(
            "nested schema dialects are unsupported".into(),
        ));
    }
    if let Some(properties) = schema.get("properties").and_then(Value::as_object) {
        for property in properties.values() {
            reject_nested_dialects(property, false)?;
        }
    }
    if let Some(item) = schema.get("items") {
        reject_nested_dialects(item, false)?;
    }
    if let Some(variants) = schema.get("anyOf").and_then(Value::as_array) {
        for variant in variants {
            reject_nested_dialects(variant, false)?;
        }
    }
    Ok(())
}

#[derive(Default)]
struct NullNormalizationPlan {
    fields: Vec<(String, bool, Self)>,
    items: Option<Box<Self>>,
}

impl NullNormalizationPlan {
    fn build(schema: &Value, dialect: Option<&Value>) -> Result<Self, StrictSchemaError> {
        let mut plan = Self::default();
        if let Some(item) = schema.get("items") {
            plan.items = Some(Box::new(Self::build(item, dialect)?));
        }
        if let Some(properties) = schema.get("properties").and_then(Value::as_object) {
            let required = schema.get("required").and_then(Value::as_array);
            for (key, property) in properties {
                let is_required =
                    required.is_some_and(|items| items.iter().any(|v| v.as_str() == Some(key)));
                let rejects_null = if is_required {
                    false
                } else {
                    let mut candidate = property.clone();
                    if let (Some(dialect), Some(object)) = (dialect, candidate.as_object_mut()) {
                        object.insert("$schema".into(), dialect.clone());
                    }
                    let validator = jsonschema::validator_for(&candidate).map_err(|error| {
                        StrictSchemaError(format!(
                            "cannot validate optional property {key}: {error}"
                        ))
                    })?;
                    !validator.is_valid(&Value::Null)
                };
                plan.fields
                    .push((key.clone(), rejects_null, Self::build(property, dialect)?));
            }
        }
        Ok(plan)
    }

    fn apply(&self, value: &mut Value) {
        if let Some(items) = value.as_array_mut() {
            if let Some(plan) = &self.items {
                for item in items {
                    plan.apply(item);
                }
            }
            return;
        }
        if let Some(object) = value.as_object_mut() {
            for (key, rejects_null, plan) in &self.fields {
                if *rejects_null && object.get(key) == Some(&Value::Null) {
                    object.remove(key);
                } else if let Some(item) = object.get_mut(key) {
                    plan.apply(item);
                }
            }
        }
    }
}

/// Clone a tool schema and make object properties required and closed.
///
/// Optional properties are made nullable. `unsupported` checks additional
/// provider-specific keywords and is called for every schema node.
pub fn strict_tool_schema(
    schema: &Value,
    unsupported: impl Fn(&str, &Value) -> bool,
) -> Result<Value, StrictSchemaError> {
    if !schema.is_object() {
        return Err(StrictSchemaError(
            "root schema must have type object".into(),
        ));
    }
    let mut result = schema.clone();
    make_strict(&mut result, &unsupported)?;
    if result.get("type").and_then(Value::as_str) != Some("object") {
        return Err(StrictSchemaError(
            "root schema must have type object".into(),
        ));
    }
    Ok(result)
}

fn js_order(names: &mut [String]) {
    // JavaScript enumerates canonical array indices before other keys.
    names.sort_by_key(|name| {
        name.parse::<u32>()
            .ok()
            .filter(|index| *index != u32::MAX && index.to_string() == *name)
            .map_or((1, 0), |index| (0, index))
    });
}

fn structured(value: &Value) -> bool {
    let Some(schema) = value.as_object() else {
        return false;
    };
    let kind = schema.get("type");
    kind.and_then(Value::as_str)
        .is_some_and(|v| matches!(v, "object" | "array"))
        || kind.and_then(Value::as_array).is_some_and(|v| {
            v.iter()
                .any(|t| matches!(t.as_str(), Some("object" | "array")))
        })
        || schema.contains_key("properties")
        || schema.contains_key("items")
}

fn allows_null(value: &Value) -> bool {
    let Some(schema) = value.as_object() else {
        return false;
    };
    let kind = schema.get("type");
    kind == Some(&json!("null"))
        || kind
            .and_then(Value::as_array)
            .is_some_and(|a| a.contains(&json!("null")))
        || schema.get("const") == Some(&Value::Null)
        || schema
            .get("enum")
            .and_then(Value::as_array)
            .is_some_and(|a| a.contains(&Value::Null))
        || schema
            .get("anyOf")
            .and_then(Value::as_array)
            .is_some_and(|a| a.iter().any(allows_null))
}

fn make_strict(
    value: &mut Value,
    unsupported: &impl Fn(&str, &Value) -> bool,
) -> Result<(), StrictSchemaError> {
    let schema = value
        .as_object_mut()
        .ok_or_else(|| StrictSchemaError("boolean schemas are unsupported".into()))?;
    for key in [
        "$ref",
        "$defs",
        "definitions",
        "allOf",
        "oneOf",
        "patternProperties",
        "dependentSchemas",
        "dependencies",
        "unevaluatedProperties",
        "propertyNames",
        "contains",
        "prefixItems",
        "not",
        "if",
        "then",
        "else",
    ] {
        if schema.contains_key(key) {
            return Err(StrictSchemaError(format!("{key} schemas are unsupported")));
        }
    }
    let mut schema_keys: Vec<_> = schema.keys().cloned().collect();
    js_order(&mut schema_keys);
    for key in &schema_keys {
        let value = &schema[key];
        if unsupported(key, value) {
            return Err(StrictSchemaError(format!("{key}: {value} is unsupported")));
        }
    }
    if !schema.contains_key("type") && !schema.contains_key("anyOf") && !schema.contains_key("enum")
    {
        return Err(StrictSchemaError(
            "schemas without a type are unsupported".into(),
        ));
    }
    if let Some(any_of) = schema.get_mut("anyOf") {
        let variants = any_of
            .as_array_mut()
            .filter(|items| !items.is_empty())
            .ok_or_else(|| StrictSchemaError("anyOf must contain at least one schema".into()))?;
        for variant in variants {
            if structured(variant) {
                return Err(StrictSchemaError(
                    "object and array unions are unsupported".into(),
                ));
            }
            make_strict(variant, unsupported)?;
        }
    }
    if let Some(items) = schema.get_mut("items") {
        if items.is_array() {
            return Err(StrictSchemaError("tuple schemas are unsupported".into()));
        }
        make_strict(items, unsupported)?;
    }
    if schema.get("type").and_then(Value::as_str) != Some("object") {
        if schema.contains_key("properties") {
            return Err(StrictSchemaError("properties require type object".into()));
        }
        return Ok(());
    }
    if schema
        .get("additionalProperties")
        .is_some_and(|v| v != &Value::Bool(false))
    {
        return Err(StrictSchemaError(
            "schema-valued or true additionalProperties is unsupported".into(),
        ));
    }
    if schema
        .get("properties")
        .is_some_and(|value| !value.is_object())
    {
        return Err(StrictSchemaError(
            "object properties must be a schema map".into(),
        ));
    }
    let required = match schema.get("required") {
        None => Vec::new(),
        Some(Value::Array(items)) if items.iter().all(Value::is_string) => items
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_owned)
            .collect(),
        Some(_) => {
            return Err(StrictSchemaError(
                "object required must be a string array".into(),
            ));
        }
    };
    if !schema.contains_key("properties") {
        if !required.is_empty() {
            return Err(StrictSchemaError(
                "required contains an unknown property".into(),
            ));
        }
        schema.insert("required".into(), json!([]));
        schema.insert("additionalProperties".into(), Value::Bool(false));
        return Ok(());
    }
    let properties = schema
        .get_mut("properties")
        .and_then(Value::as_object_mut)
        .ok_or_else(|| StrictSchemaError("object properties must be a schema map".into()))?;
    if required.iter().any(|key| !properties.contains_key(key)) {
        return Err(StrictSchemaError(
            "required contains an unknown property".into(),
        ));
    }
    let mut names: Vec<_> = properties.keys().cloned().collect();
    js_order(&mut names);
    let mut ordered = serde_json::Map::new();
    for name in &names {
        let mut property = properties.remove(name).expect("known property");
        make_strict(&mut property, unsupported)?;
        if !required.contains(name) && !allows_null(&property) {
            property = json!({"anyOf":[property,{"type":"null"}]});
        }
        ordered.insert(name.clone(), property);
    }
    *properties = ordered;
    schema.insert("required".into(), json!(names));
    schema.insert("additionalProperties".into(), Value::Bool(false));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validated_decision_normalizes_only_actual_strict_calls() {
        let spec = crate::ToolSpec {
            name: "read".into(),
            description: String::new(),
            parameters: json!({"type":"object", "properties":{"path":{"type":"string"},"limit":{"type":"integer"}}, "required":["path"]}),
        };
        let request = crate::ModelRequest {
            model: "m".into(),
            messages: vec![],
            tools: vec![spec.clone()],
            max_output_tokens: 10,
        };
        let context = crate::ToolDispatchContext {
            request_id: uuid::Uuid::new_v4(),
            session_id: uuid::Uuid::new_v4(),
            external_id: "external".into(),
            summary: false,
        };
        let declaration = ToolDeclaration {
            spec: spec.clone(),
            constrained_sampling: Some(ToolConstraint::JsonSchema {
                strict: StrictToolPolicy::Prefer,
            }),
        };
        let call = crate::ToolCall {
            id: "call".into(),
            name: "read".into(),
            arguments: r#"{"path":"file","limit":null}"#.into(),
        };
        for (sampling, expected) in [
            (
                EffectiveToolSampling::StrictJsonSchema {
                    original_schema: spec.parameters.clone(),
                    normalization_version: STRICT_TOOL_NORMALIZATION_VERSION,
                },
                json!({"path":"file"}),
            ),
            (
                EffectiveToolSampling::Unconstrained,
                json!({"path":"file","limit":null}),
            ),
        ] {
            let snapshot = ToolDecisionSnapshot::capture(
                &context,
                &request,
                std::slice::from_ref(&declaration),
                &[EffectiveToolDeclaration {
                    spec: spec.clone(),
                    sampling,
                }],
            )
            .unwrap();
            let validated = snapshot
                .validate(
                    &context,
                    &request,
                    &request,
                    std::slice::from_ref(&declaration),
                )
                .unwrap();
            assert_eq!(validated.normalized_arguments(&call).unwrap(), expected);
            let mut unknown = call.clone();
            unknown.name = "unknown".into();
            assert!(validated.normalized_arguments(&unknown).is_err());
        }
        assert!(call.arguments.contains("null"));
    }

    #[test]
    fn decision_snapshot_rejects_changed_request_context_and_version() {
        let spec = crate::ToolSpec {
            name: "read".into(),
            description: String::new(),
            parameters: json!({"type":"object"}),
        };
        let mut request = crate::ModelRequest {
            model: "m".into(),
            messages: vec![],
            tools: vec![spec.clone()],
            max_output_tokens: 10,
        };
        let mut context = crate::ToolDispatchContext {
            request_id: uuid::Uuid::new_v4(),
            session_id: uuid::Uuid::new_v4(),
            external_id: "external".into(),
            summary: false,
        };
        let declaration = ToolDeclaration {
            spec: spec.clone(),
            constrained_sampling: Some(ToolConstraint::JsonSchema {
                strict: StrictToolPolicy::Require,
            }),
        };
        let effective = EffectiveToolDeclaration {
            spec: spec.clone(),
            sampling: EffectiveToolSampling::StrictJsonSchema {
                original_schema: spec.parameters,
                normalization_version: STRICT_TOOL_NORMALIZATION_VERSION,
            },
        };
        let snapshot = ToolDecisionSnapshot::capture(
            &context,
            &request,
            std::slice::from_ref(&declaration),
            &[effective],
        )
        .unwrap();
        let encoded = serde_json::to_value(&snapshot).unwrap();
        let decoded: ToolDecisionSnapshot = serde_json::from_value(encoded.clone()).unwrap();
        assert!(
            decoded
                .validate(
                    &context,
                    &request,
                    &request,
                    std::slice::from_ref(&declaration)
                )
                .is_ok()
        );
        assert!(
            decoded
                .validate_recorded(
                    &context,
                    snapshot.request_sha256(),
                    snapshot.dispatch_sha256(),
                    snapshot.binding_sha256()
                )
                .is_ok()
        );
        assert!(
            decoded
                .validate_recorded(
                    &context,
                    "wrong",
                    snapshot.dispatch_sha256(),
                    snapshot.binding_sha256()
                )
                .is_err()
        );
        context.summary = true;
        assert!(
            decoded
                .validate(
                    &context,
                    &request,
                    &request,
                    std::slice::from_ref(&declaration)
                )
                .is_err()
        );
        context.summary = false;
        request.max_output_tokens += 1;
        assert!(
            decoded
                .validate(
                    &context,
                    &request,
                    &request,
                    std::slice::from_ref(&declaration)
                )
                .is_err()
        );
        request.max_output_tokens -= 1;
        let mut changed = encoded;
        changed["version"] = json!(2);
        let decoded: ToolDecisionSnapshot = serde_json::from_value(changed).unwrap();
        assert!(
            decoded
                .validate(
                    &context,
                    &request,
                    &request,
                    std::slice::from_ref(&declaration)
                )
                .is_err()
        );
        let mut changed = serde_json::to_value(&snapshot).unwrap();
        changed["context"]["external_id"] = json!("changed");
        let decoded: ToolDecisionSnapshot = serde_json::from_value(changed).unwrap();
        assert!(
            decoded
                .validate(
                    &context,
                    &request,
                    &request,
                    std::slice::from_ref(&declaration)
                )
                .is_err()
        );
    }

    #[test]
    fn decision_snapshot_detects_a_valid_but_changed_effective_choice() {
        let spec = crate::ToolSpec {
            name: "read".into(),
            description: String::new(),
            parameters: json!({"type":"object"}),
        };
        let request = crate::ModelRequest {
            model: "m".into(),
            messages: vec![],
            tools: vec![spec.clone()],
            max_output_tokens: 1,
        };
        let context = crate::ToolDispatchContext {
            request_id: uuid::Uuid::new_v4(),
            session_id: uuid::Uuid::new_v4(),
            external_id: "s".into(),
            summary: false,
        };
        let declaration = ToolDeclaration {
            spec: spec.clone(),
            constrained_sampling: Some(ToolConstraint::JsonSchema {
                strict: StrictToolPolicy::Prefer,
            }),
        };
        let effective = EffectiveToolDeclaration {
            spec: spec.clone(),
            sampling: EffectiveToolSampling::StrictJsonSchema {
                original_schema: spec.parameters,
                normalization_version: STRICT_TOOL_NORMALIZATION_VERSION,
            },
        };
        let snapshot = ToolDecisionSnapshot::capture(
            &context,
            &request,
            std::slice::from_ref(&declaration),
            &[effective],
        )
        .unwrap();
        let fallback = EffectiveToolDeclaration {
            spec: declaration.spec.clone(),
            sampling: EffectiveToolSampling::Unconstrained,
        };
        let fallback_snapshot =
            ToolDecisionSnapshot::capture(&context, &request, &[declaration], &[fallback]).unwrap();
        assert!(
            fallback_snapshot
                .validate_recorded(
                    &context,
                    snapshot.request_sha256(),
                    snapshot.dispatch_sha256(),
                    snapshot.binding_sha256()
                )
                .is_err()
        );
        assert!(
            fallback_snapshot
                .validate_recorded(
                    &context,
                    fallback_snapshot.request_sha256(),
                    fallback_snapshot.dispatch_sha256(),
                    fallback_snapshot.binding_sha256()
                )
                .is_ok()
        );
        let required = ToolDeclaration {
            spec: request.tools[0].clone(),
            constrained_sampling: Some(ToolConstraint::JsonSchema {
                strict: StrictToolPolicy::Require,
            }),
        };
        assert!(
            fallback_snapshot
                .validate(&context, &request, &request, &[required])
                .is_err()
        );
        let mut altered = serde_json::to_value(&snapshot).unwrap();
        altered["effective"][0]["sampling"] = json!({"type":"unconstrained"});
        let altered: ToolDecisionSnapshot = serde_json::from_value(altered).unwrap();
        assert!(
            altered
                .validate_recorded(
                    &context,
                    snapshot.request_sha256(),
                    snapshot.dispatch_sha256(),
                    snapshot.binding_sha256()
                )
                .is_err()
        );
    }

    #[test]
    fn decision_snapshot_binds_caller_and_filtered_requests() {
        let request = crate::ModelRequest {
            model: "m".into(),
            messages: vec![crate::Message::User {
                content: "before filtering".into(),
            }],
            tools: vec![],
            max_output_tokens: 1,
        };
        let mut filtered = request.clone();
        filtered.messages = vec![crate::Message::User {
            content: "after filtering".into(),
        }];
        let context = crate::ToolDispatchContext {
            request_id: uuid::Uuid::new_v4(),
            session_id: uuid::Uuid::new_v4(),
            external_id: "s".into(),
            summary: false,
        };
        let snapshot =
            ToolDecisionSnapshot::capture_dispatch(&context, &request, &filtered, &[], &[])
                .unwrap();
        assert_ne!(snapshot.request_sha256(), snapshot.dispatch_sha256());
        assert!(
            snapshot
                .validate(&context, &request, &filtered, &[])
                .is_ok()
        );
        assert!(
            snapshot
                .validate(&context, &request, &request, &[])
                .is_err()
        );
        assert!(
            snapshot
                .validate_recorded(
                    &context,
                    snapshot.request_sha256(),
                    snapshot.dispatch_sha256(),
                    snapshot.binding_sha256()
                )
                .is_ok()
        );
    }

    #[test]
    fn effective_decisions_cannot_downgrade_or_change_tool_identity() {
        let spec = crate::ToolSpec {
            name: "read".into(),
            description: String::new(),
            parameters: json!({"type":"object"}),
        };
        let request = crate::ModelRequest {
            model: "m".into(),
            messages: vec![],
            tools: vec![spec.clone()],
            max_output_tokens: 1,
        };
        let declaration = ToolDeclaration {
            spec: spec.clone(),
            constrained_sampling: Some(ToolConstraint::JsonSchema {
                strict: StrictToolPolicy::Require,
            }),
        };
        let mut effective = EffectiveToolDeclaration {
            spec,
            sampling: EffectiveToolSampling::Unconstrained,
        };
        let mut preferred = declaration.clone();
        preferred.constrained_sampling = Some(ToolConstraint::JsonSchema {
            strict: StrictToolPolicy::Prefer,
        });
        assert!(
            validate_effective_tool_declarations(
                &request,
                &[preferred],
                std::slice::from_ref(&effective)
            )
            .is_ok()
        );
        assert!(
            validate_effective_tool_declarations(
                &request,
                std::slice::from_ref(&declaration),
                std::slice::from_ref(&effective)
            )
            .is_err()
        );
        effective.sampling = EffectiveToolSampling::StrictJsonSchema {
            original_schema: request.tools[0].parameters.clone(),
            normalization_version: STRICT_TOOL_NORMALIZATION_VERSION,
        };
        assert!(
            validate_effective_tool_declarations(
                &request,
                std::slice::from_ref(&declaration),
                std::slice::from_ref(&effective)
            )
            .is_ok()
        );
        let mut disabled = declaration.clone();
        disabled.constrained_sampling = Some(ToolConstraint::Disabled);
        assert!(
            validate_effective_tool_declarations(
                &request,
                &[disabled],
                std::slice::from_ref(&effective)
            )
            .is_err()
        );
        let duplicate_request = crate::ModelRequest {
            tools: vec![request.tools[0].clone(), request.tools[0].clone()],
            ..request.clone()
        };
        assert!(
            validate_effective_tool_declarations(
                &duplicate_request,
                &[declaration.clone(), declaration.clone()],
                &[effective.clone(), effective.clone()]
            )
            .is_err()
        );
        if let EffectiveToolSampling::StrictJsonSchema {
            normalization_version,
            ..
        } = &mut effective.sampling
        {
            *normalization_version += 1;
        }
        assert!(
            validate_effective_tool_declarations(
                &request,
                std::slice::from_ref(&declaration),
                std::slice::from_ref(&effective)
            )
            .is_err()
        );
        if let EffectiveToolSampling::StrictJsonSchema {
            normalization_version,
            original_schema,
        } = &mut effective.sampling
        {
            *normalization_version = STRICT_TOOL_NORMALIZATION_VERSION;
            *original_schema = json!({"type":"object","properties":{"stale":{"type":"string"}}});
        }
        assert!(
            validate_effective_tool_declarations(
                &request,
                std::slice::from_ref(&declaration),
                std::slice::from_ref(&effective)
            )
            .is_err()
        );
        if let EffectiveToolSampling::StrictJsonSchema {
            original_schema, ..
        } = &mut effective.sampling
        {
            *original_schema = request.tools[0].parameters.clone();
        }
        effective.spec.name = "other".into();
        assert!(
            validate_effective_tool_declarations(&request, &[declaration], &[effective]).is_err()
        );
    }

    #[test]
    fn normalizes_only_optional_nulls_on_a_copy() {
        let schema = json!({"type":"object","properties":{
            "required":{"type":"string"},"optional":{"type":"string"},
            "nullable":{"type":["string","null"]},
            "items":{"type":"array","items":{"type":"object","properties":{"x":{"type":"integer"}}}}
        },"required":["required"]});
        let arguments = json!({"required":null,"optional":null,"nullable":null,"items":[{"x":null},{"x":3}],"extra":null});
        let normalized = normalize_strict_tool_arguments(&schema, &arguments).unwrap();
        assert_eq!(
            normalized,
            json!({"required":null,"nullable":null,"items":[{},{"x":3}],"extra":null})
        );
        assert_eq!(arguments["optional"], Value::Null);
        assert!(arguments.as_object().unwrap().contains_key("optional"));
        assert!(
            normalize_strict_tool_arguments(
                &json!({"type":"object","properties":{"x":{"$ref":"#/x"}}}),
                &arguments
            )
            .is_err()
        );
    }

    #[test]
    fn null_normalization_uses_root_dialect_and_rejects_dynamic_refs() {
        let mut schema = json!({"$schema":"http://json-schema.org/draft-04/schema#","type":"object","properties":{
            "optional":{"type":["string","null"],"const":"x"}
        }});
        let arguments = json!({"optional":null});
        assert_eq!(
            normalize_strict_tool_arguments(&schema, &arguments).unwrap(),
            arguments
        );
        schema["$schema"] = json!("https://json-schema.org/draft/2020-12/schema");
        assert_eq!(
            normalize_strict_tool_arguments(&schema, &arguments).unwrap(),
            json!({})
        );
        let dynamic =
            json!({"type":"object","properties":{"x":{"type":"string","$dynamicRef":"#node"}}});
        assert!(normalize_strict_tool_arguments(&dynamic, &json!({"x":null})).is_err());
        for keyword in ["$dynamicRef", "$recursiveRef"] {
            let mut reference = json!({"type":"object","properties":{"x":{"type":"string"}}});
            reference["properties"]["x"][keyword] = json!("#node");
            assert_eq!(
                resolve_strict_tool_schema(
                    "x",
                    &reference,
                    StrictToolPolicy::Prefer,
                    true,
                    |_, _| false
                )
                .unwrap(),
                None
            );
            assert!(
                resolve_strict_tool_schema(
                    "x",
                    &reference,
                    StrictToolPolicy::Require,
                    true,
                    |_, _| false
                )
                .is_err()
            );
        }
        let mixed = json!({"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","properties":{
            "x":{"$id":"https://example.test/nested","$schema":"http://json-schema.org/draft-04/schema#","type":["string","null"],"const":"x"}
        }});
        assert!(normalize_strict_tool_arguments(&mixed, &json!({"x":null})).is_err());
        assert_eq!(
            resolve_strict_tool_schema("x", &mixed, StrictToolPolicy::Prefer, true, |_, _| false)
                .unwrap(),
            None
        );
        assert!(
            resolve_strict_tool_schema("x", &mixed, StrictToolPolicy::Require, true, |_, _| false)
                .is_err()
        );
        for nested in [
            json!({"type":"object","properties":{"x":{"type":"array","items":{"type":"string","$schema":"http://json-schema.org/draft-04/schema#"}}}}),
            json!({"type":"object","properties":{"x":{"anyOf":[{"type":"string","$schema":"http://json-schema.org/draft-04/schema#"},{"type":"null"}]}}}),
        ] {
            assert!(normalize_strict_tool_arguments(&nested, &json!({})).is_err());
        }
    }

    #[test]
    fn declarations_preserve_pi_constraint_shapes() {
        for constraint in [
            Value::Bool(false),
            json!({"type":"json_schema","strict":"prefer"}),
            json!({"type":"json_schema","strict":"require"}),
            json!({"type":"grammar","variants":{"openai_lark":"start: /x/"}}),
            json!({"type":"grammar","variants":{"openai_lark":"lark","openai_regex":"regex"}}),
            json!({"type":"grammar","variants":{}}),
        ] {
            let input = json!({"name":"read","description":"read a file","parameters":{"type":"object"},"constrainedSampling":constraint});
            let declaration: ToolDeclaration = serde_json::from_value(input.clone()).unwrap();
            assert_eq!(serde_json::to_value(declaration).unwrap(), input);
        }
        let input =
            json!({"name":"read","description":"read a file","parameters":{"type":"object"}});
        let declaration: ToolDeclaration = serde_json::from_value(input.clone()).unwrap();
        assert_eq!(serde_json::to_value(declaration).unwrap(), input);
        assert!(serde_json::from_value::<ToolConstraint>(Value::Bool(true)).is_err());
        for invalid in [
            Value::Null,
            json!({"type":"json_schema","strict":"unknown"}),
            json!({"type":"unknown"}),
        ] {
            let input = json!({"name":"read","description":"read a file","parameters":{"type":"object"},"constrainedSampling":invalid});
            assert!(serde_json::from_value::<ToolDeclaration>(input).is_err());
        }
    }

    #[test]
    fn strict_policy_falls_back_only_when_preferred() {
        let schema = json!({"type":"object","properties":{"x":{"$ref":"#/x"}}});
        for supported in [false, true] {
            assert_eq!(
                resolve_strict_tool_schema(
                    "tool",
                    &schema,
                    StrictToolPolicy::Prefer,
                    supported,
                    |_, _| false
                )
                .unwrap(),
                None
            );
            assert!(
                resolve_strict_tool_schema(
                    "tool",
                    &schema,
                    StrictToolPolicy::Require,
                    supported,
                    |_, _| false
                )
                .unwrap_err()
                .to_string()
                .contains("Tool \"tool\" requires JSON-schema constrained sampling")
            );
        }
        let valid = json!({"type":"object","properties":{"x":{"type":"string"}}});
        for policy in [StrictToolPolicy::Prefer, StrictToolPolicy::Require] {
            assert!(
                resolve_strict_tool_schema("tool", &valid, policy, true, |_, _| false)
                    .unwrap()
                    .is_some()
            );
        }
    }

    #[test]
    fn converts_nested_objects_and_preserves_original() {
        let input = json!({"type":"object","properties":{"path":{"type":"string"},"options":{"type":"object","properties":{"limit":{"type":"integer"}},"required":[]}},"required":["path"]});
        let result = strict_tool_schema(&input, |_, _| false).unwrap();
        assert_eq!(
            result,
            json!({"type":"object","properties":{"path":{"type":"string"},"options":{"anyOf":[{"type":"object","properties":{"limit":{"anyOf":[{"type":"integer"},{"type":"null"}]}},"required":["limit"],"additionalProperties":false},{"type":"null"}]}},"required":["path","options"],"additionalProperties":false})
        );
        assert!(input.get("additionalProperties").is_none());
    }

    #[test]
    fn rejects_unsupported_schema_and_provider_keywords() {
        for input in [
            json!(true),
            json!({"type":"object","properties":{"x":{"$ref":"#/x"}}}),
            json!({"type":"object","required":["missing"]}),
            json!({"type":"object","properties":{"x":{"anyOf":[{"type":"array"}]}}}),
            json!({"type":"object","properties":{"x":{"const":"untyped"}}}),
        ] {
            assert!(strict_tool_schema(&input, |_, _| false).is_err(), "{input}");
        }
        assert!(
            strict_tool_schema(
                &json!({"type":"object","properties":{"x":{"type":"string","pattern":"x"}}}),
                |key, _| key == "pattern"
            )
            .is_err()
        );
    }

    #[test]
    fn keeps_existing_null_and_omitted_properties() {
        let empty = json!({"type":"object"});
        assert_eq!(
            strict_tool_schema(&empty, |_, _| false).unwrap(),
            json!({"type":"object","required":[],"additionalProperties":false})
        );
        let nullable = json!({"type":"object","properties":{"x":{"type":["string","null"]}}});
        let result = strict_tool_schema(&nullable, |_, _| false).unwrap();
        assert_eq!(result["properties"]["x"], json!({"type":["string","null"]}));
    }

    #[test]
    fn follows_javascript_property_enumeration_and_typed_nodes() {
        let schema = json!({"type":"object","properties":{
            "4294967295":{"type":"string"},"10":{"type":"string"},
            "01":{"type":"string"},"2":{"enum":[1,2]},"0":{"type":"string"},
            "4294967294":{"type":"string"},"other":{"type":"string"}
        }});
        let result = strict_tool_schema(&schema, |_, _| false).unwrap();
        assert_eq!(
            result["required"],
            json!(["0", "2", "10", "4294967294", "4294967295", "01", "other"])
        );
        for value in [
            json!({}),
            json!({"type":"array","items":{}}),
            json!({"anyOf":[{}]}),
        ] {
            assert!(
                strict_tool_schema(
                    &json!({"type":"object","properties":{"x":value}}),
                    |_, _| false
                )
                .is_err()
            );
        }
    }
}
