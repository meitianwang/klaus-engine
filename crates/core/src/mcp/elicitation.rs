//! Typed MCP server-initiated elicitation and session-scoped response routing.

use crate::runtime_values::runtime_codec::{RuntimeEnumVariant, RuntimeExternalVariant};
use crate::runtime_values::{
    runtime_codec, RuntimeCodecError, RuntimeCodecErrorKind, RuntimeDecode, RuntimeEncode,
    RuntimeEncoder, RuntimeJson, RuntimeMap, RuntimeObjectEncoder, RuntimeString,
};
use crate::types::abort::AbortSignal;
use crate::types::permission::{ApprovalPolicy, SandboxMode};
use crate::types::tool::UuidFn;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{oneshot, watch};

fn decode_provider_unit_enum<T: Copy>(
    value: &RuntimeJson,
    variants: &[RuntimeEnumVariant<'_>],
    values: &[T],
) -> Result<T, RuntimeCodecError> {
    runtime_codec::decode_external(value, variants, |variant| match variant {
        RuntimeExternalVariant::Unit { variant } => values
            .get(variant)
            .copied()
            .ok_or_else(|| RuntimeCodecError::new(RuntimeCodecErrorKind::UnknownEnumVariant)),
        RuntimeExternalVariant::Payload {
            variant: _variant,
            value: _value,
        } => Err(RuntimeCodecError::new(
            RuntimeCodecErrorKind::InvalidEnumShape,
        )),
    })
}

macro_rules! impl_runtime_object_codec {
    (
        $type:ty,
        deny_unknown = $deny_unknown:expr,
        required { $( $required_field:ident => $required_wire:literal ),* $(,)? },
        optional { $( $optional_field:ident => $optional_wire:literal ),* $(,)? }
    ) => {
        impl RuntimeEncode for $type {
            fn encode_runtime(
                &self,
                out: &mut RuntimeEncoder,
            ) -> Result<(), RuntimeCodecError> {
                let mut object = RuntimeObjectEncoder::new();
                $( object.field($required_wire, &self.$required_field)?; )*
                $(
                    if let Some(value) = &self.$optional_field {
                        object.field($optional_wire, value)?;
                    }
                )*
                out.emit(RuntimeJson::Object(object.finish()))
            }
        }

        impl RuntimeDecode for $type {
            fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
                runtime_codec::decode_object(value, $deny_unknown, |object| {
                    Ok(Self {
                        $( $required_field: object.required($required_wire, &[])?, )*
                        $( $optional_field: object.optional($optional_wire, &[])?, )*
                    })
                })
            }
        }
    };
}

macro_rules! impl_runtime_untagged_codec {
    ($type:ident { $( $variant:ident($payload:ty) ),+ $(,)? }) => {
        impl RuntimeEncode for $type {
            fn encode_runtime(
                &self,
                out: &mut RuntimeEncoder,
            ) -> Result<(), RuntimeCodecError> {
                match self {
                    $( Self::$variant(value) => value.encode_runtime(out), )+
                }
            }
        }

        impl RuntimeDecode for $type {
            fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
                $(
                    if let Ok(decoded) = <$payload>::decode_runtime(value) {
                        return Ok(Self::$variant(decoded));
                    }
                )+
                Err(RuntimeCodecError::new(
                    RuntimeCodecErrorKind::NoMatchingUntaggedVariant,
                ))
            }
        }
    };
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum McpElicitationAction {
    Accept,
    Decline,
    Cancel,
}

impl RuntimeEncode for McpElicitationAction {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Accept => "accept",
            Self::Decline => "decline",
            Self::Cancel => "cancel",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for McpElicitationAction {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("accept", &[]),
                RuntimeEnumVariant::new("decline", &[]),
                RuntimeEnumVariant::new("cancel", &[]),
            ],
            &[Self::Accept, Self::Decline, Self::Cancel],
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct McpElicitationResponse {
    pub action: McpElicitationAction,
    pub content: Option<RuntimeJson>,
    pub meta: Option<RuntimeJson>,
}

impl McpElicitationResponse {
    pub fn decline() -> Self {
        Self {
            action: McpElicitationAction::Decline,
            content: None,
            meta: None,
        }
    }

    pub fn cancel() -> Self {
        Self {
            action: McpElicitationAction::Cancel,
            content: None,
            meta: None,
        }
    }

    fn normalized(mut self) -> Self {
        if self.action != McpElicitationAction::Accept {
            self.content = None;
        }
        self
    }
}

/// Restricted top-level object schema defined by MCP form elicitation.
#[derive(Debug, Clone, PartialEq)]
pub struct McpElicitationSchema {
    pub schema_uri: Option<RuntimeString>,
    pub type_: McpElicitationObjectType,
    pub properties: RuntimeMap<McpElicitationPrimitiveSchema>,
    pub required: Option<Vec<RuntimeString>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum McpElicitationObjectType {
    Object,
}

impl RuntimeEncode for McpElicitationObjectType {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Object => "object",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for McpElicitationObjectType {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[RuntimeEnumVariant::new("object", &[])],
            &[Self::Object],
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum McpElicitationPrimitiveSchema {
    Enum(McpElicitationEnumSchema),
    String(McpElicitationStringSchema),
    Number(McpElicitationNumberSchema),
    Boolean(McpElicitationBooleanSchema),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpElicitationStringSchema {
    pub type_: McpElicitationStringType,
    pub title: Option<RuntimeString>,
    pub description: Option<RuntimeString>,
    pub min_length: Option<u32>,
    pub max_length: Option<u32>,
    pub format: Option<McpElicitationStringFormat>,
    pub default: Option<RuntimeString>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum McpElicitationStringType {
    String,
}

impl RuntimeEncode for McpElicitationStringType {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::String => "string",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for McpElicitationStringType {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[RuntimeEnumVariant::new("string", &[])],
            &[Self::String],
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum McpElicitationStringFormat {
    Email,
    Uri,
    Date,
    DateTime,
}

impl RuntimeEncode for McpElicitationStringFormat {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Email => "email",
            Self::Uri => "uri",
            Self::Date => "date",
            Self::DateTime => "date-time",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for McpElicitationStringFormat {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("email", &[]),
                RuntimeEnumVariant::new("uri", &[]),
                RuntimeEnumVariant::new("date", &[]),
                RuntimeEnumVariant::new("date-time", &[]),
            ],
            &[Self::Email, Self::Uri, Self::Date, Self::DateTime],
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct McpElicitationNumberSchema {
    pub type_: McpElicitationNumberType,
    pub title: Option<RuntimeString>,
    pub description: Option<RuntimeString>,
    pub minimum: Option<f64>,
    pub maximum: Option<f64>,
    pub default: Option<f64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum McpElicitationNumberType {
    Number,
    Integer,
}

impl RuntimeEncode for McpElicitationNumberType {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Number => "number",
            Self::Integer => "integer",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for McpElicitationNumberType {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("number", &[]),
                RuntimeEnumVariant::new("integer", &[]),
            ],
            &[Self::Number, Self::Integer],
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpElicitationBooleanSchema {
    pub type_: McpElicitationBooleanType,
    pub title: Option<RuntimeString>,
    pub description: Option<RuntimeString>,
    pub default: Option<bool>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum McpElicitationBooleanType {
    Boolean,
}

impl RuntimeEncode for McpElicitationBooleanType {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Boolean => "boolean",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for McpElicitationBooleanType {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[RuntimeEnumVariant::new("boolean", &[])],
            &[Self::Boolean],
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum McpElicitationEnumSchema {
    SingleSelect(McpElicitationSingleSelectEnumSchema),
    MultiSelect(McpElicitationMultiSelectEnumSchema),
    Legacy(McpElicitationLegacyTitledEnumSchema),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpElicitationLegacyTitledEnumSchema {
    pub type_: McpElicitationStringType,
    pub title: Option<RuntimeString>,
    pub description: Option<RuntimeString>,
    pub enum_: Vec<RuntimeString>,
    pub enum_names: Option<Vec<RuntimeString>>,
    pub default: Option<RuntimeString>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum McpElicitationSingleSelectEnumSchema {
    Untitled(McpElicitationUntitledSingleSelectEnumSchema),
    Titled(McpElicitationTitledSingleSelectEnumSchema),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpElicitationUntitledSingleSelectEnumSchema {
    pub type_: McpElicitationStringType,
    pub title: Option<RuntimeString>,
    pub description: Option<RuntimeString>,
    pub enum_: Vec<RuntimeString>,
    pub default: Option<RuntimeString>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpElicitationTitledSingleSelectEnumSchema {
    pub type_: McpElicitationStringType,
    pub title: Option<RuntimeString>,
    pub description: Option<RuntimeString>,
    pub one_of: Vec<McpElicitationConstOption>,
    pub default: Option<RuntimeString>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum McpElicitationMultiSelectEnumSchema {
    Untitled(McpElicitationUntitledMultiSelectEnumSchema),
    Titled(McpElicitationTitledMultiSelectEnumSchema),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpElicitationUntitledMultiSelectEnumSchema {
    pub type_: McpElicitationArrayType,
    pub title: Option<RuntimeString>,
    pub description: Option<RuntimeString>,
    pub min_items: Option<u64>,
    pub max_items: Option<u64>,
    pub items: McpElicitationUntitledEnumItems,
    pub default: Option<Vec<RuntimeString>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpElicitationTitledMultiSelectEnumSchema {
    pub type_: McpElicitationArrayType,
    pub title: Option<RuntimeString>,
    pub description: Option<RuntimeString>,
    pub min_items: Option<u64>,
    pub max_items: Option<u64>,
    pub items: McpElicitationTitledEnumItems,
    pub default: Option<Vec<RuntimeString>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum McpElicitationArrayType {
    Array,
}

impl RuntimeEncode for McpElicitationArrayType {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Array => "array",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for McpElicitationArrayType {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[RuntimeEnumVariant::new("array", &[])],
            &[Self::Array],
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpElicitationUntitledEnumItems {
    pub type_: McpElicitationStringType,
    pub enum_: Vec<RuntimeString>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpElicitationTitledEnumItems {
    pub any_of: Vec<McpElicitationConstOption>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpElicitationConstOption {
    pub const_: RuntimeString,
    pub title: RuntimeString,
}

impl_runtime_object_codec!(
    McpElicitationResponse,
    deny_unknown = false,
    required { action => "action" },
    optional { content => "content", meta => "_meta" }
);
impl_runtime_object_codec!(
    McpElicitationSchema,
    deny_unknown = true,
    required { type_ => "type", properties => "properties" },
    optional { schema_uri => "$schema", required => "required" }
);
impl_runtime_object_codec!(
    McpElicitationStringSchema,
    deny_unknown = true,
    required { type_ => "type" },
    optional {
        title => "title",
        description => "description",
        min_length => "minLength",
        max_length => "maxLength",
        format => "format",
        default => "default"
    }
);
impl_runtime_object_codec!(
    McpElicitationNumberSchema,
    deny_unknown = true,
    required { type_ => "type" },
    optional {
        title => "title",
        description => "description",
        minimum => "minimum",
        maximum => "maximum",
        default => "default"
    }
);
impl_runtime_object_codec!(
    McpElicitationBooleanSchema,
    deny_unknown = true,
    required { type_ => "type" },
    optional {
        title => "title",
        description => "description",
        default => "default"
    }
);
impl_runtime_object_codec!(
    McpElicitationLegacyTitledEnumSchema,
    deny_unknown = true,
    required { type_ => "type", enum_ => "enum" },
    optional {
        title => "title",
        description => "description",
        enum_names => "enumNames",
        default => "default"
    }
);
impl_runtime_object_codec!(
    McpElicitationUntitledSingleSelectEnumSchema,
    deny_unknown = true,
    required { type_ => "type", enum_ => "enum" },
    optional {
        title => "title",
        description => "description",
        default => "default"
    }
);
impl_runtime_object_codec!(
    McpElicitationTitledSingleSelectEnumSchema,
    deny_unknown = true,
    required { type_ => "type", one_of => "oneOf" },
    optional {
        title => "title",
        description => "description",
        default => "default"
    }
);
impl_runtime_object_codec!(
    McpElicitationUntitledMultiSelectEnumSchema,
    deny_unknown = true,
    required { type_ => "type", items => "items" },
    optional {
        title => "title",
        description => "description",
        min_items => "minItems",
        max_items => "maxItems",
        default => "default"
    }
);
impl_runtime_object_codec!(
    McpElicitationTitledMultiSelectEnumSchema,
    deny_unknown = true,
    required { type_ => "type", items => "items" },
    optional {
        title => "title",
        description => "description",
        min_items => "minItems",
        max_items => "maxItems",
        default => "default"
    }
);
impl_runtime_object_codec!(
    McpElicitationUntitledEnumItems,
    deny_unknown = true,
    required { type_ => "type", enum_ => "enum" },
    optional {}
);
impl_runtime_object_codec!(
    McpElicitationConstOption,
    deny_unknown = true,
    required { const_ => "const", title => "title" },
    optional {}
);

impl RuntimeEncode for McpElicitationTitledEnumItems {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("anyOf", &self.any_of)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for McpElicitationTitledEnumItems {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, true, |object| {
            Ok(Self {
                any_of: object.required("anyOf", &["oneOf"])?,
            })
        })
    }
}

impl_runtime_untagged_codec!(McpElicitationPrimitiveSchema {
    Enum(McpElicitationEnumSchema),
    String(McpElicitationStringSchema),
    Number(McpElicitationNumberSchema),
    Boolean(McpElicitationBooleanSchema),
});
impl_runtime_untagged_codec!(McpElicitationEnumSchema {
    SingleSelect(McpElicitationSingleSelectEnumSchema),
    MultiSelect(McpElicitationMultiSelectEnumSchema),
    Legacy(McpElicitationLegacyTitledEnumSchema),
});
impl_runtime_untagged_codec!(McpElicitationSingleSelectEnumSchema {
    Untitled(McpElicitationUntitledSingleSelectEnumSchema),
    Titled(McpElicitationTitledSingleSelectEnumSchema),
});
impl_runtime_untagged_codec!(McpElicitationMultiSelectEnumSchema {
    Untitled(McpElicitationUntitledMultiSelectEnumSchema),
    Titled(McpElicitationTitledMultiSelectEnumSchema),
});

#[derive(Debug, Clone, PartialEq)]
pub enum McpElicitationRequest {
    Form {
        meta: Option<RuntimeJson>,
        message: RuntimeString,
        requested_schema: McpElicitationSchema,
    },
    OpenAiForm {
        meta: Option<RuntimeJson>,
        message: RuntimeString,
        requested_schema: RuntimeJson,
    },
    Url {
        meta: Option<RuntimeJson>,
        message: RuntimeString,
        url: RuntimeString,
        elicitation_id: RuntimeString,
    },
}

impl McpElicitationRequest {
    pub fn meta(&self) -> Option<&RuntimeJson> {
        match self {
            Self::Form { meta, .. } | Self::OpenAiForm { meta, .. } | Self::Url { meta, .. } => {
                meta.as_ref()
            }
        }
    }
}

impl RuntimeEncode for McpElicitationRequest {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let (mode, fields) = match self {
            Self::Form {
                meta,
                message,
                requested_schema,
            } => {
                let mut fields = RuntimeObjectEncoder::new();
                if let Some(meta) = meta {
                    fields.field("_meta", meta)?;
                }
                fields.field("message", message)?;
                fields.field("requestedSchema", requested_schema)?;
                ("form", fields.finish())
            }
            Self::OpenAiForm {
                meta,
                message,
                requested_schema,
            } => {
                let mut fields = RuntimeObjectEncoder::new();
                if let Some(meta) = meta {
                    fields.field("_meta", meta)?;
                }
                fields.field("message", message)?;
                fields.field("requestedSchema", requested_schema)?;
                ("openai/form", fields.finish())
            }
            Self::Url {
                meta,
                message,
                url,
                elicitation_id,
            } => {
                let mut fields = RuntimeObjectEncoder::new();
                if let Some(meta) = meta {
                    fields.field("_meta", meta)?;
                }
                fields.field("message", message)?;
                fields.field("url", url)?;
                fields.field("elicitationId", elicitation_id)?;
                ("url", fields.finish())
            }
        };
        runtime_codec::encode_internal(out, "mode", &[], mode, &fields)
    }
}

impl RuntimeDecode for McpElicitationRequest {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_internal(
            value,
            false,
            "mode",
            &[],
            &[
                RuntimeEnumVariant::new("form", &[]),
                RuntimeEnumVariant::new("openai/form", &[]),
                RuntimeEnumVariant::new("url", &[]),
            ],
            |variant, object| match variant {
                0 => Ok(Self::Form {
                    meta: object.optional("_meta", &[])?,
                    message: object.required("message", &[])?,
                    requested_schema: object.required("requestedSchema", &[])?,
                }),
                1 => Ok(Self::OpenAiForm {
                    meta: object.optional("_meta", &[])?,
                    message: object.required("message", &[])?,
                    requested_schema: object.required("requestedSchema", &[])?,
                }),
                2 => Ok(Self::Url {
                    meta: object.optional("_meta", &[])?,
                    message: object.required("message", &[])?,
                    url: object.required("url", &[])?,
                    elicitation_id: object.required("elicitationId", &[])?,
                }),
                _ => Err(RuntimeCodecError::new(
                    RuntimeCodecErrorKind::UnknownEnumVariant,
                )),
            },
        )
    }
}

struct FormParams {
    meta: Option<RuntimeJson>,
    mode: Option<RuntimeString>,
    message: RuntimeString,
    requested_schema: McpElicitationSchema,
}

struct UrlParams {
    meta: Option<RuntimeJson>,
    mode: RuntimeString,
    message: RuntimeString,
    url: RuntimeString,
    elicitation_id: RuntimeString,
}

struct OpenAiFormParams {
    meta: Option<RuntimeJson>,
    message: RuntimeString,
    requested_schema: RuntimeJson,
}

impl_runtime_object_codec!(
    FormParams,
    deny_unknown = false,
    required { message => "message", requested_schema => "requestedSchema" },
    optional { meta => "_meta", mode => "mode" }
);
impl_runtime_object_codec!(
    UrlParams,
    deny_unknown = false,
    required {
        mode => "mode",
        message => "message",
        url => "url",
        elicitation_id => "elicitationId"
    },
    optional { meta => "_meta" }
);
impl_runtime_object_codec!(
    OpenAiFormParams,
    deny_unknown = false,
    required { message => "message", requested_schema => "requestedSchema" },
    optional { meta => "_meta" }
);

pub(crate) fn parse_mcp_elicitation_request(
    method: &str,
    params: RuntimeJson,
) -> Result<McpElicitationRequest, String> {
    if method == "openai/form" {
        let params = runtime_codec::decode_value::<OpenAiFormParams>(&params)
            .map_err(|error| error.to_string())?;
        return Ok(McpElicitationRequest::OpenAiForm {
            meta: params.meta,
            message: params.message,
            requested_schema: params.requested_schema,
        });
    }

    let mode = params
        .get("mode")
        .and_then(RuntimeJson::as_string)
        .cloned()
        .unwrap_or_else(|| RuntimeString::from("form"));
    if mode == "form" {
        let params = runtime_codec::decode_value::<FormParams>(&params)
            .map_err(|error| error.to_string())?;
        if params.mode.as_ref().is_some_and(|mode| mode != "form") {
            return Err("unsupported MCP elicitation mode".to_string());
        }
        return Ok(McpElicitationRequest::Form {
            meta: params.meta,
            message: params.message,
            requested_schema: params.requested_schema,
        });
    }
    if mode == "url" {
        let params =
            runtime_codec::decode_value::<UrlParams>(&params).map_err(|error| error.to_string())?;
        if params.mode != "url" {
            return Err("unsupported MCP elicitation mode".to_string());
        }
        url::Url::parse(&url_parser_input(&params.url))
            .map_err(|error| format!("invalid MCP elicitation URL: {error}"))?;
        return Ok(McpElicitationRequest::Url {
            meta: params.meta,
            message: params.message,
            url: params.url,
            elicitation_id: params.elicitation_id,
        });
    }
    Err(format!(
        "unsupported MCP elicitation mode: {}",
        exact_error_fragment(&mode)
    ))
}

/// WHATWG URL parsing consumes a Web-IDL `USVString`; lone surrogates become
/// U+FFFD at that semantic terminal while the retained request field stays
/// exact UTF-16.
fn url_parser_input(value: &RuntimeString) -> String {
    value
        .code_points()
        .map(|point| char::from_u32(point).unwrap_or(char::REPLACEMENT_CHARACTER))
        .collect()
}

/// Preserve unpaired units in diagnostics without collapsing them into U+FFFD.
/// Ordinary scalar text keeps the legacy spelling; only non-scalars use a
/// deterministic JavaScript-style escape.
fn exact_error_fragment(value: &RuntimeString) -> String {
    let mut output = String::new();
    for point in value.code_points() {
        if let Some(value) = char::from_u32(point) {
            output.push(value);
        } else {
            use std::fmt::Write;
            let _ = write!(output, "\\u{point:04x}");
        }
    }
    output
}

#[derive(Debug, Clone, PartialEq)]
pub struct McpElicitationEvent {
    pub request_id: RuntimeString,
    pub server_name: RuntimeString,
    pub request: McpElicitationRequest,
}

impl_runtime_object_codec!(
    McpElicitationEvent,
    deny_unknown = false,
    required {
        request_id => "requestId",
        server_name => "serverName",
        request => "request"
    },
    optional {}
);

/// Reference-counted "a user answer is being awaited" flag shared by every MCP
/// connection of one session.
///
/// Waiting for a human must not consume a tool call's time budget, so the wait
/// pauses the budget instead: the first waiter flips the watch channel to
/// `true`, the last one back to `false`, and deadline loops subscribe to the
/// channel to stop their clocks while paused.
#[derive(Clone)]
pub struct ElicitationPauseState {
    active_count: Arc<AtomicUsize>,
    paused: Arc<watch::Sender<bool>>,
}

/// Held for the duration of one elicitation wait; dropping it releases the pause.
pub struct ElicitationPauseGuard {
    pause_state: ElicitationPauseState,
}

impl Default for ElicitationPauseState {
    fn default() -> Self {
        Self::new()
    }
}

impl ElicitationPauseState {
    pub fn new() -> Self {
        let (paused, _rx) = watch::channel(false);
        Self {
            active_count: Arc::new(AtomicUsize::new(0)),
            paused: Arc::new(paused),
        }
    }

    pub fn enter(&self) -> ElicitationPauseGuard {
        if self.active_count.fetch_add(1, Ordering::AcqRel) == 0 {
            self.paused.send_replace(true);
        }
        ElicitationPauseGuard {
            pause_state: self.clone(),
        }
    }

    pub fn subscribe(&self) -> watch::Receiver<bool> {
        self.paused.subscribe()
    }
}

impl Drop for ElicitationPauseGuard {
    fn drop(&mut self) {
        if self.pause_state.active_count.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.pause_state.paused.send_replace(false);
        }
    }
}

#[async_trait]
pub trait McpElicitationPresenter: Send + Sync {
    async fn present(&self, event: McpElicitationEvent);

    /// A pending elicitation was dropped without a user answer (turn ended,
    /// turn aborted, connection closed). The host uses this to close the card
    /// it presented for `request_id`; the default keeps presenters that predate
    /// the callback working.
    async fn cancelled(&self, _request_id: RuntimeString) {}
}

struct PendingElicitation {
    owner: u64,
    generation: Option<u64>,
    sender: oneshot::Sender<McpElicitationResponse>,
}

struct RouterInner {
    presenter: Option<Arc<dyn McpElicitationPresenter>>,
    uuid: UuidFn,
    sequence: AtomicU64,
    next_owner: AtomicU64,
    pending: Mutex<HashMap<RuntimeString, PendingElicitation>>,
}

#[derive(Clone)]
struct ElicitationTurn {
    generation: u64,
    signal: AbortSignal,
}

struct RouterOwner {
    id: u64,
    next_generation: AtomicU64,
    turn: Mutex<Option<ElicitationTurn>>,
    permission_policy_provider: Mutex<Option<Arc<dyn Fn() -> McpElicitationPolicy + Send + Sync>>>,
    form_input_generations: Mutex<HashSet<u64>>,
    pause_state: ElicitationPauseState,
}

/// One live snapshot determines both interactive and unattended confirmation authority.
#[derive(Clone, Copy)]
pub struct McpElicitationPolicy {
    pub approval_policy: ApprovalPolicy,
    pub sandbox_mode: SandboxMode,
}

impl McpElicitationPolicy {
    fn auto_approves_confirmation(self) -> bool {
        self.approval_policy == ApprovalPolicy::Never
            && self.sandbox_mode == SandboxMode::DangerFullAccess
    }
}

/// Connections share public reply ids, while each runtime owns its query
/// cancellation and time-budget pause state independently.
#[derive(Clone)]
pub struct McpElicitationRouter {
    inner: Arc<RouterInner>,
    owner: Arc<RouterOwner>,
}

pub(crate) struct McpElicitationQueryLease {
    router: McpElicitationRouter,
    generation: u64,
}

impl Drop for McpElicitationQueryLease {
    fn drop(&mut self) {
        self.router.clear_turn_scope(self.generation);
    }
}

struct PendingElicitationGuard {
    router: McpElicitationRouter,
    request_id: RuntimeString,
}

impl Drop for PendingElicitationGuard {
    fn drop(&mut self) {
        if self
            .router
            .inner
            .pending
            .lock()
            .unwrap()
            .remove(&self.request_id)
            .is_some()
        {
            self.router.notify_cancelled(self.request_id.clone());
        }
    }
}

impl McpElicitationRouter {
    pub fn new(presenter: Option<Arc<dyn McpElicitationPresenter>>, uuid: UuidFn) -> Self {
        Self {
            inner: Arc::new(RouterInner {
                presenter,
                uuid,
                sequence: AtomicU64::new(0),
                next_owner: AtomicU64::new(1),
                pending: Mutex::new(HashMap::new()),
            }),
            owner: Arc::new(RouterOwner {
                id: 0,
                next_generation: AtomicU64::new(0),
                turn: Mutex::new(None),
                permission_policy_provider: Mutex::new(None),
                form_input_generations: Mutex::new(HashSet::new()),
                pause_state: ElicitationPauseState::new(),
            }),
        }
    }

    pub(crate) fn fork_owner(&self) -> Self {
        let parent_policy = self
            .owner
            .permission_policy_provider
            .lock()
            .unwrap()
            .clone();
        Self {
            inner: self.inner.clone(),
            owner: Arc::new(RouterOwner {
                id: self.inner.next_owner.fetch_add(1, Ordering::Relaxed),
                next_generation: AtomicU64::new(0),
                turn: Mutex::new(None),
                // A child connection has not been bound to its own sandbox authority.
                // Inherit prompt policy, never the parent's unattended confirmation grant.
                permission_policy_provider: Mutex::new(parent_policy.map(|provider| {
                    Arc::new(move || McpElicitationPolicy {
                        approval_policy: provider().approval_policy,
                        sandbox_mode: SandboxMode::ReadOnly,
                    }) as Arc<dyn Fn() -> McpElicitationPolicy + Send + Sync>
                })),
                form_input_generations: Mutex::new(HashSet::new()),
                pause_state: ElicitationPauseState::new(),
            }),
        }
    }

    pub(crate) fn begin_query(&self, signal: AbortSignal) -> McpElicitationQueryLease {
        let generation = self.owner.next_generation.fetch_add(1, Ordering::Relaxed);
        self.set_turn_scope(generation, signal);
        McpElicitationQueryLease {
            router: self.clone(),
            generation,
        }
    }

    /// Bind the owning session's live policy without copying its authority into this router.
    pub fn set_approval_policy_provider(
        &self,
        provider: Arc<dyn Fn() -> crate::types::permission::ApprovalPolicy + Send + Sync>,
    ) {
        self.set_permission_policy_provider(Arc::new(move || McpElicitationPolicy {
            approval_policy: provider(),
            sandbox_mode: SandboxMode::ReadOnly,
        }));
    }

    pub fn set_permission_policy_provider(
        &self,
        provider: Arc<dyn Fn() -> McpElicitationPolicy + Send + Sync>,
    ) {
        *self.owner.permission_policy_provider.lock().unwrap() = Some(provider);
    }

    fn permission_policy(&self) -> McpElicitationPolicy {
        let provider = self
            .owner
            .permission_policy_provider
            .lock()
            .unwrap()
            .clone();
        provider.map_or(
            McpElicitationPolicy {
                approval_policy: ApprovalPolicy::OnRequest,
                sandbox_mode: SandboxMode::ReadOnly,
            },
            |provider| provider(),
        )
    }

    pub(crate) fn policy_allows_elicitation(&self) -> bool {
        self.permission_policy()
            .approval_policy
            .allows_prompt(crate::types::permission::ApprovalPromptKind::McpElicitation)
    }

    /// Only calls made by this runtime pause while its questions are pending.
    pub fn pause_state(&self) -> ElicitationPauseState {
        self.owner.pause_state.clone()
    }

    pub fn disabled() -> Self {
        Self::new(None, Arc::new(|| "disabled".to_string()))
    }

    pub fn has_presenter(&self) -> bool {
        self.inner.presenter.is_some()
    }

    pub fn set_turn_signal(&self, signal: Option<AbortSignal>) {
        match signal {
            Some(signal) => {
                let generation = self.owner.next_generation.fetch_add(1, Ordering::Relaxed);
                self.set_turn_scope(generation, signal);
            }
            None => {
                *self.owner.turn.lock().unwrap() = None;
                self.owner.form_input_generations.lock().unwrap().clear();
            }
        }
    }

    pub(crate) fn set_turn_scope(&self, generation: u64, signal: AbortSignal) {
        *self.owner.turn.lock().unwrap() = Some(ElicitationTurn { generation, signal });
    }

    /// Enable ordinary input only after a running root turn has initialized its MCP runtime.
    pub(crate) fn enable_form_input_for_turn(&self, generation: u64) {
        if self.owner.id != 0 || !self.has_presenter() {
            return;
        }
        let turn = self.owner.turn.lock().unwrap();
        if turn
            .as_ref()
            .is_some_and(|turn| turn.generation == generation && !turn.signal.is_cancelled())
        {
            self.owner
                .form_input_generations
                .lock()
                .unwrap()
                .insert(generation);
        }
    }

    fn full_access_input_allowed(&self, generation: Option<u64>) -> bool {
        if self.owner.id != 0
            || !self.has_presenter()
            || !self.permission_policy().auto_approves_confirmation()
        {
            return false;
        }
        let turn = self.owner.turn.lock().unwrap();
        turn.as_ref().is_some_and(|turn| {
            Some(turn.generation) == generation
                && !turn.signal.is_cancelled()
                && self
                    .owner
                    .form_input_generations
                    .lock()
                    .unwrap()
                    .contains(&turn.generation)
        })
    }

    fn is_ordinary_input_form(request: &McpElicitationRequest) -> bool {
        matches!(request, McpElicitationRequest::Form { requested_schema, .. }
            if !requested_schema.properties.is_empty())
            && !request.meta().is_some_and(|meta| {
                meta.get("persist").is_some()
                    || meta
                        .get("klaus")
                        .and_then(|meta| meta.get("computerUseApp"))
                        .is_some()
            })
    }

    pub(crate) fn clear_turn_scope(&self, generation: u64) {
        let mut turn = self.owner.turn.lock().unwrap();
        self.owner
            .form_input_generations
            .lock()
            .unwrap()
            .remove(&generation);
        if turn
            .as_ref()
            .is_some_and(|turn| turn.generation == generation)
        {
            *turn = None;
        }
        let mut pending = self.inner.pending.lock().unwrap();
        let ids = pending
            .iter()
            .filter(|(_, request)| {
                request.owner == self.owner.id && request.generation == Some(generation)
            })
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>();
        let cancelled = ids
            .into_iter()
            .filter_map(|id| pending.remove(&id).map(|request| (id, request)))
            .collect::<Vec<_>>();
        drop(pending);
        drop(turn);
        for (id, request) in cancelled {
            let _ = request.sender.send(McpElicitationResponse::cancel());
            self.notify_cancelled(id);
        }
    }

    pub fn clear_turn_signal_and_cancel(&self) {
        let generation = self
            .owner
            .turn
            .lock()
            .unwrap()
            .as_ref()
            .map(|turn| turn.generation);
        if let Some(generation) = generation {
            self.clear_turn_scope(generation);
        }
    }

    pub fn pending_count(&self) -> usize {
        self.inner.pending.lock().unwrap().len()
    }

    /// Linearize a synchronous model commit with server-initiated prompts.
    /// A healthy connection with no pending prompt does not block the commit.
    #[cfg(test)]
    fn with_no_pending<T>(&self, commit: impl FnOnce() -> Result<T, String>) -> Result<T, String> {
        let pending = self.inner.pending.lock().unwrap();
        if pending
            .values()
            .any(|request| request.owner == self.owner.id)
        {
            return Err("cannot reconfigure while MCP elicitation is pending".to_string());
        }
        commit()
    }

    pub fn resolve(
        &self,
        request_id: &RuntimeString,
        response: McpElicitationResponse,
    ) -> Result<(), String> {
        let request_id_fragment = exact_error_fragment(request_id);
        self.inner
            .pending
            .lock()
            .unwrap()
            .remove(request_id)
            .ok_or_else(|| format!("request_not_found: MCP elicitation '{request_id_fragment}'"))?
            .sender
            .send(response.normalized())
            .map_err(|_| format!("request_not_found: MCP elicitation '{request_id_fragment}'"))
    }

    pub fn cancel_all(&self) {
        let pending = self
            .inner
            .pending
            .lock()
            .unwrap()
            .drain()
            .collect::<Vec<_>>();
        for (request_id, request) in pending {
            let _ = request.sender.send(McpElicitationResponse::cancel());
            self.notify_cancelled(request_id);
        }
    }

    /// Tell the host that a presented card will never receive a user answer,
    /// so it closes instead of staying clickable forever.
    fn notify_cancelled(&self, request_id: RuntimeString) {
        let Some(presenter) = self.inner.presenter.clone() else {
            return;
        };
        tokio::spawn(async move { presenter.cancelled(request_id).await });
    }

    pub(crate) async fn route(
        &self,
        server_name: RuntimeString,
        request: McpElicitationRequest,
        connection_signal: AbortSignal,
        cancel_signal: AbortSignal,
    ) -> McpElicitationResponse {
        if self.permission_policy().auto_approves_confirmation()
            && matches!(
                &request,
                McpElicitationRequest::Form { requested_schema, .. }
                    if requested_schema.properties.is_empty()
            )
        {
            let turn = self.owner.turn.lock().unwrap().clone();
            if connection_signal.is_cancelled()
                || cancel_signal.is_cancelled()
                || turn.as_ref().is_some_and(|turn| turn.signal.is_cancelled())
            {
                return McpElicitationResponse::cancel();
            }
            if turn.is_none() {
                return McpElicitationResponse::decline();
            }
            return McpElicitationResponse {
                action: McpElicitationAction::Accept,
                content: Some(RuntimeJson::Object(Default::default())),
                meta: None,
            };
        }
        let input_form = Self::is_ordinary_input_form(&request);
        let input_generation = self
            .owner
            .turn
            .lock()
            .unwrap()
            .as_ref()
            .map(|turn| turn.generation);
        if !(self.policy_allows_elicitation()
            || input_form && self.full_access_input_allowed(input_generation))
        {
            return McpElicitationResponse::decline();
        }
        let Some(presenter) = self.inner.presenter.clone() else {
            return McpElicitationResponse::decline();
        };
        let sequence = self.inner.sequence.fetch_add(1, Ordering::Relaxed);
        let request_id =
            RuntimeString::from(format!("mcp-elicit-{}-{sequence}", (self.inner.uuid)()));
        let (tx, rx) = oneshot::channel();
        // Register under the same owner lock used when a query ends. A prompt
        // cannot enter that ended generation between capture and insertion.
        let query = {
            let turn = self.owner.turn.lock().unwrap();
            let query = turn.clone();
            self.inner.pending.lock().unwrap().insert(
                request_id.clone(),
                PendingElicitation {
                    owner: self.owner.id,
                    generation: query.as_ref().map(|query| query.generation),
                    sender: tx,
                },
            );
            query
        };
        let _pending = PendingElicitationGuard {
            router: self.clone(),
            request_id: request_id.clone(),
        };
        let query_generation = query.as_ref().map(|query| query.generation);
        if !(self.policy_allows_elicitation()
            || input_form && self.full_access_input_allowed(query_generation))
        {
            return McpElicitationResponse::decline();
        }
        let event = McpElicitationEvent {
            request_id,
            server_name,
            request,
        };
        let owner_cancelled = async {
            match query {
                Some(query) => query.signal.cancelled().await,
                None => std::future::pending::<()>().await,
            }
        };

        // Each connection subscribes only to its own runtime's pause state.
        let _pause = self.owner.pause_state.enter();
        let mut rx = rx;
        let presented = presenter.present(event);
        tokio::pin!(owner_cancelled);
        tokio::pin!(presented);
        let response = tokio::select! {
            result = &mut rx => result.unwrap_or_else(|_| McpElicitationResponse::cancel()),
            _ = &mut owner_cancelled => McpElicitationResponse::cancel(),
            _ = connection_signal.cancelled() => McpElicitationResponse::cancel(),
            _ = cancel_signal.cancelled() => McpElicitationResponse::cancel(),
            _ = &mut presented => tokio::select! {
                result = rx => result.unwrap_or_else(|_| McpElicitationResponse::cancel()),
                _ = &mut owner_cancelled => McpElicitationResponse::cancel(),
                _ = connection_signal.cancelled() => McpElicitationResponse::cancel(),
                _ = cancel_signal.cancelled() => McpElicitationResponse::cancel(),
            },
        };
        if input_form && response.action == McpElicitationAction::Cancel {
            return response.normalized();
        }
        if !(self.policy_allows_elicitation()
            || input_form && self.full_access_input_allowed(query_generation))
        {
            return McpElicitationResponse::decline();
        }
        response.normalized()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_values::runtime_json;

    fn runtime(text: &str) -> RuntimeJson {
        runtime_json::parse(&RuntimeString::from(text)).unwrap()
    }

    struct OwnerPresenter {
        events: tokio::sync::mpsc::UnboundedSender<McpElicitationEvent>,
        cancelled: tokio::sync::mpsc::UnboundedSender<RuntimeString>,
    }

    #[async_trait]
    impl McpElicitationPresenter for OwnerPresenter {
        async fn present(&self, event: McpElicitationEvent) {
            self.events.send(event).unwrap();
        }

        async fn cancelled(&self, request_id: RuntimeString) {
            self.cancelled.send(request_id).unwrap();
        }
    }

    fn owned_prompt(
        router: &McpElicitationRouter,
        name: &str,
        connection: AbortSignal,
    ) -> tokio::task::JoinHandle<McpElicitationResponse> {
        let router = router.clone();
        let name = RuntimeString::from(name);
        tokio::spawn(async move {
            router
                .route(
                    name,
                    McpElicitationRequest::Url {
                        meta: None,
                        message: "Approve this operation".into(),
                        url: "https://example.invalid/approval".into(),
                        elicitation_id: "owner-query".into(),
                    },
                    connection,
                    AbortSignal::new(),
                )
                .await
        })
    }

    async fn presented_owner_prompt(
        events: &mut tokio::sync::mpsc::UnboundedReceiver<McpElicitationEvent>,
    ) -> McpElicitationEvent {
        tokio::time::timeout(std::time::Duration::from_secs(2), events.recv())
            .await
            .unwrap()
            .unwrap()
    }

    #[tokio::test]
    async fn query_completion_preserves_other_owners_and_shared_reply_routing() {
        let (events, mut receive) = tokio::sync::mpsc::unbounded_channel();
        let (cancelled, mut cancellations) = tokio::sync::mpsc::unbounded_channel();
        let root = McpElicitationRouter::new(
            Some(Arc::new(OwnerPresenter { events, cancelled })),
            Arc::new(|| "shared-reply-id".to_owned()),
        );
        let child = root.fork_owner();
        let root_signal = AbortSignal::new();
        let root_query = root.begin_query(root_signal.clone());
        let child_query = child.begin_query(AbortSignal::new());
        let root_waiter = owned_prompt(&root, "root", AbortSignal::new());
        let root_event = presented_owner_prompt(&mut receive).await;
        let child_waiter = owned_prompt(&child, "child", AbortSignal::new());
        let child_event = presented_owner_prompt(&mut receive).await;
        assert_ne!(root_event.request_id, child_event.request_id);
        assert!(*root.pause_state().subscribe().borrow());
        assert!(*child.pause_state().subscribe().borrow());

        drop(root_query);
        assert_eq!(
            root_waiter.await.unwrap().action,
            McpElicitationAction::Cancel
        );
        assert_eq!(cancellations.recv().await.unwrap(), root_event.request_id);
        assert!(
            !root_signal.is_cancelled(),
            "ending a question scope does not cancel its task"
        );
        assert!(!child_waiter.is_finished());
        assert!(!*root.pause_state().subscribe().borrow());
        assert!(*child.pause_state().subscribe().borrow());
        assert_eq!(root.with_no_pending(|| Ok(17)).unwrap(), 17);

        root.resolve(
            &child_event.request_id,
            McpElicitationResponse {
                action: McpElicitationAction::Accept,
                content: None,
                meta: None,
            },
        )
        .unwrap();
        assert_eq!(
            child_waiter.await.unwrap().action,
            McpElicitationAction::Accept
        );
        assert!(!*child.pause_state().subscribe().borrow());
        drop(child_query);
        assert_eq!(root.pending_count(), 0);
        assert!(cancellations.try_recv().is_err());
    }

    #[tokio::test]
    async fn stale_query_drop_and_connection_close_do_not_cancel_newer_or_foreign_questions() {
        let (events, mut receive) = tokio::sync::mpsc::unbounded_channel();
        let (cancelled, mut cancellations) = tokio::sync::mpsc::unbounded_channel();
        let root = McpElicitationRouter::new(
            Some(Arc::new(OwnerPresenter { events, cancelled })),
            Arc::new(|| "query-generations".to_owned()),
        );
        let old_query = root.begin_query(AbortSignal::new());
        let old = owned_prompt(&root, "old", AbortSignal::new());
        let old_event = presented_owner_prompt(&mut receive).await;
        let new_query = root.begin_query(AbortSignal::new());
        let connection = AbortSignal::new();
        let new = owned_prompt(&root, "new", connection.clone());
        let new_event = presented_owner_prompt(&mut receive).await;
        let child = root.fork_owner();
        let child_query = child.begin_query(AbortSignal::new());
        let foreign = owned_prompt(&child, "child", AbortSignal::new());
        let foreign_event = presented_owner_prompt(&mut receive).await;

        drop(old_query);
        assert_eq!(old.await.unwrap().action, McpElicitationAction::Cancel);
        assert_eq!(cancellations.recv().await.unwrap(), old_event.request_id);
        assert!(!new.is_finished());
        assert!(!foreign.is_finished());
        let later = owned_prompt(&root, "later", AbortSignal::new());
        let later_event = presented_owner_prompt(&mut receive).await;

        connection.cancel();
        assert_eq!(new.await.unwrap().action, McpElicitationAction::Cancel);
        assert_eq!(cancellations.recv().await.unwrap(), new_event.request_id);
        assert!(!later.is_finished());
        assert!(!foreign.is_finished());
        drop(new_query);
        assert_eq!(later.await.unwrap().action, McpElicitationAction::Cancel);
        assert_eq!(cancellations.recv().await.unwrap(), later_event.request_id);
        assert!(!foreign.is_finished());
        root.cancel_all();
        assert_eq!(foreign.await.unwrap().action, McpElicitationAction::Cancel);
        assert_eq!(
            cancellations.recv().await.unwrap(),
            foreign_event.request_id
        );
        drop(child_query);
        assert_eq!(root.pending_count(), 0);
    }

    #[tokio::test]
    async fn query_end_cancels_a_prompt_before_presentation_acknowledgement() {
        struct HeldPresenter(tokio::sync::mpsc::UnboundedSender<McpElicitationEvent>);
        #[async_trait]
        impl McpElicitationPresenter for HeldPresenter {
            async fn present(&self, event: McpElicitationEvent) {
                self.0.send(event).unwrap();
                std::future::pending::<()>().await;
            }
        }
        let (events, mut receive) = tokio::sync::mpsc::unbounded_channel();
        let router = McpElicitationRouter::new(
            Some(Arc::new(HeldPresenter(events))),
            Arc::new(|| "pending-presentation".to_owned()),
        );
        let query = router.begin_query(AbortSignal::new());
        let waiter = owned_prompt(&router, "held", AbortSignal::new());
        let _event = presented_owner_prompt(&mut receive).await;
        drop(query);
        let response = tokio::time::timeout(std::time::Duration::from_secs(2), waiter)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(response.action, McpElicitationAction::Cancel);
        assert_eq!(router.pending_count(), 0);
        assert!(!*router.pause_state().subscribe().borrow());
    }

    #[tokio::test]
    async fn live_model_commit_rejects_pending_elicitation_then_allows_the_same_idle_connection() {
        struct Presenter(tokio::sync::mpsc::UnboundedSender<McpElicitationEvent>);
        #[async_trait]
        impl McpElicitationPresenter for Presenter {
            async fn present(&self, event: McpElicitationEvent) {
                self.0.send(event).unwrap();
            }
        }
        let (send, mut receive) = tokio::sync::mpsc::unbounded_channel();
        let router = McpElicitationRouter::new(
            Some(Arc::new(Presenter(send))),
            Arc::new(|| "fixture".to_string()),
        );
        let routed = router.clone();
        let waiter = tokio::spawn(async move {
            routed
                .route(
                    "fixture-server".into(),
                    McpElicitationRequest::Url {
                        meta: None,
                        message: "Approve fixture".into(),
                        url: "https://example.invalid/fixture".into(),
                        elicitation_id: "fixture".into(),
                    },
                    AbortSignal::new(),
                    AbortSignal::new(),
                )
                .await
        });
        let event = tokio::time::timeout(std::time::Duration::from_secs(2), receive.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(router
            .with_no_pending(|| Ok(()))
            .unwrap_err()
            .contains("elicitation is pending"));
        router
            .resolve(&event.request_id, McpElicitationResponse::cancel())
            .unwrap();
        assert_eq!(waiter.await.unwrap().action, McpElicitationAction::Cancel);
        assert_eq!(router.with_no_pending(|| Ok(17)).unwrap(), 17);
    }

    #[tokio::test]
    async fn a_dropped_elicitation_reports_cancellation_to_the_presenter() {
        struct Presenter {
            events: tokio::sync::mpsc::UnboundedSender<McpElicitationEvent>,
            cancelled: tokio::sync::mpsc::UnboundedSender<RuntimeString>,
        }
        #[async_trait]
        impl McpElicitationPresenter for Presenter {
            async fn present(&self, event: McpElicitationEvent) {
                self.events.send(event).unwrap();
            }
            async fn cancelled(&self, request_id: RuntimeString) {
                self.cancelled.send(request_id).unwrap();
            }
        }
        let (events, mut receive) = tokio::sync::mpsc::unbounded_channel();
        let (cancelled, mut cancellations) = tokio::sync::mpsc::unbounded_channel();
        let router = McpElicitationRouter::new(
            Some(Arc::new(Presenter { events, cancelled })),
            Arc::new(|| "fixture".to_string()),
        );
        let turn_signal = AbortSignal::new();
        router.set_turn_signal(Some(turn_signal.clone()));
        let request = parse_mcp_elicitation_request(
            "elicitation/create",
            runtime(r#"{"message":"Allow fixture?","requestedSchema":{"type":"object","properties":{}}}"#),
        )
        .unwrap();
        let routed = router.clone();
        let waiter = tokio::spawn(async move {
            routed
                .route(
                    "fixture-server".into(),
                    request,
                    AbortSignal::new(),
                    AbortSignal::new(),
                )
                .await
        });
        let event = receive.recv().await.unwrap();
        turn_signal.cancel();
        assert_eq!(waiter.await.unwrap().action, McpElicitationAction::Cancel);
        // The host must learn its card will never be answered.
        assert_eq!(cancellations.recv().await.unwrap(), event.request_id);
    }

    #[test]
    fn parses_backward_compatible_form_url_and_openai_form_requests() {
        let form = parse_mcp_elicitation_request(
            "elicitation/create",
            runtime(
                r#"{
                    "_meta": { "persist": true },
                    "message": "Configure access",
                    "requestedSchema": {
                        "type": "object",
                        "properties": {
                            "name": { "type": "string" },
                            "count": { "type": "number" },
                            "enabled": { "type": "boolean" },
                            "role": { "type": "string", "enum": ["reader", "writer"] },
                            "labels": {
                                "type": "array",
                                "items": { "type": "string", "enum": ["work", "personal"] }
                            }
                        }
                    }
                }"#,
            ),
        )
        .unwrap();
        assert_eq!(form.meta(), Some(&runtime(r#"{"persist":true}"#)));
        assert_eq!(
            runtime_codec::encode_value(&form)
                .unwrap()
                .get("mode")
                .and_then(RuntimeJson::as_string),
            Some(&RuntimeString::from("form"))
        );

        let url = parse_mcp_elicitation_request(
            "elicitation/create",
            runtime(
                r#"{
                    "mode": "url",
                    "message": "Authorize",
                    "url": "https://example.com/authorize",
                    "elicitationId": "opaque-1"
                }"#,
            ),
        )
        .unwrap();
        assert_eq!(
            runtime_codec::encode_value(&url)
                .unwrap()
                .get("mode")
                .and_then(RuntimeJson::as_string),
            Some(&RuntimeString::from("url"))
        );

        let openai = parse_mcp_elicitation_request(
            "openai/form",
            runtime(
                r#"{
                    "_meta": { "persist": false },
                    "message": "Pick one",
                    "requestedSchema": { "type": "object", "ui": { "columns": 2 } }
                }"#,
            ),
        )
        .unwrap();
        assert_eq!(
            runtime_codec::encode_value(&openai)
                .unwrap()
                .get("mode")
                .and_then(RuntimeJson::as_string),
            Some(&RuntimeString::from("openai/form"))
        );
    }

    #[test]
    fn elicitation_request_and_schema_preserve_surrogates_exactly() {
        let params = runtime(
            r#"{
                "message":"A\ud800B",
                "requestedSchema":{
                    "type":"object",
                    "properties":{
                        "key\udc00":{
                            "type":"string",
                            "title":"T\ud800",
                            "default":"\uFDD0ud800"
                        }
                    },
                    "required":["key\udc00"]
                }
            }"#,
        );
        let request = parse_mcp_elicitation_request("elicitation/create", params).unwrap();
        let encoded = runtime_codec::encode_value(&request).unwrap();
        let message = encoded
            .get("message")
            .and_then(RuntimeJson::as_string)
            .unwrap();
        assert_eq!(
            message,
            &RuntimeString::from_utf16([b'A' as u16, 0xd800, b'B' as u16])
        );
        let properties = encoded
            .get("requestedSchema")
            .and_then(RuntimeJson::as_object)
            .and_then(|schema| schema.get("properties"))
            .and_then(RuntimeJson::as_object)
            .unwrap();
        assert!(properties
            .get(RuntimeString::from_utf16([
                b'k' as u16,
                b'e' as u16,
                b'y' as u16,
                0xdc00,
            ]))
            .is_some());
    }

    #[test]
    fn elicitation_response_runtime_codec_omits_absent_fields() {
        let response = McpElicitationResponse::decline();
        assert_eq!(
            runtime_codec::encode_value(&response).unwrap(),
            runtime(r#"{"action":"decline"}"#)
        );
    }
}

#[cfg(test)]
mod approval_policy_tests {
    use super::*;
    use crate::types::permission::{ApprovalPolicy, GranularApprovalConfig};
    use std::sync::atomic::{AtomicBool, Ordering};

    struct Presenter(tokio::sync::mpsc::Sender<McpElicitationEvent>);
    #[async_trait]
    impl McpElicitationPresenter for Presenter {
        async fn present(&self, event: McpElicitationEvent) {
            self.0.send(event).await.unwrap();
        }
    }

    fn policy(allowed: bool) -> ApprovalPolicy {
        ApprovalPolicy::Granular(GranularApprovalConfig {
            sandbox_approval: false,
            rules: false,
            skill_approval: false,
            request_permissions: false,
            mcp_elicitations: allowed,
        })
    }

    fn request() -> McpElicitationRequest {
        McpElicitationRequest::Url {
            meta: None,
            message: "Authorize account".into(),
            url: "https://example.invalid/auth".into(),
            elicitation_id: "fixture".into(),
        }
    }

    fn confirmation() -> McpElicitationRequest {
        parse_mcp_elicitation_request(
            "elicitation/create",
            crate::runtime_values::runtime_json::parse(&RuntimeString::from(
                r#"{"message":"Allow application access?","requestedSchema":{"type":"object","properties":{}}}"#,
            ))
            .unwrap(),
        )
        .unwrap()
    }

    fn full_access_router() -> McpElicitationRouter {
        let router = McpElicitationRouter::disabled();
        router.set_permission_policy_provider(Arc::new(|| McpElicitationPolicy {
            approval_policy: ApprovalPolicy::Never,
            sandbox_mode: SandboxMode::DangerFullAccess,
        }));
        router
    }

    fn input_request() -> McpElicitationRequest {
        parse_mcp_elicitation_request(
            "elicitation/create",
            crate::runtime_values::runtime_json::parse(&RuntimeString::from(
                r#"{"message":"Which label?","requestedSchema":{"type":"object","properties":{"label":{"type":"string"}},"required":["label"]}}"#,
            ))
            .unwrap(),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn full_access_input_preserves_the_user_answer_and_rechecks_live_policy() {
        for narrow_policy in [false, true] {
            let (events, mut receive) = tokio::sync::mpsc::channel(1);
            let router = McpElicitationRouter::new(
                Some(Arc::new(Presenter(events))),
                Arc::new(|| "input".to_owned()),
            );
            let full = Arc::new(AtomicBool::new(true));
            let current = full.clone();
            router.set_permission_policy_provider(Arc::new(move || McpElicitationPolicy {
                approval_policy: ApprovalPolicy::Never,
                sandbox_mode: if current.load(Ordering::Acquire) {
                    SandboxMode::DangerFullAccess
                } else {
                    SandboxMode::WorkspaceWrite
                },
            }));
            let query = router.begin_query(AbortSignal::new());
            router.enable_form_input_for_turn(query.generation);
            let routed = router.clone();
            let waiting = tokio::spawn(async move {
                routed
                    .route(
                        "input-server".into(),
                        input_request(),
                        AbortSignal::new(),
                        AbortSignal::new(),
                    )
                    .await
            });
            let event = receive.recv().await.unwrap();
            assert!(*router.pause_state().subscribe().borrow());
            if narrow_policy {
                full.store(false, Ordering::Release);
            }
            let answer = crate::runtime_values::runtime_json::parse(&RuntimeString::from(
                r#"{"label":"user supplied value"}"#,
            ))
            .unwrap();
            router
                .resolve(
                    &event.request_id,
                    McpElicitationResponse {
                        action: McpElicitationAction::Accept,
                        content: Some(answer.clone()),
                        meta: None,
                    },
                )
                .unwrap();
            let response = waiting.await.unwrap();
            assert_eq!(
                response.action,
                if narrow_policy {
                    McpElicitationAction::Decline
                } else {
                    McpElicitationAction::Accept
                }
            );
            assert_eq!(
                response.content,
                if narrow_policy { None } else { Some(answer) }
            );
            assert_eq!(router.pending_count(), 0);
            assert!(!*router.pause_state().subscribe().borrow());
        }
    }

    #[tokio::test]
    async fn full_access_input_excludes_initialization_children_and_approval_forms() {
        let (events, mut receive) = tokio::sync::mpsc::channel(1);
        let router = McpElicitationRouter::new(
            Some(Arc::new(Presenter(events))),
            Arc::new(|| "input".to_owned()),
        );
        router.set_permission_policy_provider(Arc::new(|| McpElicitationPolicy {
            approval_policy: ApprovalPolicy::Never,
            sandbox_mode: SandboxMode::DangerFullAccess,
        }));
        let query = router.begin_query(AbortSignal::new());
        // Initialization has a query lease, but not an enabled input generation.
        assert_eq!(
            router
                .route(
                    "input-server".into(),
                    input_request(),
                    AbortSignal::new(),
                    AbortSignal::new()
                )
                .await
                .action,
            McpElicitationAction::Decline
        );
        router.enable_form_input_for_turn(query.generation);
        for metadata in [
            r#"{"persist":["session"]}"#,
            r#"{"klaus":{"computerUseApp":"Fixture App"}}"#,
        ] {
            let mut request = input_request();
            if let McpElicitationRequest::Form { meta, .. } = &mut request {
                *meta = Some(
                    crate::runtime_values::runtime_json::parse(&RuntimeString::from(metadata))
                        .unwrap(),
                );
            }
            assert_eq!(
                router
                    .route(
                        "input-server".into(),
                        request,
                        AbortSignal::new(),
                        AbortSignal::new()
                    )
                    .await
                    .action,
                McpElicitationAction::Decline
            );
        }
        let child = router.fork_owner();
        let child_query = child.begin_query(AbortSignal::new());
        child.enable_form_input_for_turn(child_query.generation);
        assert_eq!(
            child
                .route(
                    "input-server".into(),
                    input_request(),
                    AbortSignal::new(),
                    AbortSignal::new()
                )
                .await
                .action,
            McpElicitationAction::Decline
        );
        router.clear_turn_scope(query.generation);
        assert_eq!(
            router
                .route(
                    "input-server".into(),
                    input_request(),
                    AbortSignal::new(),
                    AbortSignal::new()
                )
                .await
                .action,
            McpElicitationAction::Decline
        );
        assert!(receive.try_recv().is_err());
    }

    #[tokio::test]
    async fn full_access_confirmation_needs_no_presenter_or_pending_card() {
        let router = full_access_router();
        let _query = router.begin_query(AbortSignal::new());
        let response = router
            .route(
                "registered-server".into(),
                confirmation(),
                AbortSignal::new(),
                AbortSignal::new(),
            )
            .await;
        assert_eq!(response.action, McpElicitationAction::Accept);
        assert_eq!(
            response.content,
            Some(RuntimeJson::Object(Default::default()))
        );
        assert_eq!(router.pending_count(), 0);
        assert!(!*router.pause_state().subscribe().borrow());
    }

    #[tokio::test]
    async fn full_access_does_not_invent_answers_or_authorize_url_flows() {
        let router = full_access_router();
        let _query = router.begin_query(AbortSignal::new());
        let mut form = confirmation();
        if let McpElicitationRequest::Form {
            requested_schema, ..
        } = &mut form
        {
            requested_schema.properties.insert(
                "answer".into(),
                McpElicitationPrimitiveSchema::Boolean(McpElicitationBooleanSchema {
                    type_: McpElicitationBooleanType::Boolean,
                    title: None,
                    description: None,
                    default: None,
                }),
            );
        }
        let extended = McpElicitationRequest::OpenAiForm {
            meta: None,
            message: "Confirm extended request".into(),
            requested_schema: RuntimeJson::Object(Default::default()),
        };
        for request in [form, request(), extended] {
            assert_eq!(
                router
                    .route(
                        "registered-server".into(),
                        request,
                        AbortSignal::new(),
                        AbortSignal::new()
                    )
                    .await
                    .action,
                McpElicitationAction::Decline
            );
        }
        assert_eq!(router.pending_count(), 0);
    }

    #[tokio::test]
    async fn full_access_confirmation_preserves_cancellation_and_query_ownership() {
        let router = full_access_router();
        assert_eq!(
            router
                .route(
                    "registered-server".into(),
                    confirmation(),
                    AbortSignal::new(),
                    AbortSignal::new()
                )
                .await
                .action,
            McpElicitationAction::Decline
        );
        for source in 0..3 {
            let turn = AbortSignal::new();
            let connection = AbortSignal::new();
            let call = AbortSignal::new();
            let _query = router.begin_query(turn.clone());
            [&turn, &connection, &call][source].cancel();
            assert_eq!(
                router
                    .route("registered-server".into(), confirmation(), connection, call)
                    .await
                    .action,
                McpElicitationAction::Cancel
            );
        }
        let child = router.fork_owner();
        let _child_query = child.begin_query(AbortSignal::new());
        assert_eq!(
            child
                .route(
                    "registered-server".into(),
                    confirmation(),
                    AbortSignal::new(),
                    AbortSignal::new()
                )
                .await
                .action,
            McpElicitationAction::Decline
        );
    }

    #[tokio::test]
    async fn confirmation_reads_live_sandbox_authority_instead_of_a_cached_preset() {
        let router = full_access_router();
        let full = Arc::new(AtomicBool::new(true));
        let current = full.clone();
        router.set_permission_policy_provider(Arc::new(move || McpElicitationPolicy {
            approval_policy: ApprovalPolicy::Never,
            sandbox_mode: if current.load(Ordering::Acquire) {
                SandboxMode::DangerFullAccess
            } else {
                SandboxMode::WorkspaceWrite
            },
        }));
        let _query = router.begin_query(AbortSignal::new());
        for expected in [McpElicitationAction::Accept, McpElicitationAction::Decline] {
            let response = router
                .route(
                    "registered-server".into(),
                    confirmation(),
                    AbortSignal::new(),
                    AbortSignal::new(),
                )
                .await;
            assert_eq!(response.action, expected);
            full.store(false, Ordering::Release);
        }
    }

    #[tokio::test]
    async fn disabled_elicitation_does_not_present_a_card_or_retain_a_pending_request() {
        let (events, mut receive) = tokio::sync::mpsc::channel(1);
        let router = McpElicitationRouter::new(
            Some(Arc::new(Presenter(events))),
            Arc::new(|| "fixture".to_owned()),
        );
        router.set_approval_policy_provider(Arc::new(|| policy(false)));
        let response = router
            .route(
                "registered-server".into(),
                request(),
                AbortSignal::new(),
                AbortSignal::new(),
            )
            .await;
        assert_eq!(response.action, McpElicitationAction::Decline);
        assert_eq!(router.pending_count(), 0);
        assert!(receive.try_recv().is_err());
    }

    #[tokio::test]
    async fn changed_live_policy_rejects_an_answer_to_an_earlier_eligible_card() {
        let (events, mut receive) = tokio::sync::mpsc::channel(1);
        let router = McpElicitationRouter::new(
            Some(Arc::new(Presenter(events))),
            Arc::new(|| "fixture".to_owned()),
        );
        let allowed = Arc::new(AtomicBool::new(true));
        let current = allowed.clone();
        router.set_approval_policy_provider(Arc::new(move || {
            policy(current.load(Ordering::Acquire))
        }));
        let routed = router.clone();
        let waiting = tokio::spawn(async move {
            routed
                .route(
                    "registered-server".into(),
                    request(),
                    AbortSignal::new(),
                    AbortSignal::new(),
                )
                .await
        });
        let event = receive.recv().await.unwrap();
        allowed.store(false, Ordering::Release);
        router
            .resolve(
                &event.request_id,
                McpElicitationResponse {
                    action: McpElicitationAction::Accept,
                    content: None,
                    meta: None,
                },
            )
            .unwrap();
        assert_eq!(waiting.await.unwrap().action, McpElicitationAction::Decline);
        assert_eq!(router.pending_count(), 0);
    }
}
