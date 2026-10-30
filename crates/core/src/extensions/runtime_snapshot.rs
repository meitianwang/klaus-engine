//! Effective extension diagnostics for one running session.
//!
//! This surface intentionally carries identity and provenance only. Extension
//! bodies, prompts, hook commands, environments, settings, and pricing rates
//! remain in their runtime owners and never cross a diagnostics boundary.

use crate::extensions::plugins::runtime::PluginRuntimeSnapshot;
use crate::hooks::types::HookEvent;
use crate::runtime_values::runtime_codec::{RuntimeEnumVariant, RuntimeExternalVariant};
use crate::runtime_values::{
    runtime_codec, RuntimeCodecError, RuntimeCodecErrorKind, RuntimeDecode, RuntimeEncode,
    RuntimeEncoder, RuntimeJson, RuntimeObjectEncoder, RuntimeString,
};

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
        RuntimeExternalVariant::Payload { .. } => Err(RuntimeCodecError::new(
            RuntimeCodecErrorKind::InvalidEnumShape,
        )),
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExtensionKind {
    Agent,
    Skill,
    OutputStyle,
}

impl RuntimeEncode for ExtensionKind {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Agent => "agent",
            Self::Skill => "skill",
            Self::OutputStyle => "output_style",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for ExtensionKind {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("agent", &[]),
                RuntimeEnumVariant::new("skill", &[]),
                RuntimeEnumVariant::new("output_style", &[]),
            ],
            &[Self::Agent, Self::Skill, Self::OutputStyle],
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExtensionSourceKind {
    Builtin,
    Plugin,
    Host,
    Session,
}

impl RuntimeEncode for ExtensionSourceKind {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Builtin => "builtin",
            Self::Plugin => "plugin",
            Self::Host => "host",
            Self::Session => "session",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for ExtensionSourceKind {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("builtin", &[]),
                RuntimeEnumVariant::new("plugin", &[]),
                RuntimeEnumVariant::new("host", &[]),
                RuntimeEnumVariant::new("session", &[]),
            ],
            &[Self::Builtin, Self::Plugin, Self::Host, Self::Session],
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtensionSource {
    pub kind: ExtensionSourceKind,
    pub id: Option<RuntimeString>,
}

impl RuntimeEncode for ExtensionSource {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("kind", &self.kind)?;
        if let Some(value) = &self.id {
            object.field("id", value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for ExtensionSource {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                kind: object.required("kind", &[])?,
                id: object.optional("id", &[])?,
            })
        })
    }
}

impl ExtensionSource {
    pub fn builtin() -> Self {
        Self {
            kind: ExtensionSourceKind::Builtin,
            id: None,
        }
    }

    pub fn plugin(name: impl Into<RuntimeString>) -> Self {
        Self {
            kind: ExtensionSourceKind::Plugin,
            id: Some(name.into()),
        }
    }

    pub fn host() -> Self {
        Self {
            kind: ExtensionSourceKind::Host,
            id: None,
        }
    }

    pub fn session() -> Self {
        Self {
            kind: ExtensionSourceKind::Session,
            id: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EffectiveExtension {
    pub name: RuntimeString,
    pub source: ExtensionSource,
}

impl RuntimeEncode for EffectiveExtension {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("name", &self.name)?;
        object.field("source", &self.source)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for EffectiveExtension {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                name: object.required("name", &[])?,
                source: object.required("source", &[])?,
            })
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HookHandlerKind {
    Command,
    Function,
}

impl RuntimeEncode for HookHandlerKind {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Command => "command",
            Self::Function => "function",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for HookHandlerKind {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("command", &[]),
                RuntimeEnumVariant::new("function", &[]),
            ],
            &[Self::Command, Self::Function],
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EffectiveHook {
    pub event: HookEvent,
    pub matcher: Option<RuntimeString>,
    pub handler_kind: HookHandlerKind,
    pub is_async: bool,
    pub timeout_ms: Option<u64>,
    pub source: ExtensionSource,
}

impl RuntimeEncode for EffectiveHook {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("event", &self.event)?;
        if let Some(value) = &self.matcher {
            object.field("matcher", value)?;
        }
        object.field("handlerKind", &self.handler_kind)?;
        object.field("isAsync", &self.is_async)?;
        if let Some(value) = &self.timeout_ms {
            object.field("timeoutMs", value)?;
        }
        object.field("source", &self.source)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for EffectiveHook {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                event: object.required("event", &[])?,
                matcher: object.optional("matcher", &[])?,
                handler_kind: object.required("handlerKind", &[])?,
                is_async: object.required("isAsync", &[])?,
                timeout_ms: object.optional("timeoutMs", &[])?,
                source: object.required("source", &[])?,
            })
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PricingRuntimeSnapshot {
    pub model_ids: Vec<RuntimeString>,
    pub source: ExtensionSource,
}

impl RuntimeEncode for PricingRuntimeSnapshot {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("modelIds", &self.model_ids)?;
        object.field("source", &self.source)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for PricingRuntimeSnapshot {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                model_ids: object.required("modelIds", &[])?,
                source: object.required("source", &[])?,
            })
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SuppressedExtension {
    pub kind: ExtensionKind,
    pub name: RuntimeString,
    pub source: ExtensionSource,
    pub replaced_by: ExtensionSource,
    pub reason: RuntimeString,
}

impl RuntimeEncode for SuppressedExtension {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("kind", &self.kind)?;
        object.field("name", &self.name)?;
        object.field("source", &self.source)?;
        object.field("replacedBy", &self.replaced_by)?;
        object.field("reason", &self.reason)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for SuppressedExtension {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                kind: object.required("kind", &[])?,
                name: object.required("name", &[])?,
                source: object.required("source", &[])?,
                replaced_by: object.required("replacedBy", &[])?,
                reason: object.required("reason", &[])?,
            })
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtensionRuntimeSnapshot {
    pub agents: Vec<EffectiveExtension>,
    pub skills: Vec<EffectiveExtension>,
    pub output_styles: Vec<EffectiveExtension>,
    pub selected_output_style: Option<RuntimeString>,
    pub hooks: Vec<EffectiveHook>,
    pub pricing: PricingRuntimeSnapshot,
    pub suppressed: Vec<SuppressedExtension>,
    pub plugin_runtime: PluginRuntimeSnapshot,
}

impl RuntimeEncode for ExtensionRuntimeSnapshot {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("agents", &self.agents)?;
        object.field("skills", &self.skills)?;
        object.field("outputStyles", &self.output_styles)?;
        if let Some(value) = &self.selected_output_style {
            object.field("selectedOutputStyle", value)?;
        }
        object.field("hooks", &self.hooks)?;
        object.field("pricing", &self.pricing)?;
        object.field("suppressed", &self.suppressed)?;
        object.field("pluginRuntime", &self.plugin_runtime)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for ExtensionRuntimeSnapshot {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                agents: object.required("agents", &[])?,
                skills: object.required("skills", &[])?,
                output_styles: object.required("outputStyles", &[])?,
                selected_output_style: object.optional("selectedOutputStyle", &[])?,
                hooks: object.required("hooks", &[])?,
                pricing: object.required("pricing", &[])?,
                suppressed: object.required("suppressed", &[])?,
                plugin_runtime: object.required("pluginRuntime", &[])?,
            })
        })
    }
}

impl Default for ExtensionRuntimeSnapshot {
    fn default() -> Self {
        Self {
            agents: Vec::new(),
            skills: Vec::new(),
            output_styles: Vec::new(),
            selected_output_style: None,
            hooks: Vec::new(),
            pricing: PricingRuntimeSnapshot {
                model_ids: Vec::new(),
                source: ExtensionSource::builtin(),
            },
            suppressed: Vec::new(),
            plugin_runtime: PluginRuntimeSnapshot::default(),
        }
    }
}
