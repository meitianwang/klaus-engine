//! Provider contract: request/response shapes + the `ModelProvider` seam.

use crate::runtime_values::runtime_codec::{RuntimeEnumVariant, RuntimeExternalVariant};
use crate::runtime_values::{
    runtime_codec, RuntimeCodecError, RuntimeCodecErrorKind, RuntimeDecode, RuntimeEncode,
    RuntimeEncoder, RuntimeJson, RuntimeObject, RuntimeObjectEncoder, RuntimeString,
};
use crate::schemas::config::ContextManagementConfig;
pub use crate::schemas::content::WebSearchAction;
use crate::schemas::content::{Citation, NativeSearchResult};
use crate::schemas::effort::{EffortLevel, EffortValue};
use crate::schemas::message::{Message, Usage};
use crate::types::abort::AbortSignal;
use crate::types::errors::ProviderError;
use futures::stream::Stream;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::num::NonZeroU32;
use std::pin::Pin;
use std::sync::Arc;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolSpecKind {
    #[default]
    Function,
    Freeform,
    ToolSearch,
}

impl ToolSpecKind {
    fn is_function(value: &Self) -> bool {
        *value == Self::Function
    }
}

impl RuntimeEncode for ToolSpecKind {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Function => "function",
            Self::Freeform => "freeform",
            Self::ToolSearch => "tool_search",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for ToolSpecKind {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("function", &[]),
                RuntimeEnumVariant::new("freeform", &[]),
                RuntimeEnumVariant::new("tool_search", &[]),
            ],
            &[Self::Function, Self::Freeform, Self::ToolSearch],
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ToolSpecGrammarSyntax {
    Lark,
    Regex,
}

impl RuntimeEncode for ToolSpecGrammarSyntax {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Lark => "lark",
            Self::Regex => "regex",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for ToolSpecGrammarSyntax {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("lark", &[]),
                RuntimeEnumVariant::new("regex", &[]),
            ],
            &[Self::Lark, Self::Regex],
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ToolSpecFormat {
    Text,
    Grammar {
        syntax: ToolSpecGrammarSyntax,
        definition: RuntimeString,
    },
}

impl RuntimeEncode for ToolSpecFormat {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let (tag, fields) = match self {
            Self::Text => ("text", RuntimeObjectEncoder::new()),
            Self::Grammar { syntax, definition } => {
                let mut fields = RuntimeObjectEncoder::new();
                fields.field("syntax", syntax)?;
                fields.field("definition", definition)?;
                ("grammar", fields)
            }
        };
        runtime_codec::encode_internal(out, "type", &[], tag, &fields.finish())
    }
}

impl RuntimeDecode for ToolSpecFormat {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_internal(
            value,
            false,
            "type",
            &[],
            &[
                RuntimeEnumVariant::new("text", &[]),
                RuntimeEnumVariant::new("grammar", &[]),
            ],
            |variant, object| match variant {
                0 => Ok(Self::Text),
                1 => Ok(Self::Grammar {
                    syntax: object.required("syntax", &[])?,
                    definition: object.required("definition", &[])?,
                }),
                _ => Err(RuntimeCodecError::new(
                    RuntimeCodecErrorKind::UnknownEnumVariant,
                )),
            },
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ToolSpecFormatWire {
    Canonical(ToolSpecFormat),
    Legacy(RuntimeString),
}

impl RuntimeDecode for ToolSpecFormatWire {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        match value {
            RuntimeJson::String(value) => Ok(Self::Legacy(value.clone())),
            _ => ToolSpecFormat::decode_runtime(value).map(Self::Canonical),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ToolSpecWire {
    name: RuntimeString,
    description: RuntimeString,
    input_schema: Option<RuntimeJson>,
    kind: Option<ToolSpecKind>,
    format: Option<ToolSpecFormatWire>,
}

impl RuntimeDecode for ToolSpecWire {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                name: object.required("name", &[])?,
                description: object.required("description", &[])?,
                input_schema: object.optional("inputSchema", &[])?,
                kind: object.optional("kind", &[])?,
                format: object.optional("format", &[])?,
            })
        })
    }
}

/// A provider-agnostic tool spec sent in the request.
#[derive(Debug, Clone, PartialEq)]
pub struct ToolSpec {
    pub name: RuntimeString,
    pub description: RuntimeString,
    pub input_schema: RuntimeJson,
    pub kind: ToolSpecKind,
    pub format: Option<ToolSpecFormat>,
}

impl RuntimeEncode for ToolSpec {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("name", &self.name)?;
        object.field("description", &self.description)?;
        object.field("inputSchema", &self.input_schema)?;
        if !ToolSpecKind::is_function(&self.kind) {
            object.field("kind", &self.kind)?;
        }
        if let Some(format) = &self.format {
            object.field("format", format)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for ToolSpec {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        let (value, external_kind) = match value {
            RuntimeJson::Object(object) if object.len() == 1 => {
                if let Some(value) = object.get("Function") {
                    (value, Some(ToolSpecKind::Function))
                } else if let Some(value) = object.get("Freeform") {
                    (value, Some(ToolSpecKind::Freeform))
                } else {
                    (value, None)
                }
            }
            value => (value, None),
        };
        let wire = ToolSpecWire::decode_runtime(value)?;
        let declared_kind = external_kind.or(wire.kind);
        let legacy_format = matches!(wire.format.as_ref(), Some(ToolSpecFormatWire::Legacy(_)));
        if legacy_format && declared_kind != Some(ToolSpecKind::Freeform) {
            return Err(RuntimeCodecError::new(RuntimeCodecErrorKind::Custom(
                RuntimeString::from("legacy string tool format requires an explicit freeform kind"),
            )));
        }
        let legacy_schema_less_freeform = wire.input_schema.is_none()
            && declared_kind == Some(ToolSpecKind::Freeform)
            && legacy_format;
        let kind = declared_kind.unwrap_or(ToolSpecKind::Function);
        let input_schema = match wire.input_schema {
            Some(schema) => schema,
            None if legacy_schema_less_freeform => open_fallback_tool_schema(),
            None => {
                return Err(RuntimeCodecError::new(RuntimeCodecErrorKind::MissingField)
                    .prepend("inputSchema"));
            }
        };
        let format = wire.format.map(|format| match format {
            ToolSpecFormatWire::Canonical(format) => format,
            ToolSpecFormatWire::Legacy(_legacy) => ToolSpecFormat::Text,
        });
        Ok(Self {
            name: wire.name,
            description: wire.description,
            input_schema,
            kind,
            format,
        })
    }
}

pub(crate) fn open_fallback_tool_schema() -> RuntimeJson {
    let mut object = RuntimeObject::new();
    object.insert(
        RuntimeString::from("type"),
        RuntimeJson::String(RuntimeString::from("object")),
    );
    object.insert(
        RuntimeString::from("properties"),
        RuntimeJson::Object(RuntimeObject::new()),
    );
    object.insert(
        RuntimeString::from("additionalProperties"),
        RuntimeJson::Bool(true),
    );
    RuntimeJson::Object(object)
}

impl ToolSpec {
    pub fn name(&self) -> &RuntimeString {
        &self.name
    }

    pub fn description(&self) -> &RuntimeString {
        &self.description
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderToolCallKind {
    #[default]
    Function,
    Custom,
    ToolSearch,
}

impl ProviderToolCallKind {
    pub(crate) fn is_implicit(value: &Option<Self>) -> bool {
        matches!(value, None | Some(Self::Function))
    }
}

impl RuntimeEncode for ProviderToolCallKind {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Function => "function",
            Self::Custom => "custom",
            Self::ToolSearch => "tool_search",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for ProviderToolCallKind {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("function", &[]),
                RuntimeEnumVariant::new("custom", &[]),
                RuntimeEnumVariant::new("tool_search", &[]),
            ],
            &[Self::Function, Self::Custom, Self::ToolSearch],
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolInputFormat {
    #[default]
    Json,
    Raw,
}

impl ToolInputFormat {
    fn is_implicit(value: &Option<Self>) -> bool {
        matches!(value, None | Some(Self::Json))
    }
}

impl RuntimeEncode for ToolInputFormat {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Json => "json",
            Self::Raw => "raw",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for ToolInputFormat {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("json", &[]),
                RuntimeEnumVariant::new("raw", &[]),
            ],
            &[Self::Json, Self::Raw],
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ToolChoice {
    Auto,
    Any,
    Tool { name: RuntimeString },
    None,
}

impl RuntimeEncode for ToolChoice {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let (tag, fields) = match self {
            Self::Auto => ("auto", RuntimeObjectEncoder::new()),
            Self::Any => ("any", RuntimeObjectEncoder::new()),
            Self::Tool { name } => {
                let mut fields = RuntimeObjectEncoder::new();
                fields.field("name", name)?;
                ("tool", fields)
            }
            Self::None => ("none", RuntimeObjectEncoder::new()),
        };
        runtime_codec::encode_internal(out, "type", &[], tag, &fields.finish())
    }
}

impl RuntimeDecode for ToolChoice {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_internal(
            value,
            false,
            "type",
            &[],
            &[
                RuntimeEnumVariant::new("auto", &[]),
                RuntimeEnumVariant::new("any", &[]),
                RuntimeEnumVariant::new("tool", &[]),
                RuntimeEnumVariant::new("none", &[]),
            ],
            |variant, object| match variant {
                0 => Ok(Self::Auto),
                1 => Ok(Self::Any),
                2 => Ok(Self::Tool {
                    name: object.required("name", &[])?,
                }),
                3 => Ok(Self::None),
                _ => Err(RuntimeCodecError::new(
                    RuntimeCodecErrorKind::UnknownEnumVariant,
                )),
            },
        )
    }
}

/// Extended-thinking request — a three-state thinking config. `adaptive` lets
/// the model size its own thinking; `enabled` requests a fixed token budget;
/// `disabled` omits thinking. The provider translates this per model capability.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "lowercase",
    rename_all_fields = "camelCase"
)]
pub enum ThinkingConfig {
    Adaptive,
    Enabled {
        #[serde(skip_serializing_if = "Option::is_none", default)]
        budget_tokens: Option<u64>,
    },
    Disabled,
}

impl RuntimeEncode for ThinkingConfig {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let (tag, fields) = match self {
            Self::Adaptive => ("adaptive", RuntimeObjectEncoder::new()),
            Self::Enabled { budget_tokens } => {
                let mut fields = RuntimeObjectEncoder::new();
                if let Some(budget_tokens) = budget_tokens {
                    fields.field("budgetTokens", budget_tokens)?;
                }
                ("enabled", fields)
            }
            Self::Disabled => ("disabled", RuntimeObjectEncoder::new()),
        };
        runtime_codec::encode_internal(out, "type", &[], tag, &fields.finish())
    }
}

impl RuntimeDecode for ThinkingConfig {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_internal(
            value,
            false,
            "type",
            &[],
            &[
                RuntimeEnumVariant::new("adaptive", &[]),
                RuntimeEnumVariant::new("enabled", &[]),
                RuntimeEnumVariant::new("disabled", &[]),
            ],
            |variant, object| match variant {
                0 => Ok(Self::Adaptive),
                1 => Ok(Self::Enabled {
                    budget_tokens: object.optional("budgetTokens", &[])?,
                }),
                2 => Ok(Self::Disabled),
                _ => Err(RuntimeCodecError::new(
                    RuntimeCodecErrorKind::UnknownEnumVariant,
                )),
            },
        )
    }
}

/// A session-invariant cache scope for a leading system prompt part
/// (the 'global' cache scope).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CacheScope {
    Global,
}

impl RuntimeEncode for CacheScope {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Global => "global",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for CacheScope {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[RuntimeEnumVariant::new("global", &[])],
            &[Self::Global],
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SystemPromptPart {
    /// Stable name for memo-caching and prompt-cache breakpoints.
    pub name: RuntimeString,
    pub text: RuntimeString,
    /// Hint the provider to set a cache breakpoint after this part.
    pub cacheable: Option<bool>,
    /// Mark a session-invariant part: its cache breakpoint is reserved and, on
    /// first-party, carries `scope:'global'` so the prefix is cached ACROSS
    /// sessions/org rather than per-session. Only valid on a contiguous LEADING
    /// run of invariant parts.
    pub cache_scope: Option<CacheScope>,
}

impl RuntimeEncode for SystemPromptPart {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("name", &self.name)?;
        object.field("text", &self.text)?;
        if let Some(cacheable) = &self.cacheable {
            object.field("cacheable", cacheable)?;
        }
        if let Some(cache_scope) = &self.cache_scope {
            object.field("cacheScope", cache_scope)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for SystemPromptPart {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                name: object.required("name", &[])?,
                text: object.required("text", &[])?,
                cacheable: object.optional("cacheable", &[])?,
                cache_scope: object.optional("cacheScope", &[])?,
            })
        })
    }
}

/// Provider-side search context budget: the `search_context_size`
/// knob for hosted web-search providers that expose it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WebSearchContextSize {
    Low,
    Medium,
    High,
}

impl RuntimeEncode for WebSearchContextSize {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for WebSearchContextSize {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("low", &[]),
                RuntimeEnumVariant::new("medium", &[]),
                RuntimeEnumVariant::new("high", &[]),
            ],
            &[Self::Low, Self::Medium, Self::High],
        )
    }
}

/// Approximate user location forwarded to a provider-native web search.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct UserLocation {
    pub city: Option<RuntimeString>,
    pub region: Option<RuntimeString>,
    pub country: Option<RuntimeString>,
    pub timezone: Option<RuntimeString>,
}

impl RuntimeEncode for UserLocation {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        if let Some(city) = &self.city {
            object.field("city", city)?;
        }
        if let Some(region) = &self.region {
            object.field("region", region)?;
        }
        if let Some(country) = &self.country {
            object.field("country", country)?;
        }
        if let Some(timezone) = &self.timezone {
            object.field("timezone", timezone)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for UserLocation {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                city: object.optional("city", &[])?,
                region: object.optional("region", &[])?,
                country: object.optional("country", &[])?,
                timezone: object.optional("timezone", &[])?,
            })
        })
    }
}

/// Neutral parameters for a provider-native web search (see
/// [`ModelRequest::web_search`]).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct WebSearchRequest {
    pub access: crate::schemas::config::WebSearchAccess,
    /// Cap on server-side searches this turn (Anthropic `max_uses`; default 8).
    pub max_uses: Option<u64>,
    /// Restrict results to these domains. Mutually exclusive with
    /// `blocked_domains`.
    pub allowed_domains: Option<Vec<RuntimeString>>,
    /// Exclude these domains. Anthropic-only; OpenAI/Gemini adapters drop it.
    pub blocked_domains: Option<Vec<RuntimeString>>,
    /// Approximate user location forwarded to the search.
    pub user_location: Option<UserLocation>,
    /// Provider-side search context budget (OpenAI `search_context_size`).
    pub context_size: Option<WebSearchContextSize>,
}

impl RuntimeEncode for WebSearchRequest {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        if self.access != crate::schemas::config::WebSearchAccess::Live {
            object.field("access", &self.access)?;
        }
        if let Some(max_uses) = &self.max_uses {
            object.field("maxUses", max_uses)?;
        }
        if let Some(allowed_domains) = &self.allowed_domains {
            object.field("allowedDomains", allowed_domains)?;
        }
        if let Some(blocked_domains) = &self.blocked_domains {
            object.field("blockedDomains", blocked_domains)?;
        }
        if let Some(user_location) = &self.user_location {
            object.field("userLocation", user_location)?;
        }
        if let Some(context_size) = &self.context_size {
            object.field("contextSize", context_size)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for WebSearchRequest {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                access: object.defaulted("access", &[], Default::default)?,
                max_uses: object.optional("maxUses", &[])?,
                allowed_domains: object.optional("allowedDomains", &[])?,
                blocked_domains: object.optional("blockedDomains", &[])?,
                user_location: object.optional("userLocation", &[])?,
                context_size: object.optional("contextSize", &[])?,
            })
        })
    }
}

/// Use the 1-hour prompt-cache TTL instead of the default 5 minutes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CacheTtl {
    #[serde(rename = "1h")]
    OneHour,
}

/// Idle gap past which every provider prompt cache is certainly gone: the longest
/// TTL any of them offers is one hour, so beyond it no cached prefix can survive
/// and nothing is lost by re-declaring what the next request sends.
pub const PROMPT_CACHE_COLD_AFTER_MS: i64 = 60 * 60 * 1_000;

impl RuntimeEncode for CacheTtl {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::OneHour => "1h",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for CacheTtl {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[RuntimeEnumVariant::new("1h", &[])],
            &[Self::OneHour],
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ModelRequest {
    /// Native-only capability binding; decoded requests must rebind their model.
    pub supports_image_detail_original: bool,
    pub model: RuntimeString,
    /// Internal request-view limits captured for this model, not upstream API parameters.
    pub image_limits: Option<ImageLimits>,
    pub max_media_blocks: Option<NonZeroU32>,
    pub system: Vec<SystemPromptPart>,
    pub messages: Vec<Message>,
    pub tools: Vec<ToolSpec>,
    pub tool_choice: Option<ToolChoice>,
    pub max_output_tokens: u64,
    /// Agentic task budget (Anthropic task-budgets beta): the output-token
    /// budget the model self-moderates against across the whole loop, distinct
    /// from the enforced `max_output_tokens` per-response ceiling.
    pub task_budget: Option<u64>,
    pub output_schema: Option<RuntimeJson>,
    /// Request the PROVIDER-NATIVE server-side web search for this turn.
    pub web_search: Option<WebSearchRequest>,
    pub thinking: Option<ThinkingConfig>,
    /// Normalized reasoning-effort signal. Handed to the provider
    /// unchanged; each provider owns the wire encoding.
    pub effort: Option<EffortValue>,
    pub model_verbosity: Option<OutputVerbosity>,
    pub reasoning_summary: ReasoningSummary,
    /// Fire-and-forget request whose message suffix no later request will resume
    /// (skips the cache write — set for detached forks).
    pub skip_cache_write: Option<bool>,
    /// Use the 1-hour prompt-cache TTL instead of the default 5 minutes.
    pub cache_ttl: Option<CacheTtl>,
    /// Stable per-conversation cache-routing key (the `prompt_cache_key` field).
    pub cache_key: Option<RuntimeString>,
    /// API-level context editing (Anthropic `context-management` beta). When
    /// present, the Anthropic first-party adapter asks the server to clear aged
    /// tool blocks in-flight; other adapters ignore it.
    pub context_management: Option<ContextManagementConfig>,
    /// Captured native context-hint intent. Decoded requests must explicitly
    /// rebind it; it is never a serialized model request or conversation field.
    pub context_hints: Option<crate::context::context_hints::ContextHintRequest>,
    /// Server-side handling of prior-turn thinking blocks, or `None` to leave them
    /// to whatever policy the server applies by default.
    pub thinking_context: Option<ThinkingContextPolicy>,
}

impl ModelRequest {
    /// Rebind from the selected model's snapshot, including undeclared defaults.
    /// An empty declaration still pins the resolver's defaults rather than
    /// allowing a transport to borrow newly refreshed provider metadata.
    pub fn bind_media_capabilities(&mut self, capabilities: &ProviderCapabilities) {
        self.supports_image_detail_original = capabilities.supports_image_detail_original;
        self.image_limits = Some(capabilities.image_limits.unwrap_or_default());
        self.max_media_blocks = Some(
            capabilities
                .max_media_blocks
                .unwrap_or(DEFAULT_MAX_MEDIA_BLOCKS),
        );
    }
}

/// How the server should treat the thinking blocks of PRIOR assistant turns.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ThinkingContextPolicy {
    /// Pin every thinking turn, so no server-side default sheds reasoning the
    /// model may still need.
    KeepAll,
    /// Keep only the most recent thinking turn. Chosen once the session has been
    /// idle past the longest prompt-cache lifetime: the cached prefix is already
    /// gone, so the aged thinking blocks are pure weight on a prefix that has to
    /// be rewritten anyway. The choice is sticky for the rest of the session
    /// because it is part of the cache key — flipping back would invalidate the
    /// prefix a second time.
    KeepMostRecentTurn,
}

impl RuntimeEncode for ThinkingContextPolicy {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::KeepAll => "keepAll",
            Self::KeepMostRecentTurn => "keepMostRecentTurn",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for ThinkingContextPolicy {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("keepAll", &[]),
                RuntimeEnumVariant::new("keepMostRecentTurn", &[]),
            ],
            &[Self::KeepAll, Self::KeepMostRecentTurn],
        )
    }
}

impl RuntimeEncode for ModelRequest {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("model", &self.model)?;
        if let Some(value) = &self.image_limits {
            object.field("imageLimits", value)?;
        }
        if let Some(value) = &self.max_media_blocks {
            object.field("maxMediaBlocks", value)?;
        }
        object.field("system", &self.system)?;
        object.field("messages", &self.messages)?;
        object.field("tools", &self.tools)?;
        if let Some(tool_choice) = &self.tool_choice {
            object.field("toolChoice", tool_choice)?;
        }
        object.field("maxOutputTokens", &self.max_output_tokens)?;
        if let Some(task_budget) = &self.task_budget {
            object.field("taskBudget", task_budget)?;
        }
        if let Some(output_schema) = &self.output_schema {
            object.field("outputSchema", output_schema)?;
        }
        if let Some(web_search) = &self.web_search {
            object.field("webSearch", web_search)?;
        }
        if let Some(thinking) = &self.thinking {
            object.field("thinking", thinking)?;
        }
        if let Some(effort) = &self.effort {
            object.field("effort", effort)?;
        }
        if let Some(verbosity) = &self.model_verbosity {
            object.field("modelVerbosity", verbosity)?;
        }
        if self.reasoning_summary != ReasoningSummary::Auto {
            object.field("reasoningSummary", &self.reasoning_summary)?;
        }
        if let Some(skip_cache_write) = &self.skip_cache_write {
            object.field("skipCacheWrite", skip_cache_write)?;
        }
        if let Some(cache_ttl) = &self.cache_ttl {
            object.field("cacheTtl", cache_ttl)?;
        }
        if let Some(cache_key) = &self.cache_key {
            object.field("cacheKey", cache_key)?;
        }
        if let Some(context_management) = &self.context_management {
            object.field("contextManagement", context_management)?;
        }
        if let Some(thinking_context) = &self.thinking_context {
            object.field("thinkingContext", thinking_context)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for ModelRequest {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                model: object.required("model", &[])?,
                supports_image_detail_original: false,
                image_limits: object.optional("imageLimits", &[])?,
                max_media_blocks: object.optional("maxMediaBlocks", &[])?,
                system: object.required("system", &[])?,
                messages: object.required("messages", &[])?,
                tools: object.required("tools", &[])?,
                tool_choice: object.optional("toolChoice", &[])?,
                max_output_tokens: object.required("maxOutputTokens", &[])?,
                task_budget: object.optional("taskBudget", &[])?,
                output_schema: object.optional("outputSchema", &[])?,
                web_search: object.optional("webSearch", &[])?,
                thinking: object.optional("thinking", &[])?,
                effort: object.optional("effort", &[])?,
                model_verbosity: object.optional("modelVerbosity", &[])?,
                reasoning_summary: object.defaulted("reasoningSummary", &[], Default::default)?,
                skip_cache_write: object.optional("skipCacheWrite", &[])?,
                cache_ttl: object.optional("cacheTtl", &[])?,
                cache_key: object.optional("cacheKey", &[])?,
                context_management: object.optional("contextManagement", &[])?,
                context_hints: None,
                thinking_context: object.optional("thinkingContext", &[])?,
            })
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelStopReason {
    EndTurn,
    /// The provider explicitly requested another sampling step.
    Continue,
    ToolUse,
    MaxTokens,
    StopSequence,
    Refusal,
}

impl RuntimeEncode for ModelStopReason {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::EndTurn => "end_turn",
            Self::Continue => "continue",
            Self::ToolUse => "tool_use",
            Self::MaxTokens => "max_tokens",
            Self::StopSequence => "stop_sequence",
            Self::Refusal => "refusal",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for ModelStopReason {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("end_turn", &[]),
                RuntimeEnumVariant::new("continue", &[]),
                RuntimeEnumVariant::new("tool_use", &[]),
                RuntimeEnumVariant::new("max_tokens", &[]),
                RuntimeEnumVariant::new("stop_sequence", &[]),
                RuntimeEnumVariant::new("refusal", &[]),
            ],
            &[
                Self::EndTurn,
                Self::Continue,
                Self::ToolUse,
                Self::MaxTokens,
                Self::StopSequence,
                Self::Refusal,
            ],
        )
    }
}

/// Normalized streaming event — every provider maps its wire format onto this.
#[derive(Debug, Clone, PartialEq)]
pub enum ModelStreamEvent {
    /// A provider accepted one complete text item; a later response error does not undo it.
    TextMessageCompleted {
        id: RuntimeString,
        phase: Option<RuntimeString>,
        text: RuntimeString,
    },
    /// Identity and intent of a provider-native assistant message.
    TextMessage {
        id: RuntimeString,
        phase: Option<RuntimeString>,
    },
    MessageStart {
        model: RuntimeString,
    },
    TextDelta {
        text: RuntimeString,
    },
    ThinkingDelta {
        text: RuntimeString,
    },
    /// A completed thinking block, delivered at its content_block_stop with the
    /// full accumulated text + an opaque replay token (`signature` for Anthropic
    /// signed-thinking, `encrypted_content` for OpenAI Responses reasoning).
    ThinkingBlock {
        text: RuntimeString,
        /// True only when the provider identifies this completed text as a
        /// reasoning summary. Raw thoughts and unknown sources remain false.
        is_summary: bool,
        signature: Option<RuntimeString>,
        encrypted_content: Option<RuntimeString>,
    },
    /// A completed redacted-thinking block (encrypted reasoning, no plaintext).
    RedactedThinkingBlock {
        data: RuntimeString,
    },
    /// A tool call begins. `thought_signature` (Gemini only) is the opaque
    /// signed reasoning attached to this functionCall part.
    ToolUseStart {
        id: RuntimeString,
        name: RuntimeString,
        thought_signature: Option<RuntimeString>,
        tool_kind: Option<ProviderToolCallKind>,
        input_format: Option<ToolInputFormat>,
    },
    ToolUseInputDelta {
        id: RuntimeString,
        partial_json: RuntimeString,
    },
    ToolUseStop {
        id: RuntimeString,
    },
    /// The model issued a provider-native web search.
    ServerSearchQuery {
        id: RuntimeString,
        query: RuntimeString,
        action: Option<WebSearchAction>,
    },
    /// Completed native search results in stream order.
    WebSearchResult {
        id: RuntimeString,
        results: Vec<NativeSearchResult>,
        error: Option<RuntimeString>,
    },
    /// A citation tying a span of the current assistant text to a source.
    Citation {
        citation: Citation,
    },
    /// Cumulative actual usage reported for the current provider request.
    /// Consumers reconcile repeated snapshots and the terminal usage by delta.
    UsageSnapshot {
        usage: Usage,
    },
    MessageStop {
        stop_reason: ModelStopReason,
        usage: Usage,
    },
    Error {
        error: ProviderError,
    },
    /// Request establishment failed and the next attempt is scheduled after
    /// `delay_ms`. Emitted DURING the backoff (not after it) so consumers can
    /// surface retry progress in real time; never carried in the assembled
    /// assistant message.
    RetryScheduled {
        /// 1-based index of the establishment attempt that just failed.
        attempt: u32,
        /// Total establishment attempts the policy allows, including the first.
        max_attempts: u32,
        /// Backoff before the next attempt, in milliseconds.
        delay_ms: u64,
        /// The classified error that triggered the retry.
        error: ProviderError,
        /// True when this retry is an unbounded network wait past the attempt
        /// cap: `max_attempts` no longer bounds it, so consumers should show a
        /// "waiting for network" status instead of a numbered retry.
        network_wait: bool,
    },
}

impl RuntimeEncode for ModelStreamEvent {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let (tag, fields) = match self {
            Self::TextMessageCompleted { id, phase, text } => {
                let mut fields = RuntimeObjectEncoder::new();
                fields.field("id", id)?;
                if let Some(phase) = phase {
                    fields.field("phase", phase)?;
                }
                fields.field("text", text)?;
                ("text_message_completed", fields)
            }
            Self::TextMessage { id, phase } => {
                let mut fields = RuntimeObjectEncoder::new();
                fields.field("id", id)?;
                if let Some(phase) = phase {
                    fields.field("phase", phase)?;
                }
                ("text_message", fields)
            }
            Self::MessageStart { model } => {
                let mut fields = RuntimeObjectEncoder::new();
                fields.field("model", model)?;
                ("message_start", fields)
            }
            Self::TextDelta { text } => {
                let mut fields = RuntimeObjectEncoder::new();
                fields.field("text", text)?;
                ("text_delta", fields)
            }
            Self::ThinkingDelta { text } => {
                let mut fields = RuntimeObjectEncoder::new();
                fields.field("text", text)?;
                ("thinking_delta", fields)
            }
            Self::ThinkingBlock {
                text,
                is_summary,
                signature,
                encrypted_content,
            } => {
                let mut fields = RuntimeObjectEncoder::new();
                fields.field("text", text)?;
                if let Some(signature) = signature {
                    fields.field("signature", signature)?;
                }
                if let Some(encrypted_content) = encrypted_content {
                    fields.field("encryptedContent", encrypted_content)?;
                }
                if *is_summary {
                    fields.field("isSummary", is_summary)?;
                }
                ("thinking_block", fields)
            }
            Self::RedactedThinkingBlock { data } => {
                let mut fields = RuntimeObjectEncoder::new();
                fields.field("data", data)?;
                ("redacted_thinking_block", fields)
            }
            Self::ToolUseStart {
                id,
                name,
                thought_signature,
                tool_kind,
                input_format,
            } => {
                let mut fields = RuntimeObjectEncoder::new();
                fields.field("id", id)?;
                fields.field("name", name)?;
                if let Some(thought_signature) = thought_signature {
                    fields.field("thoughtSignature", thought_signature)?;
                }
                if !ProviderToolCallKind::is_implicit(tool_kind) {
                    fields.field("toolKind", tool_kind)?;
                }
                if !ToolInputFormat::is_implicit(input_format) {
                    fields.field("inputFormat", input_format)?;
                }
                ("tool_use_start", fields)
            }
            Self::ToolUseInputDelta { id, partial_json } => {
                let mut fields = RuntimeObjectEncoder::new();
                fields.field("id", id)?;
                fields.field("partialJson", partial_json)?;
                ("tool_use_input_delta", fields)
            }
            Self::ToolUseStop { id } => {
                let mut fields = RuntimeObjectEncoder::new();
                fields.field("id", id)?;
                ("tool_use_stop", fields)
            }
            Self::ServerSearchQuery { id, query, action } => {
                let mut fields = RuntimeObjectEncoder::new();
                fields.field("id", id)?;
                fields.field("query", query)?;
                if let Some(action) = action {
                    fields.field("action", action)?;
                }
                ("server_search_query", fields)
            }
            Self::WebSearchResult { id, results, error } => {
                let mut fields = RuntimeObjectEncoder::new();
                fields.field("id", id)?;
                fields.field("results", results)?;
                if let Some(error) = error {
                    fields.field("error", error)?;
                }
                ("web_search_result", fields)
            }
            Self::Citation { citation } => {
                let mut fields = RuntimeObjectEncoder::new();
                fields.field("citation", citation)?;
                ("citation", fields)
            }
            Self::UsageSnapshot { usage } => {
                let mut fields = RuntimeObjectEncoder::new();
                fields.field("usage", usage)?;
                ("usage_snapshot", fields)
            }
            Self::MessageStop { stop_reason, usage } => {
                let mut fields = RuntimeObjectEncoder::new();
                fields.field("stopReason", stop_reason)?;
                fields.field("usage", usage)?;
                ("message_stop", fields)
            }
            Self::Error { error } => {
                let mut fields = RuntimeObjectEncoder::new();
                fields.field("error", error)?;
                ("error", fields)
            }
            Self::RetryScheduled {
                attempt,
                max_attempts,
                delay_ms,
                error,
                network_wait,
            } => {
                let mut fields = RuntimeObjectEncoder::new();
                fields.field("attempt", attempt)?;
                fields.field("maxAttempts", max_attempts)?;
                fields.field("delayMs", delay_ms)?;
                fields.field("error", error)?;
                fields.field("networkWait", network_wait)?;
                ("retry_scheduled", fields)
            }
        };
        runtime_codec::encode_internal(out, "type", &[], tag, &fields.finish())
    }
}

impl RuntimeDecode for ModelStreamEvent {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_internal(
            value,
            false,
            "type",
            &[],
            &[
                RuntimeEnumVariant::new("message_start", &[]),
                RuntimeEnumVariant::new("text_delta", &[]),
                RuntimeEnumVariant::new("thinking_delta", &[]),
                RuntimeEnumVariant::new("thinking_block", &[]),
                RuntimeEnumVariant::new("redacted_thinking_block", &[]),
                RuntimeEnumVariant::new("tool_use_start", &[]),
                RuntimeEnumVariant::new("tool_use_input_delta", &[]),
                RuntimeEnumVariant::new("tool_use_stop", &[]),
                RuntimeEnumVariant::new("server_search_query", &[]),
                RuntimeEnumVariant::new("web_search_result", &[]),
                RuntimeEnumVariant::new("citation", &[]),
                RuntimeEnumVariant::new("message_stop", &[]),
                RuntimeEnumVariant::new("error", &[]),
                RuntimeEnumVariant::new("retry_scheduled", &[]),
                RuntimeEnumVariant::new("text_message", &[]),
                RuntimeEnumVariant::new("usage_snapshot", &[]),
                RuntimeEnumVariant::new("text_message_completed", &[]),
            ],
            |variant, object| match variant {
                0 => Ok(Self::MessageStart {
                    model: object.required("model", &[])?,
                }),
                1 => Ok(Self::TextDelta {
                    text: object.required("text", &[])?,
                }),
                2 => Ok(Self::ThinkingDelta {
                    text: object.required("text", &[])?,
                }),
                3 => Ok(Self::ThinkingBlock {
                    text: object.required("text", &[])?,
                    is_summary: object.optional("isSummary", &[])?.unwrap_or(false),
                    signature: object.optional("signature", &[])?,
                    encrypted_content: object.optional("encryptedContent", &[])?,
                }),
                4 => Ok(Self::RedactedThinkingBlock {
                    data: object.required("data", &[])?,
                }),
                5 => Ok(Self::ToolUseStart {
                    id: object.required("id", &[])?,
                    name: object.required("name", &[])?,
                    thought_signature: object.optional("thoughtSignature", &[])?,
                    tool_kind: object.optional("toolKind", &[])?,
                    input_format: object.optional("inputFormat", &[])?,
                }),
                6 => Ok(Self::ToolUseInputDelta {
                    id: object.required("id", &[])?,
                    partial_json: object.required("partialJson", &[])?,
                }),
                7 => Ok(Self::ToolUseStop {
                    id: object.required("id", &[])?,
                }),
                8 => Ok(Self::ServerSearchQuery {
                    id: object.required("id", &[])?,
                    query: object.required("query", &[])?,
                    action: object.optional("action", &[])?,
                }),
                9 => Ok(Self::WebSearchResult {
                    id: object.required("id", &[])?,
                    results: object.required("results", &[])?,
                    error: object.optional("error", &[])?,
                }),
                10 => Ok(Self::Citation {
                    citation: object.required("citation", &[])?,
                }),
                11 => Ok(Self::MessageStop {
                    stop_reason: object.required("stopReason", &[])?,
                    usage: object.required("usage", &[])?,
                }),
                12 => Ok(Self::Error {
                    error: object.required("error", &[])?,
                }),
                13 => Ok(Self::RetryScheduled {
                    attempt: object.required("attempt", &[])?,
                    max_attempts: object.required("maxAttempts", &[])?,
                    delay_ms: object.required("delayMs", &[])?,
                    error: object.required("error", &[])?,
                    network_wait: object.optional("networkWait", &[])?.unwrap_or(false),
                }),
                14 => Ok(Self::TextMessage {
                    id: object.required("id", &[])?,
                    phase: object.optional("phase", &[])?,
                }),
                15 => Ok(Self::UsageSnapshot {
                    usage: object.required("usage", &[])?,
                }),
                16 => Ok(Self::TextMessageCompleted {
                    id: object.required("id", &[])?,
                    phase: object.optional("phase", &[])?,
                    text: object.required("text", &[])?,
                }),
                _ => Err(RuntimeCodecError::new(
                    RuntimeCodecErrorKind::UnknownEnumVariant,
                )),
            },
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderInfo {
    pub name: RuntimeString,
    pub base_url: Option<RuntimeString>,
    pub env_key: Option<RuntimeString>,
    pub default_model: RuntimeString,
}

impl RuntimeEncode for ProviderInfo {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("name", &self.name)?;
        if let Some(base_url) = &self.base_url {
            object.field("baseUrl", base_url)?;
        }
        if let Some(env_key) = &self.env_key {
            object.field("envKey", env_key)?;
        }
        object.field("defaultModel", &self.default_model)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for ProviderInfo {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                name: object.required("name", &[])?,
                base_url: object.optional("baseUrl", &[])?,
                env_key: object.optional("envKey", &[])?,
                default_model: object.required("defaultModel", &[])?,
            })
        })
    }
}

impl RuntimeEncode for NonZeroU32 {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        self.get().encode_runtime(out)
    }
}

impl RuntimeDecode for NonZeroU32 {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        let value = u32::decode_runtime(value)?;
        Self::new(value)
            .ok_or_else(|| RuntimeCodecError::new(RuntimeCodecErrorKind::IntegerOutOfRange))
    }
}

/// Conservative request-view count when no model metadata declares a ceiling.
pub const DEFAULT_MAX_MEDIA_BLOCKS: NonZeroU32 =
    NonZeroU32::new(100).expect("positive default media count");

/// Declared image constraints. Omission retains the model resolver's default.
/// The raw target remains absent until resolution so a changed base64 ceiling
/// can derive its own three-quarter raw-byte target.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ImageLimits {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_width: Option<NonZeroU32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_height: Option<NonZeroU32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_base64_size: Option<NonZeroU32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_raw_size: Option<NonZeroU32>,
}

impl ImageLimits {
    /// Merge declarations, without resolving any absent field prematurely.
    pub fn overlay(&mut self, overrides: &Self) {
        self.max_width = overrides.max_width.or(self.max_width);
        self.max_height = overrides.max_height.or(self.max_height);
        self.max_base64_size = overrides.max_base64_size.or(self.max_base64_size);
        self.target_raw_size = overrides.target_raw_size.or(self.target_raw_size);
    }
}

impl RuntimeEncode for ImageLimits {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        if let Some(value) = &self.max_width {
            object.field("maxWidth", value)?;
        }
        if let Some(value) = &self.max_height {
            object.field("maxHeight", value)?;
        }
        if let Some(value) = &self.max_base64_size {
            object.field("maxBase64Size", value)?;
        }
        if let Some(value) = &self.target_raw_size {
            object.field("targetRawSize", value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for ImageLimits {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, true, |object| {
            Ok(Self {
                max_width: object.optional("maxWidth", &[])?,
                max_height: object.optional("maxHeight", &[])?,
                max_base64_size: object.optional("maxBase64Size", &[])?,
                target_raw_size: object.optional("targetRawSize", &[])?,
            })
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProviderCapabilities {
    /// The selected model accepts the native original-resolution image hint.
    #[serde(default, skip_serializing_if = "original_image_detail_disabled")]
    pub supports_image_detail_original: bool,
    pub streaming: bool,
    pub tools: bool,
    pub thinking: bool,
    /// Native structured output (provider-enforced JSON schema).
    pub structured_output: bool,
    /// Provider can satisfy a NATIVE server-side web search for THIS model.
    pub web_search: bool,
    /// Honors REQUEST-SIDE prompt-cache control (Anthropic's `cache_control`
    /// breakpoints). False for providers whose caching is automatic.
    pub prompt_cache_control: bool,
    /// Effective context window (tokens) for the active model.
    pub context_window: u64,
    /// Cap on output tokens for the active model.
    pub max_output_tokens: u64,
    /// The model can ingest a PDF NATIVELY.
    pub native_pdf: bool,
    /// The model can ingest IMAGES (vision input).
    pub vision: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image_limits: Option<ImageLimits>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_media_blocks: Option<NonZeroU32>,
}

fn original_image_detail_disabled(value: &bool) -> bool {
    !*value
}

impl RuntimeEncode for ProviderCapabilities {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let Self {
            supports_image_detail_original,
            streaming,
            tools,
            thinking,
            structured_output,
            web_search,
            prompt_cache_control,
            context_window,
            max_output_tokens,
            native_pdf,
            vision,
            image_limits,
            max_media_blocks,
        } = self;
        let mut object = RuntimeObjectEncoder::new();
        if *supports_image_detail_original {
            object.field(
                "supportsImageDetailOriginal",
                supports_image_detail_original,
            )?;
        }
        object.field("streaming", streaming)?;
        object.field("tools", tools)?;
        object.field("thinking", thinking)?;
        object.field("structuredOutput", structured_output)?;
        object.field("webSearch", web_search)?;
        object.field("promptCacheControl", prompt_cache_control)?;
        object.field("contextWindow", context_window)?;
        object.field("maxOutputTokens", max_output_tokens)?;
        object.field("nativePdf", native_pdf)?;
        object.field("vision", vision)?;
        if let Some(value) = image_limits {
            object.field("imageLimits", value)?;
        }
        if let Some(value) = max_media_blocks {
            object.field("maxMediaBlocks", value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for ProviderCapabilities {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                supports_image_detail_original: object
                    .optional("supportsImageDetailOriginal", &[])?
                    .unwrap_or(false),
                streaming: object.required("streaming", &[])?,
                tools: object.required("tools", &[])?,
                thinking: object.required("thinking", &[])?,
                structured_output: object.required("structuredOutput", &[])?,
                web_search: object.required("webSearch", &[])?,
                prompt_cache_control: object.required("promptCacheControl", &[])?,
                context_window: object.required("contextWindow", &[])?,
                max_output_tokens: object.required("maxOutputTokens", &[])?,
                native_pdf: object.required("nativePdf", &[])?,
                vision: object.required("vision", &[])?,
                image_limits: object.optional("imageLimits", &[])?,
                max_media_blocks: object.optional("maxMediaBlocks", &[])?,
            })
        })
    }
}

/// The reasoning-effort ladder a model supports, supplied by the client. An
/// absent [`ModelCapabilities::effort`] means the model has no effort knob at
/// all; an empty `levels` here means the same.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EffortCapability {
    /// Supported provider levels. Ultra is a local orchestration selection. Order is irrelevant;
    /// clamping uses the enum ordering.
    pub levels: Vec<EffortLevel>,
    /// Preferred level when the request does not pin one.
    pub default: Option<EffortLevel>,
}

impl EffortCapability {
    /// The lowest provider level this model actually declares. Local Ultra is
    /// not a provider rung and is never selected. `None` when the ladder is
    /// empty (no effort knob).
    pub fn lowest(&self) -> Option<EffortLevel> {
        self.levels
            .iter()
            .copied()
            .filter(|level| !level.is_ultra())
            .min()
    }

    /// Clamp a requested level to the supported set: the highest supported level
    /// not above the request, else the lowest supported level. `None` when the
    /// ladder is empty (no effort knob).
    pub fn clamp(&self, requested: EffortLevel) -> Option<EffortLevel> {
        // Ultra never belongs on a provider wire. Prefer xhigh and descend;
        // a max-only endpoint may still use its sole concrete supported level.
        let requested = if requested.is_ultra() {
            EffortLevel::Xhigh
        } else {
            requested
        };
        let levels = self
            .levels
            .iter()
            .copied()
            .filter(|level| !level.is_ultra());
        if levels.clone().any(|level| level == requested) {
            return Some(requested);
        }
        levels
            .clone()
            .filter(|level| *level <= requested)
            .max()
            .or_else(|| levels.min())
    }
}

/// Extended-thinking support a model advertises.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ThinkingCapability {
    /// Model-sized adaptive thinking (no fixed budget).
    pub adaptive: bool,
    /// Honors the interleaved-thinking beta (thinking between tool calls).
    pub interleaved: bool,
    /// Whether the model accepts an explicit "thinking disabled" request.
    /// Defaults to true for sources that do not declare it; set false when a
    /// model rejects the disabled shape so callers can fall back to the lowest
    /// available reasoning rung.
    pub can_disable_thinking: bool,
}

/// Native output detail for a model that explicitly supports this control.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OutputVerbosity {
    Low,
    Medium,
    High,
}

impl OutputVerbosity {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "low" => Some(Self::Low),
            "medium" => Some(Self::Medium),
            "high" => Some(Self::High),
            _ => None,
        }
    }
}

impl RuntimeEncode for OutputVerbosity {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        runtime_codec::encode_external_unit(out, self.as_str())
    }
}

impl RuntimeDecode for OutputVerbosity {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("low", &[]),
                RuntimeEnumVariant::new("medium", &[]),
                RuntimeEnumVariant::new("high", &[]),
            ],
            &[Self::Low, Self::Medium, Self::High],
        )
    }
}

/// Requested public reasoning summary. None suppresses the summary while
/// retaining encrypted reasoning for stateless continuation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ReasoningSummary {
    #[default]
    Auto,
    Concise,
    Detailed,
    None,
}

impl ReasoningSummary {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Concise => "concise",
            Self::Detailed => "detailed",
            Self::None => "none",
        }
    }
}

impl RuntimeEncode for ReasoningSummary {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        runtime_codec::encode_external_unit(out, self.as_str())
    }
}

impl RuntimeDecode for ReasoningSummary {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("auto", &[]),
                RuntimeEnumVariant::new("concise", &[]),
                RuntimeEnumVariant::new("detailed", &[]),
                RuntimeEnumVariant::new("none", &[]),
            ],
            &[Self::Auto, Self::Concise, Self::Detailed, Self::None],
        )
    }
}

/// The per-model capability set the CLIENT declares at session init. The engine
/// never infers these from the model id: a provider reads exactly what was
/// declared, and every absent/false field means "not supported". This is what
/// lets each model source own its own model system without the engine keeping a
/// model-id table.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ModelCapabilities {
    /// Tool/function calling. None keeps the provider's backward-compatible
    /// default; false explicitly keeps a plain-chat model tool-free.
    pub tools: Option<bool>,
    /// Reasoning effort ladder. `None` = no effort knob.
    pub effort: Option<EffortCapability>,
    /// Supported native output detail. Absent means undeclared or unsupported;
    /// a declared value is independent of reasoning effort and token budgets.
    pub output_verbosity: Option<OutputVerbosity>,
    /// Model-owned approval behavior and the dedicated review target.
    pub auto_review_model_override: Option<RuntimeString>,
    pub requires_sandboxed_review: bool,
    pub node_repl_auto_review_required: bool,
    pub guardian: Option<RuntimeJson>,
    pub guardian_v2: Option<RuntimeJson>,
    pub model_messages: Option<RuntimeJson>,
    pub comp_hash: Option<RuntimeString>,
    pub supports_reasoning_summary_parameter: Option<bool>,
    pub default_reasoning_summary: Option<ReasoningSummary>,
    /// Complete source effort vocabulary for internal review requests.
    pub supported_reasoning_efforts: Vec<RuntimeString>,
    pub default_reasoning_effort: Option<RuntimeString>,
    /// Preferred effort for multi-agent work, subject to the model's supported efforts.
    pub multi_agent_reasoning_effort: Option<RuntimeString>,
    pub effective_context_window_percent: Option<i64>,
    pub auto_compact_token_limit: Option<u64>,
    pub used_fallback_model_metadata: bool,
    pub model_specialty: Option<RuntimeString>,
    pub supports_image_detail_original: bool,
    /// Extended thinking. `None` = the model does not think.
    pub thinking: Option<ThinkingCapability>,
    /// Provider-native structured output (grammar-constrained JSON) vs the
    /// forced-tool emulation the engine can always fall back to.
    pub structured_output: bool,
    /// Provider-native server-side web search.
    pub web_search: bool,
    /// Honors request-side prompt-cache breakpoints (Anthropic `cache_control`).
    pub prompt_cache_control: bool,
    /// The model ingests PDF natively.
    pub native_pdf: bool,
    /// The model ingests images (vision input).
    pub vision: bool,
    /// Anthropic 1M-token context beta.
    pub one_million_context: bool,
    /// Anthropic `output_config.task_budget`.
    pub task_budgets: bool,
    /// The exact model supports the provider's separately billed Fast service.
    pub fast_mode: bool,
    /// Effective context window (tokens). `None` = the provider's own default.
    pub context_window: Option<u64>,
    /// Cap on output tokens. `None` = the provider's own default.
    pub max_output_tokens: Option<u64>,
    pub image_limits: Option<ImageLimits>,
    pub max_media_blocks: Option<NonZeroU32>,
    /// Exact native audio/video MIME types accepted as model input. Empty
    /// means unsupported; this does not declare media generation capabilities.
    pub media_input_mime_types: Vec<RuntimeString>,
}

impl ModelCapabilities {
    /// The output budget a request may actually ask for.
    ///
    /// A declared ceiling is the API's own limit, so a bigger budget is a refused
    /// request rather than a longer answer — and the turn's budget climbs on its
    /// own (the max-token recovery ladder doubles it on truncation, and the host
    /// can raise the session default). An undeclared ceiling clamps nothing: the
    /// host said nothing about this model, so nothing is assumed for it.
    pub fn clamp_output_tokens(&self, requested: u64) -> u64 {
        match self.max_output_tokens {
            Some(ceiling) => requested.min(ceiling),
            None => requested,
        }
    }
}

impl RuntimeEncode for EffortCapability {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        if !self.levels.is_empty() {
            object.field("levels", &self.levels)?;
        }
        if let Some(value) = &self.default {
            object.field("default", value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for EffortCapability {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, true, |object| {
            Ok(Self {
                levels: object.defaulted("levels", &[], Vec::new)?,
                default: object.optional("default", &[])?,
            })
        })
    }
}

impl RuntimeEncode for ThinkingCapability {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        if self.adaptive {
            object.field("adaptive", &self.adaptive)?;
        }
        if self.interleaved {
            object.field("interleaved", &self.interleaved)?;
        }
        if !self.can_disable_thinking {
            object.field("canDisableThinking", &self.can_disable_thinking)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for ThinkingCapability {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, true, |object| {
            Ok(Self {
                adaptive: object.defaulted("adaptive", &[], || false)?,
                interleaved: object.defaulted("interleaved", &[], || false)?,
                can_disable_thinking: object.defaulted(
                    "canDisableThinking",
                    &["can_disable_thinking"],
                    || false,
                )?,
            })
        })
    }
}

impl RuntimeEncode for ModelCapabilities {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        if let Some(value) = &self.tools {
            object.field("tools", value)?;
        }
        if let Some(value) = &self.effort {
            object.field("effort", value)?;
        }
        if let Some(value) = &self.output_verbosity {
            object.field("outputVerbosity", value)?;
        }
        if let Some(value) = &self.auto_review_model_override {
            object.field("autoReviewModelOverride", value)?;
        }
        if self.requires_sandboxed_review {
            object.field("requiresSandboxedReview", &true)?;
        }
        if self.node_repl_auto_review_required {
            object.field("nodeReplAutoReviewRequired", &true)?;
        }
        if let Some(value) = &self.guardian {
            object.field("guardian", value)?;
        }
        if let Some(value) = &self.guardian_v2 {
            object.field("guardianV2", value)?;
        }
        if let Some(value) = &self.model_messages {
            object.field("modelMessages", value)?;
        }
        if let Some(value) = &self.comp_hash {
            object.field("compHash", value)?;
        }
        if let Some(value) = &self.supports_reasoning_summary_parameter {
            object.field("supportsReasoningSummaryParameter", value)?;
        }
        if let Some(value) = &self.default_reasoning_summary {
            object.field("defaultReasoningSummary", value)?;
        }
        if !self.supported_reasoning_efforts.is_empty() {
            object.field(
                "supportedReasoningEfforts",
                &self.supported_reasoning_efforts,
            )?;
        }
        if let Some(value) = &self.default_reasoning_effort {
            object.field("defaultReasoningEffort", value)?;
        }
        if let Some(value) = &self.multi_agent_reasoning_effort {
            object.field("multiAgentReasoningEffort", value)?;
        }
        if let Some(value) = &self.auto_compact_token_limit {
            object.field("autoCompactTokenLimit", value)?;
        }
        if let Some(value) = &self.effective_context_window_percent {
            object.field("effectiveContextWindowPercent", value)?;
        }
        if self.used_fallback_model_metadata {
            object.field("usedFallbackModelMetadata", &true)?;
        }
        if let Some(value) = &self.model_specialty {
            object.field("modelSpecialty", value)?;
        }
        if self.supports_image_detail_original {
            object.field("supportsImageDetailOriginal", &true)?;
        }

        if let Some(value) = &self.thinking {
            object.field("thinking", value)?;
        }
        for (name, value) in [
            ("structuredOutput", self.structured_output),
            ("webSearch", self.web_search),
            ("promptCacheControl", self.prompt_cache_control),
            ("nativePdf", self.native_pdf),
            ("vision", self.vision),
            ("oneMillionContext", self.one_million_context),
            ("taskBudgets", self.task_budgets),
            ("fastMode", self.fast_mode),
        ] {
            if value {
                object.field(name, &value)?;
            }
        }
        if let Some(value) = &self.context_window {
            object.field("contextWindow", value)?;
        }
        if let Some(value) = &self.max_output_tokens {
            object.field("maxOutputTokens", value)?;
        }
        if let Some(value) = &self.image_limits {
            object.field("imageLimits", value)?;
        }
        if let Some(value) = &self.max_media_blocks {
            object.field("maxMediaBlocks", value)?;
        }
        if !self.media_input_mime_types.is_empty() {
            object.field("mediaInputMimeTypes", &self.media_input_mime_types)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for ModelCapabilities {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                tools: object.optional("tools", &[])?,
                effort: object.optional("effort", &[])?,
                output_verbosity: object.optional("outputVerbosity", &[])?,
                auto_review_model_override: object
                    .optional("autoReviewModelOverride", &["auto_review_model_override"])?,
                requires_sandboxed_review: object.defaulted(
                    "requiresSandboxedReview",
                    &["requires_sandboxed_review"],
                    || false,
                )?,
                node_repl_auto_review_required: object.defaulted(
                    "nodeReplAutoReviewRequired",
                    &["node_repl_auto_review_required"],
                    || false,
                )?,
                guardian: object.optional("guardian", &[])?,
                guardian_v2: object.optional("guardianV2", &["guardian_v2"])?,
                model_messages: object.optional("modelMessages", &["model_messages"])?,
                comp_hash: object.optional("compHash", &["comp_hash"])?,
                supports_reasoning_summary_parameter: object.optional(
                    "supportsReasoningSummaryParameter",
                    &["supports_reasoning_summary_parameter"],
                )?,
                default_reasoning_summary: object
                    .optional("defaultReasoningSummary", &["default_reasoning_summary"])?,
                supported_reasoning_efforts: object.defaulted(
                    "supportedReasoningEfforts",
                    &["supported_reasoning_efforts"],
                    Vec::new,
                )?,
                default_reasoning_effort: object
                    .optional("defaultReasoningEffort", &["default_reasoning_effort"])?,
                multi_agent_reasoning_effort: object.optional(
                    "multiAgentReasoningEffort",
                    &["multi_agent_reasoning_effort"],
                )?,
                auto_compact_token_limit: object
                    .optional("autoCompactTokenLimit", &["auto_compact_token_limit"])?,
                effective_context_window_percent: object.optional(
                    "effectiveContextWindowPercent",
                    &["effective_context_window_percent"],
                )?,
                used_fallback_model_metadata: object.defaulted(
                    "usedFallbackModelMetadata",
                    &["used_fallback_model_metadata"],
                    || false,
                )?,
                model_specialty: object.optional("modelSpecialty", &["model_specialty"])?,
                supports_image_detail_original: object.defaulted(
                    "supportsImageDetailOriginal",
                    &["supports_image_detail_original"],
                    || false,
                )?,

                thinking: object.optional("thinking", &[])?,
                structured_output: object.defaulted("structuredOutput", &[], || false)?,
                web_search: object.defaulted("webSearch", &[], || false)?,
                prompt_cache_control: object.defaulted("promptCacheControl", &[], || false)?,
                native_pdf: object.defaulted("nativePdf", &[], || false)?,
                vision: object.defaulted("vision", &[], || false)?,
                one_million_context: object.defaulted("oneMillionContext", &[], || false)?,
                task_budgets: object.defaulted("taskBudgets", &[], || false)?,
                fast_mode: object.defaulted("fastMode", &[], || false)?,
                context_window: object.optional("contextWindow", &[])?,
                max_output_tokens: object.optional("maxOutputTokens", &[])?,
                image_limits: object.optional("imageLimits", &[])?,
                max_media_blocks: object.optional("maxMediaBlocks", &[])?,
                media_input_mime_types: object.defaulted("mediaInputMimeTypes", &[], Vec::new)?,
            })
        })
    }
}

pub type AuthHeaders = HashMap<String, String>;

/// A normalized provider stream — an async stream of `ModelStreamEvent` values.
pub type ModelStream = Pin<Box<dyn Stream<Item = ModelStreamEvent> + Send>>;

/// A transport declaration, independent of the selected model's name.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum RemoteCompactionSupport {
    #[default]
    Unsupported,
    ResponsesV1,
    ResponsesV2,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RemoteCompactionResult {
    pub checkpoint: crate::schemas::message::NativeCompactionCheckpoint,
    pub usage: Option<crate::schemas::message::Usage>,
}

#[async_trait::async_trait]
pub trait ModelProvider: Send + Sync {
    fn info(&self) -> &ProviderInfo;
    fn remote_compaction_support(&self, _model: &RuntimeString) -> RemoteCompactionSupport {
        RemoteCompactionSupport::Unsupported
    }
    fn effective_context_window_percent(&self, model: &RuntimeString) -> u8 {
        self.model_metadata(model)
            .effective_context_window_percent
            .unwrap_or(95)
            .clamp(1, 100) as u8
    }
    fn model_auto_compact_token_limit(&self, model: &RuntimeString) -> Option<u64> {
        self.model_metadata(model).auto_compact_token_limit
    }
    async fn native_checkpoint_compatibility(
        &self,
        _checkpoint: &crate::schemas::message::NativeCompactionCheckpoint,
        _model: &RuntimeString,
        _signal: &AbortSignal,
    ) -> Result<bool, ProviderError> {
        Ok(false)
    }
    fn compaction_compatibility_hash(&self, model: &RuntimeString) -> Option<RuntimeString> {
        self.model_metadata(model).comp_hash
    }
    /// A provider-specific stream retry limit, expressed as retries after the first request.
    fn compaction_stream_max_retries(&self) -> Option<u32> {
        None
    }
    fn supports_compaction_model_fallback(&self) -> bool {
        false
    }

    async fn compact_remote(
        &self,
        _request: ModelRequest,
        _signal: AbortSignal,
    ) -> Result<RemoteCompactionResult, ProviderError> {
        Err(ProviderError {
            kind: crate::types::errors::ProviderErrorKind::InvalidRequest,
            message: "This provider does not support native context compaction.".into(),
            status: None,
            code: Some("native_compaction_unsupported".into()),
            error_type: None,
            request_id: None,
            media_origins: None,
            media_repair_complete: false,
            retry_after_ms: None,
        })
    }
    async fn compact_remote_with_idle_timeout(
        &self,
        request: ModelRequest,
        signal: AbortSignal,
        _idle_ms: u64,
    ) -> Result<RemoteCompactionResult, ProviderError> {
        self.compact_remote(request, signal).await
    }
    /// Observe account ownership independently from token refresh revisions.
    fn classifier_auth_changes(
        &self,
    ) -> Option<tokio::sync::watch::Receiver<crate::provider::auth::CredentialChangeState>> {
        None
    }
    /// Create a runtime-scoped transport for background classifications.
    fn create_approval_classifier_transport(
        &self,
        _attribution: crate::provider::approval_classifier_transport::ClassifierAttribution,
        _target: Option<crate::provider::approval_classifier_transport::ClassifierTarget>,
    ) -> Option<Arc<dyn crate::provider::approval_classifier_transport::ApprovalClassifierTransport>>
    {
        None
    }
    fn capabilities(&self, model: &RuntimeString) -> ProviderCapabilities;
    /// Opt in to fitting active tool declarations to this model's input budget.
    /// Other providers retain their direct inventory regardless of local estimates.
    fn uses_model_tool_budget(&self, _model: &RuntimeString) -> bool {
        false
    }
    /// Model-owned metadata, distinct from transport runtime capabilities.
    fn model_metadata(&self, _model: &RuntimeString) -> ModelCapabilities {
        ModelCapabilities::default()
    }
    /// Full locally available catalog, including models hidden from pickers.
    /// This read must never perform network I/O.
    fn offline_model_catalog(&self) -> Vec<(RuntimeString, ModelCapabilities)> {
        Vec::new()
    }
    fn approval_review_preferred_model(&self) -> RuntimeString {
        "codex-auto-review".into()
    }
    /// Whether the authenticated transport uses the standard OpenAI API catalog.
    fn uses_official_api_catalog(&self) -> bool {
        false
    }
    fn managed_review_cloud_auth_source(
        &self,
    ) -> Option<Arc<dyn crate::engine::approval_requirements_cloud::ManagedReviewCloudAuthSource>>
    {
        None
    }

    /// The original transport used only to create a separately admitted review target.
    /// Ordinary request dispatch continues to reject unbound model identifiers.
    fn review_target_provider(&self) -> Option<Arc<dyn ModelProvider>> {
        None
    }
    /// Send an explicitly captured target with the selected catalog's metadata.
    fn stream_with_model_metadata(
        &self,
        req: ModelRequest,
        metadata: ModelCapabilities,
        raw_effort: Option<RuntimeString>,
        _total_attempts: u32,
        signal: AbortSignal,
    ) -> ModelStream {
        let mut req = req;
        req.max_output_tokens = metadata.clamp_output_tokens(req.max_output_tokens);
        if let Err(error) =
            crate::provider::review_target::apply_typed_review_effort(&mut req, raw_effort.as_ref())
        {
            return Box::pin(futures::stream::once(async move {
                ModelStreamEvent::Error { error }
            }));
        }
        self.stream(req, signal)
    }
    fn approval_request_tokens(&self, request: &ModelRequest) -> Result<usize, RuntimeString> {
        crate::engine::session::estimate_approval_request_tokens(request)
    }
    fn approval_request_tokens_with_metadata(
        &self,
        request: &ModelRequest,
        _metadata: ModelCapabilities,
        _raw_effort: Option<RuntimeString>,
    ) -> Result<usize, RuntimeString> {
        self.approval_request_tokens(request)
    }

    /// Capture effective capabilities at a work boundary. Decorators backed by
    /// mutable state can override this to resolve the whole set under one lock.
    fn capture_capabilities(
        &self,
        models: &[RuntimeString],
    ) -> HashMap<RuntimeString, ProviderCapabilities> {
        models
            .iter()
            .map(|model| (model.clone(), self.capabilities(model)))
            .collect()
    }
    /// A captured provider must not consult live state for an unbound model.
    fn has_model_capabilities(&self, _model: &RuntimeString) -> bool {
        true
    }
    fn stream(&self, req: ModelRequest, signal: AbortSignal) -> ModelStream;
    /// Pre-turn token estimate (engine supplies a chars/4 fallback if absent —
    /// modeled as `None`).
    fn estimate_tokens(&self, _req: &ModelRequest) -> Option<u64> {
        None
    }
    /// Refresh credentials after an auth error (§0.5). `None` ⇒ the provider has
    /// no refresh path; `Some(Err(_))` ⇒ refresh
    /// failed.
    async fn resolve_auth(&self) -> Option<Result<AuthHeaders, ProviderError>> {
        None
    }
    /// The account id the provider's credential queue is currently sending,
    /// when the provider carries one. Captured before a request so a quota
    /// refusal can name the account that failed.
    fn credential_account_id(&self) -> Option<String> {
        None
    }
    /// Advance the provider's multi-account credential queue after a quota
    /// refusal. `failed_id` is the account the failed request authenticated
    /// with; a provider whose queue already moved past it reports
    /// [`crate::provider::rotation::RotateOutcome::AlreadyAdvanced`] so the
    /// caller retries without burning a cycle step. Providers without a queue
    /// return `None`.
    async fn rotate_credential(
        &self,
        _failed_id: &str,
    ) -> Option<crate::provider::rotation::RotateOutcome> {
        None
    }
    /// Whether the provider talks to HTTPS endpoints through its own HTTP
    /// client and therefore benefits from transport prewarm (DNS + TCP + TLS
    /// handshake done ahead of the first request). Test doubles and
    /// in-process providers stay `false`.
    fn has_prewarm_endpoints(&self) -> bool {
        false
    }
    /// Probe every resolved endpoint through the provider's own client so the
    /// first real request reuses a warm keep-alive connection. One outcome
    /// per endpoint; failures degrade silently (never fail the caller).
    fn prewarm_transport(
        &self,
    ) -> Pin<
        Box<dyn std::future::Future<Output = Vec<crate::provider::prewarm::PrewarmOutcome>> + Send>,
    > {
        Box::pin(async { Vec::new() })
    }
}

/// The model-call seam — decoupled from the provider object so tests can inject.
pub type CallModel = Box<dyn Fn(ModelRequest, AbortSignal) -> ModelStream + Send + Sync>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_values::runtime_json;
    use crate::schemas::content::ContentBlock;
    use crate::schemas::message::Message;

    fn parse(wire: &str) -> RuntimeJson {
        runtime_json::parse(&RuntimeString::from(wire)).expect("valid exact JSON fixture")
    }

    #[test]
    fn image_declarations_merge_each_field_without_pinning_a_raw_target() {
        let mut base = ImageLimits {
            max_width: NonZeroU32::new(2_000),
            max_height: NonZeroU32::new(2_000),
            max_base64_size: NonZeroU32::new(5_242_880),
            target_raw_size: None,
        };
        base.overlay(&ImageLimits {
            max_width: NonZeroU32::new(1_200),
            max_base64_size: NonZeroU32::new(10_485_760),
            ..Default::default()
        });
        assert_eq!(base.max_width.unwrap().get(), 1_200);
        assert_eq!(base.max_height.unwrap().get(), 2_000);
        assert_eq!(base.max_base64_size.unwrap().get(), 10_485_760);
        assert_eq!(base.target_raw_size, None);
        base.target_raw_size = NonZeroU32::new(512_000);
        base.overlay(&ImageLimits {
            max_base64_size: NonZeroU32::new(20_000_000),
            ..Default::default()
        });
        assert_eq!(base.target_raw_size.unwrap().get(), 512_000);
    }

    #[test]
    fn image_detail_capability_binding_is_native_and_never_restored_from_input() {
        let request_json = crate::runtime_values::runtime_json::parse(&RuntimeString::from(r#"{"model":"review","system":[],"messages":[],"tools":[],"maxOutputTokens":1024,"supportsImageDetailOriginal":true}"#)).unwrap();
        let mut request: ModelRequest = runtime_codec::decode_value(&request_json).unwrap();
        assert!(!request.supports_image_detail_original);
        let mut capabilities: ProviderCapabilities = runtime_codec::decode_text(&RuntimeString::from(r#"{"streaming":true,"tools":true,"thinking":false,"structuredOutput":false,"webSearch":false,"promptCacheControl":false,"contextWindow":128000,"maxOutputTokens":1024,"nativePdf":false,"vision":true}"#)).unwrap();
        assert!(!capabilities.supports_image_detail_original);
        capabilities.supports_image_detail_original = true;
        request.bind_media_capabilities(&capabilities);
        assert!(request.supports_image_detail_original);
        assert!(runtime_codec::encode_value(&request)
            .unwrap()
            .get("supportsImageDetailOriginal")
            .is_none());
    }

    #[test]
    fn serde_image_limits_omit_absence_and_reject_invalid_declarations() {
        assert_eq!(
            serde_json::to_value(ImageLimits::default()).unwrap(),
            serde_json::json!({})
        );
        for field in ["maxWidth", "maxHeight", "maxBase64Size", "targetRawSize"] {
            for value in [
                serde_json::json!(0),
                serde_json::json!(-1),
                serde_json::json!(1.5),
            ] {
                let mut wire = serde_json::Map::new();
                wire.insert(field.into(), value);
                assert!(serde_json::from_value::<ImageLimits>(wire.into()).is_err());
            }
        }
        assert!(
            serde_json::from_value::<ImageLimits>(serde_json::json!({"maxWidht": 2_000})).is_err()
        );
    }

    #[test]
    fn exact_media_codecs_omit_absence_and_reject_zero_and_unknown_fields() {
        assert_canonical::<ImageLimits>("{}", "{}");
        assert_canonical::<ModelCapabilities>("{}", "{}");
        assert_canonical::<ModelCapabilities>(
            r#"{"imageLimits":{"maxBase64Size":10485760},"maxMediaBlocks":600}"#,
            r#"{"imageLimits":{"maxBase64Size":10485760},"maxMediaBlocks":600}"#,
        );
        for field in ["maxWidth", "maxHeight", "maxBase64Size", "targetRawSize"] {
            for value in ["0", "-1", "1.5", "4294967296"] {
                let wire = format!(r#"{{"{field}":{value}}}"#);
                assert!(runtime_codec::decode_value::<ImageLimits>(&parse(&wire)).is_err());
            }
        }
        for wire in [
            r#"{"imageLimits":{"maxWidth":0}}"#,
            r#"{"maxMediaBlocks":0}"#,
            r#"{"imageLimits":{"maxWidht":2000}}"#,
        ] {
            assert!(runtime_codec::decode_value::<ModelCapabilities>(&parse(wire)).is_err());
        }
        assert_canonical::<ModelCapabilities>(
            r#"{"unknown":true,"maxMediaBlocks":100}"#,
            r#"{"maxMediaBlocks":100}"#,
        );
    }

    #[test]
    fn multi_agent_effort_metadata_round_trips_without_inventing_a_default() {
        for wire in ["{}", r#"{"multiAgentReasoningEffort":null}"#] {
            let capabilities = decode::<ModelCapabilities>(wire);
            assert!(capabilities.multi_agent_reasoning_effort.is_none());
            assert_eq!(encoded(&capabilities), parse("{}"));
        }
        for wire in [
            r#"{"multiAgentReasoningEffort":"custom/Exact"}"#,
            r#"{"multi_agent_reasoning_effort":"custom/Exact"}"#,
        ] {
            let capabilities = decode::<ModelCapabilities>(wire);
            assert_eq!(
                capabilities.multi_agent_reasoning_effort,
                Some("custom/Exact".into())
            );
            assert_eq!(
                encoded(&capabilities),
                parse(r#"{"multiAgentReasoningEffort":"custom/Exact"}"#)
            );
        }
        assert!(runtime_codec::decode_value::<ModelCapabilities>(&parse(
            r#"{"multiAgentReasoningEffort":42}"#
        ))
        .is_err());
    }

    #[test]
    fn request_media_binding_replaces_the_previous_model_and_pins_default_absence() {
        let mut request = decode::<ModelRequest>(
            r#"{"model":"fixture","system":[],"messages":[],"tools":[],"maxOutputTokens":100,"imageLimits":{"maxWidth":3000},"maxMediaBlocks":600}"#,
        );
        let capabilities = decode::<ProviderCapabilities>(
            r#"{"streaming":true,"tools":true,"thinking":false,"structuredOutput":false,"webSearch":false,"promptCacheControl":false,"contextWindow":200000,"maxOutputTokens":100,"nativePdf":true,"vision":true}"#,
        );
        request.bind_media_capabilities(&capabilities);
        assert_eq!(request.image_limits, Some(ImageLimits::default()));
        assert_eq!(request.max_media_blocks.unwrap().get(), 100);
        let wire = encoded(&request);
        assert_eq!(
            wire.as_object().unwrap().get("imageLimits"),
            Some(&parse("{}"))
        );
        assert!(encoded(&capabilities)
            .as_object()
            .unwrap()
            .get("imageLimits")
            .is_none());
    }

    /// The declared ceiling is what the API accepts, so a larger budget is held
    /// down rather than sent and refused; an undeclared one assumes nothing.
    #[test]
    fn a_declared_output_ceiling_holds_a_request_budget_down() {
        let declared = ModelCapabilities {
            max_output_tokens: Some(64_000),
            ..Default::default()
        };
        assert_eq!(declared.clamp_output_tokens(128_000), 64_000);
        assert_eq!(declared.clamp_output_tokens(8_192), 8_192);
        assert_eq!(
            ModelCapabilities::default().clamp_output_tokens(128_000),
            128_000
        );
    }

    fn decode<T: RuntimeDecode>(wire: &str) -> T {
        runtime_codec::decode_value(&parse(wire)).expect("fixture decodes")
    }

    fn encoded<T: RuntimeEncode>(value: &T) -> RuntimeJson {
        runtime_codec::encode_value(value).expect("value encodes")
    }

    fn assert_canonical<T: RuntimeDecode + RuntimeEncode>(input: &str, expected: &str) {
        assert_eq!(encoded(&decode::<T>(input)), parse(expected));
    }

    #[test]
    fn retry_scheduled_stream_events_round_trip() {
        assert_canonical::<ModelStreamEvent>(
            r#"{"type":"retry_scheduled","attempt":1,"maxAttempts":3,"delayMs":1200,"error":{"kind":"overloaded","message":"upstream capacity","status":529,"retryAfterMs":1200},"networkWait":false}"#,
            r#"{"type":"retry_scheduled","attempt":1,"maxAttempts":3,"delayMs":1200,"error":{"kind":"overloaded","message":"upstream capacity","status":529,"retryAfterMs":1200},"networkWait":false}"#,
        );
    }

    #[test]
    fn function_kind_and_json_input_format_stay_implicit() {
        assert_canonical::<ToolSpec>(
            r#"{"name":"wait","description":"wait for work","inputSchema":{"type":"object"}}"#,
            r#"{"name":"wait","description":"wait for work","inputSchema":{"type":"object"}}"#,
        );
        assert_canonical::<ModelStreamEvent>(
            r#"{"type":"tool_use_start","id":"call_1","name":"wait","toolKind":"function","inputFormat":"json"}"#,
            r#"{"type":"tool_use_start","id":"call_1","name":"wait"}"#,
        );
        assert_canonical::<ContentBlock>(
            r#"{"type":"tool_use","id":"call_1","name":"wait","input":{},"toolKind":"function"}"#,
            r#"{"type":"tool_use","id":"call_1","name":"wait","input":{}}"#,
        );
        assert_canonical::<Message>(
            r#"{"type":"tool_result","uuid":"result_1","toolUseId":"call_1","content":[],"isError":false,"toolKind":"function"}"#,
            r#"{"type":"tool_result","uuid":"result_1","toolUseId":"call_1","content":[],"isError":false}"#,
        );
    }

    #[test]
    fn tool_specs_use_canonical_optional_fields_and_read_legacy_tagged_shapes() {
        let fallback = r#"{"type":"object","properties":{},"additionalProperties":true}"#;
        assert_canonical::<ToolSpec>(
            r#"{"name":"exec","description":"execute source","inputSchema":{"type":"object","properties":{},"additionalProperties":true},"kind":"freeform","format":{"type":"text"}}"#,
            r#"{"name":"exec","description":"execute source","inputSchema":{"type":"object","properties":{},"additionalProperties":true},"kind":"freeform","format":{"type":"text"}}"#,
        );
        assert_canonical::<ToolSpec>(
            r#"{"name":"select","description":"select one item","inputSchema":{"type":"object","properties":{},"additionalProperties":true},"kind":"freeform","format":{"type":"grammar","syntax":"lark","definition":"start: WORD"}}"#,
            r#"{"name":"select","description":"select one item","inputSchema":{"type":"object","properties":{},"additionalProperties":true},"kind":"freeform","format":{"type":"grammar","syntax":"lark","definition":"start: WORD"}}"#,
        );
        assert_canonical::<ToolSpec>(
            r#"{"kind":"function","name":"legacy_wait","description":"legacy wait","inputSchema":{"type":"object"}}"#,
            r#"{"name":"legacy_wait","description":"legacy wait","inputSchema":{"type":"object"}}"#,
        );
        assert_canonical::<ToolSpec>(
            r#"{"kind":"freeform","name":"legacy_exec","description":"legacy execute","format":"javascript"}"#,
            &format!(
                r#"{{"name":"legacy_exec","description":"legacy execute","inputSchema":{fallback},"kind":"freeform","format":{{"type":"text"}}}}"#
            ),
        );
        assert_canonical::<ToolSpec>(
            r#"{"Function":{"name":"external_wait","description":"external wait","inputSchema":{"type":"object"}}}"#,
            r#"{"name":"external_wait","description":"external wait","inputSchema":{"type":"object"}}"#,
        );
        assert_canonical::<ToolSpec>(
            r#"{"Freeform":{"name":"external_exec","description":"external execute","format":"javascript"}}"#,
            &format!(
                r#"{{"name":"external_exec","description":"external execute","inputSchema":{fallback},"kind":"freeform","format":{{"type":"text"}}}}"#
            ),
        );
    }

    #[test]
    fn tool_specs_reject_schema_less_nonlegacy_freeform_shapes() {
        let malformed = [
            r#"{"kind":"freeform","name":"canonical_text","description":"canonical text without a schema","format":{"type":"text"}}"#,
            r#"{"kind":"freeform","name":"missing_format","description":"freeform without a schema or format"}"#,
            r#"{"name":"untagged_legacy_format","description":"a string format is not enough to establish legacy identity","format":"javascript"}"#,
            r#"{"Freeform":{"name":"external_canonical_text","description":"external canonical text without a schema","format":{"type":"text"}}}"#,
            r#"{"kind":"function","name":"function_without_schema","description":"function without a schema"}"#,
            r#"{"kind":"tool_search","name":"search_without_schema","description":"tool search without a schema"}"#,
        ];

        for wire in malformed {
            assert!(
                ToolSpec::decode_runtime(&parse(wire)).is_err(),
                "malformed ToolSpec was accepted: {wire:?}"
            );
        }
    }

    #[test]
    fn tool_spec_deserializes_legacy_function_shape_without_kind() {
        let spec: ToolSpec = decode(
            r#"{"name":"wait","description":"wait for work","inputSchema":{"type":"object"}}"#,
        );

        assert_eq!(
            spec,
            ToolSpec {
                name: "wait".into(),
                description: "wait for work".into(),
                input_schema: parse(r#"{"type":"object"}"#),
                kind: ToolSpecKind::Function,
                format: None,
            }
        );
    }

    #[test]
    fn provider_exact_fields_round_trip_lone_surrogates_without_collision() {
        let lone_high = RuntimeString::from_utf16(vec![0xd800]);
        let lone_low = RuntimeString::from_utf16(vec![0xdc00]);
        let spec = ToolSpec {
            name: lone_high.clone(),
            description: lone_low.clone(),
            input_schema: RuntimeJson::String(lone_high.clone()),
            kind: ToolSpecKind::Freeform,
            format: Some(ToolSpecFormat::Grammar {
                syntax: ToolSpecGrammarSyntax::Regex,
                definition: lone_low.clone(),
            }),
        };
        let wire = encoded(&spec);
        assert_eq!(ToolSpec::decode_runtime(&wire).unwrap(), spec);

        let event = ModelStreamEvent::ThinkingBlock {
            text: lone_high,
            signature: Some(lone_low.clone()),
            encrypted_content: Some(lone_low),
            is_summary: false,
        };
        let wire = encoded(&event);
        assert_eq!(ModelStreamEvent::decode_runtime(&wire).unwrap(), event);
    }

    #[test]
    fn provider_wrappers_preserve_canonical_tags_order_and_optional_fields() {
        for wire in [
            r#"{"type":"auto"}"#,
            r#"{"type":"any"}"#,
            r#"{"type":"tool","name":"wait"}"#,
            r#"{"type":"none"}"#,
        ] {
            assert_canonical::<ToolChoice>(wire, wire);
        }
        assert_canonical::<SystemPromptPart>(
            r#"{"name":"base","text":"hello","cacheable":true,"cacheScope":"global"}"#,
            r#"{"name":"base","text":"hello","cacheable":true,"cacheScope":"global"}"#,
        );
        assert_canonical::<UserLocation>(
            r#"{"city":"Seattle","region":"WA","country":"US","timezone":"America/Los_Angeles"}"#,
            r#"{"city":"Seattle","region":"WA","country":"US","timezone":"America/Los_Angeles"}"#,
        );
        assert_canonical::<WebSearchRequest>(
            r#"{"maxUses":3,"allowedDomains":["example.com"],"userLocation":{"country":"US"},"contextSize":"high"}"#,
            r#"{"maxUses":3,"allowedDomains":["example.com"],"userLocation":{"country":"US"},"contextSize":"high"}"#,
        );
        assert_canonical::<ModelRequest>(
            r#"{"model":"model-x","system":[],"messages":[],"tools":[],"maxOutputTokens":1024,"outputSchema":{"type":"object"},"cacheKey":"conversation"}"#,
            r#"{"model":"model-x","system":[],"messages":[],"tools":[],"maxOutputTokens":1024,"outputSchema":{"type":"object"},"cacheKey":"conversation"}"#,
        );
        assert_canonical::<ProviderInfo>(
            r#"{"name":"provider","baseUrl":"https://example.com","envKey":"API_KEY","defaultModel":"model-x"}"#,
            r#"{"name":"provider","baseUrl":"https://example.com","envKey":"API_KEY","defaultModel":"model-x"}"#,
        );
    }

    #[test]
    fn all_model_stream_event_variants_round_trip_their_exact_wire_tags() {
        for wire in [
            r#"{"type":"message_start","model":"model-x"}"#,
            r#"{"type":"text_delta","text":"hello"}"#,
            r#"{"type":"thinking_delta","text":"thinking"}"#,
            r#"{"type":"thinking_block","text":"thinking","signature":"sig","encryptedContent":"enc"}"#,
            r#"{"type":"redacted_thinking_block","data":"ciphertext"}"#,
            r#"{"type":"tool_use_start","id":"call_1","name":"wait","thoughtSignature":"sig","toolKind":"custom","inputFormat":"raw"}"#,
            r#"{"type":"tool_use_input_delta","id":"call_1","partialJson":"{}"}"#,
            r#"{"type":"tool_use_stop","id":"call_1"}"#,
            r#"{"type":"server_search_query","id":"search_1","query":"rust","action":{"type":"search","query":"rust"}}"#,
            r#"{"type":"web_search_result","id":"search_1","results":[{"title":"Rust","url":"https://www.rust-lang.org"}],"error":"partial"}"#,
            r#"{"type":"citation","citation":{"url":"https://www.rust-lang.org","title":"Rust"}}"#,
            r#"{"type":"usage_snapshot","usage":{"inputTokens":1,"outputTokens":2,"cacheReadInputTokens":3,"cacheCreationInputTokens":4,"totalTokens":10}}"#,
            r#"{"type":"message_stop","stopReason":"end_turn","usage":{"inputTokens":1,"outputTokens":2,"cacheReadInputTokens":3,"cacheCreationInputTokens":4,"totalTokens":10}}"#,
            r#"{"type":"error","error":{"kind":"unknown","message":"failed","status":500}}"#,
        ] {
            assert_canonical::<ModelStreamEvent>(wire, wire);
        }
    }
}
