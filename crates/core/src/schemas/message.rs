//! Conversation message schemas.

use crate::runtime_values::runtime_codec::{RuntimeEnumVariant, RuntimeExternalVariant};
use crate::runtime_values::{
    runtime_codec, RuntimeCodecError, RuntimeCodecErrorKind, RuntimeDecode, RuntimeEncode,
    RuntimeEncoder, RuntimeJson, RuntimeObject, RuntimeObjectDecoder, RuntimeObjectEncoder,
    RuntimeString,
};
use crate::schemas::content::{ContentBlock, OutputBlock};
use crate::types::provider::ProviderToolCallKind;

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

/// Token accounting, sourced authoritatively from provider `usage` responses,
/// normalized to a single provider-agnostic shape. Two protocol conventions are
/// preserved exactly, one for each provider protocol:
///
///  - Anthropic style: the four token fields are DISJOINT —
///    `input_tokens` is the UNCACHED prompt only, separate from the two cache
///    fields — so the context size is their SUM. `total_tokens` is omitted.
///  - OpenAI style: the server reports an authoritative `total_tokens`
///    that already bundles input + cached + output (+ reasoning); the provider
///    carries it in `total_tokens` and the counter TRUSTS it directly rather
///    than re-summing.
///
/// For cost, the four fields are kept disjoint by BOTH providers so each is
/// priced once (OpenAI subtracts its cached subset out of `input_tokens`).
/// `tokens_from_usage` prefers `total_tokens` when present, else sums.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Usage {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_read_input_tokens: u64,
    pub cache_creation_input_tokens: u64,
    /// Authoritative server total (OpenAI). Absent for Anthropic.
    pub total_tokens: Option<u64>,
}

impl RuntimeEncode for Usage {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let Self {
            input_tokens,
            output_tokens,
            cache_read_input_tokens,
            cache_creation_input_tokens,
            total_tokens,
        } = self;
        let mut object = RuntimeObjectEncoder::new();
        object.field("inputTokens", input_tokens)?;
        object.field("outputTokens", output_tokens)?;
        object.field("cacheReadInputTokens", cache_read_input_tokens)?;
        object.field("cacheCreationInputTokens", cache_creation_input_tokens)?;
        if let Some(value) = total_tokens {
            object.field("totalTokens", value)?;
        }
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for Usage {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                input_tokens: object.required("inputTokens", &[])?,
                output_tokens: object.required("outputTokens", &[])?,
                cache_read_input_tokens: object.defaulted("cacheReadInputTokens", &[], || 0)?,
                cache_creation_input_tokens: object.defaulted(
                    "cacheCreationInputTokens",
                    &[],
                    || 0,
                )?,
                total_tokens: object.optional("totalTokens", &[])?,
            })
        })
    }
}

/// The rendered kind of a system message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SystemSubtype {
    Info,
    Warning,
    CompactNotice,
    LocalCommand,
    /// Host-owned user-turn rendering metadata. Providers ignore all system
    /// messages; history projections pair this record to its user by uuid.
    UserDisplay,
    /// Records one model-driven history removal: the note the model reads plus
    /// the uuid ledger that keeps the removal applied after a resume.
    SnipBoundary,
}

impl RuntimeEncode for SystemSubtype {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Info => "info",
            Self::Warning => "warning",
            Self::CompactNotice => "compact_notice",
            Self::LocalCommand => "local_command",
            Self::UserDisplay => "user_display",
            Self::SnipBoundary => "snip_boundary",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for SystemSubtype {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("info", &[]),
                RuntimeEnumVariant::new("warning", &[]),
                RuntimeEnumVariant::new("compact_notice", &[]),
                RuntimeEnumVariant::new("local_command", &[]),
                RuntimeEnumVariant::new("user_display", &[]),
                RuntimeEnumVariant::new("snip_boundary", &[]),
            ],
            &[
                Self::Info,
                Self::Warning,
                Self::CompactNotice,
                Self::LocalCommand,
                Self::UserDisplay,
                Self::SnipBoundary,
            ],
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct UserMessage {
    pub question_answer: Option<Box<crate::engine::questions::QuestionAnswer>>,
    pub uuid: RuntimeString,
    pub content: Vec<ContentBlock>,
    /// Engine-injected user-role message that is NOT a genuine user turn — an
    /// automatic re-prompt the loop folds in mid-turn (an outputSchema re-sample
    /// nudge, a blocking Stop-hook continuation). Persisted so resume sees the
    /// same stream, but excluded from user-turn counting for conversation
    /// rollback. Omitted for real user input.
    pub is_meta: Option<bool>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AssistantMessage {
    pub uuid: RuntimeString,
    pub content: Vec<ContentBlock>,
    pub model: RuntimeString,
    pub usage: Option<Usage>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ToolResultMessage {
    pub uuid: RuntimeString,
    pub tool_use_id: RuntimeString,
    pub content: Vec<OutputBlock>,
    pub is_error: bool,
    pub tool_kind: Option<ProviderToolCallKind>,
    pub native_tool_output: Option<RuntimeJson>,
    /// Non-persisted structured output kept in-memory so reentrant tool bridges
    /// can consume typed results without scraping model-visible text.
    pub output: Option<RuntimeJson>,
    /// Media that belongs on the SAME user turn as this tool_result but cannot
    /// live inside `content` — currently a native PDF `document` block from a
    /// Read. Rendered as SIBLING blocks of the tool_result in its coalesced user
    /// turn. Persisted so the attachment survives replay. Omitted for the common
    /// no-attachment case.
    pub extra_user_blocks: Option<Vec<ContentBlock>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SystemMessage {
    pub uuid: RuntimeString,
    pub text: RuntimeString,
    pub subtype: SystemSubtype,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactBoundaryMessage {
    pub uuid: RuntimeString,
    pub summary: RuntimeString,
    /// An opaque checkpoint is replayed only by its authenticated source.
    pub native_compaction: Option<NativeCompactionCheckpoint>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeCompactionCheckpoint {
    pub source: RuntimeString,
    pub auth_scope: RuntimeString,
    pub model: RuntimeString,
    pub comp_hash: Option<RuntimeString>,
    pub response_id: RuntimeString,
    pub encrypted_content: RuntimeString,
    pub retained_input: Vec<RuntimeJson>,
    /// Aligned private provenance; absent legacy entries are ordinary user input.
    pub retained_input_metadata: Vec<Option<NativeRetainedInputMetadata>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeRetainedInputMetadata {
    pub source_message_id: RuntimeString,
    /// Required origin blocks, never an unbudgeted duplicate of the message body.
    pub provenance: Vec<RuntimeJson>,
}

/// The canonical conversation message. `tool_result` is split out as its own
/// message (rather than only a content block) so the rollout can stream it as a
/// standalone item and resume can rebuild ordering deterministically.
#[derive(Debug, Clone, PartialEq)]
pub enum Message {
    User(UserMessage),
    Assistant(AssistantMessage),
    ToolResult(ToolResultMessage),
    System(SystemMessage),
    CompactBoundary(CompactBoundaryMessage),
}

macro_rules! impl_runtime_object_codec {
    ($type:ty) => {
        impl RuntimeEncode for $type {
            fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
                out.emit(RuntimeJson::Object(self.encode_fields()?))
            }
        }

        impl RuntimeDecode for $type {
            fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
                runtime_codec::decode_object(value, false, Self::decode_fields)
            }
        }
    };
}

impl UserMessage {
    fn encode_fields(&self) -> Result<RuntimeObject, RuntimeCodecError> {
        let Self {
            uuid,
            content,
            is_meta,
            question_answer,
        } = self;
        let mut object = RuntimeObjectEncoder::new();
        object.field("uuid", uuid)?;
        object.field("content", content)?;
        if let Some(value) = question_answer {
            object.field("questionAnswer", value)?;
        }
        if let Some(value) = is_meta {
            object.field("isMeta", value)?;
        }
        Ok(object.finish())
    }

    fn decode_fields(object: &mut RuntimeObjectDecoder<'_>) -> Result<Self, RuntimeCodecError> {
        Ok(Self {
            uuid: object.required("uuid", &[])?,
            content: object.required("content", &[])?,
            is_meta: object.optional("isMeta", &[])?,
            question_answer: object.optional("questionAnswer", &[])?,
        })
    }
}

impl_runtime_object_codec!(UserMessage);

impl AssistantMessage {
    fn encode_fields(&self) -> Result<RuntimeObject, RuntimeCodecError> {
        let Self {
            uuid,
            content,
            model,
            usage,
        } = self;
        let mut object = RuntimeObjectEncoder::new();
        object.field("uuid", uuid)?;
        object.field("content", content)?;
        object.field("model", model)?;
        if let Some(value) = usage {
            object.field("usage", value)?;
        }
        Ok(object.finish())
    }

    fn decode_fields(object: &mut RuntimeObjectDecoder<'_>) -> Result<Self, RuntimeCodecError> {
        Ok(Self {
            uuid: object.required("uuid", &[])?,
            content: object.required("content", &[])?,
            model: object.required("model", &[])?,
            usage: object.optional("usage", &[])?,
        })
    }
}

impl_runtime_object_codec!(AssistantMessage);

impl ToolResultMessage {
    fn encode_fields(&self) -> Result<RuntimeObject, RuntimeCodecError> {
        let Self {
            uuid,
            tool_use_id,
            content,
            is_error,
            tool_kind,
            native_tool_output,
            output: _,
            extra_user_blocks,
        } = self;
        let mut object = RuntimeObjectEncoder::new();
        object.field("uuid", uuid)?;
        object.field("toolUseId", tool_use_id)?;
        object.field("content", content)?;
        object.field("isError", is_error)?;
        if !ProviderToolCallKind::is_implicit(tool_kind) {
            object.field("toolKind", tool_kind)?;
        }
        if let Some(value) = native_tool_output {
            object.field("nativeToolOutput", value)?;
        }
        if let Some(value) = extra_user_blocks {
            object.field("extraUserBlocks", value)?;
        }
        Ok(object.finish())
    }

    fn decode_fields(object: &mut RuntimeObjectDecoder<'_>) -> Result<Self, RuntimeCodecError> {
        Ok(Self {
            uuid: object.required("uuid", &[])?,
            tool_use_id: object.required("toolUseId", &[])?,
            content: object.required("content", &[])?,
            is_error: object.required("isError", &[])?,
            tool_kind: object.optional("toolKind", &[])?,
            native_tool_output: object.optional("nativeToolOutput", &[])?,
            output: None,
            extra_user_blocks: object.optional("extraUserBlocks", &[])?,
        })
    }
}

impl_runtime_object_codec!(ToolResultMessage);

impl SystemMessage {
    fn encode_fields(&self) -> Result<RuntimeObject, RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("uuid", &self.uuid)?;
        object.field("text", &self.text)?;
        object.field("subtype", &self.subtype)?;
        Ok(object.finish())
    }

    fn decode_fields(object: &mut RuntimeObjectDecoder<'_>) -> Result<Self, RuntimeCodecError> {
        Ok(Self {
            uuid: object.required("uuid", &[])?,
            text: object.required("text", &[])?,
            subtype: object.required("subtype", &[])?,
        })
    }
}

impl_runtime_object_codec!(SystemMessage);

impl CompactBoundaryMessage {
    fn encode_fields(&self) -> Result<RuntimeObject, RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("uuid", &self.uuid)?;
        object.field("summary", &self.summary)?;
        if let Some(checkpoint) = &self.native_compaction {
            object.field("nativeCompaction", checkpoint)?;
        }
        Ok(object.finish())
    }

    fn decode_fields(object: &mut RuntimeObjectDecoder<'_>) -> Result<Self, RuntimeCodecError> {
        Ok(Self {
            uuid: object.required("uuid", &[])?,
            summary: object.required("summary", &[])?,
            native_compaction: object.optional("nativeCompaction", &[])?,
        })
    }
}

impl_runtime_object_codec!(CompactBoundaryMessage);

impl NativeCompactionCheckpoint {
    /// Estimate model-visible payload rather than counting encoded transport bytes.
    pub fn estimated_context_tokens(&self) -> u64 {
        let opaque_bytes = self
            .encrypted_content
            .len_utf16()
            .saturating_mul(3)
            .saturating_div(4)
            .saturating_sub(650);
        let opaque_tokens = (opaque_bytes as u64).div_ceil(4).max(1);
        opaque_tokens.saturating_add(
            self.retained_input
                .iter()
                .map(native_retained_item_tokens)
                .sum::<u64>(),
        )
    }
    fn encode_fields(&self) -> Result<RuntimeObject, RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("source", &self.source)?;
        object.field("authScope", &self.auth_scope)?;
        object.field("model", &self.model)?;
        if let Some(value) = &self.comp_hash {
            object.field("compHash", value)?;
        }
        object.field("responseId", &self.response_id)?;
        object.field("encryptedContent", &self.encrypted_content)?;
        object.field("retainedInput", &self.retained_input)?;
        if !self.retained_input_metadata.is_empty() {
            object.field("retainedInputMetadata", &self.retained_input_metadata)?;
        }
        Ok(object.finish())
    }

    fn decode_fields(object: &mut RuntimeObjectDecoder<'_>) -> Result<Self, RuntimeCodecError> {
        Ok(Self {
            source: object.required("source", &[])?,
            auth_scope: object.required("authScope", &[])?,
            model: object.required("model", &[])?,
            comp_hash: object.optional("compHash", &[])?,
            response_id: object.required("responseId", &[])?,
            encrypted_content: object.required("encryptedContent", &[])?,
            retained_input: object.required("retainedInput", &[])?,
            retained_input_metadata: object.defaulted("retainedInputMetadata", &[], Vec::new)?,
        })
    }
}

pub(crate) fn native_retained_item_tokens(item: &RuntimeJson) -> u64 {
    let Some(content) = item.get("content") else {
        return 0;
    };
    match content {
        RuntimeJson::String(_) => native_retained_part_tokens(content),
        RuntimeJson::Array(blocks) => blocks
            .iter()
            .map(native_retained_part_tokens)
            .fold(0, u64::saturating_add),
        _ => 0,
    }
}

pub(crate) fn native_retained_part_tokens(part: &RuntimeJson) -> u64 {
    if let Some(text) = part
        .as_str()
        .or_else(|| part.get("text").and_then(RuntimeJson::as_str))
    {
        return (text.to_node_utf8().len() as u64).div_ceil(4);
    }
    if part
        .get("type")
        .and_then(RuntimeJson::as_str)
        .is_some_and(|kind| {
            matches!(
                kind.try_to_utf8().ok().as_deref(),
                Some("input_image" | "input_file" | "input_audio" | "input_video")
            )
        })
    {
        crate::context::tokens::MEDIA_TOKENS
    } else {
        0
    }
}

impl_runtime_object_codec!(NativeCompactionCheckpoint);

impl NativeRetainedInputMetadata {
    fn encode_fields(&self) -> Result<RuntimeObject, RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("sourceMessageId", &self.source_message_id)?;
        object.field("provenance", &self.provenance)?;
        Ok(object.finish())
    }

    fn decode_fields(object: &mut RuntimeObjectDecoder<'_>) -> Result<Self, RuntimeCodecError> {
        Ok(Self {
            source_message_id: object.required("sourceMessageId", &[])?,
            provenance: object.required("provenance", &[])?,
        })
    }
}

impl_runtime_object_codec!(NativeRetainedInputMetadata);

impl RuntimeEncode for Message {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let (tag, fields) = match self {
            Self::User(value) => ("user", value.encode_fields()?),
            Self::Assistant(value) => ("assistant", value.encode_fields()?),
            Self::ToolResult(value) => ("tool_result", value.encode_fields()?),
            Self::System(value) => ("system", value.encode_fields()?),
            Self::CompactBoundary(value) => ("compact_boundary", value.encode_fields()?),
        };
        runtime_codec::encode_internal(out, "type", &[], tag, &fields)
    }
}

impl RuntimeDecode for Message {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_internal(
            value,
            false,
            "type",
            &[],
            &[
                RuntimeEnumVariant::new("user", &[]),
                RuntimeEnumVariant::new("assistant", &[]),
                RuntimeEnumVariant::new("tool_result", &[]),
                RuntimeEnumVariant::new("system", &[]),
                RuntimeEnumVariant::new("compact_boundary", &[]),
            ],
            |variant, object| match variant {
                0 => UserMessage::decode_fields(object).map(Self::User),
                1 => AssistantMessage::decode_fields(object).map(Self::Assistant),
                2 => ToolResultMessage::decode_fields(object).map(Self::ToolResult),
                3 => SystemMessage::decode_fields(object).map(Self::System),
                4 => CompactBoundaryMessage::decode_fields(object).map(Self::CompactBoundary),
                _ => Err(RuntimeCodecError::new(
                    RuntimeCodecErrorKind::UnknownEnumVariant,
                )),
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schemas::content::TextBlock;

    fn parse(source: &str) -> RuntimeJson {
        crate::runtime_values::runtime_json::parse(&RuntimeString::from(source)).unwrap()
    }

    fn assert_message_round_trip(source: &str) {
        let wire = parse(source);
        let decoded = runtime_codec::decode_value::<Message>(&wire).unwrap();
        assert_eq!(runtime_codec::encode_value(&decoded).unwrap(), wire);
    }

    fn result(tool_kind: Option<ProviderToolCallKind>) -> ToolResultMessage {
        ToolResultMessage {
            uuid: "result-1".into(),
            tool_use_id: "call-1".into(),
            content: vec![OutputBlock::Text(TextBlock {
                text: "done".into(),
                citations: None,
                harness_envelope: None,
                message_id: None,
                phase: None,
            })],
            is_error: false,
            tool_kind,
            native_tool_output: None,
            output: None,
            extra_user_blocks: None,
        }
    }

    #[test]
    fn every_message_variant_preserves_exact_tag_field_names_and_order() {
        for source in [
            r#"{"type":"user","uuid":"user-1","content":[{"type":"text","text":"hello"}],"isMeta":true}"#,
            r#"{"type":"assistant","uuid":"assistant-1","content":[{"type":"thinking","text":"summary"}],"model":"model-1","usage":{"inputTokens":3,"outputTokens":5,"cacheReadInputTokens":7,"cacheCreationInputTokens":11,"totalTokens":26}}"#,
            r#"{"type":"tool_result","uuid":"result-1","toolUseId":"call-1","content":[{"type":"text","text":"done"}],"isError":false,"toolKind":"tool_search","nativeToolOutput":{"tools":[{"name":"read"}]},"extraUserBlocks":[{"type":"document","mimeType":"application/pdf","base64":"JVBERi0="}]}"#,
            r#"{"type":"system","uuid":"system-1","text":"warning","subtype":"compact_notice"}"#,
            r#"{"type":"system","uuid":"display-1","text":"{\"displayText\":\"/push-code\"}","subtype":"user_display"}"#,
            r#"{"type":"compact_boundary","uuid":"compact-1","summary":"summary"}"#,
        ] {
            assert_message_round_trip(source);
        }
    }

    #[test]
    fn usage_defaults_are_decode_only_and_optional_null_is_omitted() {
        let minimal = parse(r#"{"inputTokens":3,"outputTokens":5,"totalTokens":null,"extra":1}"#);
        let usage = runtime_codec::decode_value::<Usage>(&minimal).unwrap();
        assert_eq!(usage.cache_read_input_tokens, 0);
        assert_eq!(usage.cache_creation_input_tokens, 0);
        assert_eq!(usage.total_tokens, None);
        assert_eq!(
            runtime_codec::encode_value(&usage).unwrap(),
            parse(
                r#"{"inputTokens":3,"outputTokens":5,"cacheReadInputTokens":0,"cacheCreationInputTokens":0}"#
            )
        );

        let explicit_null = parse(
            r#"{"inputTokens":3,"outputTokens":5,"cacheReadInputTokens":null,"cacheCreationInputTokens":0}"#,
        );
        let error = runtime_codec::decode_value::<Usage>(&explicit_null).unwrap_err();
        assert_eq!(error.kind(), &RuntimeCodecErrorKind::ExpectedNumber);
        assert_eq!(
            error.pointer(),
            RuntimeString::from("/cacheReadInputTokens")
        );
    }

    #[test]
    fn tool_result_skip_and_provider_metadata_semantics_are_exact() {
        let absent = result(None);
        let absent_value = runtime_codec::encode_value(&absent).unwrap();
        assert_eq!(
            absent_value,
            parse(
                r#"{"uuid":"result-1","toolUseId":"call-1","content":[{"type":"text","text":"done"}],"isError":false}"#
            )
        );
        assert_eq!(
            runtime_codec::decode_value::<ToolResultMessage>(&absent_value).unwrap(),
            absent
        );

        let explicit_default = result(Some(ProviderToolCallKind::Function));
        assert_eq!(
            runtime_codec::encode_value(&explicit_default).unwrap(),
            absent_value
        );

        let with_skipped_output = ToolResultMessage {
            output: Some(parse(r#"{"private":true}"#)),
            ..result(None)
        };
        assert_eq!(
            runtime_codec::encode_value(&with_skipped_output).unwrap(),
            absent_value
        );

        let incoming_skipped = parse(
            r#"{"uuid":"result-1","toolUseId":"call-1","content":[{"type":"text","text":"done"}],"isError":false,"toolKind":"function","nativeToolOutput":null,"output":{"private":true},"unknown":1}"#,
        );
        let incoming_skipped =
            runtime_codec::decode_value::<ToolResultMessage>(&incoming_skipped).unwrap();
        assert_eq!(
            incoming_skipped.tool_kind,
            Some(ProviderToolCallKind::Function)
        );
        assert_eq!(incoming_skipped.native_tool_output, None);
        assert_eq!(incoming_skipped.output, None);
        assert_eq!(
            runtime_codec::encode_value(&incoming_skipped).unwrap(),
            absent_value
        );

        for (kind, wire) in [
            (
                ProviderToolCallKind::Custom,
                r#"{"uuid":"result-1","toolUseId":"call-1","content":[{"type":"text","text":"done"}],"isError":false,"toolKind":"custom"}"#,
            ),
            (
                ProviderToolCallKind::ToolSearch,
                r#"{"uuid":"result-1","toolUseId":"call-1","content":[{"type":"text","text":"done"}],"isError":false,"toolKind":"tool_search","nativeToolOutput":{"tools":[{"name":"read"}]}}"#,
            ),
        ] {
            let mut present = result(Some(kind));
            if kind == ProviderToolCallKind::ToolSearch {
                present.native_tool_output = Some(parse(r#"{"tools":[{"name":"read"}]}"#));
            }
            let present_value = runtime_codec::encode_value(&present).unwrap();
            assert_eq!(present_value, parse(wire));
            assert_eq!(
                runtime_codec::decode_value::<ToolResultMessage>(&present_value).unwrap(),
                present
            );
        }
    }

    #[test]
    fn message_codec_preserves_non_scalar_strings_without_serde() {
        let message = Message::CompactBoundary(CompactBoundaryMessage {
            native_compaction: None,
            uuid: RuntimeString::from_utf16(vec![0xd800]),
            summary: RuntimeString::from_utf16(vec![0xdc00]),
        });
        let encoded = runtime_codec::encode_value(&message).unwrap();
        assert_eq!(
            runtime_codec::decode_value::<Message>(&encoded).unwrap(),
            message
        );
    }
}
