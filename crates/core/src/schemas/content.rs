//! Content-block schemas: the assistant/user message body shapes.

use crate::runtime_values::runtime_codec::{RuntimeEnumVariant, RuntimeExternalVariant};
use crate::runtime_values::{
    runtime_codec, RuntimeCodecError, RuntimeCodecErrorKind, RuntimeDecode, RuntimeEncode,
    RuntimeEncoder, RuntimeJson, RuntimeObject, RuntimeObjectDecoder, RuntimeObjectEncoder,
    RuntimeString,
};
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

/// A citation tying a span of assistant text to a web source. Provider-neutral
/// home for Anthropic `web_search_result_location`, OpenAI `url_citation`
/// annotations, and Gemini `groundingSupport` spans — each adapter maps its
/// native shape onto this. Attached as a field on the OWNING text block (not a
/// standalone block), matching all three wire models.
#[derive(Debug, Clone, PartialEq)]
pub struct Citation {
    pub url: RuntimeString,
    pub title: Option<RuntimeString>,
    /// The source excerpt the citation quotes (Anthropic `cited_text`).
    pub cited_text: Option<RuntimeString>,
    /// Char range into the owning text block this citation annotates
    /// (Gemini/OpenAI spans).
    pub start_index: Option<u64>,
    pub end_index: Option<u64>,
    /// Provider-opaque re-citation handle required to re-cite on replay
    /// (Anthropic `encrypted_index`).
    pub encrypted_index: Option<RuntimeString>,
}

/// A single native web-search hit (neutral across providers).
#[derive(Debug, Clone, PartialEq)]
pub struct NativeSearchResult {
    pub title: RuntimeString,
    pub url: RuntimeString,
    /// Provider-opaque token required to replay this result verbatim for
    /// re-citation in a later turn (Anthropic `encrypted_content`). Omitted by
    /// providers that don't need it. Never rendered.
    pub encrypted_token: Option<RuntimeString>,
    /// Freshness hint when the provider supplies one (Anthropic `page_age`).
    pub page_age: Option<RuntimeString>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum WebSearchAction {
    Search {
        query: Option<RuntimeString>,
        queries: Option<Vec<RuntimeString>>,
    },
    OpenPage {
        url: Option<RuntimeString>,
    },
    FindInPage {
        url: Option<RuntimeString>,
        pattern: Option<RuntimeString>,
    },
    Other,
}

/// A text content block. May carry web-search citations annotating spans of its
/// text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HarnessEnvelope {
    SystemReminder,
    /// Trusted provenance accompanies the plain text; it never grants authority.
    SubagentMessage,
    /// Completed output of a context-summary operation.
    CompactionOutput,
}

impl RuntimeEncode for HarnessEnvelope {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::SystemReminder => "system-reminder",
            Self::SubagentMessage => "subagent-message",
            Self::CompactionOutput => "compaction-output",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for HarnessEnvelope {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("system-reminder", &[]),
                RuntimeEnumVariant::new("subagent-message", &[]),
                RuntimeEnumVariant::new("compaction-output", &[]),
            ],
            &[
                Self::SystemReminder,
                Self::SubagentMessage,
                Self::CompactionOutput,
            ],
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TextBlock {
    pub text: RuntimeString,
    pub citations: Option<Vec<Citation>>,
    pub harness_envelope: Option<HarnessEnvelope>,
    /// Stable identity and provider intent for this displayed text message.
    pub message_id: Option<RuntimeString>,
    pub phase: Option<RuntimeString>,
}

/// A provider server-issued web search (the query + its results), as an
/// assistant content block. Holds the raw native results for transcript fidelity
/// + replay.
#[derive(Debug, Clone, PartialEq)]
pub struct WebSearchCallBlock {
    pub id: RuntimeString,
    pub query: RuntimeString,
    pub action: Option<WebSearchAction>,
    pub results: Vec<NativeSearchResult>,
    pub error: Option<RuntimeString>,
}

/// Extended thinking block (model reasoning). Carries up to one opaque
/// per-provider replay token:
/// - `signature` — Anthropic's signed-thinking signature (the API rejects
///   unsigned thinking in history; the engine replays only signed blocks).
/// - `encrypted_content` — OpenAI Responses reasoning `encrypted_content` (the
///   model's opaque reasoning state; `text` holds the visible summary).
///
/// Each provider populates only its own field; they never co-occur on one block.
#[derive(Debug, Clone, PartialEq)]
pub struct ThinkingBlock {
    pub text: RuntimeString,
    /// Stable live item identity, retained by persisted and aborted messages.
    pub message_id: Option<RuntimeString>,
    pub signature: Option<RuntimeString>,
    pub encrypted_content: Option<RuntimeString>,
}

/// Redacted thinking block: thinking the API encrypted (safety filtering). It
/// has no plaintext — only an opaque `data` payload — and is replayed verbatim
/// so the model's reasoning trajectory stays intact across turns.
#[derive(Debug, Clone, PartialEq)]
pub struct RedactedThinkingBlock {
    pub data: RuntimeString,
}

/// The provider's requested image inspection quality.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImageDetail {
    Auto,
    Low,
    High,
    Original,
}

impl ImageDetail {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Low => "low",
            Self::High => "high",
            Self::Original => "original",
        }
    }
}

impl RuntimeEncode for ImageDetail {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        runtime_codec::encode_external_unit(out, self.as_str())
    }
}

impl RuntimeDecode for ImageDetail {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("auto", &[]),
                RuntimeEnumVariant::new("low", &[]),
                RuntimeEnumVariant::new("high", &[]),
                RuntimeEnumVariant::new("original", &[]),
            ],
            &[Self::Auto, Self::Low, Self::High, Self::Original],
        )
    }
}

/// An image block (base64-encoded).
#[derive(Debug, Clone, PartialEq)]
pub struct ImageBlock {
    /// Optional native detail hint, retained across history and tool bridges.
    pub detail: Option<ImageDetail>,
    pub mime_type: RuntimeString,
    pub base64: RuntimeString,
    /// The local file the bytes were read from, when the sender knew one.
    /// Display-only provenance: history projections emit this path instead of
    /// re-sending the bytes, and a missing file falls back to the base64 copy.
    /// Providers never see it — model input is built from `base64` alone.
    pub source_path: Option<RuntimeString>,
}

/// A document block (base64-encoded) — currently PDFs delivered to the model
/// NATIVELY (no client-side rasterization). Each provider maps it to its own
/// file primitive: Anthropic `document` source, OpenAI `input_file`, Gemini
/// `inlineData`.
#[derive(Debug, Clone, PartialEq)]
pub struct DocumentBlock {
    pub mime_type: RuntimeString,
    pub base64: RuntimeString,
    pub filename: Option<RuntimeString>,
}

/// Native audio or video input. The selected model must explicitly advertise
/// the MIME type; this is independent of image and PDF capabilities.
#[derive(Debug, Clone, PartialEq)]
pub struct MediaBlock {
    pub mime_type: RuntimeString,
    pub base64: RuntimeString,
    pub filename: Option<RuntimeString>,
}

/// A model-issued tool call.
#[derive(Debug, Clone, PartialEq)]
pub struct ToolUseBlock {
    pub id: RuntimeString,
    pub name: RuntimeString,
    pub input: RuntimeJson,
    pub tool_kind: Option<ProviderToolCallKind>,
    /// Gemini's opaque `thoughtSignature` for this call — the model's signed
    /// reasoning that led to it. Absent for Anthropic/OpenAI.
    pub thought_signature: Option<RuntimeString>,
}

/// The result of executing a tool call, fed back to the model.
#[derive(Debug, Clone, PartialEq)]
pub struct ToolResultBlock {
    pub tool_use_id: RuntimeString,
    pub content: Vec<OutputBlock>,
    pub is_error: Option<bool>,
}

/// Blocks that may appear inside a tool_result. NOTE: a `document` block is NOT
/// here — every provider places PDFs at the top level of a user message, never
/// inside a tool_result, so a read PDF rides `ToolResultMessage.extra_user_blocks`
/// rather than this content.
#[derive(Debug, Clone, PartialEq)]
pub enum OutputBlock {
    Text(TextBlock),
    Image(ImageBlock),
}

/// The full content-block union that can appear on a message.
#[derive(Debug, Clone, PartialEq)]
pub enum ContentBlock {
    Text(TextBlock),
    Thinking(ThinkingBlock),
    RedactedThinking(RedactedThinkingBlock),
    Image(ImageBlock),
    Document(DocumentBlock),
    Media(MediaBlock),
    ToolUse(ToolUseBlock),
    ToolResult(ToolResultBlock),
    WebSearchCall(WebSearchCallBlock),
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

impl Citation {
    fn encode_fields(&self) -> Result<RuntimeObject, RuntimeCodecError> {
        let Self {
            url,
            title,
            cited_text,
            start_index,
            end_index,
            encrypted_index,
        } = self;
        let mut object = RuntimeObjectEncoder::new();
        object.field("url", url)?;
        if let Some(value) = title {
            object.field("title", value)?;
        }
        if let Some(value) = cited_text {
            object.field("citedText", value)?;
        }
        if let Some(value) = start_index {
            object.field("startIndex", value)?;
        }
        if let Some(value) = end_index {
            object.field("endIndex", value)?;
        }
        if let Some(value) = encrypted_index {
            object.field("encryptedIndex", value)?;
        }
        Ok(object.finish())
    }

    fn decode_fields(object: &mut RuntimeObjectDecoder<'_>) -> Result<Self, RuntimeCodecError> {
        Ok(Self {
            url: object.required("url", &[])?,
            title: object.optional("title", &[])?,
            cited_text: object.optional("citedText", &[])?,
            start_index: object.optional("startIndex", &[])?,
            end_index: object.optional("endIndex", &[])?,
            encrypted_index: object.optional("encryptedIndex", &[])?,
        })
    }
}

impl_runtime_object_codec!(Citation);

impl NativeSearchResult {
    fn encode_fields(&self) -> Result<RuntimeObject, RuntimeCodecError> {
        let Self {
            title,
            url,
            encrypted_token,
            page_age,
        } = self;
        let mut object = RuntimeObjectEncoder::new();
        object.field("title", title)?;
        object.field("url", url)?;
        if let Some(value) = encrypted_token {
            object.field("encryptedToken", value)?;
        }
        if let Some(value) = page_age {
            object.field("pageAge", value)?;
        }
        Ok(object.finish())
    }

    fn decode_fields(object: &mut RuntimeObjectDecoder<'_>) -> Result<Self, RuntimeCodecError> {
        Ok(Self {
            title: object.required("title", &[])?,
            url: object.required("url", &[])?,
            encrypted_token: object.optional("encryptedToken", &[])?,
            page_age: object.optional("pageAge", &[])?,
        })
    }
}

impl_runtime_object_codec!(NativeSearchResult);

impl RuntimeEncode for WebSearchAction {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let (tag, fields) = match self {
            Self::Search { query, queries } => {
                let mut fields = RuntimeObjectEncoder::new();
                if let Some(value) = query {
                    fields.field("query", value)?;
                }
                if let Some(value) = queries {
                    fields.field("queries", value)?;
                }
                ("search", fields.finish())
            }
            Self::OpenPage { url } => {
                let mut fields = RuntimeObjectEncoder::new();
                if let Some(value) = url {
                    fields.field("url", value)?;
                }
                ("open_page", fields.finish())
            }
            Self::FindInPage { url, pattern } => {
                let mut fields = RuntimeObjectEncoder::new();
                if let Some(value) = url {
                    fields.field("url", value)?;
                }
                if let Some(value) = pattern {
                    fields.field("pattern", value)?;
                }
                ("find_in_page", fields.finish())
            }
            Self::Other => ("other", RuntimeObject::new()),
        };
        runtime_codec::encode_internal(out, "type", &[], tag, &fields)
    }
}

impl RuntimeDecode for WebSearchAction {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_internal(
            value,
            false,
            "type",
            &[],
            &[
                RuntimeEnumVariant::new("search", &[]),
                RuntimeEnumVariant::new("open_page", &[]),
                RuntimeEnumVariant::new("find_in_page", &[]),
                RuntimeEnumVariant::new("other", &[]),
            ],
            |variant, object| match variant {
                0 => Ok(Self::Search {
                    query: object.optional("query", &[])?,
                    queries: object.optional("queries", &[])?,
                }),
                1 => Ok(Self::OpenPage {
                    url: object.optional("url", &[])?,
                }),
                2 => Ok(Self::FindInPage {
                    url: object.optional("url", &[])?,
                    pattern: object.optional("pattern", &[])?,
                }),
                3 => Ok(Self::Other),
                _ => Err(RuntimeCodecError::new(
                    RuntimeCodecErrorKind::UnknownEnumVariant,
                )),
            },
        )
    }
}

impl TextBlock {
    fn encode_fields(&self) -> Result<RuntimeObject, RuntimeCodecError> {
        let Self {
            text,
            citations,
            harness_envelope,
            message_id,
            phase,
        } = self;
        let mut object = RuntimeObjectEncoder::new();
        object.field("text", text)?;
        if let Some(value) = citations {
            object.field("citations", value)?;
        }
        if let Some(value) = harness_envelope {
            object.field("harnessEnvelope", value)?;
        }
        if let Some(value) = message_id {
            object.field("messageId", value)?;
        }
        if let Some(value) = phase {
            object.field("phase", value)?;
        }
        Ok(object.finish())
    }

    fn decode_fields(object: &mut RuntimeObjectDecoder<'_>) -> Result<Self, RuntimeCodecError> {
        Ok(Self {
            text: object.required("text", &[])?,
            citations: object.optional("citations", &[])?,
            harness_envelope: object.optional("harnessEnvelope", &[])?,
            message_id: object.optional("messageId", &[])?,
            phase: object.optional("phase", &[])?,
        })
    }
}

impl_runtime_object_codec!(TextBlock);

impl WebSearchCallBlock {
    fn encode_fields(&self) -> Result<RuntimeObject, RuntimeCodecError> {
        let Self {
            id,
            query,
            action,
            results,
            error,
        } = self;
        let mut object = RuntimeObjectEncoder::new();
        object.field("id", id)?;
        object.field("query", query)?;
        if let Some(value) = action {
            object.field("action", value)?;
        }
        object.field("results", results)?;
        if let Some(value) = error {
            object.field("error", value)?;
        }
        Ok(object.finish())
    }

    fn decode_fields(object: &mut RuntimeObjectDecoder<'_>) -> Result<Self, RuntimeCodecError> {
        Ok(Self {
            id: object.required("id", &[])?,
            query: object.required("query", &[])?,
            action: object.optional("action", &[])?,
            results: object.required("results", &[])?,
            error: object.optional("error", &[])?,
        })
    }
}

impl_runtime_object_codec!(WebSearchCallBlock);

impl ThinkingBlock {
    fn encode_fields(&self) -> Result<RuntimeObject, RuntimeCodecError> {
        let Self {
            text,
            message_id,
            signature,
            encrypted_content,
        } = self;
        let mut object = RuntimeObjectEncoder::new();
        object.field("text", text)?;
        if let Some(value) = message_id {
            object.field("messageId", value)?;
        }
        if let Some(value) = signature {
            object.field("signature", value)?;
        }
        if let Some(value) = encrypted_content {
            object.field("encryptedContent", value)?;
        }
        Ok(object.finish())
    }

    fn decode_fields(object: &mut RuntimeObjectDecoder<'_>) -> Result<Self, RuntimeCodecError> {
        Ok(Self {
            text: object.required("text", &[])?,
            message_id: object.optional("messageId", &[])?,
            signature: object.optional("signature", &[])?,
            encrypted_content: object.optional("encryptedContent", &[])?,
        })
    }
}

impl_runtime_object_codec!(ThinkingBlock);

impl RedactedThinkingBlock {
    fn encode_fields(&self) -> Result<RuntimeObject, RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("data", &self.data)?;
        Ok(object.finish())
    }

    fn decode_fields(object: &mut RuntimeObjectDecoder<'_>) -> Result<Self, RuntimeCodecError> {
        Ok(Self {
            data: object.required("data", &[])?,
        })
    }
}

impl_runtime_object_codec!(RedactedThinkingBlock);

impl ImageBlock {
    fn encode_fields(&self) -> Result<RuntimeObject, RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("mimeType", &self.mime_type)?;
        object.field("base64", &self.base64)?;
        if let Some(value) = &self.detail {
            object.field("detail", value)?;
        }
        if let Some(value) = &self.source_path {
            object.field("sourcePath", value)?;
        }
        Ok(object.finish())
    }

    fn decode_fields(object: &mut RuntimeObjectDecoder<'_>) -> Result<Self, RuntimeCodecError> {
        Ok(Self {
            mime_type: object.required("mimeType", &[])?,
            base64: object.required("base64", &[])?,
            source_path: object.optional("sourcePath", &[])?,
            detail: object.optional("detail", &[])?,
        })
    }
}

impl_runtime_object_codec!(ImageBlock);

impl DocumentBlock {
    fn encode_fields(&self) -> Result<RuntimeObject, RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("mimeType", &self.mime_type)?;
        object.field("base64", &self.base64)?;
        if let Some(value) = &self.filename {
            object.field("filename", value)?;
        }
        Ok(object.finish())
    }

    fn decode_fields(object: &mut RuntimeObjectDecoder<'_>) -> Result<Self, RuntimeCodecError> {
        Ok(Self {
            mime_type: object.required("mimeType", &[])?,
            base64: object.required("base64", &[])?,
            filename: object.optional("filename", &[])?,
        })
    }
}

impl_runtime_object_codec!(DocumentBlock);

impl MediaBlock {
    fn encode_fields(&self) -> Result<RuntimeObject, RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("mimeType", &self.mime_type)?;
        object.field("base64", &self.base64)?;
        if let Some(filename) = &self.filename {
            object.field("filename", filename)?;
        }
        Ok(object.finish())
    }

    fn decode_fields(object: &mut RuntimeObjectDecoder<'_>) -> Result<Self, RuntimeCodecError> {
        Ok(Self {
            mime_type: object.required("mimeType", &[])?,
            base64: object.required("base64", &[])?,
            filename: object.optional("filename", &[])?,
        })
    }
}

impl_runtime_object_codec!(MediaBlock);

impl ToolUseBlock {
    fn encode_fields(&self) -> Result<RuntimeObject, RuntimeCodecError> {
        let Self {
            id,
            name,
            input,
            tool_kind,
            thought_signature,
        } = self;
        let mut object = RuntimeObjectEncoder::new();
        object.field("id", id)?;
        object.field("name", name)?;
        object.field("input", input)?;
        if !ProviderToolCallKind::is_implicit(tool_kind) {
            object.field("toolKind", tool_kind)?;
        }
        if let Some(value) = thought_signature {
            object.field("thoughtSignature", value)?;
        }
        Ok(object.finish())
    }

    fn decode_fields(object: &mut RuntimeObjectDecoder<'_>) -> Result<Self, RuntimeCodecError> {
        Ok(Self {
            id: object.required("id", &[])?,
            name: object.required("name", &[])?,
            input: object.required("input", &[])?,
            tool_kind: object.optional("toolKind", &[])?,
            thought_signature: object.optional("thoughtSignature", &[])?,
        })
    }
}

impl_runtime_object_codec!(ToolUseBlock);

impl ToolResultBlock {
    fn encode_fields(&self) -> Result<RuntimeObject, RuntimeCodecError> {
        let Self {
            tool_use_id,
            content,
            is_error,
        } = self;
        let mut object = RuntimeObjectEncoder::new();
        object.field("toolUseId", tool_use_id)?;
        object.field("content", content)?;
        if let Some(value) = is_error {
            object.field("isError", value)?;
        }
        Ok(object.finish())
    }

    fn decode_fields(object: &mut RuntimeObjectDecoder<'_>) -> Result<Self, RuntimeCodecError> {
        Ok(Self {
            tool_use_id: object.required("toolUseId", &[])?,
            content: object.required("content", &[])?,
            is_error: object.optional("isError", &[])?,
        })
    }
}

impl_runtime_object_codec!(ToolResultBlock);

impl RuntimeEncode for OutputBlock {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let (tag, fields) = match self {
            Self::Text(value) => ("text", value.encode_fields()?),
            Self::Image(value) => ("image", value.encode_fields()?),
        };
        runtime_codec::encode_internal(out, "type", &[], tag, &fields)
    }
}

impl RuntimeDecode for OutputBlock {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_internal(
            value,
            false,
            "type",
            &[],
            &[
                RuntimeEnumVariant::new("text", &[]),
                RuntimeEnumVariant::new("image", &[]),
            ],
            |variant, object| match variant {
                0 => TextBlock::decode_fields(object).map(Self::Text),
                1 => ImageBlock::decode_fields(object).map(Self::Image),
                _ => Err(RuntimeCodecError::new(
                    RuntimeCodecErrorKind::UnknownEnumVariant,
                )),
            },
        )
    }
}

impl RuntimeEncode for ContentBlock {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let (tag, fields) = match self {
            Self::Text(value) => ("text", value.encode_fields()?),
            Self::Thinking(value) => ("thinking", value.encode_fields()?),
            Self::RedactedThinking(value) => ("redacted_thinking", value.encode_fields()?),
            Self::Image(value) => ("image", value.encode_fields()?),
            Self::Document(value) => ("document", value.encode_fields()?),
            Self::Media(value) => ("media", value.encode_fields()?),
            Self::ToolUse(value) => ("tool_use", value.encode_fields()?),
            Self::ToolResult(value) => ("tool_result", value.encode_fields()?),
            Self::WebSearchCall(value) => ("web_search_call", value.encode_fields()?),
        };
        runtime_codec::encode_internal(out, "type", &[], tag, &fields)
    }
}

impl RuntimeDecode for ContentBlock {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_internal(
            value,
            false,
            "type",
            &[],
            &[
                RuntimeEnumVariant::new("text", &[]),
                RuntimeEnumVariant::new("thinking", &[]),
                RuntimeEnumVariant::new("redacted_thinking", &[]),
                RuntimeEnumVariant::new("image", &[]),
                RuntimeEnumVariant::new("document", &[]),
                RuntimeEnumVariant::new("tool_use", &[]),
                RuntimeEnumVariant::new("tool_result", &[]),
                RuntimeEnumVariant::new("web_search_call", &[]),
                RuntimeEnumVariant::new("media", &[]),
            ],
            |variant, object| match variant {
                0 => TextBlock::decode_fields(object).map(Self::Text),
                1 => ThinkingBlock::decode_fields(object).map(Self::Thinking),
                2 => RedactedThinkingBlock::decode_fields(object).map(Self::RedactedThinking),
                3 => ImageBlock::decode_fields(object).map(Self::Image),
                4 => DocumentBlock::decode_fields(object).map(Self::Document),
                5 => ToolUseBlock::decode_fields(object).map(Self::ToolUse),
                6 => ToolResultBlock::decode_fields(object).map(Self::ToolResult),
                7 => WebSearchCallBlock::decode_fields(object).map(Self::WebSearchCall),
                8 => MediaBlock::decode_fields(object).map(Self::Media),
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
    use std::fmt::Debug;

    fn parse(source: &str) -> RuntimeJson {
        crate::runtime_values::runtime_json::parse(&RuntimeString::from(source)).unwrap()
    }

    fn assert_wire_round_trip<T>(source: &str)
    where
        T: RuntimeDecode + RuntimeEncode + PartialEq + Debug,
    {
        let wire = parse(source);
        let decoded = runtime_codec::decode_value::<T>(&wire).unwrap();
        assert_eq!(runtime_codec::encode_value(&decoded).unwrap(), wire);
    }

    #[test]
    fn every_content_variant_preserves_exact_tag_field_names_and_order() {
        for source in [
            r#"{"type":"text","text":"hello","citations":[{"url":"https://example.test","title":"Example","citedText":"quote","startIndex":1,"endIndex":6,"encryptedIndex":"opaque"}],"harnessEnvelope":"system-reminder"}"#,
            r#"{"type":"thinking","text":"summary","signature":"signed","encryptedContent":"opaque"}"#,
            r#"{"type":"redacted_thinking","data":"ciphertext"}"#,
            r#"{"type":"image","mimeType":"image/png","base64":"AAAA"}"#,
            r#"{"type":"document","mimeType":"application/pdf","base64":"JVBERi0=","filename":"notes.pdf"}"#,
            r#"{"type":"tool_use","id":"custom_1","name":"exec","input":"console.log('hi')","toolKind":"custom","thoughtSignature":"signed"}"#,
            r#"{"type":"tool_result","toolUseId":"custom_1","content":[{"type":"text","text":"done"},{"type":"image","mimeType":"image/png","base64":"AAAA"}],"isError":false}"#,
            r#"{"type":"web_search_call","id":"search_1","query":"rust","action":{"type":"find_in_page","url":"https://example.test","pattern":"codec"},"results":[{"title":"Result","url":"https://example.test","encryptedToken":"token","pageAge":"today"}],"error":"partial"}"#,
        ] {
            assert_wire_round_trip::<ContentBlock>(source);
        }
    }

    #[test]
    fn web_search_action_variants_use_internal_snake_case_tags() {
        for source in [
            r#"{"type":"search","query":"rust","queries":["rust codec","exact dto"]}"#,
            r#"{"type":"open_page","url":"https://example.test"}"#,
            r#"{"type":"find_in_page","url":"https://example.test","pattern":"codec"}"#,
            r#"{"type":"other"}"#,
        ] {
            assert_wire_round_trip::<WebSearchAction>(source);
        }
    }

    #[test]
    fn tool_use_skip_default_required_nullable_and_unknown_field_semantics_are_exact() {
        let absent = parse(r#"{"id":"call-1","name":"read","input":null}"#);
        let absent_block = runtime_codec::decode_value::<ToolUseBlock>(&absent).unwrap();
        assert_eq!(absent_block.input, RuntimeJson::Null);
        assert_eq!(runtime_codec::encode_value(&absent_block).unwrap(), absent);

        let explicit_default = parse(
            r#"{"id":"call-1","name":"read","input":null,"toolKind":"function","unknown":true}"#,
        );
        let explicit_default =
            runtime_codec::decode_value::<ToolUseBlock>(&explicit_default).unwrap();
        assert_eq!(
            explicit_default.tool_kind,
            Some(ProviderToolCallKind::Function)
        );
        assert_eq!(
            runtime_codec::encode_value(&explicit_default).unwrap(),
            absent
        );

        let explicit_null = parse(
            r#"{"id":"call-1","name":"read","input":null,"toolKind":null,"thoughtSignature":null}"#,
        );
        let explicit_null = runtime_codec::decode_value::<ToolUseBlock>(&explicit_null).unwrap();
        assert_eq!(explicit_null.tool_kind, None);
        assert_eq!(explicit_null.thought_signature, None);
        assert_eq!(runtime_codec::encode_value(&explicit_null).unwrap(), absent);

        let missing = parse(r#"{"id":"call-1","name":"read"}"#);
        let error = runtime_codec::decode_value::<ToolUseBlock>(&missing).unwrap_err();
        assert_eq!(error.kind(), &RuntimeCodecErrorKind::MissingField);
        assert_eq!(error.pointer(), RuntimeString::from("/input"));
    }

    #[test]
    fn runtime_content_codec_preserves_non_scalar_strings_without_serde() {
        let lone_surrogate = RuntimeString::from_utf16(vec![0xd800]);
        let block = ContentBlock::Text(TextBlock {
            text: lone_surrogate,
            citations: None,
            harness_envelope: None,
            message_id: None,
            phase: None,
        });
        let encoded = runtime_codec::encode_value(&block).unwrap();
        assert_eq!(
            runtime_codec::decode_value::<ContentBlock>(&encoded).unwrap(),
            block
        );
    }
}
