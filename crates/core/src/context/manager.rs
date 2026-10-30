use crate::provider::behavior::{
    render_provider_text_envelope, ProviderBehaviorProfile, ProviderTextEnvelopeKind,
};
use crate::runtime_values::runtime_codec::{RuntimeEnumVariant, RuntimeExternalVariant};
use crate::runtime_values::{
    runtime_codec, RuntimeCodecError, RuntimeCodecErrorKind, RuntimeDecode, RuntimeEncode,
    RuntimeEncoder, RuntimeJson, RuntimeObjectEncoder, RuntimeString,
};
use crate::schemas::content::{ContentBlock, HarnessEnvelope, TextBlock};
use crate::schemas::message::{Message, UserMessage};
use crate::types::provider::{CacheScope, SystemPromptPart};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

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

/// Deterministic context-fragment assembly for system prompt and user-context
/// reinsertion.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ContextManager {
    fragments: BTreeMap<RuntimeString, ContextFragment>,
    version: u64,
}

/// One named context fragment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextFragment {
    pub id: RuntimeString,
    pub placement: ContextFragmentPlacement,
    pub phase: ContextFragmentPhase,
    pub order: i32,
    pub text: RuntimeString,
    pub cacheable: Option<bool>,
    pub cache_scope: Option<CacheScope>,
    pub max_chars: Option<usize>,
    pub wrap_in_system_reminder: bool,
}

impl ContextFragment {
    pub fn system(
        id: impl Into<RuntimeString>,
        phase: ContextFragmentPhase,
        order: i32,
        text: impl Into<RuntimeString>,
    ) -> Self {
        Self {
            id: id.into(),
            placement: ContextFragmentPlacement::System,
            phase,
            order,
            text: text.into(),
            cacheable: None,
            cache_scope: None,
            max_chars: None,
            wrap_in_system_reminder: false,
        }
    }

    pub fn user_reinsertion(
        id: impl Into<RuntimeString>,
        order: i32,
        text: impl Into<RuntimeString>,
    ) -> Self {
        Self {
            id: id.into(),
            placement: ContextFragmentPlacement::User,
            phase: ContextFragmentPhase::Reinsertion,
            order,
            text: text.into(),
            cacheable: None,
            cache_scope: None,
            max_chars: None,
            wrap_in_system_reminder: true,
        }
    }

    pub fn with_cacheable(mut self, cacheable: bool) -> Self {
        self.cacheable = Some(cacheable);
        self
    }

    pub fn with_cache_scope(mut self, cache_scope: CacheScope) -> Self {
        self.cache_scope = Some(cache_scope);
        self
    }

    pub fn with_max_chars(mut self, max_chars: usize) -> Self {
        self.max_chars = Some(max_chars);
        self
    }

    fn render_text(&self) -> RuntimeString {
        // System parts remain canonical; user reinsertions are provider-rendered
        // later by `render_messages`.
        truncate_chars(&self.text, self.max_chars)
    }
}

impl RuntimeEncode for ContextFragment {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("id", &self.id)?;
        object.field("placement", &self.placement)?;
        object.field("phase", &self.phase)?;
        object.field("order", &self.order)?;
        object.field("text", &self.text)?;
        if let Some(value) = &self.cacheable {
            object.field("cacheable", value)?;
        }
        if let Some(value) = &self.cache_scope {
            object.field("cacheScope", value)?;
        }
        if let Some(value) = &self.max_chars {
            object.field("maxChars", value)?;
        }
        object.field("wrapInSystemReminder", &self.wrap_in_system_reminder)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for ContextFragment {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                id: object.required("id", &[])?,
                placement: object.required("placement", &[])?,
                phase: object.required("phase", &[])?,
                order: object.required("order", &[])?,
                text: object.required("text", &[])?,
                cacheable: object.optional("cacheable", &[])?,
                cache_scope: object.optional("cacheScope", &[])?,
                max_chars: object.optional("maxChars", &[])?,
                wrap_in_system_reminder: object.required("wrapInSystemReminder", &[])?,
            })
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ContextFragmentPlacement {
    System,
    User,
}

impl RuntimeEncode for ContextFragmentPlacement {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::System => "system",
            Self::User => "user",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for ContextFragmentPlacement {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("system", &[]),
                RuntimeEnumVariant::new("user", &[]),
            ],
            &[Self::System, Self::User],
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ContextFragmentPhase {
    Base,
    Environment,
    Memory,
    Reminder,
    Reinsertion,
}

impl RuntimeEncode for ContextFragmentPhase {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Base => "base",
            Self::Environment => "environment",
            Self::Memory => "memory",
            Self::Reminder => "reminder",
            Self::Reinsertion => "reinsertion",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for ContextFragmentPhase {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("base", &[]),
                RuntimeEnumVariant::new("environment", &[]),
                RuntimeEnumVariant::new("memory", &[]),
                RuntimeEnumVariant::new("reminder", &[]),
                RuntimeEnumVariant::new("reinsertion", &[]),
            ],
            &[
                Self::Base,
                Self::Environment,
                Self::Memory,
                Self::Reminder,
                Self::Reinsertion,
            ],
        )
    }
}

impl ContextManager {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn version(&self) -> u64 {
        self.version
    }

    pub fn upsert(&mut self, fragment: ContextFragment) -> Option<ContextFragment> {
        let previous = self.fragments.insert(fragment.id.clone(), fragment);
        self.version = self.version.saturating_add(1);
        previous
    }

    pub fn remove(&mut self, id: &str) -> Option<ContextFragment> {
        let removed = self.fragments.remove(&RuntimeString::from(id));
        if removed.is_some() {
            self.version = self.version.saturating_add(1);
        }
        removed
    }

    pub fn render_system_parts(&self) -> Vec<SystemPromptPart> {
        self.ordered_fragments(ContextFragmentPlacement::System)
            .into_iter()
            .map(|fragment| SystemPromptPart {
                name: fragment.id.clone(),
                text: fragment.render_text(),
                cacheable: fragment.cacheable,
                cache_scope: fragment.cache_scope,
            })
            .collect()
    }

    pub fn render_messages(
        &self,
        messages: Vec<Message>,
        uuid: &dyn Fn() -> String,
        profile: &ProviderBehaviorProfile,
    ) -> Vec<Message> {
        let blocks: Vec<ContentBlock> = self
            .ordered_fragments(ContextFragmentPlacement::User)
            .into_iter()
            .map(|fragment| {
                ContentBlock::Text(TextBlock {
                    text: if fragment.wrap_in_system_reminder {
                        render_provider_text_envelope(
                            ProviderTextEnvelopeKind::SystemReminder,
                            truncate_chars(&fragment.text, fragment.max_chars),
                            profile,
                            Default::default(),
                        )
                    } else {
                        truncate_chars(&fragment.text, fragment.max_chars)
                    },
                    citations: None,
                    harness_envelope: fragment
                        .wrap_in_system_reminder
                        .then_some(HarnessEnvelope::SystemReminder),
                    message_id: None,
                    phase: None,
                })
            })
            .collect();
        if blocks.is_empty() {
            return messages;
        }

        let mut messages = messages;
        if let Some(Message::User(last)) = messages.last_mut() {
            last.content.extend(blocks);
            return messages;
        }

        messages.push(Message::User(UserMessage {
            question_answer: None,
            uuid: uuid().into(),
            content: blocks,
            is_meta: Some(true),
        }));
        messages
    }

    pub fn render_after_compaction(
        &self,
        replacement_history: Vec<Message>,
        uuid: &dyn Fn() -> String,
        profile: &ProviderBehaviorProfile,
    ) -> Vec<Message> {
        let mut replacement = self.render_messages(replacement_history, uuid, profile);
        crate::context::compact::full_compact::place_retained_context(&mut replacement);
        replacement
    }

    fn ordered_fragments(&self, placement: ContextFragmentPlacement) -> Vec<&ContextFragment> {
        let mut fragments: Vec<&ContextFragment> = self
            .fragments
            .values()
            .filter(|fragment| fragment.placement == placement)
            .collect();
        fragments.sort_by(|left, right| {
            (left.phase, left.order, &left.id).cmp(&(right.phase, right.order, &right.id))
        });
        fragments
    }
}

fn truncate_chars(text: &RuntimeString, max_chars: Option<usize>) -> RuntimeString {
    let Some(max_chars) = max_chars else {
        return text.clone();
    };
    if text.len_utf16() <= max_chars {
        return text.clone();
    }
    text.slice_utf16(..max_chars)
        .concat(&RuntimeString::from("... [truncated]"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::behavior::resolve_provider_behavior_profile;
    use crate::schemas::content::{ContentBlock, OutputBlock, TextBlock};
    use crate::schemas::message::{
        CompactBoundaryMessage, Message, ToolResultMessage, UserMessage,
    };
    use crate::types::provider::ProviderInfo;

    fn provider(name: &str) -> ProviderInfo {
        ProviderInfo {
            name: name.into(),
            base_url: None,
            env_key: None,
            default_model: "unused".into(),
        }
    }

    fn anthropic_profile() -> ProviderBehaviorProfile {
        resolve_provider_behavior_profile(
            Some(&provider("anthropic")),
            &RuntimeString::from("claude-sonnet-4-6"),
        )
    }

    fn openai_profile() -> ProviderBehaviorProfile {
        resolve_provider_behavior_profile(Some(&provider("openai")), &RuntimeString::from("gpt-5"))
    }

    fn qwen_profile() -> ProviderBehaviorProfile {
        resolve_provider_behavior_profile(
            Some(&provider("openai")),
            &RuntimeString::from("qwen-max"),
        )
    }

    fn text_of(block: &ContentBlock) -> &RuntimeString {
        match block {
            ContentBlock::Text(text) => &text.text,
            _ => panic!("expected text block"),
        }
    }

    fn user(id: &str, text: &str) -> Message {
        Message::User(UserMessage {
            question_answer: None,
            uuid: id.into(),
            content: vec![ContentBlock::Text(TextBlock {
                text: text.into(),
                citations: None,
                harness_envelope: None,
                message_id: None,
                phase: None,
            })],
            is_meta: None,
        })
    }

    fn compact(id: &str, summary: &str) -> Message {
        Message::CompactBoundary(CompactBoundaryMessage {
            native_compaction: None,
            uuid: id.into(),
            summary: summary.into(),
        })
    }

    fn tool_result(id: &str) -> Message {
        Message::ToolResult(ToolResultMessage {
            uuid: id.into(),
            tool_use_id: format!("tool-{id}").into(),
            content: vec![OutputBlock::Text(TextBlock {
                text: "tool output".into(),
                citations: None,
                harness_envelope: None,
                message_id: None,
                phase: None,
            })],
            is_error: false,
            tool_kind: None,
            native_tool_output: None,
            output: None,
            extra_user_blocks: None,
        })
    }

    #[test]
    fn system_fragments_render_in_deterministic_order() {
        let mut manager = ContextManager::new();
        manager.upsert(ContextFragment::system(
            "memory-b",
            ContextFragmentPhase::Memory,
            20,
            "memory b",
        ));
        manager.upsert(ContextFragment::system(
            "base",
            ContextFragmentPhase::Base,
            10,
            "base",
        ));
        manager.upsert(ContextFragment::system(
            "memory-a",
            ContextFragmentPhase::Memory,
            10,
            "memory a",
        ));
        manager.upsert(ContextFragment::system(
            "env",
            ContextFragmentPhase::Environment,
            10,
            "env",
        ));

        let rendered = manager.render_system_parts();

        assert_eq!(
            rendered
                .iter()
                .map(|part| part.name.clone())
                .collect::<Vec<_>>(),
            vec!["base", "env", "memory-a", "memory-b"]
        );
        assert_eq!(
            rendered
                .iter()
                .map(|part| part.text.clone())
                .collect::<Vec<_>>(),
            vec!["base", "env", "memory a", "memory b"]
        );
    }

    #[test]
    fn fragment_caps_truncate_text_before_rendering() {
        let mut manager = ContextManager::new();
        manager.upsert(
            ContextFragment::system(
                "large",
                ContextFragmentPhase::Memory,
                10,
                "abcdefghijklmnopqrstuvwxyz",
            )
            .with_max_chars(6),
        );

        let rendered = manager.render_system_parts();

        assert_eq!(rendered[0].text, "abcdef... [truncated]");
    }

    #[test]
    fn reinsertion_after_compaction_preserves_history_and_adds_context() {
        let mut manager = ContextManager::new();
        manager.upsert(ContextFragment::user_reinsertion(
            "running-task",
            10,
            "Background task bg-1 is still running.",
        ));
        let replacement_history = vec![compact("compact-1", "older work summary")];

        let rendered = manager.render_after_compaction(
            replacement_history.clone(),
            &|| "ctx-1".to_string(),
            &anthropic_profile(),
        );

        assert_eq!(rendered.len(), 2);
        assert_eq!(rendered[1], replacement_history[0]);
        let Message::User(user) = &rendered[0] else {
            panic!("expected reinsertion user message");
        };
        assert_eq!(user.uuid, "ctx-1");
        assert_eq!(user.is_meta, Some(true));
        assert_eq!(
            text_of(&user.content[0]),
            "<system-reminder>\nBackground task bg-1 is still running.\n</system-reminder>"
        );
    }

    #[test]
    fn reinsertion_after_tool_result_creates_meta_user_message() {
        let mut manager = ContextManager::new();
        manager.upsert(ContextFragment::user_reinsertion(
            "surfaced-tools",
            10,
            "Previously surfaced tools remain available.",
        ));
        let history = vec![user("u1", "run it"), tool_result("tr1")];

        let rendered = manager.render_messages(
            history.clone(),
            &|| "ctx-2".to_string(),
            &anthropic_profile(),
        );

        assert_eq!(rendered.len(), 3);
        assert_eq!(rendered[0], history[0]);
        assert_eq!(rendered[1], history[1]);
        let Message::User(user) = &rendered[2] else {
            panic!("expected meta user message");
        };
        assert_eq!(user.uuid, "ctx-2");
        assert_eq!(user.is_meta, Some(true));
        assert_eq!(
            text_of(&user.content[0]),
            "<system-reminder>\nPreviously surfaced tools remain available.\n</system-reminder>"
        );
    }

    #[test]
    fn reinsertion_into_trailing_user_preserves_fragment_order() {
        let mut manager = ContextManager::new();
        manager.upsert(ContextFragment::user_reinsertion(
            "second",
            20,
            "second note",
        ));
        manager.upsert(ContextFragment::user_reinsertion("first", 10, "first note"));
        let history = vec![user("u1", "question")];

        let rendered =
            manager.render_messages(history, &|| "unused".to_string(), &anthropic_profile());

        let Message::User(user) = &rendered[0] else {
            panic!("expected original user message");
        };
        assert_eq!(user.uuid, "u1");
        assert_eq!(user.is_meta, None);
        assert_eq!(user.content.len(), 3);
        assert_eq!(text_of(&user.content[0]), "question");
        assert_eq!(
            text_of(&user.content[1]),
            "<system-reminder>\nfirst note\n</system-reminder>"
        );
        assert_eq!(
            text_of(&user.content[2]),
            "<system-reminder>\nsecond note\n</system-reminder>"
        );
    }

    fn inserted_text(profile: &ProviderBehaviorProfile) -> RuntimeString {
        let mut manager = ContextManager::new();
        manager.upsert(ContextFragment::user_reinsertion(
            "test-reminder",
            10,
            "remember this",
        ));
        let messages = manager.render_messages(Vec::new(), &|| "ctx-1".to_string(), profile);
        let Message::User(user) = &messages[0] else {
            panic!("expected user reinsertion");
        };
        let ContentBlock::Text(text) = &user.content[0] else {
            panic!("expected text reinsertion");
        };
        text.text.clone()
    }

    #[test]
    fn reinsertion_uses_the_selected_provider_envelope() {
        assert_eq!(
            inserted_text(&anthropic_profile()),
            "<system-reminder>\nremember this\n</system-reminder>"
        );
        assert_eq!(
            inserted_text(&openai_profile()),
            "# System Reminder\n\nremember this"
        );
        assert_eq!(inserted_text(&qwen_profile()), "remember this");
    }
}
