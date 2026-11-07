use crate::runtime_values::{
    ConfigJson, RuntimeCodecError, RuntimeCodecErrorKind, RuntimeDecode, RuntimeEncode,
    RuntimeEncoder, RuntimeJson, RuntimeString,
};
use serde::{Deserialize, Serialize};

pub const CLASSIFICATION_OUTPUT_INSTRUCTIONS: &str = "Your first output token is the entire classification: `high` for high risk or `low` for low risk. Output that token immediately and nothing else.";

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AsyncClassifierMode {
    #[default]
    Snapshot,
    Conversation,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TranscriptFormat {
    #[default]
    Line,
    Json,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TranscriptSource {
    ToolCalls,
    ToolOutputs,
    Reasoning,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TranscriptOptions {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sources: Option<Vec<TranscriptSource>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub include_images: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_message_entry_tokens: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_tool_entry_tokens: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_message_transcript_tokens: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_tool_transcript_tokens: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_recent_non_user_entries: Option<usize>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LocalReviewScope {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub computer_use_only: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sandboxed_exec_commands: Option<bool>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AsyncLocalConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transcript_mode: Option<TranscriptFormat>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub async_classifier_mode: Option<AsyncClassifierMode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub async_classifier_conversation_token_limit: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    /// Deprecated compatibility input. The host emits a warning when supplied.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub free_guardian: Option<bool>,
    /// Deprecated compatibility input. The host emits a warning when supplied.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_context: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub persist_scores: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub classifier_instructions: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub review_threshold: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_tool_call_lag: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_action_tokens: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_classifier_instruction_tokens: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reuse_parent_compaction: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_parent_compaction_tokens: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub review_scope: Option<LocalReviewScope>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transcript: Option<TranscriptOptions>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(untagged)]
pub enum AsyncFeatureConfig {
    Enabled(bool),
    Config(Box<AsyncLocalConfig>),
}

impl<'de> Deserialize<'de> for AsyncFeatureConfig {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct FeatureVisitor;

        impl<'de> serde::de::Visitor<'de> for FeatureVisitor {
            type Value = AsyncFeatureConfig;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a boolean or an approval review configuration object")
            }

            fn visit_bool<E: serde::de::Error>(self, value: bool) -> Result<Self::Value, E> {
                Ok(AsyncFeatureConfig::Enabled(value))
            }

            fn visit_map<M: serde::de::MapAccess<'de>>(
                self,
                map: M,
            ) -> Result<Self::Value, M::Error> {
                // Decode the map directly so numeric fields keep the format's native
                // typed number deserialization rather than an untagged buffer.
                let value = AsyncLocalConfig::deserialize(
                    serde::de::value::MapAccessDeserializer::new(map),
                )?;
                AsyncReviewConfig::resolve(value.clone()).map_err(serde::de::Error::custom)?;
                Ok(AsyncFeatureConfig::Config(Box::new(value)))
            }
        }

        deserializer.deserialize_any(FeatureVisitor)
    }
}

impl RuntimeEncode for AsyncFeatureConfig {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        ConfigJson::serialize_config(self)?.encode_runtime(out)
    }
}

impl RuntimeDecode for AsyncFeatureConfig {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_config(value)
    }
}

pub(super) fn decode_config<T: serde::de::DeserializeOwned>(
    value: &RuntimeJson,
) -> Result<T, RuntimeCodecError> {
    ConfigJson::decode_runtime_document(value)?
        .deserialize_config()
        .map_err(|error| {
            RuntimeCodecError::new(RuntimeCodecErrorKind::Custom(RuntimeString::from(
                error.to_string(),
            )))
        })
}

impl AsyncFeatureConfig {
    pub fn enabled(&self) -> Option<bool> {
        match self {
            Self::Enabled(enabled) => Some(*enabled),
            Self::Config(config) => config.enabled,
        }
    }

    pub fn local_config(&self) -> Option<&AsyncLocalConfig> {
        match self {
            Self::Enabled(_) => None,
            Self::Config(config) => Some(config),
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct AsyncTranscriptModelConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sources: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub include_images: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_message_entry_tokens: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_tool_entry_tokens: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_message_transcript_tokens: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_tool_transcript_tokens: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_recent_non_user_entries: Option<usize>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct AsyncModelConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub async_classifier_mode: Option<AsyncClassifierMode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub async_classifier_conversation_token_limit: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub classifier_instructions: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub review_threshold_basis_points: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_tool_call_lag: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transcript: Option<AsyncTranscriptModelConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_action_tokens: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_classifier_instruction_tokens: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reuse_parent_compaction: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_parent_compaction_tokens: Option<usize>,
}

impl RuntimeEncode for AsyncModelConfig {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        ConfigJson::serialize_config(self)?.encode_runtime(out)
    }
}

impl RuntimeDecode for AsyncModelConfig {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_config(value)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AsyncTranscriptConfig {
    pub format: TranscriptFormat,
    pub sources: Vec<TranscriptSource>,
    pub include_images: bool,
    pub max_message_entry_tokens: usize,
    pub max_tool_entry_tokens: usize,
    pub max_message_transcript_tokens: usize,
    pub max_tool_transcript_tokens: usize,
    pub max_recent_non_user_entries: usize,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AsyncReviewConfig {
    local: AsyncLocalConfig,
    pub persist_scores: bool,
    pub classifier_mode: AsyncClassifierMode,
    /// None selects the complete bundled classifier instructions.
    pub classifier_instructions: Option<String>,
    pub review_threshold: f64,
    pub max_tool_call_lag: usize,
    pub async_classifier_conversation_token_limit: usize,
    pub reasoning_effort: String,
    pub max_action_tokens: usize,
    pub max_classifier_instruction_tokens: Option<usize>,
    pub reuse_parent_compaction: bool,
    pub max_parent_compaction_tokens: usize,
    pub transcript: AsyncTranscriptConfig,
}

impl AsyncReviewConfig {
    pub fn resolve(local: AsyncLocalConfig) -> Result<Self, String> {
        Self::resolve_effective(local.clone(), local)
    }

    pub fn with_model_defaults(&self, defaults: Option<&AsyncModelConfig>) -> Result<Self, String> {
        let mut local = self.local.clone();
        if let Some(model) = defaults {
            local.async_classifier_mode =
                local.async_classifier_mode.or(model.async_classifier_mode);
            local.async_classifier_conversation_token_limit = local
                .async_classifier_conversation_token_limit
                .or(model.async_classifier_conversation_token_limit);
            local.classifier_instructions = local
                .classifier_instructions
                .or_else(|| model.classifier_instructions.clone());
            local.review_threshold = local.review_threshold.or_else(|| {
                model
                    .review_threshold_basis_points
                    .map(|basis_points| f64::from(basis_points) / 10_000.0)
            });
            local.max_tool_call_lag = local.max_tool_call_lag.or(model.max_tool_call_lag);
            local.reasoning_effort = local
                .reasoning_effort
                .or_else(|| model.reasoning_effort.clone());
            local.max_action_tokens = local.max_action_tokens.or(model.max_action_tokens);
            local.max_classifier_instruction_tokens = local
                .max_classifier_instruction_tokens
                .or(model.max_classifier_instruction_tokens);
            local.reuse_parent_compaction = local
                .reuse_parent_compaction
                .or(model.reuse_parent_compaction);
            local.max_parent_compaction_tokens = local
                .max_parent_compaction_tokens
                .or(model.max_parent_compaction_tokens);
            if let Some(model) = &model.transcript {
                let transcript = local
                    .transcript
                    .get_or_insert_with(TranscriptOptions::default);
                if transcript.sources.is_none() {
                    transcript.sources = model
                        .sources
                        .as_ref()
                        .map(|sources| {
                            sources
                                .iter()
                                .map(|source| match source.as_str() {
                                    "tool_calls" => Ok(TranscriptSource::ToolCalls),
                                    "tool_outputs" => Ok(TranscriptSource::ToolOutputs),
                                    "reasoning" => Ok(TranscriptSource::Reasoning),
                                    _ => Err(format!("invalid review transcript source: {source}")),
                                })
                                .collect::<Result<_, _>>()
                        })
                        .transpose()?;
                }
                transcript.include_images = transcript.include_images.or(model.include_images);
                transcript.max_message_entry_tokens = transcript
                    .max_message_entry_tokens
                    .or(model.max_message_entry_tokens);
                transcript.max_tool_entry_tokens = transcript
                    .max_tool_entry_tokens
                    .or(model.max_tool_entry_tokens);
                transcript.max_message_transcript_tokens = transcript
                    .max_message_transcript_tokens
                    .or(model.max_message_transcript_tokens);
                transcript.max_tool_transcript_tokens = transcript
                    .max_tool_transcript_tokens
                    .or(model.max_tool_transcript_tokens);
                transcript.max_recent_non_user_entries = transcript
                    .max_recent_non_user_entries
                    .or(model.max_recent_non_user_entries);
            }
        }
        Self::resolve_effective(local, self.local.clone())
    }

    fn resolve_effective(
        config: AsyncLocalConfig,
        local: AsyncLocalConfig,
    ) -> Result<Self, String> {
        let limit = config
            .async_classifier_conversation_token_limit
            .unwrap_or(100_000);
        if limit == 0 {
            return Err("async_classifier_conversation_token_limit must be positive".into());
        }
        let threshold = config.review_threshold.unwrap_or_else(|| {
            if config.classifier_instructions.is_some() {
                0.8
            } else {
                0.5
            }
        });
        if !threshold.is_finite() || !(0.0..=1.0).contains(&threshold) {
            return Err("review_threshold must be between 0.0 and 1.0".into());
        }
        let transcript = config.transcript.as_ref();
        let message_entry = bounded_tokens(
            transcript.and_then(|t| t.max_message_entry_tokens),
            2_000,
            "max_message_entry_tokens",
        )?;
        let tool_entry = bounded_tokens(
            transcript.and_then(|t| t.max_tool_entry_tokens),
            1_000,
            "max_tool_entry_tokens",
        )?;
        let message_total = bounded_tokens(
            transcript.and_then(|t| t.max_message_transcript_tokens),
            10_000,
            "max_message_transcript_tokens",
        )?;
        let tool_total = bounded_tokens(
            transcript.and_then(|t| t.max_tool_transcript_tokens),
            10_000,
            "max_tool_transcript_tokens",
        )?;
        if message_entry > message_total || tool_entry > tool_total {
            return Err(
                "transcript entry token allowance must not exceed its transcript allowance".into(),
            );
        }
        let recent = transcript
            .and_then(|t| t.max_recent_non_user_entries)
            .unwrap_or(40);
        if recent == 0 {
            return Err("max_recent_non_user_entries must be positive".into());
        }
        let effort = config.reasoning_effort.as_deref().unwrap_or("low");
        Ok(Self {
            local,
            persist_scores: config.persist_scores.unwrap_or(false),
            classifier_mode: config.async_classifier_mode.unwrap_or_default(),
            review_threshold: threshold,
            max_tool_call_lag: config.max_tool_call_lag.unwrap_or(2),
            async_classifier_conversation_token_limit: limit,
            reasoning_effort: effort.to_owned(),
            max_action_tokens: bounded_tokens(
                config.max_action_tokens,
                10_000,
                "max_action_tokens",
            )?,
            max_classifier_instruction_tokens: config
                .max_classifier_instruction_tokens
                .map(|tokens| {
                    bounded_tokens(Some(tokens), tokens, "max_classifier_instruction_tokens")
                })
                .transpose()?,
            reuse_parent_compaction: config.reuse_parent_compaction.unwrap_or(true),
            max_parent_compaction_tokens: bounded_tokens(
                config.max_parent_compaction_tokens,
                25_000,
                "max_parent_compaction_tokens",
            )?,
            classifier_instructions: config.classifier_instructions,
            transcript: AsyncTranscriptConfig {
                format: config.transcript_mode.unwrap_or_default(),
                sources: transcript
                    .and_then(|t| t.sources.clone())
                    .unwrap_or_else(|| {
                        vec![TranscriptSource::ToolCalls, TranscriptSource::ToolOutputs]
                    }),
                include_images: transcript.and_then(|t| t.include_images).unwrap_or(true),
                max_message_entry_tokens: message_entry,
                max_tool_entry_tokens: tool_entry,
                max_message_transcript_tokens: message_total,
                max_tool_transcript_tokens: tool_total,
                max_recent_non_user_entries: recent,
            },
        })
    }

    pub fn deprecation_warnings(&self) -> impl Iterator<Item = &'static str> {
        [
            self.local
                .free_guardian
                .map(|_| "free_guardian is deprecated; review billing is provider controlled"),
            self.local.thread_context.map(|_| {
                "thread_context is deprecated; thread-owned review context is always enabled"
            }),
        ]
        .into_iter()
        .flatten()
    }
}

fn bounded_tokens(value: Option<usize>, default: usize, setting: &str) -> Result<usize, String> {
    let value = value.unwrap_or(default);
    if !(100..=100_000).contains(&value) {
        return Err(format!("{setting} must be between 100 and 100000 tokens"));
    }
    Ok(value)
}
