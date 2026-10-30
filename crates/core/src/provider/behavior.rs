//! Pure provider behavior selection and schema capability metadata.

use crate::runtime_values::{
    runtime_json, RuntimeJson, RuntimeNumber, RuntimeObject, RuntimeString,
};
use crate::schemas::content::{HarnessEnvelope, OutputBlock, TextBlock};
use crate::schemas::subagent_message::message_preview;
use crate::types::json::Json;
use crate::types::provider::{
    ProviderInfo, SystemPromptPart, ToolSpec, ToolSpecFormat, ToolSpecGrammarSyntax, ToolSpecKind,
};

macro_rules! exact_value {
    (null) => {
        RuntimeJson::Null
    };
    ([ $( $element:tt ),* $(,)? ]) => {
        RuntimeJson::Array(vec![$(exact_value!($element)),*])
    };
    ({ $( $key:literal : $value:tt ),* $(,)? }) => {{
        #[allow(unused_mut)]
        let mut object = RuntimeObject::new();
        $(object.insert($key, exact_value!($value));)*
        RuntimeJson::Object(object)
    }};
    ($value:expr) => {
        RuntimeJson::from($value)
    };
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderPromptStyle {
    AnthropicReminder,
    OpenAiDeveloper,
    Generic,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderToolDescriptionStyle {
    Directive,
    SchemaFirst,
    Compact,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderReasoningReplayMode {
    AnthropicSignedThinking,
    OpenAiEncryptedReasoning,
    None,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderBehaviorProfile {
    pub provider: RuntimeString,
    pub family: Option<&'static str>,
    pub prompt_style: ProviderPromptStyle,
    pub tool_description_style: ProviderToolDescriptionStyle,
    pub prefers_explicit_parallelism_hint: bool,
    pub prefers_explicit_delegation_criteria: bool,
    pub reasoning_replay: ProviderReasoningReplayMode,
    pub reasoning_hint: Option<&'static str>,
    /// Whether this provider supports namespaced tool names and the `tool_search`
    /// mechanism. When true, MCP tools default to `Deferred` exposure and a
    /// `tool_search` tool is injected; when false, all tools are `Direct`.
    pub namespace_tools: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderTextEnvelopeKind {
    SystemReminder,
    ProviderBehavior,
    TeammateContext,
    CompactBoundary,
    ToolError,
    SubagentReport,
    CommandOutput,
    DiffOutput,
    TruncatedToolOutput,
    ContextBudgetTrim,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderToolResultEnvelopeKind {
    PlainText,
    ToolError,
    SubagentReport,
    CommandOutput,
    DiffOutput,
}

#[derive(Debug, Clone, Default)]
pub struct ProviderTextEnvelopeOptions {
    pub tool_name: Option<RuntimeString>,
}

fn concat_runtime_strings(parts: &[RuntimeString]) -> RuntimeString {
    parts
        .iter()
        .fold(RuntimeString::default(), |joined, part| joined.concat(part))
}

fn join_runtime_strings(values: &[RuntimeString], separator: &str) -> RuntimeString {
    let separator = RuntimeString::from(separator);
    let mut joined = RuntimeString::default();
    for (index, value) in values.iter().enumerate() {
        if index != 0 {
            joined = joined.concat(&separator);
        }
        joined = joined.concat(value);
    }
    joined
}

fn wrap_runtime_string(prefix: &str, text: &RuntimeString, suffix: &str) -> RuntimeString {
    concat_runtime_strings(&[prefix.into(), text.clone(), suffix.into()])
}

/// Named scalar projection used only for provider/model classification. The
/// authoritative runtime carrier is never rewritten with this projected key.
fn runtime_classification_key(value: &RuntimeString) -> String {
    value.try_to_utf8().unwrap_or_else(|_| {
        String::from_utf8(value.to_node_utf8()).expect("Node UTF-8 projection is valid UTF-8")
    })
}

pub fn render_provider_text_envelope(
    kind: ProviderTextEnvelopeKind,
    text: RuntimeString,
    profile: &ProviderBehaviorProfile,
    options: ProviderTextEnvelopeOptions,
) -> RuntimeString {
    match kind {
        ProviderTextEnvelopeKind::SystemReminder => match profile.prompt_style {
            ProviderPromptStyle::AnthropicReminder => {
                wrap_runtime_string("<system-reminder>\n", &text, "\n</system-reminder>")
            }
            ProviderPromptStyle::OpenAiDeveloper => {
                wrap_runtime_string("# System Reminder\n\n", &text, "")
            }
            ProviderPromptStyle::Generic => text,
        },
        ProviderTextEnvelopeKind::ProviderBehavior => match profile.prompt_style {
            ProviderPromptStyle::AnthropicReminder => wrap_runtime_string(
                "<system-reminder>\nHarness guidance:\n",
                &text,
                "\n</system-reminder>",
            ),
            ProviderPromptStyle::OpenAiDeveloper => {
                wrap_runtime_string("# Provider Behavior\n\n", &text, "")
            }
            ProviderPromptStyle::Generic => text,
        },
        ProviderTextEnvelopeKind::TeammateContext => match profile.prompt_style {
            ProviderPromptStyle::AnthropicReminder => render_provider_text_envelope(
                ProviderTextEnvelopeKind::SystemReminder,
                text,
                profile,
                options,
            ),
            ProviderPromptStyle::OpenAiDeveloper => {
                wrap_runtime_string("# Teammate Context\n\n", &text, "")
            }
            ProviderPromptStyle::Generic => text,
        },
        ProviderTextEnvelopeKind::CompactBoundary => match profile.prompt_style {
            ProviderPromptStyle::AnthropicReminder => render_provider_text_envelope(
                ProviderTextEnvelopeKind::SystemReminder,
                wrap_runtime_string("Earlier conversation summary:\n", &text, ""),
                profile,
                options,
            ),
            ProviderPromptStyle::OpenAiDeveloper => {
                wrap_runtime_string("# Earlier Conversation Summary\n\n", &text, "")
            }
            ProviderPromptStyle::Generic => {
                wrap_runtime_string("[Earlier conversation summarized]\n", &text, "")
            }
        },
        ProviderTextEnvelopeKind::ToolError => match profile.prompt_style {
            ProviderPromptStyle::AnthropicReminder => concat_runtime_strings(&[
                "<tool_use_error tool=\"".into(),
                options.tool_name.unwrap_or_default(),
                "\">\n".into(),
                text,
                "\n</tool_use_error>".into(),
            ]),
            ProviderPromptStyle::OpenAiDeveloper => concat_runtime_strings(&[
                "Tool error from `".into(),
                options.tool_name.unwrap_or_default(),
                "`:\n".into(),
                text,
            ]),
            ProviderPromptStyle::Generic => text,
        },
        ProviderTextEnvelopeKind::SubagentReport => match profile.prompt_style {
            ProviderPromptStyle::AnthropicReminder => concat_runtime_strings(&[
                "<subagent_report tool=\"".into(),
                options.tool_name.unwrap_or_default(),
                "\">\n".into(),
                text,
                "\n</subagent_report>".into(),
            ]),
            ProviderPromptStyle::OpenAiDeveloper => {
                wrap_runtime_string("# Sub-Agent Report\n\n", &text, "")
            }
            ProviderPromptStyle::Generic => text,
        },
        ProviderTextEnvelopeKind::CommandOutput => match profile.prompt_style {
            ProviderPromptStyle::AnthropicReminder => concat_runtime_strings(&[
                "<command_output tool=\"".into(),
                options.tool_name.unwrap_or_default(),
                "\">\n".into(),
                text,
                "\n</command_output>".into(),
            ]),
            ProviderPromptStyle::OpenAiDeveloper => {
                wrap_runtime_string("# Command Output\n\n", &text, "")
            }
            ProviderPromptStyle::Generic => text,
        },
        ProviderTextEnvelopeKind::DiffOutput => match profile.prompt_style {
            ProviderPromptStyle::AnthropicReminder => concat_runtime_strings(&[
                "<diff_output tool=\"".into(),
                options.tool_name.unwrap_or_default(),
                "\">\n".into(),
                text,
                "\n</diff_output>".into(),
            ]),
            ProviderPromptStyle::OpenAiDeveloper => {
                wrap_runtime_string("# Diff Output\n\n", &text, "")
            }
            ProviderPromptStyle::Generic => text,
        },
        ProviderTextEnvelopeKind::TruncatedToolOutput => match profile.prompt_style {
            ProviderPromptStyle::OpenAiDeveloper => {
                wrap_runtime_string("# Truncated Tool Output\n\n", &text, "")
            }
            ProviderPromptStyle::AnthropicReminder | ProviderPromptStyle::Generic => {
                render_provider_text_envelope(
                    ProviderTextEnvelopeKind::SystemReminder,
                    text,
                    profile,
                    options,
                )
            }
        },
        ProviderTextEnvelopeKind::ContextBudgetTrim => match profile.prompt_style {
            ProviderPromptStyle::OpenAiDeveloper => {
                wrap_runtime_string("# Context-Budget Trim\n\n", &text, "")
            }
            ProviderPromptStyle::AnthropicReminder | ProviderPromptStyle::Generic => {
                render_provider_text_envelope(
                    ProviderTextEnvelopeKind::SystemReminder,
                    text,
                    profile,
                    options,
                )
            }
        },
    }
}

pub fn resolve_tool_result_envelope_kind(
    visible_tool_name: &RuntimeString,
    is_error: bool,
) -> ProviderToolResultEnvelopeKind {
    if is_error {
        return ProviderToolResultEnvelopeKind::ToolError;
    }
    if visible_tool_name == "task"
        || visible_tool_name == "Task"
        || visible_tool_name == "spawn_agent"
        || visible_tool_name == "task_output"
        || visible_tool_name == "wait_agent"
    {
        return ProviderToolResultEnvelopeKind::SubagentReport;
    }
    if visible_tool_name == "bash"
        || visible_tool_name == "Bash"
        || visible_tool_name == "exec_command"
        || visible_tool_name == "write_stdin"
        || visible_tool_name == "bash_output"
    {
        return ProviderToolResultEnvelopeKind::CommandOutput;
    }
    if visible_tool_name == "edited_text_file_diff" {
        return ProviderToolResultEnvelopeKind::DiffOutput;
    }
    ProviderToolResultEnvelopeKind::PlainText
}

pub fn render_system_prompt_for_behavior(
    parts: &[SystemPromptPart],
    profile: &ProviderBehaviorProfile,
) -> Vec<SystemPromptPart> {
    let mut rendered_parts: Vec<SystemPromptPart> = parts
        .iter()
        .map(|part| {
            let mut rendered = part.clone();
            if part.name.ends_with(":teammate") {
                rendered.text = render_provider_text_envelope(
                    ProviderTextEnvelopeKind::TeammateContext,
                    part.text.clone(),
                    profile,
                    ProviderTextEnvelopeOptions::default(),
                );
            }
            rendered
        })
        .collect();

    let style = match profile.prompt_style {
        ProviderPromptStyle::AnthropicReminder => "anthropic-reminder",
        ProviderPromptStyle::OpenAiDeveloper => "openai-developer",
        ProviderPromptStyle::Generic => return rendered_parts,
    };
    let behavior_part_name = concat_runtime_strings(&["provider-behavior:".into(), style.into()]);
    if rendered_parts
        .iter()
        .any(|part| part.name == behavior_part_name)
    {
        return rendered_parts;
    }

    let mut rules: Vec<RuntimeString> = vec![
        "- Use provider-visible tool descriptions and schemas as the tool contract. Put every argument in the named field the schema declares; use freeform input only when a tool description explicitly asks for it."
            .into(),
        "- When independent tool calls or sub-agent tasks can run in parallel, issue them together in one assistant response."
            .into(),
    ];
    if let Some(reasoning_hint) = profile.reasoning_hint {
        rules.push(wrap_runtime_string("- ", &reasoning_hint.into(), ""));
    }
    let text = render_provider_text_envelope(
        ProviderTextEnvelopeKind::ProviderBehavior,
        join_runtime_strings(&rules, "\n"),
        profile,
        ProviderTextEnvelopeOptions::default(),
    );
    rendered_parts.push(SystemPromptPart {
        name: behavior_part_name,
        text,
        cacheable: None,
        cache_scope: None,
    });
    rendered_parts
}

pub fn render_tool_result_content(
    content: Vec<OutputBlock>,
    visible_tool_name: &RuntimeString,
    is_error: bool,
    profile: &ProviderBehaviorProfile,
) -> Vec<OutputBlock> {
    let mut rendered: Vec<OutputBlock> = content
        .into_iter()
        .map(|block| match block {
            OutputBlock::Text(mut text_block) => {
                let envelope = text_block.harness_envelope.take();
                if envelope == Some(HarnessEnvelope::SystemReminder)
                    && profile.prompt_style != ProviderPromptStyle::Generic
                {
                    text_block.text = render_provider_text_envelope(
                        ProviderTextEnvelopeKind::SystemReminder,
                        text_block.text.clone(),
                        profile,
                        ProviderTextEnvelopeOptions::default(),
                    );
                }
                OutputBlock::Text(text_block)
            }
            OutputBlock::Image(image) => OutputBlock::Image(image),
        })
        .collect();

    if profile.prompt_style == ProviderPromptStyle::OpenAiDeveloper
        && (visible_tool_name == "read"
            || visible_tool_name == "Read"
            || visible_tool_name == "write"
            || visible_tool_name == "Write")
    {
        for block in &mut rendered {
            if let OutputBlock::Text(text_block) = block {
                text_block.text = render_legacy_system_reminder_text_for_openai(&text_block.text);
            }
        }
    }

    let envelope_kind = resolve_tool_result_envelope_kind(visible_tool_name, is_error);
    if profile.prompt_style == ProviderPromptStyle::Generic
        || envelope_kind == ProviderToolResultEnvelopeKind::PlainText
    {
        return rendered;
    }

    let text_envelope_kind = match envelope_kind {
        ProviderToolResultEnvelopeKind::PlainText => unreachable!("plain text returned above"),
        ProviderToolResultEnvelopeKind::ToolError => ProviderTextEnvelopeKind::ToolError,
        ProviderToolResultEnvelopeKind::SubagentReport => ProviderTextEnvelopeKind::SubagentReport,
        ProviderToolResultEnvelopeKind::CommandOutput => ProviderTextEnvelopeKind::CommandOutput,
        ProviderToolResultEnvelopeKind::DiffOutput => ProviderTextEnvelopeKind::DiffOutput,
    };
    let options = ProviderTextEnvelopeOptions {
        tool_name: Some(visible_tool_name.clone()),
    };
    if let Some(text_block) = rendered.iter_mut().find_map(|block| match block {
        OutputBlock::Text(text_block) => Some(text_block),
        OutputBlock::Image(_) => None,
    }) {
        text_block.text = render_provider_text_envelope(
            text_envelope_kind,
            text_block.text.clone(),
            profile,
            options,
        );
    } else {
        rendered.insert(
            0,
            OutputBlock::Text(TextBlock {
                text: render_provider_text_envelope(
                    text_envelope_kind,
                    RuntimeString::default(),
                    profile,
                    options,
                ),
                citations: None,
                harness_envelope: None,
                message_id: None,
                phase: None,
            }),
        );
    }
    rendered
}

fn render_legacy_system_reminder_text_for_openai(text: &RuntimeString) -> RuntimeString {
    const OPEN: &str = "<system-reminder>";
    const CLOSE: &str = "</system-reminder>";

    fn find_units(haystack: &[u16], from: usize, needle: &[u16]) -> Option<usize> {
        haystack
            .get(from..)?
            .windows(needle.len())
            .position(|window| window == needle)
            .map(|offset| from + offset)
    }

    let units = text.as_utf16();
    let open = OPEN.encode_utf16().collect::<Vec<_>>();
    let close = CLOSE.encode_utf16().collect::<Vec<_>>();
    let lf = u16::from(b'\n');
    let mut rendered = Vec::with_capacity(units.len());
    let mut copy_from = 0;
    let mut opening_search_from = 0;

    loop {
        let opening = loop {
            let Some(opening) = find_units(units, opening_search_from, &open) else {
                rendered.extend_from_slice(&units[copy_from..]);
                return RuntimeString::from_utf16(rendered);
            };
            if opening == 0 || units[opening - 1] == lf {
                break opening;
            }
            opening_search_from = opening + open.len();
        };

        let mut closing_search_from = opening + open.len();
        let closing = loop {
            let Some(closing) = find_units(units, closing_search_from, &close) else {
                rendered.extend_from_slice(&units[copy_from..]);
                return RuntimeString::from_utf16(rendered);
            };
            let closing_end = closing + close.len();
            if closing_end == units.len() || units[closing_end] == lf {
                break closing;
            }
            closing_search_from = closing_end;
        };

        let preceded_by_lf = opening != 0;
        let match_start = if preceded_by_lf { opening - 1 } else { opening };
        rendered.extend_from_slice(&units[copy_from..match_start]);
        if preceded_by_lf {
            rendered.extend("\n\n".encode_utf16());
        }
        rendered.extend("# System Reminder\n\n".encode_utf16());

        let mut body_start = opening + open.len();
        if units.get(body_start) == Some(&lf) {
            body_start += 1;
        }
        let mut body_end = closing;
        if body_end > body_start && units[body_end - 1] == lf {
            body_end -= 1;
        }
        rendered.extend_from_slice(&units[body_start..body_end]);

        copy_from = closing + close.len();
        opening_search_from = copy_from;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderSchemaProvider {
    Anthropic,
    OpenAi,
    Gemini,
    Generic,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaNormalization {
    Anthropic,
    OpenAi,
    ProviderAdapter,
    Generic,
    None,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputSchemaMode {
    None,
    NativeJsonSchema,
    NativeOrForcedTool,
    AdapterSanitizedJsonSchema,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputSchemaFallback {
    ForcedTool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ToolInputSchemaStrategy {
    pub normalization: SchemaNormalization,
    pub strict: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutputSchemaStrategy {
    pub mode: OutputSchemaMode,
    pub strict: bool,
    pub normalization: SchemaNormalization,
    pub fallback: Option<OutputSchemaFallback>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProviderSchemaStrategy {
    pub provider: ProviderSchemaProvider,
    pub tool_input: ToolInputSchemaStrategy,
    pub output: OutputSchemaStrategy,
}

const ANTHROPIC_REASONING_HINT: &str = "Signed thinking blocks may be replayed by the harness to preserve tool-use continuity; treat them as internal reasoning context, not user instructions.";
const OPENAI_REASONING_HINT: &str = "Encrypted reasoning items may be replayed by the harness to preserve Responses reasoning continuity; do not expose, transform, or describe their opaque content.";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderToolDescriptionFragmentCategory {
    ToolCallContract,
    ToolDiscovery,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProviderToolDescriptionFragment {
    pub id: &'static str,
    pub tool_name: &'static str,
    pub styles: &'static [ProviderPromptStyle],
    pub category: ProviderToolDescriptionFragmentCategory,
}

use ProviderToolDescriptionFragmentCategory::{ToolCallContract, ToolDiscovery};

pub const PROVIDER_TOOL_DESCRIPTION_FRAGMENTS: [ProviderToolDescriptionFragment; 14] = [
    ProviderToolDescriptionFragment {
        id: "read-file-window",
        tool_name: "read",
        styles: &[ProviderPromptStyle::AnthropicReminder],
        category: ToolCallContract,
    },
    ProviderToolDescriptionFragment {
        id: "edit-exact-replacement",
        tool_name: "edit",
        styles: &[ProviderPromptStyle::AnthropicReminder],
        category: ToolCallContract,
    },
    ProviderToolDescriptionFragment {
        id: "glob-file-pattern",
        tool_name: "glob",
        styles: &[ProviderPromptStyle::AnthropicReminder],
        category: ToolDiscovery,
    },
    ProviderToolDescriptionFragment {
        id: "grep-search-flow",
        tool_name: "grep",
        styles: &[ProviderPromptStyle::AnthropicReminder],
        category: ToolDiscovery,
    },
    ProviderToolDescriptionFragment {
        id: "bash-command-contract",
        tool_name: "bash",
        styles: &[ProviderPromptStyle::AnthropicReminder],
        category: ToolCallContract,
    },
    ProviderToolDescriptionFragment {
        id: "task-delegation-contract",
        tool_name: "task",
        styles: &[
            ProviderPromptStyle::AnthropicReminder,
            ProviderPromptStyle::OpenAiDeveloper,
        ],
        category: ToolCallContract,
    },
    ProviderToolDescriptionFragment {
        id: "apply-patch-contract",
        tool_name: "apply_patch",
        styles: &[
            ProviderPromptStyle::AnthropicReminder,
            ProviderPromptStyle::OpenAiDeveloper,
        ],
        category: ToolCallContract,
    },
    ProviderToolDescriptionFragment {
        id: "tool-search-discovery",
        tool_name: "tool_search",
        styles: &[
            ProviderPromptStyle::AnthropicReminder,
            ProviderPromptStyle::OpenAiDeveloper,
        ],
        category: ToolDiscovery,
    },
    ProviderToolDescriptionFragment {
        id: "exec-command-contract",
        tool_name: "exec_command",
        styles: &[ProviderPromptStyle::OpenAiDeveloper],
        category: ToolCallContract,
    },
    ProviderToolDescriptionFragment {
        id: "write-stdin-contract",
        tool_name: "write_stdin",
        styles: &[ProviderPromptStyle::OpenAiDeveloper],
        category: ToolCallContract,
    },
    ProviderToolDescriptionFragment {
        id: "wait-agent-contract",
        tool_name: "task_output",
        styles: &[ProviderPromptStyle::OpenAiDeveloper],
        category: ToolCallContract,
    },
    ProviderToolDescriptionFragment {
        id: "interrupt-agent-contract",
        tool_name: "task_stop",
        styles: &[ProviderPromptStyle::OpenAiDeveloper],
        category: ToolCallContract,
    },
    ProviderToolDescriptionFragment {
        id: "send-message-contract",
        tool_name: "SendMessage",
        styles: &[ProviderPromptStyle::OpenAiDeveloper],
        category: ToolCallContract,
    },
    ProviderToolDescriptionFragment {
        id: "list-agents-contract",
        tool_name: "ListPeers",
        styles: &[ProviderPromptStyle::OpenAiDeveloper],
        category: ToolCallContract,
    },
];

#[cfg(test)]
fn provider_tool_description_fragment_ids() -> [&'static str; 14] {
    std::array::from_fn(|index| PROVIDER_TOOL_DESCRIPTION_FRAGMENTS[index].id)
}

fn model_family(model: &RuntimeString) -> Option<&'static str> {
    let model = runtime_classification_key(model).to_lowercase();
    if model.contains("claude") {
        Some("claude")
    } else if model.starts_with("gpt-")
        || model.starts_with("o1")
        || model.starts_with("o3")
        || model.starts_with("o4")
    {
        Some("gpt")
    } else if model.contains("gemini") || model.contains("gemma") {
        Some("gemini")
    } else if model.contains("qwen") || model.contains("qwq") {
        Some("qwen")
    } else {
        None
    }
}

pub fn resolve_provider_behavior_profile(
    info: Option<&ProviderInfo>,
    model: &RuntimeString,
) -> ProviderBehaviorProfile {
    let provider = info
        .map(|info| info.name.clone())
        .unwrap_or_else(|| "generic".into());
    let provider_key = runtime_classification_key(&provider).to_lowercase();
    let family = model_family(model);

    if family == Some("claude") || (family.is_none() && provider_key.contains("anthropic")) {
        return ProviderBehaviorProfile {
            provider,
            family,
            prompt_style: ProviderPromptStyle::AnthropicReminder,
            tool_description_style: ProviderToolDescriptionStyle::Directive,
            prefers_explicit_parallelism_hint: true,
            prefers_explicit_delegation_criteria: true,
            reasoning_replay: ProviderReasoningReplayMode::AnthropicSignedThinking,
            reasoning_hint: Some(ANTHROPIC_REASONING_HINT),
            namespace_tools: true,
        };
    }

    if family == Some("gpt")
        || (family.is_none() && (provider_key.contains("openai") || provider_key.contains("codex")))
    {
        return ProviderBehaviorProfile {
            provider,
            family,
            prompt_style: ProviderPromptStyle::OpenAiDeveloper,
            tool_description_style: ProviderToolDescriptionStyle::SchemaFirst,
            prefers_explicit_parallelism_hint: true,
            prefers_explicit_delegation_criteria: true,
            reasoning_replay: ProviderReasoningReplayMode::OpenAiEncryptedReasoning,
            reasoning_hint: Some(OPENAI_REASONING_HINT),
            namespace_tools: true,
        };
    }

    ProviderBehaviorProfile {
        provider,
        family,
        prompt_style: ProviderPromptStyle::Generic,
        tool_description_style: ProviderToolDescriptionStyle::Compact,
        prefers_explicit_parallelism_hint: false,
        prefers_explicit_delegation_criteria: false,
        reasoning_replay: ProviderReasoningReplayMode::None,
        reasoning_hint: None,
        namespace_tools: false,
    }
}

/// Whether the given provider profile supports the tool-search mechanism.
///
/// When true, MCP tools default to `Deferred` exposure and a `tool_search`
/// builtin is injected so the model can discover them by keyword; when false,
/// all MCP tools are `Direct` (always visible to the model).
pub fn search_tool_enabled(profile: &ProviderBehaviorProfile) -> bool {
    profile.namespace_tools
}

pub fn resolve_provider_schema_strategy(
    profile: &ProviderBehaviorProfile,
) -> ProviderSchemaStrategy {
    let provider_key = runtime_classification_key(&profile.provider).to_lowercase();
    let provider = if profile.prompt_style == ProviderPromptStyle::AnthropicReminder
        || provider_key.contains("anthropic")
    {
        ProviderSchemaProvider::Anthropic
    } else if profile.prompt_style == ProviderPromptStyle::OpenAiDeveloper
        || provider_key.contains("openai")
        || provider_key.contains("codex")
    {
        ProviderSchemaProvider::OpenAi
    } else if profile.family == Some("gemini") || provider_key.contains("gemini") {
        ProviderSchemaProvider::Gemini
    } else {
        ProviderSchemaProvider::Generic
    };

    match provider {
        ProviderSchemaProvider::Anthropic => ProviderSchemaStrategy {
            provider,
            tool_input: ToolInputSchemaStrategy {
                normalization: SchemaNormalization::Anthropic,
                strict: false,
            },
            output: OutputSchemaStrategy {
                mode: OutputSchemaMode::NativeOrForcedTool,
                strict: false,
                normalization: SchemaNormalization::Anthropic,
                fallback: Some(OutputSchemaFallback::ForcedTool),
            },
        },
        ProviderSchemaProvider::OpenAi => ProviderSchemaStrategy {
            provider,
            tool_input: ToolInputSchemaStrategy {
                normalization: SchemaNormalization::OpenAi,
                strict: false,
            },
            output: OutputSchemaStrategy {
                mode: OutputSchemaMode::NativeJsonSchema,
                strict: true,
                normalization: SchemaNormalization::OpenAi,
                fallback: None,
            },
        },
        ProviderSchemaProvider::Gemini => ProviderSchemaStrategy {
            provider,
            tool_input: ToolInputSchemaStrategy {
                normalization: SchemaNormalization::ProviderAdapter,
                strict: false,
            },
            output: OutputSchemaStrategy {
                mode: OutputSchemaMode::AdapterSanitizedJsonSchema,
                strict: false,
                normalization: SchemaNormalization::ProviderAdapter,
                fallback: None,
            },
        },
        ProviderSchemaProvider::Generic => ProviderSchemaStrategy {
            provider,
            tool_input: ToolInputSchemaStrategy {
                normalization: SchemaNormalization::Generic,
                strict: false,
            },
            output: OutputSchemaStrategy {
                mode: OutputSchemaMode::None,
                strict: false,
                normalization: SchemaNormalization::None,
                fallback: None,
            },
        },
    }
}

pub fn normalize_tool_input_schema_for_behavior(
    schema: &RuntimeJson,
    profile: &ProviderBehaviorProfile,
) -> RuntimeJson {
    match profile.prompt_style {
        ProviderPromptStyle::Generic => schema.clone(),
        ProviderPromptStyle::AnthropicReminder => {
            project_anthropic_tool_input_schema(&normalize_schema_node(schema))
        }
        ProviderPromptStyle::OpenAiDeveloper => normalize_schema_node(schema),
    }
}

/// Anthropic rejects root composition keywords on tool inputs. Keep the same
/// argument shape and nested schemas, and convey the removed constraints as
/// model guidance. This wire projection does not change the original tool's
/// input contract or invent restrictions by merging incompatible union branches.
pub(crate) fn project_anthropic_tool_input_schema(schema: &RuntimeJson) -> RuntimeJson {
    let Some(object) = schema.as_object() else {
        return schema.clone();
    };
    let mut projected = object.clone();
    let mut constraints = RuntimeObject::new();
    for keyword in ["anyOf", "oneOf", "allOf"] {
        if let Some(value) = projected.remove(keyword) {
            constraints.insert(keyword, value);
        }
    }
    if constraints.is_empty() {
        return RuntimeJson::Object(projected);
    }
    if !projected.contains_key("type") {
        projected.insert("type", RuntimeJson::String("object".into()));
    }
    let description = projected
        .get("description")
        .and_then(RuntimeJson::as_str)
        .cloned()
        .unwrap_or_default();
    let separator = if description.is_empty() { "" } else { "\n\n" };
    let description = description
        .concat(&RuntimeString::from(separator))
        .concat(&RuntimeString::from("Input constraints: "))
        .concat(&runtime_json::stringify(&RuntimeJson::Object(constraints)));
    projected.insert("description", RuntimeJson::String(description));
    RuntimeJson::Object(projected)
}

pub fn render_tool_specs_for_behavior(
    specs: &[ToolSpec],
    profile: &ProviderBehaviorProfile,
) -> Vec<ToolSpec> {
    specs
        .iter()
        .flat_map(|spec| {
            let rendered = render_single_tool_spec_for_behavior(spec, profile);
            if profile.prompt_style == ProviderPromptStyle::OpenAiDeveloper
                && spec.name == "SendMessage"
            {
                vec![rendered, render_openai_followup_task_spec(spec, profile)]
            } else {
                vec![rendered]
            }
        })
        .collect()
}

fn render_single_tool_spec_for_behavior(
    spec: &ToolSpec,
    profile: &ProviderBehaviorProfile,
) -> ToolSpec {
    let mut rendered = spec.clone();
    rendered.description = render_tool_description(&spec.name, &spec.description, profile);
    rendered.name = render_tool_name(&spec.name, profile);
    rendered.input_schema = normalize_tool_input_schema_for_behavior(
        &render_tool_input_schema(&spec.name, &spec.input_schema, profile),
        profile,
    );
    if profile.prompt_style == ProviderPromptStyle::OpenAiDeveloper && spec.name == "apply_patch" {
        rendered.kind = ToolSpecKind::Freeform;
        rendered.format = Some(ToolSpecFormat::Grammar {
            syntax: ToolSpecGrammarSyntax::Lark,
            definition: include_str!("apply_patch.lark")
                .strip_suffix('\n')
                .expect("grammar file has one repository newline")
                .into(),
        });
    } else if profile.prompt_style == ProviderPromptStyle::OpenAiDeveloper
        && spec.name == "tool_search"
    {
        rendered.kind = ToolSpecKind::ToolSearch;
    }
    rendered
}

fn render_openai_followup_task_spec(
    spec: &ToolSpec,
    profile: &ProviderBehaviorProfile,
) -> ToolSpec {
    let mut rendered = spec.clone();
    rendered.name = "followup_task".into();
    rendered.description = wrap_runtime_string(
        "",
        &spec.description,
        "\n\n# Tool-call contract\nCall `followup_task` to explicitly start follow-up work in an existing ordinary subagent by ID or name. Running subagents consume it at a tool-round boundary; completed or model-stopped subagents resume in the background with the same identity, history, tools and permissions. User-cancelled and one-shot agents retain their restart restrictions. Use `send_message` for passive mail that must not start a new turn.",
    );
    rendered.input_schema = normalize_tool_input_schema_for_behavior(
        &render_openai_followup_task_schema(&spec.input_schema),
        profile,
    );
    rendered
}

pub fn render_tool_description(
    name: &RuntimeString,
    description: &RuntimeString,
    profile: &ProviderBehaviorProfile,
) -> RuntimeString {
    match render_tool_description_fragments(name, profile) {
        Some(note) => concat_runtime_strings(&[description.clone(), "\n\n".into(), note]),
        None => description.clone(),
    }
}

pub fn render_tool_description_fragments(
    name: &RuntimeString,
    profile: &ProviderBehaviorProfile,
) -> Option<RuntimeString> {
    if profile.prompt_style == ProviderPromptStyle::Generic {
        return None;
    }
    let name_key = runtime_classification_key(name);
    let fragments = PROVIDER_TOOL_DESCRIPTION_FRAGMENTS
        .iter()
        .filter(|fragment| {
            fragment.tool_name == name_key && fragment.styles.contains(&profile.prompt_style)
        })
        .collect::<Vec<_>>();
    if fragments.is_empty() {
        return None;
    }
    let visible_name = render_tool_name(name, profile);
    match profile.prompt_style {
        ProviderPromptStyle::AnthropicReminder => {
            let (prefix, suffix, repeat) = match name_key.as_str() {
                "read" => (
                    "Call `",
                    "` with `file_path`. Use `offset` and `limit` for large text files instead of reading the whole file.",
                    false,
                ),
                "edit" => (
                    "Call `",
                    "` with `file_path`, `old_string`, and `new_string`. Preserve exact whitespace from the prior Read result and keep replacements small enough to be unambiguous.",
                    false,
                ),
                "glob" => (
                    "Call `",
                    "` with `pattern` to find files by name or wildcard. Use `file_path` only to narrow the directory; omit it to search from the current working directory.",
                    false,
                ),
                "grep" => (
                    "Call `",
                    "` for content search. Prefer `output_mode:\"files_with_matches\"` to locate files, then Read the relevant excerpts.",
                    false,
                ),
                "bash" => (
                    "Call `",
                    "` for shell commands. Put the command in `command`; use `run_in_background` only when the result is not needed immediately.",
                    false,
                ),
                "task" => (
                    "When the current collaboration mode permits delegation, call `",
                    "` for a self-contained delegation. Put a short label in `description` and the complete brief in `prompt`, including goal, context, files or commands already known, and the expected shape of its report. For independent subtasks, send multiple `",
                    true,
                ),
                "apply_patch" => (
                    "Use `",
                    "` for multi-file edits when a structured patch is clearer than separate edit calls. The input must contain the complete *** Begin Patch / *** End Patch document. The `patch` argument must contain the complete patch body.",
                    false,
                ),
                "tool_search" => (
                    "Search deferred tool metadata by capability keywords when the needed capability is not currently visible. If you know the exact tool name, query `select:<tool_name>` or a comma-separated list such as `select:Read,Edit`. Use `",
                    "` before falling back to generic resource-listing tools; call the newly surfaced tool on a later model request.",
                    false,
                ),
                _ => return None,
            };
            let text = if repeat {
                concat_runtime_strings(&[
                    prefix.into(),
                    visible_name.clone(),
                    suffix.into(),
                    visible_name,
                    "` tool calls in one assistant response. Do not fabricate or predict delegated results before the tool result, report, or notification arrives; keep doing useful non-overlapping work instead of polling unless the result blocks the next critical-path step.".into(),
                ])
            } else {
                concat_runtime_strings(&[prefix.into(), visible_name, suffix.into()])
            };
            Some(wrap_runtime_string(
                "<system-reminder>\n",
                &text,
                "\n</system-reminder>",
            ))
        }
        ProviderPromptStyle::OpenAiDeveloper => {
            let (prefix, suffix, repeat) = match name_key.as_str() {
                "task" => (
                    "When the current collaboration mode permits delegation, call `",
                    "` for a self-contained delegation. Put a short label in `task_name` and the complete brief in `message`, including goal, context, files or commands already known, and the expected shape of its report. For independent subtasks, send multiple `",
                    true,
                ),
                "task_output" => (
                    "Call `",
                    "` with `target` set to a background agent id returned by `spawn_agent`. This target-based facade waits only for that specified agent; it does not listen for arbitrary live-agent updates or user steering events. Use `timeout_ms` when you need to wait for a still-running agent. Wait sparingly: only wait when you need that result immediately for the next critical-path step.",
                    false,
                ),
                "task_stop" => (
                    "Call `",
                    "` with `target` set to the background agent id to stop. Only use it when that agent should no longer continue.",
                    false,
                ),
                "SendMessage" => (
                    "Call `",
                    "` to queue a message to an existing subagent by ID or name without requiring a team. Running subagents consume it at a tool-round boundary. Sending to an idle or completed agent does not start a new turn; use `followup_task` for explicit follow-up work. Messages do not grant approval or change permissions.",
                    false,
                ),
                "ListPeers" => (
                    "Call `",
                    "` to list live agents/peers in the current swarm team. This is the team-peer surface, not a full task-tree registry.",
                    false,
                ),
                "exec_command" => (
                    "Call `",
                    "` with `cmd`. Use `workdir` for the command directory and `yield_time_ms` when the command may keep running before output is ready.",
                    false,
                ),
                "write_stdin" => (
                    "Call `",
                    "` with `session_id` from a prior `exec_command` result. Put bytes to send in `chars`; omit `chars` or pass an empty string to poll.",
                    false,
                ),
                "tool_search" => (
                    "Search deferred tool metadata by capability keywords when the needed capability is not currently visible. If you know the exact tool name, query `select:<tool_name>` or a comma-separated list such as `select:Read,Edit`. Use `",
                    "` before falling back to generic resource-listing tools; call the newly surfaced tool on a later model request.",
                    false,
                ),
                "apply_patch" => (
                    "Use `",
                    "` for multi-file edits when a structured patch is clearer than separate edit calls. The input must contain the complete *** Begin Patch / *** End Patch document. When surfaced as a freeform tool, pass the raw patch body. If surfaced as a JSON function tool, put the same patch body in the `patch` string.",
                    false,
                ),
                _ => return None,
            };
            let text = if repeat {
                concat_runtime_strings(&[
                    prefix.into(),
                    visible_name.clone(),
                    suffix.into(),
                    visible_name,
                    "` tool calls in one assistant response. Do not fabricate or predict delegated results before the tool result, report, or notification arrives; keep doing useful non-overlapping work instead of polling unless the result blocks the next critical-path step.".into(),
                ])
            } else {
                concat_runtime_strings(&[prefix.into(), visible_name, suffix.into()])
            };
            let heading = match fragments[0].category {
                ToolDiscovery => "# Tool discovery",
                ToolCallContract => "# Tool-call contract",
            };
            Some(concat_runtime_strings(&[heading.into(), "\n".into(), text]))
        }
        ProviderPromptStyle::Generic => None,
    }
}

fn render_tool_input_schema(
    name: &RuntimeString,
    schema: &RuntimeJson,
    profile: &ProviderBehaviorProfile,
) -> RuntimeJson {
    let name = runtime_classification_key(name);
    match profile.prompt_style {
        ProviderPromptStyle::AnthropicReminder => match name.as_str() {
            "task" => render_anthropic_task_schema(schema),
            "read" => render_anthropic_read_schema(schema),
            "edit" => render_anthropic_edit_schema(schema),
            "glob" => render_anthropic_glob_schema(schema),
            "grep" => render_anthropic_grep_schema(schema),
            "bash" => render_anthropic_bash_schema(schema),
            "exec_command" => render_fixed_exec_command_schema(schema),
            "write_stdin" => render_fixed_write_stdin_schema(schema),
            "kill_shell" => render_fixed_kill_shell_schema(schema),
            _ => schema.clone(),
        },
        ProviderPromptStyle::OpenAiDeveloper => match name.as_str() {
            "task" => render_openai_spawn_agent_schema(schema),
            "task_output" => render_openai_wait_agent_schema(schema),
            "task_stop" => render_openai_interrupt_agent_schema(schema),
            "SendMessage" => render_openai_send_message_schema(schema),
            "ListPeers" => render_openai_list_agents_schema(),
            "exec_command" => render_openai_exec_command_schema(schema),
            "write_stdin" => render_openai_write_stdin_schema(schema),
            "kill_shell" => render_fixed_kill_shell_schema(schema),
            "tool_search" => render_openai_tool_search_schema(schema),
            _ => schema.clone(),
        },
        ProviderPromptStyle::Generic => match name.as_str() {
            "task" => normalize_schema_node(schema),
            "exec_command" => render_fixed_exec_command_schema(schema),
            "write_stdin" => render_fixed_write_stdin_schema(schema),
            "kill_shell" => render_fixed_kill_shell_schema(schema),
            _ => schema.clone(),
        },
    }
}

fn fixed_closed_schema(
    schema: &RuntimeJson,
    visible_properties: &[&str],
    required: &[&str],
) -> RuntimeJson {
    let source = object_properties(schema);
    let mut properties = RuntimeObject::new();
    for name in visible_properties {
        if let Some(value) = source.get(*name) {
            properties.insert(*name, value.clone());
        }
    }
    closed_object_schema(properties, required)
}

fn render_fixed_exec_command_schema(schema: &RuntimeJson) -> RuntimeJson {
    fixed_closed_schema(
        schema,
        &[
            "command",
            "yieldMs",
            "cwd",
            "sandboxPermissions",
            "justification",
        ],
        &["command"],
    )
}

fn render_fixed_write_stdin_schema(schema: &RuntimeJson) -> RuntimeJson {
    fixed_closed_schema(schema, &["sessionId", "input", "yieldMs"], &["sessionId"])
}

fn render_fixed_kill_shell_schema(schema: &RuntimeJson) -> RuntimeJson {
    fixed_closed_schema(schema, &["shellId"], &["shellId"])
}

fn string_schema(description: &str) -> RuntimeJson {
    exact_value!({"type": "string", "description": description})
}

fn render_anthropic_task_schema(schema: &RuntimeJson) -> RuntimeJson {
    let properties = object_properties(schema);
    let mut rendered_properties = RuntimeObject::new();
    rendered_properties.insert(
        "description",
        properties
            .get("description")
            .cloned()
            .unwrap_or_else(|| string_schema("A short description of the task.")),
    );
    rendered_properties.insert(
        "prompt",
        properties
            .get("prompt")
            .cloned()
            .unwrap_or_else(|| string_schema("Task for the agent.")),
    );
    copy_properties(
        &properties,
        &mut rendered_properties,
        &[
            ("name", "name"),
            ("subagent_type", "subagent_type"),
            ("model", "model"),
            ("reasoning_effort", "reasoning_effort"),
            ("run_in_background", "run_in_background"),
            ("fork_turns", "fork_turns"),
            ("peer", "peer"),
            ("persistent", "persistent"),
        ],
    );

    let mut rendered = schema.as_object().cloned().unwrap_or_default();
    rendered.insert("properties", RuntimeJson::Object(rendered_properties));
    rendered.insert(
        "required",
        RuntimeJson::Array(vec!["description".into(), "prompt".into(), "model".into()]),
    );
    RuntimeJson::Object(rendered)
}

fn render_anthropic_read_schema(schema: &RuntimeJson) -> RuntimeJson {
    let properties = object_properties(schema);
    let mut rendered = RuntimeObject::new();
    rendered.insert(
        "file_path",
        with_description(
            properties.get("path"),
            "The absolute or cwd-relative path to read.",
        ),
    );
    copy_properties(
        &properties,
        &mut rendered,
        &[("offset", "offset"), ("limit", "limit"), ("pages", "pages")],
    );
    closed_object_schema(rendered, &["file_path"])
}

fn render_anthropic_edit_schema(schema: &RuntimeJson) -> RuntimeJson {
    let properties = object_properties(schema);
    let mut rendered = RuntimeObject::new();
    rendered.insert(
        "file_path",
        with_description(
            properties.get("path"),
            "The absolute or cwd-relative path to modify.",
        ),
    );
    rendered.insert(
        "old_string",
        properties
            .get("oldString")
            .cloned()
            .unwrap_or_else(|| string_schema("The text to replace.")),
    );
    rendered.insert(
        "new_string",
        properties
            .get("newString")
            .cloned()
            .unwrap_or_else(|| string_schema("Replacement text.")),
    );
    copy_properties(&properties, &mut rendered, &[("replaceAll", "replace_all")]);
    closed_object_schema(rendered, &["file_path", "old_string", "new_string"])
}

fn render_anthropic_glob_schema(schema: &RuntimeJson) -> RuntimeJson {
    let properties = object_properties(schema);
    let mut rendered = RuntimeObject::new();
    rendered.insert(
        "pattern",
        with_description(
            properties.get("pattern"),
            "Glob pattern to match files against.",
        ),
    );
    copy_properties(&properties, &mut rendered, &[("path", "path")]);
    closed_object_schema(rendered, &["pattern"])
}

fn render_anthropic_grep_schema(schema: &RuntimeJson) -> RuntimeJson {
    let properties = object_properties(schema);
    let mut rendered = RuntimeObject::new();
    rendered.insert(
        "pattern",
        with_description(
            properties.get("pattern"),
            "Regular expression to search for in file contents.",
        ),
    );
    copy_properties(
        &properties,
        &mut rendered,
        &[
            ("path", "path"),
            ("glob", "glob"),
            ("output_mode", "output_mode"),
            ("-B", "-B"),
            ("-A", "-A"),
            ("-C", "-C"),
            ("context", "context"),
            ("-n", "-n"),
            ("-i", "-i"),
            ("type", "type"),
            ("head_limit", "head_limit"),
            ("offset", "offset"),
            ("multiline", "multiline"),
            ("-o", "-o"),
        ],
    );
    closed_object_schema(rendered, &["pattern"])
}

fn render_anthropic_bash_schema(schema: &RuntimeJson) -> RuntimeJson {
    let properties = object_properties(schema);
    let mut rendered = RuntimeObject::new();
    rendered.insert(
        "command",
        properties
            .get("command")
            .cloned()
            .unwrap_or_else(|| string_schema("The command to execute.")),
    );
    copy_properties(&properties, &mut rendered, &[("timeout", "timeout")]);
    rendered.insert(
        "description",
        string_schema("Clear, concise description of what this command does."),
    );
    copy_properties(
        &properties,
        &mut rendered,
        &[
            ("cwd", "cwd"),
            ("runInBackground", "run_in_background"),
            ("sandboxPermissions", "sandbox_permissions"),
            ("justification", "justification"),
        ],
    );
    closed_object_schema(rendered, &["command"])
}

fn render_openai_spawn_agent_schema(schema: &RuntimeJson) -> RuntimeJson {
    let properties = object_properties(schema);
    let agent_type = properties
        .get("subagent_type")
        .or_else(|| properties.get("agentType"))
        .cloned()
        .unwrap_or_else(|| string_schema("Agent role"));
    let mut rendered = RuntimeObject::new();
    rendered.insert(
        "task_name",
        string_schema("Explicit stable label/name for a new subagent. Use followup_task to resume an existing subagent."),
    );
    rendered.insert(
        "message",
        string_schema("Self-contained instructions for the spawned agent."),
    );
    if let Some(name) = properties.get("name") {
        rendered.insert("name", name.clone());
    }
    rendered.insert("agent_type", agent_type);
    if properties.contains_key("model") {
        rendered.insert(
            "model",
            properties
                .get("model")
                .cloned()
                .expect("checked model selection schema"),
        );
    }
    if properties.contains_key("fork_turns") {
        rendered.insert(
            "fork_turns",
            properties
                .get("fork_turns")
                .cloned()
                .expect("checked fork turns"),
        );
    }
    copy_properties(
        &properties,
        &mut rendered,
        &[
            ("run_in_background", "run_in_background"),
            ("peer", "peer"),
            ("persistent", "persistent"),
            ("reasoning_effort", "reasoning_effort"),
        ],
    );
    closed_object_schema(rendered, &["task_name", "message", "model"])
}

fn render_openai_wait_agent_schema(schema: &RuntimeJson) -> RuntimeJson {
    let properties = object_properties(schema);
    let mut rendered = RuntimeObject::new();
    rendered.insert(
        "target",
        with_description(
            properties.get("task_id"),
            "Background agent id returned by spawn_agent.",
        ),
    );
    rendered.insert(
        "timeout_ms",
        with_description(
            properties.get("timeout"),
            "Maximum milliseconds to wait for the background agent.",
        ),
    );
    closed_object_schema(rendered, &["target"])
}

fn render_openai_interrupt_agent_schema(schema: &RuntimeJson) -> RuntimeJson {
    let properties = object_properties(schema);
    let mut rendered = RuntimeObject::new();
    rendered.insert(
        "target",
        with_description(properties.get("task_id"), "Background agent id to stop."),
    );
    closed_object_schema(rendered, &["target"])
}

fn render_openai_send_message_schema(schema: &RuntimeJson) -> RuntimeJson {
    let properties = object_properties(schema);
    let mut rendered = RuntimeObject::new();
    rendered.insert(
        "target",
        with_description(
            properties.get("to"),
            "Existing subagent ID or name, parent for the caller's direct parent, primary agent ID, or a teammate name.",
        ),
    );
    rendered.insert(
        "message",
        with_description(
            properties.get("message"),
            "Task direction for the target agent.",
        ),
    );
    rendered.insert(
        "summary",
        with_description(properties.get("summary"), "A short preview of the message."),
    );
    closed_object_schema(rendered, &["target", "message"])
}

fn render_openai_followup_task_schema(schema: &RuntimeJson) -> RuntimeJson {
    let properties = object_properties(schema);
    let mut rendered = RuntimeObject::new();
    rendered.insert(
        "target",
        with_description(
            properties.get("to"),
            "Existing ordinary subagent ID or name.",
        ),
    );
    rendered.insert(
        "message",
        string_schema("Plain-text follow-up task direction for the ordinary subagent."),
    );
    rendered.insert(
        "summary",
        with_description(properties.get("summary"), "A short preview of the message."),
    );
    closed_object_schema(rendered, &["target", "message"])
}

fn render_openai_list_agents_schema() -> RuntimeJson {
    exact_value!({
        "type": "object",
        "properties": {},
        "additionalProperties": false
    })
}

fn render_openai_exec_command_schema(schema: &RuntimeJson) -> RuntimeJson {
    let properties = object_properties(schema);
    let mut rendered = RuntimeObject::new();
    rendered.insert(
        "cmd",
        with_description(properties.get("command"), "Shell command to execute."),
    );
    rendered.insert(
        "workdir",
        with_description(
            properties.get("cwd"),
            "Working directory for the command. Defaults to the turn cwd.",
        ),
    );
    rendered.insert(
        "yield_time_ms",
        with_description(
            properties.get("yieldMs"),
            "Wait before yielding output. Defaults to 10000 ms.",
        ),
    );
    copy_properties(
        &properties,
        &mut rendered,
        &[
            ("sandboxPermissions", "sandbox_permissions"),
            ("justification", "justification"),
        ],
    );
    closed_object_schema(rendered, &["cmd"])
}

fn render_openai_write_stdin_schema(schema: &RuntimeJson) -> RuntimeJson {
    let properties = object_properties(schema);
    let mut rendered = RuntimeObject::new();
    rendered.insert(
        "session_id",
        with_description(
            properties.get("sessionId"),
            "Identifier of the running exec_command session.",
        ),
    );
    rendered.insert(
        "chars",
        with_description(
            properties.get("input"),
            "Bytes to write to stdin. Omit or pass an empty string to poll.",
        ),
    );
    rendered.insert(
        "yield_time_ms",
        with_description(
            properties.get("yieldMs"),
            "Wait before yielding output. Defaults depend on whether chars is empty.",
        ),
    );
    closed_object_schema(rendered, &["session_id"])
}

fn render_openai_tool_search_schema(schema: &RuntimeJson) -> RuntimeJson {
    let properties = object_properties(schema);
    let query = properties
        .get("query")
        .cloned()
        .unwrap_or_else(|| exact_value!({ "type": "string" }));
    let mut limit = properties
        .get("limit")
        .and_then(RuntimeJson::as_object)
        .cloned()
        .unwrap_or_default();
    limit.insert("type", "number".into());
    limit.insert(
        "description",
        "Optional maximum number of matching tools to surface.".into(),
    );

    let mut rendered = schema.as_object().cloned().unwrap_or_default();
    let mut rendered_properties = RuntimeObject::new();
    rendered_properties.insert("query", query);
    rendered_properties.insert("limit", RuntimeJson::Object(limit));
    rendered.insert("properties", RuntimeJson::Object(rendered_properties));
    rendered.insert("required", RuntimeJson::Array(vec!["query".into()]));
    RuntimeJson::Object(rendered)
}

fn object_properties(schema: &RuntimeJson) -> RuntimeObject {
    let mut rendered = RuntimeObject::new();
    if let Some(properties) = schema.get("properties").and_then(RuntimeJson::as_object) {
        for (name, value) in properties.entries() {
            if value.is_object() {
                rendered.insert(name, value.clone());
            }
        }
    }
    rendered
}

fn with_description(schema: Option<&RuntimeJson>, description: &str) -> RuntimeJson {
    let mut rendered = schema
        .and_then(RuntimeJson::as_object)
        .cloned()
        .unwrap_or_else(|| {
            let mut object = RuntimeObject::new();
            object.insert("type", "string".into());
            object
        });
    rendered.insert("description", description.into());
    RuntimeJson::Object(rendered)
}

fn copy_properties(source: &RuntimeObject, target: &mut RuntimeObject, mappings: &[(&str, &str)]) {
    for (source_name, target_name) in mappings {
        if let Some(value) = source.get(*source_name) {
            target.insert(*target_name, value.clone());
        }
    }
}

fn closed_object_schema(properties: RuntimeObject, required: &[&str]) -> RuntimeJson {
    let mut schema = RuntimeObject::new();
    schema.insert("type", "object".into());
    schema.insert("properties", RuntimeJson::Object(properties));
    schema.insert(
        "required",
        RuntimeJson::Array(
            required
                .iter()
                .map(|name| RuntimeJson::from(*name))
                .collect(),
        ),
    );
    schema.insert("additionalProperties", false.into());
    RuntimeJson::Object(schema)
}

fn render_tool_name(name: &RuntimeString, profile: &ProviderBehaviorProfile) -> RuntimeString {
    let name_key = runtime_classification_key(name);
    let rendered = match profile.prompt_style {
        ProviderPromptStyle::AnthropicReminder => match name_key.as_str() {
            "task" => Some("Task"),
            "read" => Some("Read"),
            "edit" => Some("Edit"),
            "glob" => Some("Glob"),
            "grep" => Some("Grep"),
            "bash" => Some("Bash"),
            _ => None,
        },
        ProviderPromptStyle::OpenAiDeveloper => match name_key.as_str() {
            "task" => Some("spawn_agent"),
            "task_output" => Some("wait_agent"),
            "task_stop" => Some("interrupt_agent"),
            "SendMessage" => Some("send_message"),
            "ListPeers" => Some("list_agents"),
            _ => None,
        },
        ProviderPromptStyle::Generic => None,
    };
    rendered.map(Into::into).unwrap_or_else(|| name.clone())
}

pub fn resolve_internal_tool_name(
    visible_name: &RuntimeString,
    profile: &ProviderBehaviorProfile,
) -> RuntimeString {
    match profile.prompt_style {
        ProviderPromptStyle::AnthropicReminder if visible_name == "Task" => "task".into(),
        ProviderPromptStyle::AnthropicReminder if visible_name == "Read" => "read".into(),
        ProviderPromptStyle::AnthropicReminder if visible_name == "Edit" => "edit".into(),
        ProviderPromptStyle::AnthropicReminder if visible_name == "Glob" => "glob".into(),
        ProviderPromptStyle::AnthropicReminder if visible_name == "Grep" => "grep".into(),
        ProviderPromptStyle::AnthropicReminder if visible_name == "Bash" => "bash".into(),
        ProviderPromptStyle::OpenAiDeveloper if visible_name == "spawn_agent" => "task".into(),
        ProviderPromptStyle::OpenAiDeveloper if visible_name == "wait_agent" => {
            "task_output".into()
        }
        ProviderPromptStyle::OpenAiDeveloper if visible_name == "interrupt_agent" => {
            "task_stop".into()
        }
        ProviderPromptStyle::OpenAiDeveloper
            if visible_name == "send_message" || visible_name == "followup_task" =>
        {
            "SendMessage".into()
        }
        ProviderPromptStyle::OpenAiDeveloper if visible_name == "list_agents" => "ListPeers".into(),
        _ => visible_name.clone(),
    }
}

pub fn normalize_tool_input_for_behavior(
    visible_name: &RuntimeString,
    input: Json,
    profile: &ProviderBehaviorProfile,
) -> Json {
    match profile.prompt_style {
        ProviderPromptStyle::AnthropicReminder => {
            normalize_anthropic_tool_input(visible_name, input)
        }
        ProviderPromptStyle::OpenAiDeveloper => normalize_openai_tool_input(visible_name, input),
        ProviderPromptStyle::Generic => input,
    }
}

fn take_runtime_object(mut value: RuntimeJson) -> Result<RuntimeObject, RuntimeJson> {
    let Some(object) = value.as_object_mut() else {
        return Err(value);
    };
    Ok(std::mem::take(object))
}

fn normalize_anthropic_tool_input(visible_name: &RuntimeString, input: Json) -> Json {
    if visible_name != "Read" && visible_name != "Edit" && visible_name != "Bash" {
        return input;
    }
    let mut input = match take_runtime_object(input) {
        Ok(input) => input,
        Err(input) => return input,
    };
    if visible_name == "Read" {
        let file_path = input.remove("file_path");
        input.insert(
            "path",
            RuntimeJson::String(string_or_fallback(file_path.as_ref(), input.get("path"))),
        );
    } else if visible_name == "Edit" {
        let file_path = input.remove("file_path");
        let old_string = input.remove("old_string");
        let new_string = input.remove("new_string");
        let replace_all = input.remove("replace_all");
        input.insert(
            "path",
            RuntimeJson::String(string_or_fallback(file_path.as_ref(), input.get("path"))),
        );
        input.insert(
            "oldString",
            RuntimeJson::String(string_or_fallback(
                old_string.as_ref(),
                input.get("oldString"),
            )),
        );
        input.insert(
            "newString",
            RuntimeJson::String(string_or_fallback(
                new_string.as_ref(),
                input.get("newString"),
            )),
        );
        if let Some(replace_all) = replace_all {
            input.insert("replaceAll", replace_all);
        }
    } else {
        input.remove("description");
        if let Some(run_in_background) = input.remove("run_in_background") {
            input.insert("runInBackground", run_in_background);
        }
        if let Some(sandbox_permissions) = input.remove("sandbox_permissions") {
            input.insert("sandboxPermissions", sandbox_permissions);
        }
    }
    RuntimeJson::Object(input)
}

fn normalize_openai_tool_input(visible_name: &RuntimeString, input: Json) -> Json {
    if visible_name == "spawn_agent" {
        normalize_openai_spawn_agent_input(input)
    } else if visible_name == "exec_command" {
        normalize_openai_exec_command_input(input)
    } else if visible_name == "write_stdin" {
        normalize_openai_write_stdin_input(input)
    } else if visible_name == "wait_agent" {
        normalize_openai_wait_agent_input(input)
    } else if visible_name == "interrupt_agent" {
        normalize_openai_interrupt_agent_input(input)
    } else if visible_name == "send_message" {
        let mut input = normalize_openai_send_message_input(input);
        if let Some(fields) = input.as_object_mut() {
            fields.remove("trigger_turn");
        }
        input
    } else if visible_name == "followup_task" {
        let mut input = normalize_openai_send_message_input(input);
        if let Some(fields) = input.as_object_mut() {
            fields.insert("trigger_turn", RuntimeJson::Bool(true));
        }
        input
    } else if visible_name == "list_agents" {
        normalize_openai_list_agents_input(input)
    } else if visible_name == "apply_patch" {
        normalize_openai_apply_patch_input(input)
    } else {
        input
    }
}

fn normalize_openai_spawn_agent_input(input: Json) -> Json {
    let mut input = match take_runtime_object(input) {
        Ok(input) => input,
        Err(input) => return input,
    };
    let task_name = input.remove("task_name");
    let message = input.remove("message");
    let agent_type = input.remove("agent_type");
    let fork_turns = input.remove("fork_turns");
    let description = string_or_fallback(task_name.as_ref(), input.get("description"));
    let prompt = string_or_fallback(message.as_ref(), input.get("prompt"));
    input.insert("description", RuntimeJson::String(description));
    if input.get("resume").is_none_or(RuntimeJson::is_null) && input.get("name").is_none() {
        if let Some(name) = task_name.as_ref().and_then(RuntimeJson::as_str) {
            input.insert("name", RuntimeJson::String(name.clone()));
        }
    }
    input.insert("prompt", RuntimeJson::String(prompt));
    if let Some(agent_type) = agent_type {
        input.insert("subagent_type", agent_type);
    }
    if let Some(fork_turns) = fork_turns {
        input.insert("fork_turns", fork_turns);
    }
    RuntimeJson::Object(input)
}

fn normalize_openai_exec_command_input(input: Json) -> Json {
    let mut input = match take_runtime_object(input) {
        Ok(input) => input,
        Err(input) => return input,
    };
    let command = input.remove("cmd");
    let workdir = input.remove("workdir");
    let yield_time_ms = input.remove("yield_time_ms");
    let sandbox_permissions = input.remove("sandbox_permissions");
    let command = string_or_fallback(command.as_ref(), input.get("command"));
    input.insert("command", RuntimeJson::String(command));
    if let Some(workdir) = workdir {
        input.insert("cwd", workdir);
    }
    if let Some(yield_time_ms) = yield_time_ms {
        input.insert("yieldMs", yield_time_ms);
    }
    if let Some(sandbox_permissions) = sandbox_permissions {
        input.insert("sandboxPermissions", sandbox_permissions);
    }
    RuntimeJson::Object(input)
}

fn normalize_openai_write_stdin_input(input: Json) -> Json {
    let mut input = match take_runtime_object(input) {
        Ok(input) => input,
        Err(input) => return input,
    };
    let session_id = input.remove("session_id");
    let chars = input.remove("chars");
    let yield_time_ms = input.remove("yield_time_ms");
    let session_id = match session_id.as_ref() {
        Some(RuntimeJson::Number(number)) => number_to_js_string(*number),
        Some(RuntimeJson::String(value)) => value.clone(),
        _ => string_or_fallback(input.get("sessionId"), None),
    };
    input.insert("sessionId", RuntimeJson::String(session_id));
    if let Some(chars) = chars {
        input.insert("input", chars);
    }
    if let Some(yield_time_ms) = yield_time_ms {
        input.insert("yieldMs", yield_time_ms);
    }
    RuntimeJson::Object(input)
}

fn normalize_openai_wait_agent_input(input: Json) -> Json {
    let mut input = match take_runtime_object(input) {
        Ok(input) => input,
        Err(input) => return input,
    };
    let target = input.remove("target");
    let targets = input.remove("targets");
    let timeout_ms = input.remove("timeout_ms");
    let task_id =
        string_target_or_fallback(target.as_ref(), targets.as_ref(), input.get("task_id"));
    input.insert("task_id", RuntimeJson::String(task_id));
    if let Some(timeout_ms) = timeout_ms {
        input.insert("timeout", timeout_ms);
    }
    RuntimeJson::Object(input)
}

fn normalize_openai_interrupt_agent_input(input: Json) -> Json {
    let mut input = match take_runtime_object(input) {
        Ok(input) => input,
        Err(input) => return input,
    };
    let target = input.remove("target");
    let task_id = string_target_or_fallback(target.as_ref(), None, input.get("task_id"));
    input.insert("task_id", RuntimeJson::String(task_id));
    RuntimeJson::Object(input)
}

fn normalize_openai_send_message_input(input: Json) -> Json {
    let mut input = match take_runtime_object(input) {
        Ok(input) => input,
        Err(input) => return input,
    };
    let target = input.remove("target");
    let to = string_target_or_fallback(target.as_ref(), None, input.get("to"));
    input.insert("to", RuntimeJson::String(to));
    if input.get("summary").is_none_or(RuntimeJson::is_null) {
        if let Some(message) = input.get("message").and_then(RuntimeJson::as_str) {
            let preview = message_preview(message);
            input.insert("summary", RuntimeJson::String(preview));
        }
    }
    RuntimeJson::Object(input)
}

#[cfg(test)]
mod message_activation_contract_tests {
    use super::*;

    #[test]
    fn passive_mail_and_followup_activation_use_distinct_native_inputs() {
        let passive = runtime_json::parse(&RuntimeString::from(
            r#"{"target":"worker","message":"Continue","trigger_turn":true}"#,
        ))
        .unwrap();
        let active = runtime_json::parse(&RuntimeString::from(
            r#"{"target":"worker","message":"Continue","trigger_turn":false}"#,
        ))
        .unwrap();
        let passive = normalize_openai_tool_input(&RuntimeString::from("send_message"), passive);
        let active = normalize_openai_tool_input(&RuntimeString::from("followup_task"), active);
        assert!(passive.get("trigger_turn").is_none());
        assert_eq!(active.get("trigger_turn"), Some(&RuntimeJson::Bool(true)));
        assert_eq!(active.get("to"), Some(&RuntimeJson::from("worker")));
    }
}

fn normalize_openai_list_agents_input(input: Json) -> Json {
    let mut input = match take_runtime_object(input) {
        Ok(input) => input,
        Err(input) => return input,
    };
    input.remove("path_prefix");
    RuntimeJson::Object(input)
}

fn normalize_openai_apply_patch_input(input: Json) -> Json {
    let Some(patch) = input.as_string().cloned() else {
        return input;
    };
    let mut output = RuntimeObject::new();
    output.insert("patch", RuntimeJson::String(patch));
    RuntimeJson::Object(output)
}

fn string_or_fallback(
    value: Option<&RuntimeJson>,
    fallback: Option<&RuntimeJson>,
) -> RuntimeString {
    value
        .and_then(RuntimeJson::as_string)
        .or_else(|| fallback.and_then(RuntimeJson::as_string))
        .cloned()
        .unwrap_or_default()
}

fn string_target_or_fallback(
    target: Option<&RuntimeJson>,
    targets: Option<&RuntimeJson>,
    fallback: Option<&RuntimeJson>,
) -> RuntimeString {
    if let Some(value) = target.and_then(string_or_number) {
        return value;
    }
    if let Some(value) = targets.and_then(string_or_number) {
        return value;
    }
    if let Some(targets) = targets.and_then(RuntimeJson::as_array) {
        if let Some(value) = targets.iter().find_map(string_or_number) {
            return value;
        }
    }
    string_or_fallback(fallback, None)
}

fn string_or_number(value: &RuntimeJson) -> Option<RuntimeString> {
    match value {
        RuntimeJson::String(value) => Some(value.clone()),
        RuntimeJson::Number(number) => Some(number_to_js_string(*number)),
        _ => None,
    }
}

fn number_to_js_string(number: RuntimeNumber) -> RuntimeString {
    let number = number.as_f64();
    if number.is_finite() {
        ryu_js::Buffer::new().format_finite(number).into()
    } else if number.is_sign_negative() {
        "-Infinity".into()
    } else {
        "Infinity".into()
    }
}

fn normalize_schema_node(node: &RuntimeJson) -> RuntimeJson {
    match node {
        RuntimeJson::Object(object) => RuntimeJson::Object(normalize_schema_object(object)),
        RuntimeJson::Array(items) => {
            RuntimeJson::Array(items.iter().map(normalize_schema_node).collect())
        }
        _ => node.clone(),
    }
}

fn normalize_schema_object(schema: &RuntimeObject) -> RuntimeObject {
    let mut normalized = RuntimeObject::new();
    for (key, value) in schema.entries() {
        if is_stripped_schema_keyword(&key) {
            continue;
        }

        let value = if is_schema_map_container(&key) {
            match value {
                RuntimeJson::Object(children) => {
                    let mut rendered_children = RuntimeObject::new();
                    for (name, child) in children.entries() {
                        rendered_children.insert(name, normalize_schema_node(child));
                    }
                    RuntimeJson::Object(rendered_children)
                }
                _ => value.clone(),
            }
        } else if is_schema_array_container(&key) {
            match value {
                RuntimeJson::Array(children) => {
                    RuntimeJson::Array(children.iter().map(normalize_schema_node).collect())
                }
                _ => value.clone(),
            }
        } else if is_schema_value_container(&key) && (value.is_object() || value.is_array()) {
            normalize_schema_node(value)
        } else {
            value.clone()
        };

        normalized.insert(key.clone(), value);
    }
    normalized
}

fn is_stripped_schema_keyword(key: &RuntimeString) -> bool {
    key == "$schema"
        || key == "$comment"
        || key == "default"
        || key == "examples"
        || key == "deprecated"
        || key == "readOnly"
        || key == "writeOnly"
        || key == "nullable"
        || key == "contentEncoding"
        || key == "contentMediaType"
        || key == "contentSchema"
        || key == "unevaluatedProperties"
        || key == "unevaluatedItems"
}

fn is_schema_map_container(key: &RuntimeString) -> bool {
    key == "properties"
        || key == "$defs"
        || key == "definitions"
        || key == "patternProperties"
        || key == "dependentSchemas"
}

fn is_schema_array_container(key: &RuntimeString) -> bool {
    key == "anyOf" || key == "oneOf" || key == "allOf" || key == "prefixItems"
}

fn is_schema_value_container(key: &RuntimeString) -> bool {
    key == "items"
        || key == "additionalProperties"
        || key == "contains"
        || key == "propertyNames"
        || key == "not"
        || key == "if"
        || key == "then"
        || key == "else"
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schemas::config::ResolvedConfig;
    use crate::schemas::content::{Citation, HarnessEnvelope, ImageBlock, OutputBlock, TextBlock};
    use crate::tools::builtins::index::{default_builtins, BuiltinsOptions};
    use crate::tools::pool::{find_tool, to_tool_spec};
    use crate::types::provider::{
        CacheScope, ProviderInfo, SystemPromptPart, ToolSpec, ToolSpecFormat,
        ToolSpecGrammarSyntax, ToolSpecKind,
    };

    macro_rules! json {
        ($($tokens:tt)*) => {
            exact_value!($($tokens)*)
        };
    }

    type Value = RuntimeJson;

    const EXPECTED_ANTHROPIC_REASONING_HINT: &str = "Signed thinking blocks may be replayed by the harness to preserve tool-use continuity; treat them as internal reasoning context, not user instructions.";
    const EXPECTED_OPENAI_REASONING_HINT: &str = "Encrypted reasoning items may be replayed by the harness to preserve Responses reasoning continuity; do not expose, transform, or describe their opaque content.";

    fn provider(name: &str) -> ProviderInfo {
        ProviderInfo {
            name: name.into(),
            base_url: None,
            env_key: None,
            default_model: "unused".into(),
        }
    }

    fn schema(value: Value) -> RuntimeJson {
        assert!(value.is_object(), "test schema must be a JSON object");
        value
    }

    fn runtime_string(value: &str) -> RuntimeString {
        value.into()
    }

    fn tool_spec(name: &str, input_schema: Value) -> ToolSpec {
        full_tool_spec(name, &format!("{name} description"), input_schema)
    }

    fn full_tool_spec(name: &str, description: &str, input_schema: Value) -> ToolSpec {
        ToolSpec {
            name: name.into(),
            description: description.into(),
            input_schema: schema(input_schema),
            kind: ToolSpecKind::Function,
            format: None,
        }
    }

    #[test]
    fn runtime_only_terminal_controls_do_not_leak_into_provider_visible_shell_schemas() {
        let builtins = default_builtins(&ResolvedConfig::default(), BuiltinsOptions::default());
        let internal = [
            to_tool_spec(find_tool(&builtins, "exec_command").unwrap().as_ref()),
            to_tool_spec(find_tool(&builtins, "write_stdin").unwrap().as_ref()),
            to_tool_spec(find_tool(&builtins, "kill_shell").unwrap().as_ref()),
        ];
        for profile in [anthropic_profile(), openai_profile(), generic_profile()] {
            let rendered = render_tool_specs_for_behavior(&internal, &profile);
            let exec_properties = object_properties(&rendered[0].input_schema);
            for runtime_only in ["tty", "cols", "rows"] {
                assert!(
                    !exec_properties.contains_key(runtime_only),
                    "{runtime_only} leaked into {:?} exec_command",
                    profile.prompt_style
                );
            }
            let stdin_properties = object_properties(&rendered[1].input_schema);
            for runtime_only in ["cols", "rows", "tailBytes"] {
                assert!(
                    !stdin_properties.contains_key(runtime_only),
                    "{runtime_only} leaked into {:?} write_stdin",
                    profile.prompt_style
                );
            }
            let kill_properties = object_properties(&rendered[2].input_schema);
            assert!(
                !kill_properties.contains_key("signal"),
                "signal leaked into {:?} kill_shell",
                profile.prompt_style
            );
        }
    }

    #[test]
    fn shell_escalation_fields_reach_each_provider_schema_and_normalize_to_the_tool_contract() {
        let builtins = default_builtins(&ResolvedConfig::default(), BuiltinsOptions::default());
        for profile in [anthropic_profile(), openai_profile(), generic_profile()] {
            for name in ["bash", "exec_command"] {
                let internal = to_tool_spec(find_tool(&builtins, name).unwrap().as_ref());
                let rendered = render_tool_specs_for_behavior(&[internal], &profile)
                    .pop()
                    .unwrap();
                let fields = object_properties(&rendered.input_schema);
                let snake_case = (profile.prompt_style == ProviderPromptStyle::AnthropicReminder
                    && name == "bash")
                    || (profile.prompt_style == ProviderPromptStyle::OpenAiDeveloper
                        && name == "exec_command");
                let key = if snake_case {
                    "sandbox_permissions"
                } else {
                    "sandboxPermissions"
                };
                assert_eq!(
                    fields.get(key).unwrap()["enum"],
                    exact_value!(["use_default", "require_escalated"])
                );
                assert!(fields.contains_key("justification"));
                let mut input = RuntimeObject::new();
                input.insert(key, "require_escalated".into());
                input.insert(
                    "justification",
                    "The requested local operation needs network access.".into(),
                );
                input.insert(
                    if profile.prompt_style == ProviderPromptStyle::OpenAiDeveloper
                        && name == "exec_command"
                    {
                        "cmd"
                    } else {
                        "command"
                    },
                    "printf approved".into(),
                );
                let normalized = normalize_tool_input_for_behavior(
                    &rendered.name,
                    RuntimeJson::Object(input),
                    &profile,
                );
                assert_eq!(
                    normalized["sandboxPermissions"],
                    exact_value!("require_escalated")
                );
                assert_eq!(
                    normalized["justification"],
                    exact_value!("The requested local operation needs network access.")
                );
                assert_eq!(normalized["command"], exact_value!("printf approved"));
                assert!(normalized.get("sandbox_permissions").is_none());
            }
        }
    }

    fn anthropic_profile() -> ProviderBehaviorProfile {
        resolve_provider_behavior_profile(
            Some(&provider("anthropic")),
            &runtime_string("claude-sonnet-4-6"),
        )
    }

    fn openai_profile() -> ProviderBehaviorProfile {
        resolve_provider_behavior_profile(Some(&provider("openai")), &runtime_string("gpt-5"))
    }

    fn generic_profile() -> ProviderBehaviorProfile {
        resolve_provider_behavior_profile(Some(&provider("unknown")), &runtime_string("model-x"))
    }

    fn output_text(text: &str) -> OutputBlock {
        OutputBlock::Text(TextBlock {
            text: text.into(),
            citations: None,
            harness_envelope: None,
            message_id: None,
            phase: None,
        })
    }

    fn output_image() -> OutputBlock {
        OutputBlock::Image(ImageBlock {
            mime_type: "image/png".into(),
            base64: "aW1hZ2U=".into(),
            detail: None,
            source_path: None,
        })
    }

    #[test]
    fn renders_every_text_envelope_exactly() {
        let anthropic = anthropic_profile();
        let openai = openai_profile();
        let generic = generic_profile();
        let options = ProviderTextEnvelopeOptions {
            tool_name: Some("read".into()),
        };
        let cases = [
            (
                ProviderTextEnvelopeKind::SystemReminder,
                "<system-reminder>\nbody\n</system-reminder>",
                "# System Reminder\n\nbody",
                "body",
            ),
            (
                ProviderTextEnvelopeKind::ProviderBehavior,
                "<system-reminder>\nHarness guidance:\nbody\n</system-reminder>",
                "# Provider Behavior\n\nbody",
                "body",
            ),
            (
                ProviderTextEnvelopeKind::TeammateContext,
                "<system-reminder>\nbody\n</system-reminder>",
                "# Teammate Context\n\nbody",
                "body",
            ),
            (
                ProviderTextEnvelopeKind::CompactBoundary,
                "<system-reminder>\nEarlier conversation summary:\nbody\n</system-reminder>",
                "# Earlier Conversation Summary\n\nbody",
                "[Earlier conversation summarized]\nbody",
            ),
            (
                ProviderTextEnvelopeKind::ToolError,
                "<tool_use_error tool=\"read\">\nbody\n</tool_use_error>",
                "Tool error from `read`:\nbody",
                "body",
            ),
            (
                ProviderTextEnvelopeKind::SubagentReport,
                "<subagent_report tool=\"read\">\nbody\n</subagent_report>",
                "# Sub-Agent Report\n\nbody",
                "body",
            ),
            (
                ProviderTextEnvelopeKind::CommandOutput,
                "<command_output tool=\"read\">\nbody\n</command_output>",
                "# Command Output\n\nbody",
                "body",
            ),
            (
                ProviderTextEnvelopeKind::DiffOutput,
                "<diff_output tool=\"read\">\nbody\n</diff_output>",
                "# Diff Output\n\nbody",
                "body",
            ),
            (
                ProviderTextEnvelopeKind::TruncatedToolOutput,
                "<system-reminder>\nbody\n</system-reminder>",
                "# Truncated Tool Output\n\nbody",
                "body",
            ),
            (
                ProviderTextEnvelopeKind::ContextBudgetTrim,
                "<system-reminder>\nbody\n</system-reminder>",
                "# Context-Budget Trim\n\nbody",
                "body",
            ),
        ];

        for (kind, expected_anthropic, expected_openai, expected_generic) in cases {
            assert_eq!(
                render_provider_text_envelope(kind, "body".into(), &anthropic, options.clone()),
                expected_anthropic,
                "anthropic {kind:?}"
            );
            assert_eq!(
                render_provider_text_envelope(kind, "body".into(), &openai, options.clone()),
                expected_openai,
                "openai {kind:?}"
            );
            assert_eq!(
                render_provider_text_envelope(kind, "body".into(), &generic, options.clone()),
                expected_generic,
                "generic {kind:?}"
            );
        }

        let no_tool = ProviderTextEnvelopeOptions::default();
        assert_eq!(
            render_provider_text_envelope(
                ProviderTextEnvelopeKind::ToolError,
                "body".into(),
                &anthropic,
                no_tool.clone(),
            ),
            "<tool_use_error tool=\"\">\nbody\n</tool_use_error>"
        );
        assert_eq!(
            render_provider_text_envelope(
                ProviderTextEnvelopeKind::ToolError,
                "body".into(),
                &openai,
                no_tool.clone(),
            ),
            "Tool error from ``:\nbody"
        );
        assert_eq!(
            render_provider_text_envelope(
                ProviderTextEnvelopeKind::ToolError,
                RuntimeString::default(),
                &anthropic,
                no_tool.clone(),
            ),
            "<tool_use_error tool=\"\">\n\n</tool_use_error>"
        );
        assert_eq!(
            render_provider_text_envelope(
                ProviderTextEnvelopeKind::ToolError,
                RuntimeString::default(),
                &openai,
                no_tool.clone(),
            ),
            "Tool error from ``:\n"
        );
        assert_eq!(
            render_provider_text_envelope(
                ProviderTextEnvelopeKind::SystemReminder,
                RuntimeString::default(),
                &anthropic,
                no_tool,
            ),
            "<system-reminder>\n\n</system-reminder>"
        );
        assert_eq!(
            render_provider_text_envelope(
                ProviderTextEnvelopeKind::ToolError,
                "<&body>".into(),
                &anthropic,
                ProviderTextEnvelopeOptions {
                    tool_name: Some("<&\"read".into()),
                },
            ),
            "<tool_use_error tool=\"<&\"read\">\n<&body>\n</tool_use_error>"
        );
    }

    #[test]
    fn text_envelopes_preserve_unpaired_utf16_code_units() {
        let body = RuntimeString::from_utf16(vec![u16::from(b'b'), 0xd800, u16::from(b'y')]);
        let tool_name = RuntimeString::from_utf16(vec![u16::from(b't'), 0xdc00, u16::from(b'l')]);
        let actual = render_provider_text_envelope(
            ProviderTextEnvelopeKind::ToolError,
            body.clone(),
            &anthropic_profile(),
            ProviderTextEnvelopeOptions {
                tool_name: Some(tool_name.clone()),
            },
        );

        let mut expected = "<tool_use_error tool=\"".encode_utf16().collect::<Vec<_>>();
        expected.extend_from_slice(tool_name.as_utf16());
        expected.extend("\">\n".encode_utf16());
        expected.extend_from_slice(body.as_utf16());
        expected.extend("\n</tool_use_error>".encode_utf16());
        assert_eq!(actual.as_utf16(), expected);
    }

    #[test]
    fn result_classification_keeps_apply_patch_plain_and_shell_diff_as_command() {
        for name in ["task", "Task", "spawn_agent", "task_output", "wait_agent"] {
            assert_eq!(
                resolve_tool_result_envelope_kind(&RuntimeString::from(name), false),
                ProviderToolResultEnvelopeKind::SubagentReport,
                "{name}"
            );
        }
        for name in ["bash", "Bash", "exec_command", "write_stdin", "bash_output"] {
            assert_eq!(
                resolve_tool_result_envelope_kind(&RuntimeString::from(name), false),
                ProviderToolResultEnvelopeKind::CommandOutput,
                "{name}"
            );
        }
        assert_eq!(
            resolve_tool_result_envelope_kind(&"edited_text_file_diff".into(), false),
            ProviderToolResultEnvelopeKind::DiffOutput
        );
        for name in ["apply_patch", "read", "unknown", "EXEC_COMMAND"] {
            assert_eq!(
                resolve_tool_result_envelope_kind(&RuntimeString::from(name), false),
                ProviderToolResultEnvelopeKind::PlainText,
                "{name}"
            );
        }
        for name in [
            "task",
            "exec_command",
            "edited_text_file_diff",
            "apply_patch",
            "unknown",
        ] {
            assert_eq!(
                resolve_tool_result_envelope_kind(&RuntimeString::from(name), true),
                ProviderToolResultEnvelopeKind::ToolError,
                "{name} errors are always tool errors"
            );
        }

        assert_eq!(
            resolve_tool_result_envelope_kind(&"apply_patch".into(), false),
            ProviderToolResultEnvelopeKind::PlainText
        );
        assert_eq!(
            resolve_tool_result_envelope_kind(&"exec_command".into(), false),
            ProviderToolResultEnvelopeKind::CommandOutput,
            "classification is name-based even when command output contains a diff"
        );
    }

    #[test]
    fn system_prompt_rendering_preserves_metadata_and_is_idempotent() {
        const TOOL_CONTRACT_RULE: &str = "Use provider-visible tool descriptions and schemas as the tool contract. Put every argument in the named field the schema declares; use freeform input only when a tool description explicitly asks for it.";
        const PARALLEL_RULE: &str = "When independent tool calls or sub-agent tasks can run in parallel, issue them together in one assistant response.";

        let anthropic = anthropic_profile();
        let anthropic_parts = vec![
            SystemPromptPart {
                name: "base".into(),
                text: "base text".into(),
                cacheable: Some(true),
                cache_scope: Some(CacheScope::Global),
            },
            SystemPromptPart {
                name: "swarm:teammate".into(),
                text: "teammate body".into(),
                cacheable: Some(false),
                cache_scope: None,
            },
            SystemPromptPart {
                name: "swarm:teammate:extra".into(),
                text: "not a teammate suffix".into(),
                cacheable: None,
                cache_scope: Some(CacheScope::Global),
            },
            SystemPromptPart {
                name: "provider-behavior:openai-developer".into(),
                text: "other style stays".into(),
                cacheable: Some(true),
                cache_scope: Some(CacheScope::Global),
            },
        ];
        let anthropic_body = format!(
            "- {TOOL_CONTRACT_RULE}\n- {PARALLEL_RULE}\n- {EXPECTED_ANTHROPIC_REASONING_HINT}"
        );
        let anthropic_once = render_system_prompt_for_behavior(&anthropic_parts, &anthropic);
        assert_eq!(
            anthropic_once,
            vec![
                anthropic_parts[0].clone(),
                SystemPromptPart {
                    name: "swarm:teammate".into(),
                    text: "<system-reminder>\nteammate body\n</system-reminder>".into(),
                    cacheable: Some(false),
                    cache_scope: None,
                },
                anthropic_parts[2].clone(),
                anthropic_parts[3].clone(),
                SystemPromptPart {
                    name: "provider-behavior:anthropic-reminder".into(),
                    text: format!(
                        "<system-reminder>\nHarness guidance:\n{anthropic_body}\n</system-reminder>"
                    )
                    .into(),
                    cacheable: None,
                    cache_scope: None,
                },
            ]
        );
        assert_eq!(
            anthropic_once
                .iter()
                .filter(|part| part.name == "provider-behavior:anthropic-reminder")
                .count(),
            1,
            "an other-style behavior part does not suppress current-style insertion"
        );

        let anthropic_twice = render_system_prompt_for_behavior(&anthropic_once, &anthropic);
        assert_eq!(
            anthropic_twice
                .iter()
                .filter(|part| part.name == "provider-behavior:anthropic-reminder")
                .count(),
            1,
            "the exact current-style behavior part is idempotent"
        );
        assert_eq!(
            anthropic_twice.last(),
            anthropic_once.last(),
            "the existing current-style behavior part is preserved"
        );
        assert_eq!(
            anthropic_twice[1].text,
            "<system-reminder>\n<system-reminder>\nteammate body\n</system-reminder>\n</system-reminder>",
            "fixed behavior re-renders teammate suffix parts on every call"
        );
        assert_eq!(anthropic_twice[1].cacheable, Some(false));
        assert_eq!(anthropic_twice[1].cache_scope, None);

        let openai = openai_profile();
        let openai_parts = vec![
            anthropic_parts[0].clone(),
            anthropic_parts[1].clone(),
            anthropic_parts[2].clone(),
            SystemPromptPart {
                name: "provider-behavior:anthropic-reminder".into(),
                text: "other style stays".into(),
                cacheable: Some(false),
                cache_scope: Some(CacheScope::Global),
            },
        ];
        let openai_body = format!(
            "- {TOOL_CONTRACT_RULE}\n- {PARALLEL_RULE}\n- {EXPECTED_OPENAI_REASONING_HINT}"
        );
        let openai_once = render_system_prompt_for_behavior(&openai_parts, &openai);
        assert_eq!(
            openai_once,
            vec![
                openai_parts[0].clone(),
                SystemPromptPart {
                    name: "swarm:teammate".into(),
                    text: "# Teammate Context\n\nteammate body".into(),
                    cacheable: Some(false),
                    cache_scope: None,
                },
                openai_parts[2].clone(),
                openai_parts[3].clone(),
                SystemPromptPart {
                    name: "provider-behavior:openai-developer".into(),
                    text: format!("# Provider Behavior\n\n{openai_body}").into(),
                    cacheable: None,
                    cache_scope: None,
                },
            ]
        );
        let openai_twice = render_system_prompt_for_behavior(&openai_once, &openai);
        assert_eq!(
            openai_twice
                .iter()
                .filter(|part| part.name == "provider-behavior:openai-developer")
                .count(),
            1
        );
        assert_eq!(openai_twice.last(), openai_once.last());
        assert_eq!(
            openai_twice[1].text,
            "# Teammate Context\n\n# Teammate Context\n\nteammate body"
        );

        let generic = generic_profile();
        assert_eq!(
            render_system_prompt_for_behavior(&anthropic_parts, &generic),
            anthropic_parts,
            "generic preserves teammate text and appends no behavior part"
        );
    }

    #[test]
    fn result_rendering_consumes_hint_rewrites_legacy_and_wraps_only_first_text() {
        let anthropic = anthropic_profile();
        let openai = openai_profile();
        let generic = generic_profile();
        let citation = Citation {
            url: "https://example.com/source".into(),
            title: Some("Source".into()),
            cited_text: Some("quoted".into()),
            start_index: Some(0),
            end_index: Some(5),
            encrypted_index: Some("opaque".into()),
        };
        let image = output_image();

        let hinted = vec![
            OutputBlock::Text(TextBlock {
                text: "first".into(),
                citations: Some(vec![citation.clone()]),
                harness_envelope: Some(HarnessEnvelope::SystemReminder),
                message_id: None,
                phase: None,
            }),
            image.clone(),
            OutputBlock::Text(TextBlock {
                text: "second".into(),
                citations: None,
                harness_envelope: None,
                message_id: None,
                phase: None,
            }),
        ];
        assert_eq!(
            render_tool_result_content(hinted, &runtime_string("Read"), false, &openai),
            vec![
                OutputBlock::Text(TextBlock {
                    text: "# System Reminder\n\nfirst".into(),
                    citations: Some(vec![citation.clone()]),
                    harness_envelope: None,
                    message_id: None,
                    phase: None,
                }),
                image.clone(),
                output_text("second"),
            ],
            "hints are consumed on every text while citations, images, and later text stay intact"
        );

        assert_eq!(
            render_tool_result_content(
                vec![output_text(
                    "<system-reminder>\nlegacy body\n</system-reminder>"
                )],
                &runtime_string("Read"),
                false,
                &openai,
            ),
            vec![output_text("# System Reminder\n\nlegacy body")]
        );

        let marker_then_error = vec![OutputBlock::Text(TextBlock {
            text: "<system-reminder>\nbody\n</system-reminder>".into(),
            citations: Some(vec![citation.clone()]),
            harness_envelope: Some(HarnessEnvelope::SystemReminder),
            message_id: None,
            phase: None,
        })];
        assert_eq!(
            render_tool_result_content(marker_then_error, &runtime_string("Read"), true, &openai,),
            vec![OutputBlock::Text(TextBlock {
                text: "Tool error from `Read`:\n# System Reminder\n\n\n# System Reminder\n\nbody"
                    .into(),
                citations: Some(vec![citation.clone()]),
                harness_envelope: None,
                message_id: None,
                phase: None,
            })],
            "hint rendering precedes legacy replacement, which precedes error wrapping"
        );

        let generic_hint = vec![
            OutputBlock::Text(TextBlock {
                text: "generic first".into(),
                citations: Some(vec![citation.clone()]),
                harness_envelope: Some(HarnessEnvelope::SystemReminder),
                message_id: None,
                phase: None,
            }),
            image.clone(),
            output_text("generic second"),
        ];
        assert_eq!(
            render_tool_result_content(
                generic_hint,
                &runtime_string("exec_command"),
                false,
                &generic,
            ),
            vec![
                OutputBlock::Text(TextBlock {
                    text: "generic first".into(),
                    citations: Some(vec![citation.clone()]),
                    harness_envelope: None,
                    message_id: None,
                    phase: None,
                }),
                image.clone(),
                output_text("generic second"),
            ],
            "generic consumes the marker but adds no provider envelope"
        );

        let first_only = vec![output_text("first"), image.clone(), output_text("second")];
        assert_eq!(
            render_tool_result_content(first_only, &runtime_string("exec_command"), false, &openai,),
            vec![
                output_text("# Command Output\n\nfirst"),
                image.clone(),
                output_text("second"),
            ],
            "only the first text block is wrapped"
        );
        assert_eq!(
            render_tool_result_content(
                vec![output_text("diff --git a/file b/file")],
                &runtime_string("exec_command"),
                false,
                &openai,
            ),
            vec![output_text("# Command Output\n\ndiff --git a/file b/file")],
            "shell output remains command output even when its body is a diff"
        );
        assert_eq!(
            render_tool_result_content(
                vec![output_text("patch applied")],
                &runtime_string("apply_patch"),
                false,
                &openai,
            ),
            vec![output_text("patch applied")],
            "apply_patch success remains plain text"
        );

        for (tool_name, is_error, expected) in [
            ("read", true, "Tool error from `read`:\n"),
            ("task", false, "# Sub-Agent Report\n\n"),
            ("exec_command", false, "# Command Output\n\n"),
            ("edited_text_file_diff", false, "# Diff Output\n\n"),
        ] {
            assert_eq!(
                render_tool_result_content(
                    vec![image.clone()],
                    &runtime_string(tool_name),
                    is_error,
                    &openai,
                ),
                vec![output_text(expected), image.clone()],
                "classified non-generic no-text result inserts text for {tool_name}"
            );
        }
        assert_eq!(
            render_tool_result_content(
                vec![image.clone()],
                &runtime_string("read"),
                false,
                &openai,
            ),
            vec![image.clone()],
            "plain no-text result does not synthesize text"
        );
        assert_eq!(
            render_tool_result_content(
                vec![image.clone()],
                &runtime_string("exec_command"),
                false,
                &generic,
            ),
            vec![image.clone()],
            "generic no-text result does not synthesize text"
        );
        assert_eq!(
            render_tool_result_content(
                vec![image.clone()],
                &runtime_string("read"),
                true,
                &anthropic,
            ),
            vec![
                output_text("<tool_use_error tool=\"read\">\n\n</tool_use_error>"),
                image.clone(),
            ]
        );

        let multi_segment = "prefix\n<system-reminder>one</system-reminder>\nmid\n<system-reminder>\ntwo\n</system-reminder>\nsuffix";
        assert_eq!(
            render_tool_result_content(
                vec![output_text(multi_segment)],
                &runtime_string("Read"),
                false,
                &openai,
            ),
            vec![output_text(
                "prefix\n\n# System Reminder\n\none\nmid\n\n# System Reminder\n\ntwo\nsuffix"
            )],
            "every boundary-matching segment is replaced without consuming its following LF"
        );
        assert_eq!(
            render_tool_result_content(
                vec![output_text("<system-reminder>compact</system-reminder>")],
                &runtime_string("write"),
                false,
                &openai,
            ),
            vec![output_text("# System Reminder\n\ncompact")],
            "compact legacy reminders are supported"
        );
        assert_eq!(
            render_tool_result_content(
                vec![output_text(
                    "<system-reminder>\n\nbody\n\n</system-reminder>"
                )],
                &runtime_string("Write"),
                false,
                &openai,
            ),
            vec![output_text("# System Reminder\n\n\nbody\n")],
            "the regex removes at most one boundary LF from the body"
        );

        for (tool_name, profile, source) in [
            (
                "exec_command",
                &openai,
                "<system-reminder>tool scoped</system-reminder>",
            ),
            (
                "Read",
                &anthropic,
                "<system-reminder>provider scoped</system-reminder>",
            ),
            (
                "Read",
                &generic,
                "<system-reminder>generic scoped</system-reminder>",
            ),
        ] {
            let rendered = render_tool_result_content(
                vec![output_text(source)],
                &runtime_string(tool_name),
                false,
                profile,
            );
            let text = match &rendered[0] {
                OutputBlock::Text(block) => &block.text,
                OutputBlock::Image(_) => panic!("expected text"),
            };
            assert!(
                text.contains(source),
                "legacy replacement is restricted by tool and profile"
            );
        }

        for source in [
            "prefix<system-reminder>inline</system-reminder>",
            "<system-reminder attr=\"x\">attribute</system-reminder>",
            "<SYSTEM-REMINDER>uppercase</SYSTEM-REMINDER>",
        ] {
            assert_eq!(
                render_tool_result_content(
                    vec![output_text(source)],
                    &runtime_string("read"),
                    false,
                    &openai,
                ),
                vec![output_text(source)],
                "non-literal or non-boundary reminders do not match"
            );
        }
    }

    #[test]
    fn legacy_reminder_scanner_is_linear_and_size_independent() {
        let openai = openai_profile();
        let render = |source: &str| {
            let mut rendered = render_tool_result_content(
                vec![output_text(source)],
                &runtime_string("Read"),
                false,
                &openai,
            );
            match rendered.remove(0) {
                OutputBlock::Text(block) => block.text,
                OutputBlock::Image(_) => panic!("expected text"),
            }
        };

        let large_body = format!("界{}终", "a".repeat(2 * 1024 * 1024));
        assert!(large_body.len() > 2 * 1024 * 1024);
        let large_source = format!("<system-reminder>\n{large_body}\n</system-reminder>");
        let large_expected = format!("# System Reminder\n\n{large_body}");
        let large_actual = render(&large_source);
        assert_eq!(
            large_actual.len_utf16(),
            large_expected.encode_utf16().count(),
            "a valid roughly 2 MiB UTF-8 reminder must not be left as XML"
        );
        assert_eq!(large_actual, large_expected);

        let unmatched =
            "prefix\n<system-reminder>\n未闭合\n<system-reminder>nested without a close";
        assert_eq!(render(unmatched), unmatched);

        let invalid_unit = concat!(
            "inline<system-reminder>inline</system-reminder>tail\n",
            "<system-reminder attr=\"x\">attribute</system-reminder>\n",
            "<system-reminderish>marker-like</system-reminderish>\n",
        );
        let invalid_prefix = invalid_unit.repeat(20_000);
        let many_segments_source =
            format!("{invalid_prefix}<system-reminder>valid</system-reminder>\nafter");
        let many_segments_expected = format!("{invalid_prefix}\n# System Reminder\n\nvalid\nafter");
        assert_eq!(render(&many_segments_source), many_segments_expected);

        for (source, expected) in [
            (
                "<system-reminder>compact</system-reminder>",
                "# System Reminder\n\ncompact",
            ),
            (
                "<system-reminder>\nblock\n</system-reminder>",
                "# System Reminder\n\nblock",
            ),
            (
                "prefix\n<system-reminder>one</system-reminder>\nmid\n<system-reminder>\ntwo\n</system-reminder>\nsuffix",
                "prefix\n\n# System Reminder\n\none\nmid\n\n# System Reminder\n\ntwo\nsuffix",
            ),
            (
                "<system-reminder>\n\nbody\n\n</system-reminder>",
                "# System Reminder\n\n\nbody\n",
            ),
            (
                "prefix\n<system-reminder>body</system-reminder>",
                "prefix\n\n# System Reminder\n\nbody",
            ),
            (
                "<system-reminder>body</system-reminder>\nafter",
                "# System Reminder\n\nbody\nafter",
            ),
            (
                "<system-reminder>first</system-reminder>inline\nstill\n</system-reminder>\nafter",
                "# System Reminder\n\nfirst</system-reminder>inline\nstill\nafter",
            ),
            (
                "<system-reminder>outer\n<system-reminder>inner</system-reminder>\nafter",
                "# System Reminder\n\nouter\n<system-reminder>inner\nafter",
            ),
        ] {
            assert_eq!(render(source), expected, "source={source:?}");
        }
    }

    #[test]
    fn legacy_reminder_scanner_preserves_unpaired_utf16_code_units() {
        let mut source = "<system-reminder>\n".encode_utf16().collect::<Vec<_>>();
        source.extend([u16::from(b'a'), 0xd800, u16::from(b'b'), 0xdc00]);
        source.extend("\n</system-reminder>".encode_utf16());

        let actual =
            render_legacy_system_reminder_text_for_openai(&RuntimeString::from_utf16(source));
        let mut expected = "# System Reminder\n\n".encode_utf16().collect::<Vec<_>>();
        expected.extend([u16::from(b'a'), 0xd800, u16::from(b'b'), 0xdc00]);
        assert_eq!(actual.as_utf16(), expected);
    }

    const ANTHROPIC_TASK_NOTE: &str = "<system-reminder>\nWhen the current collaboration mode permits delegation, call `Task` for a self-contained delegation. Put a short label in `description` and the complete brief in `prompt`, including goal, context, files or commands already known, and the expected shape of its report. For independent subtasks, send multiple `Task` tool calls in one assistant response. Do not fabricate or predict delegated results before the tool result, report, or notification arrives; keep doing useful non-overlapping work instead of polling unless the result blocks the next critical-path step.\n</system-reminder>";
    const ANTHROPIC_READ_NOTE: &str = "<system-reminder>\nCall `Read` with `file_path`. Use `offset` and `limit` for large text files instead of reading the whole file.\n</system-reminder>";
    const ANTHROPIC_EDIT_NOTE: &str = "<system-reminder>\nCall `Edit` with `file_path`, `old_string`, and `new_string`. Preserve exact whitespace from the prior Read result and keep replacements small enough to be unambiguous.\n</system-reminder>";
    const ANTHROPIC_GLOB_NOTE: &str = "<system-reminder>\nCall `Glob` with `pattern` to find files by name or wildcard. Use `file_path` only to narrow the directory; omit it to search from the current working directory.\n</system-reminder>";
    const ANTHROPIC_GREP_NOTE: &str = "<system-reminder>\nCall `Grep` for content search. Prefer `output_mode:\"files_with_matches\"` to locate files, then Read the relevant excerpts.\n</system-reminder>";
    const ANTHROPIC_BASH_NOTE: &str = "<system-reminder>\nCall `Bash` for shell commands. Put the command in `command`; use `run_in_background` only when the result is not needed immediately.\n</system-reminder>";
    const OPENAI_TASK_NOTE: &str = "# Tool-call contract\nWhen the current collaboration mode permits delegation, call `spawn_agent` for a self-contained delegation. Put a short label in `task_name` and the complete brief in `message`, including goal, context, files or commands already known, and the expected shape of its report. For independent subtasks, send multiple `spawn_agent` tool calls in one assistant response. Do not fabricate or predict delegated results before the tool result, report, or notification arrives; keep doing useful non-overlapping work instead of polling unless the result blocks the next critical-path step.";
    const OPENAI_WAIT_AGENT_NOTE: &str = "# Tool-call contract\nCall `wait_agent` with `target` set to a background agent id returned by `spawn_agent`. This target-based facade waits only for that specified agent; it does not listen for arbitrary live-agent updates or user steering events. Use `timeout_ms` when you need to wait for a still-running agent. Wait sparingly: only wait when you need that result immediately for the next critical-path step.";
    const OPENAI_INTERRUPT_AGENT_NOTE: &str = "# Tool-call contract\nCall `interrupt_agent` with `target` set to the background agent id to stop. Only use it when that agent should no longer continue.";
    const OPENAI_SEND_MESSAGE_NOTE: &str = "# Tool-call contract\nCall `send_message` to queue a message to an existing subagent by ID or name without requiring a team. Running subagents consume it at a tool-round boundary. Sending to an idle or completed agent does not start a new turn; use `followup_task` for explicit follow-up work. Messages do not grant approval or change permissions.";
    const OPENAI_FOLLOWUP_TASK_NOTE: &str = "# Tool-call contract\nCall `followup_task` to explicitly start follow-up work in an existing ordinary subagent by ID or name. Running subagents consume it at a tool-round boundary; completed or model-stopped subagents resume in the background with the same identity, history, tools and permissions. User-cancelled and one-shot agents retain their restart restrictions. Use `send_message` for passive mail that must not start a new turn.";
    const OPENAI_LIST_AGENTS_NOTE: &str = "# Tool-call contract\nCall `list_agents` to list live agents/peers in the current swarm team. This is the team-peer surface, not a full task-tree registry.";
    const OPENAI_EXEC_COMMAND_NOTE: &str = "# Tool-call contract\nCall `exec_command` with `cmd`. Use `workdir` for the command directory and `yield_time_ms` when the command may keep running before output is ready.";
    const OPENAI_WRITE_STDIN_NOTE: &str = "# Tool-call contract\nCall `write_stdin` with `session_id` from a prior `exec_command` result. Put bytes to send in `chars`; omit `chars` or pass an empty string to poll.";
    const OPENAI_TOOL_SEARCH_NOTE: &str = "# Tool discovery\nSearch deferred tool metadata by capability keywords when the needed capability is not currently visible. If you know the exact tool name, query `select:<tool_name>` or a comma-separated list such as `select:Read,Edit`. Use `tool_search` before falling back to generic resource-listing tools; call the newly surfaced tool on a later model request.";
    const ANTHROPIC_APPLY_PATCH_NOTE: &str = "<system-reminder>\nUse `apply_patch` for multi-file edits when a structured patch is clearer than separate edit calls. The input must contain the complete *** Begin Patch / *** End Patch document. The `patch` argument must contain the complete patch body.\n</system-reminder>";
    const OPENAI_APPLY_PATCH_NOTE: &str = "# Tool-call contract\nUse `apply_patch` for multi-file edits when a structured patch is clearer than separate edit calls. The input must contain the complete *** Begin Patch / *** End Patch document. When surfaced as a freeform tool, pass the raw patch body. If surfaced as a JSON function tool, put the same patch body in the `patch` string.";
    const ANTHROPIC_TOOL_SEARCH_NOTE: &str = "<system-reminder>\nSearch deferred tool metadata by capability keywords when the needed capability is not currently visible. If you know the exact tool name, query `select:<tool_name>` or a comma-separated list such as `select:Read,Edit`. Use `tool_search` before falling back to generic resource-listing tools; call the newly surfaced tool on a later model request.\n</system-reminder>";

    fn anthropic_internal_specs() -> Vec<ToolSpec> {
        vec![
            full_tool_spec(
                "task",
                "Delegate work.",
                json!({
                    "type": "object",
                    "properties": {
                        "subagent_type": { "type": "string", "enum": ["Explore", "general-purpose"] },
                        "model": { "type": "string", "description": "Model override." },
                        "run_in_background": { "type": "boolean" },
                        "resume": { "type": "string" },
                        "peer": { "type": "string" },
                        "persistent": { "type": "boolean" },
                        "fork_context": { "type": "boolean" },
                        "ignored": { "type": "string" }
                    },
                    "required": ["ignored"],
                    "additionalProperties": false
                }),
            ),
            full_tool_spec(
                "read",
                "Read a file.",
                json!({
                    "type": "object",
                    "properties": {
                        "path": { "type": "string" },
                        "offset": { "type": "integer", "minimum": 1 },
                        "limit": { "type": "integer", "minimum": 1 },
                        "pages": { "type": "string" },
                        "ignored": { "type": "boolean" }
                    },
                    "required": ["path"]
                }),
            ),
            full_tool_spec(
                "edit",
                "Edit a file.",
                json!({
                    "type": "object",
                    "properties": {
                        "path": { "type": "string" },
                        "oldString": { "type": "string", "minLength": 1 },
                        "newString": { "type": "string" },
                        "replaceAll": { "type": "boolean" },
                        "ignored": { "type": "boolean" }
                    },
                    "required": ["path", "oldString", "newString"]
                }),
            ),
            full_tool_spec(
                "glob",
                "Find files by glob.",
                json!({
                    "type": "object",
                    "properties": {
                        "pattern": { "type": "string" },
                        "path": { "type": "string" },
                        "ignored": { "type": "boolean" }
                    },
                    "required": ["pattern"]
                }),
            ),
            full_tool_spec(
                "grep",
                "Search file contents.",
                json!({
                    "type": "object",
                    "properties": {
                        "pattern": { "type": "string" },
                        "path": { "type": "string" },
                        "glob": { "type": "string" },
                        "output_mode": { "type": "string", "enum": ["content", "files_with_matches", "count"] },
                        "-B": { "type": "integer", "minimum": 0 },
                        "-A": { "type": "integer", "minimum": 0 },
                        "-C": { "type": "integer", "minimum": 0 },
                        "context": { "type": "integer", "minimum": 0 },
                        "-n": { "type": "boolean" },
                        "-i": { "type": "boolean" },
                        "type": { "type": "string" },
                        "head_limit": { "type": "integer", "minimum": 0 },
                        "offset": { "type": "integer", "minimum": 0 },
                        "multiline": { "type": "boolean" },
                        "-o": { "type": "boolean" },
                        "ignored": { "type": "boolean" }
                    },
                    "required": ["pattern"]
                }),
            ),
            full_tool_spec(
                "bash",
                "Run shell command.",
                json!({
                    "type": "object",
                    "properties": {
                        "command": { "type": "string" },
                        "timeout": { "type": "integer", "minimum": 1 },
                        "cwd": { "type": "string" },
                        "runInBackground": { "type": "boolean" },
                        "ignored": { "type": "boolean" }
                    },
                    "required": ["command"]
                }),
            ),
        ]
    }

    fn openai_agent_lifecycle_internal_specs() -> Vec<ToolSpec> {
        vec![
            full_tool_spec(
                "task",
                "Delegate work.",
                json!({
                    "type": "object",
                    "properties": {
                        "description": { "type": "string" },
                        "prompt": { "type": "string" },
                        "subagent_type": { "type": "string", "enum": ["Explore", "general-purpose"] },
                        "agentType": { "type": "string", "description": "lower precedence" },
                        "model": { "type": "string", "description": "old model description", "default": "gpt-5-mini" },
                        "fork_turns": { "type": "string", "description": "Conversation history to inherit: all (default) or none. Positive integer strings also inherit the full conversation." },
                        "run_in_background": { "type": "boolean" },
                        "resume": { "type": "string" },
                        "peer": { "type": "string" },
                        "persistent": { "type": "boolean" },
                        "reasoning_effort": { "type": "string" },
                        "service_tier": { "type": "string" }
                    },
                    "required": ["description", "prompt"]
                }),
            ),
            full_tool_spec(
                "task_output",
                "Read a background task.",
                json!({
                    "type": "object",
                    "properties": {
                        "task_id": { "type": "string" },
                        "block": { "type": "boolean" },
                        "timeout": { "type": "number", "minimum": 0 }
                    },
                    "required": ["task_id"]
                }),
            ),
            full_tool_spec(
                "task_stop",
                "Stop a background task.",
                json!({
                    "type": "object",
                    "properties": {
                        "task_id": { "type": "string" },
                        "shell_id": { "type": "string" }
                    }
                }),
            ),
        ]
    }

    fn openai_team_internal_specs() -> Vec<ToolSpec> {
        vec![
            full_tool_spec(
                "SendMessage",
                "Send a message to a teammate.",
                json!({
                    "type": "object",
                    "properties": {
                        "to": { "type": "string", "description": "old target description", "default": "worker" },
                        "message": { "type": "string", "description": "old message description" },
                        "ignored": { "type": "boolean" }
                    },
                    "required": ["to", "message"]
                }),
            ),
            full_tool_spec(
                "ListPeers",
                "List peers.",
                json!({
                    "type": "object",
                    "properties": {
                        "path_prefix": { "type": "string" },
                        "ignored": { "type": "boolean" }
                    }
                }),
            ),
        ]
    }

    fn openai_runtime_internal_specs() -> Vec<ToolSpec> {
        let mut tool_search = full_tool_spec(
            "tool_search",
            "Search tools.",
            json!({
                "type": "object",
                "title": "Deferred tool search",
                "properties": {
                    "query": { "type": "string", "minLength": 1 },
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "default": 5,
                        "description": "old limit description"
                    },
                    "max_results": { "type": "integer" }
                },
                "required": ["query", "max_results"],
                "additionalProperties": true,
                "x-provider": { "default": "extension data" }
            }),
        );
        tool_search.format = Some(ToolSpecFormat::Text);

        vec![
            full_tool_spec(
                "exec_command",
                "Run command.",
                json!({
                    "type": "object",
                    "properties": {
                        "command": { "type": "string", "default": "pwd" },
                        "cwd": { "type": "string" },
                        "yieldMs": { "type": "number", "minimum": 0 },
                        "tty": { "type": "boolean" },
                        "max_output_tokens": { "type": "integer" },
                        "shell": { "type": "string" },
                        "login": { "type": "boolean" }
                    },
                    "required": ["command"]
                }),
            ),
            full_tool_spec(
                "write_stdin",
                "Write stdin.",
                json!({
                    "type": "object",
                    "properties": {
                        "sessionId": { "type": "string" },
                        "input": { "type": "string" },
                        "yieldMs": { "type": "number", "minimum": 0 },
                        "ignored": { "type": "boolean" }
                    },
                    "required": ["sessionId"]
                }),
            ),
            tool_search,
        ]
    }

    #[test]
    fn anthropic_facade_specs_match_fixed_full_json() {
        let rendered =
            render_tool_specs_for_behavior(&anthropic_internal_specs(), &anthropic_profile());
        assert_eq!(rendered.len(), 6);
        assert_eq!(
            crate::runtime_values::runtime_codec::encode_value(&rendered)
                .expect("encode rendered specs"),
            json!([
                {
                    "name": "Task",
                    "description": (format!("Delegate work.\n\n{ANTHROPIC_TASK_NOTE}")),
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "description": { "type": "string", "description": "A short description of the task." },
                            "prompt": { "type": "string", "description": "Task for the agent." },
                            "subagent_type": { "type": "string", "enum": ["Explore", "general-purpose"] },
                            "model": { "type": "string", "description": "Model override." },
                            "run_in_background": { "type": "boolean" },
                            "peer": { "type": "string" },
                            "persistent": { "type": "boolean" }
                        },
                        "required": ["description", "prompt", "model"],
                        "additionalProperties": false
                    }
                },
                {
                    "name": "Read",
                    "description": (format!("Read a file.\n\n{ANTHROPIC_READ_NOTE}")),
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "file_path": { "type": "string", "description": "The absolute or cwd-relative path to read." },
                            "offset": { "type": "integer", "minimum": 1 },
                            "limit": { "type": "integer", "minimum": 1 },
                            "pages": { "type": "string" }
                        },
                        "required": ["file_path"],
                        "additionalProperties": false
                    }
                },
                {
                    "name": "Edit",
                    "description": (format!("Edit a file.\n\n{ANTHROPIC_EDIT_NOTE}")),
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "file_path": { "type": "string", "description": "The absolute or cwd-relative path to modify." },
                            "old_string": { "type": "string", "minLength": 1 },
                            "new_string": { "type": "string" },
                            "replace_all": { "type": "boolean" }
                        },
                        "required": ["file_path", "old_string", "new_string"],
                        "additionalProperties": false
                    }
                },
                {
                    "name": "Glob",
                    "description": (format!("Find files by glob.\n\n{ANTHROPIC_GLOB_NOTE}")),
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "pattern": { "type": "string", "description": "Glob pattern to match files against." },
                            "path": { "type": "string" }
                        },
                        "required": ["pattern"],
                        "additionalProperties": false
                    }
                },
                {
                    "name": "Grep",
                    "description": (format!("Search file contents.\n\n{ANTHROPIC_GREP_NOTE}")),
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "pattern": { "type": "string", "description": "Regular expression to search for in file contents." },
                            "path": { "type": "string" },
                            "glob": { "type": "string" },
                            "output_mode": { "type": "string", "enum": ["content", "files_with_matches", "count"] },
                            "-B": { "type": "integer", "minimum": 0 },
                            "-A": { "type": "integer", "minimum": 0 },
                            "-C": { "type": "integer", "minimum": 0 },
                            "context": { "type": "integer", "minimum": 0 },
                            "-n": { "type": "boolean" },
                            "-i": { "type": "boolean" },
                            "type": { "type": "string" },
                            "head_limit": { "type": "integer", "minimum": 0 },
                            "offset": { "type": "integer", "minimum": 0 },
                            "multiline": { "type": "boolean" },
                            "-o": { "type": "boolean" }
                        },
                        "required": ["pattern"],
                        "additionalProperties": false
                    }
                },
                {
                    "name": "Bash",
                    "description": (format!("Run shell command.\n\n{ANTHROPIC_BASH_NOTE}")),
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "command": { "type": "string" },
                            "timeout": { "type": "integer", "minimum": 1 },
                            "description": { "type": "string", "description": "Clear, concise description of what this command does." },
                            "cwd": { "type": "string" },
                            "run_in_background": { "type": "boolean" }
                        },
                        "required": ["command"],
                        "additionalProperties": false
                    }
                }
            ])
        );
    }

    #[test]
    fn openai_agent_lifecycle_specs_match_fixed_full_json() {
        let rendered = render_tool_specs_for_behavior(
            &openai_agent_lifecycle_internal_specs(),
            &openai_profile(),
        );
        assert_eq!(rendered.len(), 3);
        assert_eq!(
            crate::runtime_values::runtime_codec::encode_value(&rendered)
                .expect("encode rendered specs"),
            json!([
                {
                    "name": "spawn_agent",
                    "description": (format!("Delegate work.\n\n{OPENAI_TASK_NOTE}")),
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "task_name": {
                                "type": "string",
                                "description": "Explicit stable label/name for a new subagent. Use followup_task to resume an existing subagent."
                            },
                            "message": {
                                "type": "string",
                                "description": "Self-contained instructions for the spawned agent."
                            },
                            "agent_type": {
                                "type": "string",
                                "enum": ["Explore", "general-purpose"]
                            },
                            "model": {
                                "type": "string",
                                "description": "Required for every new child task. Select low for simple mechanical work, medium for standard implementation or investigation, and high for complex reasoning; alternatively supply a concrete model ID, qualified as source:modelId when selecting another source. Choose by task complexity without asking the user to choose a tier. This explicit selection takes precedence over role and configured child defaults. Do not omit it or use \"inherit\". Only an invalid or unavailable selection falls back to the captured parent model. Existing children keep their original model; use followup_task where available, or the message tool's explicit turn trigger."
                            },
                            "fork_turns": {
                                "type": "string",
                                "description": "Conversation history to inherit: all (default) or none. Positive integer strings also inherit the full conversation."
                            },
                            "run_in_background": { "type": "boolean" },
                            "peer": { "type": "string" },
                            "persistent": { "type": "boolean" },
                            "reasoning_effort": { "type": "string" }
                        },
                        "required": ["task_name", "message", "model"],
                        "additionalProperties": false
                    }
                },
                {
                    "name": "wait_agent",
                    "description": (format!("Read a background task.\n\n{OPENAI_WAIT_AGENT_NOTE}")),
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "target": {
                                "type": "string",
                                "description": "Background agent id returned by spawn_agent."
                            },
                            "timeout_ms": {
                                "type": "number",
                                "minimum": 0,
                                "description": "Maximum milliseconds to wait for the background agent."
                            }
                        },
                        "required": ["target"],
                        "additionalProperties": false
                    }
                },
                {
                    "name": "interrupt_agent",
                    "description": (format!("Stop a background task.\n\n{OPENAI_INTERRUPT_AGENT_NOTE}")),
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "target": {
                                "type": "string",
                                "description": "Background agent id to stop."
                            }
                        },
                        "required": ["target"],
                        "additionalProperties": false
                    }
                }
            ])
        );
    }

    #[test]
    fn openai_team_facade_specs_match_fixed_full_json() {
        let rendered =
            render_tool_specs_for_behavior(&openai_team_internal_specs(), &openai_profile());
        assert_eq!(rendered.len(), 3);
        assert_eq!(
            crate::runtime_values::runtime_codec::encode_value(&rendered)
                .expect("encode rendered specs"),
            json!([
                {
                    "name": "send_message",
                    "description": (format!("Send a message to a teammate.\n\n{OPENAI_SEND_MESSAGE_NOTE}")),
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "target": {
                                "type": "string",
                                "description": "Existing subagent ID or name, parent for the caller's direct parent, primary agent ID, or a teammate name."
                            },
                            "message": {
                                "type": "string",
                                "description": "Task direction for the target agent."
                            },
                            "summary": { "type": "string", "description": "A short preview of the message." }
                        },
                        "required": ["target", "message"],
                        "additionalProperties": false
                    }
                },
                {
                    "name": "followup_task",
                    "description": (format!("Send a message to a teammate.\n\n{OPENAI_FOLLOWUP_TASK_NOTE}")),
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "target": {
                                "type": "string",
                                "description": "Existing ordinary subagent ID or name."
                            },
                            "message": {
                                "type": "string",
                                "description": "Plain-text follow-up task direction for the ordinary subagent."
                            },
                            "summary": { "type": "string", "description": "A short preview of the message." }
                        },
                        "required": ["target", "message"],
                        "additionalProperties": false
                    }
                },
                {
                    "name": "list_agents",
                    "description": (format!("List peers.\n\n{OPENAI_LIST_AGENTS_NOTE}")),
                    "inputSchema": {
                        "type": "object",
                        "properties": {},
                        "additionalProperties": false
                    }
                }
            ])
        );
    }

    #[test]
    fn openai_runtime_facade_specs_match_fixed_full_json() {
        let rendered =
            render_tool_specs_for_behavior(&openai_runtime_internal_specs(), &openai_profile());
        assert_eq!(rendered.len(), 3);
        assert_eq!(
            crate::runtime_values::runtime_codec::encode_value(&rendered)
                .expect("encode rendered specs"),
            json!([
                {
                    "name": "exec_command",
                    "description": (format!("Run command.\n\n{OPENAI_EXEC_COMMAND_NOTE}")),
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "cmd": {
                                "type": "string",
                                "description": "Shell command to execute."
                            },
                            "workdir": {
                                "type": "string",
                                "description": "Working directory for the command. Defaults to the turn cwd."
                            },
                            "yield_time_ms": {
                                "type": "number",
                                "minimum": 0,
                                "description": "Wait before yielding output. Defaults to 10000 ms."
                            }
                        },
                        "required": ["cmd"],
                        "additionalProperties": false
                    }
                },
                {
                    "name": "write_stdin",
                    "description": (format!("Write stdin.\n\n{OPENAI_WRITE_STDIN_NOTE}")),
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "session_id": {
                                "type": "string",
                                "description": "Identifier of the running exec_command session."
                            },
                            "chars": {
                                "type": "string",
                                "description": "Bytes to write to stdin. Omit or pass an empty string to poll."
                            },
                            "yield_time_ms": {
                                "type": "number",
                                "minimum": 0,
                                "description": "Wait before yielding output. Defaults depend on whether chars is empty."
                            }
                        },
                        "required": ["session_id"],
                        "additionalProperties": false
                    }
                },
                {
                    "name": "tool_search",
                    "description": (format!("Search tools.\n\n{OPENAI_TOOL_SEARCH_NOTE}")),
                    "inputSchema": {
                        "type": "object",
                        "title": "Deferred tool search",
                        "properties": {
                            "query": { "type": "string", "minLength": 1 },
                            "limit": {
                                "type": "number",
                                "minimum": 1,
                                "description": "Optional maximum number of matching tools to surface."
                            }
                        },
                        "required": ["query"],
                        "additionalProperties": true,
                        "x-provider": { "default": "extension data" }
                    },
                    "kind": "tool_search",
                    "format": { "type": "text" }
                }
            ])
        );
    }

    #[test]
    fn tool_description_fragments_match_fixed_full_strings() {
        assert_eq!(
            provider_tool_description_fragment_ids(),
            [
                "read-file-window",
                "edit-exact-replacement",
                "glob-file-pattern",
                "grep-search-flow",
                "bash-command-contract",
                "task-delegation-contract",
                "apply-patch-contract",
                "tool-search-discovery",
                "exec-command-contract",
                "write-stdin-contract",
                "wait-agent-contract",
                "interrupt-agent-contract",
                "send-message-contract",
                "list-agents-contract",
            ]
        );

        let anthropic = anthropic_profile();
        let openai = openai_profile();
        let cases = [
            ("read-file-window", "read", true, ANTHROPIC_READ_NOTE),
            ("edit-exact-replacement", "edit", true, ANTHROPIC_EDIT_NOTE),
            ("glob-file-pattern", "glob", true, ANTHROPIC_GLOB_NOTE),
            ("grep-search-flow", "grep", true, ANTHROPIC_GREP_NOTE),
            ("bash-command-contract", "bash", true, ANTHROPIC_BASH_NOTE),
            (
                "task-delegation-contract",
                "task",
                true,
                ANTHROPIC_TASK_NOTE,
            ),
            ("task-delegation-contract", "task", false, OPENAI_TASK_NOTE),
            (
                "apply-patch-contract",
                "apply_patch",
                true,
                ANTHROPIC_APPLY_PATCH_NOTE,
            ),
            (
                "apply-patch-contract",
                "apply_patch",
                false,
                OPENAI_APPLY_PATCH_NOTE,
            ),
            (
                "tool-search-discovery",
                "tool_search",
                true,
                ANTHROPIC_TOOL_SEARCH_NOTE,
            ),
            (
                "tool-search-discovery",
                "tool_search",
                false,
                OPENAI_TOOL_SEARCH_NOTE,
            ),
            (
                "exec-command-contract",
                "exec_command",
                false,
                OPENAI_EXEC_COMMAND_NOTE,
            ),
            (
                "write-stdin-contract",
                "write_stdin",
                false,
                OPENAI_WRITE_STDIN_NOTE,
            ),
            (
                "wait-agent-contract",
                "task_output",
                false,
                OPENAI_WAIT_AGENT_NOTE,
            ),
            (
                "interrupt-agent-contract",
                "task_stop",
                false,
                OPENAI_INTERRUPT_AGENT_NOTE,
            ),
            (
                "send-message-contract",
                "SendMessage",
                false,
                OPENAI_SEND_MESSAGE_NOTE,
            ),
            (
                "list-agents-contract",
                "ListPeers",
                false,
                OPENAI_LIST_AGENTS_NOTE,
            ),
        ];
        assert_eq!(cases.len(), 17);
        for (id, tool_name, use_anthropic, expected) in cases {
            let profile = if use_anthropic { &anthropic } else { &openai };
            assert_eq!(
                render_tool_description_fragments(&RuntimeString::from(tool_name), profile),
                Some(expected.into()),
                "fragment {id}"
            );
        }
    }

    #[test]
    fn numeric_identifiers_preserve_javascript_formatting_without_coercing_fork_strings() {
        let openai = openai_profile();
        for fork_turns in ["3", "9007199254740993", "18446744073709551615"] {
            let normalized = normalize_tool_input_for_behavior(
                &runtime_string("spawn_agent"),
                json!({"task_name":"history", "message":"inherit", "fork_turns":fork_turns}),
                &openai,
            );
            assert_eq!(normalized.get("fork_turns"), Some(&json!(fork_turns)));
            assert!(normalized.get("fork_context").is_none());
        }

        let unsafe_integer = RuntimeJson::Number(RuntimeNumber::from_f64(
            "9007199254740993".parse().expect("valid JavaScript number"),
        ));
        let u64_max = RuntimeJson::Number(RuntimeNumber::from_f64(
            "18446744073709551615"
                .parse()
                .expect("valid JavaScript number"),
        ));
        for (number, expected) in [
            (unsafe_integer, "9007199254740992"),
            (u64_max, "18446744073709552000"),
        ] {
            assert_eq!(
                normalize_tool_input_for_behavior(
                    &runtime_string("write_stdin"),
                    json!({ "session_id": (number.clone()) }),
                    &openai,
                ),
                json!({ "sessionId": expected })
            );
            assert_eq!(
                normalize_tool_input_for_behavior(
                    &runtime_string("wait_agent"),
                    json!({ "target": number }),
                    &openai,
                ),
                json!({ "task_id": expected })
            );
        }

        let notation_cases = [
            ("1e-7", "1e-7"),
            ("1e-6", "0.000001"),
            ("1e20", "100000000000000000000"),
            ("1e21", "1e+21"),
            ("-0", "0"),
            ("-42", "-42"),
            ("1.25", "1.25"),
        ];
        assert_eq!(notation_cases.len(), 7);
        for (token, expected) in notation_cases {
            let number = RuntimeJson::Number(RuntimeNumber::from_f64(
                token.parse().expect("valid JavaScript number token"),
            ));
            assert_eq!(
                normalize_tool_input_for_behavior(
                    &runtime_string("write_stdin"),
                    json!({ "session_id": (number.clone()) }),
                    &openai,
                ),
                json!({ "sessionId": expected }),
                "write_stdin token {token}"
            );
            assert_eq!(
                normalize_tool_input_for_behavior(
                    &runtime_string("wait_agent"),
                    json!({ "target": number }),
                    &openai,
                ),
                json!({ "task_id": expected }),
                "wait_agent token {token}"
            );
        }

        assert_eq!(
            normalize_tool_input_for_behavior(
                &runtime_string("write_stdin"),
                json!({ "session_id": 42 }),
                &openai,
            ),
            json!({ "sessionId": "42" })
        );
        assert_eq!(
            normalize_tool_input_for_behavior(
                &runtime_string("write_stdin"),
                json!({ "session_id": "session-42" }),
                &openai,
            ),
            json!({ "sessionId": "session-42" })
        );
    }

    #[test]
    fn reverse_input_normalization_matches_fixed_literal_table() {
        let anthropic = anthropic_profile();
        let openai = openai_profile();
        let anthropic_cases = [
            (
                "Task",
                json!({ "description": "scan", "prompt": "read docs", "persistent": true }),
                json!({ "description": "scan", "prompt": "read docs", "persistent": true }),
            ),
            (
                "Read",
                json!({ "file_path": "src/a.rs", "offset": 2, "limit": 5 }),
                json!({ "offset": 2, "limit": 5, "path": "src/a.rs" }),
            ),
            (
                "Read",
                json!({ "file_path": null, "path": "fallback.rs", "pages": "1-2" }),
                json!({ "path": "fallback.rs", "pages": "1-2" }),
            ),
            (
                "Read",
                json!({ "offset": 1 }),
                json!({ "offset": 1, "path": "" }),
            ),
            (
                "Edit",
                json!({
                    "file_path": "a",
                    "old_string": "x",
                    "new_string": "y",
                    "replace_all": true
                }),
                json!({ "path": "a", "oldString": "x", "newString": "y", "replaceAll": true }),
            ),
            (
                "Edit",
                json!({
                    "file_path": "",
                    "path": "fallback.rs",
                    "old_string": null,
                    "oldString": "old fallback",
                    "new_string": 7,
                    "newString": "new fallback",
                    "replace_all": null,
                    "extra": "kept"
                }),
                json!({
                    "path": "",
                    "oldString": "old fallback",
                    "newString": "new fallback",
                    "extra": "kept",
                    "replaceAll": null
                }),
            ),
            (
                "Glob",
                json!({ "pattern": "**/*.rs", "path": "src" }),
                json!({ "pattern": "**/*.rs", "path": "src" }),
            ),
            (
                "Grep",
                json!({ "pattern": "TODO", "output_mode": "files_with_matches", "-i": true }),
                json!({ "pattern": "TODO", "output_mode": "files_with_matches", "-i": true }),
            ),
            (
                "Bash",
                json!({
                    "command": "cargo test",
                    "description": "run tests",
                    "run_in_background": true,
                    "timeout": 1000
                }),
                json!({ "command": "cargo test", "timeout": 1000, "runInBackground": true }),
            ),
            (
                "Bash",
                json!({ "command": "pwd", "description": null, "run_in_background": null }),
                json!({ "command": "pwd", "runInBackground": null }),
            ),
            (
                "Bash",
                json!({ "command": "pwd", "runInBackground": false }),
                json!({ "command": "pwd", "runInBackground": false }),
            ),
            (
                "Read",
                json!(["not", "an", "object"]),
                json!(["not", "an", "object"]),
            ),
            (
                "Unknown",
                json!({ "file_path": "unchanged" }),
                json!({ "file_path": "unchanged" }),
            ),
        ];
        assert_eq!(anthropic_cases.len(), 13);
        for (visible_name, input, expected) in anthropic_cases {
            assert_eq!(
                normalize_tool_input_for_behavior(&runtime_string(visible_name), input, &anthropic),
                expected,
                "Anthropic {visible_name}"
            );
        }

        let openai_cases = [
            (
                "spawn_agent",
                json!({
                    "task_name": "scan docs",
                    "message": "read notes",
                    "agent_type": "Explore",
                    "fork_turns": "none"
                }),
                json!({
                    "description": "scan docs",
                    "name": "scan docs",
                    "prompt": "read notes",
                    "subagent_type": "Explore",
                    "fork_turns": "none"
                }),
            ),
            (
                "spawn_agent",
                json!({ "task_name": "all", "message": "inherit", "fork_turns": " ALL " }),
                json!({ "description": "all", "name": "all", "prompt": "inherit", "fork_turns": " ALL " }),
            ),
            (
                "spawn_agent",
                json!({ "task_name": "recent", "message": "inherit", "fork_turns": " 3 " }),
                json!({ "description": "recent", "name": "recent", "prompt": "inherit", "fork_turns": " 3 " }),
            ),
            (
                "spawn_agent",
                json!({ "task_name": "bool", "message": "inherit", "fork_turns": false }),
                json!({ "description": "bool", "name": "bool", "prompt": "inherit", "fork_turns": false }),
            ),
            (
                "spawn_agent",
                json!({ "task_name": "num", "message": "inherit", "fork_turns": 4 }),
                json!({ "description": "num", "name": "num", "prompt": "inherit", "fork_turns": 4 }),
            ),
            (
                "spawn_agent",
                json!({ "task_name": "bad", "message": "inherit", "fork_turns": " 03x " }),
                json!({ "description": "bad", "name": "bad", "prompt": "inherit", "fork_turns": " 03x " }),
            ),
            (
                "spawn_agent",
                json!({ "task_name": "nulls", "message": "kept", "agent_type": null, "fork_turns": null }),
                json!({
                    "description": "nulls",
                    "name": "nulls",
                    "prompt": "kept",
                    "subagent_type": null,
                    "fork_turns": null
                }),
            ),
            (
                "spawn_agent",
                json!({
                    "task_name": 9,
                    "description": "fallback label",
                    "message": false,
                    "prompt": "fallback prompt",
                    "peer": "worker"
                }),
                json!({
                    "description": "fallback label",
                    "prompt": "fallback prompt",
                    "peer": "worker"
                }),
            ),
            (
                "spawn_agent",
                json!({ "task_name": "", "message": "", "description": "ignored", "prompt": "ignored" }),
                json!({ "description": "", "prompt": "", "name": "" }),
            ),
            ("spawn_agent", json!("raw"), json!("raw")),
            (
                "exec_command",
                json!({ "cmd": "echo hi", "workdir": "src", "yield_time_ms": 250 }),
                json!({ "command": "echo hi", "cwd": "src", "yieldMs": 250 }),
            ),
            (
                "exec_command",
                json!({ "cmd": null, "command": "fallback", "workdir": null, "yield_time_ms": null }),
                json!({ "command": "fallback", "cwd": null, "yieldMs": null }),
            ),
            (
                "exec_command",
                json!({ "cmd": "", "command": "ignored", "extra": true }),
                json!({ "command": "", "extra": true }),
            ),
            (
                "write_stdin",
                json!({ "session_id": 42, "chars": "exit\n", "yield_time_ms": 500 }),
                json!({ "sessionId": "42", "input": "exit\n", "yieldMs": 500 }),
            ),
            (
                "write_stdin",
                json!({ "session_id": false, "sessionId": "fallback", "chars": null }),
                json!({ "sessionId": "fallback", "input": null }),
            ),
            (
                "write_stdin",
                json!({ "session_id": null, "yield_time_ms": null, "extra": 1 }),
                json!({ "extra": 1, "sessionId": "", "yieldMs": null }),
            ),
            (
                "wait_agent",
                json!({ "target": "agent-1", "timeout_ms": 500 }),
                json!({ "task_id": "agent-1", "timeout": 500 }),
            ),
            (
                "wait_agent",
                json!({ "targets": 7, "timeout_ms": null }),
                json!({ "task_id": "7", "timeout": null }),
            ),
            (
                "wait_agent",
                json!({ "targets": [null, false, 8, "later"] }),
                json!({ "task_id": "8" }),
            ),
            (
                "wait_agent",
                json!({ "target": {}, "targets": [null], "task_id": "fallback" }),
                json!({ "task_id": "fallback" }),
            ),
            ("wait_agent", json!({}), json!({ "task_id": "" })),
            (
                "interrupt_agent",
                json!({ "target": 9, "extra": true }),
                json!({ "extra": true, "task_id": "9" }),
            ),
            (
                "interrupt_agent",
                json!({ "target": null, "task_id": "fallback" }),
                json!({ "task_id": "fallback" }),
            ),
            (
                "send_message",
                json!({ "target": 5, "message": "hello" }),
                json!({ "message": "hello", "to": "5", "summary": "hello" }),
            ),
            (
                "send_message",
                json!({ "target": null, "to": "", "message": null }),
                json!({ "to": "", "message": null }),
            ),
            (
                "followup_task",
                json!({ "target": "worker", "message": "continue" }),
                json!({ "message": "continue", "to": "worker", "summary": "continue", "trigger_turn": true }),
            ),
            (
                "list_agents",
                json!({ "path_prefix": "ignored", "other": null }),
                json!({ "other": null }),
            ),
            ("list_agents", json!(false), json!(false)),
            (
                "apply_patch",
                json!("*** Begin Patch\n*** End Patch\n"),
                json!({ "patch": "*** Begin Patch\n*** End Patch\n" }),
            ),
            (
                "apply_patch",
                json!({ "patch": "already wrapped" }),
                json!({ "patch": "already wrapped" }),
            ),
            (
                "tool_search",
                json!({ "query": "browser", "limit": 3 }),
                json!({ "query": "browser", "limit": 3 }),
            ),
            (
                "unknown_visible",
                json!({ "target": "unchanged" }),
                json!({ "target": "unchanged" }),
            ),
        ];
        assert_eq!(openai_cases.len(), 32);
        for (visible_name, input, expected) in openai_cases {
            assert_eq!(
                normalize_tool_input_for_behavior(&runtime_string(visible_name), input, &openai),
                expected,
                "OpenAI {visible_name}"
            );
        }

        for (visible, internal) in [
            ("Task", "task"),
            ("Read", "read"),
            ("Edit", "edit"),
            ("Glob", "glob"),
            ("Grep", "grep"),
            ("Bash", "bash"),
        ] {
            assert_eq!(
                resolve_internal_tool_name(&runtime_string(visible), &anthropic),
                internal
            );
        }
        for (visible, internal) in [
            ("spawn_agent", "task"),
            ("wait_agent", "task_output"),
            ("interrupt_agent", "task_stop"),
            ("send_message", "SendMessage"),
            ("followup_task", "SendMessage"),
            ("list_agents", "ListPeers"),
            ("exec_command", "exec_command"),
            ("write_stdin", "write_stdin"),
            ("apply_patch", "apply_patch"),
            ("tool_search", "tool_search"),
            ("unknown_visible", "unknown_visible"),
        ] {
            assert_eq!(
                resolve_internal_tool_name(&runtime_string(visible), &openai),
                internal
            );
        }
    }

    #[test]
    fn spawn_normalization_names_fresh_agents_without_renaming_resume_targets() {
        let profile = openai_profile();
        for (input, expected) in [
            (
                json!({"task_name":"reader","message":"Read notes"}),
                json!({"description":"reader","name":"reader","prompt":"Read notes"}),
            ),
            (
                json!({"task_name":"reader","name":"stable-reader","message":"Read notes"}),
                json!({"name":"stable-reader","description":"reader","prompt":"Read notes"}),
            ),
            (
                json!({"task_name":"follow up","message":"Continue","resume":"existing"}),
                json!({"resume":"existing","description":"follow up","prompt":"Continue"}),
            ),
            (
                json!({"task_name":"reader","message":"Read notes","resume":null}),
                json!({"resume":null,"description":"reader","name":"reader","prompt":"Read notes"}),
            ),
            (
                json!({"task_name":"reader","name":null,"message":"Read notes"}),
                json!({"name":null,"description":"reader","prompt":"Read notes"}),
            ),
            (
                json!({"task_name":"follow up","message":"Continue","resume":""}),
                json!({"resume":"","description":"follow up","prompt":"Continue"}),
            ),
        ] {
            assert_eq!(
                normalize_tool_input_for_behavior(&runtime_string("spawn_agent"), input, &profile),
                expected,
            );
        }
    }

    #[test]
    fn task_rendering_preserves_the_optional_native_name_without_renaming_resume_targets() {
        let tool = crate::tools::builtins::task::create_task_tool(Default::default());
        let spec = crate::tools::pool::to_tool_spec(tool.as_ref());
        for profile in [anthropic_profile(), openai_profile(), generic_profile()] {
            let rendered = render_tool_specs_for_behavior(std::slice::from_ref(&spec), &profile);
            assert_eq!(rendered.len(), 1);
            assert_eq!(
                rendered[0].input_schema.pointer("/properties/name"),
                spec.input_schema.pointer("/properties/name"),
            );
            assert!(!rendered[0].input_schema["required"]
                .as_array()
                .unwrap()
                .contains(&RuntimeJson::from("name")));
        }
    }

    #[test]
    fn rendered_facade_name_union_is_complete_and_preserves_collisions() {
        let anthropic_names =
            render_tool_specs_for_behavior(&anthropic_internal_specs(), &anthropic_profile())
                .into_iter()
                .map(|spec| spec.name)
                .collect::<Vec<_>>();

        let lifecycle = openai_agent_lifecycle_internal_specs();
        let runtime = openai_runtime_internal_specs();
        let team = openai_team_internal_specs();
        let openai_specs = vec![
            lifecycle[0].clone(),
            runtime[0].clone(),
            runtime[1].clone(),
            lifecycle[1].clone(),
            lifecycle[2].clone(),
            team[0].clone(),
            team[1].clone(),
            full_tool_spec(
                "apply_patch",
                "Apply a patch.",
                json!({ "type": "object", "properties": {} }),
            ),
            runtime[2].clone(),
        ];
        let openai_names = render_tool_specs_for_behavior(&openai_specs, &openai_profile())
            .into_iter()
            .map(|spec| spec.name)
            .collect::<Vec<_>>();
        let complete_union = anthropic_names
            .into_iter()
            .chain(openai_names)
            .collect::<Vec<_>>();
        assert_eq!(
            complete_union,
            [
                "Task",
                "Read",
                "Edit",
                "Glob",
                "Grep",
                "Bash",
                "spawn_agent",
                "exec_command",
                "write_stdin",
                "wait_agent",
                "interrupt_agent",
                "send_message",
                "followup_task",
                "list_agents",
                "apply_patch",
                "tool_search",
            ]
        );

        let collision = full_tool_spec(
            "send_message",
            "Existing visible collision.",
            json!({ "type": "object", "properties": {} }),
        );
        let mut send_message = team[0].clone();
        send_message.kind = ToolSpecKind::Freeform;
        send_message.format = Some(ToolSpecFormat::Text);
        let collision_rendered =
            render_tool_specs_for_behavior(&[collision, send_message], &openai_profile());
        assert_eq!(collision_rendered.len(), 3);
        assert_eq!(
            collision_rendered
                .iter()
                .map(|spec| spec.name.clone())
                .collect::<Vec<_>>(),
            ["send_message", "send_message", "followup_task"]
                .into_iter()
                .map(RuntimeString::from)
                .collect::<Vec<_>>()
        );
        assert_eq!(collision_rendered[1].kind, ToolSpecKind::Freeform);
        assert_eq!(collision_rendered[1].format, Some(ToolSpecFormat::Text));
        assert_eq!(collision_rendered[2].kind, ToolSpecKind::Freeform);
        assert_eq!(collision_rendered[2].format, Some(ToolSpecFormat::Text));

        let mut generic_specs = vec![full_tool_spec(
            "apply_patch",
            "Leave every field alone.",
            json!({
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "properties": { "patch": { "type": "string", "default": "keep" } }
            }),
        )];
        generic_specs[0].kind = ToolSpecKind::Freeform;
        generic_specs[0].format = Some(ToolSpecFormat::Grammar {
            syntax: ToolSpecGrammarSyntax::Regex,
            definition: ".*".into(),
        });
        let generic = resolve_provider_behavior_profile(
            Some(&provider("unknown")),
            &runtime_string("model-x"),
        );
        assert_eq!(
            render_tool_specs_for_behavior(&generic_specs, &generic),
            generic_specs
        );
        assert_eq!(
            resolve_internal_tool_name(&runtime_string("send_message"), &generic),
            "send_message"
        );
        assert_eq!(
            normalize_tool_input_for_behavior(
                &runtime_string("apply_patch"),
                json!("raw stays raw"),
                &generic,
            ),
            json!("raw stays raw")
        );

        let boolean_property_schema = full_tool_spec(
            "exec_command",
            "Boolean property schema is ignored.",
            json!({
                "type": "object",
                "properties": {
                    "command": true,
                    "cwd": false,
                    "yieldMs": { "type": "number" }
                }
            }),
        );
        let rendered =
            render_tool_specs_for_behavior(&[boolean_property_schema], &openai_profile());
        assert_eq!(
            rendered[0].input_schema,
            schema(json!({
                "type": "object",
                "properties": {
                    "cmd": { "type": "string", "description": "Shell command to execute." },
                    "workdir": {
                        "type": "string",
                        "description": "Working directory for the command. Defaults to the turn cwd."
                    },
                    "yield_time_ms": {
                        "type": "number",
                        "description": "Wait before yielding output. Defaults to 10000 ms."
                    }
                },
                "required": ["cmd"],
                "additionalProperties": false
            }))
        );
    }

    #[test]
    fn renders_anthropic_and_openai_facades_and_normalizes_back() {
        let anthropic = resolve_provider_behavior_profile(
            Some(&provider("anthropic")),
            &runtime_string("claude-sonnet-4-6"),
        );
        let anthropic_specs = render_tool_specs_for_behavior(
            &[
                tool_spec("task", json!({ "type": "object", "properties": {} })),
                tool_spec("read", json!({ "type": "object", "properties": {} })),
                tool_spec("edit", json!({ "type": "object", "properties": {} })),
                tool_spec("glob", json!({ "type": "object", "properties": {} })),
                tool_spec("grep", json!({ "type": "object", "properties": {} })),
                tool_spec("bash", json!({ "type": "object", "properties": {} })),
            ],
            &anthropic,
        );
        assert_eq!(
            anthropic_specs
                .iter()
                .map(|spec| spec.name.clone())
                .collect::<Vec<_>>(),
            ["Task", "Read", "Edit", "Glob", "Grep", "Bash"]
                .into_iter()
                .map(RuntimeString::from)
                .collect::<Vec<_>>()
        );
        assert_eq!(
            normalize_tool_input_for_behavior(
                &runtime_string("Edit"),
                json!({
                    "file_path": "a",
                    "old_string": "x",
                    "new_string": "y",
                    "replace_all": true
                }),
                &anthropic,
            ),
            json!({ "path": "a", "oldString": "x", "newString": "y", "replaceAll": true })
        );

        let openai =
            resolve_provider_behavior_profile(Some(&provider("openai")), &runtime_string("gpt-5"));
        let openai_specs = render_tool_specs_for_behavior(
            &[tool_spec(
                "SendMessage",
                json!({
                    "type": "object",
                    "properties": {
                        "to": { "type": "string" },
                        "message": { "type": "string" }
                    },
                    "required": ["to", "message"]
                }),
            )],
            &openai,
        );
        assert_eq!(
            openai_specs
                .iter()
                .map(|spec| spec.name.clone())
                .collect::<Vec<_>>(),
            ["send_message", "followup_task"]
                .into_iter()
                .map(RuntimeString::from)
                .collect::<Vec<_>>()
        );
        assert_eq!(
            resolve_internal_tool_name(&runtime_string("followup_task"), &openai),
            "SendMessage"
        );
    }

    #[test]
    fn openai_apply_patch_is_exact_lark_custom_tool() {
        let openai = openai_profile();
        let [rendered] = render_tool_specs_for_behavior(
            &[tool_spec(
                "apply_patch",
                json!({ "type": "object", "properties": {} }),
            )],
            &openai,
        )
        .try_into()
        .expect("apply_patch renders exactly one spec");
        let expected_grammar = include_str!("apply_patch.lark")
            .strip_suffix('\n')
            .expect("grammar file has one repository newline");
        assert_eq!(
            expected_grammar,
            concat!(
                "start: begin_patch hunk+ end_patch\n",
                "begin_patch: \"*** Begin Patch\" LF\n",
                "end_patch: \"*** End Patch\" LF?\n",
                "\n",
                "hunk: add_hunk | delete_hunk | update_hunk\n",
                "add_hunk: \"*** Add File: \" filename LF add_line+\n",
                "delete_hunk: \"*** Delete File: \" filename LF\n",
                "update_hunk: \"*** Update File: \" filename LF change_move? change?\n",
                "\n",
                "filename: /(.+)/\n",
                "add_line: \"+\" /(.*)/ LF -> line\n",
                "\n",
                "change_move: \"*** Move to: \" filename LF\n",
                "change: (change_context | change_line)+ eof_line?\n",
                "change_context: (\"@@\" | \"@@ \" /(.+)/) LF\n",
                "change_line: (\"+\" | \"-\" | \" \") /(.*)/ LF\n",
                "eof_line: \"*** End of File\" LF\n",
                "\n",
                "%import common.LF"
            )
        );
        assert_eq!(rendered.kind, ToolSpecKind::Freeform);
        assert_eq!(
            rendered.format,
            Some(ToolSpecFormat::Grammar {
                syntax: ToolSpecGrammarSyntax::Lark,
                definition: expected_grammar.into(),
            })
        );
        assert_eq!(
            crate::runtime_values::runtime_codec::encode_value(&rendered)
                .expect("encode apply_patch spec"),
            json!({
                "name": "apply_patch",
                "description": (format!("apply_patch description\n\n{OPENAI_APPLY_PATCH_NOTE}")),
                "inputSchema": { "type": "object", "properties": {} },
                "kind": "freeform",
                "format": {
                    "type": "grammar",
                    "syntax": "lark",
                    "definition": expected_grammar
                }
            })
        );
        let ToolSpecFormat::Grammar { definition, .. } = rendered.format.unwrap() else {
            panic!("apply_patch must use grammar format");
        };
        assert!(!definition.ends_with("\n"));
    }

    #[test]
    fn classification_and_adapters_preserve_exact_runtime_carriers() {
        let provider_name = RuntimeString::from_utf16(vec![
            u16::from(b'v'),
            u16::from(b'e'),
            u16::from(b'n'),
            0xd800,
            u16::from(b'r'),
        ]);
        let model = RuntimeString::from_utf16(vec![
            u16::from(b'g'),
            u16::from(b'p'),
            u16::from(b't'),
            u16::from(b'-'),
            0xdc00,
        ]);
        let info = ProviderInfo {
            name: provider_name.clone(),
            base_url: None,
            env_key: None,
            default_model: model.clone(),
        };
        let profile = resolve_provider_behavior_profile(Some(&info), &model);
        assert_eq!(profile.provider.as_utf16(), provider_name.as_utf16());
        assert_eq!(profile.family, Some("gpt"));

        let tool_name = RuntimeString::from_utf16(vec![u16::from(b'x'), 0xd800]);
        let description = RuntimeString::from_utf16(vec![u16::from(b'd'), 0xdc00]);
        let rendered = render_tool_specs_for_behavior(
            &[ToolSpec {
                name: tool_name.clone(),
                description: description.clone(),
                input_schema: json!({ "type": "object", "properties": {} }),
                kind: ToolSpecKind::Function,
                format: None,
            }],
            &profile,
        );
        assert_eq!(rendered[0].name.as_utf16(), tool_name.as_utf16());
        assert_eq!(rendered[0].description.as_utf16(), description.as_utf16());
        assert_eq!(
            resolve_internal_tool_name(&tool_name, &profile).as_utf16(),
            tool_name.as_utf16()
        );

        let payload = RuntimeString::from_utf16(vec![u16::from(b'p'), 0xd800, 0xdc00, 0xdfff]);
        let mut input = RuntimeObject::new();
        input.insert("task_name", RuntimeJson::String(payload.clone()));
        input.insert("message", RuntimeJson::String(payload.clone()));
        let normalized_input = normalize_tool_input_for_behavior(
            &runtime_string("spawn_agent"),
            RuntimeJson::Object(input),
            &profile,
        );
        assert_eq!(
            normalized_input
                .get("description")
                .and_then(RuntimeJson::as_string),
            Some(&payload)
        );
        assert_eq!(
            normalized_input
                .get("prompt")
                .and_then(RuntimeJson::as_string),
            Some(&payload)
        );

        let property_name = RuntimeString::from_utf16(vec![u16::from(b'k'), 0xdc00]);
        let mut property_schema = RuntimeObject::new();
        property_schema.insert("type", "string".into());
        property_schema.insert("description", RuntimeJson::String(payload.clone()));
        property_schema.insert("default", "strip".into());
        let mut properties = RuntimeObject::new();
        properties.insert(property_name.clone(), RuntimeJson::Object(property_schema));
        let mut schema = RuntimeObject::new();
        schema.insert("type", "object".into());
        schema.insert("properties", RuntimeJson::Object(properties));
        let normalized_schema =
            normalize_tool_input_schema_for_behavior(&RuntimeJson::Object(schema), &profile);
        let normalized_property = normalized_schema
            .get("properties")
            .and_then(RuntimeJson::as_object)
            .and_then(|properties| properties.get(property_name.clone()))
            .and_then(RuntimeJson::as_object)
            .expect("exact property key remains present");
        assert_eq!(
            normalized_property
                .get("description")
                .and_then(RuntimeJson::as_string),
            Some(&payload)
        );
        assert!(!normalized_property.contains_key("default"));
    }

    #[test]
    fn profile_resolution_is_model_first_and_keeps_qwen_generic() {
        let cases = [
            (
                "claude model overrides OpenAI provider",
                provider("OpenAI"),
                "claude-3-7-sonnet",
                "OpenAI",
                Some("claude"),
                ProviderPromptStyle::AnthropicReminder,
                ProviderToolDescriptionStyle::Directive,
                true,
                true,
                ProviderReasoningReplayMode::AnthropicSignedThinking,
                Some(EXPECTED_ANTHROPIC_REASONING_HINT),
            ),
            (
                "GPT model overrides Anthropic provider",
                provider("anthropic"),
                "gpt-5",
                "anthropic",
                Some("gpt"),
                ProviderPromptStyle::OpenAiDeveloper,
                ProviderToolDescriptionStyle::SchemaFirst,
                true,
                true,
                ProviderReasoningReplayMode::OpenAiEncryptedReasoning,
                Some(EXPECTED_OPENAI_REASONING_HINT),
            ),
            (
                "Qwen remains generic on an OpenAI-compatible endpoint",
                provider("openai-compatible"),
                "qwen-max",
                "openai-compatible",
                Some("qwen"),
                ProviderPromptStyle::Generic,
                ProviderToolDescriptionStyle::Compact,
                false,
                false,
                ProviderReasoningReplayMode::None,
                None,
            ),
            (
                "Gemini keeps generic prompt behavior",
                provider("google"),
                "gemini-2.5-pro",
                "google",
                Some("gemini"),
                ProviderPromptStyle::Generic,
                ProviderToolDescriptionStyle::Compact,
                false,
                false,
                ProviderReasoningReplayMode::None,
                None,
            ),
            (
                "unknown model and provider remain generic",
                provider("unknown"),
                "model-x",
                "unknown",
                None,
                ProviderPromptStyle::Generic,
                ProviderToolDescriptionStyle::Compact,
                false,
                false,
                ProviderReasoningReplayMode::None,
                None,
            ),
        ];

        for (
            case,
            info,
            model,
            expected_provider,
            expected_family,
            expected_prompt_style,
            expected_tool_description_style,
            expected_parallelism,
            expected_delegation,
            expected_replay,
            expected_hint,
        ) in cases
        {
            let profile = resolve_provider_behavior_profile(Some(&info), &runtime_string(model));

            assert_eq!(profile.provider, expected_provider, "{case}");
            assert_eq!(profile.family, expected_family, "{case}");
            assert_eq!(profile.prompt_style, expected_prompt_style, "{case}");
            assert_eq!(
                profile.tool_description_style, expected_tool_description_style,
                "{case}"
            );
            assert_eq!(
                profile.prefers_explicit_parallelism_hint, expected_parallelism,
                "{case}"
            );
            assert_eq!(
                profile.prefers_explicit_delegation_criteria, expected_delegation,
                "{case}"
            );
            assert_eq!(profile.reasoning_replay, expected_replay, "{case}");
            assert_eq!(profile.reasoning_hint, expected_hint, "{case}");
        }
    }

    #[test]
    fn behavior_schema_normalization_strips_only_reference_keywords() {
        let input = schema(json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$comment": "strip schema comments",
            "type": "object",
            "default": {},
            "examples": [{ "default": "data, not a schema" }],
            "deprecated": true,
            "readOnly": true,
            "writeOnly": true,
            "nullable": true,
            "contentEncoding": "base64",
            "contentMediaType": "application/json",
            "contentSchema": { "type": "object" },
            "unevaluatedProperties": false,
            "unevaluatedItems": false,
            "properties": {
                "default": {
                    "type": "string",
                    "default": "chosen-by-schema",
                    "minLength": 3
                }
            },
            "required": ["default"],
            "additionalProperties": false
        }));
        let expected = schema(json!({
            "type": "object",
            "properties": {
                "default": {
                    "type": "string",
                    "minLength": 3
                }
            },
            "required": ["default"],
            "additionalProperties": false
        }));
        let profile =
            resolve_provider_behavior_profile(Some(&provider("openai")), &runtime_string("gpt-5"));

        assert_eq!(
            normalize_tool_input_schema_for_behavior(&input, &profile),
            expected
        );
    }

    #[test]
    fn root_tool_composition_is_projected_only_for_anthropic_and_is_idempotent() {
        let input = json!({
            "type": "object",
            "properties": { "target": { "oneOf": [{"type": "string"}, {"type": "number"}] } },
            "anyOf": [{"required": ["target"]}]
        });
        let anthropic = anthropic_profile();
        let projected = normalize_tool_input_schema_for_behavior(&input, &anthropic);
        assert!(projected.get("anyOf").is_none());
        assert_eq!(projected["properties"], input["properties"]);
        assert_eq!(
            normalize_tool_input_schema_for_behavior(&projected, &anthropic),
            projected
        );
        for profile in [openai_profile(), generic_profile()] {
            assert_eq!(
                normalize_tool_input_schema_for_behavior(&input, &profile),
                input
            );
        }
    }

    #[test]
    fn schema_strategy_resolution_matches_adapter_capabilities() {
        let anthropic = ProviderSchemaStrategy {
            provider: ProviderSchemaProvider::Anthropic,
            tool_input: ToolInputSchemaStrategy {
                normalization: SchemaNormalization::Anthropic,
                strict: false,
            },
            output: OutputSchemaStrategy {
                mode: OutputSchemaMode::NativeOrForcedTool,
                strict: false,
                normalization: SchemaNormalization::Anthropic,
                fallback: Some(OutputSchemaFallback::ForcedTool),
            },
        };
        let openai = ProviderSchemaStrategy {
            provider: ProviderSchemaProvider::OpenAi,
            tool_input: ToolInputSchemaStrategy {
                normalization: SchemaNormalization::OpenAi,
                strict: false,
            },
            output: OutputSchemaStrategy {
                mode: OutputSchemaMode::NativeJsonSchema,
                strict: true,
                normalization: SchemaNormalization::OpenAi,
                fallback: None,
            },
        };
        let gemini = ProviderSchemaStrategy {
            provider: ProviderSchemaProvider::Gemini,
            tool_input: ToolInputSchemaStrategy {
                normalization: SchemaNormalization::ProviderAdapter,
                strict: false,
            },
            output: OutputSchemaStrategy {
                mode: OutputSchemaMode::AdapterSanitizedJsonSchema,
                strict: false,
                normalization: SchemaNormalization::ProviderAdapter,
                fallback: None,
            },
        };
        let generic = ProviderSchemaStrategy {
            provider: ProviderSchemaProvider::Generic,
            tool_input: ToolInputSchemaStrategy {
                normalization: SchemaNormalization::Generic,
                strict: false,
            },
            output: OutputSchemaStrategy {
                mode: OutputSchemaMode::None,
                strict: false,
                normalization: SchemaNormalization::None,
                fallback: None,
            },
        };

        let cases = [
            ("anthropic", "model-x", anthropic),
            ("openai", "gpt-5", openai),
            ("codex", "model-x", openai),
            ("gemini", "gemini-2.5-pro", gemini),
            ("google", "gemma-3", gemini),
            ("openai-compatible", "qwen-max", openai),
            ("unknown", "model-x", generic),
        ];

        for (provider_name, model, expected) in cases {
            let profile = resolve_provider_behavior_profile(
                Some(&provider(provider_name)),
                &runtime_string(model),
            );
            if model == "qwen-max" {
                assert_eq!(profile.prompt_style, ProviderPromptStyle::Generic);
            }
            assert_eq!(
                resolve_provider_schema_strategy(&profile),
                expected,
                "provider={provider_name}, model={model}"
            );
        }
    }

    #[test]
    fn schema_walker_preserves_extension_objects_and_schema_map_keys() {
        let input = schema(json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "default": { "root": "schema keyword" },
            "properties": {
                "default": {
                    "type": "string",
                    "default": "schema keyword",
                    "minLength": 1,
                    "x-provider": {
                        "default": "extension data",
                        "nested": { "default": "extension data" }
                    }
                },
                "payload": {
                    "type": "object",
                    "properties": {
                        "value": { "type": "string", "default": "schema keyword" }
                    },
                    "x-extension": {
                        "default": "extension data",
                        "nested": { "default": "extension data" }
                    }
                }
            },
            "$defs": {
                "entry": {
                    "type": "object",
                    "default": {},
                    "properties": {
                        "default": { "type": "number", "default": 1 }
                    }
                }
            },
            "allOf": [
                { "type": "object", "default": {}, "required": ["default"] }
            ],
            "additionalProperties": {
                "type": "string",
                "default": "schema keyword"
            },
            "x-provider": {
                "default": "extension data",
                "x-extension": { "default": "extension data" }
            },
            "x-extension": [
                { "default": "extension array data" }
            ]
        }));
        let expected = schema(json!({
            "type": "object",
            "properties": {
                "default": {
                    "type": "string",
                    "minLength": 1,
                    "x-provider": {
                        "default": "extension data",
                        "nested": { "default": "extension data" }
                    }
                },
                "payload": {
                    "type": "object",
                    "properties": {
                        "value": { "type": "string" }
                    },
                    "x-extension": {
                        "default": "extension data",
                        "nested": { "default": "extension data" }
                    }
                }
            },
            "$defs": {
                "entry": {
                    "type": "object",
                    "properties": {
                        "default": { "type": "number" }
                    }
                }
            },
            "allOf": [
                { "type": "object", "required": ["default"] }
            ],
            "additionalProperties": {
                "type": "string"
            },
            "x-provider": {
                "default": "extension data",
                "x-extension": { "default": "extension data" }
            },
            "x-extension": [
                { "default": "extension array data" }
            ]
        }));
        let openai =
            resolve_provider_behavior_profile(Some(&provider("openai")), &runtime_string("gpt-5"));

        assert_eq!(
            normalize_tool_input_schema_for_behavior(&input, &openai),
            expected
        );

        for key in [
            "properties",
            "$defs",
            "definitions",
            "patternProperties",
            "dependentSchemas",
        ] {
            let mut input = RuntimeObject::new();
            input.insert(
                key.to_owned(),
                json!({ "named": { "type": "string", "default": "strip" } }),
            );
            let normalized =
                normalize_tool_input_schema_for_behavior(&RuntimeJson::Object(input), &openai);
            assert_eq!(normalized[key]["named"], json!({ "type": "string" }));
        }

        for key in ["anyOf", "oneOf", "allOf", "prefixItems"] {
            let mut input = RuntimeObject::new();
            input.insert(
                key.to_owned(),
                json!([{ "type": "string", "default": "strip" }]),
            );
            let normalized =
                normalize_tool_input_schema_for_behavior(&RuntimeJson::Object(input), &openai);
            assert_eq!(normalized[key][0], json!({ "type": "string" }));
        }

        for key in [
            "items",
            "additionalProperties",
            "contains",
            "propertyNames",
            "not",
            "if",
            "then",
            "else",
        ] {
            let mut input = RuntimeObject::new();
            input.insert(
                key.to_owned(),
                json!({ "type": "string", "default": "strip" }),
            );
            let normalized =
                normalize_tool_input_schema_for_behavior(&RuntimeJson::Object(input), &openai);
            assert_eq!(normalized[key], json!({ "type": "string" }));
        }

        let qwen = resolve_provider_behavior_profile(
            Some(&provider("openai-compatible")),
            &runtime_string("qwen-max"),
        );
        let generic = resolve_provider_behavior_profile(
            Some(&provider("unknown")),
            &runtime_string("model-x"),
        );
        assert_eq!(
            normalize_tool_input_schema_for_behavior(&input, &qwen),
            input
        );
        assert_eq!(
            normalize_tool_input_schema_for_behavior(&input, &generic),
            input
        );
    }
}
