//! Source-scoped, append-only runtime reminder snapshots.
//!
//! The canonical meta messages are the journal: normal persistence, history
//! cuts and compaction own their lifetime. Request preparation only projects a
//! possible next snapshot; the turn owner admits it before calling the model.

use crate::provider::behavior::resolve_provider_behavior_profile;
use crate::runtime_values::RuntimeString;
use crate::schemas::config::ResolvedConfig;
use crate::schemas::content::{ContentBlock, TextBlock};
use crate::schemas::message::{Message, UserMessage};
use crate::types::provider::{ModelRequest, ProviderInfo, SystemPromptPart};
use sha2::{Digest, Sha256};

const PHASE: &str = "antigravity-runtime-reminders";
const ID_PREFIX: &str = "runtime-reminder-step:";
const SNAPSHOT_PREFIX: &str = "<system-reminder>\nRuntime reminder update. This snapshot supersedes all earlier runtime reminder snapshots; only the entries below remain active.\n";
const SNAPSHOT_SUFFIX: &str = "\n</system-reminder>";

pub(crate) fn is_antigravity_source(config: &ResolvedConfig) -> bool {
    config
        .model_source
        .as_ref()
        .and_then(|source| source.source.as_deref())
        == Some("antigravity")
}

fn is_official_first_party_base_url(raw: &str) -> bool {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return true;
    }
    let Ok(url) = url::Url::parse(trimmed) else {
        return false;
    };
    matches!(
        url.host_str(),
        Some(
            "api.anthropic.com"
                | "api.openai.com"
                | "generativelanguage.googleapis.com"
                | "chatgpt.com"
        )
    ) && url.port().is_none()
}

pub(crate) fn is_prefix_cached_compat_source(
    config: &ResolvedConfig,
    provider_info: Option<&ProviderInfo>,
) -> bool {
    let source = config
        .model_source
        .as_ref()
        .and_then(|source| source.source.as_deref());
    match source {
        Some(
            "kimi" | "bailian" | "minimax" | "zhipu" | "ark" | "qoder" | "qwenwork" | "workbuddy"
            | "trae",
        ) => true,
        Some("custom") => provider_info
            .and_then(|info| info.base_url.as_ref())
            .and_then(|url| url.try_to_utf8().ok())
            .is_some_and(|url| !url.trim().is_empty() && !is_official_first_party_base_url(&url)),
        _ => false,
    }
}

pub(crate) fn enabled_with_provider(
    config: &ResolvedConfig,
    provider_info: Option<&ProviderInfo>,
) -> bool {
    is_antigravity_source(config) || is_prefix_cached_compat_source(config, provider_info)
}

pub(crate) fn project_all_non_cacheable(
    config: &ResolvedConfig,
    provider_info: Option<&ProviderInfo>,
    model: &RuntimeString,
) -> bool {
    is_prefix_cached_compat_source(config, provider_info)
        || (is_antigravity_source(config)
            && resolve_provider_behavior_profile(provider_info, model).family == Some("gemini"))
}

fn is_dynamic(part: &SystemPromptPart, include_all_non_cacheable: bool) -> bool {
    part.name == "task-plan-reminder"
        || part.name == "edited-files"
        || part.name == "loop-recovery"
        || part.name == "total-tokens"
        || (part.cacheable == Some(false)
            && (include_all_non_cacheable
                || part.name == "reminder"
                || part.name == "hook-context"))
}

pub(crate) fn is_step(message: &Message) -> bool {
    matches!(message, Message::User(user)
        if user.is_meta == Some(true)
        && user.uuid.starts_with(ID_PREFIX)
        && user.content.len() == 1
        && matches!(&user.content[0], ContentBlock::Text(text)
            if text.phase.as_ref().is_some_and(|phase| phase == PHASE)))
}

/// Runtime snapshots are not task content and must never enter a summary.
/// Keep the owning replacement's original source ids so discarded steps are
/// retired with the history they accompanied.
pub(crate) fn without_steps(messages: &[Message]) -> Vec<Message> {
    messages
        .iter()
        .filter(|message| !is_step(message))
        .cloned()
        .collect()
}

fn snapshot(parts: &[SystemPromptPart], include_all_non_cacheable: bool) -> RuntimeString {
    let mut parts: Vec<_> = parts
        .iter()
        .filter(|part| is_dynamic(part, include_all_non_cacheable))
        .collect();
    parts.sort_by(|a, b| a.name.cmp(&b.name));
    let mut text = RuntimeString::from(SNAPSHOT_PREFIX);
    if parts.is_empty() {
        text = text.concat(&RuntimeString::from(
            "No runtime reminder entries are currently active.",
        ));
    } else {
        for part in parts {
            text = text
                .concat(&RuntimeString::from("\n["))
                .concat(&part.name)
                .concat(&RuntimeString::from("]\n"))
                .concat(&part.text)
                .concat(&RuntimeString::from("\n"));
        }
    }
    text.concat(&RuntimeString::from(SNAPSHOT_SUFFIX))
}

/// Pure request projection. Repeated estimates and retries do not mutate the
/// session or mint new identities for the same transition.
pub(crate) fn project_for_source(
    request: &mut ModelRequest,
    config: &ResolvedConfig,
    provider_info: Option<&ProviderInfo>,
) {
    let active = enabled_with_provider(config, provider_info);
    let include_all_non_cacheable =
        project_all_non_cacheable(config, provider_info, &request.model);
    project_with_mode(request, active, include_all_non_cacheable);
}

fn project_with_mode(request: &mut ModelRequest, active: bool, include_all_non_cacheable: bool) {
    if !active {
        request.messages.retain(|message| !is_step(message));
        return;
    }
    let previous = request
        .messages
        .iter()
        .rev()
        .find(|message| is_step(message));
    let has_current = request
        .system
        .iter()
        .any(|part| is_dynamic(part, include_all_non_cacheable));
    if !has_current && previous.is_none() {
        return;
    }
    let text = snapshot(&request.system, include_all_non_cacheable);
    request
        .system
        .retain(|part| !is_dynamic(part, include_all_non_cacheable));
    if previous.is_some_and(|message| {
        matches!(message, Message::User(user)
            if matches!(&user.content[0], ContentBlock::Text(previous) if previous.text == text))
    }) {
        return;
    }
    let mut digest = Sha256::new();
    if let Some(last) = request.messages.last() {
        let id = match last {
            Message::User(message) => &message.uuid,
            Message::Assistant(message) => &message.uuid,
            Message::ToolResult(message) => &message.uuid,
            Message::System(message) => &message.uuid,
            Message::CompactBoundary(message) => &message.uuid,
        };
        for unit in id.as_utf16() {
            digest.update(unit.to_le_bytes());
        }
    }
    digest.update([0, 0]);
    for unit in text.as_utf16() {
        digest.update(unit.to_le_bytes());
    }
    let id = format!("{ID_PREFIX}{:x}", digest.finalize());
    request.messages.push(Message::User(UserMessage {
        question_answer: None,
        uuid: id.into(),
        content: vec![ContentBlock::Text(TextBlock {
            text,
            citations: None,
            harness_envelope: None,
            message_id: None,
            phase: Some(PHASE.into()),
        })],
        is_meta: Some(true),
    }));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::events::InferenceSource;
    use crate::types::provider::ToolSpec;

    fn config_with_source(source: Option<&str>) -> ResolvedConfig {
        let mut cfg = crate::schemas::config::default_config();
        cfg.model_source = source.map(|name| InferenceSource {
            source: Some(name.to_string()),
            model_id: Some("test-model".to_string()),
            selection: Some(format!("{name}:test-model")),
            tier: None,
        });
        cfg
    }

    fn provider_with_base(base_url: Option<&str>) -> ProviderInfo {
        ProviderInfo {
            name: "test".into(),
            default_model: "test-model".into(),
            env_key: None,
            base_url: base_url.map(RuntimeString::from),
        }
    }

    fn empty_request(model: &str, system: Vec<SystemPromptPart>) -> ModelRequest {
        ModelRequest {
            supports_image_detail_original: false,
            model: model.into(),
            image_limits: None,
            max_media_blocks: None,
            system,
            messages: Vec::new(),
            tools: Vec::<ToolSpec>::new(),
            native_deferred_tools: None,
            deferred_tools: Vec::new(),
            wire_caps: None,
            frozen_tools: None,
            tool_inventory_events: Vec::new(),
            tool_choice: None,
            max_output_tokens: 1024,
            task_budget: None,
            output_schema: None,
            web_search: None,
            thinking: None,
            effort: None,
            model_verbosity: None,
            reasoning_summary: Default::default(),
            skip_cache_write: None,
            cache_ttl: None,
            cache_key: None,
            cache_domain: None,
            context_management: None,
            context_hints: None,
            thinking_context: None,
        }
    }

    #[test]
    fn domestic_and_third_party_compat_sources_demote_all_non_cacheable_system_blocks() {
        for source in [
            "kimi",
            "bailian",
            "minimax",
            "zhipu",
            "ark",
            "qoder",
            "qwenwork",
            "workbuddy",
            "trae",
        ] {
            let cfg = config_with_source(Some(source));
            assert!(
                enabled_with_provider(&cfg, None),
                "{source} must be enabled"
            );
            let mut req = empty_request(
                "glm-5.3",
                vec![
                    SystemPromptPart {
                        name: "base".into(),
                        text: "Stable instructions".into(),
                        cacheable: Some(true),
                        cache_scope: None,
                    },
                    SystemPromptPart {
                        name: "permissions".into(),
                        text: "Current permissions".into(),
                        cacheable: Some(false),
                        cache_scope: None,
                    },
                    SystemPromptPart {
                        name: "collaboration-mode".into(),
                        text: "Default collaboration mode".into(),
                        cacheable: Some(false),
                        cache_scope: None,
                    },
                ],
            );
            project_for_source(&mut req, &cfg, None);
            assert_eq!(
                req.system.len(),
                1,
                "{source} should retain only stable system part"
            );
            assert_eq!(req.system[0].name, "base");
            assert_eq!(
                req.messages.len(),
                1,
                "{source} should append one snapshot message"
            );
            assert!(is_step(&req.messages[0]));
        }

        // Custom third-party gateway demotes dynamic blocks; official first-party APIs keep native placement.
        let custom_cfg = config_with_source(Some("custom"));
        let deepseek_info = provider_with_base(Some("https://api.deepseek.com/v1"));
        assert!(enabled_with_provider(&custom_cfg, Some(&deepseek_info)));
        assert!(project_all_non_cacheable(
            &custom_cfg,
            Some(&deepseek_info),
            &RuntimeString::from("deepseek-chat")
        ));

        for official in [
            "https://api.anthropic.com",
            "https://api.openai.com/v1",
            "https://generativelanguage.googleapis.com",
        ] {
            let info = provider_with_base(Some(official));
            assert!(
                !enabled_with_provider(&custom_cfg, Some(&info)),
                "official endpoint {official} must preserve native placement"
            );
        }
        assert!(
            !enabled_with_provider(&custom_cfg, None),
            "custom without explicit third-party base_url stays inactive"
        );
        for native_source in ["subscription", "codex", "gemini"] {
            let cfg = config_with_source(Some(native_source));
            assert!(!enabled_with_provider(&cfg, None));
        }
    }
}
