//! Defect: local orchestration overwrote native reasoning and was lost on runtime updates.
//! Root cause: one effort field carried both provider intensity and delegation authorization.
//! Text frontmatter scalars also lost explicit false before legacy normalization.
//! Work item: 20261020 Ultra mode split.

use super::r7c876d49_spec_restores_latest_permission_policy::{agent_options, process_supervisor};
use klaus_agent_core::engine::agent::{Agent, StartSessionOptions};
use klaus_agent_core::engine::session::resolve_config;
use klaus_agent_core::extensions::agent_loader::load_agent_from_file;
use klaus_agent_core::extensions::skills::loader::load_one_skill;
use klaus_agent_core::runtime_process::RuntimeProcessContext;
use klaus_agent_core::runtime_values::{runtime_codec, RuntimeString};
use klaus_agent_core::schemas::config::EngineConfig;
use klaus_agent_core::schemas::effort::{EffortLevel, EffortValue};
use serde_json::json;
use std::path::{Path, PathBuf};

struct MarkdownDirectory(PathBuf);

impl MarkdownDirectory {
    fn new() -> Self {
        let path =
            std::env::temp_dir().join(format!("klaus-ultra-markdown-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for MarkdownDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[tokio::test]
async fn markdown_overrides_preserve_explicit_mode_and_native_effort() {
    let directory = MarkdownDirectory::new();
    let skill_path = directory.path().join("skill.md");
    let agent_path = directory.path().join("agent.md");
    for (fields, mode, effort) in [
        ("effort: ultra\n", Some(true), EffortLevel::Ultra),
        (
            "effort: ultra\nultraMode: true\n",
            Some(true),
            EffortLevel::Ultra,
        ),
        (
            "effort: ultra\nultraMode: false\n",
            Some(false),
            EffortLevel::Ultra,
        ),
        ("effort: low\n", None, EffortLevel::Low),
        (
            "effort: low\nultraMode: true\n",
            Some(true),
            EffortLevel::Low,
        ),
        (
            "effort: low\nultraMode: false\n",
            Some(false),
            EffortLevel::Low,
        ),
    ] {
        let content = format!(
            "---\nname: mode\ndescription: Inspect the task\n{fields}---\nReview the workspace."
        );
        std::fs::write(&skill_path, &content).unwrap();
        std::fs::write(&agent_path, &content).unwrap();
        let skill = load_one_skill(skill_path.to_str().unwrap(), "mode")
            .await
            .unwrap();
        let agent = load_agent_from_file(agent_path.to_str().unwrap())
            .await
            .unwrap();
        assert_eq!(skill.ultra_mode, mode);
        assert_eq!(agent.ultra_mode, mode);
        assert_eq!(skill.effort, Some(EffortValue::Level(effort)));
        assert_eq!(agent.effort, Some(EffortValue::Level(effort)));
    }
}

#[tokio::test]
async fn markdown_overrides_reject_invalid_mode_values() {
    let directory = MarkdownDirectory::new();
    let skill_path = directory.path().join("skill.md");
    let agent_path = directory.path().join("agent.md");
    for value in ["yes\n", "0\n", "null\n", "FALSE\n", "\n  - false\n"] {
        let content = format!("---\ndescription: Inspect the task\neffort: ultra\nultraMode: {value}---\nReview the workspace.");
        std::fs::write(&skill_path, &content).unwrap();
        std::fs::write(&agent_path, &content).unwrap();
        assert!(load_one_skill(skill_path.to_str().unwrap(), "mode")
            .await
            .is_none());
        assert!(load_agent_from_file(agent_path.to_str().unwrap())
            .await
            .is_none());
    }
}

#[test]
fn legacy_config_normalizes_before_defaults_and_preserves_explicit_false() {
    for (raw, expected) in [
        (json!({"effort":"ultra"}), true),
        (json!({"effort":"ultra", "ultraMode":false}), false),
    ] {
        let config = resolve_config(raw).unwrap();
        assert_eq!(config.effort, Some(EffortValue::Level(EffortLevel::Ultra)));
        assert_eq!(config.ultra_mode, expected);
    }
    let decoded: EngineConfig = runtime_codec::decode_text(&RuntimeString::from(
        r#"{"effort":"ultra","ultraMode":false}"#,
    ))
    .unwrap();
    assert!(!decoded.ultra_mode);
    assert_eq!(decoded.effort, Some(EffortValue::Level(EffortLevel::Ultra)));
}

#[test]
fn native_reasoning_and_orchestration_have_independent_public_config() {
    for effort in ["low", "medium", "high", "xhigh", "max"] {
        let config = resolve_config(json!({"effort":effort,"ultraMode":true})).unwrap();
        assert!(config.ultra_mode);
        assert_eq!(serde_json::to_value(config.effort).unwrap(), json!(effort));
    }
    let config = resolve_config(json!({"thinking":false,"ultraMode":true})).unwrap();
    assert!(!config.thinking);
    assert!(config.ultra_mode);
    assert!(config.effort.is_none());
}

#[tokio::test]
async fn live_mode_updates_capture_new_revision_without_replacing_native_effort() {
    let process = RuntimeProcessContext::initialize(process_supervisor()).unwrap();
    let mut options = agent_options(process);
    options.config.as_mut().unwrap()["effort"] = json!("low");
    let session = Agent::new(options).start_session(StartSessionOptions::default());
    let initial = session.model_revision();
    assert!(!session.ultra_mode());
    assert!(session.set_ultra_mode(true).unwrap());
    assert!(session.model_revision() > initial);
    let (_, config, _) = session.model_runtime();
    assert_eq!(config.effort, Some(EffortValue::Level(EffortLevel::Low)));
    assert!(config.ultra_mode);
    assert!(!session.set_ultra_mode(false).unwrap());
    let (_, config, _) = session.model_runtime();
    assert_eq!(config.effort, Some(EffortValue::Level(EffortLevel::Low)));
    assert!(!config.ultra_mode);
}
