//! Private collaboration evidence binds a worker to its admitted instruction mode.

use crate::planning::CollaborationMode;
use crate::runtime_values::{
    runtime_codec, RuntimeCodecError, RuntimeCodecErrorKind, RuntimeDecode, RuntimeEncode,
    RuntimeEncoder, RuntimeJson, RuntimeObjectEncoder, RuntimeString,
};
use crate::schemas::config::ResolvedConfig;
use crate::types::provider::SystemPromptPart;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SubagentCollaborationOrigin {
    mode: CollaborationMode,
}

impl SubagentCollaborationOrigin {
    pub(crate) fn capture(mode: CollaborationMode) -> Self {
        Self { mode }
    }

    pub(crate) fn apply(
        self,
        config: &ResolvedConfig,
        mut parts: Vec<SystemPromptPart>,
    ) -> Vec<SystemPromptPart> {
        parts.retain(|part| part.name != "collaboration-mode");
        parts.push(crate::planning::instructions::instruction_part(
            config, self.mode,
        ));
        parts
    }
}

impl RuntimeEncode for SubagentCollaborationOrigin {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("version", &1_u32)?;
        object.field("mode", &self.mode)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for SubagentCollaborationOrigin {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, true, |object| {
            if object.required::<u32>("version", &[])? != 1 {
                return Err(RuntimeCodecError::new(RuntimeCodecErrorKind::Custom(
                    "Unsupported collaboration origin version.".into(),
                )));
            }
            let value = object.required::<RuntimeString>("mode", &[])?;
            let mode = match value.try_to_utf8().ok().as_deref() {
                Some("default") => CollaborationMode::Default,
                Some("plan") => CollaborationMode::Plan,
                _ => {
                    return Err(RuntimeCodecError::new(RuntimeCodecErrorKind::Custom(
                        "Unsupported original collaboration mode.".into(),
                    )));
                }
            };
            Ok(Self { mode })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schemas::config::default_config;

    macro_rules! exact_json {
        ($($json:tt)+) => {{
            crate::runtime_values::runtime_json::parse(&RuntimeString::from(stringify!($($json)+)))
                .expect("test JSON literal must parse")
        }};
    }

    #[test]
    fn private_collaboration_origin_roundtrips_canonical_modes() {
        for mode in [CollaborationMode::Default, CollaborationMode::Plan] {
            let origin = SubagentCollaborationOrigin::capture(mode);
            let value = runtime_codec::encode_value(&origin).unwrap();
            assert_eq!(
                SubagentCollaborationOrigin::decode_runtime(&value).unwrap(),
                origin
            );
            assert_eq!(
                value.get("mode").and_then(RuntimeJson::as_str),
                Some(&RuntimeString::from(mode.wire_name()))
            );
        }
    }

    #[test]
    fn private_collaboration_origin_rejects_null_unknown_missing_and_noncanonical_evidence() {
        for invalid in [
            exact_json!(null),
            exact_json!({"version":1}),
            exact_json!({"version":1,"mode":"code"}),
            exact_json!({"version":1,"mode":"PLAN"}),
            exact_json!({"version":1,"mode":"unknown"}),
            exact_json!({"version":2,"mode":"default"}),
            exact_json!({"version":1,"mode":"default","extra":true}),
            exact_json!({"mode":"default"}),
        ] {
            assert!(SubagentCollaborationOrigin::decode_runtime(&invalid).is_err());
        }
    }

    #[test]
    fn original_collaboration_wrapper_preserves_other_parts_and_cache_metadata() {
        let base = SystemPromptPart {
            name: "original-base".into(),
            text: "Retain the entire admitted base instruction.".into(),
            cacheable: Some(true),
            cache_scope: None,
        };
        let old = SystemPromptPart {
            name: "collaboration-mode".into(),
            text: "A different current mode is not original authority.".into(),
            cacheable: Some(true),
            cache_scope: None,
        };
        let config = default_config();
        let actual = SubagentCollaborationOrigin::capture(CollaborationMode::Plan)
            .apply(&config, vec![base.clone(), old]);
        assert_eq!(actual.len(), 2);
        assert_eq!(actual[0], base);
        assert_eq!(
            actual[1],
            crate::planning::instructions::instruction_part(&config, CollaborationMode::Plan)
        );
        assert_eq!(actual[1].cacheable, Some(false));
        assert_eq!(actual[1].cache_scope, None);
    }

    #[test]
    fn equal_custom_templates_keep_distinct_original_mode_boundaries() {
        for template in ["", "The same custom instruction body."] {
            let mut config = default_config();
            config.collaboration_default_instructions = Some(template.into());
            config.collaboration_plan_instructions = Some(template.into());
            let default = crate::planning::instructions::instruction_part(
                &config,
                CollaborationMode::Default,
            );
            let plan =
                crate::planning::instructions::instruction_part(&config, CollaborationMode::Plan);
            assert!(default
                .text
                .try_to_utf8()
                .unwrap()
                .starts_with("<collaboration_mode>\n# Collaboration Mode: Default"));
            assert!(plan
                .text
                .try_to_utf8()
                .unwrap()
                .starts_with("<collaboration_mode>\n# Collaboration Mode: Plan"));
            assert_ne!(default, plan);
        }
    }
}
