//! The parent baseline and a child's admitted task authority are separate captures.

use super::tighten_subagent_permission;
use crate::runtime_values::{
    runtime_codec, RuntimeCodecError, RuntimeCodecErrorKind, RuntimeDecode, RuntimeEncode,
    RuntimeEncoder, RuntimeJson, RuntimeObjectEncoder, RuntimePath,
};
use crate::types::permission::{PermissionContext, PermissionMode, PermissionRule};

/// Private creation evidence, never a tool input or renderer permission option.
#[derive(Debug, Clone)]
pub(crate) struct SubagentPermissionOrigin {
    parent_allow_rules: Vec<PermissionRule>,
    parent_writable_roots: Vec<RuntimePath>,
}

impl SubagentPermissionOrigin {
    pub(crate) fn capture(parent: &PermissionContext) -> Self {
        Self {
            parent_allow_rules: parent.allow_rules.clone(),
            parent_writable_roots: parent.additional_writable_roots.clone(),
        }
    }
    pub(crate) fn is_task_scoped_root(&self, root: &RuntimePath) -> bool {
        !self.parent_writable_roots.contains(root)
    }
}

impl RuntimeEncode for SubagentPermissionOrigin {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("version", &2_u32)?;
        object.field("parentAllowRules", &self.parent_allow_rules)?;
        object.field("parentWritableRoots", &self.parent_writable_roots)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for SubagentPermissionOrigin {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, true, |object| {
            let version = object.required::<u32>("version", &[])?;
            if !matches!(version, 1 | 2) {
                return Err(RuntimeCodecError::new(RuntimeCodecErrorKind::Custom(
                    "Unsupported permission origin version.".into(),
                )));
            }
            if version == 1 {
                let _: PermissionMode = object.required("parentMode", &[])?;
            }
            Ok(Self {
                parent_allow_rules: object.required("parentAllowRules", &[])?,
                parent_writable_roots: object.required("parentWritableRoots", &[])?,
            })
        })
    }
}

/// Apply only later parent restrictions to the one admitted child context.
/// Missing legacy evidence keeps the conservative intersection policy.
pub(crate) fn project_subagent_permission(
    admitted: &PermissionContext,
    current: &PermissionContext,
    origin: Option<&SubagentPermissionOrigin>,
    frozen: bool,
) -> PermissionContext {
    let Some(origin) = origin.filter(|_| frozen) else {
        return tighten_subagent_permission(admitted, current, frozen);
    };
    let mut current = current.clone();
    // Scoped grants were admitted to this task, independently of the parent
    // baseline. A removed baseline grant is still revoked; new grants stay out.
    for rule in &admitted.allow_rules {
        if !origin.parent_allow_rules.contains(rule) && !current.allow_rules.contains(rule) {
            current.allow_rules.push(rule.clone());
        }
    }
    for root in &admitted.additional_writable_roots {
        if !origin.parent_writable_roots.contains(root)
            && !current.additional_writable_roots.contains(root)
        {
            current.additional_writable_roots.push(root.clone());
        }
    }
    tighten_subagent_permission(admitted, &current, frozen)
}
