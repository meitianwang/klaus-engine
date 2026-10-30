//! A child keeps the exact host capability scope admitted by its parent.

use crate::capabilities::{
    RuntimeCapabilities, RuntimeCapabilityResolution, SelectedRuntimeCapabilityRoot,
};
use crate::engine::session::TurnOptions;
use crate::runtime_values::{
    runtime_codec, RuntimeEncode, RuntimeEncoder, RuntimeJson, RuntimeObject, RuntimeObjectEncoder,
    RuntimeString,
};

#[derive(Clone, Default)]
pub(crate) struct SubagentCapabilityCapture {
    resolution: RuntimeCapabilityResolution,
}

impl SubagentCapabilityCapture {
    pub(crate) fn capture(resolution: RuntimeCapabilityResolution) -> Self {
        Self { resolution }
    }

    pub(crate) fn capabilities(&self) -> &RuntimeCapabilities {
        &self.resolution.capabilities
    }

    fn evidence(&self) -> Result<RuntimeJson, crate::runtime_values::RuntimeCodecError> {
        let mut evidence = RuntimeObject::new();
        evidence.insert("resolution", runtime_codec::encode_value(&self.resolution)?);
        let capabilities = self.capabilities();
        evidence.insert(
            "browserKind",
            RuntimeJson::String(if capabilities.browser_runtime.is_some() {
                "runtime".into()
            } else if capabilities.browser_tool_mapping.is_some() {
                "mapping".into()
            } else {
                "none".into()
            }),
        );
        if let Some(mapping) = &capabilities.browser_tool_mapping {
            let mut bindings = RuntimeObject::new();
            for (owner, host) in crate::engine::browser_tool_dependencies::bindings(mapping) {
                bindings.insert(owner, RuntimeJson::String(host.into()));
            }
            evidence.insert("browserBindings", RuntimeJson::Object(bindings));
        }
        Ok(RuntimeJson::Object(evidence))
    }

    pub(crate) fn restore(
        session: &super::Session,
        value: Option<&RuntimeJson>,
        current_capture: &Self,
        evicted_capture: Option<&Self>,
    ) -> Result<Self, RuntimeString> {
        let Some(value) = value else {
            // Missing historical authority cannot acquire current default roots.
            return Ok(Self::default());
        };
        let (version, selected, evidence) = runtime_codec::decode_object(value, true, |object| {
            Ok((
                object.required::<u32>("version", &[])?,
                object.required::<Vec<SelectedRuntimeCapabilityRoot>>("selectedRoots", &[])?,
                object.required::<RuntimeJson>("evidence", &[])?,
            ))
        })
        .map_err(|error| {
            RuntimeString::from(format!("Invalid original host capability capture: {error}"))
        })?;
        if version != 1 {
            return Err("Unsupported original host capability capture version.".into());
        }
        let opaque_browser = evidence.get("browserKind").and_then(RuntimeJson::as_str)
            == Some(&RuntimeString::from("runtime"));
        let restored = if opaque_browser {
            // Only the target's successful local eviction can retain this owner.
            // Matching serialized roots or the sender's scope cannot prove it.
            let evicted = evicted_capture.ok_or_else(|| {
                RuntimeString::from(
                    "The original opaque browser runtime cannot be proved after restart.",
                )
            })?;
            let retained_evidence = evicted
                .evidence()
                .map_err(|error| RuntimeString::from(error.to_string()))?;
            if selected.is_empty()
                || evicted.resolution.selected_roots != selected
                || !retained_evidence.semantic_eq_unordered(&evidence)
            {
                return Err(
                    "The retained host capability scope does not match the original worker.".into(),
                );
            }
            // Resolve the target's selection again: a removed or replaced owner
            // must fail even while its old Arc remains alive in this process.
            let registered =
                Self::capture(session.runtime_capability_snapshot_for_turn(&TurnOptions {
                    selected_capability_roots: Some(selected),
                    ..Default::default()
                }));
            match (
                evicted.capabilities().browser_runtime.as_ref(),
                registered.capabilities().browser_runtime.as_ref(),
            ) {
                (Some(original), Some(current)) if std::sync::Arc::ptr_eq(original, current) => {}
                _ => {
                    return Err(
                        "The original local browser runtime is unavailable or changed.".into(),
                    )
                }
            }
            registered
        } else if selected.is_empty() {
            // The public resolver treats an empty selection as all roots.
            // A saved empty admitted scope must instead remain empty.
            Self::default()
        } else if current_capture.resolution.selected_roots == selected {
            current_capture.clone()
        } else {
            Self::capture(session.runtime_capability_snapshot_for_turn(&TurnOptions {
                selected_capability_roots: Some(selected),
                ..Default::default()
            }))
        };
        let current = restored
            .evidence()
            .map_err(|error| RuntimeString::from(error.to_string()))?;
        if !current.semantic_eq_unordered(&evidence) {
            return Err("The original host capability roots are unavailable or changed.".into());
        }
        Ok(restored)
    }
}

impl RuntimeEncode for SubagentCapabilityCapture {
    fn encode_runtime(
        &self,
        out: &mut RuntimeEncoder,
    ) -> Result<(), crate::runtime_values::RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("version", &1_u32)?;
        object.field("selectedRoots", &self.resolution.selected_roots)?;
        object.field("evidence", &self.evidence()?)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}
