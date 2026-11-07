//! Runtime capability roots supplied by the host or loaded plugins.
//!
//! The split is deliberate: the engine owns typed capability
//! contracts and deterministic resolution, while a host/plugin owns concrete
//! runtime implementations such as a browser controller.

use std::sync::Arc;

use crate::browser::{BrowserHostToolMapping, BrowserRuntime};
use crate::runtime_values::runtime_codec::{RuntimeEnumVariant, RuntimeExternalVariant};
use crate::runtime_values::{
    runtime_codec, RuntimeCodecError, RuntimeCodecErrorKind, RuntimeDecode, RuntimeEncode,
    RuntimeEncoder, RuntimeJson, RuntimeObjectEncoder, RuntimeString,
};
use serde::{Deserialize, Serialize};

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RuntimeCapabilityName {
    Browser,
}

impl RuntimeEncode for RuntimeCapabilityName {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Browser => "browser",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for RuntimeCapabilityName {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[RuntimeEnumVariant::new("browser", &[])],
            &[Self::Browser],
        )
    }
}

impl RuntimeCapabilityName {
    pub fn as_str(self) -> &'static str {
        match self {
            RuntimeCapabilityName::Browser => "browser",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum RuntimeCapabilitySourceKind {
    Host,
    Plugin,
    Environment,
}

impl RuntimeEncode for RuntimeCapabilitySourceKind {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::Host => "host",
            Self::Plugin => "plugin",
            Self::Environment => "environment",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for RuntimeCapabilitySourceKind {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("host", &[]),
                RuntimeEnumVariant::new("plugin", &[]),
                RuntimeEnumVariant::new("environment", &[]),
            ],
            &[Self::Host, Self::Plugin, Self::Environment],
        )
    }
}

impl RuntimeCapabilitySourceKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Host => "host",
            Self::Plugin => "plugin",
            Self::Environment => "environment",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeCapabilitySource {
    pub kind: RuntimeCapabilitySourceKind,
    pub id: RuntimeString,
    pub name: RuntimeString,
    pub version: Option<RuntimeString>,
    pub root: Option<RuntimeString>,
    pub enabled: bool,
}

impl RuntimeEncode for RuntimeCapabilitySource {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("kind", &self.kind)?;
        object.field("id", &self.id)?;
        object.field("name", &self.name)?;
        if let Some(value) = &self.version {
            object.field("version", value)?;
        }
        if let Some(value) = &self.root {
            object.field("root", value)?;
        }
        object.field("enabled", &self.enabled)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for RuntimeCapabilitySource {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                kind: object.required("kind", &[])?,
                id: object.required("id", &[])?,
                name: object.required("name", &[])?,
                version: object.optional("version", &[])?,
                root: object.optional("root", &[])?,
                enabled: object.required("enabled", &[])?,
            })
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectedRuntimeCapabilityRoot {
    pub kind: RuntimeCapabilitySourceKind,
    pub id: RuntimeString,
}

impl RuntimeEncode for SelectedRuntimeCapabilityRoot {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("kind", &self.kind)?;
        object.field("id", &self.id)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for SelectedRuntimeCapabilityRoot {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                kind: object.required("kind", &[])?,
                id: object.required("id", &[])?,
            })
        })
    }
}

impl SelectedRuntimeCapabilityRoot {
    pub fn host(id: impl Into<RuntimeString>) -> Self {
        Self {
            kind: RuntimeCapabilitySourceKind::Host,
            id: id.into(),
        }
    }

    pub fn plugin(id: impl Into<RuntimeString>) -> Self {
        Self {
            kind: RuntimeCapabilitySourceKind::Plugin,
            id: id.into(),
        }
    }

    pub fn environment(
        environment_id: impl Into<RuntimeString>,
        root: impl Into<RuntimeString>,
    ) -> Self {
        let environment_id = environment_id.into();
        let root = root.into();
        Self {
            kind: RuntimeCapabilitySourceKind::Environment,
            id: environment_root_id(&environment_id, &root),
        }
    }

    pub fn from_source(source: &RuntimeCapabilitySource) -> Self {
        Self {
            kind: source.kind,
            id: source.id.clone(),
        }
    }
}

impl RuntimeCapabilitySource {
    pub fn host(id: impl Into<RuntimeString>) -> Self {
        let id = id.into();
        Self {
            kind: RuntimeCapabilitySourceKind::Host,
            name: id.clone(),
            id,
            version: None,
            root: None,
            enabled: true,
        }
    }

    pub fn plugin(
        name: impl Into<RuntimeString>,
        version: Option<impl Into<RuntimeString>>,
        root: impl Into<RuntimeString>,
    ) -> Self {
        let name = name.into();
        Self {
            kind: RuntimeCapabilitySourceKind::Plugin,
            id: name.clone(),
            name,
            version: version.map(Into::into),
            root: Some(root.into()),
            enabled: true,
        }
    }

    pub fn environment(
        environment_id: impl Into<RuntimeString>,
        root: impl Into<RuntimeString>,
    ) -> Self {
        let environment_id = environment_id.into();
        let root = root.into();
        Self {
            kind: RuntimeCapabilitySourceKind::Environment,
            id: environment_root_id(&environment_id, &root),
            name: environment_id,
            version: None,
            root: Some(root),
            enabled: true,
        }
    }

    pub fn disabled(mut self) -> Self {
        self.enabled = false;
        self
    }
}

#[derive(Clone)]
pub struct RuntimeCapabilityRoot {
    pub source: RuntimeCapabilitySource,
    pub browser_runtime: Option<Arc<dyn BrowserRuntime>>,
    pub browser_tool_mapping: Option<BrowserHostToolMapping>,
}

impl RuntimeCapabilityRoot {
    pub fn host(id: impl Into<RuntimeString>) -> Self {
        Self {
            source: RuntimeCapabilitySource::host(id),
            browser_runtime: None,
            browser_tool_mapping: None,
        }
    }

    pub fn plugin(
        name: impl Into<RuntimeString>,
        version: Option<impl Into<RuntimeString>>,
        root: impl Into<RuntimeString>,
    ) -> Self {
        Self {
            source: RuntimeCapabilitySource::plugin(name, version, root),
            browser_runtime: None,
            browser_tool_mapping: None,
        }
    }

    pub fn environment(
        environment_id: impl Into<RuntimeString>,
        root: impl Into<RuntimeString>,
    ) -> Self {
        Self {
            source: RuntimeCapabilitySource::environment(environment_id, root),
            browser_runtime: None,
            browser_tool_mapping: None,
        }
    }

    pub fn with_browser_runtime(mut self, runtime: Arc<dyn BrowserRuntime>) -> Self {
        self.browser_runtime = Some(runtime);
        self.browser_tool_mapping = None;
        self
    }

    pub fn with_browser_tool_mapping(mut self, mapping: BrowserHostToolMapping) -> Self {
        self.browser_runtime = None;
        self.browser_tool_mapping = Some(mapping);
        self
    }

    pub fn disabled(mut self) -> Self {
        self.source.enabled = false;
        self
    }

    fn capability_names(&self) -> Vec<RuntimeCapabilityName> {
        let mut names = Vec::new();
        if self.browser_runtime.is_some() || self.browser_tool_mapping.is_some() {
            names.push(RuntimeCapabilityName::Browser);
        }
        names
    }
}

fn environment_root_id(environment_id: &RuntimeString, root: &RuntimeString) -> RuntimeString {
    environment_id
        .concat(&RuntimeString::from(":"))
        .concat(root)
}

#[derive(Clone, Default)]
pub struct RuntimeCapabilities {
    pub browser_runtime: Option<Arc<dyn BrowserRuntime>>,
    pub browser_tool_mapping: Option<BrowserHostToolMapping>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeCapabilityDescriptor {
    pub name: RuntimeCapabilityName,
    pub source: RuntimeCapabilitySource,
}

impl RuntimeEncode for RuntimeCapabilityDescriptor {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("name", &self.name)?;
        object.field("source", &self.source)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for RuntimeCapabilityDescriptor {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                name: object.required("name", &[])?,
                source: object.required("source", &[])?,
            })
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedRuntimeCapabilityRoot {
    pub selection: SelectedRuntimeCapabilityRoot,
    pub source: RuntimeCapabilitySource,
    pub capabilities: Vec<RuntimeCapabilityName>,
}

impl RuntimeEncode for ResolvedRuntimeCapabilityRoot {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("selection", &self.selection)?;
        object.field("source", &self.source)?;
        object.field("capabilities", &self.capabilities)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for ResolvedRuntimeCapabilityRoot {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                selection: object.required("selection", &[])?,
                source: object.required("source", &[])?,
                capabilities: object.required("capabilities", &[])?,
            })
        })
    }
}

#[derive(Clone, Default)]
pub struct RuntimeCapabilityResolution {
    pub capabilities: RuntimeCapabilities,
    pub descriptors: Vec<RuntimeCapabilityDescriptor>,
    pub selected_roots: Vec<SelectedRuntimeCapabilityRoot>,
    pub effective_roots: Vec<ResolvedRuntimeCapabilityRoot>,
    pub missing_roots: Vec<SelectedRuntimeCapabilityRoot>,
}

impl RuntimeEncode for RuntimeCapabilityResolution {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("descriptors", &self.descriptors)?;
        object.field("selectedRoots", &self.selected_roots)?;
        object.field("effectiveRoots", &self.effective_roots)?;
        object.field("missingRoots", &self.missing_roots)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for RuntimeCapabilityResolution {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                capabilities: RuntimeCapabilities::default(),
                descriptors: object.required("descriptors", &[])?,
                selected_roots: object.required("selectedRoots", &[])?,
                effective_roots: object.required("effectiveRoots", &[])?,
                missing_roots: object.required("missingRoots", &[])?,
            })
        })
    }
}

impl std::fmt::Debug for RuntimeCapabilityResolution {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RuntimeCapabilityResolution")
            .field("descriptors", &self.descriptors)
            .field("selected_roots", &self.selected_roots)
            .field("effective_roots", &self.effective_roots)
            .field("missing_roots", &self.missing_roots)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Default)]
pub struct RuntimeCapabilityRegistry {
    roots: Vec<RuntimeCapabilityRoot>,
}

impl RuntimeCapabilityRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register one root. Re-registering the same source id replaces the old
    /// root and moves it to the end, matching reload/override semantics.
    pub fn register(&mut self, root: RuntimeCapabilityRoot) {
        self.roots.retain(|existing| {
            !(existing.source.kind == root.source.kind && existing.source.id == root.source.id)
        });
        self.roots.push(root);
    }

    pub fn from_roots(roots: impl IntoIterator<Item = RuntimeCapabilityRoot>) -> Self {
        let mut registry = Self::new();
        for root in roots {
            registry.register(root);
        }
        registry
    }

    /// Resolve the effective runtime capability set. Later enabled roots win, so
    /// explicit session/host roots can override plugin defaults deterministically.
    pub fn resolve(&self) -> RuntimeCapabilities {
        self.resolve_selected(None).capabilities
    }

    /// Resolve the selected capability roots. `None` or an empty slice keeps the
    /// historical behavior: every enabled root participates in registry order.
    /// A non-empty selection narrows the surface: only named roots are
    /// visible, missing roots are reported, and later selected roots win.
    pub fn resolve_selected(
        &self,
        selected: Option<&[SelectedRuntimeCapabilityRoot]>,
    ) -> RuntimeCapabilityResolution {
        let selected_roots = match selected {
            Some(list) if !list.is_empty() => list.to_vec(),
            _ => self
                .roots
                .iter()
                .filter(|root| root.source.enabled)
                .map(|root| SelectedRuntimeCapabilityRoot::from_source(&root.source))
                .collect(),
        };
        let mut missing_roots = Vec::new();
        let effective_root_pairs = selected_roots
            .iter()
            .filter_map(|selection| match self.find_enabled_root(selection) {
                Some(root) => Some((selection.clone(), root)),
                None => {
                    missing_roots.push(selection.clone());
                    None
                }
            })
            .collect::<Vec<_>>();

        let capabilities = resolve_capabilities(effective_root_pairs.iter().map(|(_, root)| *root));
        let descriptors = descriptors_for_roots(effective_root_pairs.iter().map(|(_, root)| *root));
        let effective_roots = effective_root_pairs
            .into_iter()
            .map(|(selection, root)| ResolvedRuntimeCapabilityRoot {
                selection,
                source: root.source.clone(),
                capabilities: root.capability_names(),
            })
            .collect();

        RuntimeCapabilityResolution {
            capabilities,
            descriptors,
            selected_roots,
            effective_roots,
            missing_roots,
        }
    }

    pub fn descriptors(&self) -> Vec<RuntimeCapabilityDescriptor> {
        self.resolve_selected(None).descriptors
    }

    pub fn roots(&self) -> &[RuntimeCapabilityRoot] {
        &self.roots
    }

    fn find_enabled_root(
        &self,
        selection: &SelectedRuntimeCapabilityRoot,
    ) -> Option<&RuntimeCapabilityRoot> {
        self.roots.iter().find(|root| {
            root.source.enabled
                && root.source.kind == selection.kind
                && root.source.id == selection.id
        })
    }
}

fn resolve_capabilities<'a>(
    roots: impl IntoIterator<Item = &'a RuntimeCapabilityRoot>,
) -> RuntimeCapabilities {
    let mut caps = RuntimeCapabilities::default();
    for root in roots {
        if !root.source.enabled {
            continue;
        }
        if let Some(runtime) = &root.browser_runtime {
            caps.browser_runtime = Some(runtime.clone());
            caps.browser_tool_mapping = None;
        } else if let Some(mapping) = &root.browser_tool_mapping {
            caps.browser_runtime = None;
            caps.browser_tool_mapping = Some(mapping.clone());
        }
    }
    caps
}

fn descriptors_for_roots<'a>(
    roots: impl IntoIterator<Item = &'a RuntimeCapabilityRoot>,
) -> Vec<RuntimeCapabilityDescriptor> {
    roots
        .into_iter()
        .filter(|root| root.source.enabled)
        .flat_map(|root| {
            root.capability_names()
                .into_iter()
                .map(|name| RuntimeCapabilityDescriptor {
                    name,
                    source: root.source.clone(),
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::browser::{
        attest_spawn_free, BrowserEventSink, BrowserExecutor, BrowserRuntimeError,
        BrowserRuntimeRequest, BrowserRuntimeResponse,
    };
    use crate::types::abort::AbortSignal;
    use async_trait::async_trait;

    struct RuntimeA;
    struct RuntimeB;

    #[async_trait]
    unsafe impl BrowserExecutor for RuntimeA {
        async fn execute(
            &self,
            _request: BrowserRuntimeRequest,
            _signal: AbortSignal,
            _emit: BrowserEventSink,
        ) -> Result<BrowserRuntimeResponse, BrowserRuntimeError> {
            Ok(BrowserRuntimeResponse::default())
        }
    }

    #[async_trait]
    unsafe impl BrowserExecutor for RuntimeB {
        async fn execute(
            &self,
            _request: BrowserRuntimeRequest,
            _signal: AbortSignal,
            _emit: BrowserEventSink,
        ) -> Result<BrowserRuntimeResponse, BrowserRuntimeError> {
            Ok(BrowserRuntimeResponse::default())
        }
    }

    fn spawn_free<T>(executor: Arc<T>) -> Arc<dyn BrowserRuntime>
    where
        T: BrowserExecutor + 'static,
    {
        // SAFETY: these test executors only return in-memory fixture responses
        // and have no local process authority.
        unsafe { attest_spawn_free(executor) }
    }

    #[test]
    fn later_enabled_roots_override_earlier_browser_runtimes() {
        let a: Arc<dyn BrowserRuntime> = spawn_free(Arc::new(RuntimeA));
        let b: Arc<dyn BrowserRuntime> = spawn_free(Arc::new(RuntimeB));
        let mut registry = RuntimeCapabilityRegistry::new();
        registry.register(
            RuntimeCapabilityRoot::plugin("docs", Some("1"), "/p").with_browser_runtime(a),
        );
        registry.register(RuntimeCapabilityRoot::host("session").with_browser_runtime(b.clone()));

        let resolved = registry.resolve();
        assert!(resolved.browser_runtime.is_some());
        assert!(Arc::ptr_eq(resolved.browser_runtime.as_ref().unwrap(), &b));
    }

    #[test]
    fn disabled_roots_do_not_surface_descriptors() {
        let mut registry = RuntimeCapabilityRegistry::new();
        registry.register(
            RuntimeCapabilityRoot::plugin("docs", Some("1"), "/p")
                .with_browser_runtime(spawn_free(Arc::new(RuntimeA)))
                .disabled(),
        );

        assert!(registry.descriptors().is_empty());
        assert!(registry.resolve().browser_runtime.is_none());
    }

    #[test]
    fn selected_roots_filter_registry_and_report_missing_roots() {
        let docs: Arc<dyn BrowserRuntime> = spawn_free(Arc::new(RuntimeA));
        let jira: Arc<dyn BrowserRuntime> = spawn_free(Arc::new(RuntimeB));
        let mut registry = RuntimeCapabilityRegistry::new();
        registry.register(
            RuntimeCapabilityRoot::plugin("docs", Some("1"), "/plugins/docs")
                .with_browser_runtime(docs.clone()),
        );
        registry.register(
            RuntimeCapabilityRoot::plugin("jira", Some("1"), "/plugins/jira")
                .with_browser_runtime(jira.clone()),
        );

        let selected = vec![
            SelectedRuntimeCapabilityRoot::plugin("docs"),
            SelectedRuntimeCapabilityRoot::plugin("missing"),
        ];
        let resolved = registry.resolve_selected(Some(&selected));

        assert!(Arc::ptr_eq(
            resolved.capabilities.browser_runtime.as_ref().unwrap(),
            &docs
        ));
        assert_eq!(resolved.descriptors.len(), 1);
        assert_eq!(resolved.descriptors[0].source.id, "docs");
        assert_eq!(resolved.selected_roots, selected);
        assert_eq!(resolved.effective_roots.len(), 1);
        assert_eq!(resolved.effective_roots[0].selection.id, "docs");
        assert_eq!(
            resolved.missing_roots,
            vec![SelectedRuntimeCapabilityRoot::plugin("missing")]
        );
    }

    #[test]
    fn selected_roots_resolve_in_selection_order_so_later_roots_win() {
        let plugin_runtime: Arc<dyn BrowserRuntime> = spawn_free(Arc::new(RuntimeA));
        let host_runtime: Arc<dyn BrowserRuntime> = spawn_free(Arc::new(RuntimeB));
        let mut registry = RuntimeCapabilityRegistry::new();
        registry.register(
            RuntimeCapabilityRoot::plugin("docs", Some("1"), "/plugins/docs")
                .with_browser_runtime(plugin_runtime),
        );
        registry.register(
            RuntimeCapabilityRoot::host("session.browser")
                .with_browser_runtime(host_runtime.clone()),
        );

        let selected = vec![
            SelectedRuntimeCapabilityRoot::plugin("docs"),
            SelectedRuntimeCapabilityRoot::host("session.browser"),
        ];
        let resolved = registry.resolve_selected(Some(&selected));

        assert!(Arc::ptr_eq(
            resolved.capabilities.browser_runtime.as_ref().unwrap(),
            &host_runtime
        ));
        assert_eq!(
            resolved
                .selected_roots
                .iter()
                .map(|root| root.id.try_to_utf8().unwrap())
                .collect::<Vec<_>>(),
            vec!["docs".to_owned(), "session.browser".to_owned()]
        );
    }

    #[test]
    fn environment_roots_are_selected_by_environment_and_root_location() {
        let docs: Arc<dyn BrowserRuntime> = spawn_free(Arc::new(RuntimeA));
        let tools: Arc<dyn BrowserRuntime> = spawn_free(Arc::new(RuntimeB));
        let mut registry = RuntimeCapabilityRegistry::new();
        registry.register(
            RuntimeCapabilityRoot::environment("env-1", "/plugins/docs")
                .with_browser_runtime(docs.clone()),
        );
        registry.register(
            RuntimeCapabilityRoot::environment("env-1", "/plugins/tools")
                .with_browser_runtime(tools),
        );

        let resolved =
            registry.resolve_selected(Some(&[SelectedRuntimeCapabilityRoot::environment(
                "env-1",
                "/plugins/docs",
            )]));

        assert!(Arc::ptr_eq(
            resolved.capabilities.browser_runtime.as_ref().unwrap(),
            &docs
        ));
        assert_eq!(
            resolved.effective_roots[0].selection,
            SelectedRuntimeCapabilityRoot::environment("env-1", "/plugins/docs")
        );
        assert_eq!(resolved.effective_roots[0].source.name, "env-1");
        assert_eq!(
            resolved.effective_roots[0]
                .source
                .root
                .as_ref()
                .map(|root| root.try_to_utf8().unwrap()),
            Some("/plugins/docs".to_owned())
        );
    }

    #[test]
    fn resolution_wire_omits_live_objects_and_preserves_selected_effective_missing_roots() {
        let mut registry = RuntimeCapabilityRegistry::new();
        registry.register(
            RuntimeCapabilityRoot::host("host.browser")
                .with_browser_runtime(spawn_free(Arc::new(RuntimeA))),
        );
        let selected = vec![
            SelectedRuntimeCapabilityRoot::host("host.browser"),
            SelectedRuntimeCapabilityRoot::plugin("missing"),
        ];
        let resolution = registry.resolve_selected(Some(&selected));
        let value = runtime_codec::encode_value(&resolution).unwrap();
        let object = value.as_object().unwrap();

        assert!(object.get("capabilities").is_none());
        assert_eq!(
            object
                .get("selectedRoots")
                .unwrap()
                .as_array()
                .unwrap()
                .len(),
            2
        );
        assert_eq!(
            object
                .get("effectiveRoots")
                .unwrap()
                .as_array()
                .unwrap()
                .len(),
            1
        );
        let missing = object.get("missingRoots").unwrap().as_array().unwrap();
        let missing_object = missing[0].as_object().unwrap();
        assert_eq!(
            missing_object.get("id"),
            Some(&RuntimeJson::from("missing"))
        );

        let decoded = runtime_codec::decode_value::<RuntimeCapabilityResolution>(&value).unwrap();
        assert!(decoded.capabilities.browser_runtime.is_none());
        assert_eq!(decoded.selected_roots, selected);
        assert_eq!(decoded.effective_roots.len(), 1);
    }

    #[test]
    fn capability_runtime_wire_preserves_lone_surrogates_without_serde_projection() {
        let id = RuntimeString::from_utf16([b'p' as u16, 0xd800]);
        let source = RuntimeCapabilitySource::host(id.clone());
        let encoded = runtime_codec::encode_value(&source).unwrap();
        let decoded = runtime_codec::decode_value::<RuntimeCapabilitySource>(&encoded).unwrap();

        assert_eq!(decoded.id, id);
        assert_eq!(decoded.name, id);
    }
}
