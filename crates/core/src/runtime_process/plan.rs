use std::fmt;

use crate::runtime_values::RuntimeString;

use super::{normalize_child_env, RuntimeEnv};

/// An exact process atom. Runtime-derived values remain UTF-16 until the
/// selected process-image backend performs its one terminal projection.
#[derive(Clone, Eq, Hash, PartialEq)]
pub enum ProcessAtom {
    Runtime(RuntimeString),
    Native(NativeOsString),
}

impl fmt::Debug for ProcessAtom {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Runtime(_) => formatter.write_str("Runtime(..)"),
            Self::Native(_) => formatter.write_str("Native(..)"),
        }
    }
}

/// Policy-bound native host atom for an already-native Rust API value.
///
/// The representation and every constructor are crate-private. There is no
/// public conversion from [`RuntimeString`], `String`, `Path`, or `OsString`.
#[derive(Clone, Eq, Hash, PartialEq)]
pub struct NativeOsString {
    #[cfg(unix)]
    bytes: Vec<u8>,
    #[cfg(windows)]
    wide: Vec<u16>,
    #[cfg(not(any(unix, windows)))]
    opaque: Vec<u8>,
}

impl fmt::Debug for NativeOsString {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("NativeOsString(..)")
    }
}

impl NativeOsString {
    pub(super) fn from_host_os_str(value: &std::ffi::OsStr) -> Self {
        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStrExt;
            Self {
                bytes: value.as_bytes().to_vec(),
            }
        }
        #[cfg(windows)]
        {
            use std::os::windows::ffi::OsStrExt;
            Self {
                wide: value.encode_wide().collect(),
            }
        }
        #[cfg(not(any(unix, windows)))]
        {
            Self {
                opaque: value.to_string_lossy().as_bytes().to_vec(),
            }
        }
    }

    fn is_empty(&self) -> bool {
        #[cfg(unix)]
        {
            self.bytes.is_empty()
        }
        #[cfg(windows)]
        {
            self.wide.is_empty()
        }
        #[cfg(not(any(unix, windows)))]
        {
            self.opaque.is_empty()
        }
    }

    fn nul_index(&self) -> Option<usize> {
        #[cfg(unix)]
        {
            self.bytes.iter().position(|byte| *byte == 0)
        }
        #[cfg(windows)]
        {
            self.wide.iter().position(|unit| *unit == 0)
        }
        #[cfg(not(any(unix, windows)))]
        {
            self.opaque.iter().position(|byte| *byte == 0)
        }
    }

    /// Borrow the exact Unix host bytes at the process-image boundary.
    #[cfg(unix)]
    pub(super) fn as_unix_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Borrow the exact Windows host code units at the process-image boundary.
    #[cfg(windows)]
    pub(super) fn as_windows_wide(&self) -> &[u16] {
        &self.wide
    }

    /// Deterministic native-host fixture constructor. Production constructors
    /// remain policy-bound and are introduced with their manifest call sites.
    #[cfg(all(test, unix))]
    pub(super) fn from_unix_bytes_for_test(bytes: Vec<u8>) -> Self {
        Self { bytes }
    }
}

/// Unnormalized logical request supplied by a named process surface.
#[derive(Clone, Eq, PartialEq)]
pub struct LogicalSpawnRequest {
    file: ProcessAtom,
    argv0: Option<ProcessAtom>,
    args: Vec<ProcessAtom>,
    cwd: Option<ProcessAtom>,
    env: Option<RuntimeEnv>,
    detached: bool,
    windows_hide: bool,
    windows_verbatim_arguments: bool,
}

impl fmt::Debug for LogicalSpawnRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LogicalSpawnRequest")
            .field("file", &"<redacted>")
            .field("argv0_present", &self.argv0.is_some())
            .field("argument_count", &self.args.len())
            .field("cwd_present", &self.cwd.is_some())
            .field("environment_present", &self.env.is_some())
            .field("detached", &self.detached)
            .field("windows_hide", &self.windows_hide)
            .field(
                "windows_verbatim_arguments",
                &self.windows_verbatim_arguments,
            )
            .finish()
    }
}

impl LogicalSpawnRequest {
    pub fn new(file: ProcessAtom) -> Self {
        Self {
            file,
            argv0: None,
            args: Vec::new(),
            cwd: None,
            env: None,
            detached: false,
            windows_hide: false,
            windows_verbatim_arguments: false,
        }
    }

    pub fn with_argv0(mut self, argv0: ProcessAtom) -> Self {
        self.argv0 = Some(argv0);
        self
    }

    pub fn with_args(mut self, args: Vec<ProcessAtom>) -> Self {
        self.args = args;
        self
    }

    pub fn with_cwd(mut self, cwd: ProcessAtom) -> Self {
        self.cwd = Some(cwd);
        self
    }

    pub fn with_env(mut self, env: RuntimeEnv) -> Self {
        self.env = Some(env);
        self
    }

    pub fn with_detached(mut self, detached: bool) -> Self {
        self.detached = detached;
        self
    }

    pub fn with_windows_hide(mut self, windows_hide: bool) -> Self {
        self.windows_hide = windows_hide;
        self
    }

    /// Reserved for generated, named process-surface transforms.
    #[allow(dead_code)]
    pub(crate) fn with_windows_verbatim_arguments(mut self, enabled: bool) -> Self {
        self.windows_verbatim_arguments = enabled;
        self
    }

    /// Apply Node's omitted-value normalization without projecting any atom.
    pub fn normalize_node(
        self,
        parent_environment: &RuntimeEnv,
    ) -> Result<NodeSpawnPlan, ProcessPlanError> {
        let argv0 = self.argv0.unwrap_or_else(|| self.file.clone());
        let environment = normalize_child_env(parent_environment, self.env.as_ref());

        NodeSpawnPlan::from_normalized_parts(
            self.file,
            argv0,
            self.args,
            self.cwd,
            ProcessEnvironmentPlan::NodeRuntime(environment),
            self.detached,
            self.windows_hide,
            self.windows_verbatim_arguments,
        )
    }
}

/// Fully normalized Node-compatible logical plan, still before host projection.
#[derive(Clone, Eq, PartialEq)]
pub struct NodeSpawnPlan {
    file: ProcessAtom,
    argv0: ProcessAtom,
    args: Vec<ProcessAtom>,
    cwd: Option<ProcessAtom>,
    environment: ProcessEnvironmentPlan,
    detached: bool,
    windows_hide: bool,
    windows_verbatim_arguments: bool,
}

impl fmt::Debug for NodeSpawnPlan {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NodeSpawnPlan")
            .field("file", &"<redacted>")
            .field("argument_count", &self.args.len().saturating_add(1))
            .field("cwd_present", &self.cwd.is_some())
            .field("environment", &self.environment)
            .field("detached", &self.detached)
            .field("windows_hide", &self.windows_hide)
            .field(
                "windows_verbatim_arguments",
                &self.windows_verbatim_arguments,
            )
            .finish()
    }
}

/// Rust 1.93 process input before its source-ordered environment capture and
/// deferred NUL validation.
///
/// This type is intentionally confined to the Unix/Windows Rust image
/// backends. It cannot be passed to the Node image builder, and it never
/// carries a `NodeRuntime` environment.
#[allow(dead_code)]
pub(super) struct Rust193SpawnPreimage {
    file: ProcessAtom,
    argv0: ProcessAtom,
    args: Vec<ProcessAtom>,
    cwd: Option<ProcessAtom>,
    environment: Rust193EnvironmentPlan,
}

#[allow(dead_code)]
impl fmt::Debug for Rust193SpawnPreimage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Rust193SpawnPreimage")
            .field("file", &"<redacted>")
            .field("argument_count", &self.args.len().saturating_add(1))
            .field("cwd_present", &self.cwd.is_some())
            .field("environment_kind", &self.environment.kind_name())
            .finish()
    }
}

#[allow(dead_code)]
#[derive(Clone, Eq, PartialEq)]
pub(super) enum Rust193EnvironmentPlan {
    NativeParentInherit,
    Overlay { mutations: Vec<Rust193EnvMutation> },
}

#[allow(dead_code)]
impl Rust193EnvironmentPlan {
    fn kind_name(&self) -> &'static str {
        match self {
            Self::NativeParentInherit => "NativeParentInherit",
            Self::Overlay { .. } => "Rust193Overlay",
        }
    }
}

#[allow(dead_code)]
impl Rust193SpawnPreimage {
    /// Construct the typed Rust preimage while deliberately deferring NUL
    /// validation until after the Rust source-ordered environment capture.
    pub(super) fn from_deferred_parts(
        file: ProcessAtom,
        argv0: ProcessAtom,
        args: Vec<ProcessAtom>,
        cwd: Option<ProcessAtom>,
        environment: Rust193EnvironmentPlan,
    ) -> Self {
        Self {
            file,
            argv0,
            args,
            cwd,
            environment,
        }
    }

    pub(super) fn file(&self) -> &ProcessAtom {
        &self.file
    }

    pub(super) fn argv0(&self) -> &ProcessAtom {
        &self.argv0
    }

    pub(super) fn args(&self) -> &[ProcessAtom] {
        &self.args
    }

    pub(super) fn cwd(&self) -> Option<&ProcessAtom> {
        self.cwd.as_ref()
    }

    pub(super) fn environment(&self) -> &Rust193EnvironmentPlan {
        &self.environment
    }

    pub(super) fn validate_deferred_nul(&self) -> Result<(), ProcessPlanError> {
        validate_atom(&self.file, ProcessPlanField::File)?;
        validate_atom(&self.argv0, ProcessPlanField::Argv0)?;
        for (index, argument) in self.args.iter().enumerate() {
            validate_atom(argument, ProcessPlanField::Argument(index))?;
        }
        if let Some(cwd) = &self.cwd {
            validate_atom(cwd, ProcessPlanField::Cwd)?;
        }
        // Rust 1.93 does not validate the mutation log itself. `CommandEnv`
        // first folds set/remove operations into its target-key map, then the
        // Unix envp constructor rejects NUL only in entries that survive in
        // the final captured map. The Unix image builder owns that later pass.
        Ok(())
    }
}

impl NodeSpawnPlan {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn from_normalized_parts(
        file: ProcessAtom,
        argv0: ProcessAtom,
        args: Vec<ProcessAtom>,
        cwd: Option<ProcessAtom>,
        environment: ProcessEnvironmentPlan,
        detached: bool,
        windows_hide: bool,
        windows_verbatim_arguments: bool,
    ) -> Result<Self, ProcessPlanError> {
        if atom_is_empty(&file) {
            return Err(ProcessPlanError::new(
                ProcessPlanField::File,
                ProcessPlanErrorKind::EmptyExecutable,
            ));
        }
        validate_atom(&file, ProcessPlanField::File)?;
        validate_atom(&argv0, ProcessPlanField::Argv0)?;
        for (index, argument) in args.iter().enumerate() {
            validate_atom(argument, ProcessPlanField::Argument(index))?;
        }
        if let Some(cwd) = &cwd {
            validate_atom(cwd, ProcessPlanField::Cwd)?;
        }
        validate_environment_plan(&environment)?;

        Ok(Self {
            file,
            argv0,
            args,
            cwd,
            environment,
            detached,
            windows_hide,
            windows_verbatim_arguments,
        })
    }

    pub fn file(&self) -> &ProcessAtom {
        &self.file
    }

    pub fn argv0(&self) -> &ProcessAtom {
        &self.argv0
    }

    pub fn args(&self) -> &[ProcessAtom] {
        &self.args
    }

    pub fn cwd(&self) -> Option<&ProcessAtom> {
        self.cwd.as_ref()
    }

    pub fn environment(&self) -> &ProcessEnvironmentPlan {
        &self.environment
    }

    pub fn detached(&self) -> bool {
        self.detached
    }

    pub fn windows_hide(&self) -> bool {
        self.windows_hide
    }

    pub fn windows_verbatim_arguments(&self) -> bool {
        self.windows_verbatim_arguments
    }
}

#[derive(Clone, Eq, PartialEq)]
pub enum ProcessEnvironmentPlan {
    NodeRuntime(RuntimeEnv),
    NativeParentInherit,
    Rust193Overlay { mutations: Vec<Rust193EnvMutation> },
}

impl fmt::Debug for ProcessEnvironmentPlan {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NodeRuntime(environment) => formatter
                .debug_struct("NodeRuntime")
                .field("property_count", &environment.len())
                .finish(),
            Self::NativeParentInherit => formatter.write_str("NativeParentInherit"),
            Self::Rust193Overlay { mutations } => formatter
                .debug_struct("Rust193Overlay")
                .field("mutation_count", &mutations.len())
                .finish(),
        }
    }
}

#[derive(Clone, Eq, PartialEq)]
pub enum Rust193EnvMutation {
    RuntimeSet {
        key: RuntimeString,
        value: RuntimeString,
    },
    RuntimeRemove {
        key: RuntimeString,
    },
    NativeHostSet(NativeHostEnvOverlay),
}

impl fmt::Debug for Rust193EnvMutation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RuntimeSet { .. } => formatter.write_str("RuntimeSet(..)"),
            Self::RuntimeRemove { .. } => formatter.write_str("RuntimeRemove(..)"),
            Self::NativeHostSet(_) => formatter.write_str("NativeHostSet(..)"),
        }
    }
}

/// The sole native-host environment exception; fields and construction remain
/// crate-private and policy-bound.
#[derive(Clone, Eq, Hash, PartialEq)]
pub struct NativeHostEnvOverlay {
    key: RuntimeString,
    value: NativeOsString,
}

impl fmt::Debug for NativeHostEnvOverlay {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("NativeHostEnvOverlay(..)")
    }
}

impl NativeHostEnvOverlay {
    pub(super) fn from_host_os_str(key: RuntimeString, value: &std::ffi::OsStr) -> Self {
        Self {
            key,
            value: NativeOsString::from_host_os_str(value),
        }
    }

    pub(super) fn key(&self) -> &RuntimeString {
        &self.key
    }

    pub(super) fn value(&self) -> &NativeOsString {
        &self.value
    }

    /// Deterministic native-host fixture constructor. The production builder
    /// stays private until the generated browser-sidecar binding lands. Gated with its
    /// only user, the unix-only env-mutation fixture in `unix_tests`.
    #[cfg(all(test, unix))]
    pub(super) fn new_for_test(key: RuntimeString, value: NativeOsString) -> Self {
        Self { key, value }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProcessPlanError {
    field: ProcessPlanField,
    kind: ProcessPlanErrorKind,
}

impl ProcessPlanError {
    fn new(field: ProcessPlanField, kind: ProcessPlanErrorKind) -> Self {
        Self { field, kind }
    }

    pub fn field(self) -> ProcessPlanField {
        self.field
    }

    pub fn kind(self) -> ProcessPlanErrorKind {
        self.kind
    }
}

impl fmt::Display for ProcessPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "process plan {:?} at {:?}",
            self.kind, self.field
        )
    }
}

impl std::error::Error for ProcessPlanError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProcessPlanField {
    File,
    Argv0,
    Argument(usize),
    Cwd,
    EnvironmentKey(usize),
    EnvironmentValue(usize),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProcessPlanErrorKind {
    EmptyExecutable,
    ContainsNul { unit_index: usize },
}

fn validate_environment(environment: &RuntimeEnv) -> Result<(), ProcessPlanError> {
    for (index, (key, value)) in environment.entries().enumerate() {
        if let Some(unit_index) = key.as_utf16().iter().position(|unit| *unit == 0) {
            return Err(ProcessPlanError::new(
                ProcessPlanField::EnvironmentKey(index),
                ProcessPlanErrorKind::ContainsNul { unit_index },
            ));
        }
        if let Some(unit_index) = value.as_utf16().iter().position(|unit| *unit == 0) {
            return Err(ProcessPlanError::new(
                ProcessPlanField::EnvironmentValue(index),
                ProcessPlanErrorKind::ContainsNul { unit_index },
            ));
        }
    }
    Ok(())
}

fn validate_environment_plan(environment: &ProcessEnvironmentPlan) -> Result<(), ProcessPlanError> {
    match environment {
        ProcessEnvironmentPlan::NodeRuntime(environment) => validate_environment(environment),
        ProcessEnvironmentPlan::NativeParentInherit => Ok(()),
        ProcessEnvironmentPlan::Rust193Overlay { mutations } => {
            validate_rust193_mutations(mutations)
        }
    }
}

fn validate_rust193_mutations(mutations: &[Rust193EnvMutation]) -> Result<(), ProcessPlanError> {
    for (index, mutation) in mutations.iter().enumerate() {
        match mutation {
            Rust193EnvMutation::RuntimeSet { key, value } => {
                validate_runtime_environment_atom(key, ProcessPlanField::EnvironmentKey(index))?;
                validate_runtime_environment_atom(
                    value,
                    ProcessPlanField::EnvironmentValue(index),
                )?;
            }
            Rust193EnvMutation::RuntimeRemove { key } => {
                validate_runtime_environment_atom(key, ProcessPlanField::EnvironmentKey(index))?;
            }
            Rust193EnvMutation::NativeHostSet(overlay) => {
                validate_runtime_environment_atom(
                    &overlay.key,
                    ProcessPlanField::EnvironmentKey(index),
                )?;
                validate_native_atom(&overlay.value, ProcessPlanField::EnvironmentValue(index))?;
            }
        }
    }
    Ok(())
}

fn validate_runtime_environment_atom(
    value: &RuntimeString,
    field: ProcessPlanField,
) -> Result<(), ProcessPlanError> {
    if let Some(unit_index) = value.as_utf16().iter().position(|unit| *unit == 0) {
        return Err(ProcessPlanError::new(
            field,
            ProcessPlanErrorKind::ContainsNul { unit_index },
        ));
    }
    Ok(())
}

fn atom_is_empty(atom: &ProcessAtom) -> bool {
    match atom {
        ProcessAtom::Runtime(value) => value.is_empty(),
        ProcessAtom::Native(value) => value.is_empty(),
    }
}

fn validate_atom(atom: &ProcessAtom, field: ProcessPlanField) -> Result<(), ProcessPlanError> {
    let nul_index = match atom {
        ProcessAtom::Runtime(value) => value.as_utf16().iter().position(|unit| *unit == 0),
        ProcessAtom::Native(value) => value.nul_index(),
    };
    if let Some(unit_index) = nul_index {
        return Err(ProcessPlanError::new(
            field,
            ProcessPlanErrorKind::ContainsNul { unit_index },
        ));
    }
    Ok(())
}

fn validate_native_atom(
    value: &NativeOsString,
    field: ProcessPlanField,
) -> Result<(), ProcessPlanError> {
    if let Some(unit_index) = value.nul_index() {
        return Err(ProcessPlanError::new(
            field,
            ProcessPlanErrorKind::ContainsNul { unit_index },
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        NativeHostEnvOverlay, NativeOsString, NodeSpawnPlan, ProcessAtom, ProcessEnvironmentPlan,
        ProcessPlanErrorKind, ProcessPlanField, Rust193EnvMutation,
    };
    use crate::runtime_values::RuntimeString;

    fn atom(value: &str) -> ProcessAtom {
        ProcessAtom::Runtime(RuntimeString::from_utf8(value))
    }

    fn overlay_plan(
        mutations: Vec<Rust193EnvMutation>,
    ) -> Result<NodeSpawnPlan, super::ProcessPlanError> {
        NodeSpawnPlan::from_normalized_parts(
            atom("node"),
            atom("node"),
            Vec::new(),
            None,
            ProcessEnvironmentPlan::Rust193Overlay { mutations },
            false,
            false,
            false,
        )
    }

    #[test]
    fn normalized_rust_overlay_rejects_nul_in_each_runtime_mutation_atom() {
        let nul = RuntimeString::from_utf16(vec![b'x'.into(), 0, b'y'.into()]);
        let cases = [
            (
                Rust193EnvMutation::RuntimeSet {
                    key: nul.clone(),
                    value: RuntimeString::from_utf8("value"),
                },
                ProcessPlanField::EnvironmentKey(0),
            ),
            (
                Rust193EnvMutation::RuntimeSet {
                    key: RuntimeString::from_utf8("KEY"),
                    value: nul.clone(),
                },
                ProcessPlanField::EnvironmentValue(0),
            ),
            (
                Rust193EnvMutation::RuntimeRemove { key: nul },
                ProcessPlanField::EnvironmentKey(0),
            ),
        ];

        for (mutation, field) in cases {
            let error = overlay_plan(vec![mutation]).unwrap_err();
            assert_eq!(error.field(), field);
            assert_eq!(
                error.kind(),
                ProcessPlanErrorKind::ContainsNul { unit_index: 1 }
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn normalized_rust_overlay_rejects_nul_in_native_host_value() {
        let error = overlay_plan(vec![Rust193EnvMutation::NativeHostSet(
            NativeHostEnvOverlay {
                key: RuntimeString::from_utf8("KEY"),
                value: NativeOsString {
                    bytes: vec![b'x', 0, b'y'],
                },
            },
        )])
        .unwrap_err();

        assert_eq!(error.field(), ProcessPlanField::EnvironmentValue(0));
        assert_eq!(
            error.kind(),
            ProcessPlanErrorKind::ContainsNul { unit_index: 1 }
        );
    }

    #[cfg(windows)]
    #[test]
    fn normalized_rust_overlay_rejects_nul_in_native_host_value() {
        let error = overlay_plan(vec![Rust193EnvMutation::NativeHostSet(
            NativeHostEnvOverlay {
                key: RuntimeString::from_utf8("KEY"),
                value: NativeOsString {
                    wide: vec![b'x'.into(), 0, b'y'.into()],
                },
            },
        )])
        .unwrap_err();

        assert_eq!(error.field(), ProcessPlanField::EnvironmentValue(0));
        assert_eq!(
            error.kind(),
            ProcessPlanErrorKind::ContainsNul { unit_index: 1 }
        );
    }
}
