//! Pure Unix process-image construction.
//!
//! This module owns no process or file descriptor. It only freezes the exact
//! C storage and pointer arrays consumed later by the project-owned Unix spawn
//! backend. In particular it never delegates environment construction to a
//! standard-library command builder.

use std::ffi::{CStr, CString};
use std::fmt;
use std::ptr;

use libc::c_char;

use crate::runtime_values::RuntimeString;

use super::env::RuntimeEnvRawKey;
use super::plan::{Rust193EnvironmentPlan, Rust193SpawnPreimage};
use super::process_environment::with_process_environment_lock;
use super::{
    NativeOsString, NodeSpawnPlan, ProcessAtom, ProcessEnvironmentGuard, ProcessEnvironmentPlan,
    ProcessImageDialect, ProcessPlanError, Rust193EnvMutation,
};

const NODE_OPTIONS_PREFIX: &[u8] = b"NODE_OPTIONS=";
const NODE_OPTIONS_KEY: &[u8] = b"NODE_OPTIONS";
const RUST193_PATH_KEY: &[u8] = b"PATH";

/// Immutable Unix `exec` input, before any descriptor or process exists.
pub(super) struct UnixExecImage {
    image_dialect: ProcessImageDialect,
    rust193_path_changed: Option<bool>,
    file: CString,
    cwd: Option<CString>,
    argv_storage: Vec<CString>,
    argv_with_null: Vec<*const c_char>,
    environment: UnixEnvironmentImage,
}

// SAFETY: every raw pointer in the immutable image targets storage owned by
// the same image (`CString` heap allocations or the corresponding owned
// environment storage). Moving the image between executor threads does not
// move those allocations, and no pointer is exposed or mutated after build.
// The spawn backend consumes the complete image before any owned storage can
// be dropped.
unsafe impl Send for UnixExecImage {}

/// Either the actual native `environ` pointer or one owned, null-terminated
/// environment vector.
pub(super) enum UnixEnvironmentImage {
    NativeParentInherit,
    Constructed {
        storage: Vec<CString>,
        envp_with_null: Vec<*const c_char>,
    },
}

/// Stable location for a NUL detected before any process can exist.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum UnixExecImageField {
    File,
    Cwd,
    Argv(usize),
    EnvironmentEntry(usize),
    ParentEnvironmentEntry(usize),
}

/// Named fallible allocation sites in the immutable Unix image builder.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum UnixAllocationStage {
    SnapshotCopy,
    RuntimeProjection,
    AtomStorage,
    ArgumentStorage,
    EnvironmentFilter,
    EnvironmentMap,
    EnvironmentStorage,
    PointerImage,
}

/// Pure pre-spawn image-construction failure. No descriptor, permit, or child
/// can exist when this value is returned.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum UnixExecImageBuildError {
    ContainsNul {
        field: UnixExecImageField,
        byte_index: usize,
    },
    SnapshotFailure,
    AllocationFailure {
        stage: UnixAllocationStage,
    },
    InvalidPlan(ProcessPlanError),
    GuardMismatch,
    Rust193PreimageRequired,
    ForbiddenNodeOptionsMutation,
}

impl fmt::Display for UnixExecImageBuildError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ContainsNul { field, byte_index } => write!(
                formatter,
                "Unix process image contains NUL at {field:?}[{byte_index}]"
            ),
            Self::SnapshotFailure => formatter.write_str("Unix parent environment snapshot failed"),
            Self::AllocationFailure { stage } => {
                write!(
                    formatter,
                    "Unix process image allocation failed at {stage:?}"
                )
            }
            Self::InvalidPlan(error) => error.fmt(formatter),
            Self::GuardMismatch => formatter.write_str("Unix process environment guard mismatch"),
            Self::Rust193PreimageRequired => {
                formatter.write_str("Rust 1.93 image requires its deferred typed preimage")
            }
            Self::ForbiddenNodeOptionsMutation => formatter.write_str(
                "Custom browser environment may not set NODE_OPTIONS in the exact runtime.",
            ),
        }
    }
}

impl std::error::Error for UnixExecImageBuildError {}

impl fmt::Debug for UnixExecImage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("UnixExecImage")
            .field("image_dialect", &self.image_dialect)
            .field("file", &"<redacted>")
            .field("cwd_present", &self.cwd.is_some())
            .field("argument_count", &self.argv_storage.len())
            .field("environment", &self.environment)
            .finish()
    }
}

impl fmt::Debug for UnixEnvironmentImage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NativeParentInherit => formatter.write_str("NativeParentInherit"),
            Self::Constructed { storage, .. } => formatter
                .debug_struct("Constructed")
                .field("entry_count", &storage.len())
                .finish(),
        }
    }
}

#[derive(Clone, Copy, Default)]
struct BuildContext {
    injected_allocation_failure: Option<UnixAllocationStage>,
}

impl BuildContext {
    fn reserve<T>(
        &self,
        values: &mut Vec<T>,
        additional: usize,
        stage: UnixAllocationStage,
    ) -> Result<(), UnixExecImageBuildError> {
        if self.injected_allocation_failure == Some(stage) {
            return Err(UnixExecImageBuildError::AllocationFailure { stage });
        }
        values
            .try_reserve_exact(additional)
            .map_err(|_| UnixExecImageBuildError::AllocationFailure { stage })
    }

    fn checked_add(
        &self,
        left: usize,
        right: usize,
        stage: UnixAllocationStage,
    ) -> Result<usize, UnixExecImageBuildError> {
        left.checked_add(right)
            .ok_or(UnixExecImageBuildError::AllocationFailure { stage })
    }
}

impl UnixExecImage {
    pub(super) const RUST_VERSION: &'static str = "1.93.0";
    pub(super) const RUSTC_COMMIT: &'static str = "254b59607d4417e9dffbc307138ae5c86280fe4c";
    pub(super) const RUST_STD_SOURCE_LOCK_SHA256: &'static str =
        "732802d2024c841ccb2a185f330121fac2640caa8172b94478ac4ab03ad8fffc";
    pub(super) const RUST_STD_SOURCE_TREE_SHA256: &'static str =
        "1cf578dea031527de9bec8f54d98b9c5b974771e405e44b0a08d16ba8db1f664";
    pub(super) const RUST_STD_SOURCE_FILE_COUNT: u64 = 719;
    pub(super) const RUST_PROCESS_ENV_RS_SHA256: &'static str =
        "71af6fa6cbff40c29f0996b547bca660265a214f8268e30f56a9f91998ced676";
    pub(super) const RUST_UNIX_ENV_RS_SHA256: &'static str =
        "c2405d1d33ed3c9e9ba257312375ba14b2d4b005164592a182212a0f21d97546";
    pub(super) const RUST_UNIX_COMMON_RS_SHA256: &'static str =
        "a2648365b0d4a1b970018e05e2623c556c4434032c639321776adb58dac97794";
    pub(super) const RUST_UNIX_SPAWN_RS_SHA256: &'static str =
        "934eb64b1177e97b06367e5e7711b46c0846d4d50c34e8b7ebd622537c4e3a87";
    pub(super) const RUST_CSTRING_ARRAY_RS_SHA256: &'static str =
        "3aafeda1861686a00841100822d2819825ee21ae0888695f3d3708a9d5080d2b";

    /// Build the already-validated Node/libuv Unix image. A Rust environment
    /// cannot enter this typed entrypoint.
    pub(super) fn build_node(
        plan: &NodeSpawnPlan,
        guard: ProcessEnvironmentGuard,
    ) -> Result<Self, UnixExecImageBuildError> {
        Self::build_node_with_context(plan, guard, &BuildContext::default())
    }

    fn build_node_with_context(
        plan: &NodeSpawnPlan,
        guard: ProcessEnvironmentGuard,
        context: &BuildContext,
    ) -> Result<Self, UnixExecImageBuildError> {
        let ProcessEnvironmentPlan::NodeRuntime(environment) = plan.environment() else {
            return Err(UnixExecImageBuildError::Rust193PreimageRequired);
        };
        if guard != ProcessEnvironmentGuard::None {
            return Err(UnixExecImageBuildError::GuardMismatch);
        }

        let (file, cwd, argv_storage, argv_with_null) =
            build_atom_image(plan.file(), plan.argv0(), plan.args(), plan.cwd(), context)?;
        let environment = build_node_environment(environment, context)?;
        Ok(Self {
            image_dialect: ProcessImageDialect::Node20Libuv146,
            rust193_path_changed: None,
            file,
            cwd,
            argv_storage,
            argv_with_null,
            environment,
        })
    }

    /// Build a Rust 1.93 image in its pinned source order: capture the selected
    /// environment first, then validate deferred NULs, then perform terminal
    /// runtime/native projection and immutable storage construction.
    pub(super) fn build_rust193(
        preimage: &Rust193SpawnPreimage,
        guard: ProcessEnvironmentGuard,
    ) -> Result<Self, UnixExecImageBuildError> {
        Self::build_rust193_from_snapshot(
            preimage,
            guard,
            read_parent_environ,
            &BuildContext::default(),
        )
    }

    fn build_rust193_from_snapshot<F>(
        preimage: &Rust193SpawnPreimage,
        guard: ProcessEnvironmentGuard,
        parent_snapshot: F,
        context: &BuildContext,
    ) -> Result<Self, UnixExecImageBuildError>
    where
        F: FnOnce(&BuildContext) -> Result<Vec<Vec<u8>>, UnixExecImageBuildError>,
    {
        validate_rust193_guard(preimage.environment(), guard)?;

        let captured_parent = match preimage.environment() {
            Rust193EnvironmentPlan::NativeParentInherit
                if guard == ProcessEnvironmentGuard::None =>
            {
                None
            }
            Rust193EnvironmentPlan::NativeParentInherit
            | Rust193EnvironmentPlan::Overlay { .. } => Some(parent_snapshot(context)?),
        };

        preimage
            .validate_deferred_nul()
            .map_err(UnixExecImageBuildError::InvalidPlan)?;

        let (file, cwd, argv_storage, argv_with_null) = build_atom_image(
            preimage.file(),
            preimage.argv0(),
            preimage.args(),
            preimage.cwd(),
            context,
        )?;
        let rust193_path_changed = rust193_environment_changed_path(preimage.environment());
        let environment = match preimage.environment() {
            Rust193EnvironmentPlan::NativeParentInherit
                if guard == ProcessEnvironmentGuard::None =>
            {
                UnixEnvironmentImage::NativeParentInherit
            }
            Rust193EnvironmentPlan::NativeParentInherit => filter_native_parent(
                captured_parent.ok_or(UnixExecImageBuildError::SnapshotFailure)?,
                context,
            )?,
            Rust193EnvironmentPlan::Overlay { mutations } => build_rust193_environment(
                captured_parent.ok_or(UnixExecImageBuildError::SnapshotFailure)?,
                mutations,
                guard,
                context,
            )?,
        };

        Ok(Self {
            image_dialect: ProcessImageDialect::Rust193,
            rust193_path_changed: Some(rust193_path_changed),
            file,
            cwd,
            argv_storage,
            argv_with_null,
            environment,
        })
    }

    #[cfg(test)]
    pub(super) fn build_node_with_fault(
        plan: &NodeSpawnPlan,
        guard: ProcessEnvironmentGuard,
        stage: UnixAllocationStage,
    ) -> Result<Self, UnixExecImageBuildError> {
        Self::build_node_with_context(
            plan,
            guard,
            &BuildContext {
                injected_allocation_failure: Some(stage),
            },
        )
    }

    #[cfg(test)]
    pub(super) fn build_rust193_with_parent_snapshot<F>(
        preimage: &Rust193SpawnPreimage,
        guard: ProcessEnvironmentGuard,
        parent_snapshot: F,
    ) -> Result<Self, UnixExecImageBuildError>
    where
        F: FnOnce() -> Result<Vec<Vec<u8>>, UnixExecImageBuildError>,
    {
        Self::build_rust193_from_snapshot(
            preimage,
            guard,
            |_| parent_snapshot(),
            &BuildContext::default(),
        )
    }

    #[cfg(test)]
    pub(super) fn build_rust193_with_fault(
        preimage: &Rust193SpawnPreimage,
        guard: ProcessEnvironmentGuard,
        parent_snapshot: Result<Vec<Vec<u8>>, UnixExecImageBuildError>,
        stage: UnixAllocationStage,
    ) -> Result<Self, UnixExecImageBuildError> {
        Self::build_rust193_from_snapshot(
            preimage,
            guard,
            |_| parent_snapshot,
            &BuildContext {
                injected_allocation_failure: Some(stage),
            },
        )
    }

    #[cfg(test)]
    pub(super) fn build_rust193_with_actual_snapshot_fault(
        preimage: &Rust193SpawnPreimage,
        guard: ProcessEnvironmentGuard,
        stage: UnixAllocationStage,
    ) -> Result<Self, UnixExecImageBuildError> {
        Self::build_rust193_from_snapshot(
            preimage,
            guard,
            read_parent_environ,
            &BuildContext {
                injected_allocation_failure: Some(stage),
            },
        )
    }

    pub(super) fn file(&self) -> &CStr {
        &self.file
    }

    pub(super) const fn image_dialect(&self) -> ProcessImageDialect {
        self.image_dialect
    }

    pub(super) const fn rust193_path_changed(&self) -> Option<bool> {
        self.rust193_path_changed
    }

    pub(super) fn cwd(&self) -> Option<&CStr> {
        self.cwd.as_deref()
    }

    pub(super) fn argv_storage(&self) -> &[CString] {
        &self.argv_storage
    }

    pub(super) fn argv_with_null(&self) -> &[*const c_char] {
        &self.argv_with_null
    }

    pub(super) fn environment(&self) -> &UnixEnvironmentImage {
        &self.environment
    }
}

fn rust193_environment_changed_path(environment: &Rust193EnvironmentPlan) -> bool {
    let Rust193EnvironmentPlan::Overlay { mutations } = environment else {
        return false;
    };
    mutations.iter().any(|mutation| {
        let key = match mutation {
            Rust193EnvMutation::RuntimeSet { key, .. }
            | Rust193EnvMutation::RuntimeRemove { key } => key,
            Rust193EnvMutation::NativeHostSet(overlay) => overlay.key(),
        };
        key.as_utf16()
            .iter()
            .copied()
            .eq(RUST193_PATH_KEY.iter().copied().map(u16::from))
    })
}

impl UnixEnvironmentImage {
    pub(super) fn constructed_storage(&self) -> Option<&[CString]> {
        match self {
            Self::NativeParentInherit => None,
            Self::Constructed { storage, .. } => Some(storage),
        }
    }

    pub(super) fn constructed_envp(&self) -> Option<&[*const c_char]> {
        match self {
            Self::NativeParentInherit => None,
            Self::Constructed { envp_with_null, .. } => Some(envp_with_null),
        }
    }
}

fn validate_rust193_guard(
    environment: &Rust193EnvironmentPlan,
    guard: ProcessEnvironmentGuard,
) -> Result<(), UnixExecImageBuildError> {
    if guard != ProcessEnvironmentGuard::SealedNodeNoPreloadV1 {
        return Ok(());
    }
    let Rust193EnvironmentPlan::Overlay { mutations } = environment else {
        return Ok(());
    };
    let forbidden = mutations.iter().any(|mutation| match mutation {
        Rust193EnvMutation::RuntimeSet { key, .. } => key_targets_node_options(key),
        Rust193EnvMutation::NativeHostSet(overlay) => key_targets_node_options(overlay.key()),
        Rust193EnvMutation::RuntimeRemove { .. } => false,
    });
    if forbidden {
        Err(UnixExecImageBuildError::ForbiddenNodeOptionsMutation)
    } else {
        Ok(())
    }
}

fn key_targets_node_options(key: &RuntimeString) -> bool {
    key.as_utf16()
        .iter()
        .copied()
        .eq(NODE_OPTIONS_KEY.iter().copied().map(u16::from))
}

fn build_atom_image(
    file: &ProcessAtom,
    argv0: &ProcessAtom,
    args: &[ProcessAtom],
    cwd: Option<&ProcessAtom>,
    context: &BuildContext,
) -> Result<AtomImage, UnixExecImageBuildError> {
    let file = project_atom(file, UnixExecImageField::File, context)?;
    let cwd = cwd
        .map(|cwd| project_atom(cwd, UnixExecImageField::Cwd, context))
        .transpose()?;
    let argument_count =
        context.checked_add(args.len(), 1, UnixAllocationStage::ArgumentStorage)?;
    let mut argv_storage = Vec::new();
    context.reserve(
        &mut argv_storage,
        argument_count,
        UnixAllocationStage::ArgumentStorage,
    )?;
    argv_storage.push(project_atom(argv0, UnixExecImageField::Argv(0), context)?);
    for (index, argument) in args.iter().enumerate() {
        argv_storage.push(project_atom(
            argument,
            UnixExecImageField::Argv(index + 1),
            context,
        )?);
    }
    let argv_with_null = pointer_image(&argv_storage, context)?;
    Ok((file, cwd, argv_storage, argv_with_null))
}

type AtomImage = (CString, Option<CString>, Vec<CString>, Vec<*const c_char>);

fn project_atom(
    atom: &ProcessAtom,
    field: UnixExecImageField,
    context: &BuildContext,
) -> Result<CString, UnixExecImageBuildError> {
    let (bytes, stage) = match atom {
        ProcessAtom::Runtime(value) => (
            project_runtime_string(value, context)?,
            UnixAllocationStage::AtomStorage,
        ),
        ProcessAtom::Native(value) => (
            copy_bytes(
                native_unix_bytes(value),
                UnixAllocationStage::AtomStorage,
                context,
            )?,
            UnixAllocationStage::AtomStorage,
        ),
    };
    cstring(bytes, field, stage, context)
}

/// The module's sole logical-runtime-to-Unix projection algorithm.
fn project_runtime_string(
    value: &RuntimeString,
    context: &BuildContext,
) -> Result<Vec<u8>, UnixExecImageBuildError> {
    let capacity =
        value
            .len_utf16()
            .checked_mul(3)
            .ok_or(UnixExecImageBuildError::AllocationFailure {
                stage: UnixAllocationStage::RuntimeProjection,
            })?;
    let mut output = Vec::new();
    context.reserve(
        &mut output,
        capacity,
        UnixAllocationStage::RuntimeProjection,
    )?;
    for point in value.code_points() {
        let scalar = char::from_u32(point).unwrap_or(char::REPLACEMENT_CHARACTER);
        let mut bytes = [0; 4];
        output.extend_from_slice(scalar.encode_utf8(&mut bytes).as_bytes());
    }
    Ok(output)
}

fn native_unix_bytes(value: &NativeOsString) -> &[u8] {
    value.as_unix_bytes()
}

fn copy_bytes(
    source: &[u8],
    stage: UnixAllocationStage,
    context: &BuildContext,
) -> Result<Vec<u8>, UnixExecImageBuildError> {
    let mut output = Vec::new();
    context.reserve(&mut output, source.len(), stage)?;
    output.extend_from_slice(source);
    Ok(output)
}

fn cstring(
    mut bytes: Vec<u8>,
    field: UnixExecImageField,
    stage: UnixAllocationStage,
    context: &BuildContext,
) -> Result<CString, UnixExecImageBuildError> {
    context.reserve(&mut bytes, 1, stage)?;
    CString::new(bytes).map_err(|error| UnixExecImageBuildError::ContainsNul {
        field,
        byte_index: error.nul_position(),
    })
}

fn pointer_image(
    storage: &[CString],
    context: &BuildContext,
) -> Result<Vec<*const c_char>, UnixExecImageBuildError> {
    let count = context.checked_add(storage.len(), 1, UnixAllocationStage::PointerImage)?;
    let mut pointers = Vec::new();
    context.reserve(&mut pointers, count, UnixAllocationStage::PointerImage)?;
    pointers.extend(storage.iter().map(|entry| entry.as_ptr()));
    pointers.push(ptr::null());
    Ok(pointers)
}

fn constructed_environment(
    storage: Vec<CString>,
    context: &BuildContext,
) -> Result<UnixEnvironmentImage, UnixExecImageBuildError> {
    let envp_with_null = pointer_image(&storage, context)?;
    Ok(UnixEnvironmentImage::Constructed {
        storage,
        envp_with_null,
    })
}

fn build_node_environment(
    environment: &super::RuntimeEnv,
    context: &BuildContext,
) -> Result<UnixEnvironmentImage, UnixExecImageBuildError> {
    let mut storage = Vec::new();
    context.reserve(
        &mut storage,
        environment.len(),
        UnixAllocationStage::EnvironmentStorage,
    )?;
    for (index, (key, value)) in environment.raw_entries().enumerate() {
        let mut entry = match key {
            RuntimeEnvRawKey::ArrayIndex(index) => project_array_index(index, context)?,
            RuntimeEnvRawKey::String(key) => project_runtime_string(key, context)?,
        };
        let value = project_runtime_string(value, context)?;
        let additional =
            context.checked_add(value.len(), 1, UnixAllocationStage::EnvironmentStorage)?;
        context.reserve(
            &mut entry,
            additional,
            UnixAllocationStage::EnvironmentStorage,
        )?;
        entry.push(b'=');
        entry.extend_from_slice(&value);
        storage.push(cstring(
            entry,
            UnixExecImageField::EnvironmentEntry(index),
            UnixAllocationStage::EnvironmentStorage,
            context,
        )?);
    }
    constructed_environment(storage, context)
}

fn project_array_index(
    mut index: u32,
    context: &BuildContext,
) -> Result<Vec<u8>, UnixExecImageBuildError> {
    let mut digits = [0_u8; 10];
    let mut cursor = digits.len();
    loop {
        cursor -= 1;
        digits[cursor] = b'0' + (index % 10) as u8;
        index /= 10;
        if index == 0 {
            break;
        }
    }
    copy_bytes(
        &digits[cursor..],
        UnixAllocationStage::RuntimeProjection,
        context,
    )
}

fn filter_native_parent(
    entries: Vec<Vec<u8>>,
    context: &BuildContext,
) -> Result<UnixEnvironmentImage, UnixExecImageBuildError> {
    let kept = entries
        .iter()
        .filter(|entry| !entry.starts_with(NODE_OPTIONS_PREFIX))
        .count();
    let mut storage = Vec::new();
    context.reserve(&mut storage, kept, UnixAllocationStage::EnvironmentFilter)?;
    for (index, entry) in entries.into_iter().enumerate() {
        if entry.starts_with(NODE_OPTIONS_PREFIX) {
            continue;
        }
        storage.push(cstring(
            entry,
            UnixExecImageField::ParentEnvironmentEntry(index),
            UnixAllocationStage::EnvironmentFilter,
            context,
        )?);
    }
    constructed_environment(storage, context)
}

struct SortedEnvironmentMap {
    entries: Vec<(Vec<u8>, Vec<u8>)>,
}

impl SortedEnvironmentMap {
    fn capture(
        raw_entries: Vec<Vec<u8>>,
        mutation_count: usize,
        context: &BuildContext,
    ) -> Result<Self, UnixExecImageBuildError> {
        let capacity = context.checked_add(
            raw_entries.len(),
            mutation_count,
            UnixAllocationStage::EnvironmentMap,
        )?;
        let mut entries = Vec::new();
        context.reserve(&mut entries, capacity, UnixAllocationStage::EnvironmentMap)?;
        let mut map = Self { entries };
        for raw in raw_entries {
            if raw.is_empty() {
                continue;
            }
            let Some(equals) = raw[1..]
                .iter()
                .position(|byte| *byte == b'=')
                .map(|position| position + 1)
            else {
                continue;
            };
            let key = copy_bytes(&raw[..equals], UnixAllocationStage::EnvironmentMap, context)?;
            let value = copy_bytes(
                &raw[equals + 1..],
                UnixAllocationStage::EnvironmentMap,
                context,
            )?;
            map.insert(key, value);
        }
        Ok(map)
    }

    fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) {
        match self
            .entries
            .binary_search_by(|(existing, _)| existing.as_slice().cmp(&key))
        {
            Ok(index) => self.entries[index].1 = value,
            Err(index) => self.entries.insert(index, (key, value)),
        }
    }

    fn remove(&mut self, key: &[u8]) {
        if let Ok(index) = self
            .entries
            .binary_search_by(|(existing, _)| existing.as_slice().cmp(key))
        {
            self.entries.remove(index);
        }
    }
}

fn build_rust193_environment(
    parent: Vec<Vec<u8>>,
    mutations: &[Rust193EnvMutation],
    guard: ProcessEnvironmentGuard,
    context: &BuildContext,
) -> Result<UnixEnvironmentImage, UnixExecImageBuildError> {
    let mut environment = SortedEnvironmentMap::capture(parent, mutations.len(), context)?;
    for mutation in mutations {
        match mutation {
            Rust193EnvMutation::RuntimeSet { key, value } => environment.insert(
                project_runtime_string(key, context)?,
                project_runtime_string(value, context)?,
            ),
            Rust193EnvMutation::RuntimeRemove { key } => {
                environment.remove(&project_runtime_string(key, context)?);
            }
            Rust193EnvMutation::NativeHostSet(overlay) => environment.insert(
                project_runtime_string(overlay.key(), context)?,
                copy_bytes(
                    native_unix_bytes(overlay.value()),
                    UnixAllocationStage::EnvironmentMap,
                    context,
                )?,
            ),
        }
    }
    if guard == ProcessEnvironmentGuard::SealedNodeNoPreloadV1 {
        environment.remove(NODE_OPTIONS_KEY);
    }

    let mut storage = Vec::new();
    context.reserve(
        &mut storage,
        environment.entries.len(),
        UnixAllocationStage::EnvironmentStorage,
    )?;
    for (index, (mut key, value)) in environment.entries.into_iter().enumerate() {
        let additional =
            context.checked_add(value.len(), 1, UnixAllocationStage::EnvironmentStorage)?;
        context.reserve(
            &mut key,
            additional,
            UnixAllocationStage::EnvironmentStorage,
        )?;
        key.push(b'=');
        key.extend_from_slice(&value);
        storage.push(cstring(
            key,
            UnixExecImageField::EnvironmentEntry(index),
            UnixAllocationStage::EnvironmentStorage,
            context,
        )?);
    }
    constructed_environment(storage, context)
}

fn read_parent_environ(context: &BuildContext) -> Result<Vec<Vec<u8>>, UnixExecImageBuildError> {
    with_process_environment_lock(|| {
        let mut entries = Vec::new();
        // SAFETY: the shared process-environment lock covers the complete raw
        // snapshot, and each C entry is copied before the cursor advances.
        unsafe {
            let mut cursor = parent_environ();
            while !cursor.is_null() && !(*cursor).is_null() {
                context.reserve(&mut entries, 1, UnixAllocationStage::SnapshotCopy)?;
                entries.push(copy_bytes(
                    CStr::from_ptr(*cursor).to_bytes(),
                    UnixAllocationStage::SnapshotCopy,
                    context,
                )?);
                cursor = cursor.add(1);
            }
        }
        Ok(entries)
    })
}

#[cfg(target_vendor = "apple")]
unsafe fn parent_environ() -> *mut *mut c_char {
    unsafe extern "C" {
        fn _NSGetEnviron() -> *mut *mut *mut c_char;
    }
    // SAFETY: Darwin exposes its live `environ` slot through this function.
    unsafe { *_NSGetEnviron() }
}

#[cfg(not(target_vendor = "apple"))]
unsafe fn parent_environ() -> *mut *mut c_char {
    unsafe extern "C" {
        static mut environ: *mut *mut c_char;
    }
    // SAFETY: copy the exported pointer without retaining a Rust reference to
    // the mutable static.
    unsafe { environ }
}
