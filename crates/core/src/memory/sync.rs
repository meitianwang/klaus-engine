use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

use crate::runtime_values::runtime_codec::{RuntimeEnumVariant, RuntimeExternalVariant};
use crate::runtime_values::{
    runtime_codec, RuntimeCodecError, RuntimeCodecErrorKind, RuntimeDecode, RuntimeEncode,
    RuntimeEncoder, RuntimeJson, RuntimeObjectEncoder, RuntimeString,
};

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

/// One versioned memory entry from either local storage or a remote backend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemorySyncItem {
    pub key: RuntimeString,
    pub version: Option<RuntimeString>,
    pub body: Option<RuntimeString>,
    pub deleted: bool,
}

impl RuntimeEncode for MemorySyncItem {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("key", &self.key)?;
        object.field("version", &self.version)?;
        object.field("body", &self.body)?;
        object.field("deleted", &self.deleted)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for MemorySyncItem {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                key: object.required("key", &[])?,
                version: object.optional("version", &[])?,
                body: object.optional("body", &[])?,
                deleted: object.required("deleted", &[])?,
            })
        })
    }
}

/// Whether memory sync is disabled or should compare local and remote state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum MemorySyncMode {
    LocalOnly,
    Synced,
}

impl RuntimeEncode for MemorySyncMode {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let tag = match self {
            Self::LocalOnly => "localOnly",
            Self::Synced => "synced",
        };
        runtime_codec::encode_external_unit(out, tag)
    }
}

impl RuntimeDecode for MemorySyncMode {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        decode_provider_unit_enum(
            value,
            &[
                RuntimeEnumVariant::new("localOnly", &[]),
                RuntimeEnumVariant::new("synced", &[]),
            ],
            &[Self::LocalOnly, Self::Synced],
        )
    }
}

/// Remote state as seen by the host.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemoteMemoryState {
    Ready(Vec<MemorySyncItem>),
    Unavailable(RuntimeString),
}

impl RuntimeEncode for RemoteMemoryState {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        match self {
            Self::Ready(items) => runtime_codec::encode_external_payload(out, "ready", items),
            Self::Unavailable(reason) => {
                runtime_codec::encode_external_payload(out, "unavailable", reason)
            }
        }
    }
}

impl RuntimeDecode for RemoteMemoryState {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_external(
            value,
            &[
                RuntimeEnumVariant::new("ready", &[]),
                RuntimeEnumVariant::new("unavailable", &[]),
            ],
            |variant| match variant {
                RuntimeExternalVariant::Payload { variant: 0, value } => {
                    Ok(Self::Ready(Vec::<MemorySyncItem>::decode_runtime(value)?))
                }
                RuntimeExternalVariant::Payload { variant: 1, value } => {
                    Ok(Self::Unavailable(RuntimeString::decode_runtime(value)?))
                }
                RuntimeExternalVariant::Unit { .. } => Err(RuntimeCodecError::new(
                    RuntimeCodecErrorKind::InvalidEnumShape,
                )),
                RuntimeExternalVariant::Payload { .. } => unreachable!("two declared variants"),
            },
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemorySyncInput {
    pub mode: MemorySyncMode,
    pub local: Vec<MemorySyncItem>,
    pub remote: RemoteMemoryState,
}

impl RuntimeEncode for MemorySyncInput {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("mode", &self.mode)?;
        object.field("local", &self.local)?;
        object.field("remote", &self.remote)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for MemorySyncInput {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                mode: object.required("mode", &[])?,
                local: object.required("local", &[])?,
                remote: object.required("remote", &[])?,
            })
        })
    }
}

/// Deterministic memory sync plan. Conflicts are reported instead of producing
/// destructive operations for that key.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MemorySyncPlan {
    pub operations: Vec<MemorySyncOperation>,
    pub conflicts: Vec<MemorySyncConflict>,
}

impl RuntimeEncode for MemorySyncPlan {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("operations", &self.operations)?;
        object.field("conflicts", &self.conflicts)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for MemorySyncPlan {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                operations: object.required("operations", &[])?,
                conflicts: object.required("conflicts", &[])?,
            })
        })
    }
}

/// Host-reported result after applying a sync plan's non-conflicting operations.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MemorySyncApplyResult {
    pub applied_operations: usize,
}

impl RuntimeEncode for MemorySyncApplyResult {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let Self { applied_operations } = self;
        let mut object = RuntimeObjectEncoder::new();
        object.field("appliedOperations", applied_operations)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for MemorySyncApplyResult {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                applied_operations: object.required("appliedOperations", &[])?,
            })
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MemorySyncOperation {
    KeepLocal {
        key: RuntimeString,
    },
    PushLocal {
        item: MemorySyncItem,
    },
    PullRemote {
        item: MemorySyncItem,
    },
    DeleteLocal {
        key: RuntimeString,
    },
    DeleteRemote {
        key: RuntimeString,
        version: Option<RuntimeString>,
    },
}

impl RuntimeEncode for MemorySyncOperation {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut fields = RuntimeObjectEncoder::new();
        let tag = match self {
            Self::KeepLocal { key } => {
                fields.field("key", key)?;
                "keepLocal"
            }
            Self::PushLocal { item } => {
                fields.field("item", item)?;
                "pushLocal"
            }
            Self::PullRemote { item } => {
                fields.field("item", item)?;
                "pullRemote"
            }
            Self::DeleteLocal { key } => {
                fields.field("key", key)?;
                "deleteLocal"
            }
            Self::DeleteRemote { key, version } => {
                fields.field("key", key)?;
                fields.field("version", version)?;
                "deleteRemote"
            }
        };
        runtime_codec::encode_internal(out, "type", &[], tag, &fields.finish())
    }
}

impl RuntimeDecode for MemorySyncOperation {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_internal(
            value,
            false,
            "type",
            &[],
            &[
                RuntimeEnumVariant::new("keepLocal", &[]),
                RuntimeEnumVariant::new("pushLocal", &[]),
                RuntimeEnumVariant::new("pullRemote", &[]),
                RuntimeEnumVariant::new("deleteLocal", &[]),
                RuntimeEnumVariant::new("deleteRemote", &[]),
            ],
            |variant, object| match variant {
                0 => Ok(Self::KeepLocal {
                    key: object.required("key", &[])?,
                }),
                1 => Ok(Self::PushLocal {
                    item: object.required("item", &[])?,
                }),
                2 => Ok(Self::PullRemote {
                    item: object.required("item", &[])?,
                }),
                3 => Ok(Self::DeleteLocal {
                    key: object.required("key", &[])?,
                }),
                4 => Ok(Self::DeleteRemote {
                    key: object.required("key", &[])?,
                    version: object.optional("version", &[])?,
                }),
                _ => unreachable!("variant index is bounded by the declaration"),
            },
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemorySyncConflict {
    pub key: RuntimeString,
    pub local_version: Option<RuntimeString>,
    pub remote_version: Option<RuntimeString>,
}

impl RuntimeEncode for MemorySyncConflict {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("key", &self.key)?;
        object.field("localVersion", &self.local_version)?;
        object.field("remoteVersion", &self.remote_version)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for MemorySyncConflict {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, false, |object| {
            Ok(Self {
                key: object.required("key", &[])?,
                local_version: object.optional("localVersion", &[])?,
                remote_version: object.optional("remoteVersion", &[])?,
            })
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MemorySyncError {
    BackendUnavailable { reason: RuntimeString },
}

impl RuntimeEncode for MemorySyncError {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut fields = RuntimeObjectEncoder::new();
        let tag = match self {
            Self::BackendUnavailable { reason } => {
                fields.field("reason", reason)?;
                "backendUnavailable"
            }
        };
        runtime_codec::encode_internal(out, "type", &[], tag, &fields.finish())
    }
}

impl RuntimeDecode for MemorySyncError {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_internal(
            value,
            false,
            "type",
            &[],
            &[RuntimeEnumVariant::new("backendUnavailable", &[])],
            |variant, object| match variant {
                0 => Ok(Self::BackendUnavailable {
                    reason: object.required("reason", &[])?,
                }),
                _ => unreachable!("variant index is bounded by the declaration"),
            },
        )
    }
}

pub fn plan_memory_sync(input: MemorySyncInput) -> Result<MemorySyncPlan, MemorySyncError> {
    let local = keyed(input.local);
    if input.mode == MemorySyncMode::LocalOnly {
        let operations = local
            .values()
            .filter(|item| !item.deleted)
            .map(|item| MemorySyncOperation::KeepLocal {
                key: item.key.clone(),
            })
            .collect();
        return Ok(MemorySyncPlan {
            operations,
            conflicts: Vec::new(),
        });
    }

    let remote = match input.remote {
        RemoteMemoryState::Ready(items) => keyed(items),
        RemoteMemoryState::Unavailable(reason) => {
            return Err(MemorySyncError::BackendUnavailable { reason });
        }
    };

    let mut keys: BTreeSet<RuntimeString> = local.keys().cloned().collect();
    keys.extend(remote.keys().cloned());

    let mut plan = MemorySyncPlan::default();
    for key in keys {
        match (local.get(&key), remote.get(&key)) {
            (Some(local), None) => {
                if !local.deleted {
                    plan.operations.push(MemorySyncOperation::PushLocal {
                        item: local.clone(),
                    });
                }
            }
            (None, Some(remote)) => {
                if !remote.deleted {
                    plan.operations.push(MemorySyncOperation::PullRemote {
                        item: remote.clone(),
                    });
                }
            }
            (Some(local), Some(remote)) => plan_pair(local, remote, &mut plan),
            (None, None) => {}
        }
    }
    Ok(plan)
}

fn keyed(items: Vec<MemorySyncItem>) -> BTreeMap<RuntimeString, MemorySyncItem> {
    items
        .into_iter()
        .map(|item| (item.key.clone(), item))
        .collect()
}

fn plan_pair(local: &MemorySyncItem, remote: &MemorySyncItem, plan: &mut MemorySyncPlan) {
    if local.deleted && remote.deleted {
        return;
    }
    if local.deleted {
        if same_version(local, remote) {
            plan.operations.push(MemorySyncOperation::DeleteRemote {
                key: local.key.clone(),
                version: remote.version.clone(),
            });
        } else {
            push_conflict(local, remote, plan);
        }
        return;
    }
    if remote.deleted {
        if same_version(local, remote) {
            plan.operations.push(MemorySyncOperation::DeleteLocal {
                key: local.key.clone(),
            });
        } else {
            push_conflict(local, remote, plan);
        }
        return;
    }
    if local.body == remote.body {
        if same_version(local, remote) {
            plan.operations.push(MemorySyncOperation::KeepLocal {
                key: local.key.clone(),
            });
        } else {
            plan.operations.push(MemorySyncOperation::PullRemote {
                item: remote.clone(),
            });
        }
        return;
    }
    push_conflict(local, remote, plan);
}

fn same_version(local: &MemorySyncItem, remote: &MemorySyncItem) -> bool {
    local.version == remote.version
}

fn push_conflict(local: &MemorySyncItem, remote: &MemorySyncItem, plan: &mut MemorySyncPlan) {
    plan.conflicts.push(MemorySyncConflict {
        key: local.key.clone(),
        local_version: local.version.clone(),
        remote_version: remote.version.clone(),
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item(key: &str, version: Option<&str>, body: &str) -> MemorySyncItem {
        MemorySyncItem {
            key: key.into(),
            version: version.map(RuntimeString::from),
            body: Some(body.into()),
            deleted: false,
        }
    }

    fn deleted(key: &str, version: Option<&str>) -> MemorySyncItem {
        MemorySyncItem {
            key: key.into(),
            version: version.map(RuntimeString::from),
            body: None,
            deleted: true,
        }
    }

    #[test]
    fn local_only_mode_keeps_local_records_without_remote() {
        let plan = plan_memory_sync(MemorySyncInput {
            mode: MemorySyncMode::LocalOnly,
            local: vec![item("alpha", None, "A"), deleted("old", Some("v1"))],
            remote: RemoteMemoryState::Unavailable("network down".into()),
        })
        .expect("local-only sync should not require remote state");

        assert_eq!(
            plan.operations,
            vec![MemorySyncOperation::KeepLocal {
                key: "alpha".into()
            }]
        );
        assert!(plan.conflicts.is_empty());
    }

    #[test]
    fn synced_mode_pushes_local_and_pulls_remote() {
        let plan = plan_memory_sync(MemorySyncInput {
            mode: MemorySyncMode::Synced,
            local: vec![item("local", None, "local body")],
            remote: RemoteMemoryState::Ready(vec![item("remote", Some("rv1"), "remote body")]),
        })
        .expect("synced plan");

        assert_eq!(
            plan.operations,
            vec![
                MemorySyncOperation::PushLocal {
                    item: item("local", None, "local body")
                },
                MemorySyncOperation::PullRemote {
                    item: item("remote", Some("rv1"), "remote body")
                }
            ]
        );
        assert!(plan.conflicts.is_empty());
    }

    #[test]
    fn different_live_versions_are_conflicts() {
        let plan = plan_memory_sync(MemorySyncInput {
            mode: MemorySyncMode::Synced,
            local: vec![item("shared", Some("v1"), "local body")],
            remote: RemoteMemoryState::Ready(vec![item("shared", Some("v2"), "remote body")]),
        })
        .expect("conflicts are reported in the plan");

        assert!(plan.operations.is_empty());
        assert_eq!(
            plan.conflicts,
            vec![MemorySyncConflict {
                key: "shared".into(),
                local_version: Some("v1".into()),
                remote_version: Some("v2".into()),
            }]
        );
    }

    #[test]
    fn matching_version_deletions_propagate() {
        let plan = plan_memory_sync(MemorySyncInput {
            mode: MemorySyncMode::Synced,
            local: vec![
                deleted("remove-remote", Some("v1")),
                item("remove-local", Some("v2"), "x"),
            ],
            remote: RemoteMemoryState::Ready(vec![
                item("remove-remote", Some("v1"), "x"),
                deleted("remove-local", Some("v2")),
            ]),
        })
        .expect("delete propagation plan");

        assert_eq!(
            plan.operations,
            vec![
                MemorySyncOperation::DeleteLocal {
                    key: "remove-local".into()
                },
                MemorySyncOperation::DeleteRemote {
                    key: "remove-remote".into(),
                    version: Some("v1".into()),
                }
            ]
        );
        assert!(plan.conflicts.is_empty());
    }

    #[test]
    fn unavailable_backend_returns_typed_error() {
        let error = plan_memory_sync(MemorySyncInput {
            mode: MemorySyncMode::Synced,
            local: vec![item("alpha", None, "A")],
            remote: RemoteMemoryState::Unavailable("timeout".into()),
        })
        .expect_err("synced mode needs a remote backend");

        assert_eq!(
            error,
            MemorySyncError::BackendUnavailable {
                reason: "timeout".into()
            }
        );
    }

    #[test]
    fn sync_codecs_preserve_declaration_order_nulls_and_non_scalar_utf16() {
        let key = RuntimeString::from_utf16([b'k' as u16, 0xd800]);
        let item = MemorySyncItem {
            key: key.clone(),
            version: None,
            body: Some(RuntimeString::from_utf16([0xdc00])),
            deleted: false,
        };
        let operation = MemorySyncOperation::DeleteRemote {
            key: key.clone(),
            version: None,
        };

        let item_text = runtime_codec::encode_text(&item).expect("encode exact sync item");
        assert_eq!(
            item_text,
            RuntimeString::from(
                r#"{"key":"k\ud800","version":null,"body":"\udc00","deleted":false}"#,
            ),
        );
        assert_eq!(
            runtime_codec::decode_text::<MemorySyncItem>(&item_text).expect("decode sync item"),
            item,
        );

        let operation_text =
            runtime_codec::encode_text(&operation).expect("encode exact sync operation");
        assert_eq!(
            operation_text,
            RuntimeString::from(r#"{"type":"deleteRemote","key":"k\ud800","version":null}"#),
        );
        assert_eq!(
            runtime_codec::decode_text::<MemorySyncOperation>(&operation_text)
                .expect("decode sync operation"),
            operation,
        );
    }
}
