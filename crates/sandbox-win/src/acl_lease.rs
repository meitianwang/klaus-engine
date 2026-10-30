//! Cross-process ownership of filesystem grants for sandbox executions.
//!
//! The ledger records complete policies and object baselines, rather than one
//! execution's before image of another execution's temporary permissions. The
//! short file transaction covers the intent record, native writes, readback and
//! durable completion. A failed transaction retains its actual store and object
//! handles. No Drop implementation restores permissions or infers target exit.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use windows_sys::Win32::Foundation::{LocalFree, PSID};
use windows_sys::Win32::Security::Authorization::ConvertSidToStringSidW;
use windows_sys::Win32::Security::{GetLengthSid, IsValidSid};

use crate::acl::Rule;
use crate::acl_object::{Dacl, DaclSnapshot, Object, ObjectId, Propagation};
use crate::acl_projection::{project, projection_paths, ExecutionPolicy, ObjectProjection};
use crate::acl_store::Store;
use crate::capability::CapabilitySid;
use crate::identity::AppContainerSid;
use crate::AbsPath;

const SCHEMA_VERSION: u32 = 1;
const MAX_ATTACHMENTS: usize = 4096;
const MAX_OBJECTS: usize = 16_384;
const INHERITED: u8 = 0x10;
const INHERIT_ONLY: u8 = 0x08;
const CHILDREN_INHERIT: u8 = 0x03;
const DACL_PROTECTED: u16 = 0x1000;

type Result<T> = std::result::Result<T, String>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Attachment {
    id: u64,
    package_sid: String,
    capability_sids: Vec<String>,
    rules: Vec<Rule>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ObjectRecord {
    path: AbsPath,
    identity: ObjectId,
    baseline: DaclSnapshot,
    confirmed: DaclSnapshot,
    tree: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Committed {
    next_attachment: u64,
    attachments: Vec<Attachment>,
    objects: Vec<ObjectRecord>,
}

impl Default for Committed {
    fn default() -> Self {
        Self {
            next_attachment: 1,
            attachments: Vec::new(),
            objects: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PlannedWrite {
    path: AbsPath,
    identity: ObjectId,
    before: DaclSnapshot,
    after: DaclSnapshot,
    tree: bool,
    // A rollback also owns the forward transaction's declared intermediate.
    #[serde(default)]
    before_intermediate: Option<DaclSnapshot>,
    #[serde(default = "unknown_write_started")]
    started: bool,
    // Exact object state alone cannot prove a Tree setter succeeded.
    #[serde(default)]
    propagated: bool,
    confirmed: bool,
}

fn unknown_write_started() -> bool {
    // Older recovery records have no receipt proving that no setter ran.
    true
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Transition {
    attachment: u64,
    acquiring: bool,
    after: Committed,
    writes: Vec<PlannedWrite>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Ledger {
    schema_version: u32,
    committed: Committed,
    pending: Option<Transition>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    native_error: Option<serde_json::Value>,
}

impl Default for Ledger {
    fn default() -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            committed: Committed::default(),
            pending: None,
            native_error: None,
        }
    }
}

impl Ledger {
    fn decode(bytes: Option<Vec<u8>>) -> Result<Self> {
        let ledger: Self = match bytes {
            None => Self::default(),
            Some(bytes) => serde_json::from_slice(&bytes)
                .map_err(|error| format!("invalid ACL ownership ledger: {error}"))?,
        };
        if ledger.schema_version != SCHEMA_VERSION
            || ledger.committed.next_attachment == 0
            || ledger.committed.attachments.len() > MAX_ATTACHMENTS
            || ledger.committed.objects.len() > MAX_OBJECTS
        {
            return Err("unsupported or oversized ACL ownership ledger".into());
        }
        let mut ids = BTreeSet::new();
        for attachment in &ledger.committed.attachments {
            if attachment.id == 0
                || attachment.id >= ledger.committed.next_attachment
                || !ids.insert(attachment.id)
            {
                return Err("ambiguous ACL attachment identity in the ownership ledger".into());
            }
        }
        let mut paths = std::collections::HashSet::new();
        let mut objects = std::collections::HashSet::new();
        for object in &ledger.committed.objects {
            if !paths.insert(object.path.clone()) || !objects.insert(object.identity) {
                return Err("ambiguous filesystem object in the ACL ownership ledger".into());
            }
        }
        Ok(ledger)
    }
}

/// Clones refer to the same attachment, including after a failed setup.
#[derive(Clone)]
pub(crate) struct Lease(Arc<Mutex<Owner>>);

impl std::fmt::Debug for Lease {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_tuple("AclLease")
            .field(&self.recovery_record())
            .finish()
    }
}

impl PartialEq for Lease {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for Lease {}

#[derive(Default)]
struct Owner {
    attachment: Option<u64>,
    package_sid: String,
    capability_sids: Vec<String>,
    active: bool,
    released: bool,
    store: Option<Store>,
    // Present only while this owner has an unfinished native transaction.
    pending: Option<Ledger>,
    native_error: Option<serde_json::Value>,
    // Pin every object used by this transaction. On successful acquisition the
    // pins remain with the execution until its confirmed release.
    objects: Vec<Object>,
}

impl Lease {
    pub(crate) fn new(sid: &AppContainerSid, capabilities: &[CapabilitySid]) -> Result<Self> {
        let owner = Owner {
            package_sid: sid_text(sid.as_psid())?,
            capability_sids: capabilities
                .iter()
                .map(|sid| sid_text(sid.as_psid()))
                .collect::<Result<_>>()?,
            ..Owner::default()
        };
        Ok(Self(Arc::new(Mutex::new(owner))))
    }

    pub(crate) fn acquire(&self, rules: &[Rule]) -> Result<()> {
        let mut owner = self.0.lock().map_err(|_| "ACL lease owner was poisoned")?;
        if owner.attachment.is_some() || owner.active || owner.released {
            return Err("an ACL attachment cannot be acquired twice".into());
        }
        owner.open_store()?;
        let result = owner.prepare_acquire(rules);
        if result.is_err() && owner.pending.is_none() {
            // No native intent exists. Still finish the real store explicitly;
            // a finish failure stays on the owner for the caller's retry.
            let finish = owner.finish_store();
            return combine(result, finish);
        }
        result
    }

    pub(crate) fn release(&self) -> Result<()> {
        let mut owner = self.0.lock().map_err(|_| "ACL lease owner was poisoned")?;
        if owner.released {
            return owner.finish_store();
        }
        if owner.pending.is_some() {
            owner.resolve_pending()?;
        }
        owner.finish_store()?;
        if !owner.active {
            owner.released = true;
            owner.objects.clear();
            return Ok(());
        }
        owner.open_store()?;
        let result = owner.prepare_release();
        if result.is_err() && owner.pending.is_none() {
            let finish = owner.finish_store();
            return combine(result, finish);
        }
        result
    }

    pub(crate) fn recovery_record(&self) -> serde_json::Value {
        match self.0.lock() {
            Ok(owner) => serde_json::json!({
                "attachment": owner.attachment,
                "packageSid": owner.package_sid,
                "active": owner.active,
                "released": owner.released,
                "pending": owner.pending,
                "nativeError": owner.native_error,
                "store": owner.store.as_ref().map(Store::recovery_record),
                "objects": owner.objects.iter().map(Object::path).collect::<Vec<_>>(),
            }),
            Err(_) => serde_json::json!({"error":"ACL lease owner was poisoned"}),
        }
    }
}

fn combine(operation: Result<()>, finish: Result<()>) -> Result<()> {
    match (operation, finish) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
        (Err(operation), Err(finish)) => Err(format!("{operation}; ACL store finish: {finish}")),
    }
}

impl Owner {
    fn open_store(&mut self) -> Result<()> {
        if self.store.is_some() {
            return Err(
                "the previous ACL store transaction still needs explicit completion".into(),
            );
        }
        match Store::acquire() {
            Ok(store) => {
                self.store = Some(store);
                Ok(())
            }
            Err(mut error) => {
                self.store = error.take_recovery();
                Err(error.to_string())
            }
        }
    }

    fn finish_store(&mut self) -> Result<()> {
        if self.pending.is_some() {
            return Err("an unfinished ACL transaction still owns its store lock".into());
        }
        if let Some(store) = &mut self.store {
            store.finish().map_err(|error| error.to_string())?;
        }
        self.store = None;
        Ok(())
    }

    fn read_ledger(&mut self) -> Result<Ledger> {
        let bytes = self
            .store
            .as_mut()
            .ok_or("ACL store is not owned")?
            .read()
            .map_err(|error| error.to_string())?;
        let ledger = Ledger::decode(bytes)?;
        if let Some(pending) = &ledger.pending {
            return Err(format!(
                "ACL attachment {} has unfinished recovery; no new permission write was made",
                pending.attachment
            ));
        }
        Ok(ledger)
    }

    fn persist(&mut self, ledger: &Ledger) -> Result<()> {
        let bytes = serde_json::to_vec(ledger).map_err(|error| error.to_string())?;
        let store = self.store.as_mut().ok_or("ACL store is not owned")?;
        store
            .retry_pending_write()
            .map_err(|error| error.to_string())?;
        store.write(&bytes).map_err(|error| error.to_string())
    }

    fn prepare_acquire(&mut self, rules: &[Rule]) -> Result<()> {
        let ledger = self.read_ledger()?;
        if ledger.committed.attachments.len() == MAX_ATTACHMENTS {
            return Err("the ACL ownership ledger has reached its attachment limit".into());
        }
        let id = ledger.committed.next_attachment;
        let mut after = ledger.committed.clone();
        after.next_attachment = id.checked_add(1).ok_or("ACL attachment ID exhausted")?;
        after.attachments.push(Attachment {
            id,
            package_sid: self.package_sid.clone(),
            capability_sids: self.capability_sids.clone(),
            rules: rules.to_vec(),
        });
        let transition = self.plan(&ledger.committed, after, id, true)?;
        self.attachment = Some(id);
        let mut intent = ledger;
        intent.pending = Some(transition);
        // Retain the exact intent before a fallible durable write or ACL setter.
        self.pending = Some(intent.clone());
        self.persist(&intent)?;
        self.apply_pending_forward()?;
        self.active = true;
        self.finish_store()
    }

    fn prepare_release(&mut self) -> Result<()> {
        let ledger = self.read_ledger()?;
        let id = self
            .attachment
            .ok_or("active ACL lease has no attachment identity")?;
        let existing = ledger
            .committed
            .attachments
            .iter()
            .find(|entry| entry.id == id)
            .ok_or("active ACL attachment is missing from its ledger")?;
        if existing.package_sid != self.package_sid {
            return Err("ACL attachment identity belongs to another execution".into());
        }
        let mut after = ledger.committed.clone();
        after.attachments.retain(|entry| entry.id != id);
        let transition = self.plan(&ledger.committed, after, id, false)?;
        let mut intent = ledger;
        intent.pending = Some(transition);
        self.pending = Some(intent.clone());
        self.persist(&intent)?;
        self.apply_pending_forward()?;
        self.active = false;
        self.released = true;
        self.objects.clear();
        self.finish_store()
    }

    fn resolve_pending(&mut self) -> Result<()> {
        let ledger = self
            .pending
            .as_ref()
            .ok_or("ACL recovery intent is absent")?;
        let transition = ledger
            .pending
            .as_ref()
            .ok_or("ACL recovery transition is absent")?;
        if transition.acquiring {
            // The failed acquire never launched this target. Restore the complete
            // previous execution set, which can still include live peers.
            let mut rollback = ledger.clone();
            let mut restored = ledger.committed.clone();
            restored.next_attachment = restored
                .next_attachment
                .max(transition.after.next_attachment);
            let mut writes = Vec::new();
            for write in &transition.writes {
                if !write.started {
                    let object = self
                        .objects
                        .iter()
                        .find(|object| object.identity() == write.identity)
                        .ok_or("the original ACL object handle is no longer owned")?;
                    let current = object.snapshot().map_err(|error| error.to_string())?;
                    if !same_image(&current, &write.before) {
                        return Err(format!(
                            "unwritten ACL object changed at {}; recovery retained",
                            write.path.as_str()
                        ));
                    }
                    continue;
                }
                writes.push(PlannedWrite {
                    path: write.path.clone(),
                    identity: write.identity,
                    before: write.after.clone(),
                    after: write.before.clone(),
                    tree: write.tree,
                    before_intermediate: write.tree.then(|| write.after.propagation_image()),
                    started: false,
                    propagated: false,
                    confirmed: false,
                });
            }
            order_writes(&mut writes);
            rollback.pending = Some(Transition {
                attachment: transition.attachment,
                acquiring: false,
                after: restored,
                writes,
            });
            self.pending = Some(rollback.clone());
            self.persist(&rollback)?;
            self.apply_pending_forward()?;
            self.active = false;
            self.released = true;
            self.objects.clear();
            Ok(())
        } else {
            self.apply_pending_forward()?;
            self.active = false;
            self.released = true;
            self.objects.clear();
            Ok(())
        }
    }

    fn apply_pending_forward(&mut self) -> Result<()> {
        let mut ledger = self.pending.clone().ok_or("ACL write intent is absent")?;
        // A failed durable write can be retried only by the retained store owner.
        self.persist(&ledger)?;
        let count = ledger
            .pending
            .as_ref()
            .ok_or("ACL transition is absent")?
            .writes
            .len();
        for index in 0..count {
            let write = ledger
                .pending
                .as_ref()
                .ok_or("ACL transition is absent")?
                .writes[index]
                .clone();
            let object_index = self
                .objects
                .iter()
                .position(|object| object.identity() == write.identity)
                .ok_or("the original ACL object handle is no longer owned")?;
            let mut current = self.objects[object_index]
                .snapshot()
                .map_err(|error| error.to_string())?;
            if write.confirmed {
                if !same_image(&current, &write.after) {
                    return Err(format!(
                        "ACL outside change at {}; recovery retained",
                        write.path.as_str()
                    ));
                }
                continue;
            }
            let intermediate = write.after.propagation_image();
            let planned_source = same_image(&current, &write.before)
                || same_image(&current, &write.after)
                || write
                    .before_intermediate
                    .as_ref()
                    .is_some_and(|image| same_image(&current, image))
                || (write.tree && write.started && same_image(&current, &intermediate));
            if !planned_source {
                return Err(format!(
                    "ACL outside change at {}; recovery retained",
                    write.path.as_str()
                ));
            }
            // Persist a receipt before entering any setter. A successful object
            // read-back does not stand in for a successful propagation call.
            if !write.started {
                ledger
                    .pending
                    .as_mut()
                    .ok_or("ACL transition is absent")?
                    .writes[index]
                    .started = true;
                self.pending = Some(ledger.clone());
                self.persist(&ledger)?;
            }
            if write.tree && !write.propagated {
                current = self.write_object(
                    &mut ledger,
                    object_index,
                    &current,
                    &intermediate,
                    Propagation::Tree,
                )?;
                ledger
                    .pending
                    .as_mut()
                    .ok_or("ACL transition is absent")?
                    .writes[index]
                    .propagated = true;
                self.pending = Some(ledger.clone());
                self.persist(&ledger)?;
            } else if write.tree
                && !same_image(&current, &intermediate)
                && !same_image(&current, &write.after)
            {
                return Err(format!(
                    "ACL propagation receipt no longer matches {}; recovery retained",
                    write.path.as_str()
                ));
            }
            if !same_image(&current, &write.after) {
                self.write_object(
                    &mut ledger,
                    object_index,
                    &current,
                    &write.after,
                    Propagation::Local,
                )?;
            }
            ledger
                .pending
                .as_mut()
                .ok_or("ACL transition is absent")?
                .writes[index]
                .confirmed = true;
            self.pending = Some(ledger.clone());
            self.persist(&ledger)?;
        }
        let transition = ledger.pending.take().ok_or("ACL transition is absent")?;
        ledger.committed = transition.after;
        ledger.native_error = None;
        // Keep a recoverable in-memory intent until the final stable image is
        // durably confirmed. A failure here must not lose the lock or handles.
        self.persist(&ledger)?;
        self.pending = None;
        Ok(())
    }

    fn write_object(
        &mut self,
        ledger: &mut Ledger,
        object_index: usize,
        before: &DaclSnapshot,
        after: &DaclSnapshot,
        propagation: Propagation,
    ) -> Result<DaclSnapshot> {
        match self.objects[object_index].write(before, after, propagation) {
            Ok(observed) => Ok(observed),
            Err(error) => {
                let detail = format!("{error}; phase_write_attempted={}", error.write_attempted());
                let record = serde_json::json!({
                    "phase": format!("{propagation:?}"),
                    "phaseWriteAttempted": error.write_attempted(), "error": error,
                });
                self.native_error = Some(record.clone());
                ledger.native_error = Some(record);
                self.pending = Some(ledger.clone());
                // Keep actual error/readback evidence with the durable intent.
                // A failed record write retains this same store owner for retry.
                match self.persist(ledger) {
                    Ok(()) => Err(detail),
                    Err(record_error) => Err(format!(
                        "{detail}; recording native failure: {record_error}"
                    )),
                }
            }
        }
    }

    fn plan(
        &mut self,
        before: &Committed,
        mut after: Committed,
        id: u64,
        acquiring: bool,
    ) -> Result<Transition> {
        let old_policies = policies(&before.attachments);
        let new_policies = policies(&after.attachments);
        let mut paths = projection_paths(&new_policies).map_err(|error| error.to_string())?;
        for record in &before.objects {
            if !paths.contains(&record.path) {
                paths.push(record.path.clone());
            }
        }
        if paths.len() > MAX_OBJECTS {
            return Err("the ACL ownership ledger has reached its object limit".into());
        }
        // All baselines and original handles are acquired before the first write.
        let mut objects = Vec::new();
        let mut writes = Vec::new();
        after.objects.clear();
        for path in paths {
            let text = path.as_str();
            let Some(object) = Object::open(&text).map_err(|error| error.to_string())? else {
                if before.objects.iter().any(|record| record.path == path) {
                    return Err(format!("owned ACL path disappeared: {text}"));
                }
                continue;
            };
            // Multiple paths naming one object need one unambiguous policy.
            // Reject aliases rather than accidentally applying path A's policy
            // through path B's handle. Neither alias is written on this error.
            if objects
                .iter()
                .any(|old: &Object| old.identity() == object.identity())
            {
                return Err(format!(
                    "multiple ACL policy paths name the same filesystem object: {text}"
                ));
            }
            let current = object.snapshot().map_err(|error| error.to_string())?;
            let previous = before.objects.iter().find(|record| record.path == path);
            let baseline = if let Some(record) = previous {
                if record.identity != object.identity() || !same_image(&current, &record.confirmed)
                {
                    return Err(format!(
                        "ACL object identity or external permissions changed at {text}"
                    ));
                }
                record.baseline.clone()
            } else {
                remove_inherited_contributions(
                    &current,
                    &path,
                    object.is_directory(),
                    before,
                    &old_policies,
                )?
            };
            let projection = project(&new_policies, &path).map_err(|error| error.to_string())?;
            let covered = covered(&after.attachments, &path);
            let tree = previous.is_some_and(|record| record.tree)
                || after
                    .attachments
                    .iter()
                    .flat_map(|entry| &entry.rules)
                    .any(|rule| {
                        rule.path() == &path
                            && !matches!(
                                rule,
                                Rule::Allow {
                                    inheritable: false,
                                    ..
                                }
                            )
                    })
                || projection.protection_required;
            let desired = if covered {
                compose(&baseline, &projection, &after.attachments, tree)?
            } else {
                baseline.clone()
            };
            if !same_image(&current, &desired) {
                writes.push(PlannedWrite {
                    path: path.clone(),
                    identity: object.identity(),
                    before: current,
                    after: desired.clone(),
                    tree,
                    before_intermediate: None,
                    started: false,
                    propagated: false,
                    confirmed: false,
                });
            }
            if covered {
                after.objects.push(ObjectRecord {
                    path,
                    identity: object.identity(),
                    baseline,
                    confirmed: desired,
                    tree,
                });
            }
            objects.push(object);
        }
        order_writes(&mut writes);
        self.objects = objects;
        Ok(Transition {
            attachment: id,
            acquiring,
            after,
            writes,
        })
    }
}

fn policies(attachments: &[Attachment]) -> Vec<ExecutionPolicy> {
    let mut grouped: BTreeMap<&str, Vec<Rule>> = BTreeMap::new();
    for attachment in attachments {
        grouped
            .entry(&attachment.package_sid)
            .or_default()
            .extend(attachment.rules.clone());
    }
    grouped
        .into_iter()
        .map(|(sid, rules)| ExecutionPolicy {
            execution_id: sid.to_owned(),
            package_sid: sid.to_owned(),
            rules,
        })
        .collect()
}

fn covered(attachments: &[Attachment], path: &AbsPath) -> bool {
    attachments
        .iter()
        .flat_map(|entry| &entry.rules)
        .any(|rule| match rule {
            Rule::Allow {
                path: root,
                inheritable,
                ..
            } => path == root || (*inheritable && path.is_within(root)),
            Rule::Withhold { path: root, .. } => {
                path.is_within(root)
                    || root
                        .relative_components(path)
                        .is_some_and(|parts| parts.len() == 1)
            }
        })
}

fn order_writes(writes: &mut [PlannedWrite]) {
    // Protect descendants before changing ancestors. Restore ordinary ancestor
    // inheritance before unprotecting the last covered child.
    writes.sort_by_key(|write| {
        let depth = write.path.ancestors().len();
        if write.after.protected {
            (0, usize::MAX - depth, write.path.as_str())
        } else {
            (1, depth, write.path.as_str())
        }
    });
}

fn same_image(left: &DaclSnapshot, right: &DaclSnapshot) -> bool {
    left == right
}

fn sid_text(sid: PSID) -> Result<String> {
    if sid.is_null() || unsafe { IsValidSid(sid) } == 0 {
        return Err("invalid ACL package or capability SID".into());
    }
    let mut pointer = std::ptr::null_mut();
    let converted = unsafe { ConvertSidToStringSidW(sid, &mut pointer) };
    if converted == 0 || pointer.is_null() {
        if !pointer.is_null() {
            unsafe { LocalFree(pointer.cast()) };
        }
        return Err("converting ACL package SID failed".into());
    }
    let result = (0..256)
        .find(|index| unsafe { *pointer.add(*index) == 0 })
        .ok_or_else(|| "unterminated ACL SID string".to_string())
        .and_then(|length| {
            String::from_utf16(unsafe { std::slice::from_raw_parts(pointer, length) })
                .map_err(|error| error.to_string())
        });
    unsafe { LocalFree(pointer.cast()) };
    result
}

fn sid_bytes(text: &str) -> Result<Vec<u8>> {
    use windows_sys::Win32::Security::Authorization::ConvertStringSidToSidW;
    if text.len() > 256 || text.contains('\0') {
        return Err("invalid ACL SID text".into());
    }
    let wide: Vec<_> = text.encode_utf16().chain(Some(0)).collect();
    let mut pointer = std::ptr::null_mut();
    let converted = unsafe { ConvertStringSidToSidW(wide.as_ptr(), &mut pointer) };
    if converted == 0 || pointer.is_null() {
        if !pointer.is_null() {
            unsafe { LocalFree(pointer) };
        }
        return Err("parsing ACL package SID failed".into());
    }
    let result = sid_text(pointer).and_then(|canonical| {
        if canonical != text {
            return Err("noncanonical ACL package SID".into());
        }
        let length = unsafe { GetLengthSid(pointer) } as usize;
        Ok(unsafe { std::slice::from_raw_parts(pointer.cast::<u8>(), length) }.to_vec())
    });
    unsafe { LocalFree(pointer) };
    result
}

fn allow_ace(sid: &[u8], mask: u32, flags: u8) -> Result<Vec<u8>> {
    let size = u16::try_from(8 + sid.len()).map_err(|_| "ACL ACE is too large")?;
    let mut ace = Vec::with_capacity(usize::from(size));
    ace.extend([0, flags]);
    ace.extend(size.to_le_bytes());
    ace.extend(mask.to_le_bytes());
    ace.extend(sid);
    Ok(ace)
}

fn grant_aces(projection: &ObjectProjection) -> Result<Vec<Vec<u8>>> {
    let mut aces = Vec::new();
    for grant in &projection.grants {
        let sid = sid_bytes(&grant.package_sid)?;
        if grant.local_mask == grant.inherited_mask {
            if grant.local_mask != 0 {
                aces.push(allow_ace(&sid, grant.local_mask, CHILDREN_INHERIT)?);
            }
        } else {
            if grant.local_mask != 0 {
                aces.push(allow_ace(&sid, grant.local_mask, 0)?);
            }
            if grant.inherited_mask != 0 {
                aces.push(allow_ace(
                    &sid,
                    grant.inherited_mask,
                    CHILDREN_INHERIT | INHERIT_ONLY,
                )?);
            }
        }
    }
    Ok(aces)
}

fn compose(
    baseline: &DaclSnapshot,
    projection: &ObjectProjection,
    attachments: &[Attachment],
    protect: bool,
) -> Result<DaclSnapshot> {
    let Dacl::Present { revision, aces } = &baseline.dacl else {
        return Err(
            "an absent or null DACL cannot be narrowed without changing unrelated access".into(),
        );
    };
    check_ambient_grants(aces, projection, attachments)?;
    let mut result = baseline.clone();
    result.protected |= protect;
    if result.protected {
        result.control |= DACL_PROTECTED;
    }
    let mut entries = aces.clone();
    if result.protected {
        for ace in &mut entries {
            ace[1] &= !INHERITED;
        }
    }
    let new_entries = grant_aces(projection)?;
    // Existing entries keep their relative order. New allows precede inherited
    // entries and follow explicit denials; unknown ACE payloads remain intact.
    let insertion = entries
        .iter()
        .position(|ace| ace[1] & INHERITED != 0)
        .unwrap_or(entries.len());
    entries.splice(insertion..insertion, new_entries);
    result.dacl = Dacl::Present {
        revision: *revision,
        aces: entries,
    };
    Ok(result)
}

fn check_ambient_grants(
    aces: &[Vec<u8>],
    projection: &ObjectProjection,
    attachments: &[Attachment],
) -> Result<()> {
    for grant in &projection.grants {
        let mut relevant = vec![
            sid_bytes(&grant.package_sid)?,
            sid_bytes("S-1-15-2-1")?,
            sid_bytes("S-1-15-2-2")?,
        ];
        for attachment in attachments
            .iter()
            .filter(|entry| entry.package_sid == grant.package_sid)
        {
            for capability in &attachment.capability_sids {
                relevant.push(sid_bytes(capability)?);
            }
        }
        for ace in aces {
            if ace.len() < 4 {
                return Err("malformed baseline ACE".into());
            }
            if matches!(ace[0], 5 | 9 | 11) {
                return Err(
                    "a conditional or object allow ACE prevents proving this ACL boundary".into(),
                );
            }
            if ace[0] != 0 || ace.len() < 16 || !relevant.iter().any(|sid| ace[8..] == *sid) {
                continue;
            }
            let mask = u32::from_le_bytes(ace[4..8].try_into().map_err(|_| "invalid ACE mask")?);
            let local = if ace[1] & INHERIT_ONLY == 0 { mask } else { 0 };
            let inherited = if ace[1] & CHILDREN_INHERIT != 0 {
                mask
            } else {
                0
            };
            if local & !grant.local_mask != 0 || inherited & !grant.inherited_mask != 0 {
                return Err(
                    "an unrelated existing package grant exceeds the requested ACL boundary".into(),
                );
            }
        }
    }
    Ok(())
}

fn remove_inherited_contributions(
    current: &DaclSnapshot,
    path: &AbsPath,
    directory: bool,
    before: &Committed,
    policies: &[ExecutionPolicy],
) -> Result<DaclSnapshot> {
    let mut result = current.clone();
    let Dacl::Present { aces, .. } = &mut result.dacl else {
        return Ok(result);
    };
    let source = before
        .objects
        .iter()
        .filter(|record| record.tree && record.path != *path && path.is_within(&record.path))
        .max_by_key(|record| record.path.ancestors().len());
    for policy in policies {
        let sid = sid_bytes(&policy.package_sid)?;
        let mut retained = Vec::with_capacity(aces.len());
        for ace in aces.iter() {
            if ace.len() < 16 || ace[0] != 0 || ace[8..] != sid {
                retained.push(ace.clone());
                continue;
            }
            let source = source.ok_or_else(|| {
                format!(
                    "package ACE has no registered inheritance source at {}",
                    path.as_str()
                )
            })?;
            let Dacl::Present {
                aces: baseline_aces,
                ..
            } = &source.baseline.dacl
            else {
                return Err("inheritance source has no concrete original DACL".into());
            };
            if baseline_aces
                .iter()
                .any(|entry| entry.len() >= 16 && matches!(entry[0], 0 | 1) && entry[8..] == sid)
            {
                return Err(
                    "an original package ACE cannot be adopted as a temporary grant".into(),
                );
            }
            let Dacl::Present {
                aces: confirmed_aces,
                ..
            } = &source.confirmed.dacl
            else {
                return Err("inheritance source has no confirmed concrete DACL".into());
            };
            let projection = project(policies, &source.path).map_err(|error| error.to_string())?;
            let expected = grant_aces(&projection)?
                .into_iter()
                .filter(|entry| {
                    entry[8..] == sid
                        && entry[1] & CHILDREN_INHERIT == CHILDREN_INHERIT
                        && confirmed_aces.contains(entry)
                })
                .map(|mut entry| {
                    entry[1] = if directory {
                        CHILDREN_INHERIT | INHERITED
                    } else {
                        INHERITED
                    };
                    entry
                })
                .collect::<Vec<_>>();
            if !expected.contains(ace) {
                return Err(format!(
                    "unowned package ACE encountered at {}",
                    path.as_str()
                ));
            }
            // The exact SID, source object, source before/after entries, mask,
            // and descendant ACE flags all agree. No foreign ACE is filtered.
        }
        *aces = retained;
    }
    Ok(result)
}
