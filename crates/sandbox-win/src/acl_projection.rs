//! Pure permission projection for overlapping Windows sandbox executions.
//!
//! This module computes only managed package grants. It does not read ACLs,
//! identify filesystem objects, persist ownership, or authorize native writes.
//! A native adapter must preserve unrelated ACEs and validate object identity,
//! inheritance, and outside changes before applying a projection.

use std::collections::HashSet;

use crate::acl::{access_mask, Grant, Rule};
use crate::AbsPath;

const FILE_DELETE_CHILD: u32 = 0x0040;

/// The complete policy of one execution participating in an ACL transaction.
///
/// Callers must combine all lease attachments for the same execution before
/// projecting. Repeated execution IDs or package SIDs are errors, even when
/// their rules differ. `package_sid` is the canonical string supplied by the
/// identity adapter; this pure module does not parse or create native SIDs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionPolicy {
    pub execution_id: String,
    pub package_sid: String,
    pub rules: Vec<Rule>,
}

/// One execution's managed access at an object and through its inheritance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionGrant {
    pub execution_id: String,
    pub package_sid: String,
    /// Access effective on this object, including applicable ancestor grants.
    pub local_mask: u32,
    /// Access to pass to ordinary descendants, subject to their own cutouts.
    ///
    /// When different from `local_mask`, a native adapter can represent this
    /// with a separate inherit-only ACE. This is not extra access on the object.
    pub inherited_mask: u32,
}

/// All managed package grants for one object, sorted by execution ID.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectProjection {
    /// Includes executions with zero masks so their absence of access is explicit.
    pub grants: Vec<ExecutionGrant>,
    /// This exact object is a restrictive cutout or needs a local delete-child
    /// reduction. An inherited ACE must not add the removed access back.
    /// Descendants of a cutout remain capped without requiring protection at
    /// every level; the native adapter must preserve the cutout's inheritance.
    pub protection_required: bool,
}

/// Ambiguous identity ownership in an input that must be grouped by execution.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ProjectionError {
    #[error(
        "execution {execution_id} appears more than once; combine its lease attachments first"
    )]
    DuplicateExecutionId { execution_id: String },
    #[error("package SID {package_sid} belongs to more than one execution policy")]
    DuplicatePackageSid { package_sid: String },
}

/// Project complete active policies onto one lexical absolute path.
///
/// Exact allows grant local access; only inheritable allows reach descendants.
/// Every applicable withhold intersects the allowed masks, independently of rule
/// order or more specific allows. A retained read grant is only a ceiling: it
/// cannot create access that no allow supplied. Read-write withholds do not
/// narrow any grant supported by this module.
///
/// A direct restrictive child removes only this execution's local delete-child
/// bit from the parent. Ordinary descendants keep their inherited write/delete
/// access, and other executions keep their own masks.
pub fn project(
    policies: &[ExecutionPolicy],
    object: &AbsPath,
) -> Result<ObjectProjection, ProjectionError> {
    validate_identities(policies)?;
    let mut protection_required = false;
    let mut grants = Vec::with_capacity(policies.len());

    for policy in policies {
        let mut local_mask = 0;
        let mut inherited_mask = 0;
        let mut ceiling = u32::MAX;
        let mut has_restrictive_child = false;

        for rule in &policy.rules {
            match rule {
                Rule::Allow {
                    path,
                    grant,
                    inheritable,
                } => {
                    let exact = object == path;
                    let inherited = *inheritable && object.is_within(path);
                    if exact || inherited {
                        local_mask |= access_mask(*grant);
                    }
                    if inherited {
                        inherited_mask |= access_mask(*grant);
                    }
                }
                Rule::Withhold { path, keep } => {
                    if object.is_within(path) {
                        ceiling &= keep.map_or(0, access_mask);
                    }
                    if *keep != Some(Grant::ReadWrite) {
                        protection_required |= object == path;
                        has_restrictive_child |= path
                            .relative_components(object)
                            .is_some_and(|components| components.len() == 1);
                    }
                }
            }
        }

        local_mask &= ceiling;
        inherited_mask &= ceiling;
        if has_restrictive_child && local_mask & FILE_DELETE_CHILD != 0 {
            local_mask &= !FILE_DELETE_CHILD;
            protection_required = true;
        }
        grants.push(ExecutionGrant {
            execution_id: policy.execution_id.clone(),
            package_sid: policy.package_sid.clone(),
            local_mask,
            inherited_mask,
        });
    }

    grants.sort_by(|left, right| left.execution_id.cmp(&right.execution_id));
    Ok(ObjectProjection {
        grants,
        protection_required,
    })
}

/// List rule paths and the direct parents of restrictive withholds.
///
/// The result is deduplicated using `AbsPath` equality and sorted by case-folded
/// display path. It does not invent walk grants or enumerate system ancestors.
/// These are projection candidates, not an ACL write order or a complete native
/// propagation footprint; an adapter must also consider previously owned objects
/// when an execution is removed.
pub fn projection_paths(policies: &[ExecutionPolicy]) -> Result<Vec<AbsPath>, ProjectionError> {
    validate_identities(policies)?;
    let mut seen = HashSet::new();
    let mut paths = Vec::new();
    for policy in policies {
        for rule in &policy.rules {
            if seen.insert(rule.path().clone()) {
                paths.push(rule.path().clone());
            }
            if let Rule::Withhold { path, keep } = rule {
                if *keep != Some(Grant::ReadWrite) {
                    if let Some(parent) = path.ancestors().pop() {
                        if seen.insert(parent.clone()) {
                            paths.push(parent);
                        }
                    }
                }
            }
        }
    }
    paths.sort_by_cached_key(|path| path.as_str().to_ascii_uppercase());
    Ok(paths)
}

fn validate_identities(policies: &[ExecutionPolicy]) -> Result<(), ProjectionError> {
    let mut executions = HashSet::with_capacity(policies.len());
    let mut packages = HashSet::with_capacity(policies.len());
    for policy in policies {
        if !executions.insert(policy.execution_id.as_str()) {
            return Err(ProjectionError::DuplicateExecutionId {
                execution_id: policy.execution_id.clone(),
            });
        }
        if !packages.insert(policy.package_sid.as_str()) {
            return Err(ProjectionError::DuplicatePackageSid {
                package_sid: policy.package_sid.clone(),
            });
        }
    }
    Ok(())
}
