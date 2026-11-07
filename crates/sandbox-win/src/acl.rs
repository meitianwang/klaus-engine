//! Filesystem boundaries, written as ACEs for the container SID.
//!
//! # Why grants and not denials
//!
//! An AppContainer starts with no access to anything. So the shape here is "add an
//! allow ACE on each permitted root" and never "add a deny ACE on everything
//! else": an enumeration of everything else cannot be completed, and a boundary
//! built from one is only as good as the list. Denials still appear, but only for
//! carve-outs *inside* a granted root (a `.ssh` under a readable home, a `.git`
//! under a writable workspace), where the set is small and known.
//!
//! # Deletion needs the parent, not the file
//!
//! `DELETE` on a file is not what lets a process remove it. Windows accepts either
//! of two things:
//!
//! - `DELETE` on the file itself, or
//! - `FILE_DELETE_CHILD` on the **containing directory**.
//!
//! and `del`, `rmdir`, `git`, `rm -rf` and every installer take the second path,
//! because that is what the CRT's `remove()` and `DeleteFileW` end up needing for
//! a file whose own DACL is inherited rather than explicit. A grant that hands out
//! `FILE_GENERIC_WRITE` on a directory therefore produces a sandbox where a
//! command can create and modify files but cannot delete them — the writes look
//! fine and the deletes fail, which is exactly the confusing half-broken state
//! that is hard to attribute.
//!
//! So a writable root here is granted `FILE_DELETE_CHILD` alongside the write
//! bits, and the grant is inheritable so files created later get it too.
//!
//! # A deny ACE naming the container does nothing
//!
//! This was the surprise, and it was measured on a real machine rather than reasoned
//! about. An AppContainer's package SID is **not** in its token's group list — it
//! lives in `TokenAppContainerSid`, and the kernel consults it in a second sweep over
//! the DACL that only accumulates *allows*. So a `DENY` ACE naming the package SID,
//! even placed first in the DACL, is never consulted: a file inside a granted root
//! with an explicit deny at index 0 was read successfully by the container.
//!
//! A denial therefore cannot be written as a deny ACE. What it *can* be written as is
//! the absence of an allow: the same sweep means the container reaches nothing that
//! does not name it. [`Rule::Withhold`] therefore caps that execution's allows
//! instead. A shared, object-bound ledger projects every active execution at a
//! protected path, including a peer explicitly allowed to write there. Unrelated
//! ACEs are preserved; an ambient grant that prevents proving the requested
//! boundary produces an error before a write. Files created below the cutout
//! inherit its restricted grants.
//!
//! # A container must be able to walk to its roots
//!
//! Also measured: a container granted `C:\...\Temp\ws` could open and delete files
//! inside it through the raw API, while every `cmd.exe` built-in failed with "access
//! denied" — including `dir C:\Windows\System32`, which this crate never touches. The
//! cause is that the ancestors of the root deny the container, and `cmd` canonicalizes
//! an absolute path by descending from the drive root. [`Rule::Walk`] grants list and
//! traverse on each ancestor, **not inheritable**, so the container can walk the path
//! without gaining anything on the ancestors' other contents.

use crate::boundary::{AbsPath, FilesystemBoundary};

/// What a single ACE should permit, in terms this crate reasons about rather than
/// in raw access-mask bits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Grant {
    /// List a directory's names and traverse it. Enough to walk a path, not enough
    /// to read what the directory holds.
    Walk,
    /// Read and traverse. Enough to open files and list directories.
    Read,
    /// Read plus create, modify, and delete children.
    ReadWrite,
}

/// One change to make to one path's DACL.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Rule {
    /// Name the container on `path` with `grant`.
    Allow {
        path: AbsPath,
        grant: Grant,
        /// Whether the ACE propagates to the path's children. A granted root wants
        /// this, so files created later are covered; an ancestor granted only for
        /// walking must not have it, or the grant reaches everything below.
        inheritable: bool,
    },
    /// Take the container off `path`, whatever the roots above it say.
    ///
    /// Caps this execution's projected grants while retaining other executions'
    /// permissions. `keep` is a ceiling on its allows: `None` for no access,
    /// `Some(Grant::Read)` for access that must remain read-only.
    Withhold { path: AbsPath, keep: Option<Grant> },
}

impl Rule {
    /// The path this rule applies to.
    pub fn path(&self) -> &AbsPath {
        match self {
            Rule::Allow { path, .. } | Rule::Withhold { path, .. } => path,
        }
    }
}

/// Turn a boundary into the ordered list of changes that implement it.
///
/// The returned order is stable for diagnostics. The native coordinator computes
/// the whole policy before writing: it protects cutouts before changing their
/// ancestors and restores inheritance only after their last covering lease ends.
///
/// Read grants come from `readable`; when that is `None` the process keeps the
/// ambient read access an AppContainer already has (which is close to nothing) and
/// only the withholds are applied — a policy that confines writes but not reads does
/// not want this code inventing read roots for it.
pub fn plan_entries(boundary: &FilesystemBoundary) -> Vec<Rule> {
    let mut rules = Vec::new();

    // The roots themselves, inheritable so later files are covered.
    if let Some(readable) = &boundary.readable {
        for path in readable {
            rules.push(Rule::Allow {
                path: path.clone(),
                grant: Grant::Read,
                inheritable: true,
            });
        }
    }
    for path in &boundary.writable {
        rules.push(Rule::Allow {
            path: path.clone(),
            grant: Grant::ReadWrite,
            inheritable: true,
        });
    }

    // Then the walk grants, so the container can reach those roots at all. A root
    // that is itself an ancestor of another root already has a stronger grant, so it
    // is skipped rather than downgraded.
    let granted: Vec<&AbsPath> = rules.iter().map(Rule::path).collect();
    let mut walks: Vec<AbsPath> = Vec::new();
    for root in &granted {
        for ancestor in root.ancestors() {
            if granted.iter().any(|path| **path == ancestor) {
                continue;
            }
            if !walks.contains(&ancestor) {
                walks.push(ancestor);
            }
        }
    }
    let walks: Vec<Rule> = walks
        .into_iter()
        .map(|path| Rule::Allow {
            path,
            grant: Grant::Walk,
            inheritable: false,
        })
        .collect();
    rules.extend(walks);

    // Keep cutouts after grants in the diagnostic plan. Native write order is
    // derived from the complete projection rather than this list's order.
    let mut withheld: Vec<AbsPath> = Vec::new();
    for path in &boundary.deny_write {
        withheld.push(path.clone());
        rules.push(Rule::Withhold {
            path: path.clone(),
            keep: Some(Grant::Read),
        });
    }

    // Repository metadata, protected by name at the top of each writable root. This is
    // where a name like `.git` becomes an actual path: without it the policy would
    // carry the name, the request would carry the name, and nothing would enforce it.
    // Only the top level, because that is what can be named ahead of time — a nested
    // `.git` the command creates mid-run cannot be enumerated in advance.
    for root in &boundary.writable {
        for name in &boundary.protected_metadata {
            let Ok(path) = root.join(name) else { continue };
            if boundary.metadata_write_allow.contains(&path) {
                continue;
            }
            if withheld.contains(&path) {
                continue;
            }
            withheld.push(path.clone());
            rules.push(Rule::Withhold {
                path,
                keep: Some(Grant::Read),
            });
        }
    }

    for path in &boundary.deny_read {
        if withheld.contains(path) {
            // Already withheld for writes; denying reads is strictly stronger, so the
            // read denial replaces it rather than being applied on top.
            rules.retain(|rule| rule.path() != path);
        }
        rules.push(Rule::Withhold {
            path: path.clone(),
            keep: None,
        });
    }
    rules
}

/// Access mask for a grant, as the bits Windows checks.
///
/// Kept as a pure function so the mask a boundary produces can be asserted
/// without a Windows host. The constants are inlined rather than imported from
/// `windows-sys` for the same reason: this has to compile and be testable
/// everywhere, and these values are part of the NT file ABI, not a build detail.
pub fn access_mask(grant: Grant) -> u32 {
    // From winnt.h. Named locally so each bit's purpose is visible at the call
    // site instead of being a hex literal.
    const FILE_READ_DATA: u32 = 0x0001;
    const FILE_WRITE_DATA: u32 = 0x0002;
    const FILE_APPEND_DATA: u32 = 0x0004;
    const FILE_READ_EA: u32 = 0x0008;
    const FILE_WRITE_EA: u32 = 0x0010;
    const FILE_EXECUTE: u32 = 0x0020;
    /// The bit that makes deletion work; see the module note.
    const FILE_DELETE_CHILD: u32 = 0x0040;
    const FILE_READ_ATTRIBUTES: u32 = 0x0080;
    const FILE_WRITE_ATTRIBUTES: u32 = 0x0100;
    const DELETE: u32 = 0x0001_0000;
    const READ_CONTROL: u32 = 0x0002_0000;
    const SYNCHRONIZE: u32 = 0x0010_0000;

    // `FILE_READ_DATA` on a directory is "list its entries", and `FILE_EXECUTE` is
    // "traverse it". Both are needed to walk a path; neither reads a file.
    let walk = FILE_READ_DATA | FILE_EXECUTE | FILE_READ_ATTRIBUTES | READ_CONTROL | SYNCHRONIZE;
    let read = walk | FILE_READ_EA;
    match grant {
        Grant::Walk => walk,
        Grant::Read => read,
        Grant::ReadWrite => {
            read | FILE_WRITE_DATA
                | FILE_APPEND_DATA
                | FILE_WRITE_EA
                | FILE_WRITE_ATTRIBUTES
                // Both deletion paths are granted: `DELETE` covers removing the
                // granted directory itself, `FILE_DELETE_CHILD` covers removing
                // things inside it. Granting only the first is the bug described
                // in the module note.
                | DELETE
                | FILE_DELETE_CHILD
        }
    }
}

#[cfg(windows)]
mod imp {
    use std::ptr;

    use windows_sys::Win32::Foundation::{LocalFree, ERROR_SUCCESS, FALSE, HLOCAL};
    use windows_sys::Win32::Security::Authorization::{
        GetExplicitEntriesFromAclW, GetNamedSecurityInfoW, OBJECTS_AND_SID, SE_FILE_OBJECT,
        TRUSTEE_IS_OBJECTS_AND_SID, TRUSTEE_IS_SID,
    };
    use windows_sys::Win32::Security::{
        EqualSid, GetSecurityDescriptorControl, ACL, DACL_SECURITY_INFORMATION,
        PSECURITY_DESCRIPTOR, SE_DACL_PROTECTED,
    };

    #[cfg(test)]
    use windows_sys::Win32::Foundation::{GetLastError, TRUE};
    #[cfg(test)]
    use windows_sys::Win32::Security::Authorization::{
        SetEntriesInAclW, EXPLICIT_ACCESS_W, GRANT_ACCESS, NO_MULTIPLE_TRUSTEE, TRUSTEE_IS_UNKNOWN,
        TRUSTEE_W,
    };
    #[cfg(test)]
    use windows_sys::Win32::Security::{
        GetAce, InitializeSecurityDescriptor, SetFileSecurityW, SetSecurityDescriptorControl,
        SetSecurityDescriptorDacl, ACE_HEADER, CONTAINER_INHERIT_ACE, OBJECT_INHERIT_ACE,
        SECURITY_DESCRIPTOR, SE_DACL_AUTO_INHERITED,
    };
    #[cfg(test)]
    use windows_sys::Win32::System::SystemServices::SECURITY_DESCRIPTOR_REVISION;

    use super::{access_mask, Grant, Rule};
    use crate::access::{AccessError, HostAccess, TargetAccess};
    use crate::acl_lease::Lease;
    use crate::capability::CapabilitySid;
    use crate::identity::AppContainerSid;

    /// Why an ACL ownership transaction could not complete.
    #[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
    pub enum AclError {
        #[error(transparent)]
        Access(#[from] AccessError),
        #[error("reading the security descriptor of {path} failed: Win32 error {code}")]
        Read { path: String, code: u32 },
        #[error("building the ACL for {path} failed: Win32 error {code}")]
        Build { path: String, code: u32 },
        #[error("writing the ACL of {path} failed: Win32 error {code}")]
        Write { path: String, code: u32 },
        #[error("ACL ownership transaction failed: {detail}")]
        Lease { detail: String },
        #[error("an ACL grant at {path} cannot be removed without its ownership journal")]
        MissingJournal { path: String },
        #[error("{setup}; ACL rollback also failed: {rollback:?}")]
        Rollback {
            setup: String,
            rollback: Vec<String>,
            journal: Vec<SavedDacl>,
        },
    }

    fn wide(text: &str) -> Vec<u16> {
        text.encode_utf16().chain(std::iter::once(0)).collect()
    }

    struct OwnedDescriptor(PSECURITY_DESCRIPTOR);

    impl Drop for OwnedDescriptor {
        fn drop(&mut self) {
            if !self.0.is_null() {
                unsafe { LocalFree(self.0 as HLOCAL) };
            }
        }
    }

    /// One explicit attachment. Cloned journals share the same cleanup owner;
    /// a marker records an apply that did not acquire any filesystem permission.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct SavedDacl {
        lease: Option<Lease>,
    }

    impl SavedDacl {
        pub(crate) fn recovery_record(&self) -> serde_json::Value {
            self.lease
                .as_ref()
                .map_or(serde_json::Value::Null, Lease::recovery_record)
        }
    }

    /// Read a path's DACL, returning both the live pointer and whether it is protected.
    ///
    /// `Ok(None)` means the path does not exist. That is not a failure: the deny list
    /// names credential directories like `~/.aws` and `~/.ssh`, and most machines do
    /// not have all of them. Treating a missing path as an error made the whole sandbox
    /// refuse to start on any machine missing any one of them — measured, as
    /// `reading the security descriptor of C:\Users\...\.aws failed: Win32 error 2`.
    ///
    /// Only "not there" is folded away. A path that exists and cannot be read is a real
    /// failure and still propagates, because that one means the boundary would be
    /// weaker than the policy asked for without anyone noticing.
    fn read_dacl(path_text: &str) -> Result<Option<(OwnedDescriptor, *mut ACL, bool)>, AclError> {
        // From winerror.h.
        const ERROR_FILE_NOT_FOUND: u32 = 2;
        const ERROR_PATH_NOT_FOUND: u32 = 3;

        let path = wide(path_text);
        let mut dacl: *mut ACL = ptr::null_mut();
        let mut descriptor: PSECURITY_DESCRIPTOR = ptr::null_mut();
        let code = unsafe {
            GetNamedSecurityInfoW(
                path.as_ptr(),
                SE_FILE_OBJECT,
                DACL_SECURITY_INFORMATION,
                ptr::null_mut(),
                ptr::null_mut(),
                &mut dacl,
                ptr::null_mut(),
                &mut descriptor,
            )
        };
        if code == ERROR_FILE_NOT_FOUND || code == ERROR_PATH_NOT_FOUND {
            return Ok(None);
        }
        if code != ERROR_SUCCESS {
            return Err(AclError::Read {
                path: path_text.to_string(),
                code,
            });
        }
        let owned = OwnedDescriptor(descriptor);
        let mut control = 0u16;
        let mut revision = 0u32;
        let protected = unsafe {
            GetSecurityDescriptorControl(descriptor, &mut control, &mut revision) != FALSE
                && control & SE_DACL_PROTECTED != 0
        };
        Ok(Some((owned, dacl, protected)))
    }

    /// Apply one attachment and explicitly undo it if setup fails.
    pub fn apply(rules: &[Rule], sid: &AppContainerSid) -> Result<Vec<SavedDacl>, AclError> {
        let mut saved = Vec::new();
        if let Err(error) = apply_recorded(rules, sid, &mut saved) {
            let rollback = revoke(rules, &saved, sid);
            if !rollback.is_empty() {
                return Err(AclError::Rollback {
                    setup: error.to_string(),
                    rollback: rollback
                        .into_iter()
                        .map(|error| error.to_string())
                        .collect(),
                    journal: saved,
                });
            }
            return Err(error);
        }
        Ok(saved)
    }

    pub fn apply_recorded(
        rules: &[Rule],
        sid: &AppContainerSid,
        saved: &mut Vec<SavedDacl>,
    ) -> Result<(), AclError> {
        apply_recorded_with_progress(rules, sid, &[], saved, |_, _| {})
    }

    pub(crate) fn apply_recorded_with_progress(
        rules: &[Rule],
        sid: &AppContainerSid,
        capabilities: &[CapabilitySid],
        saved: &mut Vec<SavedDacl>,
        mut progress: impl FnMut(usize, &Rule),
    ) -> Result<(), AclError> {
        saved.push(SavedDacl { lease: None });
        let access = if rules.iter().any(|rule| {
            matches!(
                rule,
                Rule::Allow {
                    grant: Grant::Walk,
                    inheritable: false,
                    ..
                }
            )
        }) {
            Some(TargetAccess::new(sid, capabilities)?)
        } else {
            None
        };
        // A walk grant must also be writable by the host. System ancestors
        // (C:\\, C:\\Users) are administratively owned: an ordinary user holds no
        // WRITE_DAC there, so the ACE cannot exist and failing the whole setup
        // over it would be wrong. Container traversal through such ancestors is
        // covered by the container's SeChangeNotifyPrivilege; the shell built-ins
        // that need an ancestor to be listable only reach ancestors inside the
        // caller's own tree, where the host can grant.
        let host = match &access {
            Some(_) => Some(HostAccess::new()?),
            None => None,
        };
        let mut effective = Vec::new();
        for (index, rule) in rules.iter().enumerate() {
            if let (
                Some(access),
                Rule::Allow {
                    path,
                    grant: Grant::Walk,
                    inheritable: false,
                },
            ) = (&access, rule)
            {
                match access.allows_path(&path.as_str(), access_mask(Grant::Walk))? {
                    Some(false) => {}
                    // Already available traversal has no owned write or revoke.
                    Some(true) | None => continue,
                }
                if let Some(host) = &host {
                    if !host.can_write_dacl(&path.as_str())? {
                        continue;
                    }
                }
            }
            progress(index, rule);
            effective.push(rule.clone());
        }
        if effective.is_empty() {
            return Ok(());
        }
        let lease = Lease::new(sid, capabilities).map_err(|detail| AclError::Lease { detail })?;
        saved
            .last_mut()
            .expect("the apply marker was just inserted")
            .lease = Some(lease.clone());
        lease
            .acquire(&effective)
            .map_err(|detail| AclError::Lease { detail })
    }

    /// Release each attachment once, after its target and I/O are drained.
    /// An empty journal may prove that a SID is absent, but does not authorize
    /// deleting a pre-existing or another execution's grant.
    pub fn revoke(rules: &[Rule], saved: &[SavedDacl], sid: &AppContainerSid) -> Vec<AclError> {
        if !saved.is_empty() {
            for lease in saved.iter().rev().filter_map(|entry| entry.lease.as_ref()) {
                if let Err(detail) = lease.release() {
                    return vec![AclError::Lease { detail }];
                }
            }
            return Vec::new();
        }
        let mut failures = Vec::new();
        for rule in rules {
            let path = rule.path().as_str();
            match read_dacl(&path) {
                Ok(None) => {}
                Ok(Some((_descriptor, dacl, _))) if dacl.is_null() => {}
                Ok(Some((_descriptor, dacl, _))) => match acl_names_trustee(&path, dacl, sid) {
                    Ok(false) => {}
                    Ok(true) => failures.push(AclError::MissingJournal { path }),
                    Err(error) => failures.push(error),
                },
                Err(error) => failures.push(error),
            }
        }
        failures
    }

    fn acl_names_trustee(
        path_text: &str,
        dacl: *mut ACL,
        sid: &AppContainerSid,
    ) -> Result<bool, AclError> {
        let mut count = 0;
        let mut entries = ptr::null_mut();
        let code = unsafe { GetExplicitEntriesFromAclW(dacl, &mut count, &mut entries) };
        if code != ERROR_SUCCESS {
            return Err(AclError::Build {
                path: path_text.to_string(),
                code,
            });
        }
        // Prove absence before asking Windows to merge a new ACL: a merge is
        // allowed to normalize ordering even when it removes no trustee.
        let found = if entries.is_null() {
            count != 0
        } else {
            unsafe { std::slice::from_raw_parts(entries, count as usize) }
                .iter()
                .any(|entry| {
                    let trustee = &entry.Trustee;
                    if trustee.ptstrName.is_null() {
                        return true;
                    }
                    let candidate = match trustee.TrusteeForm {
                        TRUSTEE_IS_SID => trustee.ptstrName.cast::<std::ffi::c_void>(),
                        TRUSTEE_IS_OBJECTS_AND_SID => unsafe {
                            (*trustee.ptstrName.cast::<OBJECTS_AND_SID>())
                                .pSid
                                .cast::<std::ffi::c_void>()
                        },
                        // An unrecognized form cannot prove absence.
                        _ => return true,
                    };
                    candidate.is_null() || unsafe { EqualSid(candidate, sid.as_psid()) } != FALSE
                })
        };
        if !entries.is_null() {
            unsafe { LocalFree(entries.cast()) };
        }
        Ok(found)
    }

    #[cfg(test)]
    fn copy_acl(dacl: *mut ACL) -> Vec<u8> {
        if dacl.is_null() {
            return Vec::new();
        }
        let size = unsafe { (*dacl).AclSize } as usize;
        unsafe { std::slice::from_raw_parts(dacl.cast::<u8>(), size) }.to_vec()
    }

    #[cfg(test)]
    fn fixture_allow(path: &str, grant: Grant, sid: &AppContainerSid) -> Result<(), AclError> {
        let Some((_descriptor, existing, _)) = read_dacl(path)? else {
            return Ok(());
        };
        let access = EXPLICIT_ACCESS_W {
            grfAccessPermissions: access_mask(grant),
            grfAccessMode: GRANT_ACCESS,
            grfInheritance: 0,
            Trustee: TRUSTEE_W {
                pMultipleTrustee: ptr::null_mut(),
                MultipleTrusteeOperation: NO_MULTIPLE_TRUSTEE,
                TrusteeForm: TRUSTEE_IS_SID,
                TrusteeType: TRUSTEE_IS_UNKNOWN,
                ptstrName: sid.as_psid().cast(),
            },
        };
        let mut merged = ptr::null_mut();
        let code = unsafe { SetEntriesInAclW(1, &access, existing, &mut merged) };
        if code != ERROR_SUCCESS {
            return Err(AclError::Build {
                path: path.into(),
                code,
            });
        }
        let result = write_local_dacl(path, merged);
        if !merged.is_null() {
            unsafe { LocalFree(merged.cast()) };
        }
        result
    }

    #[cfg(test)]
    /// A walk grant changes this ancestor only. SetNamedSecurityInfo would also
    /// reapply unrelated inheritable ACEs throughout the existing descendant tree.
    /// SetFileSecurity is the documented legacy API with local-only semantics;
    /// the modern MAXIMUM_ALLOWED handle alternative fails on shared drive roots.
    /// Preserve the current protection and inheritance state with the new DACL.
    fn write_local_dacl(path_text: &str, dacl: *mut ACL) -> Result<(), AclError> {
        let Some((current, _, _)) = read_dacl(path_text)? else {
            return Ok(());
        };
        let path = wide(path_text);
        let mut control = 0;
        let mut revision = 0;
        let mut descriptor: SECURITY_DESCRIPTOR = unsafe { std::mem::zeroed() };
        let pointer = (&mut descriptor as *mut SECURITY_DESCRIPTOR).cast();
        let mask = SE_DACL_PROTECTED | SE_DACL_AUTO_INHERITED;
        let ok = unsafe {
            GetSecurityDescriptorControl(current.0, &mut control, &mut revision) != FALSE
                && InitializeSecurityDescriptor(pointer, SECURITY_DESCRIPTOR_REVISION) != FALSE
                && SetSecurityDescriptorDacl(pointer, TRUE, dacl, FALSE) != FALSE
                && SetSecurityDescriptorControl(pointer, mask, control & mask) != FALSE
                && SetFileSecurityW(path.as_ptr(), DACL_SECURITY_INFORMATION, pointer) != FALSE
        };
        if !ok {
            return Err(AclError::Write {
                path: path_text.to_string(),
                code: unsafe { GetLastError() },
            });
        }
        Ok(())
    }

    #[cfg(test)]
    mod partial_setup_tests {
        use super::*;
        use crate::boundary::AbsPath;

        #[test]
        fn a_walk_grant_does_not_refresh_unrelated_child_inheritance() {
            let snapshot = |path: &str| {
                let (_descriptor, acl, _) = read_dacl(path).unwrap().unwrap();
                copy_acl(acl)
            };
            let root = std::env::temp_dir().join(format!("klaus-acl-walk-{}", std::process::id()));
            let child = root.join("unrelated-child");
            std::fs::create_dir_all(&child).unwrap();
            let path = root.to_string_lossy().into_owned();
            let child_path = child.to_string_lossy().into_owned();
            let original_root = snapshot(&path);
            let sid =
                crate::identity::derive_sid(&format!("klaus-walk-{}", std::process::id())).unwrap();
            let seed =
                crate::identity::derive_sid(&format!("klaus-seed-{}", std::process::id())).unwrap();

            // Stage an unrelated inheritable entry on the parent alone. A walk
            // update must not refresh this pre-existing descendant's DACL.
            fixture_allow(&path, Grant::Read, &seed).unwrap();
            let (_descriptor, parent_acl, _) = read_dacl(&path).unwrap().unwrap();
            for index in 0..unsafe { (*parent_acl).AceCount } {
                let mut ace = ptr::null_mut();
                assert_ne!(
                    unsafe { GetAce(parent_acl, u32::from(index), &mut ace) },
                    FALSE
                );
                if unsafe { (*(ace as *const ACE_HEADER)).AceType } > 1 {
                    continue;
                }
                let ace_sid = unsafe { (ace as *mut u8).add(8) };
                if unsafe { windows_sys::Win32::Security::EqualSid(ace_sid.cast(), seed.as_psid()) }
                    != FALSE
                {
                    unsafe {
                        (*(ace as *mut ACE_HEADER)).AceFlags |=
                            (CONTAINER_INHERIT_ACE | OBJECT_INHERIT_ACE) as u8
                    };
                }
            }
            write_local_dacl(&path, parent_acl).unwrap();
            let seeded_root = snapshot(&path);
            // Observe after fixture preparation: NTFS may normalize inherited
            // flags when that deliberately stale parent descriptor is staged.
            let original_child = snapshot(&child_path);
            let rules = vec![Rule::Allow {
                path: AbsPath::new(&path).unwrap(),
                grant: Grant::Walk,
                inheritable: false,
            }];
            let saved = apply(&rules, &sid).unwrap();
            assert_ne!(snapshot(&path), seeded_root);
            let child_during = snapshot(&child_path);
            let cleanup = revoke(&rules, &saved, &sid);
            let child_after = snapshot(&child_path);
            let parent_after = snapshot(&path);
            let mut original = original_root;
            write_local_dacl(&path, original.as_mut_ptr().cast()).unwrap();
            std::fs::remove_dir_all(&root).unwrap();
            assert!(cleanup.is_empty(), "{cleanup:?}");
            assert_eq!(
                child_during, original_child,
                "walk setup propagated unrelated ACEs"
            );
            assert_eq!(
                child_after, original_child,
                "walk teardown propagated unrelated ACEs"
            );
            assert_eq!(
                parent_after, seeded_root,
                "walk teardown changed existing parent entries"
            );
        }

        #[test]
        fn failed_acl_preflight_retains_explicit_cleanup_owner() {
            let root =
                std::env::temp_dir().join(format!("klaus-acl-rollback-{}", std::process::id()));
            std::fs::create_dir_all(&root).unwrap();
            let path = root.to_string_lossy().into_owned();
            let (_before_descriptor, before, protected_before) = read_dacl(&path).unwrap().unwrap();
            let original = copy_acl(before);
            let name = format!("klaus-acl-rollback-{}", std::process::id());
            let sid = crate::identity::derive_sid(&name).unwrap();
            let rules = vec![
                Rule::Withhold {
                    path: AbsPath::new(&path).unwrap(),
                    keep: None,
                },
                Rule::Allow {
                    path: AbsPath::new(root.join("invalid<name>").to_string_lossy()).unwrap(),
                    grant: Grant::Read,
                    inheritable: false,
                },
            ];
            let mut saved = Vec::new();
            assert!(apply_recorded(&rules, &sid, &mut saved).is_err());
            assert_eq!(
                saved.iter().filter(|entry| entry.lease.is_some()).count(),
                1,
                "a failed preflight discarded its explicit cleanup owner"
            );
            // The entire group is inspected before any write. Cleanup still owns
            // the explicit attachment and must complete without changing ACLs.
            let cleanup = revoke(&rules, &saved, &sid);
            let (_after_descriptor, after, protected_after) = read_dacl(&path).unwrap().unwrap();
            let _ = std::io::Write::write_fmt(&mut std::io::stderr().lock(), format_args!(
                "partial_acl_cleanup={cleanup:?}; journal={saved:?}; current_acl={:?}; current_protected={protected_after}\n",
                copy_acl(after)
            ));
            assert!(cleanup.is_empty(), "{cleanup:?}");
            assert_eq!(copy_acl(after), original);
            assert_eq!(protected_after, protected_before);
            std::fs::remove_dir_all(&root).unwrap();
        }
    }
}

#[cfg(windows)]
pub use imp::{apply, apply_recorded, revoke, AclError, SavedDacl};

#[cfg(windows)]
pub(crate) use imp::apply_recorded_with_progress;

#[cfg(test)]
mod tests {
    use super::*;

    fn abs(path: &str) -> AbsPath {
        AbsPath::new(path).expect("test path parses")
    }

    #[test]
    fn a_writable_grant_includes_both_deletion_bits() {
        // The bug this pins: granting write without FILE_DELETE_CHILD produces a
        // sandbox where creating a file works and deleting it fails.
        const DELETE: u32 = 0x0001_0000;
        const FILE_DELETE_CHILD: u32 = 0x0040;
        let mask = access_mask(Grant::ReadWrite);
        assert_ne!(
            mask & DELETE,
            0,
            "DELETE missing: cannot remove the root itself"
        );
        assert_ne!(
            mask & FILE_DELETE_CHILD,
            0,
            "FILE_DELETE_CHILD missing: cannot remove anything inside the root"
        );
    }

    #[test]
    fn a_read_grant_carries_no_write_or_delete_bits() {
        const FILE_WRITE_DATA: u32 = 0x0002;
        const FILE_APPEND_DATA: u32 = 0x0004;
        const FILE_WRITE_EA: u32 = 0x0010;
        const FILE_WRITE_ATTRIBUTES: u32 = 0x0100;
        const DELETE: u32 = 0x0001_0000;
        const FILE_DELETE_CHILD: u32 = 0x0040;
        let mask = access_mask(Grant::Read);
        for (bit, name) in [
            (FILE_WRITE_DATA, "FILE_WRITE_DATA"),
            (FILE_APPEND_DATA, "FILE_APPEND_DATA"),
            (FILE_WRITE_EA, "FILE_WRITE_EA"),
            (FILE_WRITE_ATTRIBUTES, "FILE_WRITE_ATTRIBUTES"),
            (DELETE, "DELETE"),
            (FILE_DELETE_CHILD, "FILE_DELETE_CHILD"),
        ] {
            assert_eq!(mask & bit, 0, "read grant must not carry {name}");
        }
    }

    #[test]
    fn a_read_grant_can_still_traverse_and_open() {
        const FILE_READ_DATA: u32 = 0x0001;
        const FILE_EXECUTE: u32 = 0x0020;
        const FILE_READ_ATTRIBUTES: u32 = 0x0080;
        let mask = access_mask(Grant::Read);
        // Without FILE_EXECUTE (traverse) a directory grant cannot be walked into,
        // so a "readable root" would be readable in name only.
        assert_ne!(mask & FILE_EXECUTE, 0, "traverse missing");
        assert_ne!(mask & FILE_READ_DATA, 0);
        assert_ne!(mask & FILE_READ_ATTRIBUTES, 0);
    }

    #[test]
    fn a_walk_grant_lists_a_directory_without_reading_its_files() {
        // The ancestors of a granted root get this, so the container can walk to the
        // root. It must not become a way to read what those ancestors hold.
        const FILE_READ_DATA: u32 = 0x0001;
        const FILE_EXECUTE: u32 = 0x0020;
        const FILE_WRITE_DATA: u32 = 0x0002;
        const DELETE: u32 = 0x0001_0000;
        let mask = access_mask(Grant::Walk);
        // On a directory these two are "list entries" and "traverse".
        assert_ne!(
            mask & FILE_READ_DATA,
            0,
            "cannot list, so cannot canonicalize"
        );
        assert_ne!(mask & FILE_EXECUTE, 0, "cannot traverse");
        assert_eq!(mask & FILE_WRITE_DATA, 0, "a walk grant must not write");
        assert_eq!(mask & DELETE, 0, "a walk grant must not delete");
        // And it is strictly weaker than a read grant.
        assert_eq!(mask & access_mask(Grant::Read), mask);
        assert_ne!(mask, access_mask(Grant::Read));
    }

    #[test]
    fn every_withhold_is_planned_after_every_grant() {
        // Withholding breaks inheritance on its path. Applying an inheritable allow to
        // a root afterwards propagates straight back down over it, so the order here
        // is what makes a denial hold at all.
        let boundary = FilesystemBoundary {
            readable: Some(vec![abs("C:\\users\\me")]),
            writable: vec![abs("C:\\ws")],
            deny_write: vec![abs("C:\\ws\\vendor")],
            deny_read: vec![abs("C:\\users\\me\\.ssh")],
            ..Default::default()
        };
        let rules = plan_entries(&boundary);
        let first_withhold = rules
            .iter()
            .position(|rule| matches!(rule, Rule::Withhold { .. }))
            .expect("some withholds planned");
        let last_allow = rules
            .iter()
            .rposition(|rule| matches!(rule, Rule::Allow { .. }))
            .expect("some allows planned");
        assert!(
            last_allow < first_withhold,
            "allows must all precede withholds: {rules:?}"
        );
    }

    #[test]
    fn a_denial_is_an_absent_allow_and_never_a_deny_ace() {
        // Measured on a real machine: a deny ACE naming the container's own SID is not
        // consulted, so a plan that emitted one would produce a boundary that reads
        // correctly and enforces nothing.
        let boundary = FilesystemBoundary {
            writable: vec![abs("C:\\ws")],
            deny_read: vec![abs("C:\\ws\\.env")],
            deny_write: vec![abs("C:\\ws\\.git")],
            ..Default::default()
        };
        let rules = plan_entries(&boundary);
        let denied_read = rules
            .iter()
            .find(|rule| rule.path().as_str() == "C:\\ws\\.env")
            .expect("the read denial is planned");
        assert_eq!(
            *denied_read,
            Rule::Withhold {
                path: abs("C:\\ws\\.env"),
                keep: None,
            }
        );
        // A write denial still leaves the path readable: that is what "protect this
        // from being rewritten" means for repository metadata.
        let denied_write = rules
            .iter()
            .find(|rule| rule.path().as_str() == "C:\\ws\\.git")
            .expect("the write denial is planned");
        assert_eq!(
            *denied_write,
            Rule::Withhold {
                path: abs("C:\\ws\\.git"),
                keep: Some(Grant::Read),
            }
        );
    }

    #[test]
    fn every_ancestor_of_a_granted_root_gets_a_walk_grant() {
        // Without these the container can open a file inside the root through the raw
        // API while every shell built-in fails: `cmd` canonicalizes an absolute path by
        // descending from the drive root.
        let boundary = FilesystemBoundary {
            writable: vec![abs("C:\\users\\me\\ws")],
            ..Default::default()
        };
        let rules = plan_entries(&boundary);
        let walks: Vec<String> = rules
            .iter()
            .filter(|rule| {
                matches!(
                    rule,
                    Rule::Allow {
                        grant: Grant::Walk,
                        ..
                    }
                )
            })
            .map(|rule| rule.path().as_str())
            .collect();
        assert_eq!(
            walks,
            vec!["C:\\", "C:\\users", "C:\\users\\me"],
            "the whole chain down to the root's parent must be walkable"
        );
    }

    #[test]
    fn a_walk_grant_is_never_inheritable() {
        // An inheritable walk grant on `C:\` would hand the container list access to
        // every directory on the volume, which is the opposite of a boundary.
        let boundary = FilesystemBoundary {
            writable: vec![abs("C:\\users\\me\\ws")],
            ..Default::default()
        };
        for rule in plan_entries(&boundary) {
            if let Rule::Allow {
                grant,
                inheritable,
                path,
            } = rule
            {
                match grant {
                    Grant::Walk => assert!(!inheritable, "{path} must not inherit a walk grant"),
                    _ => assert!(inheritable, "{path} is a root and must cover new files"),
                }
            }
        }
    }

    #[test]
    fn an_ancestor_that_is_itself_a_granted_root_keeps_the_stronger_grant() {
        // `C:\ws` is both a writable root and an ancestor of `C:\ws\sub`. Planning a
        // walk grant for it as well would be harmless in effect but would say the
        // wrong thing, and teardown would then have two entries to reason about.
        let boundary = FilesystemBoundary {
            writable: vec![abs("C:\\ws"), abs("C:\\ws\\sub")],
            ..Default::default()
        };
        let rules = plan_entries(&boundary);
        let for_ws: Vec<&Rule> = rules
            .iter()
            .filter(|rule| rule.path().as_str() == "C:\\ws")
            .collect();
        assert_eq!(for_ws.len(), 1, "one rule per path: {for_ws:?}");
        assert!(matches!(
            for_ws[0],
            Rule::Allow {
                grant: Grant::ReadWrite,
                ..
            }
        ));
    }

    #[test]
    fn a_shared_ancestor_is_planned_once() {
        let boundary = FilesystemBoundary {
            writable: vec![abs("C:\\users\\me\\a"), abs("C:\\users\\me\\b")],
            ..Default::default()
        };
        let rules = plan_entries(&boundary);
        let me = rules
            .iter()
            .filter(|rule| rule.path().as_str() == "C:\\users\\me")
            .count();
        assert_eq!(me, 1, "the shared ancestor must appear once: {rules:?}");
    }

    #[test]
    fn a_protected_metadata_name_becomes_a_withheld_path_under_every_writable_root() {
        // The gap this closes: the policy carries the name `.git`, the request carries
        // it, and before this nothing turned it into an ACL rule — so the metadata a
        // command must not rewrite was protected by nothing at all.
        let boundary = FilesystemBoundary {
            writable: vec![abs("C:\\ws"), abs("C:\\other")],
            protected_metadata: vec![".git".to_string()],
            ..Default::default()
        };
        let rules = plan_entries(&boundary);
        let withheld: Vec<String> = rules
            .iter()
            .filter(|rule| matches!(rule, Rule::Withhold { .. }))
            .map(|rule| rule.path().as_str())
            .collect();
        assert_eq!(withheld, vec!["C:\\ws\\.git", "C:\\other\\.git"]);
        // Readable, not writable: a command has to be able to read the repository it
        // is working in.
        for rule in rules
            .iter()
            .filter(|rule| matches!(rule, Rule::Withhold { .. }))
        {
            assert_eq!(
                *rule,
                Rule::Withhold {
                    path: rule.path().clone(),
                    keep: Some(Grant::Read),
                }
            );
        }
    }

    #[test]
    fn a_metadata_exemption_leaves_that_path_alone() {
        // A policy that names its own protected list is opting the others back in, and
        // the exemption is how that arrives here.
        let boundary = FilesystemBoundary {
            writable: vec![abs("C:\\ws")],
            protected_metadata: vec![".git".to_string(), ".klaus".to_string()],
            metadata_write_allow: vec![abs("C:\\ws\\.klaus")],
            ..Default::default()
        };
        let withheld: Vec<String> = plan_entries(&boundary)
            .iter()
            .filter(|rule| matches!(rule, Rule::Withhold { .. }))
            .map(|rule| rule.path().as_str())
            .collect();
        assert_eq!(withheld, vec!["C:\\ws\\.git"]);
    }

    #[test]
    fn a_path_denied_for_reading_is_not_also_planned_as_merely_unwritable() {
        // Both lists can name one path. Two rules for it would apply the weaker one
        // second, and the weaker one re-adds the read access the stronger just took
        // away.
        let boundary = FilesystemBoundary {
            writable: vec![abs("C:\\ws")],
            deny_write: vec![abs("C:\\ws\\secret")],
            deny_read: vec![abs("C:\\ws\\secret")],
            ..Default::default()
        };
        let rules = plan_entries(&boundary);
        let for_secret: Vec<&Rule> = rules
            .iter()
            .filter(|rule| rule.path().as_str() == "C:\\ws\\secret")
            .collect();
        assert_eq!(for_secret.len(), 1, "one rule per path: {for_secret:?}");
        assert_eq!(
            *for_secret[0],
            Rule::Withhold {
                path: abs("C:\\ws\\secret"),
                keep: None,
            },
            "the stronger denial has to be the one that survives"
        );
    }

    #[test]
    fn a_protected_name_above_a_writable_root_does_not_withhold_anything() {
        // A workspace can legitimately live under a directory sharing a name with
        // protected metadata — a worktree under `.klaus` is the case that occurs. Only
        // names directly inside a writable root are withheld, so the root's own
        // ancestors are untouched and the whole tree stays writable.
        let boundary = FilesystemBoundary {
            writable: vec![abs("C:\\users\\me\\.klaus\\worktrees\\ws")],
            protected_metadata: vec![".klaus".to_string()],
            ..Default::default()
        };
        let withheld: Vec<String> = plan_entries(&boundary)
            .iter()
            .filter(|rule| matches!(rule, Rule::Withhold { .. }))
            .map(|rule| rule.path().as_str())
            .collect();
        assert_eq!(
            withheld,
            vec!["C:\\users\\me\\.klaus\\worktrees\\ws\\.klaus"],
            "the ancestor named .klaus must not be withheld"
        );
    }

    #[test]
    fn an_exemption_is_not_a_way_around_an_explicit_denial() {
        // An exemption says "this metadata directory is not protected". It must not
        // reopen a path the policy denied outright.
        let boundary = FilesystemBoundary {
            writable: vec![abs("C:\\ws")],
            deny_write: vec![abs("C:\\ws\\.git")],
            protected_metadata: vec![".git".to_string()],
            metadata_write_allow: vec![abs("C:\\ws\\.git")],
            ..Default::default()
        };
        let rules = plan_entries(&boundary);
        let for_git: Vec<&Rule> = rules
            .iter()
            .filter(|rule| rule.path().as_str() == "C:\\ws\\.git")
            .collect();
        assert_eq!(for_git.len(), 1, "one rule per path: {for_git:?}");
        assert!(matches!(for_git[0], Rule::Withhold { .. }));
    }

    #[test]
    fn a_plan_covers_exactly_the_paths_the_boundary_names() {
        let boundary = FilesystemBoundary {
            readable: Some(vec![abs("C:\\tools")]),
            writable: vec![abs("C:\\ws"), abs("C:\\temp")],
            deny_write: vec![abs("C:\\ws\\.git")],
            deny_read: vec![abs("C:\\users\\me\\.aws")],
            ..Default::default()
        };
        let rules = plan_entries(&boundary);

        let writable: Vec<_> = rules
            .iter()
            .filter(|rule| {
                matches!(
                    rule,
                    Rule::Allow {
                        grant: Grant::ReadWrite,
                        ..
                    }
                )
            })
            .map(|rule| rule.path().as_str())
            .collect();
        assert_eq!(writable, vec!["C:\\ws", "C:\\temp"]);

        let read_only: Vec<_> = rules
            .iter()
            .filter(|rule| {
                matches!(
                    rule,
                    Rule::Allow {
                        grant: Grant::Read,
                        ..
                    }
                )
            })
            .map(|rule| rule.path().as_str())
            .collect();
        assert_eq!(read_only, vec!["C:\\tools"]);

        let withheld: Vec<_> = rules
            .iter()
            .filter(|rule| matches!(rule, Rule::Withhold { .. }))
            .map(|rule| rule.path().as_str())
            .collect();
        assert_eq!(withheld, vec!["C:\\ws\\.git", "C:\\users\\me\\.aws"]);

        // Every root here sits directly on the drive, so the only ancestor to make
        // walkable is `C:\`. A withheld path gets none: the container has no access to
        // it, so nothing needs to walk there.
        let walks: Vec<_> = rules
            .iter()
            .filter(|rule| {
                matches!(
                    rule,
                    Rule::Allow {
                        grant: Grant::Walk,
                        ..
                    }
                )
            })
            .map(|rule| rule.path().as_str())
            .collect();
        assert_eq!(walks, vec!["C:\\"]);
        assert_eq!(rules.len(), 3 + 1 + 2, "unexpected plan: {rules:?}");
    }

    #[test]
    fn an_absent_readable_list_plans_no_read_grants() {
        // "No read restriction" must not become "grant read on something this code
        // picked", which would be a boundary the policy never asked for.
        let boundary = FilesystemBoundary {
            writable: vec![abs("C:\\ws")],
            deny_read: vec![abs("C:\\users\\me\\.ssh")],
            ..Default::default()
        };
        let rules = plan_entries(&boundary);
        let read_grants = rules
            .iter()
            .filter(|rule| {
                matches!(
                    rule,
                    Rule::Allow {
                        grant: Grant::Read,
                        ..
                    }
                )
            })
            .count();
        assert_eq!(read_grants, 0);
        // The denial still gets planned.
        assert!(rules
            .iter()
            .any(|rule| matches!(rule, Rule::Withhold { keep: None, .. })));
    }

    #[test]
    fn an_empty_boundary_plans_nothing() {
        assert!(plan_entries(&FilesystemBoundary::default()).is_empty());
    }
}
