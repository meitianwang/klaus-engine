//! Defect: rollback rewrote untouched DACLs and reported cleanup failures for absent SIDs.
//! Root cause: the complete setup plan and write-ahead snapshots were restored without
//! checking whether this container had changed the object.
//! Regression for the 2026-09-09 Windows full-application sandbox cleanup work item.
//!
//! These tests call the public sandbox ACL API. Every object is created below one
//! unique temporary directory; no boundary plan, profile registration, or system
//! directory is involved. Handles retaining WRITE_DAC are opened before mutation.

#![cfg(windows)]

use std::fs::{self, File, OpenOptions};
use std::io;
use std::os::windows::fs::OpenOptionsExt;
use std::os::windows::io::AsRawHandle;
use std::path::{Path, PathBuf};
use std::ptr;

use klaus_sandbox_win::acl::{self, Grant, Rule};
use klaus_sandbox_win::boundary::AbsPath;
use klaus_sandbox_win::identity::{derive_sid, AppContainerSid};
use windows_sys::Win32::Foundation::{LocalFree, ERROR_ACCESS_DENIED, ERROR_SUCCESS};
use windows_sys::Win32::Security::Authorization::{
    ConvertStringSecurityDescriptorToSecurityDescriptorW, GetSecurityInfo, SetEntriesInAclW,
    SetSecurityInfo, EXPLICIT_ACCESS_W, GRANT_ACCESS, NO_MULTIPLE_TRUSTEE, SDDL_REVISION_1,
    SE_FILE_OBJECT, TRUSTEE_IS_SID, TRUSTEE_IS_UNKNOWN, TRUSTEE_W,
};
use windows_sys::Win32::Security::{
    EqualSid, GetAce, GetSecurityDescriptorControl, GetSecurityDescriptorDacl,
    InitializeSecurityDescriptor, SetFileSecurityW, SetSecurityDescriptorControl,
    SetSecurityDescriptorDacl, ACE_HEADER, ACL, CONTAINER_INHERIT_ACE, DACL_SECURITY_INFORMATION,
    OBJECT_INHERIT_ACE, PROTECTED_DACL_SECURITY_INFORMATION, PSECURITY_DESCRIPTOR,
    SECURITY_DESCRIPTOR, SE_DACL_AUTO_INHERITED, SE_DACL_AUTO_INHERIT_REQ, SE_DACL_DEFAULTED,
    SE_DACL_PRESENT, SE_DACL_PROTECTED, UNPROTECTED_DACL_SECURITY_INFORMATION,
};
use windows_sys::Win32::Storage::FileSystem::{
    FILE_FLAG_BACKUP_SEMANTICS, FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE, READ_CONTROL,
    WRITE_DAC,
};

#[derive(Debug, PartialEq, Eq)]
struct DaclImage {
    control: u16,
    present: bool,
    defaulted: bool,
    // None is a NULL DACL; Some([]) is an empty DACL. Do not collapse them.
    aces: Option<Vec<Vec<u8>>>,
}

struct Descriptor(PSECURITY_DESCRIPTOR);

impl Descriptor {
    fn read(file: &File) -> io::Result<Self> {
        let mut raw = ptr::null_mut();
        let code = unsafe {
            GetSecurityInfo(
                file.as_raw_handle(),
                SE_FILE_OBJECT,
                DACL_SECURITY_INFORMATION,
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                &mut raw,
            )
        };
        win32(code)?;
        Ok(Self(raw))
    }

    fn from_sddl(text: &str) -> io::Result<Self> {
        let mut raw = ptr::null_mut();
        let ok = unsafe {
            ConvertStringSecurityDescriptorToSecurityDescriptorW(
                wide(text).as_ptr(),
                SDDL_REVISION_1,
                &mut raw,
                ptr::null_mut(),
            )
        };
        success(ok)?;
        Ok(Self(raw))
    }

    fn control(&self) -> io::Result<u16> {
        let mut control = 0;
        let mut revision = 0;
        success(unsafe { GetSecurityDescriptorControl(self.0, &mut control, &mut revision) })?;
        Ok(control)
    }

    fn dacl(&self) -> io::Result<(*mut ACL, bool, bool)> {
        let mut acl = ptr::null_mut();
        let mut present = 0;
        let mut defaulted = 0;
        success(unsafe {
            GetSecurityDescriptorDacl(self.0, &mut present, &mut acl, &mut defaulted)
        })?;
        Ok((acl, present != 0, defaulted != 0))
    }

    fn image(&self) -> io::Result<DaclImage> {
        let (acl, present, defaulted) = self.dacl()?;
        let aces = if acl.is_null() {
            None
        } else {
            let mut entries = Vec::new();
            for index in 0..u32::from(unsafe { (*acl).AceCount }) {
                let mut raw = ptr::null_mut();
                success(unsafe { GetAce(acl, index, &mut raw) })?;
                let length = usize::from(unsafe { (*(raw as *const ACE_HEADER)).AceSize });
                entries
                    .push(unsafe { std::slice::from_raw_parts(raw.cast::<u8>(), length) }.to_vec());
            }
            Some(entries)
        };
        // Compare ordered ACE bytes and DACL state, not allocator slack after the ACL.
        let mask = SE_DACL_PRESENT
            | SE_DACL_DEFAULTED
            | SE_DACL_PROTECTED
            | SE_DACL_AUTO_INHERITED
            | SE_DACL_AUTO_INHERIT_REQ;
        Ok(DaclImage {
            control: self.control()? & mask,
            present,
            defaulted,
            aces,
        })
    }

    fn matching_aces(&self, sid: &AppContainerSid) -> io::Result<Vec<*mut ACE_HEADER>> {
        let (acl, _, _) = self.dacl()?;
        let mut matches = Vec::new();
        if !acl.is_null() {
            for index in 0..u32::from(unsafe { (*acl).AceCount }) {
                let mut raw = ptr::null_mut();
                success(unsafe { GetAce(acl, index, &mut raw) })?;
                let header = raw.cast::<ACE_HEADER>();
                if unsafe { (*header).AceType } <= 1
                    && unsafe { EqualSid(raw.cast::<u8>().add(8).cast(), sid.as_psid()) } != 0
                {
                    matches.push(header);
                }
            }
        }
        Ok(matches)
    }
}

impl Drop for Descriptor {
    fn drop(&mut self) {
        if !self.0.is_null() {
            unsafe { LocalFree(self.0) };
        }
    }
}

struct RetainedDacl {
    file: File,
    original: Descriptor,
}

impl RetainedDacl {
    fn restore(&self) -> io::Result<()> {
        let information = DACL_SECURITY_INFORMATION
            | if self.original.control()? & SE_DACL_PROTECTED != 0 {
                PROTECTED_DACL_SECURITY_INFORMATION
            } else {
                UNPROTECTED_DACL_SECURITY_INFORMATION
            };
        win32(unsafe {
            SetSecurityInfo(
                self.file.as_raw_handle(),
                SE_FILE_OBJECT,
                information,
                ptr::null_mut(),
                ptr::null_mut(),
                self.original.dacl()?.0,
                ptr::null_mut(),
            )
        })
    }
}

struct OwnedTree {
    root: PathBuf,
    retained: Vec<RetainedDacl>,
    cleaned: bool,
}

impl OwnedTree {
    fn new() -> io::Result<Self> {
        let root = std::env::temp_dir().join(format!("klaus-acl-revoke-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&root)?;
        let mut tree = Self {
            root,
            retained: Vec::new(),
            cleaned: false,
        };
        tree.retain(&tree.root.clone())?;
        Ok(tree)
    }

    fn retain(&mut self, path: &Path) -> io::Result<usize> {
        assert!(
            path.starts_with(&self.root),
            "only owned fixture objects may be retained"
        );
        let file = open_security(path, READ_CONTROL | WRITE_DAC)?;
        let original = Descriptor::read(&file)?;
        let index = self.retained.len();
        self.retained.push(RetainedDacl { file, original });
        Ok(index)
    }

    fn descriptor(&self, index: usize) -> io::Result<Descriptor> {
        Descriptor::read(&self.retained[index].file)
    }

    fn restore_and_remove(&mut self) -> io::Result<()> {
        let mut failure = None;
        // Restore parents first and descendants afterwards, all through retained handles.
        for entry in &self.retained {
            if let Err(error) = entry.restore() {
                failure.get_or_insert(error);
            }
        }
        if let Some(error) = failure {
            return Err(error);
        }
        self.retained.clear();
        fs::remove_dir_all(&self.root)?;
        self.cleaned = true;
        Ok(())
    }
}

impl Drop for OwnedTree {
    fn drop(&mut self) {
        if !self.cleaned {
            if let Err(error) = self.restore_and_remove() {
                eprintln!(
                    "owned ACL fixture retained at {} after cleanup failure: {error}",
                    self.root.display()
                );
            }
        }
    }
}

fn wide(text: &str) -> Vec<u16> {
    text.encode_utf16().chain(std::iter::once(0)).collect()
}
fn win32(code: u32) -> io::Result<()> {
    if code == ERROR_SUCCESS {
        Ok(())
    } else {
        Err(io::Error::from_raw_os_error(code as i32))
    }
}
fn success(value: i32) -> io::Result<()> {
    if value != 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}
fn open_security(path: &Path, access: u32) -> io::Result<File> {
    OpenOptions::new()
        .access_mode(access)
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
        .open(path)
}
fn set_local(path: &Path, descriptor: &Descriptor) -> io::Result<()> {
    success(unsafe {
        SetFileSecurityW(
            wide(&path.to_string_lossy()).as_ptr(),
            DACL_SECURITY_INFORMATION,
            descriptor.0,
        )
    })
}

fn seed_local_allow(path: &Path, original: &Descriptor, sid: &AppContainerSid) -> io::Result<()> {
    // This is fixture state, not an acquired production lease. Preparing it
    // through apply and discarding its journal would strand a registered owner.
    struct OwnedAcl(*mut ACL);
    impl Drop for OwnedAcl {
        fn drop(&mut self) {
            if !self.0.is_null() {
                unsafe { LocalFree(self.0.cast()) };
            }
        }
    }
    let entry = EXPLICIT_ACCESS_W {
        grfAccessPermissions: acl::access_mask(Grant::Read),
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
    let mut merged = OwnedAcl(ptr::null_mut());
    win32(unsafe { SetEntriesInAclW(1, &entry, original.dacl()?.0, &mut merged.0) })?;
    let mut descriptor: SECURITY_DESCRIPTOR = unsafe { std::mem::zeroed() };
    let raw = (&mut descriptor as *mut SECURITY_DESCRIPTOR).cast();
    let mask = SE_DACL_PROTECTED | SE_DACL_AUTO_INHERITED;
    success(unsafe { InitializeSecurityDescriptor(raw, 1) })?;
    success(unsafe { SetSecurityDescriptorDacl(raw, 1, merged.0, 0) })?;
    success(unsafe { SetSecurityDescriptorControl(raw, mask, original.control()? & mask) })?;
    success(unsafe {
        SetFileSecurityW(
            wide(&path.to_string_lossy()).as_ptr(),
            DACL_SECURITY_INFORMATION,
            raw,
        )
    })
}

fn set_null_dacl(path: &Path) -> io::Result<()> {
    let mut descriptor: SECURITY_DESCRIPTOR = unsafe { std::mem::zeroed() };
    let raw = (&mut descriptor as *mut SECURITY_DESCRIPTOR).cast();
    // SECURITY_DESCRIPTOR_REVISION is 1 in winnt.h. Protection keeps parent
    // inheritance from replacing the intentionally NULL fixture DACL.
    success(unsafe { InitializeSecurityDescriptor(raw, 1) })?;
    success(unsafe { SetSecurityDescriptorDacl(raw, 1, ptr::null(), 0) })?;
    success(unsafe { SetSecurityDescriptorControl(raw, SE_DACL_PROTECTED, SE_DACL_PROTECTED) })?;
    success(unsafe {
        SetFileSecurityW(
            wide(&path.to_string_lossy()).as_ptr(),
            DACL_SECURITY_INFORMATION,
            raw,
        )
    })
}
fn rule(path: &Path, inheritable: bool) -> Rule {
    Rule::Allow {
        path: AbsPath::new(path.to_string_lossy()).unwrap(),
        grant: Grant::Read,
        inheritable,
    }
}
fn private_sid() -> AppContainerSid {
    derive_sid(&format!("klaus-acl-{}", uuid::Uuid::new_v4())).unwrap()
}

#[test]
fn absent_sid_and_failed_withhold_revoke_need_no_write_dac() {
    let mut tree = OwnedTree::new().unwrap();
    let path = tree.root.join("read-only-dacl.txt");
    fs::write(&path, "owned fixture").unwrap();
    let file = tree.retain(&path).unwrap();
    // OWNER RIGHTS suppresses the owner's implicit WRITE_DAC. Keep READ_CONTROL so
    // rollback can read the unchanged descriptor; the old handle still restores it.
    set_local(
        &path,
        &Descriptor::from_sddl("D:P(A;;RC;;;OW)(A;;FR;;;WD)").unwrap(),
    )
    .unwrap();
    let before = tree.descriptor(file).unwrap().image().unwrap();
    let reopen = open_security(&path, WRITE_DAC);
    let reopen_error = reopen.as_ref().err().and_then(io::Error::raw_os_error);
    drop(reopen);
    let sid = private_sid();
    let absent_before = tree
        .descriptor(file)
        .unwrap()
        .matching_aces(&sid)
        .unwrap()
        .is_empty();
    let absent_cleanup = acl::revoke(&[rule(&path, false)], &[], &sid);
    let after_absent = tree.descriptor(file).unwrap().image().unwrap();

    let withhold = [Rule::Withhold {
        path: AbsPath::new(path.to_string_lossy()).unwrap(),
        keep: None,
    }];
    let mut saved = Vec::new();
    let setup = acl::apply_recorded(&withhold, &sid, &mut saved);
    let after_setup = tree.descriptor(file).unwrap().image().unwrap();
    let failed_setup_cleanup = acl::revoke(&withhold, &saved, &sid);
    let after_cleanup = tree.descriptor(file).unwrap().image().unwrap();
    tree.restore_and_remove().unwrap();

    assert_eq!(
        reopen_error,
        Some(ERROR_ACCESS_DENIED as i32),
        "fixture must prohibit reopening WRITE_DAC"
    );
    assert!(absent_before, "the rollback SID was never granted");
    assert!(
        matches!(
            setup,
            Err(acl::AclError::Lease { ref detail })
                if detail.contains("Win32 error 5") && detail.contains(path.to_string_lossy().as_ref())
        ),
        "withhold must fail while opening its denied ACL before any write: {setup:?}"
    );
    assert_eq!(after_setup, before, "failed withhold changed the file DACL");
    assert!(
        absent_cleanup.is_empty(),
        "untouched allow rule needs no write: {absent_cleanup:?}"
    );
    assert_eq!(
        after_absent, before,
        "absent SID revocation changed the file DACL"
    );
    assert!(
        failed_setup_cleanup.is_empty(),
        "unchanged write-ahead snapshot needs no restore: {failed_setup_cleanup:?}"
    );
    assert_eq!(
        after_cleanup, before,
        "failed-setup rollback changed the file DACL"
    );
}

#[test]
fn absent_sid_revoke_does_not_refresh_unrelated_descendant_inheritance() {
    let mut tree = OwnedTree::new().unwrap();
    let child = tree.root.join("unrelated-child");
    fs::create_dir(&child).unwrap();
    let child_index = tree.retain(&child).unwrap();
    let leaf = child.join("existing.txt");
    fs::write(&leaf, "unrelated owned fixture").unwrap();
    let leaf_index = tree.retain(&leaf).unwrap();
    let seed = private_sid();
    seed_local_allow(&tree.root, &tree.descriptor(0).unwrap(), &seed).unwrap();
    let parent = tree.descriptor(0).unwrap();
    let seed_aces = parent.matching_aces(&seed).unwrap();
    for header in &seed_aces {
        unsafe { (**header).AceFlags |= (CONTAINER_INHERIT_ACE | OBJECT_INHERIT_ACE) as u8 };
    }
    // Deliberately stage an inheritable ACE on the parent alone, like the existing
    // walk-grant regression. A no-op revoke must not refresh this stale inheritance.
    set_local(&tree.root, &parent).unwrap();
    let before_parent = tree.descriptor(0).unwrap().image().unwrap();
    let before_child = tree.descriptor(child_index).unwrap().image().unwrap();
    let before_leaf = tree.descriptor(leaf_index).unwrap().image().unwrap();
    let child_seed_absent = tree
        .descriptor(child_index)
        .unwrap()
        .matching_aces(&seed)
        .unwrap()
        .is_empty();
    let sid = private_sid();
    let target_absent = tree
        .descriptor(0)
        .unwrap()
        .matching_aces(&sid)
        .unwrap()
        .is_empty();
    let cleanup = acl::revoke(&[rule(&tree.root, true)], &[], &sid);
    let after_parent = tree.descriptor(0).unwrap().image().unwrap();
    let after_child = tree.descriptor(child_index).unwrap().image().unwrap();
    let after_leaf = tree.descriptor(leaf_index).unwrap().image().unwrap();
    tree.restore_and_remove().unwrap();

    assert!(
        !seed_aces.is_empty() && child_seed_absent,
        "fixture must stage parent-only inheritance"
    );
    assert!(target_absent, "the rollback SID was never granted");
    assert!(cleanup.is_empty(), "no-op cleanup failed: {cleanup:?}");
    assert_eq!(
        after_parent, before_parent,
        "no-op revoke changed the parent DACL"
    );
    assert_eq!(
        after_child, before_child,
        "no-op revoke refreshed unrelated child inheritance"
    );
    assert_eq!(
        after_leaf, before_leaf,
        "no-op revoke refreshed unrelated file inheritance"
    );
}

#[test]
fn absent_sid_revoke_preserves_a_null_dacl() {
    let mut tree = OwnedTree::new().unwrap();
    let path = tree.root.join("null-dacl.txt");
    fs::write(&path, "owned fixture").unwrap();
    let file = tree.retain(&path).unwrap();
    set_null_dacl(&path).unwrap();
    let before = tree.descriptor(file).unwrap().image().unwrap();
    let cleanup = acl::revoke(&[rule(&path, true)], &[], &private_sid());
    // Read through the retained READ_CONTROL handle even if the regression replaced
    // unrestricted access with an empty DACL. Recovery also uses the retained handle.
    let after = tree.descriptor(file).unwrap().image().unwrap();
    tree.restore_and_remove().unwrap();

    assert!(
        before.present && before.aces.is_none(),
        "fixture must start with a present NULL DACL"
    );
    assert!(
        cleanup.is_empty(),
        "NULL DACL no-op cleanup failed: {cleanup:?}"
    );
    assert_eq!(
        after, before,
        "an absent trustee must not turn NULL into an empty DACL"
    );
}

#[test]
fn an_empty_journal_cannot_revoke_a_preexisting_grant() {
    let mut tree = OwnedTree::new().unwrap();
    let sid = private_sid();
    seed_local_allow(&tree.root, &tree.descriptor(0).unwrap(), &sid).unwrap();
    let original = tree.descriptor(0).unwrap();
    let seeded = !original.matching_aces(&sid).unwrap().is_empty();
    let before = original.image().unwrap();
    // A matching trustee is not ownership evidence. This grant predates this
    // caller's empty journal and must survive its attempted cleanup.
    let cleanup = acl::revoke(&[rule(&tree.root, false)], &[], &sid);
    let after = tree.descriptor(0).unwrap().image().unwrap();
    tree.restore_and_remove().unwrap();

    assert!(seeded, "fixture must contain the pre-existing grant");
    assert!(!cleanup.is_empty(), "cleanup must require an actual owner");
    assert_eq!(
        after, before,
        "an empty journal removed another owner's grant"
    );
}
