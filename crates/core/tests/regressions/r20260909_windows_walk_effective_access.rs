//! Defect: an already-authorized non-inheritable Walk still required WRITE_DAC.
//! Root cause: applying an allow always rewrote the DACL, and rollback did not
//! distinguish an existing authorization from an authorization installed by setup.
//! Regression for the 2026-09-09 Windows sandbox effective-access work item.
//!
//! The public apply/apply_recorded/revoke APIs are exercised against an owned empty
//! directory. Its protected DACL explicitly grants the host and, in the positive
//! case, the private AppContainer SID the complete Walk mask. OWNER RIGHTS removes
//! the owner's implicit WRITE_DAC; a handle opened before mutation retains the
//! right to restore the original descriptor. No profile, process, system ACL,
//! network configuration, or null device is created or changed by this fixture.
//! The negative case has the same host permissions but no AppContainer grant.
//! A writable-DACL case also widens an existing Read ACE, then checks restoration,
//! repeated rollback, and a second application using the same rollback journal.

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
    ConvertSidToStringSidW, ConvertStringSecurityDescriptorToSecurityDescriptorW, GetSecurityInfo,
    SetSecurityInfo, SDDL_REVISION_1, SE_FILE_OBJECT,
};
use windows_sys::Win32::Security::{
    EqualSid, GetAce, GetSecurityDescriptorControl, GetSecurityDescriptorDacl, SetFileSecurityW,
    ACCESS_ALLOWED_ACE, ACE_HEADER, ACL, DACL_SECURITY_INFORMATION,
    PROTECTED_DACL_SECURITY_INFORMATION, PSECURITY_DESCRIPTOR, SE_DACL_AUTO_INHERITED,
    SE_DACL_AUTO_INHERIT_REQ, SE_DACL_DEFAULTED, SE_DACL_PRESENT, SE_DACL_PROTECTED,
    UNPROTECTED_DACL_SECURITY_INFORMATION,
};
use windows_sys::Win32::Storage::FileSystem::{
    FILE_FLAG_BACKUP_SEMANTICS, FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE, READ_CONTROL,
    WRITE_DAC,
};

// List, traverse, read attributes, read control, and synchronize. Pin the fixture's
// rights explicitly instead of deriving its permissions from the function under test.
const WALK_ACCESS: u32 = 0x0012_00a1;

#[derive(Debug, PartialEq, Eq)]
struct DaclImage {
    control: u16,
    present: bool,
    defaulted: bool,
    // A NULL DACL is different from an empty DACL.
    aces: Option<Vec<Vec<u8>>>,
}

struct Descriptor(PSECURITY_DESCRIPTOR);

impl Descriptor {
    fn read(file: &File) -> io::Result<Self> {
        let mut raw = ptr::null_mut();
        win32(unsafe {
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
        })?;
        Ok(Self(raw))
    }

    fn from_sddl(text: &str) -> io::Result<Self> {
        let mut raw = ptr::null_mut();
        success(unsafe {
            ConvertStringSecurityDescriptorToSecurityDescriptorW(
                wide(text).as_ptr(),
                SDDL_REVISION_1,
                &mut raw,
                ptr::null_mut(),
            )
        })?;
        Ok(Self(raw))
    }

    fn control(&self) -> io::Result<u16> {
        let mut control = 0;
        let mut revision = 0;
        success(unsafe { GetSecurityDescriptorControl(self.0, &mut control, &mut revision) })?;
        Ok(control)
    }

    fn dacl(&self) -> io::Result<(*mut ACL, bool, bool)> {
        let mut raw = ptr::null_mut();
        let mut present = 0;
        let mut defaulted = 0;
        success(unsafe {
            GetSecurityDescriptorDacl(self.0, &mut present, &mut raw, &mut defaulted)
        })?;
        Ok((raw, present != 0, defaulted != 0))
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
        Ok(DaclImage {
            control: self.control()?
                & (SE_DACL_PRESENT
                    | SE_DACL_DEFAULTED
                    | SE_DACL_PROTECTED
                    | SE_DACL_AUTO_INHERITED
                    | SE_DACL_AUTO_INHERIT_REQ),
            present,
            defaulted,
            aces,
        })
    }

    fn explicit_allowed_mask(&self, sid: &AppContainerSid) -> io::Result<u32> {
        let (acl, _, _) = self.dacl()?;
        let mut mask = 0;
        if !acl.is_null() {
            for index in 0..u32::from(unsafe { (*acl).AceCount }) {
                let mut raw = ptr::null_mut();
                success(unsafe { GetAce(acl, index, &mut raw) })?;
                // ACCESS_ALLOWED_ACE_TYPE is 0; this fixture contains only explicit
                // ordinary allow ACEs, so no object/callback ACE is decoded here.
                if unsafe { (*raw.cast::<ACE_HEADER>()).AceType } == 0 {
                    let ace = raw.cast::<ACCESS_ALLOWED_ACE>();
                    if unsafe { EqualSid(ptr::addr_of_mut!((*ace).SidStart).cast(), sid.as_psid()) }
                        != 0
                    {
                        mask |= unsafe { (*ace).Mask };
                    }
                }
            }
        }
        Ok(mask)
    }
}

impl Drop for Descriptor {
    fn drop(&mut self) {
        unsafe { LocalFree(self.0) };
    }
}

struct OwnedDirectory {
    path: PathBuf,
    retained: Option<(File, Descriptor)>,
    cleaned: bool,
}

impl OwnedDirectory {
    fn new() -> io::Result<Self> {
        let path = std::env::temp_dir().join(format!("klaus-walk-access-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&path)?;
        let mut fixture = Self {
            path,
            retained: None,
            cleaned: false,
        };
        let file = open_directory(&fixture.path, READ_CONTROL | WRITE_DAC)?;
        let original = Descriptor::read(&file)?;
        fixture.retained = Some((file, original));
        Ok(fixture)
    }

    fn image(&self) -> io::Result<DaclImage> {
        Descriptor::read(
            &self
                .retained
                .as_ref()
                .expect("retained restoration handle")
                .0,
        )?
        .image()
    }

    fn restore_and_remove(&mut self) -> io::Result<()> {
        if self.cleaned {
            return Ok(());
        }
        if let Some((file, original)) = &self.retained {
            let protection = if original.control()? & SE_DACL_PROTECTED != 0 {
                PROTECTED_DACL_SECURITY_INFORMATION
            } else {
                UNPROTECTED_DACL_SECURITY_INFORMATION
            };
            win32(unsafe {
                SetSecurityInfo(
                    file.as_raw_handle(),
                    SE_FILE_OBJECT,
                    DACL_SECURITY_INFORMATION | protection,
                    ptr::null_mut(),
                    ptr::null_mut(),
                    original.dacl()?.0,
                    ptr::null_mut(),
                )
            })?;
        }
        drop(self.retained.take());
        fs::remove_dir(&self.path)?;
        self.cleaned = true;
        Ok(())
    }
}

impl Drop for OwnedDirectory {
    fn drop(&mut self) {
        if let Err(error) = self.restore_and_remove() {
            eprintln!(
                "owned Walk fixture cleanup failed at {}: {error}",
                self.path.display()
            );
        }
    }
}

fn wide(value: &str) -> Vec<u16> {
    value.encode_utf16().chain(std::iter::once(0)).collect()
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

fn open_directory(path: &Path, access: u32) -> io::Result<File> {
    OpenOptions::new()
        .access_mode(access)
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
        .open(path)
}

fn sid_text(sid: &AppContainerSid) -> io::Result<String> {
    let mut raw = ptr::null_mut();
    success(unsafe { ConvertSidToStringSidW(sid.as_psid(), &mut raw) })?;
    let mut length = 0;
    // ConvertSidToStringSidW returns a NUL-terminated, LocalAlloc-owned string.
    while unsafe { *raw.add(length) } != 0 {
        length += 1;
    }
    let text = String::from_utf16(unsafe { std::slice::from_raw_parts(raw, length) });
    unsafe { LocalFree(raw.cast()) };
    text.map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

#[derive(Debug, Clone, Copy)]
enum ApplyApi {
    Apply,
    ApplyRecorded,
}

#[derive(Debug)]
struct Round {
    api: ApplyApi,
    applied: Result<(), String>,
    after_apply: DaclImage,
    revoked: Vec<String>,
    after_revoke: DaclImage,
}

#[derive(Debug)]
struct Observation {
    host_walk: bool,
    host_write_dac_error: Option<i32>,
    before: DaclImage,
    rounds: Vec<Round>,
}

fn exercise(grant_target: bool) -> Observation {
    let mut fixture = OwnedDirectory::new().unwrap();
    let observed = (|| -> io::Result<Observation> {
        let sid = derive_sid(&format!("klaus-walk-{}", uuid::Uuid::new_v4()))
            .map_err(io::Error::other)?;
        let mut sddl = format!("D:P(A;;RC;;;OW)(A;;0x{WALK_ACCESS:08x};;;WD)");
        if grant_target {
            sddl.push_str(&format!("(A;;0x{WALK_ACCESS:08x};;;{})", sid_text(&sid)?));
        }
        let descriptor = Descriptor::from_sddl(&sddl)?;
        success(unsafe {
            SetFileSecurityW(
                wide(&fixture.path.to_string_lossy()).as_ptr(),
                DACL_SECURITY_INFORMATION,
                descriptor.0,
            )
        })?;
        let before = fixture.image()?;
        let host_walk = open_directory(&fixture.path, WALK_ACCESS).is_ok();
        let host_write_dac_error = open_directory(&fixture.path, WRITE_DAC)
            .err()
            .and_then(|error| error.raw_os_error());
        let rules = [Rule::Allow {
            path: AbsPath::new(fixture.path.to_string_lossy()).map_err(io::Error::other)?,
            grant: Grant::Walk,
            inheritable: false,
        }];
        let mut rounds = Vec::new();
        for api in [ApplyApi::Apply, ApplyApi::ApplyRecorded] {
            let mut saved = Vec::new();
            let applied = match api {
                ApplyApi::Apply => acl::apply(&rules, &sid).map(|records| saved = records),
                ApplyApi::ApplyRecorded => acl::apply_recorded(&rules, &sid, &mut saved),
            }
            .map_err(|error| error.to_string());
            let after_apply = fixture.image()?;
            // The same public teardown call must preserve an authorization that
            // predated setup, including when a caller retains the complete plan.
            let revoked = acl::revoke(&rules, &saved, &sid)
                .into_iter()
                .map(|error| error.to_string())
                .collect();
            rounds.push(Round {
                api,
                applied,
                after_apply,
                revoked,
                after_revoke: fixture.image()?,
            });
        }
        Ok(Observation {
            host_walk,
            host_write_dac_error,
            before,
            rounds,
        })
    })();
    let cleanup = fixture.restore_and_remove();
    eprintln!("walk_effective_access target_granted={grant_target} observation={observed:?}");
    eprintln!(
        "owned_walk_fixture_cleanup={cleanup:?} removed={}",
        !fixture.path.exists()
    );
    assert!(cleanup.is_ok(), "owned fixture cleanup failed: {cleanup:?}");
    assert!(
        !fixture.path.exists(),
        "owned fixture directory remained after cleanup"
    );
    observed.expect("prepare and observe the owned Walk fixture")
}

fn assert_fixture(observed: &Observation) {
    assert!(
        observed.host_walk,
        "the host must already have the complete Walk permission"
    );
    assert_eq!(
        observed.host_write_dac_error,
        Some(ERROR_ACCESS_DENIED as i32),
        "the host must be refused a new WRITE_DAC handle"
    );
    assert_ne!(
        observed.before.control & SE_DACL_PROTECTED,
        0,
        "fixture DACL must be protected"
    );
}

#[test]
fn existing_target_walk_needs_no_write_dac_and_survives_revoke() {
    let observed = exercise(true);
    assert_fixture(&observed);
    for round in &observed.rounds {
        assert!(
            round.applied.is_ok(),
            "{:?} unnecessarily required WRITE_DAC: {:?}",
            round.api,
            round.applied
        );
        assert_eq!(
            round.after_apply, observed.before,
            "{:?} changed an existing authorization",
            round.api
        );
        assert!(
            round.revoked.is_empty(),
            "{:?} revoked an authorization it did not install: {:?}",
            round.api,
            round.revoked
        );
        assert_eq!(
            round.after_revoke, observed.before,
            "{:?} removed the original target grant",
            round.api
        );
    }
}

#[test]
fn host_walk_without_target_access_is_not_a_successful_grant() {
    let observed = exercise(false);
    assert_fixture(&observed);
    for round in &observed.rounds {
        assert!(
            round.applied.is_err(),
            "{:?} treated the host's access as the AppContainer's access",
            round.api
        );
        assert_eq!(
            round.after_apply, observed.before,
            "{:?} changed a write-protected descriptor",
            round.api
        );
        assert!(
            round.revoked.is_empty(),
            "{:?} tried to revoke a grant that never succeeded: {:?}",
            round.api,
            round.revoked
        );
        assert_eq!(
            round.after_revoke, observed.before,
            "{:?} changed the ungranted descriptor during rollback",
            round.api
        );
    }
}

#[derive(Debug)]
struct JournalRound {
    applied: Result<(), String>,
    target_after_apply: u32,
    revoked: Vec<String>,
    after_revoke: DaclImage,
    repeated_revoke: Vec<String>,
    after_repeated_revoke: DaclImage,
}

#[test]
fn wider_grant_restores_prior_sid_access_when_journal_is_reused() {
    const READ_ACCESS: u32 = 0x0012_00a9;
    const READ_WRITE_ACCESS: u32 = 0x0013_01ff;
    let mut fixture = OwnedDirectory::new().unwrap();
    let observed = (|| -> io::Result<_> {
        let sid = derive_sid(&format!("klaus-walk-journal-{}", uuid::Uuid::new_v4()))
            .map_err(io::Error::other)?;
        let descriptor = Descriptor::from_sddl(&format!(
            "D:P(A;;FA;;;OW)(A;;FA;;;WD)(A;;0x{READ_ACCESS:08x};;;{})",
            sid_text(&sid)?
        ))?;
        success(unsafe {
            SetFileSecurityW(
                wide(&fixture.path.to_string_lossy()).as_ptr(),
                DACL_SECURITY_INFORMATION,
                descriptor.0,
            )
        })?;
        let host_write_dac = open_directory(&fixture.path, WRITE_DAC).is_ok();
        let read_descriptor = || {
            Descriptor::read(
                &fixture
                    .retained
                    .as_ref()
                    .expect("retained restoration handle")
                    .0,
            )
        };
        let before = read_descriptor()?.image()?;
        let target_before = read_descriptor()?.explicit_allowed_mask(&sid)?;
        let rules = [Rule::Allow {
            path: AbsPath::new(fixture.path.to_string_lossy()).map_err(io::Error::other)?,
            grant: Grant::ReadWrite,
            inheritable: false,
        }];
        let mut saved = Vec::new();
        let mut rounds = Vec::new();
        for _ in 0..2 {
            // Reuse the same journal without clearing earlier rollback records.
            let applied =
                acl::apply_recorded(&rules, &sid, &mut saved).map_err(|error| error.to_string());
            let target_after_apply = read_descriptor()?.explicit_allowed_mask(&sid)?;
            let revoked = acl::revoke(&rules, &saved, &sid)
                .into_iter()
                .map(|error| error.to_string())
                .collect();
            let after_revoke = read_descriptor()?.image()?;
            let repeated_revoke = acl::revoke(&rules, &saved, &sid)
                .into_iter()
                .map(|error| error.to_string())
                .collect();
            rounds.push(JournalRound {
                applied,
                target_after_apply,
                revoked,
                after_revoke,
                repeated_revoke,
                after_repeated_revoke: read_descriptor()?.image()?,
            });
        }
        Ok((host_write_dac, target_before, before, rounds))
    })();
    let cleanup = fixture.restore_and_remove();
    eprintln!("walk_journal_reuse observation={observed:?}");
    eprintln!(
        "owned_walk_fixture_cleanup={cleanup:?} removed={}",
        !fixture.path.exists()
    );
    assert!(cleanup.is_ok(), "owned fixture cleanup failed: {cleanup:?}");
    assert!(
        !fixture.path.exists(),
        "owned journal fixture directory remained"
    );
    let (host_write_dac, target_before, before, rounds) = observed.expect("observe journal reuse");
    assert!(
        host_write_dac,
        "this fixture must permit the real ACL write"
    );
    assert_eq!(
        target_before, READ_ACCESS,
        "the original target grant must be Read only"
    );
    for (index, round) in rounds.iter().enumerate() {
        assert!(
            round.applied.is_ok(),
            "round {index} could not widen access: {:?}",
            round.applied
        );
        assert_eq!(
            round.target_after_apply & READ_WRITE_ACCESS,
            READ_WRITE_ACCESS,
            "round {index} did not install the requested wider target permission"
        );
        assert!(
            round.revoked.is_empty(),
            "round {index} rollback failed: {:?}",
            round.revoked
        );
        assert_eq!(
            round.after_revoke, before,
            "round {index} failed to restore the prior SID ACE"
        );
        assert!(
            round.repeated_revoke.is_empty(),
            "round {index} repeated rollback failed: {:?}",
            round.repeated_revoke
        );
        assert_eq!(
            round.after_repeated_revoke, round.after_revoke,
            "round {index} repeated rollback changed the restored descriptor"
        );
    }
}
