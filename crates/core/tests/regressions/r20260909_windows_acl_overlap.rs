//! Defect: global metadata protection blocks a later execution's explicit write
//! exception, and whole-DACL rollback cannot release overlapping grants.
//! Regression for the 2026-09-09 Windows ACL ownership work item.
//!
//! One exact, ignored case uses public ACL/profile/process APIs and real targets.
//! Only fresh fixture paths receive ACL writes. Planner ancestors are recorded
//! and excluded by an exact allowlist. No full setup, NUL, network, or system ACL
//! is used. Observations follow the owned workspace launch fixture. Final fixture
//! restoration is separate from production revoke and requires both Jobs drained.

#![cfg(windows)]

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::windows::fs::OpenOptionsExt;
use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle};
use std::path::PathBuf;
use std::ptr;
use std::thread;
use std::time::{Duration, Instant};

use klaus_sandbox_win::acl::{self, Grant, Rule};
use klaus_sandbox_win::identity::{self, OwnedAppContainerProfile};
use klaus_sandbox_win::process::{self, LaunchIdentity, LaunchSpec, Target};
use klaus_sandbox_win::{AbsPath, FilesystemBoundary};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use windows_sys::Win32::Foundation::{DuplicateHandle, LocalFree, DUPLICATE_SAME_ACCESS, HANDLE};
use windows_sys::Win32::Security::Authorization::{
    ConvertSidToStringSidW, GetSecurityInfo, SetSecurityInfo, SE_FILE_OBJECT,
};
use windows_sys::Win32::Security::{
    GetAce, GetSecurityDescriptorControl, GetSecurityDescriptorDacl, GetSidSubAuthority,
    GetSidSubAuthorityCount, GetTokenInformation, IsValidSid, TokenAppContainerSid,
    TokenCapabilities, TokenElevation, TokenIntegrityLevel, TokenIsAppContainer, TokenSessionId,
    ACE_HEADER, ACL, DACL_SECURITY_INFORMATION, PROTECTED_DACL_SECURITY_INFORMATION,
    PSECURITY_DESCRIPTOR, SE_DACL_PROTECTED, TOKEN_APPCONTAINER_INFORMATION, TOKEN_ELEVATION,
    TOKEN_INFORMATION_CLASS, TOKEN_MANDATORY_LABEL, TOKEN_QUERY,
    UNPROTECTED_DACL_SECURITY_INFORMATION,
};
use windows_sys::Win32::Storage::FileSystem::{
    FILE_FLAG_BACKUP_SEMANTICS, FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE, READ_CONTROL,
    WRITE_DAC,
};
use windows_sys::Win32::System::JobObjects::IsProcessInJob;
use windows_sys::Win32::System::Pipes::{CreatePipe, PeekNamedPipe};
use windows_sys::Win32::System::Threading::{
    GetCurrentProcess, GetCurrentThread, OpenProcessToken, OpenThreadToken,
};

use super::r20260909_windows_sandbox_launch::set_error_mode;

const ENABLE: &str = "KLAUS_WINDOWS_ACL_OVERLAP_EXPERIMENT";
const ROLE: &str = "KLAUS_WINDOWS_ACL_OVERLAP_ROLE";
const ROOT: &str = "KLAUS_WINDOWS_ACL_OVERLAP_ROOT";
const PROFILE: &str = "KLAUS_WINDOWS_ACL_OVERLAP_PROFILE";
const TEST: &str = "overlapping_metadata_write_permission_preserves_each_execution";
const PREFIX: &[u8] = b"KLAUS_ACL_OVERLAP_JSON=";
const FILES: [&str; 3] = ["data.txt", ".klaus/metadata.txt", ".git/metadata.txt"];
const PAYLOAD: &[u8] = b"mark";
type ProbeResult<T> = Result<T, String>;

fn text(error: impl std::fmt::Display) -> String {
    error.to_string()
}
fn success(ok: i32) -> io::Result<()> {
    if ok == 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}
fn status(code: u32) -> io::Result<()> {
    if code == 0 {
        Ok(())
    } else {
        Err(io::Error::from_raw_os_error(code as i32))
    }
}
fn test_name() -> String {
    let module = module_path!()
        .split_once("::")
        .map_or(module_path!(), |(_, suffix)| suffix);
    format!("{module}::{TEST}")
}

fn sid_string(sid: *mut std::ffi::c_void) -> io::Result<String> {
    if sid.is_null() || unsafe { IsValidSid(sid) } == 0 {
        return Err(io::Error::other("invalid token SID"));
    }
    let mut raw = ptr::null_mut();
    success(unsafe { ConvertSidToStringSidW(sid, &mut raw) })?;
    let result = (|| {
        if raw.is_null() {
            return Err(io::Error::other("SID conversion returned null"));
        }
        let length = (0..256)
            .find(|index| unsafe { *raw.add(*index) == 0 })
            .ok_or_else(|| io::Error::other("SID text exceeds bound"))?;
        String::from_utf16(unsafe { std::slice::from_raw_parts(raw, length) })
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
    })();
    unsafe { LocalFree(raw.cast()) };
    result
}

fn information(
    token: HANDLE,
    class: TOKEN_INFORMATION_CLASS,
    minimum: usize,
) -> io::Result<Vec<usize>> {
    let mut needed = 0;
    let probe = unsafe { GetTokenInformation(token, class, ptr::null_mut(), 0, &mut needed) };
    let probe_error = io::Error::last_os_error();
    // TokenElevation reports ERROR_BAD_LENGTH (24) for a zero-size buffer.
    // Both sizing errors still require a bounded length and a successful data read.
    if probe != 0
        || !matches!(probe_error.raw_os_error(), Some(24 | 122))
        || (needed as usize) < minimum
        || needed > 65_536
    {
        return Err(io::Error::other(format!(
            "invalid token-information size probe: class={class}, result={probe}, win32={:?}, needed={needed}, minimum={minimum}",
            probe_error.raw_os_error(),
        )));
    }
    let capacity = needed;
    let mut bytes = vec![0usize; (needed as usize).div_ceil(std::mem::size_of::<usize>())];
    success(unsafe {
        GetTokenInformation(
            token,
            class,
            bytes.as_mut_ptr().cast(),
            capacity,
            &mut needed,
        )
    })?;
    if needed > capacity || (needed as usize) < minimum {
        return Err(io::Error::other("token-information length changed"));
    }
    Ok(bytes)
}

#[derive(Debug, Serialize)]
struct Token {
    app_container: bool,
    package_sid: Option<String>,
    integrity_rid: u32,
    elevated: bool,
    session_id: u32,
    capability_count: Option<u32>,
}

fn inspect_token(process: HANDLE) -> io::Result<Token> {
    let mut raw = ptr::null_mut();
    success(unsafe { OpenProcessToken(process, TOKEN_QUERY, &mut raw) })?;
    let token = unsafe { OwnedHandle::from_raw_handle(raw) };
    let scalar = |class| -> io::Result<u32> {
        let value = information(token.as_raw_handle(), class, 4)?;
        Ok(unsafe { *value.as_ptr().cast::<u32>() })
    };
    let app_container = scalar(TokenIsAppContainer)? != 0;
    let package_sid = if app_container {
        let value = information(
            token.as_raw_handle(),
            TokenAppContainerSid,
            std::mem::size_of::<TOKEN_APPCONTAINER_INFORMATION>(),
        )?;
        Some(sid_string(unsafe {
            (*value.as_ptr().cast::<TOKEN_APPCONTAINER_INFORMATION>()).TokenAppContainer
        })?)
    } else {
        None
    };
    let label = information(
        token.as_raw_handle(),
        TokenIntegrityLevel,
        std::mem::size_of::<TOKEN_MANDATORY_LABEL>(),
    )?;
    let sid = unsafe { (*label.as_ptr().cast::<TOKEN_MANDATORY_LABEL>()).Label.Sid };
    if sid.is_null() || unsafe { IsValidSid(sid) } == 0 {
        return Err(io::Error::other("invalid integrity SID"));
    }
    let count = unsafe { *GetSidSubAuthorityCount(sid) };
    if count == 0 {
        return Err(io::Error::other("integrity SID has no RID"));
    }
    let elevation = information(
        token.as_raw_handle(),
        TokenElevation,
        std::mem::size_of::<TOKEN_ELEVATION>(),
    )?;
    Ok(Token {
        app_container,
        package_sid,
        integrity_rid: unsafe { *GetSidSubAuthority(sid, u32::from(count - 1)) },
        elevated: unsafe { (*elevation.as_ptr().cast::<TOKEN_ELEVATION>()).TokenIsElevated } != 0,
        session_id: scalar(TokenSessionId)?,
        capability_count: if app_container {
            Some(scalar(TokenCapabilities)?)
        } else {
            None
        },
    })
}

fn ordinary_controller() -> ProbeResult<Token> {
    let token = inspect_token(unsafe { GetCurrentProcess() }).map_err(text)?;
    let mut thread_token = ptr::null_mut();
    if unsafe { OpenThreadToken(GetCurrentThread(), TOKEN_QUERY, 1, &mut thread_token) } != 0 {
        drop(unsafe { OwnedHandle::from_raw_handle(thread_token) });
        return Err("controller must not impersonate".into());
    }
    if io::Error::last_os_error().raw_os_error() != Some(1008)
        || token.app_container
        || token.elevated
        || token.integrity_rid != 8192
        || token.session_id == 0
    {
        return Err(format!(
            "ordinary interactive medium-integrity controller required: {token:?}"
        ));
    }
    Ok(token)
}

#[derive(Debug, Serialize, Deserialize)]
enum RawIo {
    Ok(Vec<u8>),
    Error {
        code: Option<i32>,
        kind: String,
        message: String,
    },
}
fn observed(result: io::Result<Vec<u8>>) -> RawIo {
    match result {
        Ok(bytes) => RawIo::Ok(bytes),
        Err(error) => RawIo::Error {
            code: error.raw_os_error(),
            kind: format!("{:?}", error.kind()),
            message: error.to_string(),
        },
    }
}
#[derive(Debug, Serialize, Deserialize)]
struct Access {
    path: String,
    read: RawIo,
    write: RawIo,
    after: RawIo,
}

fn target() -> ProbeResult<()> {
    let token = inspect_token(unsafe { GetCurrentProcess() }).map_err(text)?;
    let sid = identity::derive_sid(&std::env::var(PROFILE).map_err(text)?).map_err(text)?;
    if !token.app_container
        || token.package_sid != Some(sid_string(sid.as_psid()).map_err(text)?)
        || token.integrity_rid != 4096
        || token.elevated
        || token.capability_count != Some(0)
    {
        return Err(format!("target identity mismatch: {token:?}"));
    }
    let workspace = PathBuf::from(std::env::var(ROOT).map_err(text)?).join("workspace");
    loop {
        let mut command = [0];
        io::stdin().read_exact(&mut command).map_err(text)?;
        if command != *b"p" {
            return Err("unexpected probe command".into());
        }
        let checks: Vec<_> = FILES
            .iter()
            .map(|name| {
                let path = workspace.join(name);
                Access {
                    path: (*name).into(),
                    read: observed(fs::read(&path)),
                    write: observed(
                        OpenOptions::new()
                            .write(true)
                            .open(&path)
                            .and_then(|mut file| file.write_all(PAYLOAD))
                            .map(|()| Vec::new()),
                    ),
                    after: observed(fs::read(&path)),
                }
            })
            .collect();
        let mut output = io::stdout().lock();
        output.write_all(b"\n").map_err(text)?;
        output.write_all(PREFIX).map_err(text)?;
        serde_json::to_writer(&mut output, &checks).map_err(text)?;
        output.write_all(b"\n").map_err(text)?;
        output.flush().map_err(text)?;
    }
}

fn pipe() -> io::Result<(File, File)> {
    let (mut read, mut write) = (ptr::null_mut(), ptr::null_mut());
    success(unsafe { CreatePipe(&mut read, &mut write, ptr::null(), 4096) })?;
    Ok(unsafe { (File::from_raw_handle(read), File::from_raw_handle(write)) })
}
fn inheritable(file: &File) -> io::Result<File> {
    let mut raw = ptr::null_mut();
    success(unsafe {
        DuplicateHandle(
            GetCurrentProcess(),
            file.as_raw_handle(),
            GetCurrentProcess(),
            &mut raw,
            0,
            1,
            DUPLICATE_SAME_ACCESS,
        )
    })?;
    Ok(unsafe { File::from_raw_handle(raw) })
}

struct Probe {
    target: Option<Target>,
    input: File,
    output: File,
    buffered: Vec<u8>,
    drained: bool,
}
impl Probe {
    fn poll_output(&mut self) -> ProbeResult<bool> {
        let mut available = 0;
        if unsafe {
            PeekNamedPipe(
                self.output.as_raw_handle(),
                ptr::null_mut(),
                0,
                ptr::null_mut(),
                &mut available,
                ptr::null_mut(),
            )
        } == 0
        {
            let error = io::Error::last_os_error();
            return if error.kind() == io::ErrorKind::BrokenPipe {
                Ok(true)
            } else {
                Err(error.to_string())
            };
        }
        if available != 0 {
            let mut bytes = [0; 4096];
            let count = self
                .output
                .read(&mut bytes[..(available as usize).min(4096)])
                .map_err(text)?;
            if count == 0 {
                return Ok(true);
            }
            self.buffered.extend_from_slice(&bytes[..count]);
            if self.buffered.len() > 65_536 {
                return Err("target output exceeded 64 KiB".into());
            }
        }
        Ok(false)
    }

    fn read_round(&mut self) -> ProbeResult<Vec<Access>> {
        // A single byte in the owned empty control pipe cannot fill its buffer.
        self.input.write_all(b"p").map_err(text)?;
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            while let Some(end) = self.buffered.iter().position(|byte| *byte == b'\n') {
                let line: Vec<_> = self.buffered.drain(..=end).collect();
                if let Some(raw) = line.strip_prefix(PREFIX) {
                    return serde_json::from_slice(raw).map_err(text);
                }
            }
            if Instant::now() >= deadline {
                return Err("target probe exceeded 5000 ms".into());
            }
            if self.poll_output()? {
                return Err("target output ended before its report".into());
            }
            thread::sleep(Duration::from_millis(5));
        }
    }

    fn drain(&mut self) -> ProbeResult<()> {
        if self.drained {
            return Ok(());
        }
        self.target
            .as_ref()
            .expect("owned target")
            .terminate_and_wait()
            .map_err(text)?;
        let deadline = Instant::now() + Duration::from_secs(2);
        while !self.poll_output()? {
            if Instant::now() >= deadline {
                return Err("owned output did not reach EOF after Job drain".into());
            }
            thread::sleep(Duration::from_millis(5));
        }
        self.drained = true;
        Ok(())
    }
}
impl Drop for Probe {
    fn drop(&mut self) {
        if !self.drained {
            // A destructor is not a drain receipt. Retain the original native
            // process/Job owners until controller exit after unconfirmed cleanup.
            if let Some(target) = self.target.take() {
                std::mem::forget(target);
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq, Serialize)]
struct DaclImage {
    protected: bool,
    present: bool,
    aces: Option<Vec<Vec<u8>>>,
}
struct Descriptor(PSECURITY_DESCRIPTOR);
impl Descriptor {
    fn read(file: &File) -> io::Result<Self> {
        let mut raw = ptr::null_mut();
        status(unsafe {
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
    fn dacl(&self) -> io::Result<(*mut ACL, bool)> {
        let (mut raw, mut present, mut defaulted) = (ptr::null_mut(), 0, 0);
        success(unsafe {
            GetSecurityDescriptorDacl(self.0, &mut present, &mut raw, &mut defaulted)
        })?;
        Ok((raw, present != 0))
    }
    fn image(&self) -> io::Result<DaclImage> {
        let (raw, present) = self.dacl()?;
        let (mut control, mut revision) = (0, 0);
        success(unsafe { GetSecurityDescriptorControl(self.0, &mut control, &mut revision) })?;
        let aces = if raw.is_null() {
            None
        } else {
            let mut entries = Vec::new();
            for index in 0..u32::from(unsafe { (*raw).AceCount }) {
                let mut ace = ptr::null_mut();
                success(unsafe { GetAce(raw, index, &mut ace) })?;
                let size = usize::from(unsafe { (*ace.cast::<ACE_HEADER>()).AceSize });
                entries
                    .push(unsafe { std::slice::from_raw_parts(ace.cast::<u8>(), size) }.to_vec());
            }
            Some(entries)
        };
        Ok(DaclImage {
            protected: control & SE_DACL_PROTECTED != 0,
            present,
            aces,
        })
    }
}
impl Drop for Descriptor {
    fn drop(&mut self) {
        unsafe { LocalFree(self.0) };
    }
}

// No restoring Drop: explicit Job and pipe drainage gates every restoration.
struct Restore {
    path: PathBuf,
    handle: File,
    descriptor: Descriptor,
    before: DaclImage,
}
impl Restore {
    fn retain(path: PathBuf) -> io::Result<Self> {
        let handle = OpenOptions::new()
            .access_mode(READ_CONTROL | WRITE_DAC)
            .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE)
            .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
            .open(&path)?;
        let descriptor = Descriptor::read(&handle)?;
        let before = descriptor.image()?;
        Ok(Self {
            path,
            handle,
            descriptor,
            before,
        })
    }
    fn current(&self) -> io::Result<DaclImage> {
        Descriptor::read(&self.handle)?.image()
    }
    fn restore(&self) -> io::Result<()> {
        if self.current()? == self.before {
            return Ok(());
        }
        let protection = if self.before.protected {
            PROTECTED_DACL_SECURITY_INFORMATION
        } else {
            UNPROTECTED_DACL_SECURITY_INFORMATION
        };
        status(unsafe {
            SetSecurityInfo(
                self.handle.as_raw_handle(),
                SE_FILE_OBJECT,
                DACL_SECURITY_INFORMATION | protection,
                ptr::null_mut(),
                ptr::null_mut(),
                self.descriptor.dacl()?.0,
                ptr::null_mut(),
            )
        })
    }
}

#[derive(Default)]
struct Lease {
    rules: Vec<Rule>,
    journal: Vec<acl::SavedDacl>,
}
impl Lease {
    fn apply(&mut self, rules: Vec<Rule>, profile: &OwnedAppContainerProfile) -> ProbeResult<()> {
        self.rules = rules;
        // Preserve partial journals instead of rolling back while a peer target
        // is active. Final recovery waits for both owned Jobs and pipe EOFs.
        acl::apply_recorded(
            &self.rules,
            profile.sid().ok_or("profile SID absent")?,
            &mut self.journal,
        )
        .map_err(text)
    }
    fn revoke(&self, profile: &OwnedAppContainerProfile) -> Vec<String> {
        if self.journal.is_empty() {
            return Vec::new();
        }
        acl::revoke(
            &self.rules,
            &self.journal,
            profile.sid().expect("live profile SID"),
        )
        .into_iter()
        .map(text)
        .collect()
    }
}

struct Fixture {
    root: PathBuf,
    profiles: Vec<OwnedAppContainerProfile>,
    originals: Vec<Restore>,
    support: [Lease; 2],
    leases: [Lease; 2],
    targets: [Option<Probe>; 2],
    unknown_spawn: bool,
    violations: Vec<String>,
}
impl Fixture {
    fn event(&self, phase: &str, value: Value) -> ProbeResult<()> {
        let mut file = OpenOptions::new()
            .append(true)
            .open(self.root.join("observations.jsonl"))
            .map_err(text)?;
        serde_json::to_writer(&mut file, &json!({"phase": phase, "value": value})).map_err(text)?;
        file.write_all(b"\n").map_err(text)?;
        file.sync_all().map_err(text)?;
        let _ = writeln!(
            io::stderr().lock(),
            "acl_overlap_phase={phase};evidence={:?}",
            self.root.join("observations.jsonl")
        );
        Ok(())
    }
    fn acl_images(&self) -> Value {
        json!(self.originals.iter().map(|entry| json!({
            "path": entry.path, "before": entry.before, "current": entry.current().map_err(text),
        })).collect::<Vec<_>>())
    }
    fn prepare(&mut self) -> ProbeResult<()> {
        for relative in ["", "bin", "workspace", "workspace/.klaus", "workspace/.git"] {
            let path = self.root.join(relative);
            if !relative.is_empty() {
                fs::create_dir(&path).map_err(text)?;
            }
            self.originals.push(Restore::retain(path).map_err(text)?);
        }
        for relative in FILES {
            let path = self.root.join("workspace").join(relative);
            fs::write(&path, b"seed").map_err(text)?;
            self.originals.push(Restore::retain(path).map_err(text)?);
        }
        let image = self.root.join("bin/probe.exe");
        fs::copy(std::env::current_exe().map_err(text)?, &image).map_err(text)?;
        self.originals.push(Restore::retain(image).map_err(text)?);
        self.event("original-owned-acls", self.acl_images())?;
        for _ in 0..2 {
            let name = format!("klaus-exec-{}", uuid::Uuid::new_v4().simple());
            self.event("profile-create-planned", json!({"name": name}))?;
            match OwnedAppContainerProfile::create(&name) {
                Ok(profile) => self.profiles.push(profile),
                Err(error) => {
                    let (error, recovery) = error.into_parts();
                    if let Some(profile) = recovery {
                        self.profiles.push(profile);
                    }
                    return Err(error.to_string());
                }
            }
            let profile = self.profiles.last().expect("profile just created");
            self.event(
                "profile-created",
                json!({
                    "name": profile.name(), "creatorUserSid": profile.creator_user_sid(),
                }),
            )?;
        }
        for index in 0..2 {
            let rules = [self.root.clone(), self.root.join("bin")]
                .into_iter()
                .enumerate()
                .map(|(position, path)| {
                    Ok(Rule::Allow {
                        path: AbsPath::new(path.to_string_lossy()).map_err(text)?,
                        grant: Grant::Read,
                        inheritable: position == 1,
                    })
                })
                .collect::<ProbeResult<Vec<_>>>()?;
            self.event(
                "support-read-rules",
                json!({"execution": index, "rules": format!("{rules:?}")}),
            )?;
            self.support[index].apply(rules, &self.profiles[index])?;
        }
        Ok(())
    }
    fn workspace_rules(&self, index: usize) -> ProbeResult<Vec<Rule>> {
        let workspace =
            AbsPath::new(self.root.join("workspace").to_string_lossy()).map_err(text)?;
        let metadata = [
            workspace.join(".klaus").map_err(text)?,
            workspace.join(".git").map_err(text)?,
        ];
        let boundary = FilesystemBoundary {
            writable: vec![workspace.clone()],
            protected_metadata: vec![".klaus".into(), ".git".into()],
            metadata_write_allow: if index == 1 {
                metadata.to_vec()
            } else {
                Vec::new()
            },
            ..FilesystemBoundary::default()
        };
        let allowed = [workspace, metadata[0].clone(), metadata[1].clone()];
        let (applied, excluded): (Vec<_>, Vec<_>) = acl::plan_entries(&boundary)
            .into_iter()
            .partition(|rule| allowed.contains(rule.path()));
        if applied.is_empty() || applied.iter().any(|rule| !allowed.contains(rule.path())) {
            return Err("ACL rule escaped the exact owned-path allowlist".into());
        }
        self.event("workspace-plan", json!({
            "execution": index, "boundary": format!("{boundary:?}"),
            "appliedOwnedRules": format!("{applied:?}"), "excludedRulesNeverApplied": format!("{excluded:?}"),
            "metadataWithholdOmitted": !applied.iter().any(|rule| matches!(rule, Rule::Withhold { .. })),
        }))?;
        Ok(applied)
    }
    fn launch(&mut self, index: usize, session: u32) -> ProbeResult<()> {
        let profile = &self.profiles[index];
        let sid = profile.sid().ok_or("profile SID absent")?;
        let expected = sid_string(sid.as_psid()).map_err(text)?;
        let (input, input_writer) = pipe().map_err(text)?;
        let (output, output_writer) = pipe().map_err(text)?;
        let child_input = inheritable(&input).map_err(text)?;
        let child_output = inheritable(&output_writer).map_err(text)?;
        drop(input);
        drop(output_writer);
        let mut environment: BTreeMap<String, String> = std::env::vars().collect();
        environment.insert(ROLE.into(), "target".into());
        environment.insert(ROOT.into(), self.root.to_string_lossy().into_owned());
        environment.insert(PROFILE.into(), profile.name().into());
        let command = process::command_line(&[
            self.root
                .join("bin/probe.exe")
                .to_string_lossy()
                .into_owned(),
            "--exact".into(),
            test_name(),
            "--ignored".into(),
            "--nocapture".into(),
            "--test-threads=1".into(),
        ]);
        self.event(
            "target-launch-planned",
            json!({
                "execution": index, "profile": profile.name(), "packageSid": expected,
                "command": command,
            }),
        )?;
        let launched = process::launch(LaunchSpec {
            command_line: command,
            working_directory: self.root.join("workspace").to_string_lossy().into_owned(),
            environment: process::environment_block(&environment),
            identity: LaunchIdentity::AppContainer {
                container: sid,
                capabilities: &[],
            },
            inherit: vec![
                child_input.as_raw_handle() as isize,
                child_output.as_raw_handle() as isize,
            ],
            stdin: child_input.as_raw_handle() as isize,
            stdout: child_output.as_raw_handle() as isize,
            stderr: child_output.as_raw_handle() as isize,
            pseudoconsole: None,
        });
        drop(child_input);
        drop(child_output);
        let target = match launched {
            Ok(target) => target,
            Err(error) => {
                self.unknown_spawn = true;
                return Err(format!("launch returned no confirmed drain owner: {error}"));
            }
        };
        self.targets[index] = Some(Probe {
            target: Some(target),
            input: input_writer,
            output,
            buffered: Vec::new(),
            drained: false,
        });
        let target = self.targets[index]
            .as_ref()
            .unwrap()
            .target
            .as_ref()
            .unwrap();
        let token = inspect_token(target.process_handle() as HANDLE).map_err(text)?;
        let mut member = 0;
        success(unsafe {
            IsProcessInJob(
                target.process_handle() as HANDLE,
                target.tree_handle() as HANDLE,
                &mut member,
            )
        })
        .map_err(text)?;
        self.event("target-identity", json!({"execution": index, "profile": self.profiles[index].name(), "expectedSid": expected,
            "token": token, "exactJobMember": member != 0}))?;
        if !token.app_container
            || token.package_sid.as_deref() != Some(expected.as_str())
            || token.integrity_rid != 4096
            || token.elevated
            || token.session_id != session
            || token.capability_count != Some(0)
            || member == 0
        {
            return Err("target token or exact Job differs from requested identity".into());
        }
        Ok(())
    }
    fn check(&mut self, index: usize, phase: &str) -> ProbeResult<()> {
        let accesses = self.targets[index]
            .as_mut()
            .ok_or("target absent")?
            .read_round()?;
        self.event(
            phase,
            json!({"execution": index, "accesses": accesses, "acls": self.acl_images()}),
        )?;
        if accesses.len() != FILES.len() {
            return Err("incomplete target access report".into());
        }
        for (access, expected_path) in accesses.iter().zip(FILES) {
            let writable = index == 1 || expected_path == "data.txt";
            let valid_write = if writable {
                matches!(&access.write, RawIo::Ok(bytes) if bytes.is_empty())
                    && matches!(&access.after, RawIo::Ok(bytes) if bytes == PAYLOAD)
            } else {
                matches!(&access.write, RawIo::Error { code: Some(5), .. })
            };
            if access.path != expected_path || !matches!(&access.read, RawIo::Ok(_)) || !valid_write
            {
                self.violations.push(format!("{phase}: execution {index} {expected_path} expected writable={writable}: {access:?}"));
            }
        }
        Ok(())
    }
    fn exercise(&mut self, session: u32) -> ProbeResult<()> {
        self.prepare()?;
        for index in 0..2 {
            let rules = self.workspace_rules(index)?;
            self.leases[index].apply(rules, &self.profiles[index])?;
            self.event(
                "workspace-lease-acquired",
                json!({"execution": index, "acls": self.acl_images()}),
            )?;
            self.launch(index, session)?;
            if index == 0 {
                self.check(0, "a-before-b")?;
            }
        }
        self.check(1, "b-overlapping-a")?;
        self.check(0, "a-after-b-started")?;
        self.targets[0].as_mut().unwrap().drain()?;
        let revoked = self.leases[0].revoke(&self.profiles[0]);
        self.event("a-lease-released-while-b-active", json!({"aJobAndOutputDrained": true, "revokeErrors": revoked, "acls": self.acl_images()}))?;
        if !revoked.is_empty() {
            self.violations
                .push(format!("A overlapping revoke failed: {revoked:?}"));
        }
        self.check(1, "b-after-a-release")?;
        self.targets[1].as_mut().unwrap().drain()?;
        let revoked = self.leases[1].revoke(&self.profiles[1]);
        self.event("b-lease-released", json!({"bJobAndOutputDrained": true, "revokeErrors": revoked, "acls": self.acl_images()}))?;
        if !revoked.is_empty() {
            self.violations
                .push(format!("B revoke failed: {revoked:?}"));
        }
        for original in self
            .originals
            .iter()
            .filter(|entry| entry.path.starts_with(self.root.join("workspace")))
        {
            if original.current().map_err(text)? != original.before {
                self.violations.push(format!(
                    "production lease cleanup left changed ACL: {:?}",
                    original.path
                ));
            }
        }
        Ok(())
    }
    fn cleanup(&mut self) -> ProbeResult<()> {
        let drains: Vec<_> = self
            .targets
            .iter_mut()
            .map(|target| target.as_mut().map(Probe::drain))
            .collect();
        self.event(
            "final-job-and-output-drain",
            json!({"results": drains, "unknownSpawn": self.unknown_spawn}),
        )?;
        if self.unknown_spawn || drains.iter().flatten().any(Result::is_err) {
            return Err("unconfirmed drain: profiles, ACLs, targets, and fixture retained".into());
        }
        // Recovery only: these retries do not erase earlier revoke failures.
        for index in (0..self.profiles.len().min(2)).rev() {
            if self.profiles[index].sid().is_some() {
                let workspace = self.leases[index].revoke(&self.profiles[index]);
                let support = self.support[index].revoke(&self.profiles[index]);
                self.event(
                    "post-drain-revoke-retry",
                    json!({"execution": index, "workspace": workspace, "support": support}),
                )?;
            }
        }
        let restored: Vec<_> = self
            .originals
            .iter()
            .map(|entry| (entry.path.clone(), entry.restore().map_err(text)))
            .collect();
        let exact = self
            .originals
            .iter()
            .all(|entry| entry.current().is_ok_and(|image| image == entry.before));
        self.event(
            "fixture-restore-after-all-jobs-drained",
            json!({"restored": restored, "exact": exact, "acls": self.acl_images()}),
        )?;
        if !exact || restored.iter().any(|(_, result)| result.is_err()) {
            return Err("fixture DACL restoration failed; evidence and profiles retained".into());
        }
        let closed: Vec<_> = self
            .profiles
            .iter_mut()
            .map(|profile| {
                let name = profile.name().to_owned();
                (name, profile.close().map_err(text))
            })
            .collect();
        self.event("profile-cleanup-after-dacl-restore", json!(&closed))?;
        if closed.iter().any(|(_, result)| result.is_err()) {
            return Err("owned profile cleanup failed".into());
        }
        Ok(())
    }
}

fn controller() -> ProbeResult<()> {
    let token = ordinary_controller()?;
    // Only this exact test process and its children suppress native crash dialogs.
    unsafe { set_error_mode(0x0001 | 0x0002 | 0x8000) };
    let root = std::env::temp_dir().join(format!("klaus-acl-overlap-{}", uuid::Uuid::new_v4()));
    fs::create_dir(&root).map_err(text)?;
    File::create(root.join("observations.jsonl")).map_err(text)?;
    let mut fixture = Fixture {
        root,
        profiles: Vec::new(),
        originals: Vec::new(),
        support: Default::default(),
        leases: Default::default(),
        targets: Default::default(),
        unknown_spawn: false,
        violations: Vec::new(),
    };
    fixture.event("controller", json!(&token))?;
    let operation = fixture.exercise(token.session_id);
    let cleanup = fixture.cleanup();
    let result = json!({"operation": operation, "cleanup": cleanup, "violations": fixture.violations,
        "profiles": fixture.profiles.iter().map(|profile| json!({"name": profile.name(), "owned": profile.is_owned(), "creatorUserSid": profile.creator_user_sid()})).collect::<Vec<_>>()});
    let recorded = fixture.event("result", result.clone());
    let _ = writeln!(
        io::stderr().lock(),
        "acl_overlap_result={result};recorded={recorded:?};fixture={:?}",
        fixture.root
    );
    if operation.is_err() || cleanup.is_err() || !fixture.violations.is_empty() || recorded.is_err()
    {
        let error = format!(
            "ACL overlap regression failed; evidence retained at {:?}: {result}",
            fixture.root
        );
        if cleanup.is_err() {
            // Keep every original handle, journal and profile owner after an
            // unconfirmed cleanup stage, including an unknown launch outcome.
            std::mem::forget(fixture);
        }
        return Err(error);
    }
    fixture.targets = Default::default();
    fixture.originals.clear();
    fs::remove_dir_all(&fixture.root).map_err(text)
}

#[test]
#[ignore = "owned Windows ACL overlap regression; exact selection and KLAUS_WINDOWS_ACL_OVERLAP_EXPERIMENT=1 required"]
fn overlapping_metadata_write_permission_preserves_each_execution() {
    assert_eq!(
        std::env::var(ENABLE).ok().as_deref(),
        Some("1"),
        "explicit native ACL opt-in required"
    );
    let arguments: Vec<_> = std::env::args().collect();
    assert!(
        arguments.iter().any(|argument| argument == "--exact")
            && arguments
                .iter()
                .filter(|argument| *argument == &test_name())
                .count()
                == 1,
        "exact ignored test selection is required before creating resources"
    );
    match std::env::var(ROLE).ok().as_deref() {
        Some("target") => {
            let result = target();
            if let Err(error) = &result {
                let _ = writeln!(io::stderr().lock(), "acl_overlap_target_error={error}");
            }
            std::process::exit(if result.is_ok() { 7 } else { 2 });
        }
        None => {
            let result = controller();
            assert!(result.is_ok(), "{result:?}");
        }
        Some(role) => panic!("unexpected ACL overlap role: {role}"),
    }
}
