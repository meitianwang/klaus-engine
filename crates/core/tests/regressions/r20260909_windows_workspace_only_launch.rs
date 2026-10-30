//! Experiment: distinguish wrapper preparation from AppContainer file access.
//! The 2026-09-09 Windows preparation investigation observed denied drive-root
//! Walk and NUL WRITE_DAC. This case does not weaken that production boundary.
//! It exercises public ACL and process APIs with only owned workspace grants,
//! explicit pipe inheritance, and no network capabilities. Reopening NUL is an
//! observation, never a pass condition or a request to change its descriptor.
//!
//! This is deliberately ignored and requires both an exact test selection and
//! KLAUS_WINDOWS_WORKSPACE_ONLY_EXPERIMENT=1 before creating any resource. The
//! existing outer worker Job/profile controller is reused. The controller holds
//! restoration handles and the ACL journal until the worker tree has stopped.
//! No system ACL, mandatory label, loopback exemption, WFP object, or execution
//! policy is changed. A failure to initialize the copied test image is evidence,
//! not permission to grant another path or silently substitute a different target.

#![cfg(windows)]

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::windows::fs::OpenOptionsExt;
use std::os::windows::io::{AsRawHandle, FromRawHandle};
use std::path::{Path, PathBuf};
use std::ptr;
use std::thread;
use std::time::{Duration, Instant};

use klaus_sandbox_win::acl::{self, Grant, Rule};
use klaus_sandbox_win::boundary::AbsPath;
use klaus_sandbox_win::identity::{self, AppContainerSid};
use klaus_sandbox_win::process::{self, LaunchIdentity, LaunchSpec};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use windows_sys::Win32::Foundation::{DuplicateHandle, LocalFree, DUPLICATE_SAME_ACCESS, HANDLE};
use windows_sys::Win32::Security::Authorization::{
    ConvertSidToStringSidW, GetSecurityInfo, SetSecurityInfo, SE_FILE_OBJECT,
};
use windows_sys::Win32::Security::{
    GetAce, GetSecurityDescriptorControl, GetSecurityDescriptorDacl, GetSidSubAuthority,
    GetSidSubAuthorityCount, GetTokenInformation, TokenAppContainerSid, TokenElevation,
    TokenIntegrityLevel, TokenIsAppContainer, TokenSessionId, ACE_HEADER, ACL,
    DACL_SECURITY_INFORMATION, PROTECTED_DACL_SECURITY_INFORMATION, PSECURITY_DESCRIPTOR,
    SE_DACL_PROTECTED, TOKEN_APPCONTAINER_INFORMATION, TOKEN_ELEVATION, TOKEN_INFORMATION_CLASS,
    TOKEN_MANDATORY_LABEL, TOKEN_QUERY, UNPROTECTED_DACL_SECURITY_INFORMATION,
};
use windows_sys::Win32::Storage::FileSystem::{
    FILE_FLAG_BACKUP_SEMANTICS, FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE, READ_CONTROL,
    WRITE_DAC,
};
use windows_sys::Win32::System::JobObjects::IsProcessInJob;
use windows_sys::Win32::System::Pipes::{CreatePipe, PeekNamedPipe};
use windows_sys::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};

use super::r20260909_windows_sandbox_launch::{
    set_error_mode, ProfileOwner, ScratchDirectory, Worker, PROFILE, REPORT, ROLE,
};

const ENABLE: &str = "KLAUS_WINDOWS_WORKSPACE_ONLY_EXPERIMENT";
const WORKER: &str = "workspace-only-worker";
const TARGET: &str = "workspace-only-target";
const TEST: &str = "workspace_grants_and_inherited_pipes_preserve_appcontainer_boundary";
const PREFIX: &[u8] = b"KLAUS_WORKSPACE_PROBE_JSON=";
const SEED: &[u8] = b"owned-read-only-sentinel";
const PAYLOAD: &[u8] = b"owned-target-write";
type ProbeResult<T> = Result<T, String>;

fn success(ok: i32) -> io::Result<()> {
    if ok != 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}
fn status(code: u32) -> io::Result<()> {
    if code == 0 {
        Ok(())
    } else {
        Err(io::Error::from_raw_os_error(code as i32))
    }
}
fn text(error: impl std::fmt::Display) -> String {
    error.to_string()
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
impl RawIo {
    fn bytes_equal(&self, expected: &[u8]) -> bool {
        matches!(self, Self::Ok(bytes) if bytes == expected)
    }
    fn denied(&self) -> bool {
        matches!(self, Self::Error { kind, .. } if kind == "PermissionDenied")
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct FileCheck {
    operation: String,
    path: PathBuf,
    result: RawIo,
    valid: bool,
}
#[derive(Debug, Serialize, Deserialize)]
struct Token {
    app_container: bool,
    package_sid: String,
    integrity_rid: u32,
    elevated: bool,
    session_id: u32,
}
#[derive(Debug, Serialize, Deserialize)]
struct TargetReport {
    token: Token,
    input: RawIo,
    checks: Vec<FileCheck>,
    nul_open: RawIo,
}

fn token_information(token: HANDLE, class: TOKEN_INFORMATION_CLASS) -> io::Result<Vec<usize>> {
    let mut needed = 0;
    unsafe { GetTokenInformation(token, class, ptr::null_mut(), 0, &mut needed) };
    if needed == 0 || needed > 65_536 {
        return Err(io::Error::last_os_error());
    }
    let mut bytes = vec![0usize; (needed as usize).div_ceil(std::mem::size_of::<usize>())];
    success(unsafe {
        GetTokenInformation(token, class, bytes.as_mut_ptr().cast(), needed, &mut needed)
    })?;
    Ok(bytes)
}
fn sid_string(sid: *mut std::ffi::c_void) -> io::Result<String> {
    let mut raw = ptr::null_mut();
    success(unsafe { ConvertSidToStringSidW(sid, &mut raw) })?;
    let mut length = 0;
    while unsafe { *raw.add(length) } != 0 {
        length += 1;
    }
    let result = String::from_utf16(unsafe { std::slice::from_raw_parts(raw, length) })
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error));
    unsafe { LocalFree(raw.cast()) };
    result
}
fn inspect_token(process: HANDLE) -> io::Result<Token> {
    let mut raw = ptr::null_mut();
    success(unsafe { OpenProcessToken(process, TOKEN_QUERY, &mut raw) })?;
    // OwnedHandle closes token handles as well as file handles.
    let token = unsafe { std::os::windows::io::OwnedHandle::from_raw_handle(raw) };
    let is_app = token_information(token.as_raw_handle(), TokenIsAppContainer)?;
    let app_container = unsafe { *is_app.as_ptr().cast::<u32>() } != 0;
    let package_sid = if app_container {
        let package = token_information(token.as_raw_handle(), TokenAppContainerSid)?;
        let sid = unsafe {
            (*package.as_ptr().cast::<TOKEN_APPCONTAINER_INFORMATION>()).TokenAppContainer
        };
        if sid.is_null() {
            return Err(io::Error::other("AppContainer token has no package SID"));
        }
        sid_string(sid)?
    } else {
        String::new()
    };
    let label = token_information(token.as_raw_handle(), TokenIntegrityLevel)?;
    let sid = unsafe { (*label.as_ptr().cast::<TOKEN_MANDATORY_LABEL>()).Label.Sid };
    if sid.is_null() {
        return Err(io::Error::other("integrity SID is absent"));
    }
    let count = unsafe { *GetSidSubAuthorityCount(sid) };
    if count == 0 {
        return Err(io::Error::other("integrity SID has no subauthority"));
    }
    let integrity_rid = unsafe { *GetSidSubAuthority(sid, u32::from(count - 1)) };
    let elevation = token_information(token.as_raw_handle(), TokenElevation)?;
    let elevated = unsafe { (*elevation.as_ptr().cast::<TOKEN_ELEVATION>()).TokenIsElevated } != 0;
    let session = token_information(token.as_raw_handle(), TokenSessionId)?;
    let session_id = unsafe { *session.as_ptr().cast::<u32>() };
    Ok(Token {
        app_container,
        package_sid,
        integrity_rid,
        elevated,
        session_id,
    })
}

fn target_probe(root: &Path) -> ProbeResult<TargetReport> {
    let token = inspect_token(unsafe { GetCurrentProcess() }).map_err(text)?;
    let sid = identity::derive_sid(&std::env::var(PROFILE).map_err(text)?).map_err(text)?;
    let expected = sid_string(sid.as_psid()).map_err(text)?;
    if !token.app_container
        || token.package_sid != expected
        || token.integrity_rid > 4096
        || token.elevated
    {
        return Err(format!(
            "target is not the requested AppContainer: {token:?}"
        ));
    }
    let input = observed((|| {
        let mut nonce = [0];
        io::stdin().read_exact(&mut nonce)?;
        Ok(nonce.to_vec())
    })());
    if !input.bytes_equal(b"x") {
        return Err(format!(
            "parent did not authorize the inspected target: {input:?}"
        ));
    }
    let mut checks = Vec::new();
    for absolute in [false, true] {
        let path = |directory: &str, file: &str| {
            if absolute {
                root.join(directory).join(file)
            } else if directory == "writable" {
                PathBuf::from(file)
            } else {
                Path::new("..").join(directory).join(file)
            }
        };
        let read_only = path("readonly", "seed.txt");
        let read = observed(fs::read(&read_only));
        let valid = read.bytes_equal(SEED);
        checks.push(FileCheck {
            operation: "readonly-read".into(),
            path: read_only.clone(),
            result: read,
            valid,
        });
        let write = observed(OpenOptions::new().write(true).open(&read_only).map(|file| {
            drop(file);
            Vec::new()
        }));
        let valid = write.denied();
        checks.push(FileCheck {
            operation: "readonly-write-open".into(),
            path: read_only,
            result: write,
            valid,
        });
        let blocked = path("blocked", "seed.txt");
        let read = observed(fs::read(&blocked));
        let valid = read.denied();
        checks.push(FileCheck {
            operation: "blocked-read".into(),
            path: blocked,
            result: read,
            valid,
        });
        let file = path(
            "writable",
            if absolute {
                "absolute.bin"
            } else {
                "relative.bin"
            },
        );
        let write = observed(fs::write(&file, PAYLOAD).map(|()| Vec::new()));
        let valid = write.bytes_equal(&[]);
        checks.push(FileCheck {
            operation: "create-write".into(),
            path: file.clone(),
            result: write,
            valid,
        });
        let read = observed(fs::read(&file));
        let valid = read.bytes_equal(PAYLOAD);
        checks.push(FileCheck {
            operation: "read-back".into(),
            path: file.clone(),
            result: read,
            valid,
        });
        let removed = observed(fs::remove_file(&file).map(|()| Vec::new()));
        let valid = removed.bytes_equal(&[]);
        checks.push(FileCheck {
            operation: "delete".into(),
            path: file,
            result: removed,
            valid,
        });
    }
    // Open only. This does not write data or change the null device's descriptor.
    let nul_open = observed(
        OpenOptions::new()
            .read(true)
            .write(true)
            .open(r"\\.\NUL")
            .map(|handle| {
                drop(handle);
                Vec::new()
            }),
    );
    Ok(TargetReport {
        token,
        input,
        checks,
        nul_open,
    })
}

#[derive(Debug, Serialize, Deserialize)]
struct Execution {
    target_token: Option<Token>,
    exact_job_member: Option<bool>,
    exit_code: Option<u32>,
    output_eof: bool,
    eof_code: Option<i32>,
    output: Vec<u8>,
    target: Option<TargetReport>,
    error: Option<String>,
    target_cleanup: Option<Result<(), String>>,
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
fn test_name() -> String {
    let module = module_path!()
        .split_once("::")
        .map_or(module_path!(), |(_, suffix)| suffix);
    format!("{module}::{TEST}")
}
fn observe_launch(root: &Path, report: &mut Execution) -> ProbeResult<()> {
    let sid = identity::derive_sid(&std::env::var(PROFILE).map_err(text)?).map_err(text)?;
    let expected = sid_string(sid.as_psid()).map_err(text)?;
    let (input, mut input_writer) = pipe().map_err(text)?;
    let (mut output, output_writer) = pipe().map_err(text)?;
    let child_input = inheritable(&input).map_err(text)?;
    let child_output = inheritable(&output_writer).map_err(text)?;
    drop(input);
    drop(output_writer);
    let mut environment: BTreeMap<String, String> = std::env::vars().collect();
    environment.insert(ROLE.into(), TARGET.into());
    let command = process::command_line(&[
        root.join("bin/probe.exe").to_string_lossy().into_owned(),
        "--exact".into(),
        test_name(),
        "--ignored".into(),
        "--nocapture".into(),
        "--test-threads=1".into(),
    ]);
    let launched = process::launch(LaunchSpec {
        command_line: command,
        working_directory: root.join("writable").to_string_lossy().into_owned(),
        environment: process::environment_block(&environment),
        identity: LaunchIdentity::AppContainer {
            container: &sid,
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
    eprintln!(
        "workspace_create_target={:?}",
        launched.as_ref().map(|_| "created").map_err(text)
    );
    drop(child_input);
    drop(child_output);
    let target = launched.map_err(text)?;
    let operation = (|| -> ProbeResult<()> {
        let mut member = 0;
        success(unsafe {
            IsProcessInJob(
                target.process_handle() as HANDLE,
                target.tree_handle() as HANDLE,
                &mut member,
            )
        })
        .map_err(text)?;
        report.exact_job_member = Some(member != 0);
        let token = inspect_token(target.process_handle() as HANDLE).map_err(text)?;
        let permitted = token.app_container
            && token.package_sid == expected
            && token.integrity_rid <= 4096
            && member != 0;
        report.target_token = Some(token);
        if !permitted {
            return Err("target identity or exact Job differs from requested boundary".into());
        }
        // One byte in an empty 4096-byte pipe cannot block on buffer capacity.
        input_writer.write_all(b"x").map_err(text)?;
        drop(input_writer);
        let deadline = Instant::now() + Duration::from_secs(5);
        while report.exit_code.is_none() || !report.output_eof {
            if Instant::now() >= deadline {
                return Err("target exit/output EOF exceeded 5000 ms".into());
            }
            if !report.output_eof {
                let mut available = 0;
                if unsafe {
                    PeekNamedPipe(
                        output.as_raw_handle(),
                        ptr::null_mut(),
                        0,
                        ptr::null_mut(),
                        &mut available,
                        ptr::null_mut(),
                    )
                } == 0
                {
                    let error = io::Error::last_os_error();
                    if error.kind() != io::ErrorKind::BrokenPipe {
                        return Err(error.to_string());
                    }
                    report.output_eof = true;
                    report.eof_code = error.raw_os_error();
                } else if available != 0 {
                    let mut bytes = [0; 4096];
                    let count = output
                        .read(&mut bytes[..(available as usize).min(4096)])
                        .map_err(text)?;
                    report.output_eof = count == 0;
                    report.output.extend_from_slice(&bytes[..count]);
                    if report.output.len() > 65_536 {
                        return Err("target exceeded 64 KiB output limit".into());
                    }
                }
            }
            if report.exit_code.is_none() {
                report.exit_code = target
                    .try_wait()
                    .map_err(text)?
                    .map(|outcome| outcome.exit_code);
            }
            thread::sleep(Duration::from_millis(5));
        }
        let raw = report
            .output
            .split(|byte| *byte == b'\n')
            .find_map(|line| line.strip_prefix(PREFIX))
            .ok_or("target emitted no structured result after image initialization")?;
        report.target = Some(serde_json::from_slice(raw).map_err(text)?);
        Ok(())
    })();
    report.target_cleanup = Some(target.terminate_and_wait().map_err(text));
    drop(target);
    drop(output);
    operation
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
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
        let mut control = 0;
        let mut revision = 0;
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
    fn restore(&self) -> io::Result<()> {
        if self.unchanged()? {
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
    fn unchanged(&self) -> io::Result<bool> {
        Ok(Descriptor::read(&self.handle)?.image()? == self.before)
    }
}
impl Drop for Restore {
    fn drop(&mut self) {
        if let Err(error) = self.restore() {
            eprintln!(
                "workspace_restore_fallback path={:?} error={error}",
                self.path
            );
        }
    }
}

fn image_hash(path: &Path) -> io::Result<String> {
    let mut file = File::open(path)?;
    let mut hash = Sha256::new();
    let mut buffer = [0; 65_536];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        hash.update(&buffer[..count]);
    }
    Ok(format!("{:x}", hash.finalize()))
}
fn prepare(
    root: &Path,
    sid: &AppContainerSid,
    originals: &mut Vec<Restore>,
    rules: &mut Vec<Rule>,
    journal: &mut Vec<acl::SavedDacl>,
) -> ProbeResult<()> {
    for name in ["bin", "readonly", "writable", "blocked"] {
        let path = root.join(name);
        let created = fs::create_dir(&path);
        eprintln!("workspace_create_directory path={path:?} result={created:?}");
        created.map_err(text)?;
        originals.push(Restore::retain(path).map_err(text)?);
    }
    for name in ["readonly", "blocked"] {
        let path = root.join(name).join("seed.txt");
        let written = fs::write(&path, SEED);
        eprintln!(
            "workspace_seed_write path={path:?} bytes={} result={written:?}",
            SEED.len()
        );
        written.map_err(text)?;
        originals.push(Restore::retain(path).map_err(text)?);
    }
    let destination = root.join("bin/probe.exe");
    let source = std::env::current_exe().map_err(text)?;
    let copied = fs::copy(&source, &destination);
    eprintln!(
        "workspace_copy_image source={source:?} destination={destination:?} result={copied:?}"
    );
    copied.map_err(text)?;
    let source_hash = image_hash(&source).map_err(text)?;
    let copied_hash = image_hash(&destination).map_err(text)?;
    eprintln!("workspace_image_sha256 source={source_hash} copied={copied_hash}");
    if source_hash != copied_hash {
        return Err("copied image hash differs".into());
    }
    originals.push(Restore::retain(destination).map_err(text)?);
    // Do not call plan_entries: that planner deliberately includes system ancestors.
    for (name, grant) in [
        ("bin", Grant::Read),
        ("readonly", Grant::Read),
        ("writable", Grant::ReadWrite),
    ] {
        rules.push(Rule::Allow {
            path: AbsPath::new(root.join(name).to_string_lossy()).map_err(text)?,
            grant,
            inheritable: true,
        });
    }
    rules.push(Rule::Withhold {
        path: AbsPath::new(root.join("blocked").to_string_lossy()).map_err(text)?,
        keep: None,
    });
    let applied = acl::apply_recorded(rules, sid, journal).map_err(text);
    eprintln!("workspace_apply rules={rules:?} result={applied:?}");
    applied
}

fn controller() -> ProbeResult<()> {
    let controller_token = inspect_token(unsafe { GetCurrentProcess() }).map_err(text)?;
    eprintln!("workspace_controller_token={controller_token:?}");
    if controller_token.app_container
        || controller_token.elevated
        || controller_token.integrity_rid != 8192
        || controller_token.session_id == 0
    {
        return Err(
            "experiment requires an ordinary interactive medium-integrity controller".into(),
        );
    }
    let root = ScratchDirectory::create().map_err(text)?;
    let mut profile = match ProfileOwner::create() {
        Ok(profile) => profile,
        Err(error) => {
            let cleanup = root.close();
            eprintln!(
                "workspace_profile_create_failed error={error} directory_cleanup={cleanup:?}"
            );
            return Err(error);
        }
    };
    let sid = match identity::derive_sid(profile.0.as_deref().expect("owned profile name")) {
        Ok(sid) => sid,
        Err(error) => {
            let profile_cleanup = profile.close();
            let directory_cleanup = root.close();
            eprintln!("workspace_sid_derive_failed error={error} profile_cleanup={profile_cleanup:?} directory_cleanup={directory_cleanup:?}");
            return Err(text(error));
        }
    };
    let (mut originals, mut rules, mut journal) = (Vec::new(), Vec::new(), Vec::new());
    let setup = prepare(root.path(), &sid, &mut originals, &mut rules, &mut journal);
    let mut execution = Err("setup prevented worker launch".to_string());
    let mut worker_cleanup = Ok(());
    let mut worker_exit = None;
    let mut owned_worker = None;
    if setup.is_ok() {
        match Worker::spawn_ignored(
            WORKER,
            &test_name(),
            root.path(),
            profile.0.as_deref().unwrap_or(""),
        ) {
            Ok(mut worker) => {
                worker_exit = Some(worker.wait());
                execution = fs::read(root.path().join("report.json"))
                    .map_err(text)
                    .and_then(|bytes| serde_json::from_slice::<Execution>(&bytes).map_err(text));
                let log = fs::read(root.path().join("worker.log"));
                eprintln!("workspace_worker_log_bytes={log:?}");
                worker_cleanup = worker.cleanup();
                owned_worker = Some(worker);
            }
            Err(error) => {
                // The shared launch API can fail after creating its suspended
                // process. An error carries no confirmed drain receipt.
                worker_cleanup = Err(format!(
                    "worker spawn returned no confirmed cleanup: {error}"
                ));
                execution = Err(error);
            }
        }
    }
    if let Err(error) = &worker_cleanup {
        let recovery = serde_json::json!({
            "cleanupConfirmed": false,
            "error": error,
            "root": root.path(),
            "profile": profile.0,
            "workerExit": worker_exit,
            "aclBefore": originals.iter().map(|entry| serde_json::json!({
                "path": entry.path, "image": entry.before,
            })).collect::<Vec<_>>(),
        });
        let recovery_path = root.path().join("cleanup-unconfirmed.json");
        let saved = serde_json::to_vec_pretty(&recovery)
            .map_err(text)
            .and_then(|bytes| fs::write(&recovery_path, bytes).map_err(text));
        eprintln!("workspace_cleanup_unconfirmed root={:?} profile={:?} recovery={recovery_path:?} saved={saved:?} error={error}", root.path(), profile.0);
        let error = error.clone();
        // Retain security state and recovery evidence until the exact process
        // tree is proved empty. Automatic Restore/Profile drops must not undo
        // the boundary while an owned target may still be alive. The outer test
        // process owns the retained Job handle until it exits.
        std::mem::forget(originals);
        std::mem::forget(profile);
        std::mem::forget(owned_worker);
        std::mem::forget(root);
        return Err(error);
    }
    drop(owned_worker);
    // The outer Job is stopped before any package ACE or profile is removed.
    let revoked: Vec<_> = if journal.is_empty() {
        Vec::new()
    } else {
        acl::revoke(&rules, &journal, &sid)
            .into_iter()
            .map(text)
            .collect()
    };
    let rollback: Vec<_> = originals
        .iter()
        .map(|entry| (entry.path.clone(), entry.unchanged().map_err(text)))
        .collect();
    let restores: Vec<_> = originals
        .iter()
        .map(|entry| (entry.path.clone(), entry.restore().map_err(text)))
        .collect();
    let final_acls: Vec<_> = originals
        .iter()
        .map(|entry| (entry.path.clone(), entry.unchanged().map_err(text)))
        .collect();
    let seed = fs::read(root.path().join("readonly/seed.txt"));
    let writes_removed = ["relative.bin", "absolute.bin"]
        .iter()
        .all(|name| !root.path().join("writable").join(name).exists());
    eprintln!(
        "workspace_experiment setup={setup:?} worker_exit={worker_exit:?} execution={execution:?}"
    );
    eprintln!("workspace_cleanup worker={worker_cleanup:?} revoke={revoked:?} rollback={rollback:?} restore={restores:?} final_acls={final_acls:?} seed={seed:?} writes_removed={writes_removed}");
    drop(originals);
    drop(sid);
    let profile_cleanup = profile.close();
    let removed = root.close().map_err(text);
    eprintln!("workspace_final_cleanup profile={profile_cleanup:?} directory={removed:?}");
    // Every assertion below runs after all restoration/removal attempts.
    worker_cleanup?;
    profile_cleanup?;
    removed?;
    setup?;
    if !revoked.is_empty()
        || rollback.iter().any(|(_, result)| result != &Ok(true))
        || restores.iter().any(|(_, result)| result.is_err())
        || final_acls.iter().any(|(_, result)| result != &Ok(true))
    {
        return Err("owned ACL rollback or restoration was not confirmed".into());
    }
    if seed.map_err(text)? != SEED || !writes_removed {
        return Err("target left unexpected file changes".into());
    }
    if worker_exit != Some(Ok(0)) {
        return Err(format!("worker exit was not successful: {worker_exit:?}"));
    }
    let report = execution?;
    if report.error.is_some()
        || report.exact_job_member != Some(true)
        || report.exit_code != Some(7)
        || !report.output_eof
        || report.target_cleanup != Some(Ok(()))
    {
        return Err(format!("target lifecycle failed: {report:?}"));
    }
    let target = report.target.ok_or("target result absent")?;
    if !target.input.bytes_equal(b"x")
        || target.checks.len() != 12
        || target.checks.iter().any(|check| !check.valid)
    {
        return Err(format!("target filesystem boundary failed: {target:?}"));
    }
    Ok(())
}

#[test]
#[ignore = "explicit ordinary-token Windows workspace-only experiment; requires exact selection and opt-in environment"]
fn workspace_grants_and_inherited_pipes_preserve_appcontainer_boundary() {
    assert_eq!(
        std::env::var(ENABLE).ok().as_deref(),
        Some("1"),
        "explicit experiment opt-in required before creating resources"
    );
    let arguments: Vec<_> = std::env::args().collect();
    assert!(
        arguments.iter().any(|argument| argument == "--exact")
            && arguments
                .iter()
                .filter(|argument| *argument == &test_name())
                .count()
                == 1,
        "exact test selection is required"
    );
    match std::env::var(ROLE).ok().as_deref() {
        Some(TARGET) => {
            let root = PathBuf::from(std::env::var(REPORT).expect("owned report path"))
                .parent()
                .expect("owned root")
                .to_path_buf();
            let result = target_probe(&root);
            match result {
                Ok(report) => {
                    let mut output = io::stdout().lock();
                    output.write_all(b"\n").unwrap();
                    output.write_all(PREFIX).unwrap();
                    serde_json::to_writer(&mut output, &report).unwrap();
                    output.write_all(b"\n").unwrap();
                    output.flush().unwrap();
                    drop(output);
                    std::process::exit(7);
                }
                Err(error) => {
                    eprintln!("workspace_target_error={error}");
                    std::process::exit(2);
                }
            }
        }
        Some(WORKER) => {
            // Affect only this owned worker and its target, never the controller.
            unsafe { set_error_mode(0x0001 | 0x0002 | 0x8000) };
            let output = PathBuf::from(std::env::var(REPORT).expect("owned report path"));
            let root = output.parent().expect("owned root");
            let mut report = Execution {
                target_token: None,
                exact_job_member: None,
                exit_code: None,
                output_eof: false,
                eof_code: None,
                output: Vec::new(),
                target: None,
                error: None,
                target_cleanup: None,
            };
            report.error = observe_launch(root, &mut report).err();
            fs::write(&output, serde_json::to_vec(&report).unwrap())
                .expect("preserve worker observation");
            std::process::exit(i32::from(report.error.is_some()));
        }
        None => {
            let result = controller();
            assert!(result.is_ok(), "{result:?}");
        }
        Some(role) => panic!("unexpected inherited worker role: {role}"),
    }
}
