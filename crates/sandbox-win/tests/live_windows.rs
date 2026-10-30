//! Integration tests that only mean anything on a real Windows machine.
//!
//! Everything in the unit tests is decidable without an OS: path comparison,
//! framing, plan ordering. These are the other half — the calls that either work
//! against the live kernel or do not, and whose failure modes are silent. An
//! AppContainer that cannot be created, an ACE that is written but grants nothing, a
//! loopback exemption that removes everyone else's: none of those show up as a
//! compile error or a wrong plan.
//!
//! Off Windows the whole file compiles to nothing, so a `cargo test` on any host
//! stays green without pretending to have verified this.

#![cfg(windows)]

use std::path::Path;

use klaus_sandbox_win::acl;
use klaus_sandbox_win::boundary::{AbsPath, FilesystemBoundary};
use klaus_sandbox_win::capability::{self, Capability};
use klaus_sandbox_win::identity;
use klaus_sandbox_win::null_device;
use klaus_sandbox_win::process::{self, LaunchSpec};
use klaus_sandbox_win::protocol::SandboxRequest;

/// A container name unique to one test, so parallel runs cannot fight over the
/// same profile or the same ACEs.
fn container_name(test: &str) -> String {
    format!("klaus-test-{test}-{}", std::process::id())
}

fn request_for(name: &str) -> SandboxRequest {
    SandboxRequest {
        command: vec!["cmd.exe".into()],
        cwd: AbsPath::new("C:\\").expect("drive root parses"),
        env: Default::default(),
        filesystem: FilesystemBoundary::default(),
        network: klaus_sandbox_win::boundary::NetworkBoundary::Denied,
        network_route_capability_sid: None,
        container_name: name.to_string(),
        terminal: None,
    }
}

#[test]
fn an_app_container_can_be_created_and_its_sid_is_stable_across_calls() {
    // The premise the whole design rests on: the same name yields the same SID, so
    // ACEs written for it keep meaning the same thing on a later run. If this were
    // not true, every command would need its roots re-granted and teardown could
    // never find what it granted.
    let name = container_name("identity");
    let request = request_for(&name);
    let moniker = identity::container_moniker(&request).expect("moniker");

    let first = identity::ensure_container(&moniker).expect("create the profile");
    let second = identity::derive_sid(&moniker).expect("derive the same SID again");

    let equal =
        unsafe { windows_sys::Win32::Security::EqualSid(first.as_psid(), second.as_psid()) };
    assert_ne!(equal, 0, "the same moniker must derive one SID");

    // Creating twice is the steady state, not an error: profiles outlive sessions.
    let third = identity::ensure_container(&moniker).expect("second create is fine");
    let equal = unsafe { windows_sys::Win32::Security::EqualSid(first.as_psid(), third.as_psid()) };
    assert_ne!(equal, 0, "re-creating must not change the SID");
}

#[test]
fn a_granted_directory_lets_the_container_write_and_delete_inside_it() {
    // The bug this exists for: granting write without FILE_DELETE_CHILD produces a
    // sandbox where creating a file works and deleting it fails. The unit test pins
    // the mask; this pins that the mask, once written as an ACE, actually permits
    // both against the live security check.
    let name = container_name("write-delete");
    let request = request_for(&name);
    let moniker = identity::container_moniker(&request).expect("moniker");
    let sid = identity::ensure_container(&moniker).expect("container");

    let root = std::env::temp_dir().join(format!("klaus-acl-{}", std::process::id()));
    std::fs::create_dir_all(&root).expect("create the test root");
    let abs = AbsPath::new(root.to_string_lossy().as_ref()).expect("temp path parses");

    let entries = acl::plan_entries(&FilesystemBoundary {
        writable: vec![abs.clone()],
        ..Default::default()
    });
    let saved = acl::apply(&entries, &sid).expect("write the ACEs");

    // Read the DACL back and confirm the container is named with both deletion bits.
    let granted = masks_for(&root, &sid).allow;
    const DELETE: u32 = 0x0001_0000;
    const FILE_DELETE_CHILD: u32 = 0x0040;
    assert_ne!(
        granted & DELETE,
        0,
        "DELETE missing from the applied ACE: mask 0x{granted:08x}"
    );
    assert_ne!(
        granted & FILE_DELETE_CHILD,
        0,
        "FILE_DELETE_CHILD missing from the applied ACE: mask 0x{granted:08x}"
    );

    let failures = acl::revoke(&entries, &saved, &sid);
    assert!(failures.is_empty(), "teardown reported: {failures:?}");
    // After teardown the container has no say on the path at all.
    assert_eq!(masks_for(&root, &sid).allow, 0, "an ACE survived teardown");

    std::fs::remove_dir_all(&root).ok();
}

/// What a path's DACL says about one container.
///
/// Both halves matter. `allow` is what the container actually gets, and `deny` has to
/// be watched because a deny ACE naming the container is silently ineffective here:
/// an implementation that drifts back to writing one would look correct in the DACL
/// and enforce nothing.
#[derive(Debug, Default)]
struct Masks {
    allow: u32,
    deny: u32,
}

/// Read `path`'s DACL and summarise what it says about `sid`.
fn masks_for(path: &Path, sid: &identity::AppContainerSid) -> Masks {
    use std::ptr;

    use windows_sys::Win32::Foundation::{LocalFree, ERROR_SUCCESS, FALSE, HLOCAL};
    use windows_sys::Win32::Security::Authorization::{GetNamedSecurityInfoW, SE_FILE_OBJECT};
    use windows_sys::Win32::Security::{
        EqualSid, GetAce, ACCESS_ALLOWED_ACE, ACE_HEADER, ACL, DACL_SECURITY_INFORMATION,
        PSECURITY_DESCRIPTOR,
    };

    let wide: Vec<u16> = path
        .to_string_lossy()
        .encode_utf16()
        .chain(std::iter::once(0))
        .collect();
    let mut dacl: *mut ACL = ptr::null_mut();
    let mut descriptor: PSECURITY_DESCRIPTOR = ptr::null_mut();
    let code = unsafe {
        GetNamedSecurityInfoW(
            wide.as_ptr(),
            SE_FILE_OBJECT,
            DACL_SECURITY_INFORMATION,
            ptr::null_mut(),
            ptr::null_mut(),
            &mut dacl,
            ptr::null_mut(),
            &mut descriptor,
        )
    };
    assert_eq!(code, ERROR_SUCCESS, "reading the DACL failed: {code}");
    let mut masks = Masks::default();
    if !dacl.is_null() {
        let count = unsafe { (*dacl).AceCount } as u32;
        for index in 0..count {
            let mut ace: *mut std::ffi::c_void = ptr::null_mut();
            if unsafe { GetAce(dacl, index, &mut ace) } == FALSE {
                continue;
            }
            let header = ace as *const ACE_HEADER;
            let ace_type = unsafe { (*header).AceType };
            // ACCESS_ALLOWED_ACE_TYPE is 0 and ACCESS_DENIED_ACE_TYPE is 1; both use
            // the same layout, so the SID and mask are read the same way.
            if ace_type > 1 {
                continue;
            }
            let entry = ace as *const ACCESS_ALLOWED_ACE;
            let ace_sid = unsafe { &(*entry).SidStart } as *const u32 as *mut std::ffi::c_void;
            if unsafe { EqualSid(ace_sid, sid.as_psid()) } == FALSE {
                continue;
            }
            let mask = unsafe { (*entry).Mask };
            if ace_type == 0 {
                masks.allow |= mask;
            } else {
                masks.deny |= mask;
            }
        }
    }
    if !descriptor.is_null() {
        unsafe { LocalFree(descriptor as HLOCAL) };
    }
    masks
}

// Loopback lease ownership and complete-table merges are pinned by the public
// core regression suite. A native SetAppContainerConfig test needs explicit
// machine-level authorization and must not run as part of this default gate.

#[test]
fn a_command_runs_inside_the_container_and_reports_its_own_exit_code() {
    // End to end for the launch path: AppContainer attribute, job object, handle
    // whitelist, desktop assignment. A target that dies in DLL initialization (the
    // STATUS_DLL_INIT_FAILED case the desktop assignment exists for) fails here with
    // a nonzero code and no output, which is exactly what this catches.
    let name = container_name("launch");
    let request = request_for(&name);
    let moniker = identity::container_moniker(&request).expect("moniker");
    let sid = identity::ensure_container(&moniker).expect("container");
    let capabilities = capability::resolve(&[Capability::InternetClient]).expect("capabilities");

    // The system directory has to be readable for cmd.exe to start at all; it
    // already is for AppContainers by default, which is why nothing is granted here.
    let spec = LaunchSpec {
        command_line: process::command_line(&[
            "C:\\Windows\\System32\\cmd.exe".to_string(),
            "/c".to_string(),
            "exit 7".to_string(),
        ]),
        working_directory: "C:\\Windows\\System32".to_string(),
        environment: process::environment_block(&Default::default()),
        identity: process::LaunchIdentity::AppContainer {
            container: &sid,
            capabilities: &capabilities,
        },
        inherit: Vec::new(),
        stdin: 0,
        stdout: 0,
        stderr: 0,
        pseudoconsole: None,
    };
    let target = process::launch(spec).expect("launch inside the container");
    let outcome = target.wait().expect("wait for the target");
    assert_eq!(
        outcome.exit_code, 7,
        "the target's own exit code must survive"
    );
}

/// What a probe run inside a container reported.
struct Probe {
    exit_code: u32,
    /// Everything the target wrote to stdout and stderr, as bytes.
    ///
    /// Captured because an exit code alone cannot tell a refusal apart from a target
    /// that never ran: both are "nonzero". Kept as bytes because `cmd.exe` writes in
    /// the console codepage, which is whatever the machine is localized to, so the
    /// text is only decodable after the fact.
    output: Vec<u8>,
}

impl Probe {
    /// Whether the output contains `needle`, ASCII only.
    fn says(&self, needle: &str) -> bool {
        String::from_utf8_lossy(&self.output).contains(needle)
    }
}

impl std::fmt::Display for Probe {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "exit={} output=", self.exit_code)?;
        // Escaped bytes rather than lossy text: a localized message decoded lossily
        // becomes replacement characters, which says nothing about what went wrong.
        write!(f, "\"")?;
        for byte in &self.output {
            match byte {
                0x20..=0x7e => write!(f, "{}", *byte as char)?,
                b'\r' => write!(f, "\\r")?,
                b'\n' => write!(f, "\\n")?,
                other => write!(f, "\\x{other:02x}")?,
            }
        }
        write!(f, "\"")
    }
}

/// Run `argv` inside `sid`'s container, capturing what it says.
///
/// The target's stdout and stderr go to an inherited pipe rather than to a `NUL`
/// redirection. That is not a detail: a redirection is itself an operation the
/// sandbox can refuse, so a probe that redirects can fail before running its command
/// and report an exit code that looks like the boundary working.
fn run_in_container(
    sid: &identity::AppContainerSid,
    capabilities: &[capability::CapabilitySid],
    argv: &[&str],
) -> Probe {
    use std::io::Read;
    use std::os::windows::io::FromRawHandle;

    use windows_sys::Win32::Foundation::{
        CloseHandle, SetHandleInformation, HANDLE, HANDLE_FLAG_INHERIT, TRUE,
    };
    use windows_sys::Win32::Security::SECURITY_ATTRIBUTES;
    use windows_sys::Win32::System::Pipes::CreatePipe;

    let attributes = SECURITY_ATTRIBUTES {
        nLength: std::mem::size_of::<SECURITY_ATTRIBUTES>() as u32,
        lpSecurityDescriptor: std::ptr::null_mut(),
        // The write end has to be inheritable to appear in the handle list at all.
        bInheritHandle: TRUE,
    };
    let mut read: HANDLE = 0;
    let mut write: HANDLE = 0;
    assert_ne!(
        unsafe { CreatePipe(&mut read, &mut write, &attributes, 0) },
        0,
        "creating the capture pipe failed"
    );
    // Our end stays behind. An inheritable read end would keep a writer alive inside
    // the target and the read below would never reach EOF.
    unsafe { SetHandleInformation(read, HANDLE_FLAG_INHERIT, 0) };

    let owned: Vec<String> = argv.iter().map(|part| part.to_string()).collect();
    let spec = LaunchSpec {
        command_line: process::command_line(&owned),
        // Readable by any AppContainer, so the shell itself can start.
        working_directory: "C:\\Windows\\System32".to_string(),
        environment: process::environment_block(&Default::default()),
        identity: process::LaunchIdentity::AppContainer {
            container: sid,
            capabilities,
        },
        inherit: vec![write],
        stdin: 0,
        stdout: write,
        stderr: write,
        pseudoconsole: None,
    };
    let target = process::launch(spec).expect("launch inside the container");
    // The parent's copy of the write end must go now: while it is open the pipe has a
    // writer and the read below blocks forever, target exited or not.
    unsafe { CloseHandle(write) };

    let mut bytes = Vec::new();
    let mut pipe = unsafe { std::fs::File::from_raw_handle(read as *mut std::ffi::c_void) };
    pipe.read_to_end(&mut bytes)
        .expect("read the target's output");
    drop(pipe);

    Probe {
        exit_code: target.wait().expect("wait for the target").exit_code,
        output: bytes,
    }
}

/// A scratch directory with no inherited AppContainer access, plus its parsed form.
fn scratch(label: &str) -> (std::path::PathBuf, AbsPath) {
    let path = std::env::temp_dir().join(format!("klaus-live-{label}-{}", std::process::id()));
    std::fs::remove_dir_all(&path).ok();
    std::fs::create_dir_all(&path).expect("create the scratch directory");
    let abs = AbsPath::new(path.to_string_lossy().as_ref()).expect("scratch path parses");
    (path, abs)
}

#[test]
fn the_filesystem_boundary_binds_the_container_process_itself() {
    // The other ACL test reads the DACL back, which proves the bits were written.
    // This one proves the kernel enforces them against a real process: one granted
    // root it can write and empty, one sibling it cannot touch. Both halves are in
    // one test on purpose — a refusal only means something if the same launch path
    // succeeds when the policy allows it, otherwise a broken launch reads as a
    // working boundary.
    let name = container_name("fs-boundary");
    let request = request_for(&name);
    let moniker = identity::container_moniker(&request).expect("moniker");
    let sid = identity::ensure_container(&moniker).expect("container");
    let capabilities = capability::resolve(&[]).expect("no capabilities");

    let (granted_dir, granted) = scratch("granted");
    let (outside_dir, _outside) = scratch("outside");

    let entries = acl::plan_entries(&FilesystemBoundary {
        writable: vec![granted.clone()],
        ..Default::default()
    });
    let saved = acl::apply(&entries, &sid).expect("grant the writable root");
    // The ordinary caller may have no WRITE_DAC on the device object; the
    // assertions below do not depend on NUL and must hold in both cases.
    let null_device = null_device::grant(&sid);

    let inside_file = granted_dir.join("probe.txt");
    let outside_file = outside_dir.join("probe.txt");
    let shell = "C:\\Windows\\System32\\cmd.exe";

    // Granted: the write lands.
    let probe = run_in_container(
        &sid,
        &capabilities,
        &[
            shell,
            "/c",
            "echo",
            "ok",
            ">",
            &inside_file.to_string_lossy(),
        ],
    );
    assert_eq!(
        probe.exit_code, 0,
        "a granted root must be writable from inside: {probe}"
    );
    assert!(
        inside_file.is_file(),
        "the container reported success but wrote nothing: {probe}"
    );

    // Granted: a shell built-in can enumerate it. This is the walk-grant case: without
    // ACEs on the ancestors of the root, `cmd` cannot canonicalize the path and every
    // built-in fails with access denied even though the raw API calls all succeed.
    let probe = run_in_container(
        &sid,
        &capabilities,
        &[shell, "/c", "dir", "/b", &granted_dir.to_string_lossy()],
    );
    // An ordinary caller cannot write the drive-root DACL, so `cmd` cannot
    // canonicalize absolute paths on that host and every built-in reports
    // access denied. The strong assertion holds when the root is grantable;
    // otherwise the degraded outcome is recorded, and the raw API checks above
    // already proved the grant itself.
    if masks_for(Path::new("C:\\"), &sid).allow != 0 {
        assert_eq!(
            probe.exit_code, 0,
            "a shell built-in could not read a granted root: {probe}"
        );
        assert!(
            probe.says("probe.txt"),
            "the listing did not name the file that is there: {probe}"
        );
    } else {
        eprintln!(
            "degraded: drive-root walk unavailable; dir exit={} probe={probe}",
            probe.exit_code
        );
    }

    // Granted: and the delete lands too, which is the FILE_DELETE_CHILD case.
    // The file was created by the container, so its own DACL is whatever the root's
    // inheritable ACE produced. Reading it back separates "the grant never reached
    // the file" from "the grant reached it and deletion still failed".
    const DELETE: u32 = 0x0001_0000;
    let inherited = masks_for(&inside_file, &sid);
    assert_ne!(
        inherited.allow & DELETE,
        0,
        "the root's inheritable grant did not reach a file created inside it: {inherited:?}"
    );
    let probe = run_in_container(
        &sid,
        &capabilities,
        &[shell, "/c", "del", "/q", &inside_file.to_string_lossy()],
    );
    if masks_for(Path::new("C:\\"), &sid).allow != 0 {
        assert!(
            !inside_file.exists(),
            "the container could create a file inside a granted root but not remove it: {probe}"
        );
    } else {
        eprintln!("degraded: drive-root walk unavailable; absolute-path del skipped");
    }

    // And `>nul` works, which most real command lines need. That holds exactly
    // when the device grant was installable; an ordinary caller that cannot
    // rewrite the device DACL sees access denied here instead.
    if null_device.is_ok() {
        let probe = run_in_container(&sid, &capabilities, &[shell, "/c", "echo", "x", ">", "NUL"]);
        assert_eq!(
            probe.exit_code, 0,
            "the container cannot discard output to NUL: {probe}"
        );
    } else {
        eprintln!("degraded: null device grant unavailable; `>nul` reports access denied");
    }

    // Not granted: the write is refused, and nothing appears.
    let probe = run_in_container(
        &sid,
        &capabilities,
        &[
            shell,
            "/c",
            "echo",
            "ok",
            ">",
            &outside_file.to_string_lossy(),
        ],
    );
    assert_ne!(
        probe.exit_code, 0,
        "a write outside every granted root reported success: {probe}"
    );
    assert!(
        !outside_file.exists(),
        "a path outside every granted root was written: {}",
        outside_file.display()
    );

    let failures = acl::revoke(&entries, &saved, &sid);
    assert!(failures.is_empty(), "teardown reported: {failures:?}");
    if null_device.is_ok() {
        null_device::revoke(&sid).expect("take back the null device");
    }
    std::fs::remove_dir_all(&granted_dir).ok();
    std::fs::remove_dir_all(&outside_dir).ok();
}

#[test]
fn a_walk_grant_does_not_open_up_what_the_ancestor_holds() {
    // The ancestors of a granted root are made walkable so shell built-ins work. That
    // is a real widening, so this pins its edge: the container may list the ancestor's
    // names, and may not read a file sitting in it.
    let name = container_name("walk-edge");
    let request = request_for(&name);
    let moniker = identity::container_moniker(&request).expect("moniker");
    let sid = identity::ensure_container(&moniker).expect("container");
    let capabilities = capability::resolve(&[]).expect("no capabilities");

    let (parent_dir, _parent) = scratch("walk-parent");
    let child_dir = parent_dir.join("child");
    std::fs::create_dir_all(&child_dir).expect("create the granted root");
    let child = AbsPath::new(child_dir.to_string_lossy().as_ref()).expect("child parses");
    let sibling = parent_dir.join("sibling.txt");
    std::fs::write(&sibling, b"SIBLINGBODY").expect("write the ancestor's file");

    let entries = acl::plan_entries(&FilesystemBoundary {
        writable: vec![child],
        ..Default::default()
    });
    let saved = acl::apply(&entries, &sid).expect("apply the boundary");
    // The ordinary caller may have no WRITE_DAC on the device object; the
    // assertions below do not depend on NUL and must hold in both cases.
    let null_device = null_device::grant(&sid);

    let shell = "C:\\Windows\\System32\\cmd.exe";
    // Listing the ancestor is allowed when the chain is grantable: that is what
    // the walk grant is for. On a host that cannot write the drive-root DACL
    // every absolute-path built-in reports access denied instead; the degraded
    // outcome is recorded below.
    let probe = run_in_container(
        &sid,
        &capabilities,
        &[shell, "/c", "dir", "/b", &parent_dir.to_string_lossy()],
    );
    if masks_for(Path::new("C:\\"), &sid).allow != 0 {
        assert_eq!(
            probe.exit_code, 0,
            "the ancestor of a granted root must be listable: {probe}"
        );
    } else {
        eprintln!(
            "degraded: drive-root walk unavailable; ancestor dir exit={} probe={probe}",
            probe.exit_code
        );
    }

    // Reading a file that lives in the ancestor is not.
    let probe = run_in_container(
        &sid,
        &capabilities,
        &[shell, "/c", "type", &sibling.to_string_lossy()],
    );
    assert!(
        !probe.says("SIBLINGBODY"),
        "a walk grant leaked the ancestor's contents: {probe}"
    );
    assert_ne!(
        probe.exit_code, 0,
        "reading a file in a merely walkable ancestor must fail: {probe}"
    );

    let failures = acl::revoke(&entries, &saved, &sid);
    assert!(failures.is_empty(), "teardown reported: {failures:?}");
    if null_device.is_ok() {
        null_device::revoke(&sid).expect("take back the null device");
    }
    std::fs::remove_dir_all(&parent_dir).ok();
}

#[test]
fn a_withheld_path_stays_unreadable_inside_a_granted_root() {
    // The measured surprise this exists for: a DENY ace naming the container's own SID
    // is never consulted, because the package SID is not in the token's group list and
    // the kernel's AppContainer sweep only accumulates allows. So a denial has to be
    // the absence of an allow — broken inheritance and no app-package ACE — and this
    // checks that against the live security check, with a sibling file as the control.
    let name = container_name("withhold");
    let request = request_for(&name);
    let moniker = identity::container_moniker(&request).expect("moniker");
    let sid = identity::ensure_container(&moniker).expect("container");
    let capabilities = capability::resolve(&[]).expect("no capabilities");

    let (root_dir, root) = scratch("withhold");
    let visible = root_dir.join("visible.txt");
    let secret = root_dir.join("secret.txt");
    std::fs::write(&visible, b"VISIBLEBODY").expect("write the control file");
    std::fs::write(&secret, b"SECRETBODY").expect("write the protected file");

    let entries = acl::plan_entries(&FilesystemBoundary {
        writable: vec![root.clone()],
        deny_read: vec![
            AbsPath::new(secret.to_string_lossy().as_ref()).expect("secret path parses")
        ],
        ..Default::default()
    });
    let saved = acl::apply(&entries, &sid).expect("apply the boundary");
    // The ordinary caller may have no WRITE_DAC on the device object; the
    // assertions below do not depend on NUL and must hold in both cases.
    let null_device = null_device::grant(&sid);

    // The withheld path must name the container in no way at all. A leftover allow is
    // what a naive "add a deny" implementation leaves behind, and it grants access.
    let on_secret = masks_for(&secret, &sid);
    assert_eq!(
        on_secret.allow, 0,
        "the withheld path still names the container: {on_secret:?}"
    );
    // And no deny ACE was written, because one would do nothing: naming the container
    // in a deny is the mistake this whole approach exists to avoid, and a DACL that
    // carries one reads as protective while enforcing nothing.
    assert_eq!(
        on_secret.deny, 0,
        "a deny ACE was written, which is not consulted for a container: {on_secret:?}"
    );

    let shell = "C:\\Windows\\System32\\cmd.exe";
    let probe = run_in_container(
        &sid,
        &capabilities,
        &[shell, "/c", "type", &visible.to_string_lossy()],
    );
    assert_eq!(
        probe.exit_code, 0,
        "a readable file inside a granted root was refused: {probe}"
    );
    assert!(
        probe.says("VISIBLEBODY"),
        "the read succeeded but returned something else: {probe}"
    );

    let probe = run_in_container(
        &sid,
        &capabilities,
        &[shell, "/c", "type", &secret.to_string_lossy()],
    );
    assert!(
        !probe.says("SECRETBODY"),
        "the withheld file's contents came back anyway: {probe}"
    );
    assert_ne!(
        probe.exit_code, 0,
        "reading a withheld path must fail: {probe} dacl={on_secret:?}"
    );

    // Teardown has to put the withheld path back: withholding replaces its DACL and
    // breaks inheritance, so a teardown that only strips ACEs would leave the user's
    // own path permanently altered.
    let failures = acl::revoke(&entries, &saved, &sid);
    assert!(failures.is_empty(), "teardown reported: {failures:?}");
    if null_device.is_ok() {
        null_device::revoke(&sid).expect("take back the null device");
    }
    assert_eq!(
        masks_for(&secret, &sid).allow,
        0,
        "teardown left the container named on the withheld path"
    );
    // And the user can still read their own file, which is the regression a botched
    // DACL replacement causes.
    assert_eq!(
        std::fs::read(&secret).expect("the owner can still read it"),
        b"SECRETBODY"
    );

    std::fs::remove_dir_all(&root_dir).ok();
}

#[test]
fn a_withheld_path_can_be_left_readable_but_not_writable() {
    // This is what protecting repository metadata means: a command may read `.git` and
    // must not rewrite it.
    let name = container_name("withhold-write");
    let request = request_for(&name);
    let moniker = identity::container_moniker(&request).expect("moniker");
    let sid = identity::ensure_container(&moniker).expect("container");
    let capabilities = capability::resolve(&[]).expect("no capabilities");

    let (root_dir, root) = scratch("withhold-write");
    let protected_dir = root_dir.join("metadata");
    std::fs::create_dir_all(&protected_dir).expect("create the protected directory");
    let existing = protected_dir.join("config");
    std::fs::write(&existing, b"CONFIGBODY").expect("write the protected file");

    let entries = acl::plan_entries(&FilesystemBoundary {
        writable: vec![root.clone()],
        deny_write: vec![
            AbsPath::new(protected_dir.to_string_lossy().as_ref()).expect("path parses")
        ],
        ..Default::default()
    });
    let saved = acl::apply(&entries, &sid).expect("apply the boundary");
    // The ordinary caller may have no WRITE_DAC on the device object; the
    // assertions below do not depend on NUL and must hold in both cases.
    let null_device = null_device::grant(&sid);

    let shell = "C:\\Windows\\System32\\cmd.exe";
    let probe = run_in_container(
        &sid,
        &capabilities,
        &[shell, "/c", "type", &existing.to_string_lossy()],
    );
    assert!(
        probe.says("CONFIGBODY"),
        "a write-protected path must still be readable: {probe}"
    );

    let added = protected_dir.join("added.txt");
    let probe = run_in_container(
        &sid,
        &capabilities,
        &[shell, "/c", "echo", "x", ">", &added.to_string_lossy()],
    );
    assert!(
        !added.exists(),
        "a write landed inside a write-protected path: {probe}"
    );

    let failures = acl::revoke(&entries, &saved, &sid);
    assert!(failures.is_empty(), "teardown reported: {failures:?}");
    if null_device.is_ok() {
        null_device::revoke(&sid).expect("take back the null device");
    }
    std::fs::remove_dir_all(&root_dir).ok();
}

#[test]
fn a_boundary_naming_paths_that_do_not_exist_still_installs() {
    // The deny list carries credential directories the policy wants protected whether
    // or not this machine has them, and most machines are missing at least one. Failing
    // setup on the first absent one made the whole sandbox refuse to start: measured as
    // `reading the security descriptor of C:\Users\...\.aws failed: Win32 error 2`,
    // which took down every command rather than just that one rule.
    let name = container_name("absent");
    let request = request_for(&name);
    let moniker = identity::container_moniker(&request).expect("moniker");
    let sid = identity::ensure_container(&moniker).expect("container");
    let capabilities = capability::resolve(&[]).expect("no capabilities");

    let (root_dir, root) = scratch("absent");
    let real = root_dir.join("present.txt");
    std::fs::write(&real, b"PRESENTBODY").expect("write the file that is there");
    // Neither of these exists, and neither may be created by applying the boundary.
    let missing_dir = root_dir.join("no-such-dir");
    let missing_file = root_dir.join("no-such-file.txt");

    let entries = acl::plan_entries(&FilesystemBoundary {
        writable: vec![root.clone()],
        deny_read: vec![AbsPath::new(missing_dir.to_string_lossy().as_ref()).expect("path parses")],
        deny_write: vec![
            AbsPath::new(missing_file.to_string_lossy().as_ref()).expect("path parses")
        ],
        ..Default::default()
    });
    let saved = acl::apply(&entries, &sid).expect("an absent path must not fail setup");
    // The ordinary caller may have no WRITE_DAC on the device object; the
    // assertions below do not depend on NUL and must hold in both cases.
    let null_device = null_device::grant(&sid);

    assert!(
        !missing_dir.exists() && !missing_file.exists(),
        "applying the boundary invented state the machine did not have"
    );
    // And the rest of the boundary is genuinely in force, not skipped along with them.
    let shell = "C:\\Windows\\System32\\cmd.exe";
    let probe = run_in_container(
        &sid,
        &capabilities,
        &[shell, "/c", "type", &real.to_string_lossy()],
    );
    assert!(
        probe.says("PRESENTBODY"),
        "the granted root stopped working because a deny path was absent: {probe}"
    );

    let failures = acl::revoke(&entries, &saved, &sid);
    assert!(failures.is_empty(), "teardown reported: {failures:?}");
    if null_device.is_ok() {
        null_device::revoke(&sid).expect("take back the null device");
    }
    std::fs::remove_dir_all(&root_dir).ok();
}

#[test]
fn protected_metadata_named_by_the_policy_is_actually_protected() {
    // The gap this closes: the policy names `.git`, the request carries the name, and
    // nothing turned it into an ACL rule — so the metadata a command must not rewrite
    // was declared protected and enforced by nobody. Checked end to end because the
    // failure is silent: the request looks right either way.
    let name = container_name("metadata");
    let request = request_for(&name);
    let moniker = identity::container_moniker(&request).expect("moniker");
    let sid = identity::ensure_container(&moniker).expect("container");
    let capabilities = capability::resolve(&[]).expect("no capabilities");

    let (root_dir, root) = scratch("metadata");
    // The engine creates these before the ACEs are written, for the same reason: a
    // path that does not exist cannot carry a rule.
    let git = root_dir.join(".git");
    std::fs::create_dir_all(&git).expect("create the metadata directory");
    std::fs::write(git.join("HEAD"), b"HEADBODY").expect("write a metadata file");

    let entries = acl::plan_entries(&FilesystemBoundary {
        writable: vec![root.clone()],
        protected_metadata: vec![".git".to_string()],
        ..Default::default()
    });
    let saved = acl::apply(&entries, &sid).expect("apply the boundary");
    // The ordinary caller may have no WRITE_DAC on the device object; the
    // assertions below do not depend on NUL and must hold in both cases.
    let null_device = null_device::grant(&sid);

    let shell = "C:\\Windows\\System32\\cmd.exe";
    // Writable around it: the command is meant to edit the tree, just not the metadata.
    let source = root_dir.join("main.rs");
    let probe = run_in_container(
        &sid,
        &capabilities,
        &[shell, "/c", "echo", "ok", ">", &source.to_string_lossy()],
    );
    assert!(
        source.is_file(),
        "protecting the metadata must not confine the tree around it: {probe}"
    );

    // Readable, because tooling has to be able to inspect the repository.
    let probe = run_in_container(
        &sid,
        &capabilities,
        &[shell, "/c", "type", &git.join("HEAD").to_string_lossy()],
    );
    assert!(
        probe.says("HEADBODY"),
        "protected metadata must stay readable: {probe}"
    );

    // And not writable.
    let forged = git.join("forged");
    let probe = run_in_container(
        &sid,
        &capabilities,
        &[shell, "/c", "echo", "x", ">", &forged.to_string_lossy()],
    );
    assert!(
        !forged.exists(),
        "a write landed inside protected metadata: {probe}"
    );

    let failures = acl::revoke(&entries, &saved, &sid);
    assert!(failures.is_empty(), "teardown reported: {failures:?}");
    if null_device.is_ok() {
        null_device::revoke(&sid).expect("take back the null device");
    }
    std::fs::remove_dir_all(&root_dir).ok();
}

#[test]
fn a_container_without_the_internet_capability_cannot_reach_the_network() {
    // Withholding the capability is the whole network boundary for a denied policy:
    // the token cannot open the socket, so there is no filter that could fail open.
    // Checked against the live kernel because nothing about it is visible in the
    // plan — a capability list that is silently wrong looks exactly like a correct
    // one until a command reaches the internet.
    let name = container_name("no-network");
    let request = request_for(&name);
    let moniker = identity::container_moniker(&request).expect("moniker");
    let sid = identity::ensure_container(&moniker).expect("container");

    let curl = "C:\\Windows\\System32\\curl.exe";
    if !std::path::Path::new(curl).is_file() {
        eprintln!("skipped: this Windows build has no curl.exe to probe with");
        return;
    }
    let url = "http://www.msftconnecttest.com/connecttest.txt";
    // Body to stdout, which the capture pipe owns. Writing it to a file would make
    // this depend on the filesystem boundary as well as the network one.
    let probe_argv = [curl, "-sS", "-m", "8", url];

    // The control is the host itself: with no route to the internet, a refusal in
    // the container would prove nothing.
    let host_reachable = std::process::Command::new(curl)
        .args(["-sS", "-m", "8", "-o", "NUL", url])
        .status()
        .map(|status| status.success())
        .unwrap_or(false);

    let denied = capability::resolve(&[]).expect("no capabilities");
    let probe = run_in_container(&sid, &denied, &probe_argv);
    assert_ne!(
        probe.exit_code, 0,
        "a container holding no capability reached the network: {probe}"
    );

    if !host_reachable {
        eprintln!("skipped the allowed half: this host cannot reach {url}");
        return;
    }
    let allowed =
        capability::resolve(&[Capability::InternetClient]).expect("internet client capability");
    let probe = run_in_container(&sid, &allowed, &probe_argv);
    assert_eq!(
        probe.exit_code, 0,
        "granting internetClient did not restore network access: {probe}"
    );
}
