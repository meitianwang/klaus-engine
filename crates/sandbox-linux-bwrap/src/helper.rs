//! Early native helper dispatch and trusted adjacent-launcher resolution.
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
mod fd_mount;
mod proxy_lifecycle;
mod proxy_routing;
mod supervision;
pub mod wslg;
const HELPER: &str = "--klaus-linux-sandbox";
const BWRAP: &str = "--klaus-bubblewrap";
const SETUP_FAILURE: i32 = 189;
static ENTRY_POINT: AtomicBool = AtomicBool::new(false);

/// Locate the shipped helper without consulting PATH or user backend settings.
pub fn resolve_executable() -> Result<PathBuf, String> {
    let current = std::env::current_exe().map_err(|error| error.to_string())?;
    if ENTRY_POINT.load(Ordering::SeqCst) {
        return Ok(current);
    }
    let directory = current
        .parent()
        .ok_or("executable has no parent directory")?;
    let mut candidates = vec![directory.join("klaus-sandbox-linux")];
    if directory.file_name().is_some_and(|name| name == "deps") {
        if let Some(parent) = directory.parent() {
            candidates.push(parent.join("klaus-sandbox-linux"));
        }
    }
    candidates.into_iter().find(|path| path.metadata().is_ok_and(|metadata|
        metadata.is_file() && metadata.permissions().mode() & 0o111 != 0))
        .ok_or_else(|| "The shipped Linux sandbox helper is missing. Ship klaus-sandbox-linux beside the embedding executable or call sandbox::linux::dispatch before application startup.".into())
}

/// Call in an engine executable before its runtime or process supervisor starts.
/// Internal helper invocations never reach the application's normal entry point.
pub fn dispatch() {
    #[cfg(target_os = "linux")]
    {
        let arguments: Vec<_> = std::env::args_os().collect();
        let marker = arguments.get(1).and_then(|argument| argument.to_str());
        let result = match marker {
            Some(BWRAP) => imp::run_outer(&arguments[2..]),
            Some(HELPER) => imp::run_inner(&arguments[2..]),
            _ => {
                ENTRY_POINT.store(true, Ordering::SeqCst);
                return;
            }
        };
        if let Err(error) = result {
            eprintln!("Linux sandbox setup failed: {error}");
        }
        std::process::exit(SETUP_FAILURE);
    }
}

#[cfg(target_os = "linux")]
mod imp {
    use std::ffi::OsString;
    use std::fs::{File, OpenOptions};
    use std::io;
    use std::os::fd::AsRawFd;
    use std::os::unix::fs::OpenOptionsExt;
    use std::os::unix::process::CommandExt;
    use std::path::PathBuf;
    use std::process::Command;

    pub(super) fn run_outer(arguments: &[OsString]) -> Result<(), String> {
        use super::supervision::{BwrapArgs, ProtectedCreateTarget, SyntheticMountTarget};
        let mut protected_create_targets = Vec::new();
        let mut synthetic_mount_targets = Vec::new();
        let mut denied_files = Vec::new();
        let mut synthetic_files = Vec::new();
        let mut index = 0;
        while let Some(argument) = arguments.get(index) {
            match argument.to_str() {
                Some("--") => {
                    index += 1;
                    break;
                }
                Some("--protect-missing") | Some("--deny-file") | Some("--synthetic-file") => {
                    let path =
                        PathBuf::from(arguments.get(index + 1).ok_or("missing protected path")?);
                    if argument == "--protect-missing" {
                        protected_create_targets.push(ProtectedCreateTarget::missing(&path));
                    } else if argument == "--synthetic-file" {
                        synthetic_files.push(path);
                    } else {
                        denied_files.push(path);
                    }
                    index += 2;
                }
                Some("--fail") => {
                    return Err(arguments
                        .get(index + 1)
                        .map(|value| value.to_string_lossy().into_owned())
                        .unwrap_or_else(|| "unrepresentable filesystem policy".into()));
                }
                _ => return Err("invalid namespace helper invocation".into()),
            }
        }
        let separator = arguments[index..]
            .iter()
            .position(|argument| argument == "--")
            .map(|position| index + position)
            .ok_or("namespace target separator missing")?;
        let mut mounts = arguments[index..separator].to_vec();
        if !super::supervision::proc_mount_supported() {
            if let Some(position) = mounts.iter().position(|argument| argument == "--proc") {
                mounts.drain(position..position + 2);
            }
        }

        let mut target = arguments[separator + 1..].to_vec();
        let mut preserved_files = Vec::new();
        let mut verify_mounts = Vec::new();
        let mut position = 0;
        while position < mounts.len() {
            if mounts[position] == "--bind" || mounts[position] == "--ro-bind" {
                let source = mounts.get(position + 1).ok_or("bind source missing")?;
                let destination = mounts.get(position + 2).ok_or("bind destination missing")?;
                let file = OpenOptions::new()
                    .read(true)
                    .custom_flags(libc::O_PATH)
                    .open(source)
                    .map_err(|error| error.to_string())?;
                let descriptor = file.as_raw_fd();
                verify_mounts.push(format!(
                    "{descriptor}:{}",
                    destination
                        .to_str()
                        .ok_or("mount destination must be UTF-8")?
                ));
                mounts[position + 1] = OsString::from(format!("/proc/self/fd/{descriptor}"));
                preserved_files.push(file);
                position += 3;
            } else {
                position += 1;
            }
        }
        for path in synthetic_files {
            if let Ok(metadata) = path.symlink_metadata() {
                if metadata.is_dir() {
                    synthetic_mount_targets.push(SyntheticMountTarget::existing_empty_directory(
                        &path, &metadata,
                    ));
                    mounts.extend([
                        OsString::from("--perms"),
                        OsString::from("555"),
                        OsString::from("--tmpfs"),
                        path.clone().into_os_string(),
                        OsString::from("--remount-ro"),
                        path.into_os_string(),
                    ]);
                    continue;
                }
                synthetic_mount_targets
                    .push(SyntheticMountTarget::existing_empty_file(&path, &metadata));
            } else {
                synthetic_mount_targets.push(SyntheticMountTarget::missing(&path));
            }
            let file = File::open("/dev/null").map_err(|error| error.to_string())?;
            mounts.extend([
                OsString::from("--ro-bind-data"),
                OsString::from(file.as_raw_fd().to_string()),
                path.into_os_string(),
            ]);
            preserved_files.push(file);
        }
        for path in denied_files {
            let file = File::open("/dev/null").map_err(|error| error.to_string())?;
            mounts.extend([
                OsString::from("--perms"),
                OsString::from("000"),
                OsString::from("--ro-bind-data"),
                OsString::from(file.as_raw_fd().to_string()),
                path.into_os_string(),
            ]);
            preserved_files.push(file);
        }
        let inner_separator = target
            .iter()
            .position(|argument| argument == "--")
            .ok_or("inner command separator missing")?;
        let mut options = Vec::new();
        for verification in verify_mounts {
            options.extend([
                OsString::from("--verify-fd-mount"),
                OsString::from(verification),
            ]);
        }
        if target
            .iter()
            .take(inner_separator)
            .any(|argument| argument == "--managed-proxy")
        {
            let (specification, controls) = super::proxy_routing::prepare_host_proxy_route_spec()
                .map_err(|error| error.to_string())?;
            options.extend([
                OsString::from("--proxy-route-spec"),
                OsString::from(specification),
            ]);
            preserved_files.extend(controls);
        }
        target.splice(inner_separator..inner_separator, options);
        if !protected_create_targets.is_empty() || !synthetic_mount_targets.is_empty() {
            let registry = super::supervision::synthetic_mount_registry_root();
            std::fs::create_dir_all(&registry).map_err(|error| error.to_string())?;
            mounts.extend([
                OsString::from("--ro-bind"),
                registry.clone().into_os_string(),
                registry.into_os_string(),
            ]);
        }
        let mut args = vec![OsString::from("klaus-bwrap")];
        args.extend(mounts);
        args.push(OsString::from("--"));
        args.extend(target);
        super::supervision::run_or_exec_bwrap(BwrapArgs {
            args,
            preserved_files,
            synthetic_mount_targets,
            protected_create_targets,
        });
    }

    pub(super) fn run_inner(arguments: &[OsString]) -> Result<(), String> {
        let network_allowed = match arguments.first().and_then(|argument| argument.to_str()) {
            Some("--network-allowed") => true,
            Some("--network-restricted") => false,
            _ => return Err("invalid syscall helper policy".into()),
        };
        let mut index = 1;
        let mut verifications = Vec::new();
        let mut proxy_specification = None;
        let mut managed_proxy = false;
        let mut allow_unix_sockets = false;
        let mut legacy_landlock = false;
        let mut full_filesystem_write = false;
        while let Some(argument) = arguments.get(index) {
            match argument.to_str() {
                Some("--") => {
                    index += 1;
                    break;
                }
                Some("--verify-fd-mount") | Some("--proxy-route-spec") => {
                    let value = arguments
                        .get(index + 1)
                        .and_then(|value| value.to_str())
                        .ok_or("missing helper policy value")?;
                    if argument == "--verify-fd-mount" {
                        verifications.push(value.to_owned());
                    } else {
                        proxy_specification = Some(value);
                    }
                    index += 2;
                }
                Some("--use-legacy-landlock") => {
                    legacy_landlock = true;
                    index += 1;
                }
                Some("--full-filesystem-write") => {
                    full_filesystem_write = true;
                    index += 1;
                }
                Some("--managed-proxy") => {
                    managed_proxy = true;
                    index += 1;
                }
                Some("--allow-unix-sockets") => {
                    allow_unix_sockets = true;
                    index += 1;
                }
                _ => return Err("invalid syscall helper policy option".into()),
            }
        }
        if index >= arguments.len() {
            return Err("syscall helper has no command".into());
        }
        if legacy_landlock {
            if !full_filesystem_write {
                return Err("filesystem-restricted execution requires bubblewrap to isolate app-server sockets".into());
            }
            if !verifications.is_empty() || proxy_specification.is_some() {
                return Err(
                    "legacy execution cannot accept inner namespace verification state".into(),
                );
            }
            // Full filesystem writes leave no Landlock filesystem rule to
            // install. Network-disabled and managed requests still receive the
            // in-process restricted seccomp policy before exec.
            if !network_allowed || managed_proxy {
                install_filter(false, false, allow_unix_sockets)?;
            }
        } else {
            super::fd_mount::verify_fd_mounts(&verifications).map_err(|error| error.to_string())?;
            if managed_proxy {
                super::proxy_routing::activate_proxy_routes_in_netns(
                    proxy_specification.ok_or("missing proxy route specification")?,
                )
                .map_err(|error| error.to_string())?;
            }
            install_filter(network_allowed, managed_proxy, allow_unix_sockets)?;
        }
        let error = Command::new(&arguments[index])
            .args(&arguments[index + 1..])
            .exec();
        Err(format!("sandboxed command could not start: {error}"))
    }

    #[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
    fn instruction(code: u16, jt: u8, jf: u8, value: u32) -> libc::sock_filter {
        libc::sock_filter {
            code,
            jt,
            jf,
            k: value,
        }
    }

    fn filter_program(
        network_allowed: bool,
        managed_proxy: bool,
        allow_unix_sockets: bool,
    ) -> Result<Vec<libc::sock_filter>, String> {
        #[cfg(target_arch = "x86_64")]
        let architecture = 0xc000_003e;
        #[cfg(target_arch = "aarch64")]
        let architecture = 0xc000_00b7;
        #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
        return Err(format!(
            "this Linux architecture cannot install the required syscall policy (network={network_allowed}, proxy={managed_proxy}, unix={allow_unix_sockets})"
        ));
        #[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
        {
            const LOAD: u16 = 0x20;
            const EQUAL: u16 = 0x15;
            const RETURN: u16 = 0x06;
            const DENY: u32 = 0x0005_0000 | libc::EPERM as u32;
            let mut program = vec![
                instruction(LOAD, 0, 0, 4),
                instruction(EQUAL, 1, 0, architecture),
                instruction(RETURN, 0, 0, 0x8000_0000),
                instruction(LOAD, 0, 0, 0),
            ];
            #[cfg(target_arch = "x86_64")]
            program.extend([
                instruction(0x35, 0, 1, 0x4000_0000),
                instruction(RETURN, 0, 0, DENY),
            ]);
            let mut denied = vec![
                libc::SYS_io_uring_setup,
                libc::SYS_io_uring_enter,
                libc::SYS_io_uring_register,
            ];
            if !network_allowed {
                denied.extend([
                    libc::SYS_ptrace,
                    libc::SYS_process_vm_readv,
                    libc::SYS_process_vm_writev,
                ]);
            }
            if !network_allowed && !managed_proxy {
                denied.extend([
                    libc::SYS_connect,
                    libc::SYS_accept,
                    libc::SYS_accept4,
                    libc::SYS_bind,
                    libc::SYS_listen,
                    libc::SYS_getpeername,
                    libc::SYS_getsockname,
                    libc::SYS_shutdown,
                    libc::SYS_sendto,
                    libc::SYS_sendmmsg,
                    libc::SYS_recvmmsg,
                    libc::SYS_getsockopt,
                    libc::SYS_setsockopt,
                ]);
            }
            for syscall in denied {
                program.push(instruction(EQUAL, 0, 1, syscall as u32));
                program.push(instruction(RETURN, 0, 0, DENY));
            }
            for syscall in [libc::SYS_socket, libc::SYS_socketpair] {
                if managed_proxy {
                    let socketpair = syscall == libc::SYS_socketpair;
                    let mut families = if socketpair {
                        vec![libc::AF_UNIX]
                    } else {
                        vec![libc::AF_INET, libc::AF_INET6]
                    };
                    if !socketpair && allow_unix_sockets {
                        families.push(libc::AF_UNIX);
                    }
                    let count = families.len();
                    program.push(instruction(EQUAL, 0, (count + 3) as u8, syscall as u32));
                    program.push(instruction(LOAD, 0, 0, 16));
                    for (index, family) in families.into_iter().enumerate() {
                        program.push(instruction(EQUAL, (count - index) as u8, 0, family as u32));
                    }
                    program.push(instruction(RETURN, 0, 0, DENY));
                    program.push(instruction(LOAD, 0, 0, 0));
                } else {
                    program.push(instruction(EQUAL, 0, 4, syscall as u32));
                    program.push(instruction(LOAD, 0, 0, 16));
                    program.push(if network_allowed {
                        instruction(EQUAL, 0, 1, libc::AF_VSOCK as u32)
                    } else {
                        instruction(EQUAL, 1, 0, libc::AF_UNIX as u32)
                    });
                    program.push(instruction(RETURN, 0, 0, DENY));
                    program.push(instruction(LOAD, 0, 0, 0));
                }
            }
            program.push(instruction(RETURN, 0, 0, 0x7fff_0000));
            Ok(program)
        }
    }

    fn install_filter(
        network_allowed: bool,
        managed_proxy: bool,
        allow_unix_sockets: bool,
    ) -> Result<(), String> {
        let mut program = filter_program(network_allowed, managed_proxy, allow_unix_sockets)?;
        let filter = libc::sock_fprog {
            len: u16::try_from(program.len()).map_err(|error| error.to_string())?,
            filter: program.as_mut_ptr(),
        };
        // This fresh helper owns the filter; every descendant inherits it.
        if unsafe { libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) } != 0
            || unsafe { libc::prctl(libc::PR_SET_SECCOMP, 2, &filter) } != 0
        {
            return Err(format!(
                "cannot install syscall sandbox: {}",
                io::Error::last_os_error()
            ));
        }
        Ok(())
    }

    #[cfg(all(test, any(target_arch = "x86_64", target_arch = "aarch64")))]
    mod tests {
        use super::filter_program;

        fn action(
            network: bool,
            proxy: bool,
            unix: bool,
            syscall: libc::c_long,
            domain: i32,
        ) -> u32 {
            let program = filter_program(network, proxy, unix).unwrap();
            #[cfg(target_arch = "x86_64")]
            let architecture = 0xc000_003e;
            #[cfg(target_arch = "aarch64")]
            let architecture = 0xc000_00b7;
            let mut accumulator = 0;
            let mut pointer = 0;
            for _ in 0..program.len() * 2 {
                let instruction = &program[pointer];
                match instruction.code {
                    0x20 => {
                        accumulator = match instruction.k {
                            0 => syscall as u32,
                            4 => architecture,
                            16 => domain as u32,
                            offset => panic!("unexpected syscall data offset {offset}"),
                        }
                    }
                    0x15 => {
                        pointer += if accumulator == instruction.k {
                            instruction.jt as usize
                        } else {
                            instruction.jf as usize
                        }
                    }
                    0x35 => {
                        pointer += if accumulator >= instruction.k {
                            instruction.jt as usize
                        } else {
                            instruction.jf as usize
                        }
                    }
                    0x06 => return instruction.k,
                    code => panic!("unexpected filter instruction {code}"),
                }
                pointer += 1;
            }
            panic!("syscall filter did not return an action")
        }

        #[test]
        fn restricted_network_denies_connections_but_keeps_local_socketpairs() {
            assert_eq!(
                action(false, false, false, libc::SYS_connect, 0),
                0x0005_0000 | libc::EPERM as u32
            );
            assert_eq!(
                action(false, false, false, libc::SYS_socketpair, libc::AF_UNIX),
                0x7fff_0000
            );
            assert_eq!(
                action(false, false, false, libc::SYS_socket, libc::AF_INET),
                0x0005_0000 | libc::EPERM as u32
            );
        }

        #[test]
        fn unrestricted_network_retains_the_host_bridge_syscall_boundary() {
            assert_eq!(
                action(true, false, false, libc::SYS_socket, libc::AF_INET),
                0x7fff_0000
            );
            assert_eq!(
                action(true, false, false, libc::SYS_socket, libc::AF_VSOCK),
                0x0005_0000 | libc::EPERM as u32
            );
            assert_eq!(
                action(true, false, false, libc::SYS_io_uring_setup, 0),
                0x0005_0000 | libc::EPERM as u32
            );
        }

        #[test]
        fn proxy_namespace_allows_ip_without_granting_arbitrary_unix_sockets() {
            assert_eq!(
                action(false, true, false, libc::SYS_socket, libc::AF_INET6),
                0x7fff_0000
            );
            assert_eq!(
                action(false, true, false, libc::SYS_socket, libc::AF_UNIX),
                0x0005_0000 | libc::EPERM as u32
            );
            assert_eq!(
                action(false, true, true, libc::SYS_socket, libc::AF_UNIX),
                0x7fff_0000
            );
            assert_eq!(
                action(false, true, false, libc::SYS_ptrace, 0),
                0x0005_0000 | libc::EPERM as u32
            );
        }
    }
}
