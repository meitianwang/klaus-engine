//! Windows-only parent ownership for the browser sidecar's four broker pipes.
//!
//! Bootstrap construction is kept platform-independent so its byte contract can
//! be covered on every development host.  The Win32 portion is deliberately a
//! closed server: callers cannot select a pipe name, access mask, instance
//! count, DACL, or connection timeout.

#![cfg_attr(not(windows), allow(dead_code))]

use std::fmt;

use rand::RngCore;

use crate::runtime_values::RuntimeString;

const ENV_KEY_PREFIX: &[u8] = b"KLAUS_INTERNAL_BROWSER_BROKER_";
const ARG_PREFIX: &[u8] = b"--klaus-browser-broker-env-key=";
const VALUE_PREFIX: &[u8] = b"v1.";
const PIPE_PREFIX: &[u8] = b"\\\\.\\pipe\\klaus-browser-v1-";
const PIPE_LANES: [&[u8]; 4] = [b"sync-c2h", b"sync-h2c", b"mux-c2h", b"mux-h2c"];
const KEY_ATTEMPTS: usize = 16;
const PIPE_BUFFER_BYTES: u32 = 65_536;
const PIPE_DEFAULT_WAIT_MS: u32 = 5_000;

#[derive(Clone, Debug)]
pub(super) struct WindowsBrowserBootstrapSeed {
    generation: u64,
    nonce: [u8; 32],
    key_attempts: [[u8; 32]; KEY_ATTEMPTS],
}

impl WindowsBrowserBootstrapSeed {
    pub(super) fn generate(generation: u64) -> Result<Self, WindowsBrowserBrokerError> {
        if generation == 0 {
            return Err(WindowsBrowserBrokerError::InvalidGeneration);
        }
        let mut nonce = [0_u8; 32];
        rand::rngs::OsRng
            .try_fill_bytes(&mut nonce)
            .map_err(|_| WindowsBrowserBrokerError::EntropyUnavailable)?;
        let mut key_attempts = [[0_u8; 32]; KEY_ATTEMPTS];
        for attempt in &mut key_attempts {
            rand::rngs::OsRng
                .try_fill_bytes(attempt)
                .map_err(|_| WindowsBrowserBrokerError::EntropyUnavailable)?;
        }
        Ok(Self {
            generation,
            nonce,
            key_attempts,
        })
    }

    #[cfg(test)]
    fn from_parts(
        generation: u64,
        nonce: [u8; 32],
        key_attempts: [[u8; 32]; KEY_ATTEMPTS],
    ) -> Self {
        Self {
            generation,
            nonce,
            key_attempts,
        }
    }
}

impl Drop for WindowsBrowserBootstrapSeed {
    fn drop(&mut self) {
        self.nonce.fill(0);
        for attempt in &mut self.key_attempts {
            attempt.fill(0);
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct PreparedWindowsBrowserBootstrap {
    nonce: [u8; 32],
    environment_key: RuntimeString,
    environment_value: RuntimeString,
    argv_token: RuntimeString,
    pipe_names: [RuntimeString; 4],
}

impl PreparedWindowsBrowserBootstrap {
    pub(super) fn nonce(&self) -> [u8; 32] {
        self.nonce
    }

    pub(super) fn environment_key(&self) -> RuntimeString {
        self.environment_key.clone()
    }

    pub(super) fn environment_value(&self) -> RuntimeString {
        self.environment_value.clone()
    }

    pub(super) fn argv_token(&self) -> RuntimeString {
        self.argv_token.clone()
    }

    #[cfg(windows)]
    fn pipe_names(&self) -> &[RuntimeString; 4] {
        &self.pipe_names
    }
}

impl Drop for PreparedWindowsBrowserBootstrap {
    fn drop(&mut self) {
        self.nonce.fill(0);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum WindowsBrowserBrokerError {
    InvalidGeneration,
    EntropyUnavailable,
    BootstrapKeyCollision,
    EnvironmentComparison,
    PipeSecurity,
    PipeCreation,
    BootstrapTimedOut,
    PipeConnection,
    PipeClientIdentity,
}

impl WindowsBrowserBrokerError {
    pub(super) const fn stable_message(self) -> &'static str {
        match self {
            Self::InvalidGeneration => "browser connection generation is invalid",
            Self::EntropyUnavailable => "BrowserBootstrapEntropyUnavailable",
            Self::BootstrapKeyCollision => "BrowserBootstrapKeyCollision",
            Self::EnvironmentComparison => "browser bootstrap environment key comparison failed",
            Self::PipeSecurity => "browser broker named-pipe security setup failed",
            Self::PipeCreation => "browser broker named-pipe creation failed",
            Self::BootstrapTimedOut => "Browser broker bootstrap timed out.",
            Self::PipeConnection => "Browser broker native pipe binding failed.",
            Self::PipeClientIdentity => "Browser broker native pipe binding failed.",
        }
    }
}

impl fmt::Display for WindowsBrowserBrokerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.stable_message())
    }
}

impl std::error::Error for WindowsBrowserBrokerError {}

/// Select the one private environment key after the caller and parent key sets
/// have been fixed.  The supplied comparator is the target's case-insensitive
/// equality operation; an indeterminate comparison fails closed.
pub(super) fn prepare_windows_browser_bootstrap(
    seed: &WindowsBrowserBootstrapSeed,
    existing_keys: &[Vec<u16>],
    mut equal_ignore_case: impl FnMut(&[u16], &[u16]) -> Result<bool, ()>,
) -> Result<PreparedWindowsBrowserBootstrap, WindowsBrowserBrokerError> {
    if seed.generation == 0 {
        return Err(WindowsBrowserBrokerError::InvalidGeneration);
    }

    let mut selected_key = None;
    for attempt in &seed.key_attempts {
        let mut candidate = ENV_KEY_PREFIX.to_vec();
        append_hex(&mut candidate, attempt, b"0123456789ABCDEF");
        let candidate_wide = candidate
            .iter()
            .map(|byte| u16::from(*byte))
            .collect::<Vec<_>>();
        let mut collides = false;
        for key in existing_keys {
            if equal_ignore_case(key, &candidate_wide)
                .map_err(|()| WindowsBrowserBrokerError::EnvironmentComparison)?
            {
                collides = true;
                break;
            }
        }
        if !collides {
            selected_key = Some(candidate);
            break;
        }
    }
    let selected_key = selected_key.ok_or(WindowsBrowserBrokerError::BootstrapKeyCollision)?;

    let pipe_name_bytes = expected_pipe_names(seed.generation, &seed.nonce);
    let mut payload = Vec::new();
    payload.extend_from_slice(&seed.generation.to_be_bytes());
    payload.extend_from_slice(&seed.nonce);
    for name in &pipe_name_bytes {
        let length =
            u16::try_from(name.len()).map_err(|_| WindowsBrowserBrokerError::PipeCreation)?;
        payload.extend_from_slice(&length.to_be_bytes());
        payload.extend_from_slice(name);
    }
    let mut value = VALUE_PREFIX.to_vec();
    value.extend(encode_base64url(&payload));
    payload.fill(0);

    let mut argv = ARG_PREFIX.to_vec();
    argv.extend_from_slice(&selected_key);
    Ok(PreparedWindowsBrowserBootstrap {
        nonce: seed.nonce,
        environment_key: runtime_ascii(&selected_key),
        environment_value: runtime_ascii(&value),
        argv_token: runtime_ascii(&argv),
        pipe_names: pipe_name_bytes.map(|name| runtime_ascii(&name)),
    })
}

fn expected_pipe_names(generation: u64, nonce: &[u8; 32]) -> [Vec<u8>; 4] {
    let mut generation_hex = Vec::with_capacity(16);
    append_hex(
        &mut generation_hex,
        &generation.to_be_bytes(),
        b"0123456789abcdef",
    );
    let mut nonce_hex = Vec::with_capacity(64);
    append_hex(&mut nonce_hex, nonce, b"0123456789ABCDEF");
    PIPE_LANES.map(|lane| {
        let mut name = Vec::with_capacity(
            PIPE_PREFIX.len() + generation_hex.len() + nonce_hex.len() + lane.len() + 2,
        );
        name.extend_from_slice(PIPE_PREFIX);
        name.extend_from_slice(&generation_hex);
        name.push(b'-');
        name.extend_from_slice(&nonce_hex);
        name.push(b'-');
        name.extend_from_slice(lane);
        name
    })
}

fn append_hex(output: &mut Vec<u8>, bytes: &[u8], table: &[u8; 16]) {
    for byte in bytes {
        output.push(table[usize::from(byte >> 4)]);
        output.push(table[usize::from(byte & 0x0f)]);
    }
}

fn encode_base64url(bytes: &[u8]) -> Vec<u8> {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut output = Vec::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let first = chunk[0];
        output.push(TABLE[usize::from(first >> 2)]);
        if chunk.len() == 1 {
            output.push(TABLE[usize::from((first & 0x03) << 4)]);
            continue;
        }
        let second = chunk[1];
        output.push(TABLE[usize::from(((first & 0x03) << 4) | (second >> 4))]);
        if chunk.len() == 2 {
            output.push(TABLE[usize::from((second & 0x0f) << 2)]);
            continue;
        }
        let third = chunk[2];
        output.push(TABLE[usize::from(((second & 0x0f) << 2) | (third >> 6))]);
        output.push(TABLE[usize::from(third & 0x3f)]);
    }
    output
}

fn runtime_ascii(bytes: &[u8]) -> RuntimeString {
    debug_assert!(bytes.is_ascii());
    RuntimeString::from_utf16(
        bytes
            .iter()
            .map(|byte| u16::from(*byte))
            .collect::<Vec<_>>(),
    )
}

#[cfg(windows)]
mod windows {
    use std::ffi::c_void;
    use std::io;
    use std::mem::{size_of, size_of_val};
    use std::os::windows::io::{AsRawHandle, RawHandle};
    use std::ptr::null_mut;
    use std::time::Instant;

    use tokio::net::windows::named_pipe::NamedPipeServer;
    use windows_sys::Win32::Foundation::{CloseHandle, LocalFree, HANDLE, INVALID_HANDLE_VALUE};
    use windows_sys::Win32::Security::Authorization::{
        ConvertSidToStringSidW, ConvertStringSecurityDescriptorToSecurityDescriptorW,
        SDDL_REVISION_1,
    };
    use windows_sys::Win32::Security::{
        GetTokenInformation, TokenUser, SECURITY_ATTRIBUTES, TOKEN_QUERY, TOKEN_USER,
    };
    use windows_sys::Win32::Storage::FileSystem::{
        FILE_FLAG_FIRST_PIPE_INSTANCE, FILE_FLAG_OVERLAPPED, PIPE_ACCESS_INBOUND,
        PIPE_ACCESS_OUTBOUND,
    };
    use windows_sys::Win32::System::Pipes::{
        CreateNamedPipeW, GetNamedPipeClientProcessId, PIPE_READMODE_BYTE,
        PIPE_REJECT_REMOTE_CLIENTS, PIPE_TYPE_BYTE, PIPE_WAIT,
    };
    use windows_sys::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};

    use super::{
        PreparedWindowsBrowserBootstrap, WindowsBrowserBrokerError, PIPE_BUFFER_BYTES,
        PIPE_DEFAULT_WAIT_MS,
    };

    pub(in crate::runtime_process) struct WindowsBrowserBrokerOwner {
        sync_c2h: NamedPipeServer,
        sync_h2c: NamedPipeServer,
        mux_c2h: NamedPipeServer,
        mux_h2c: NamedPipeServer,
    }

    pub(in crate::runtime_process) struct ConnectedWindowsBrowserBroker {
        pub(in crate::runtime_process) sync_c2h: NamedPipeServer,
        pub(in crate::runtime_process) sync_h2c: NamedPipeServer,
        pub(in crate::runtime_process) mux_c2h: NamedPipeServer,
        pub(in crate::runtime_process) mux_h2c: NamedPipeServer,
        pub(in crate::runtime_process) client_pid: u32,
    }

    impl WindowsBrowserBrokerOwner {
        pub(in crate::runtime_process) fn create(
            bootstrap: &PreparedWindowsBrowserBootstrap,
        ) -> Result<Self, WindowsBrowserBrokerError> {
            let mut security = CurrentUserPipeSecurity::new()
                .map_err(|_| WindowsBrowserBrokerError::PipeSecurity)?;
            let names = bootstrap.pipe_names();
            let sync_c2h = create_server(&names[0], true, &mut security)
                .map_err(|_| WindowsBrowserBrokerError::PipeCreation)?;
            let sync_h2c = create_server(&names[1], false, &mut security)
                .map_err(|_| WindowsBrowserBrokerError::PipeCreation)?;
            let mux_c2h = create_server(&names[2], true, &mut security)
                .map_err(|_| WindowsBrowserBrokerError::PipeCreation)?;
            let mux_h2c = create_server(&names[3], false, &mut security)
                .map_err(|_| WindowsBrowserBrokerError::PipeCreation)?;
            Ok(Self {
                sync_c2h,
                sync_h2c,
                mux_c2h,
                mux_h2c,
            })
        }

        pub(in crate::runtime_process) async fn connect(
            self,
            deadline: Instant,
        ) -> Result<ConnectedWindowsBrowserBroker, WindowsBrowserBrokerError> {
            let Self {
                sync_c2h,
                sync_h2c,
                mux_c2h,
                mux_h2c,
            } = self;
            let connect_all = async {
                tokio::try_join!(
                    sync_c2h.connect(),
                    sync_h2c.connect(),
                    mux_c2h.connect(),
                    mux_h2c.connect(),
                )
            };
            tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), connect_all)
                .await
                .map_err(|_| WindowsBrowserBrokerError::BootstrapTimedOut)?
                .map_err(|_| WindowsBrowserBrokerError::PipeConnection)?;

            let client_pids = [
                pipe_client_pid(&sync_c2h),
                pipe_client_pid(&sync_h2c),
                pipe_client_pid(&mux_c2h),
                pipe_client_pid(&mux_h2c),
            ];
            let mut resolved = [0_u32; 4];
            for (slot, result) in resolved.iter_mut().zip(client_pids) {
                *slot = result.map_err(|_| WindowsBrowserBrokerError::PipeClientIdentity)?;
            }
            if resolved[0] == 0 || resolved.iter().any(|pid| *pid != resolved[0]) {
                return Err(WindowsBrowserBrokerError::PipeClientIdentity);
            }
            Ok(ConnectedWindowsBrowserBroker {
                sync_c2h,
                sync_h2c,
                mux_c2h,
                mux_h2c,
                client_pid: resolved[0],
            })
        }
    }

    fn create_server(
        name: &crate::runtime_values::RuntimeString,
        inbound: bool,
        security: &mut CurrentUserPipeSecurity,
    ) -> io::Result<NamedPipeServer> {
        let mut name = name.as_utf16().to_vec();
        name.push(0);
        let direction = if inbound {
            PIPE_ACCESS_INBOUND
        } else {
            PIPE_ACCESS_OUTBOUND
        };
        let open_mode = direction | FILE_FLAG_OVERLAPPED | FILE_FLAG_FIRST_PIPE_INSTANCE;
        let pipe_mode =
            PIPE_TYPE_BYTE | PIPE_READMODE_BYTE | PIPE_WAIT | PIPE_REJECT_REMOTE_CLIENTS;
        let handle = unsafe {
            CreateNamedPipeW(
                name.as_ptr(),
                open_mode,
                pipe_mode,
                1,
                PIPE_BUFFER_BYTES,
                PIPE_BUFFER_BYTES,
                PIPE_DEFAULT_WAIT_MS,
                security.attributes_mut(),
            )
        };
        if handle == INVALID_HANDLE_VALUE {
            return Err(io::Error::last_os_error());
        }
        unsafe { NamedPipeServer::from_raw_handle(handle as RawHandle) }
    }

    fn pipe_client_pid(pipe: &NamedPipeServer) -> io::Result<u32> {
        let mut pid = 0_u32;
        let ok = unsafe { GetNamedPipeClientProcessId(pipe.as_raw_handle() as HANDLE, &mut pid) };
        if ok == 0 || pid == 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(pid)
        }
    }

    struct CurrentUserPipeSecurity {
        descriptor: *mut c_void,
        attributes: SECURITY_ATTRIBUTES,
    }

    impl CurrentUserPipeSecurity {
        fn new() -> io::Result<Self> {
            let mut token: HANDLE = null_mut();
            if unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) } == 0 {
                return Err(io::Error::last_os_error());
            }
            let token = OwnedHandle(token);

            let mut byte_len = 0_u32;
            unsafe {
                GetTokenInformation(token.0, TokenUser, null_mut(), 0, &mut byte_len);
            }
            if byte_len < u32::try_from(size_of::<TOKEN_USER>()).unwrap_or(u32::MAX) {
                return Err(io::Error::last_os_error());
            }
            let words = usize::try_from(byte_len)
                .ok()
                .and_then(|bytes| bytes.checked_add(size_of::<usize>() - 1))
                .map(|bytes| bytes / size_of::<usize>())
                .ok_or_else(|| io::Error::other("token user allocation overflow"))?;
            let mut storage = vec![0_usize; words];
            if unsafe {
                GetTokenInformation(
                    token.0,
                    TokenUser,
                    storage.as_mut_ptr().cast(),
                    byte_len,
                    &mut byte_len,
                )
            } == 0
            {
                return Err(io::Error::last_os_error());
            }
            let token_user = unsafe { &*storage.as_ptr().cast::<TOKEN_USER>() };
            let mut sid_string = null_mut();
            if unsafe { ConvertSidToStringSidW(token_user.User.Sid, &mut sid_string) } == 0 {
                return Err(io::Error::last_os_error());
            }
            let sid_string = LocalAllocation(sid_string.cast());
            let sid_units = unsafe {
                let mut length = 0_usize;
                while *sid_string.0.cast::<u16>().add(length) != 0 {
                    length += 1;
                }
                std::slice::from_raw_parts(sid_string.0.cast::<u16>(), length)
            };
            let mut sddl = "D:P(A;;GA;;;".encode_utf16().collect::<Vec<_>>();
            sddl.extend_from_slice(sid_units);
            sddl.extend(")".encode_utf16());
            sddl.push(0);

            let mut descriptor = null_mut();
            if unsafe {
                ConvertStringSecurityDescriptorToSecurityDescriptorW(
                    sddl.as_ptr(),
                    SDDL_REVISION_1,
                    &mut descriptor,
                    null_mut(),
                )
            } == 0
            {
                return Err(io::Error::last_os_error());
            }
            let attributes = SECURITY_ATTRIBUTES {
                nLength: u32::try_from(size_of_val(&SECURITY_ATTRIBUTES::default()))
                    .expect("SECURITY_ATTRIBUTES size fits u32"),
                lpSecurityDescriptor: descriptor,
                bInheritHandle: 0,
            };
            Ok(Self {
                descriptor,
                attributes,
            })
        }

        fn attributes_mut(&mut self) -> *mut SECURITY_ATTRIBUTES {
            &mut self.attributes
        }
    }

    impl Drop for CurrentUserPipeSecurity {
        fn drop(&mut self) {
            if !self.descriptor.is_null() {
                unsafe {
                    LocalFree(self.descriptor);
                }
                self.descriptor = null_mut();
            }
        }
    }

    struct OwnedHandle(HANDLE);

    impl Drop for OwnedHandle {
        fn drop(&mut self) {
            if !self.0.is_null() {
                unsafe {
                    CloseHandle(self.0);
                }
                self.0 = null_mut();
            }
        }
    }

    struct LocalAllocation(*mut c_void);

    impl Drop for LocalAllocation {
        fn drop(&mut self) {
            if !self.0.is_null() {
                unsafe {
                    LocalFree(self.0);
                }
                self.0 = null_mut();
            }
        }
    }
}

#[cfg(windows)]
pub(super) use windows::WindowsBrowserBrokerOwner;

#[cfg(test)]
mod tests {
    use super::*;

    fn ascii_case_equal(left: &[u16], right: &[u16]) -> Result<bool, ()> {
        if left.iter().chain(right).any(|unit| *unit > 0x7f) {
            return Err(());
        }
        let uppercase = |unit: &u16| {
            if (u16::from(b'a')..=u16::from(b'z')).contains(unit) {
                *unit - u16::from(b'a' - b'A')
            } else {
                *unit
            }
        };
        Ok(left.iter().map(uppercase).eq(right.iter().map(uppercase)))
    }

    #[test]
    fn bootstrap_contract_is_canonical_and_collision_free() {
        let mut attempts = [[0_u8; 32]; KEY_ATTEMPTS];
        attempts[0] = [0x11; 32];
        attempts[1] = [0x22; 32];
        let first = format!(
            "{}{}",
            String::from_utf8_lossy(ENV_KEY_PREFIX),
            "11".repeat(32)
        );
        let existing = vec![first.encode_utf16().collect::<Vec<_>>()];
        let seed = WindowsBrowserBootstrapSeed::from_parts(0x12, [0xab; 32], attempts);
        let prepared =
            prepare_windows_browser_bootstrap(&seed, &existing, ascii_case_equal).unwrap();

        let expected_key = format!(
            "{}{}",
            String::from_utf8_lossy(ENV_KEY_PREFIX),
            "22".repeat(32)
        );
        assert_eq!(
            prepared.environment_key.as_utf16(),
            expected_key.encode_utf16().collect::<Vec<_>>()
        );
        let expected_arg = format!("{}{}", String::from_utf8_lossy(ARG_PREFIX), expected_key);
        assert_eq!(
            prepared.argv_token.as_utf16(),
            expected_arg.encode_utf16().collect::<Vec<_>>()
        );
        assert!(prepared.environment_value.starts_with("v1."));
        let names = prepared
            .pipe_names
            .iter()
            .map(|name| name.try_to_utf8().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            names,
            PIPE_LANES
                .iter()
                .map(|lane| format!(
                    "\\\\.\\pipe\\klaus-browser-v1-0000000000000012-{}-{}",
                    "AB".repeat(32),
                    String::from_utf8_lossy(lane)
                ))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn all_sixteen_collisions_fail_closed() {
        let attempts = std::array::from_fn(|index| [u8::try_from(index).unwrap(); 32]);
        let existing = attempts
            .iter()
            .map(|attempt| {
                let mut key = ENV_KEY_PREFIX.to_vec();
                append_hex(&mut key, attempt, b"0123456789ABCDEF");
                key.iter().map(|byte| u16::from(*byte)).collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let seed = WindowsBrowserBootstrapSeed::from_parts(1, [9; 32], attempts);
        assert_eq!(
            prepare_windows_browser_bootstrap(&seed, &existing, ascii_case_equal).unwrap_err(),
            WindowsBrowserBrokerError::BootstrapKeyCollision
        );
    }

    #[test]
    fn unknown_case_comparison_fails_closed() {
        let seed = WindowsBrowserBootstrapSeed::from_parts(1, [2; 32], [[3; 32]; KEY_ATTEMPTS]);
        let existing = vec![vec![0x017f]];
        assert_eq!(
            prepare_windows_browser_bootstrap(&seed, &existing, ascii_case_equal).unwrap_err(),
            WindowsBrowserBrokerError::EnvironmentComparison
        );
    }
}
